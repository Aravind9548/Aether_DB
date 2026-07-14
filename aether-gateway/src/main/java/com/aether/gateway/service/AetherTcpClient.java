package com.aether.gateway.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;

@Service
public class AetherTcpClient {

    @Value("${aether.engine.host}")
    private volatile String host;

    @Value("${aether.engine.port}")
    private volatile int port;

    @Value("${aether.pool.size:20}")
    private int poolSize;

    // Comma-separated host:port failover seeds; empty for single-node setups.
    @Value("${aether.engine.nodes:}")
    private String nodesSpec;

    private java.util.List<String> clusterNodes = java.util.Collections.emptyList();
    private final java.util.concurrent.atomic.AtomicInteger nodeCursor =
            new java.util.concurrent.atomic.AtomicInteger(0);

    // A pooled socket remembers which node it is connected to. All cluster
    // nodes share the same port, so comparing ports alone cannot detect that
    // a leader redirect moved us to a different host.
    private static final class PooledConn {
        Socket socket;
        String target; // "host:port" this socket is connected to

        PooledConn(Socket socket, String target) {
            this.socket = socket;
            this.target = target;
        }
    }

    private BlockingQueue<PooledConn> connectionPool;

    // Rotates host/port to the next configured cluster node so a dead leader
    // does not permanently strand the gateway on an unreachable address.
    private void rotateToNextNode() {
        if (clusterNodes.isEmpty()) return;
        String next = clusterNodes.get(Math.floorMod(nodeCursor.getAndIncrement(), clusterNodes.size()));
        String[] parts = next.split(":");
        if (parts.length == 2) {
            System.out.println("[Gateway] Failing over to cluster node " + next);
            this.host = parts[0];
            this.port = Integer.parseInt(parts[1]);
        }
    }

    @PostConstruct
    public void initPool() {
        if (nodesSpec != null && !nodesSpec.isBlank()) {
            clusterNodes = new java.util.ArrayList<>();
            for (String n : nodesSpec.split(",")) {
                if (!n.isBlank()) clusterNodes.add(n.trim());
            }
        }
        connectionPool = new ArrayBlockingQueue<>(poolSize);
        int maxRetries = 5;
        int delayMs = 2000;

        for (int i = 0; i < poolSize; i++) {
            PooledConn conn = null;
            for (int retry = 0; retry < maxRetries; retry++) {
                try {
                    Socket socket = new Socket(host, port);
                    socket.setKeepAlive(true);
                    conn = new PooledConn(socket, host + ":" + port);
                    break;
                } catch (IOException e) {
                    if (retry == maxRetries - 1) {
                        throw new RuntimeException("Failed to connect to AetherDB after max retries", e);
                    }
                    rotateToNextNode();
                    try { TimeUnit.MILLISECONDS.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
            connectionPool.offer(conn);
        }
    }

    public String sendCommand(String rawCommand) throws IOException {
        int retries = 5;
        Set<String> visited = new HashSet<>();
        for (int attempt = 0; attempt < retries; attempt++) {
            PooledConn conn = null;
            try {
                conn = connectionPool.take();

                // Reconnect if this pooled socket points at a different node
                // than the current leader, or is no longer healthy.
                String want = host + ":" + port;
                if (!want.equals(conn.target) || conn.socket == null || conn.socket.isClosed()
                        || !conn.socket.isConnected() || conn.socket.isOutputShutdown()) {
                    reconnect(conn);
                }

                PrintWriter out = new PrintWriter(conn.socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.socket.getInputStream()));

                out.println(rawCommand);
                String response = in.readLine();

                if (response == null) {
                    throw new IOException("Remote server closed connection");
                }

                if (response.startsWith("ERR NOT_LEADER ")) {
                    String leaderAddr = response.substring("ERR NOT_LEADER ".length()).trim();
                    if ("unknown".equalsIgnoreCase(leaderAddr) || visited.contains(leaderAddr)) {
                        // Leader election in progress or redirection loop detected, back off and retry
                        try { TimeUnit.MILLISECONDS.sleep(300); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                        visited.clear();
                        continue;
                    }

                    visited.add(leaderAddr);

                    String[] parts = leaderAddr.split(":");
                    if (parts.length == 2) {
                        String newHost = parts[0];
                        int newPort = Integer.parseInt(parts[1]);
                        if (!newHost.equals(this.host) || newPort != this.port) {
                            System.out.println("[Gateway] Leader redirected from " + this.host + ":" + this.port + " to " + newHost + ":" + newPort);
                            this.host = newHost;
                            this.port = newPort;
                        }
                    }
                    continue; // Retry sending command to the new leader
                }

                return response;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread interrupted while fetching socket", e);
            } catch (IOException e) {
                if (conn != null) {
                    closeConn(conn);
                }
                if (attempt == retries - 1) {
                    throw e;
                }
                // Current node unreachable (e.g. killed leader): try the next seed.
                rotateToNextNode();
                try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            } finally {
                if (conn != null) {
                    connectionPool.offer(conn); // Slot returns even if its socket is closed
                }
            }
        }
        throw new IOException("Failed to send command after " + retries + " retries due to leader elections");
    }

    private void closeConn(PooledConn conn) {
        if (conn.socket != null) {
            try { conn.socket.close(); } catch (IOException ignored) {}
        }
    }

    private void reconnect(PooledConn conn) throws IOException {
        closeConn(conn);
        conn.socket = new Socket(host, port);
        conn.socket.setKeepAlive(true);
        conn.target = host + ":" + port;
    }

    @PreDestroy
    public void shutdownPool() {
        for (PooledConn conn : connectionPool) {
            if (conn != null) closeConn(conn);
        }
    }
}
