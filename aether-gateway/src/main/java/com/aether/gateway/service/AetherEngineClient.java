package com.aether.gateway.service;

import aether.client.*;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AetherEngineClient {

    @Value("${aether.engine.host}")
    private volatile String host;

    @Value("${aether.engine.port}")
    private volatile int port;

    // Comma-separated host:port failover seeds; empty for single-node setups.
    @Value("${aether.engine.nodes:}")
    private String nodesSpec;

    private List<String> clusterNodes = Collections.emptyList();
    private final AtomicInteger nodeCursor = new AtomicInteger(0);

    // One persistent channel per address, reused across calls. gRPC manages
    // its own connection lifecycle internally, so unlike the old hand-rolled
    // socket pool, there is no separate connection-pool bookkeeping needed.
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private static final int RETRIES = 5;
    private static final long RPC_DEADLINE_SECONDS = 5;

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
    public void init() {
        if (nodesSpec != null && !nodesSpec.isBlank()) {
            clusterNodes = new ArrayList<>();
            for (String n : nodesSpec.split(",")) {
                if (!n.isBlank()) clusterNodes.add(n.trim());
            }
        }
    }

    private AetherClientGrpc.AetherClientBlockingStub stubFor(String targetHost, int targetPort) {
        String key = targetHost + ":" + targetPort;
        ManagedChannel channel = channels.computeIfAbsent(key, k ->
                NettyChannelBuilder.forAddress(targetHost, targetPort)
                        .usePlaintext()
                        .build());
        return AetherClientGrpc.newBlockingStub(channel)
                .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    // Applies a not_leader redirect the same way for all three ops: follow a
    // fresh leader hint, back off and retry on an unknown/looping hint.
    private boolean followRedirect(String leaderHint, Set<String> visited) {
        if (leaderHint == null || leaderHint.isBlank()
                || "unknown".equalsIgnoreCase(leaderHint) || visited.contains(leaderHint)) {
            try { TimeUnit.MILLISECONDS.sleep(300); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            visited.clear();
            return false;
        }
        visited.add(leaderHint);
        String[] parts = leaderHint.split(":");
        if (parts.length == 2) {
            String newHost = parts[0];
            int newPort = Integer.parseInt(parts[1]);
            if (!newHost.equals(host) || newPort != port) {
                System.out.println("[Gateway] Leader redirected from " + host + ":" + port + " to " + newHost + ":" + newPort);
                host = newHost;
                port = newPort;
            }
        }
        return true;
    }

    public boolean set(String key, String val) throws IOException {
        Set<String> visited = new HashSet<>();
        for (int attempt = 0; attempt < RETRIES; attempt++) {
            try {
                SetRequest req = SetRequest.newBuilder().setKey(key).setVal(val).build();
                SetResponse resp = stubFor(host, port).set(req);
                if (resp.getNotLeader()) {
                    followRedirect(resp.getLeaderHint(), visited);
                    continue;
                }
                if (!resp.getError().isEmpty()) {
                    throw new IOException("Engine operation failed: " + resp.getError());
                }
                return resp.getOk();
            } catch (StatusRuntimeException e) {
                rotateToNextNode();
                if (attempt == RETRIES - 1) {
                    throw new IOException("Failed to reach engine after " + RETRIES + " retries", e);
                }
                try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        throw new IOException("Failed to send SET after " + RETRIES + " retries due to leader elections");
    }

    public Optional<String> get(String key) throws IOException {
        Set<String> visited = new HashSet<>();
        for (int attempt = 0; attempt < RETRIES; attempt++) {
            try {
                GetRequest req = GetRequest.newBuilder().setKey(key).build();
                GetResponse resp = stubFor(host, port).get(req);
                if (resp.getNotLeader()) {
                    followRedirect(resp.getLeaderHint(), visited);
                    continue;
                }
                return resp.getFound() ? Optional.of(resp.getVal()) : Optional.empty();
            } catch (StatusRuntimeException e) {
                rotateToNextNode();
                if (attempt == RETRIES - 1) {
                    throw new IOException("Failed to reach engine after " + RETRIES + " retries", e);
                }
                try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        throw new IOException("Failed to send GET after " + RETRIES + " retries due to leader elections");
    }

    public boolean delete(String key) throws IOException {
        Set<String> visited = new HashSet<>();
        for (int attempt = 0; attempt < RETRIES; attempt++) {
            try {
                DeleteRequest req = DeleteRequest.newBuilder().setKey(key).build();
                DeleteResponse resp = stubFor(host, port).delete(req);
                if (resp.getNotLeader()) {
                    followRedirect(resp.getLeaderHint(), visited);
                    continue;
                }
                if (!resp.getError().isEmpty()) {
                    throw new IOException("Engine operation failed: " + resp.getError());
                }
                return resp.getDeleted();
            } catch (StatusRuntimeException e) {
                rotateToNextNode();
                if (attempt == RETRIES - 1) {
                    throw new IOException("Failed to reach engine after " + RETRIES + " retries", e);
                }
                try { TimeUnit.MILLISECONDS.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        throw new IOException("Failed to send DEL after " + RETRIES + " retries due to leader elections");
    }

    @PreDestroy
    public void shutdownChannels() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
    }
}
