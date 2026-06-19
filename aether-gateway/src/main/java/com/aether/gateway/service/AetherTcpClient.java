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

@Service
public class AetherTcpClient {

    @Value("${aether.engine.host}")
    private String host;

    @Value("${aether.engine.port}")
    private int port;

    @Value("${aether.pool.size:20}")
    private int poolSize;

    private BlockingQueue<Socket> connectionPool;

    @PostConstruct
    public void initPool() {
        connectionPool = new ArrayBlockingQueue<>(poolSize);
        int maxRetries = 5;
        int delayMs = 2000;

        for (int i = 0; i < poolSize; i++) {
            Socket socket = null;
            for (int retry = 0; retry < maxRetries; retry++) {
                try {
                    socket = new Socket(host, port);
                    socket.setKeepAlive(true);
                    break; 
                } catch (IOException e) {
                    if (retry == maxRetries - 1) {
                        throw new RuntimeException("Failed to connect to AetherDB after max retries", e);
                    }
                    try { TimeUnit.MILLISECONDS.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
            connectionPool.offer(socket);
        }
    }

    public String sendCommand(String rawCommand) throws IOException {
        Socket socket = null;
        try {
            // Borrow an open socket from the pool without blocking other threads
            socket = connectionPool.take();
            
            // Validate socket health before sending data
            if (socket.isClosed() || !socket.isConnected() || socket.isOutputShutdown()) {
                socket = reconnect(socket);
            }

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(rawCommand);
            String response = in.readLine();
            
            if (response == null) { // Engine dropped the link unexpectedly
                throw new IOException("Remote server closed connection");
            }
            
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while fetching socket", e);
        } catch (IOException e) {
            // Repair the connection path if an IO exception hits
            if (socket != null) {
                try { socket = reconnect(socket); } catch (Exception ignored) {}
            }
            throw e;
        } finally {
            if (socket != null) {
                connectionPool.offer(socket); // Return connection back to the pool
            }
        }
    }

    private Socket reconnect(Socket oldSocket) throws IOException {
        try { oldSocket.close(); } catch (Exception ignored) {}
        Socket newSocket = new Socket(host, port);
        newSocket.setKeepAlive(true);
        return newSocket;
    }

    @PreDestroy
    public void shutdownPool() {
        for (Socket socket : connectionPool) {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }
}
