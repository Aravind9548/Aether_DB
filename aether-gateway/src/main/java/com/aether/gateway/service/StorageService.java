package com.aether.gateway.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.Optional;

@Service
public class StorageService {

    @Autowired
    private AetherTcpClient tcpClient;

    /**
     * Constructs a length-prefixed isolated key namespace.
     * Prevents Key Injection Attacks: e.g., if tenantId=1 and userKey=:2:malicious
     * Naive mapping "1::2:malicious" could collide with tenantId=1:2 and key=malicious.
     * Length-prefix mapping creates "1:1::2:malicious", maintaining perfect structural isolation.
     */
    private String buildIsolatedKey(String tenantId, String userKey) {
        return tenantId.length() + ":" + tenantId + ":" + userKey;
    }

    public void put(String tenantId, String key, String value) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        String response = tcpClient.sendCommand("SET " + internalKey + " " + value);
        if (!"OK".equals(response)) {
            throw new IOException("Engine operation failed: " + response);
        }
    }

    public Optional<String> get(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        String response = tcpClient.sendCommand("GET " + internalKey);
        
        // Map the engine's internal representation of a missing key to an empty Java Optional
        if ("(nil)".equals(response) || response == null) {
            return Optional.empty();
        }
        return Optional.of(response);
    }

    public boolean delete(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        String response = tcpClient.sendCommand("DEL " + internalKey);
        return "1".equals(response);
    }
}
