package com.aether.gateway.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.Optional;

@Service
public class StorageService {

    @Autowired
    private AetherEngineClient engineClient;

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
        if (!engineClient.set(internalKey, value)) {
            throw new IOException("Engine operation failed for SET " + internalKey);
        }
    }

    public Optional<String> get(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        return engineClient.get(internalKey);
    }

    public boolean delete(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        return engineClient.delete(internalKey);
    }
}
