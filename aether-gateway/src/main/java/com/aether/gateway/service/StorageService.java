package com.aether.gateway.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.List;

@Service
public class StorageService {

    @Autowired
    private AetherEngineClient engineClient;

    @Autowired
    private ClusterAdminClient clusterAdminClient;

    /**
     * Constructs a length-prefixed isolated key namespace.
     * Prevents Key Injection Attacks: e.g., if tenantId=1 and userKey=:2:malicious
     * Naive mapping "1::2:malicious" could collide with tenantId=1:2 and key=malicious.
     * Length-prefix mapping creates "1:1::2:malicious", maintaining perfect structural isolation.
     */
    private String buildIsolatedKey(String tenantId, String userKey) {
        return tenantId.length() + ":" + tenantId + ":" + userKey;
    }

    public AetherEngineClient.SetResult put(String tenantId, String key, String value) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        AetherEngineClient.SetResult result = engineClient.set(internalKey, value);
        if (!result.ok()) {
            throw new IOException("Engine operation failed for SET " + internalKey);
        }
        return result;
    }

    public AetherEngineClient.GetResult get(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        return engineClient.get(internalKey);
    }

    public AetherEngineClient.DeleteResult delete(String tenantId, String key) throws IOException {
        String internalKey = buildIsolatedKey(tenantId, key);
        return engineClient.delete(internalKey);
    }

    public List<ClusterAdminClient.TenantKeyEntry> listKeys(String tenantId) throws IOException {
        return clusterAdminClient.listKeys(tenantId);
    }
}
