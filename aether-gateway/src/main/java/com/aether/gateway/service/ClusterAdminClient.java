package com.aether.gateway.service;

import aether.admin.*;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// Cluster introspection + chaos-demo client for the dashboard's Cluster
// Console. Deliberately separate from AetherEngineClient: every method here
// takes an explicit target address rather than touching that class's shared
// mutable host/port (used by the SET/GET/DEL leader-redirect-following
// path), so polling the whole cluster's status can never interfere with an
// in-flight data operation.
@Service
public class ClusterAdminClient {

    @Autowired
    private AetherEngineClient engineClient;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private static final int STATUS_DEADLINE_MS = 1500;

    public record NodeStatus(String address, boolean healthy, String role, long term,
                              long commitIndex, long lastLogIndex, String leaderHint) {}

    public record TenantKeyEntry(String key, String value, int shard) {}

    private AetherAdminGrpc.AetherAdminBlockingStub stubFor(String address) {
        String[] parts = address.split(":");
        ManagedChannel channel = channels.computeIfAbsent(address, a ->
                NettyChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                        .usePlaintext()
                        .build());
        return AetherAdminGrpc.newBlockingStub(channel)
                .withDeadlineAfter(STATUS_DEADLINE_MS, TimeUnit.MILLISECONDS);
    }

    // Queries every configured node independently - one unreachable node
    // (e.g. mid-restart after a kill) marks itself unhealthy rather than
    // failing the whole cluster view.
    public List<NodeStatus> getClusterStatus() {
        List<NodeStatus> results = new ArrayList<>();
        for (String address : engineClient.getClusterNodes()) {
            try {
                StatusResponse resp = stubFor(address).status(Empty.newBuilder().build());
                results.add(new NodeStatus(address, true, resp.getRole(), resp.getTerm(),
                        resp.getCommitIndex(), resp.getLastLogIndex(), resp.getLeaderHint()));
            } catch (StatusRuntimeException e) {
                results.add(new NodeStatus(address, false, "UNREACHABLE", 0, 0, 0, ""));
            }
        }
        return results;
    }

    // A follower-served, slightly-stale read is fine for browsing - unlike
    // SET/GET/DEL there's no correctness requirement to hit the leader here,
    // so this just tries nodes in order until one responds.
    public List<TenantKeyEntry> listKeys(String tenantId) throws IOException {
        String prefix = tenantId.length() + ":" + tenantId + ":";
        IOException lastError = null;
        for (String address : engineClient.getClusterNodes()) {
            try {
                ListKeysResponse resp = stubFor(address).listKeys(Empty.newBuilder().build());
                List<TenantKeyEntry> result = new ArrayList<>();
                for (KeyValue kv : resp.getEntriesList()) {
                    if (kv.getKey().startsWith(prefix)) {
                        result.add(new TenantKeyEntry(kv.getKey().substring(prefix.length()),
                                kv.getVal(), kv.getShardId()));
                    }
                }
                return result;
            } catch (StatusRuntimeException e) {
                lastError = new IOException("Node " + address + " unreachable: " + e.getMessage());
            }
        }
        throw lastError != null ? lastError : new IOException("No cluster nodes configured");
    }

    // Finds whichever node currently reports itself as LEADER and sends it
    // KillSelf. A clear error (not a silent no-op) if none is found - e.g. a
    // mid-election window where no node has won yet.
    public boolean killLeader() throws IOException {
        for (NodeStatus node : getClusterStatus()) {
            if (node.healthy() && "LEADER".equals(node.role())) {
                try {
                    KillSelfResponse resp = stubFor(node.address()).killSelf(Empty.newBuilder().build());
                    return resp.getAcknowledged();
                } catch (StatusRuntimeException e) {
                    throw new IOException("Failed to reach leader " + node.address() + ": " + e.getMessage());
                }
            }
        }
        throw new IOException("No current leader found - cluster may be mid-election, try again shortly");
    }

    @PreDestroy
    public void shutdownChannels() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
    }
}
