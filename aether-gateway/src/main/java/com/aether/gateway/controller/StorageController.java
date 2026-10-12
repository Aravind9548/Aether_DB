package com.aether.gateway.controller;

import com.aether.gateway.service.AetherEngineClient;
import com.aether.gateway.service.ClusterAdminClient;
import com.aether.gateway.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/storage")
public class StorageController {

    @Autowired
    private StorageService storageService;

    public record BatchEntry(String key, String value) {}
    public record BatchResult(String key, boolean ok, String error) {}

    @PostMapping
    public ResponseEntity<String> set(@RequestParam String key, @RequestBody String value, Principal principal) {
        String tenantId = principal.getName(); // Extract verified username/tenant from JWT context
        try {
            AetherEngineClient.SetResult result = storageService.put(tenantId, key, value);
            return withOpHeaders(ResponseEntity.ok(), result.shardId(), result.replicatedCount(), result.clusterSize())
                    .body("OK");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key, Principal principal) {
        String tenantId = principal.getName();
        try {
            AetherEngineClient.GetResult result = storageService.get(tenantId, key);
            if (result.value().isPresent()) {
                return withOpHeaders(ResponseEntity.ok(), result.shardId(), -1, -1).body(result.value().get());
            }
            return withOpHeaders(ResponseEntity.status(HttpStatus.NOT_FOUND), result.shardId(), -1, -1)
                    .body("Key not found in your tenant workspace");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key, Principal principal) {
        String tenantId = principal.getName();
        try {
            AetherEngineClient.DeleteResult result = storageService.delete(tenantId, key);
            ResponseEntity.BodyBuilder builder = result.deleted()
                    ? ResponseEntity.ok()
                    : ResponseEntity.status(HttpStatus.NOT_FOUND);
            return withOpHeaders(builder, result.shardId(), result.replicatedCount(), result.clusterSize())
                    .body(result.deleted() ? "Deleted" : "Key not found");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    // Key Browser (Cluster Console) - lists every key currently held in this
    // tenant's namespace, not just ones queried in this browser session.
    @GetMapping("/keys")
    public ResponseEntity<?> listKeys(Principal principal) {
        String tenantId = principal.getName();
        try {
            return ResponseEntity.ok(storageService.listKeys(tenantId));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    // Batch Operations (Cluster Console) - a client-orchestrated loop over the
    // existing single-key SET path, NOT a new atomic multi-key primitive.
    // Each entry succeeds or fails independently and is reported as such.
    @PostMapping("/batch")
    public ResponseEntity<List<BatchResult>> batchSet(@RequestBody List<BatchEntry> entries, Principal principal) {
        String tenantId = principal.getName();
        List<BatchResult> results = new ArrayList<>();
        for (BatchEntry entry : entries) {
            try {
                storageService.put(tenantId, entry.key(), entry.value());
                results.add(new BatchResult(entry.key(), true, null));
            } catch (IOException e) {
                results.add(new BatchResult(entry.key(), false, e.getMessage()));
            }
        }
        return ResponseEntity.ok(results);
    }

    private ResponseEntity.BodyBuilder withOpHeaders(ResponseEntity.BodyBuilder builder, int shardId,
                                                      int replicatedCount, int clusterSize) {
        builder.header("X-Shard-Id", String.valueOf(shardId));
        if (replicatedCount >= 0) {
            builder.header("X-Replicated-Count", String.valueOf(replicatedCount));
            builder.header("X-Cluster-Size", String.valueOf(clusterSize));
        }
        return builder;
    }
}
