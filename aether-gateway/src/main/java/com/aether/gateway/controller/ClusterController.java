package com.aether.gateway.controller;

import com.aether.gateway.service.ClusterAdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/cluster")
public class ClusterController {

    @Autowired
    private ClusterAdminClient clusterAdminClient;

    @GetMapping("/status")
    public ResponseEntity<List<ClusterAdminClient.NodeStatus>> status() {
        return ResponseEntity.ok(clusterAdminClient.getClusterStatus());
    }

    @PostMapping("/kill-leader")
    public ResponseEntity<Map<String, Object>> killLeader() {
        try {
            boolean acknowledged = clusterAdminClient.killLeader();
            return ResponseEntity.ok(Map.of("acknowledged", acknowledged));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("acknowledged", false, "error", e.getMessage()));
        }
    }
}
