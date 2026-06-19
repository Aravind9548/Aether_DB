package com.aether.gateway.controller;

import com.aether.gateway.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.security.Principal;

@RestController
@RequestMapping("/api/v1/storage")
public class StorageController {

    @Autowired
    private StorageService storageService;

    @PostMapping
    public ResponseEntity<String> set(@RequestParam String key, @RequestBody String value, Principal principal) {
        String tenantId = principal.getName(); // Extract verified username/tenant from JWT context
        try {
            storageService.put(tenantId, key, value);
            return ResponseEntity.ok("OK");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key, Principal principal) {
        String tenantId = principal.getName();
        try {
            return storageService.get(tenantId, key)
                    .map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body("Key not found in your tenant workspace"));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key, Principal principal) {
        String tenantId = principal.getName();
        try {
            boolean deleted = storageService.delete(tenantId, key);
            return deleted ? ResponseEntity.ok("Deleted") : ResponseEntity.status(HttpStatus.NOT_FOUND).body("Key not found");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
}
