#pragma once
#include <string>
#include <optional>
#include "LockManager.hpp"
#include "Persistence.hpp"

class Engine {
public:
    Engine(const std::string& wal_path, const std::string& snapshot_path, size_t shard_count = 16);
    ~Engine();

    // Reconstruct state on startup
    void initialize();

    // Core Key-Value Operations
    void set(const std::string& key, const std::string& val);
    std::optional<std::string> get(const std::string& key);
    bool remove(const std::string& key);

    // Compaction
    bool compact();

    LockManager& get_lock_manager();

private:
    LockManager lock_manager;
    WalManager wal_manager;
};
