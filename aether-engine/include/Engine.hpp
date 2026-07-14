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

    // Core Key-Value Operations (standalone path: WAL-append + apply)
    void set(const std::string& key, const std::string& val);
    std::optional<std::string> get(const std::string& key);
    bool remove(const std::string& key);

    // Apply an operation to in-memory state only, no WAL write.
    // Used by the Raft path once an entry is known to be committed.
    void apply(char op, const std::string& key, const std::string& val);

    // Compaction
    bool compact();

    LockManager& get_lock_manager();
    WalManager& get_wal_manager();

private:
    LockManager lock_manager;
    WalManager wal_manager;
};
