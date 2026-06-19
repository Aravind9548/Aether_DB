#pragma once
#include <string>
#include <mutex>
#include <cstdio>
#include "LockManager.hpp"

class WalManager {
public:
    WalManager(const std::string& wal_path, const std::string& snapshot_path);
    ~WalManager();

    // Prevent copy/move
    WalManager(const WalManager&) = delete;
    WalManager& operator=(const WalManager&) = delete;

    // WAL lifecycle
    bool open_wal();
    void close_wal();
    void append(char op, const std::string& key, const std::string& val);
    
    // Load and replay
    int replayInto(LockManager& lm);
    bool load_snapshot(LockManager& lm);

    // Compaction / Snapshotting
    bool write_snapshot(LockManager& lm);

private:
    std::string wal_path;
    std::string snapshot_path;
    FILE* wal_file = nullptr;
    std::mutex wal_mutex; // Protects WAL append/truncate

    // Helper functions for reading binary-safe records
    bool read_record(FILE* f, char& op, std::string& key, std::string& val);
};
