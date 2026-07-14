#pragma once
#include <string>
#include <mutex>
#include <cstdio>
#include <cstdint>
#include <vector>
#include "LockManager.hpp"

// A single WAL record. In v2 (Raft) WALs, term/index carry Raft log metadata;
// v1 records are surfaced with term=0 and a synthetic sequential index.
struct WalRecord {
    uint64_t term = 0;
    uint64_t index = 0;
    char op = 'S';
    std::string key;
    std::string val;
};

class WalManager {
public:
    WalManager(const std::string& wal_path, const std::string& snapshot_path);
    ~WalManager();

    // Prevent copy/move
    WalManager(const WalManager&) = delete;
    WalManager& operator=(const WalManager&) = delete;

    // WAL lifecycle
    bool open_wal();
    // Open a v2 (Raft-log) WAL: writes the R2 magic on a fresh file,
    // refuses a non-empty file that lacks the magic.
    bool open_wal_v2();
    void close_wal();
    void append(char op, const std::string& key, const std::string& val);
    // Append a v2 record carrying Raft term/index. fsyncs before returning.
    void append_entry(uint64_t term, uint64_t index, char op,
                      const std::string& key, const std::string& val);

    // Load and replay
    int replayInto(LockManager& lm);
    bool load_snapshot(LockManager& lm);
    // Read every record (v1 or v2, auto-detected) without applying it.
    std::vector<WalRecord> read_all_records();
    // Drop every record with rec.index >= index (v2 WALs only).
    // Rewrites the file atomically and reopens it for appends.
    bool truncate_from(uint64_t index);

    // Compaction / Snapshotting
    bool write_snapshot(LockManager& lm);

private:
    std::string wal_path;
    std::string snapshot_path;
    FILE* wal_file = nullptr;
    std::mutex wal_mutex; // Protects WAL append/truncate

    // Helper functions for reading binary-safe records
    bool read_record(FILE* f, char& op, std::string& key, std::string& val);
    bool read_record_v2(FILE* f, WalRecord& rec);
    bool read_length_line(FILE* f, uint64_t& out);
    // Detects the R2 magic at the current file position (start of file).
    // Leaves the position after the magic if found, at the start otherwise.
    bool detect_v2(FILE* f);
    void write_record_v2(FILE* f, const WalRecord& rec);
};
