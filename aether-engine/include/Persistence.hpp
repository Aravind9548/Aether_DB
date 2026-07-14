#pragma once
#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
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

    // --- Group-commit primitives (Raft/cluster hot path only; append()/
    // append_entry() above are untouched and still used by standalone mode
    // and the once-per-election no-op entry, respectively). ---

    // Buffers a v2 record (fwrite only, no fflush/fsync). Returns a ticket;
    // the write is durable once wait_for_durable(ticket) returns. Lets
    // concurrent callers (leader's propose()) share one fsync instead of
    // each paying their own.
    uint64_t append_entry_buffered(uint64_t term, uint64_t index, char op,
                                   const std::string& key, const std::string& val);

    // Blocks until the group-commit flusher has fsynced through `ticket`.
    void wait_for_durable(uint64_t ticket);

    // Synchronous fsync of everything buffered so far. Used by the
    // follower's AppendEntries handler to batch one RPC's entries into one
    // fsync without needing the background flusher or releasing any lock.
    void flush_now();

    // Background flusher (leader-path only): wakes when a write becomes
    // pending, waits up to window_ms to let concurrent writers add more (or
    // until max_batch entries accumulate, whichever first), then one fsync.
    bool start_group_commit_flusher(int window_ms = 5, int max_batch = 64);
    void stop_group_commit_flusher(); // drains any pending write before returning

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

    // Writes the snapshot file only (same atomic temp-then-rename) - does
    // NOT touch or truncate the WAL. Safe to call in cluster mode, unlike
    // write_snapshot() above: it never touches Raft's log-tracking state
    // (RaftNode::log_ mirrors the WAL's term/index exactly, and truncating
    // it out from under Raft without an InstallSnapshot RPC would desync
    // this node from the cluster). Used by SNAPSHOT_EXPORT.
    bool write_snapshot_only(LockManager& lm);

private:
    std::string wal_path;
    std::string snapshot_path;
    FILE* wal_file = nullptr;
    std::mutex wal_mutex; // Protects WAL append/truncate AND group-commit state below

    // Helper functions for reading binary-safe records
    bool read_record(FILE* f, char& op, std::string& key, std::string& val);
    bool read_record_v2(FILE* f, WalRecord& rec);
    bool read_length_line(FILE* f, uint64_t& out);
    // Detects the R2 magic at the current file position (start of file).
    // Leaves the position after the magic if found, at the start otherwise.
    bool detect_v2(FILE* f);
    void write_record_v2(FILE* f, const WalRecord& rec);

    // --- Group-commit state (guarded by wal_mutex) ---
    uint64_t next_ticket = 0;
    uint64_t durable_ticket = 0;
    std::condition_variable flush_cv;
    std::thread flusher_thread;
    std::atomic<bool> flusher_running{false};
    int flusher_window_ms = 5;
    int flusher_max_batch = 64;
    void flusher_loop();
};
