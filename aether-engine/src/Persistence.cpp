#include "Persistence.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>

// v2 (Raft-log) WAL files begin with this magic; files without it are v1.
static const char WAL_V2_MAGIC[] = "R2\n";
static const size_t WAL_V2_MAGIC_LEN = 3;

WalManager::WalManager(const std::string& wal_path, const std::string& snapshot_path)
    : wal_path(wal_path), snapshot_path(snapshot_path) {}

WalManager::~WalManager() {
    close_wal();
}

bool WalManager::open_wal() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (wal_file) return true;
    wal_file = std::fopen(wal_path.c_str(), "a+b");
    return wal_file != nullptr;
}

bool WalManager::open_wal_v2() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (wal_file) return true;
    wal_file = std::fopen(wal_path.c_str(), "a+b");
    if (!wal_file) return false;

    // Fresh file: stamp the v2 magic. Existing file: verify it.
    std::fseek(wal_file, 0, SEEK_END);
    long size = std::ftell(wal_file);
    if (size == 0) {
        std::fwrite(WAL_V2_MAGIC, 1, WAL_V2_MAGIC_LEN, wal_file);
        std::fflush(wal_file);
        fsync(fileno(wal_file));
    } else {
        std::fseek(wal_file, 0, SEEK_SET);
        char magic[WAL_V2_MAGIC_LEN];
        size_t n = std::fread(magic, 1, WAL_V2_MAGIC_LEN, wal_file);
        if (n != WAL_V2_MAGIC_LEN || std::memcmp(magic, WAL_V2_MAGIC, WAL_V2_MAGIC_LEN) != 0) {
            std::cerr << "WAL at " << wal_path << " is not a v2 (Raft) log." << std::endl;
            std::fclose(wal_file);
            wal_file = nullptr;
            return false;
        }
        std::fseek(wal_file, 0, SEEK_END);
    }
    return true;
}

void WalManager::close_wal() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (wal_file) {
        std::fclose(wal_file);
        wal_file = nullptr;
    }
}

void WalManager::append(char op, const std::string& key, const std::string& val) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (!wal_file) {
        throw std::runtime_error("WAL file not open for append!");
    }
    
    // Format: <S|D>\n<keylen>\n<key bytes><vallen>\n<value bytes>
    std::fputc(op, wal_file);
    std::fputc('\n', wal_file);
    
    std::fprintf(wal_file, "%zu\n", key.size());
    if (!key.empty()) {
        std::fwrite(key.data(), 1, key.size(), wal_file);
    }
    
    if (op == 'S') {
        std::fprintf(wal_file, "%zu\n", val.size());
        if (!val.empty()) {
            std::fwrite(val.data(), 1, val.size(), wal_file);
        }
    }
    
    std::fflush(wal_file);
    // Durability control: force OS to sync changes to physical storage
    fsync(fileno(wal_file));
}

void WalManager::append_entry(uint64_t term, uint64_t index, char op,
                              const std::string& key, const std::string& val) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (!wal_file) {
        throw std::runtime_error("WAL file not open for append_entry!");
    }
    WalRecord rec{term, index, op, key, val};
    write_record_v2(wal_file, rec);
    std::fflush(wal_file);
    fsync(fileno(wal_file));
}

uint64_t WalManager::append_entry_buffered(uint64_t term, uint64_t index, char op,
                                           const std::string& key, const std::string& val) {
    std::unique_lock<std::mutex> lock(wal_mutex);
    if (!wal_file) {
        throw std::runtime_error("WAL file not open for append_entry_buffered!");
    }
    WalRecord rec{term, index, op, key, val};
    write_record_v2(wal_file, rec); // fwrite only - no fflush/fsync
    uint64_t ticket = ++next_ticket;
    lock.unlock();
    // Always notify (not just on a full batch): the flusher's own idle-wait
    // only wakes on notification, so every write must signal it to transition
    // from idle into its bounded batching window. The max_batch early-exit
    // is handled inside the flusher's own wait predicate, not here.
    flush_cv.notify_all();
    return ticket;
}

void WalManager::wait_for_durable(uint64_t ticket) {
    std::unique_lock<std::mutex> lock(wal_mutex);
    // Deliberately does NOT also wake on !flusher_running: stop_group_commit_
    // flusher() guarantees a final drain (durable_ticket -> next_ticket)
    // before the flusher thread exits, so any ticket issued before shutdown
    // begins is guaranteed to satisfy this predicate for real. Waking early
    // on !flusher_running instead would let a caller believe its write is
    // durable before that final drain has actually happened - a caller must
    // not call this before start_group_commit_flusher() has been started.
    flush_cv.wait(lock, [&] { return durable_ticket >= ticket; });
}

void WalManager::flush_now() {
    std::lock_guard<std::mutex> lock(wal_mutex);
    if (wal_file && next_ticket > durable_ticket) {
        std::fflush(wal_file);
        fsync(fileno(wal_file));
        durable_ticket = next_ticket;
        flush_cv.notify_all();
    }
}

void WalManager::flusher_loop() {
    std::unique_lock<std::mutex> lock(wal_mutex);
    while (flusher_running) {
        // Idle-wait (unbounded) until a write becomes pending, or shutdown.
        flush_cv.wait(lock, [&] { return next_ticket > durable_ticket || !flusher_running; });
        if (!flusher_running && next_ticket == durable_ticket) break;

        // A write just became pending. Wait up to window_ms more to let
        // concurrent writers add to this batch (or until max_batch is hit,
        // signaled early via notify from append_entry_buffered), then flush
        // everything accumulated in one fsync.
        flush_cv.wait_for(lock, std::chrono::milliseconds(flusher_window_ms), [&] {
            return static_cast<int>(next_ticket - durable_ticket) >= flusher_max_batch || !flusher_running;
        });

        if (wal_file && next_ticket > durable_ticket) {
            std::fflush(wal_file);
            fsync(fileno(wal_file));
            durable_ticket = next_ticket;
            flush_cv.notify_all();
        }
    }
    // Final drain on shutdown so a buffered-but-unflushed write is never lost.
    if (wal_file && next_ticket > durable_ticket) {
        std::fflush(wal_file);
        fsync(fileno(wal_file));
        durable_ticket = next_ticket;
        flush_cv.notify_all();
    }
}

bool WalManager::start_group_commit_flusher(int window_ms, int max_batch) {
    flusher_window_ms = window_ms;
    flusher_max_batch = max_batch;
    flusher_running = true;
    flusher_thread = std::thread(&WalManager::flusher_loop, this);
    return true;
}

void WalManager::stop_group_commit_flusher() {
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        flusher_running = false;
    }
    flush_cv.notify_all();
    if (flusher_thread.joinable()) {
        flusher_thread.join();
    }
}

int WalManager::replayInto(LockManager& lm) {
    FILE* f = std::fopen(wal_path.c_str(), "rb");
    if (!f) return 0;

    bool v2 = detect_v2(f);
    int count = 0;

    if (v2) {
        WalRecord rec;
        while (read_record_v2(f, rec)) {
            if (rec.op == 'S') {
                lm.set(rec.key, rec.val);
            } else if (rec.op == 'D') {
                lm.remove(rec.key);
            }
            count++;
        }
    } else {
        char op;
        std::string key, val;
        while (read_record(f, op, key, val)) {
            if (op == 'S') {
                lm.set(key, val);
            } else if (op == 'D') {
                lm.remove(key);
            }
            count++;
        }
    }

    std::fclose(f);
    return count;
}

std::vector<WalRecord> WalManager::read_all_records() {
    std::vector<WalRecord> records;
    FILE* f = std::fopen(wal_path.c_str(), "rb");
    if (!f) return records;

    if (detect_v2(f)) {
        WalRecord rec;
        while (read_record_v2(f, rec)) {
            records.push_back(rec);
        }
    } else {
        // v1: no term/index on disk; synthesize sequential indices from 1.
        char op;
        std::string key, val;
        uint64_t seq = 0;
        while (read_record(f, op, key, val)) {
            records.push_back(WalRecord{0, ++seq, op, key, val});
        }
    }

    std::fclose(f);
    return records;
}

bool WalManager::truncate_from(uint64_t index) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    return rewrite_wal_locked([index](const WalRecord& r) { return r.index < index; });
}

bool WalManager::compact_prefix(uint64_t through_index) {
    std::lock_guard<std::mutex> lock(wal_mutex);
    return rewrite_wal_locked([through_index](const WalRecord& r) { return r.index > through_index; });
}

template <typename Pred>
bool WalManager::rewrite_wal_locked(Pred keep) {
    // Make sure everything appended so far is on disk before re-reading it.
    if (wal_file) {
        std::fflush(wal_file);
    }

    FILE* in = std::fopen(wal_path.c_str(), "rb");
    if (!in) return false;
    if (!detect_v2(in)) {
        std::fclose(in);
        std::cerr << "WAL rewrite requires a v2 WAL." << std::endl;
        return false;
    }

    std::vector<WalRecord> kept;
    WalRecord rec;
    while (read_record_v2(in, rec)) {
        if (keep(rec)) {
            kept.push_back(rec);
        }
    }
    std::fclose(in);

    std::string tmp_path = wal_path + ".tmp";
    FILE* out = std::fopen(tmp_path.c_str(), "wb");
    if (!out) return false;
    std::fwrite(WAL_V2_MAGIC, 1, WAL_V2_MAGIC_LEN, out);
    for (const auto& r : kept) {
        write_record_v2(out, r);
    }
    std::fflush(out);
    fsync(fileno(out));
    std::fclose(out);

    if (wal_file) {
        std::fclose(wal_file);
        wal_file = nullptr;
    }
    if (std::rename(tmp_path.c_str(), wal_path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        // Reopen the original so appends keep working even after a failure.
        wal_file = std::fopen(wal_path.c_str(), "a+b");
        return false;
    }

    fsync_parent_dir(wal_path);
    wal_file = std::fopen(wal_path.c_str(), "a+b");
    return wal_file != nullptr;
}

void WalManager::fsync_parent_dir(const std::string& path) {
    // A rename is only durable once the directory entry itself is synced.
    std::string dir = ".";
    size_t slash = path.rfind('/');
    if (slash != std::string::npos) dir = slash == 0 ? "/" : path.substr(0, slash);
    int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

static const char RAFT_SNAPSHOT_MAGIC[] = "RS2\n";
static const size_t RAFT_SNAPSHOT_MAGIC_LEN = 4;

bool WalManager::skip_raft_snapshot_header(FILE* f, uint64_t* last_index, uint64_t* last_term) {
    char magic[RAFT_SNAPSHOT_MAGIC_LEN];
    size_t n = std::fread(magic, 1, RAFT_SNAPSHOT_MAGIC_LEN, f);
    if (n != RAFT_SNAPSHOT_MAGIC_LEN || std::memcmp(magic, RAFT_SNAPSHOT_MAGIC, RAFT_SNAPSHOT_MAGIC_LEN) != 0) {
        std::fseek(f, 0, SEEK_SET);
        return false;
    }
    uint64_t idx = 0, term = 0;
    if (!read_length_line(f, idx) || !read_length_line(f, term)) {
        std::fseek(f, 0, SEEK_SET);
        return false;
    }
    if (last_index) *last_index = idx;
    if (last_term) *last_term = term;
    return true;
}

bool WalManager::write_raft_snapshot(const std::vector<std::pair<std::string, std::string>>& pairs,
                                     uint64_t last_index, uint64_t last_term) {
    std::string tmp_path = snapshot_path + ".tmp";
    FILE* f = std::fopen(tmp_path.c_str(), "wb");
    if (!f) return false;

    std::fwrite(RAFT_SNAPSHOT_MAGIC, 1, RAFT_SNAPSHOT_MAGIC_LEN, f);
    std::fprintf(f, "%llu\n%llu\n", static_cast<unsigned long long>(last_index),
                 static_cast<unsigned long long>(last_term));
    for (const auto& [k, v] : pairs) {
        std::fputc('S', f);
        std::fputc('\n', f);
        std::fprintf(f, "%zu\n", k.size());
        if (!k.empty()) std::fwrite(k.data(), 1, k.size(), f);
        std::fprintf(f, "%zu\n", v.size());
        if (!v.empty()) std::fwrite(v.data(), 1, v.size(), f);

        // Same chaos hook as write_snapshot_only(): lets chaos_suite.py land
        // a SIGKILL mid-write. Inert unless explicitly set.
        if (const char* delay_ms = std::getenv("AETHER_CHAOS_SNAPSHOT_DELAY_MS")) {
            int ms = std::atoi(delay_ms);
            if (ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }
    }

    std::fflush(f);
    fsync(fileno(f));
    std::fclose(f);
    if (std::rename(tmp_path.c_str(), snapshot_path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        return false;
    }
    // The WAL prefix is about to be discarded on the strength of this file,
    // so the rename itself must be durable first.
    fsync_parent_dir(snapshot_path);
    return true;
}

bool WalManager::read_raft_snapshot(std::vector<std::pair<std::string, std::string>>& pairs,
                                    uint64_t& last_index, uint64_t& last_term, bool* untagged) {
    if (untagged) *untagged = false;
    FILE* f = std::fopen(snapshot_path.c_str(), "rb");
    if (!f) return false;
    if (!skip_raft_snapshot_header(f, &last_index, &last_term)) {
        if (untagged) *untagged = true;
        std::fclose(f);
        return false;
    }
    char op;
    std::string key, val;
    while (read_record(f, op, key, val)) {
        if (op == 'S') pairs.emplace_back(key, val);
    }
    std::fclose(f);
    return true;
}

bool WalManager::load_snapshot(LockManager& lm) {
    FILE* f = std::fopen(snapshot_path.c_str(), "rb");
    if (!f) return false;
    skip_raft_snapshot_header(f, nullptr, nullptr); // tolerate a Raft-tagged file

    int count = 0;
    char op;
    std::string key, val;
    
    while (read_record(f, op, key, val)) {
        if (op == 'S') {
            lm.set(key, val);
            count++;
        }
    }
    
    std::fclose(f);
    std::cout << "Loaded " << count << " keys from snapshot." << std::endl;
    return true;
}

bool WalManager::write_snapshot_only(LockManager& lm) {
    std::string tmp_path = snapshot_path + ".tmp";
    FILE* f = std::fopen(tmp_path.c_str(), "wb");
    if (!f) return false;

    auto pairs = lm.get_all_pairs();
    for (const auto& [k, v] : pairs) {
        std::fputc('S', f);
        std::fputc('\n', f);
        std::fprintf(f, "%zu\n", k.size());
        if (!k.empty()) {
            std::fwrite(k.data(), 1, k.size(), f);
        }
        std::fprintf(f, "%zu\n", v.size());
        if (!v.empty()) {
            std::fwrite(v.data(), 1, v.size(), f);
        }

        // Chaos-testing hook only: lets chaos_suite.py reliably land a
        // SIGKILL mid-write to test the temp-then-rename atomicity below.
        // No-op unless explicitly set; never used in normal operation.
        if (const char* delay_ms = std::getenv("AETHER_CHAOS_SNAPSHOT_DELAY_MS")) {
            int ms = std::atoi(delay_ms);
            if (ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
        }
    }

    std::fflush(f);
    fsync(fileno(f));
    std::fclose(f);

    // Atomic rename over the real snapshot path. If a chaos test kills this
    // process before this line runs, tmp_path is simply abandoned - the real
    // snapshot_path (old snapshot, or none) is untouched, exactly the
    // property this method exists to prove.
    if (std::rename(tmp_path.c_str(), snapshot_path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        return false;
    }
    return true;
}

bool WalManager::write_snapshot(LockManager& lm) {
    if (!write_snapshot_only(lm)) {
        return false;
    }

    // Now truncate the WAL file. NOT safe in cluster mode - RaftNode::log_
    // mirrors the WAL's term/index exactly, and truncating it out from under
    // Raft without an InstallSnapshot RPC would desync this node from the
    // cluster. Only called from the standalone-mode compaction timer
    // (main.cpp gates this) and never from SNAPSHOT_EXPORT.
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        if (wal_file) {
            std::fclose(wal_file);
            wal_file = nullptr;
        }
        // Open in write-binary mode to truncate to 0 bytes
        FILE* tf = std::fopen(wal_path.c_str(), "wb");
        if (tf) {
            std::fflush(tf);
            fsync(fileno(tf));
            std::fclose(tf);
        }
        // Reopen for append
        wal_file = std::fopen(wal_path.c_str(), "a+b");
    }

    std::cout << "Snapshot compaction complete. WAL truncated." << std::endl;
    return true;
}

bool WalManager::detect_v2(FILE* f) {
    long start = std::ftell(f);
    char magic[WAL_V2_MAGIC_LEN];
    size_t n = std::fread(magic, 1, WAL_V2_MAGIC_LEN, f);
    if (n == WAL_V2_MAGIC_LEN && std::memcmp(magic, WAL_V2_MAGIC, WAL_V2_MAGIC_LEN) == 0) {
        return true;
    }
    std::fseek(f, start, SEEK_SET);
    return false;
}

bool WalManager::read_length_line(FILE* f, uint64_t& out) {
    std::string digits;
    while (true) {
        int c = std::fgetc(f);
        if (c == EOF) return false;
        if (c == '\n') break;
        digits.push_back(static_cast<char>(c));
    }
    if (digits.empty()) return false;
    try {
        out = std::stoull(digits);
    } catch (...) {
        return false;
    }
    return true;
}

void WalManager::write_record_v2(FILE* f, const WalRecord& rec) {
    // Format: <S|D>\n<term>\n<index>\n<keylen>\n<key bytes>[<vallen>\n<value bytes>]
    std::fputc(rec.op, f);
    std::fputc('\n', f);
    std::fprintf(f, "%llu\n", static_cast<unsigned long long>(rec.term));
    std::fprintf(f, "%llu\n", static_cast<unsigned long long>(rec.index));
    std::fprintf(f, "%zu\n", rec.key.size());
    if (!rec.key.empty()) {
        std::fwrite(rec.key.data(), 1, rec.key.size(), f);
    }
    if (rec.op == 'S') {
        std::fprintf(f, "%zu\n", rec.val.size());
        if (!rec.val.empty()) {
            std::fwrite(rec.val.data(), 1, rec.val.size(), f);
        }
    }
}

bool WalManager::read_record_v2(FILE* f, WalRecord& rec) {
    int c = std::fgetc(f);
    if (c == EOF) return false;
    rec.op = static_cast<char>(c);
    // 'N' = Raft no-op entry (leader commits it to finalize prior-term entries).
    if (rec.op != 'S' && rec.op != 'D' && rec.op != 'N') return false;

    int nl = std::fgetc(f);
    if (nl != '\n') return false;

    if (!read_length_line(f, rec.term)) return false;
    if (!read_length_line(f, rec.index)) return false;

    uint64_t keylen = 0;
    if (!read_length_line(f, keylen)) return false;
    rec.key.resize(keylen);
    if (keylen > 0) {
        if (std::fread(&rec.key[0], 1, keylen, f) != keylen) return false;
    }

    if (rec.op == 'S') {
        uint64_t vallen = 0;
        if (!read_length_line(f, vallen)) return false;
        rec.val.resize(vallen);
        if (vallen > 0) {
            if (std::fread(&rec.val[0], 1, vallen, f) != vallen) return false;
        }
    } else {
        rec.val.clear();
    }

    return true;
}

bool WalManager::read_record(FILE* f, char& op, std::string& key, std::string& val) {
    int c = std::fgetc(f);
    if (c == EOF) return false;
    op = static_cast<char>(c);
    if (op != 'S' && op != 'D') return false;
    
    int nl = std::fgetc(f);
    if (nl != '\n') return false;
    
    std::string keylen_str;
    while (true) {
        int kc = std::fgetc(f);
        if (kc == EOF) return false;
        if (kc == '\n') break;
        keylen_str.push_back(static_cast<char>(kc));
    }
    
    size_t keylen = 0;
    try {
        keylen = std::stoull(keylen_str);
    } catch (...) {
        return false;
    }
    
    key.resize(keylen);
    if (keylen > 0) {
        size_t read_bytes = std::fread(&key[0], 1, keylen, f);
        if (read_bytes != keylen) return false;
    }
    
    if (op == 'S') {
        std::string vallen_str;
        while (true) {
            int vc = std::fgetc(f);
            if (vc == EOF) return false;
            if (vc == '\n') break;
            vallen_str.push_back(static_cast<char>(vc));
        }
        
        size_t vallen = 0;
        try {
            vallen = std::stoull(vallen_str);
        } catch (...) {
            return false;
        }
        
        val.resize(vallen);
        if (vallen > 0) {
            size_t read_bytes = std::fread(&val[0], 1, vallen, f);
            if (read_bytes != vallen) return false;
        }
    } else {
        val.clear();
    }
    
    return true;
}
