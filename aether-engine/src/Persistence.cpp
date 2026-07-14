#include "Persistence.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <unistd.h>

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

    // Make sure everything appended so far is on disk before re-reading it.
    if (wal_file) {
        std::fflush(wal_file);
    }

    FILE* in = std::fopen(wal_path.c_str(), "rb");
    if (!in) return false;
    if (!detect_v2(in)) {
        std::fclose(in);
        std::cerr << "truncate_from requires a v2 WAL." << std::endl;
        return false;
    }

    std::vector<WalRecord> kept;
    WalRecord rec;
    while (read_record_v2(in, rec)) {
        if (rec.index < index) {
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

    wal_file = std::fopen(wal_path.c_str(), "a+b");
    return wal_file != nullptr;
}

bool WalManager::load_snapshot(LockManager& lm) {
    FILE* f = std::fopen(snapshot_path.c_str(), "rb");
    if (!f) return false;
    
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

bool WalManager::write_snapshot(LockManager& lm) {
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
    }
    
    std::fflush(f);
    fsync(fileno(f));
    std::fclose(f);
    
    // Atomic rename over the real snapshot path
    if (std::rename(tmp_path.c_str(), snapshot_path.c_str()) != 0) {
        std::remove(tmp_path.c_str());
        return false;
    }
    
    // Now truncate the WAL file
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
