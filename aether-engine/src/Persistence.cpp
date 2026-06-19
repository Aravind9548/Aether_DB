#include "Persistence.hpp"
#include <iostream>
#include <stdexcept>
#include <unistd.h>

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

int WalManager::replayInto(LockManager& lm) {
    FILE* f = std::fopen(wal_path.c_str(), "rb");
    if (!f) return 0;
    
    int count = 0;
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
    
    std::fclose(f);
    return count;
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
