#include "Engine.hpp"
#include <iostream>

Engine::Engine(const std::string& wal_path, const std::string& snapshot_path, size_t shard_count)
    : lock_manager(shard_count), wal_manager(wal_path, snapshot_path) {}

Engine::~Engine() {
    wal_manager.close_wal();
}

void Engine::initialize() {
    // 1. First load from snapshot if exists
    wal_manager.load_snapshot(lock_manager);
    
    // 2. Replay WAL edits made after snapshot
    int replayed = wal_manager.replayInto(lock_manager);
    std::cout << "Replayed " << replayed << " transactions from WAL." << std::endl;
    
    // 3. Open WAL for future appends
    if (!wal_manager.open_wal()) {
        std::cerr << "Warning: Could not open WAL for appends!" << std::endl;
    }
}

void Engine::set(const std::string& key, const std::string& val) {
    // CRITICAL: Log to WAL before applying to memory!
    // If we crash between log and memory write, the log replay restores it.
    // If we crash before logging, the client never receives "OK".
    wal_manager.append('S', key, val);
    apply('S', key, val);
}

std::optional<std::string> Engine::get(const std::string& key) {
    return lock_manager.get(key);
}

bool Engine::remove(const std::string& key) {
    // CRITICAL: Log to WAL before applying to memory!
    wal_manager.append('D', key, "");
    return lock_manager.remove(key);
}

void Engine::apply(char op, const std::string& key, const std::string& val) {
    if (op == 'S') {
        lock_manager.set(key, val);
    } else if (op == 'D') {
        lock_manager.remove(key);
    }
}

bool Engine::compact() {
    return wal_manager.write_snapshot(lock_manager);
}

bool Engine::export_snapshot() {
    return wal_manager.write_snapshot_only(lock_manager);
}

LockManager& Engine::get_lock_manager() {
    return lock_manager;
}

WalManager& Engine::get_wal_manager() {
    return wal_manager;
}
