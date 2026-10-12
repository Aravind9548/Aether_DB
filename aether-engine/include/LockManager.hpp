#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <optional>
#include <functional>
#include <utility>

class LockManager {
public:
    LockManager(size_t shard_count = 16) : shard_count(shard_count), shards(shard_count) {}

    std::optional<std::string> get(const std::string& key) {
        size_t shard_idx = get_shard_index(key);
        std::shared_lock<std::shared_mutex> lock(shards[shard_idx].mutex);
        auto it = shards[shard_idx].map.find(key);
        if (it == shards[shard_idx].map.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void set(const std::string& key, const std::string& val) {
        size_t shard_idx = get_shard_index(key);
        std::unique_lock<std::shared_mutex> lock(shards[shard_idx].mutex);
        shards[shard_idx].map[key] = val;
    }

    bool remove(const std::string& key) {
        size_t shard_idx = get_shard_index(key);
        std::unique_lock<std::shared_mutex> lock(shards[shard_idx].mutex);
        auto it = shards[shard_idx].map.find(key);
        if (it != shards[shard_idx].map.end()) {
            shards[shard_idx].map.erase(it);
            return true;
        }
        return false;
    }

    // Collect all key-value pairs safely from all shards
    std::vector<std::pair<std::string, std::string>> get_all_pairs() {
        std::vector<std::pair<std::string, std::string>> result;
        for (size_t i = 0; i < shard_count; ++i) {
            std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
            for (const auto& [k, v] : shards[i].map) {
                result.emplace_back(k, v);
            }
        }
        return result;
    }

    // Clear all shards
    void clear() {
        for (size_t i = 0; i < shard_count; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
            shards[i].map.clear();
        }
    }

    // Which shard a key routes to - exposed read-only so callers (e.g. the
    // admin/status path) can report it without duplicating the hash-mod math.
    size_t shard_of(const std::string& key) const {
        return get_shard_index(key);
    }

private:
    struct Shard {
        std::unordered_map<std::string, std::string> map;
        std::shared_mutex mutex;
    };

    size_t shard_count;
    std::vector<Shard> shards;

    size_t get_shard_index(const std::string& key) const {
        return std::hash<std::string>{}(key) % shard_count;
    }
};
