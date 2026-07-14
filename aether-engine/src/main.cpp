#include <iostream>
#include <thread>
#include <atomic>
#include <csignal>
#include <chrono>
#include <memory>
#include <sstream>
#include <cstdlib>
#include "Engine.hpp"
#include "Server.hpp"
#include "Raft.hpp"

// Raft RPCs listen at client port + this offset.
static constexpr int RAFT_PORT_OFFSET = 1000;

// Parses "2=host:8080,3=host:8080" into peers (raft port = client port + offset).
static bool parse_peers(const std::string& spec, std::vector<RaftPeer>& out) {
    std::istringstream iss(spec);
    std::string item;
    while (std::getline(iss, item, ',')) {
        if (item.empty()) continue;
        size_t eq = item.find('=');
        size_t colon = item.rfind(':');
        if (eq == std::string::npos || colon == std::string::npos || colon < eq) {
            std::cerr << "Bad peer spec (want id=host:port): " << item << std::endl;
            return false;
        }
        try {
            RaftPeer p;
            p.id = std::stoi(item.substr(0, eq));
            p.host = item.substr(eq + 1, colon - eq - 1);
            p.client_port = std::stoi(item.substr(colon + 1));
            p.raft_port = p.client_port + RAFT_PORT_OFFSET;
            out.push_back(p);
        } catch (...) {
            std::cerr << "Bad peer spec: " << item << std::endl;
            return false;
        }
    }
    return true;
}

std::atomic<bool> shutdown_flag{false};

void signal_handler(int sig) {
    shutdown_flag.store(true);
}

void signal_watcher_thread(Server& server) {
    while (!shutdown_flag.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "\n[System] Shutdown signal received. Initiating graceful shutdown..." << std::endl;
    server.stop();
}

void compaction_timer_thread(Engine& engine, std::atomic<bool>& server_running) {
    // Run compaction every 10 seconds for demo visibility
    while (server_running.load() && !shutdown_flag.load()) {
        for (int i = 0; i < 100; ++i) { // Sleep in small increments to allow rapid shutdown response
            if (shutdown_flag.load() || !server_running.load()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (shutdown_flag.load() || !server_running.load()) break;
        
        std::cout << "[Compaction] Starting auto-compaction..." << std::endl;
        if (engine.compact()) {
            std::cout << "[Compaction] Auto-compaction succeeded." << std::endl;
        } else {
            std::cerr << "[Compaction] Auto-compaction failed." << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    size_t shards = 16;
    int port = 8080;
    int node_id = 0;
    std::string peers_spec;
    std::string data_dir = ".";

    // Env first (Docker), CLI flags override.
    if (const char* e = std::getenv("AETHER_PORT")) port = std::atoi(e);
    if (const char* e = std::getenv("AETHER_NODE_ID")) node_id = std::atoi(e);
    if (const char* e = std::getenv("AETHER_PEERS")) peers_spec = e;
    if (const char* e = std::getenv("AETHER_DATA_DIR")) data_dir = e;
    int propose_timeout_ms = 5000;
    if (const char* e = std::getenv("AETHER_PROPOSE_TIMEOUT_MS")) propose_timeout_ms = std::atoi(e);

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << name << std::endl;
                std::exit(1);
            }
            return argv[++i];
        };
        try {
            if (arg == "--shards") {
                shards = std::stoull(next("--shards"));
            } else if (arg == "--port") {
                port = std::stoi(next("--port"));
            } else if (arg == "--node-id") {
                node_id = std::stoi(next("--node-id"));
            } else if (arg == "--peers") {
                peers_spec = next("--peers");
            } else if (arg == "--data-dir") {
                data_dir = next("--data-dir");
            } else if (arg == "--propose-timeout-ms") {
                propose_timeout_ms = std::stoi(next("--propose-timeout-ms"));
            } else {
                std::cerr << "Unknown argument: " << arg << std::endl;
                return 1;
            }
        } catch (...) {
            std::cerr << "Invalid value for " << arg << std::endl;
            return 1;
        }
    }

    std::vector<RaftPeer> peers;
    if (!peers_spec.empty() && !parse_peers(peers_spec, peers)) {
        return 1;
    }
    const bool cluster_mode = !peers.empty();

    std::cout << "Starting AetherDB with " << shards << " shard(s)"
              << (cluster_mode ? " in CLUSTER mode" : " in standalone mode")
              << "..." << std::endl;

    std::string wal_path = data_dir + "/aether.wal";
    std::string snapshot_path = data_dir + "/aether.snapshot";
    std::string meta_path = data_dir + "/raft.meta";

    Engine engine(wal_path, snapshot_path, shards);

    std::unique_ptr<RaftNode> raft;
    if (cluster_mode) {
        // Raft owns WAL loading/opening (v2) and applies committed entries.
        raft = std::make_unique<RaftNode>(node_id, peers, port + RAFT_PORT_OFFSET,
                                          engine, meta_path);
        if (!raft->start()) {
            std::cerr << "Failed to start Raft node." << std::endl;
            return 1;
        }
    } else {
        engine.initialize();
    }

    Server server(engine, port, raft.get(), propose_timeout_ms);

    // Watcher thread to poll shutdown_flag and stop the server
    std::thread watcher(signal_watcher_thread, std::ref(server));

    // Compactor thread running every 10s. Cluster mode disables compaction:
    // truncating the Raft log requires InstallSnapshot support (future work).
    std::atomic<bool> server_running{true};
    std::thread compactor;
    if (!cluster_mode) {
        compactor = std::thread(compaction_timer_thread, std::ref(engine), std::ref(server_running));
    }

    // Block on starting the server
    server.start();

    // Signal compactor to exit and join it
    server_running.store(false);
    if (compactor.joinable()) {
        compactor.join();
    }

    if (raft) {
        raft->stop();
    }

    // Join watcher thread
    if (watcher.joinable()) {
        watcher.join();
    }

    std::cout << "AetherDB stopped successfully." << std::endl;
    return 0;
}
