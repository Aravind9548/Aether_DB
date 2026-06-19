#include <iostream>
#include <thread>
#include <atomic>
#include <csignal>
#include <chrono>
#include "Engine.hpp"
#include "Server.hpp"

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
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--shards" && i + 1 < argc) {
            try {
                shards = std::stoull(argv[i + 1]);
            } catch (...) {
                std::cerr << "Invalid shard count: " << argv[i + 1] << std::endl;
                return 1;
            }
            i++;
        }
    }

    std::cout << "Starting AetherDB with " << shards << " shard(s)..." << std::endl;
    
    std::string wal_path = "aether.wal";
    std::string snapshot_path = "aether.snapshot";

    Engine engine(wal_path, snapshot_path, shards);
    engine.initialize();

    Server server(engine, 8080);

    // Watcher thread to poll shutdown_flag and stop the server
    std::thread watcher(signal_watcher_thread, std::ref(server));

    // Compactor thread running every 10s
    std::atomic<bool> server_running{true};
    std::thread compactor(compaction_timer_thread, std::ref(engine), std::ref(server_running));

    // Block on starting the server
    server.start();

    // Signal compactor to exit and join it
    server_running.store(false);
    if (compactor.joinable()) {
        compactor.join();
    }

    // Join watcher thread
    if (watcher.joinable()) {
        watcher.join();
    }

    std::cout << "AetherDB stopped successfully." << std::endl;
    return 0;
}
