#pragma once
#include <string>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include "Engine.hpp"

class RaftNode;

class Server {
public:
    Server(Engine& engine, int port, RaftNode* raft = nullptr,
           int propose_timeout_ms = 5000);
    ~Server();

    // Prevent copy/move
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();
    void stop();

private:
    void handle_client(int client_sock);
    std::string process_line(const std::string& line);

    Engine& engine;
    int port;
    RaftNode* raft;
    int propose_timeout_ms;
    int server_fd = -1;
    std::atomic<bool> running{false};
    std::atomic<int> active_clients{0};

    std::unordered_set<int> client_fds;
    std::mutex clients_mutex;
};
