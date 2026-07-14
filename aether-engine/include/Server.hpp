#pragma once
#include <string>
#include <optional>
#include <atomic>
#include <mutex>
#include <thread>
#include <memory>
#include <unordered_set>
#include <grpcpp/grpcpp.h>
#include "client.grpc.pb.h"
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
    // Result of a client data-path op, transport-independent. The text
    // protocol (process_line) and the gRPC AetherClient service both call
    // these and format the result for their own wire format.
    struct ClientOpResult {
        bool ok = false;      // SET: applied. DEL: erased (best-effort; matches
                               // pre-existing text-protocol behavior where the
                               // Raft path can't distinguish "erased" from
                               // "key never existed").
        bool not_leader = false;
        std::string leader_hint;
        std::optional<std::string> value; // GET only; has_value() == found
        std::string error;
    };

    ClientOpResult do_set(const std::string& key, const std::string& val);
    ClientOpResult do_get(const std::string& key);
    ClientOpResult do_delete(const std::string& key);

    void handle_client(int client_sock);
    std::string process_line(const std::string& line);

    // gRPC AetherClient service; defined in Server.cpp. Has access to
    // Server's private do_set/do_get/do_delete via nested-class member access.
    class ClientServiceImpl;
    void start_grpc_server();
    void stop_grpc_server();

    Engine& engine;
    int port;
    RaftNode* raft;
    int propose_timeout_ms;
    int server_fd = -1;
    std::atomic<bool> running{false};
    std::atomic<int> active_clients{0};

    std::unordered_set<int> client_fds;
    std::mutex clients_mutex;

    std::unique_ptr<ClientServiceImpl> grpc_service_;
    std::unique_ptr<grpc::Server> grpc_server_;
    std::thread grpc_server_thread_;
};
