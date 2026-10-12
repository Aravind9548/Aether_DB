#include "Server.hpp"
#include "Raft.hpp"
#include "admin.grpc.pb.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <signal.h>
#include <cstring>
#include <thread>
#include <iostream>
#include <sstream>
#include <algorithm>

// Client gRPC (SET/GET/DEL) listens at client port + this offset. STATUS and
// the raw-TCP text protocol stay on the original port (Phase B scoping
// decision #1 — see the plan) — only the data path moved to gRPC.
static constexpr int CLIENT_GRPC_PORT_OFFSET = 2000;

// do_set/do_get/do_delete's leader_hint (from RaftNode::get_leader_address())
// is "host:<text-protocol port>" — correct for the raw-TCP protocol's own
// redirect-follow logic, but the gRPC client needs the *gRPC* port instead.
// Rewritten here, not in Raft.cpp, since this is purely a gRPC-transport
// concern (the text protocol keeps using the untranslated hint as-is).
static std::string to_grpc_leader_hint(const std::string& text_protocol_addr) {
    size_t colon = text_protocol_addr.rfind(':');
    if (colon == std::string::npos) return text_protocol_addr; // e.g. "unknown"
    try {
        int text_port = std::stoi(text_protocol_addr.substr(colon + 1));
        return text_protocol_addr.substr(0, colon) + ":" +
               std::to_string(text_port + CLIENT_GRPC_PORT_OFFSET);
    } catch (...) {
        return text_protocol_addr;
    }
}

// gRPC AetherClient service glue: converts protobuf requests/responses
// to/from Server's transport-independent do_set/do_get/do_delete. No client
// logic lives here — same idiom as RaftNode::RaftServiceImpl from Phase A.
class Server::ClientServiceImpl final : public aether::client::AetherClient::Service {
public:
    explicit ClientServiceImpl(Server* server) : server_(server) {}

    grpc::Status Set(grpc::ServerContext*, const aether::client::SetRequest* request,
                     aether::client::SetResponse* response) override {
        auto r = server_->do_set(request->key(), request->val());
        response->set_ok(r.ok);
        response->set_not_leader(r.not_leader);
        response->set_leader_hint(to_grpc_leader_hint(r.leader_hint));
        response->set_error(r.error);
        response->set_shard_id(r.shard_id);
        response->set_replicated_count(r.replicated_count);
        response->set_cluster_size(r.cluster_size);
        return grpc::Status::OK;
    }

    grpc::Status Get(grpc::ServerContext*, const aether::client::GetRequest* request,
                     aether::client::GetResponse* response) override {
        auto r = server_->do_get(request->key());
        response->set_found(r.value.has_value());
        response->set_val(r.value.value_or(""));
        response->set_not_leader(r.not_leader);
        response->set_leader_hint(to_grpc_leader_hint(r.leader_hint));
        response->set_shard_id(r.shard_id);
        return grpc::Status::OK;
    }

    grpc::Status Delete(grpc::ServerContext*, const aether::client::DeleteRequest* request,
                        aether::client::DeleteResponse* response) override {
        auto r = server_->do_delete(request->key());
        response->set_deleted(r.ok);
        response->set_not_leader(r.not_leader);
        response->set_leader_hint(to_grpc_leader_hint(r.leader_hint));
        response->set_error(r.error);
        response->set_shard_id(r.shard_id);
        response->set_replicated_count(r.replicated_count);
        response->set_cluster_size(r.cluster_size);
        return grpc::Status::OK;
    }

private:
    Server* server_;
};

// gRPC AetherAdmin service glue: cluster introspection + chaos-demo self-kill
// for the dashboard's Cluster Console. Deliberately separate from
// ClientServiceImpl above - these are not part of the SET/GET/DEL data path.
class Server::AdminServiceImpl final : public aether::admin::AetherAdmin::Service {
public:
    explicit AdminServiceImpl(Server* server) : server_(server) {}

    grpc::Status Status(grpc::ServerContext*, const aether::admin::Empty*,
                        aether::admin::StatusResponse* response) override {
        if (server_->raft) {
            auto st = server_->raft->get_status();
            response->set_role(raft_role_name(st.role));
            response->set_term(st.term);
            response->set_commit_index(st.commit_index);
            response->set_last_log_index(st.last_log_index);
            response->set_leader_hint(server_->raft->get_leader_address());
        } else {
            response->set_role("STANDALONE");
            response->set_term(0);
            response->set_commit_index(0);
            response->set_last_log_index(0);
            response->set_leader_hint("");
        }
        return grpc::Status::OK;
    }

    grpc::Status ListKeys(grpc::ServerContext*, const aether::admin::Empty*,
                          aether::admin::ListKeysResponse* response) override {
        auto& lock_manager = server_->engine.get_lock_manager();
        for (const auto& [k, v] : lock_manager.get_all_pairs()) {
            auto* entry = response->add_entries();
            entry->set_key(k);
            entry->set_val(v);
            entry->set_shard_id(static_cast<int>(lock_manager.shard_of(k)));
        }
        return grpc::Status::OK;
    }

    grpc::Status KillSelf(grpc::ServerContext*, const aether::admin::Empty*,
                          aether::admin::KillSelfResponse* response) override {
        response->set_acknowledged(true);
        // Detach a thread so this response actually reaches the caller before
        // the process disappears. Real ungraceful crash (SIGKILL) - matches
        // what `docker kill` does today, not the graceful SIGTERM path
        // main.cpp's signal_handler already handles - Raft's crash-recovery
        // guarantees are specifically about surviving crashes, not clean exits.
        std::thread([]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
            kill(getpid(), SIGKILL);
        }).detach();
        return grpc::Status::OK;
    }

private:
    Server* server_;
};

Server::Server(Engine& engine, int port, RaftNode* raft, int propose_timeout_ms)
    : engine(engine), port(port), raft(raft), propose_timeout_ms(propose_timeout_ms) {}

Server::~Server() {
    stop();
}

void Server::start_grpc_server() {
    grpc_service_ = std::make_unique<ClientServiceImpl>(this);
    admin_service_ = std::make_unique<AdminServiceImpl>(this);
    std::string addr = "0.0.0.0:" + std::to_string(port + CLIENT_GRPC_PORT_OFFSET);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(grpc_service_.get());
    builder.RegisterService(admin_service_.get());
    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
        std::cerr << "Failed to start client gRPC server on " << addr << std::endl;
        return;
    }
    grpc_server_thread_ = std::thread([this]() { grpc_server_->Wait(); });
    std::cout << "Client gRPC service listening on " << addr << "..." << std::endl;
}

void Server::stop_grpc_server() {
    if (grpc_server_) {
        grpc_server_->Shutdown();
    }
    if (grpc_server_thread_.joinable()) {
        grpc_server_thread_.join();
    }
}

void Server::start() {
    start_grpc_server();
    // Set before the TCP setup below so that stop()'s `if (!running) return`
    // guard still runs stop_grpc_server() if socket()/bind()/listen() fails
    // partway through (otherwise the gRPC server/thread would leak).
    running = true;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket creation failed");
        return;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    if (listen(server_fd, 128) < 0) {
        perror("listen failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    std::cout << "Server listening on port " << port << "..." << std::endl;

    while (running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_sock = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        if (client_sock < 0) {
            if (!running) break;
            perror("accept failed");
            continue;
        }

        if (!running) {
            close(client_sock);
            break;
        }

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            client_fds.insert(client_sock);
        }

        std::thread(&Server::handle_client, this, client_sock).detach();
    }
}

void Server::stop() {
    if (!running) return;
    running = false;

    stop_grpc_server();

    // Shutdown the listener socket to unblock accept()
    if (server_fd != -1) {
        shutdown(server_fd, SHUT_RDWR);
        close(server_fd);
        server_fd = -1;
    }

    // Shutdown all client connections to unblock recv()
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        for (int fd : client_fds) {
            shutdown(fd, SHUT_RDWR);
        }
    }

    // Wait until all client threads exit
    while (active_clients.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void Server::handle_client(int client_sock) {
    active_clients++;
    char buffer[1024];
    std::string client_buffer;

    while (running) {
        ssize_t bytes_received = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            break; // Connection closed or error
        }

        buffer[bytes_received] = '\0';
        client_buffer += buffer;

        // Process all complete lines
        size_t pos;
        while ((pos = client_buffer.find('\n')) != std::string::npos) {
            std::string line = client_buffer.substr(0, pos);
            client_buffer.erase(0, pos + 1);

            std::string response = process_line(line);
            if (!response.empty()) {
                send(client_sock, response.c_str(), response.size(), 0);
            }
        }
    }

    // Clean up
    close(client_sock);
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        client_fds.erase(client_sock);
    }
    active_clients--;
}

// Transport-independent client ops. Logic is unchanged from the original
// inline text-protocol handling below — just extracted so the gRPC
// AetherClient service (ClientServiceImpl, top of this file) can call the
// same code instead of duplicating it.

Server::ClientOpResult Server::do_set(const std::string& key, const std::string& val) {
    ClientOpResult r;
    r.shard_id = static_cast<int>(engine.get_lock_manager().shard_of(key));
    try {
        if (raft) {
            auto res = raft->propose('S', key, val, propose_timeout_ms);
            if (res.status == RaftNode::ProposeStatus::NOT_LEADER) {
                r.not_leader = true;
                r.leader_hint = raft->get_leader_address();
            } else if (res.status == RaftNode::ProposeStatus::TIMEOUT) {
                r.error = "TIMEOUT";
            } else {
                r.ok = true;
                r.replicated_count = res.replicated_count;
                r.cluster_size = res.cluster_size;
            }
        } else {
            engine.set(key, val);
            r.ok = true;
        }
    } catch (const std::exception& e) {
        r.error = e.what();
    }
    return r;
}

Server::ClientOpResult Server::do_get(const std::string& key) {
    ClientOpResult r;
    r.shard_id = static_cast<int>(engine.get_lock_manager().shard_of(key));
    if (raft) {
        auto status = raft->get_status();
        if (status.role != RaftRole::Leader) {
            r.not_leader = true;
            r.leader_hint = raft->get_leader_address();
            return r;
        }
    }
    r.value = engine.get(key); // nullopt == not found
    return r;
}

Server::ClientOpResult Server::do_delete(const std::string& key) {
    ClientOpResult r;
    r.shard_id = static_cast<int>(engine.get_lock_manager().shard_of(key));
    try {
        if (raft) {
            auto res = raft->propose('D', key, "", propose_timeout_ms);
            if (res.status == RaftNode::ProposeStatus::NOT_LEADER) {
                r.not_leader = true;
                r.leader_hint = raft->get_leader_address();
            } else if (res.status == RaftNode::ProposeStatus::TIMEOUT) {
                r.error = "TIMEOUT";
            } else {
                // Raft path can't distinguish "erased" from "key never
                // existed" (propose() only reports commit success) — matches
                // pre-existing text-protocol behavior of always returning "1".
                r.ok = true;
                r.replicated_count = res.replicated_count;
                r.cluster_size = res.cluster_size;
            }
        } else {
            r.ok = engine.remove(key);
        }
    } catch (const std::exception& e) {
        r.error = e.what();
    }
    return r;
}

std::string Server::process_line(const std::string& line) {
    std::string clean_line = line;
    if (!clean_line.empty() && clean_line.back() == '\r') {
        clean_line.pop_back();
    }
    if (clean_line.empty()) return "";

    std::istringstream iss(clean_line);
    std::string cmd, key;
    if (!(iss >> cmd)) return "ERR syntax error\n";

    for (char &c : cmd) {
        c = std::toupper(c);
    }

    if (cmd == "SET") {
        if (!(iss >> key)) return "ERR syntax error\n";
        std::string val;
        std::ws(iss);
        std::getline(iss, val);
        auto r = do_set(key, val);
        if (r.not_leader) return "ERR NOT_LEADER " + r.leader_hint + "\n";
        if (r.error == "TIMEOUT") return "ERR TIMEOUT\n";
        if (!r.error.empty()) return "ERR " + r.error + "\n";
        return "OK\n";
    } else if (cmd == "GET") {
        if (!(iss >> key)) return "ERR syntax error\n";
        auto r = do_get(key);
        if (r.not_leader) return "ERR NOT_LEADER " + r.leader_hint + "\n";
        return r.value.has_value() ? r.value.value() + "\n" : "(nil)\n";
    } else if (cmd == "STATUS") {
        // Introspection: role/term/commit for demos and health checks.
        if (raft) {
            auto st = raft->get_status();
            std::ostringstream oss;
            oss << "role=" << raft_role_name(st.role)
                << " term=" << st.term
                << " commit=" << st.commit_index
                << " last_log=" << st.last_log_index
                << " leader=" << raft->get_leader_address() << "\n";
            return oss.str();
        }
        return "role=STANDALONE\n";
    } else if (cmd == "SNAPSHOT_EXPORT") {
        // Non-destructive: writes the snapshot file only, never touches the
        // WAL (safe in cluster mode - see Engine::export_snapshot). The
        // actual file is retrieved separately (e.g. `docker cp`), same as
        // how raft_demo.py already reads a follower's WAL directly.
        return engine.export_snapshot() ? "OK\n" : "ERR snapshot export failed\n";
    } else if (cmd == "DEL") {
        if (!(iss >> key)) return "ERR syntax error\n";
        auto r = do_delete(key);
        if (r.not_leader) return "ERR NOT_LEADER " + r.leader_hint + "\n";
        if (r.error == "TIMEOUT") return "ERR TIMEOUT\n";
        if (!r.error.empty()) return "ERR " + r.error + "\n";
        return r.ok ? "1\n" : "0\n";
    } else {
        return "ERR unknown command\n";
    }
}
