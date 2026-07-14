#pragma once
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <random>
#include <chrono>
#include <cstdint>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "Engine.hpp"
#include "Persistence.hpp"

enum class RaftRole { Follower, Candidate, Leader };

struct RaftPeer {
    int id;
    std::string host;
    int raft_port;   // peer's Raft RPC port
    int client_port; // peer's client-protocol port (for NOT_LEADER redirects)
};

// A single Raft node. Owns the election/replication threads and treats the
// engine's WAL (v2 format) as the Raft log. All Raft state is guarded by mtx.
class RaftNode {
public:
    struct Status {
        RaftRole role;
        uint64_t term;
        uint64_t commit_index;
        uint64_t last_log_index;
        int leader_id; // -1 if unknown
    };

    RaftNode(int node_id, std::vector<RaftPeer> peers, int listen_port,
             Engine& engine, const std::string& meta_path);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // Loads persistent state + WAL, then starts acceptor and ticker threads.
    bool start();
    void stop();

    Status get_status();
    std::string get_leader_address();

    enum class ProposeStatus { OK, NOT_LEADER, TIMEOUT };
    struct ProposeResult {
        ProposeStatus status;
        int leader_id; // hint for NOT_LEADER redirects; -1 if unknown
    };

    // Appends op to the replicated log and blocks until it is committed and
    // applied (majority replication), or the timeout expires.
    ProposeResult propose(char op, const std::string& key, const std::string& val,
                          int timeout_ms);

private:
    // Result of handling an inbound RPC, independent of wire format — the
    // gRPC service glue (RaftServiceImpl) translates these to/from protobuf.
    struct RequestVoteResult { uint64_t term; bool vote_granted; };
    struct AppendEntriesResult { uint64_t term; bool success; uint64_t match_index; };

    // Generated gRPC service implementation; defined in Raft.cpp. Has access
    // to RaftNode's private handlers via nested-class member access (C++11).
    class RaftServiceImpl;

    // --- persistent state (survives restarts) ---
    uint64_t current_term = 0;
    int voted_for = -1; // -1 = none this term
    std::vector<WalRecord> log_; // in-memory mirror of the v2 WAL; index is 1-based

    // --- volatile state ---
    RaftRole role = RaftRole::Follower;
    int leader_id = -1;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;

    // --- leader-only replication state (indexed like `peers`) ---
    std::vector<uint64_t> next_index;
    std::vector<uint64_t> match_index;
    std::vector<char> peer_busy; // one in-flight AppendEntries per peer

    const int node_id;
    const std::vector<RaftPeer> peers;
    const int listen_port;
    Engine& engine;
    const std::string meta_path;

    std::mutex mtx;
    std::condition_variable commit_cv; // signaled when last_applied advances
    std::atomic<bool> running{false};
    // Detached sender threads still alive (client-side outbound RPC calls we
    // spawn ourselves). Inbound RPC handling is now gRPC's own thread pool,
    // whose in-flight work is drained by grpc::Server::Shutdown() instead.
    std::atomic<int> inflight_rpcs{0};

    std::unique_ptr<grpc::Server> grpc_server_;
    std::thread grpc_server_thread_; // runs grpc_server_->Wait()
    std::unique_ptr<RaftServiceImpl> service_impl_;
    // Persistent per-peer channel + stub (created once in start(), reused
    // for the node's lifetime) — unlike the old per-RPC tcp_connect.
    std::vector<std::shared_ptr<grpc::Channel>> peer_channels_;
    std::vector<std::unique_ptr<aether::raft::RaftPeer::Stub>> peer_stubs_;

    std::thread ticker_thread;

    std::chrono::steady_clock::time_point election_deadline;
    std::chrono::steady_clock::time_point last_heartbeat_sent;
    std::mt19937 rng;

    // --- helpers (suffix _locked = caller must hold mtx) ---
    void load_persistent_state();
    void persist_meta_locked();
    void reset_election_timer_locked();
    void become_follower_locked(uint64_t term);
    void become_leader_locked();
    uint64_t last_log_index_locked() const;
    uint64_t last_log_term_locked() const;
    void restore_state_machine_locked();

    void ticker_loop();
    void start_election();

    void broadcast_append_entries();
    void broadcast_append_entries_locked();
    void send_append_entries_to_locked(size_t peer_idx);

    void advance_commit_locked();
    void apply_committed_locked();

    // Core Raft handlers, unchanged in logic from the hand-rolled-socket
    // version — only the surrounding transport (how requests/results cross
    // the wire) changed. RaftServiceImpl converts protobuf <-> these params.
    RequestVoteResult handle_request_vote(uint64_t term, int candidate_id,
                                          uint64_t last_log_index, uint64_t last_log_term);
    AppendEntriesResult handle_append_entries(uint64_t term, int leader,
                                              uint64_t prev_log_index, uint64_t prev_log_term,
                                              uint64_t leader_commit,
                                              std::vector<WalRecord> entries);
};

// Human-readable role name for logs and STATUS output.
const char* raft_role_name(RaftRole role);
