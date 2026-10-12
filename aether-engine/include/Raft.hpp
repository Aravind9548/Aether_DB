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
        uint64_t last_applied;
        uint64_t snapshot_index; // log entries <= this live only in the snapshot
        size_t log_entries;      // entries still held in the (compacted) log
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
        // Dashboard visibility: how many nodes (including self) had
        // replicated this entry by the time it committed, and cluster size.
        // Only meaningful when status == OK; -1/-1 otherwise.
        int replicated_count = -1;
        int cluster_size = -1;
    };

    // Appends op to the replicated log and blocks until it is committed and
    // applied (majority replication), or the timeout expires.
    ProposeResult propose(char op, const std::string& key, const std::string& val,
                          int timeout_ms);

    // ReadIndex (Raft thesis �6.4): returns OK only once this node has proven
    // it is still the leader *after* the call began - a heartbeat round sent
    // after the call started was acknowledged by a majority, and an entry
    // from its own term is committed. Reading the state machine after OK is
    // linearizable. Without this, a leader cut off by a partition keeps
    // answering GETs from stale local state while the majority side elects
    // a new leader and accepts newer writes.
    enum class ReadStatus { OK, NOT_LEADER, TIMEOUT };
    ReadStatus read_barrier(int timeout_ms);

    // Writes a Raft snapshot of the applied state now and compacts the log
    // prefix it covers. Also runs automatically once the log holds
    // `snapshot_threshold` applied entries beyond the last snapshot.
    bool snapshot_now();
    void set_snapshot_threshold(uint64_t entries) { snapshot_threshold = entries; }

    // Applied state machine contents plus the log index they reflect, read
    // atomically with respect to apply (used by the DIGEST convergence check).
    std::pair<uint64_t, std::vector<std::pair<std::string, std::string>>> applied_state();

private:
    // Result of handling an inbound RPC, independent of wire format — the
    // gRPC service glue (RaftServiceImpl) translates these to/from protobuf.
    struct RequestVoteResult { uint64_t term; bool vote_granted; };
    struct AppendEntriesResult { uint64_t term; bool success; uint64_t match_index; };
    struct InstallSnapshotResult { uint64_t term; uint64_t last_included_index; };

    // Generated gRPC service implementation; defined in Raft.cpp. Has access
    // to RaftNode's private handlers via nested-class member access (C++11).
    class RaftServiceImpl;

    // --- persistent state (survives restarts) ---
    uint64_t current_term = 0;
    int voted_for = -1; // -1 = none this term
    // In-memory mirror of the v2 WAL's entries *after* the snapshot:
    // log_[0] has index snap_index + 1. Use entry_at_locked()/term_at_locked()
    // rather than indexing directly.
    std::vector<WalRecord> log_;
    uint64_t snap_index = 0; // last log index covered by the snapshot
    uint64_t snap_term = 0;  // term of that entry

    // --- volatile state ---
    RaftRole role = RaftRole::Follower;
    int leader_id = -1;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;

    // --- leader-only replication state (indexed like `peers`) ---
    std::vector<uint64_t> next_index;
    std::vector<uint64_t> match_index;
    std::vector<char> peer_busy; // one in-flight AppendEntries per peer
    // Send time of the newest AppendEntries/InstallSnapshot each peer
    // acknowledged in the current term. Drives both CheckQuorum (a leader
    // that no majority has acked recently steps down) and ReadIndex (a read
    // may proceed once a majority acked a round sent after it started).
    std::vector<std::chrono::steady_clock::time_point> peer_ack_sent_at;
    int pending_reads = 0;
    std::chrono::steady_clock::time_point newest_read_start;

    // Follower-side: when we last heard from a valid leader. PreVote requests
    // are refused while this is recent, so a rejoining node cannot depose a
    // leader that the rest of the cluster can still hear.
    std::chrono::steady_clock::time_point last_leader_contact;
    std::atomic<uint64_t> prevote_round{0};

    uint64_t snapshot_threshold = 1000;
    std::atomic<bool> snapshot_running{false};
    // Serializes snapshot file writes (local compaction vs. an incoming
    // InstallSnapshot). Lock order: snapshot_mtx before mtx, never reverse.
    std::mutex snapshot_mtx;

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
    // Term of the entry at idx: snap_term at snap_index, 0 when idx is 0,
    // compacted away (< snap_index) or beyond the end of the log.
    uint64_t term_at_locked(uint64_t idx) const;
    const WalRecord& entry_at_locked(uint64_t idx) const { return log_[idx - snap_index - 1]; }
    int majority() const { return static_cast<int>(peers.size() + 1) / 2 + 1; }
    void restore_state_machine_locked();
    void step_down_locked(const char* reason);
    void check_quorum_locked(std::chrono::steady_clock::time_point now);
    void maybe_start_snapshot_locked();
    bool take_snapshot();

    void ticker_loop();
    void start_pre_vote();
    void start_election();

    void broadcast_append_entries();
    void broadcast_append_entries_locked();
    void send_append_entries_to_locked(size_t peer_idx);
    void send_install_snapshot_locked(size_t peer_idx);

    void advance_commit_locked();
    void apply_committed_locked();

    // Core Raft handlers, unchanged in logic from the hand-rolled-socket
    // version — only the surrounding transport (how requests/results cross
    // the wire) changed. RaftServiceImpl converts protobuf <-> these params.
    RequestVoteResult handle_request_vote(uint64_t term, int candidate_id,
                                          uint64_t last_log_index, uint64_t last_log_term,
                                          bool pre_vote);
    AppendEntriesResult handle_append_entries(uint64_t term, int leader,
                                              uint64_t prev_log_index, uint64_t prev_log_term,
                                              uint64_t leader_commit,
                                              std::vector<WalRecord> entries);
    InstallSnapshotResult handle_install_snapshot(
        uint64_t term, int leader, uint64_t last_index, uint64_t last_term,
        std::vector<std::pair<std::string, std::string>> pairs);
};

// Human-readable role name for logs and STATUS output.
const char* raft_role_name(RaftRole role);
