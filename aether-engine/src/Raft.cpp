#include "Raft.hpp"
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <memory>
#include <algorithm>
#include <limits>
#include <unistd.h> // fsync (persist_meta_locked)

// Tunables. Election timeout is deliberately demo-friendly (visible in logs);
// heartbeats must be several times shorter than the minimum election timeout.
static constexpr int ELECTION_TIMEOUT_MIN_MS = 800;
static constexpr int ELECTION_TIMEOUT_MAX_MS = 1600;
static constexpr int HEARTBEAT_INTERVAL_MS = 250;
static constexpr int TICK_MS = 25;
static constexpr int RPC_TIMEOUT_MS = 300;
static constexpr int INSTALL_SNAPSHOT_TIMEOUT_MS = 5000;
// CheckQuorum: a leader steps down if no majority has acknowledged anything
// it sent within this window. Same scale as the election timeout, so by the
// time the majority side can have elected someone else, we've stopped
// believing we lead.
static constexpr int CHECK_QUORUM_WINDOW_MS = ELECTION_TIMEOUT_MAX_MS;
// Catch-up batches are capped so one RPC never carries the whole log.
static constexpr size_t MAX_ENTRIES_PER_APPEND = 256;
static constexpr int MAX_SNAPSHOT_MESSAGE_BYTES = 256 * 1024 * 1024;

const char* raft_role_name(RaftRole role) {
    switch (role) {
        case RaftRole::Follower: return "FOLLOWER";
        case RaftRole::Candidate: return "CANDIDATE";
        case RaftRole::Leader: return "LEADER";
    }
    return "UNKNOWN";
}

// gRPC service glue: converts protobuf requests/responses to/from the plain
// C++ params RaftNode's handlers already took under the hand-rolled socket
// protocol. No Raft logic lives here — it all stays in RaftNode's handlers.
class RaftNode::RaftServiceImpl final : public aether::raft::RaftPeer::Service {
public:
    explicit RaftServiceImpl(RaftNode* node) : node_(node) {}

    grpc::Status RequestVote(grpc::ServerContext*,
                             const aether::raft::RequestVoteRequest* request,
                             aether::raft::RequestVoteResponse* response) override {
        auto result = node_->handle_request_vote(request->term(), request->candidate_id(),
                                                  request->last_log_index(), request->last_log_term(),
                                                  request->pre_vote());
        response->set_term(result.term);
        response->set_vote_granted(result.vote_granted);
        return grpc::Status::OK;
    }

    grpc::Status AppendEntries(grpc::ServerContext*,
                              const aether::raft::AppendEntriesRequest* request,
                              aether::raft::AppendEntriesResponse* response) override {
        std::vector<WalRecord> entries;
        entries.reserve(request->entries_size());
        for (const auto& e : request->entries()) {
            char op = e.op().empty() ? 'S' : e.op()[0];
            entries.push_back(WalRecord{e.term(), e.index(), op, e.key(), e.val()});
        }
        auto result = node_->handle_append_entries(request->term(), request->leader_id(),
                                                    request->prev_log_index(), request->prev_log_term(),
                                                    request->leader_commit(), std::move(entries));
        response->set_term(result.term);
        response->set_success(result.success);
        response->set_match_index(result.match_index);
        return grpc::Status::OK;
    }

    grpc::Status InstallSnapshot(grpc::ServerContext*,
                                 const aether::raft::InstallSnapshotRequest* request,
                                 aether::raft::InstallSnapshotResponse* response) override {
        std::vector<std::pair<std::string, std::string>> pairs;
        pairs.reserve(request->data_size());
        for (const auto& kv : request->data()) {
            pairs.emplace_back(kv.key(), kv.val());
        }
        auto result = node_->handle_install_snapshot(request->term(), request->leader_id(),
                                                      request->last_included_index(),
                                                      request->last_included_term(), std::move(pairs));
        response->set_term(result.term);
        response->set_last_included_index(result.last_included_index);
        return grpc::Status::OK;
    }

private:
    RaftNode* node_;
};

// ---------- RaftNode ----------

RaftNode::RaftNode(int node_id, std::vector<RaftPeer> peers, int listen_port,
                   Engine& engine, const std::string& meta_path)
    : node_id(node_id), peers(std::move(peers)), listen_port(listen_port),
      engine(engine), meta_path(meta_path),
      rng(std::random_device{}() ^ static_cast<unsigned>(node_id * 7919)) {}

RaftNode::~RaftNode() {
    stop();
}

bool RaftNode::start() {
    load_persistent_state();

    if (!engine.get_wal_manager().open_wal_v2()) {
        std::cerr << "[Raft] Cannot open v2 WAL; refusing to start." << std::endl;
        return false;
    }
    engine.get_wal_manager().start_group_commit_flusher(5, 64);

    // Persistent per-peer channel + stub, created once and reused for the
    // node's lifetime (unlike the old per-RPC tcp_connect). CreateChannel is
    // lazy — it doesn't actually connect until the first RPC.
    //
    // Reconnect backoff and keepalive are tightened from gRPC's defaults
    // (backoff grows to 120s; no keepalive). Under a network partition that
    // silently drops packets, the defaults left a healed link idle for many
    // seconds after the partition ended.
    grpc::ChannelArguments channel_args;
    channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
    channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 1000);
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 1000);
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    channel_args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    peer_channels_.reserve(peers.size());
    peer_stubs_.reserve(peers.size());
    for (const auto& peer : peers) {
        std::string target = peer.host + ":" + std::to_string(peer.raft_port);
        auto channel = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), channel_args);
        peer_channels_.push_back(channel);
        peer_stubs_.push_back(aether::raft::RaftPeer::NewStub(channel));
    }

    service_impl_ = std::make_unique<RaftServiceImpl>(this);
    std::string server_addr = "0.0.0.0:" + std::to_string(listen_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl_.get());
    builder.SetMaxReceiveMessageSize(MAX_SNAPSHOT_MESSAGE_BYTES); // InstallSnapshot
    // Accept the peers' keepalive pings (otherwise the server answers them
    // with GOAWAY "too_many_pings" and drops the connection).
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 500);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
        std::cerr << "[Raft] Failed to start gRPC server on " << server_addr << std::endl;
        return false;
    }

    running = true;
    {
        std::lock_guard<std::mutex> lock(mtx);
        reset_election_timer_locked();
    }
    grpc_server_thread_ = std::thread([this]() { grpc_server_->Wait(); });
    ticker_thread = std::thread(&RaftNode::ticker_loop, this);

    std::cout << "[Raft] Node " << node_id << " started on port " << listen_port
              << " (term " << current_term << ", snapshot index " << snap_index
              << ", log entries " << log_.size() << ")" << std::endl;
    return true;
}

void RaftNode::stop() {
    if (!running.exchange(false)) return;
    if (grpc_server_) {
        grpc_server_->Shutdown(); // drains in-flight inbound RPCs, then Wait() returns
    }
    if (grpc_server_thread_.joinable()) grpc_server_thread_.join();
    if (ticker_thread.joinable()) ticker_thread.join();
    {
        // Wake any propose()/read_barrier() still waiting so they return.
        std::lock_guard<std::mutex> lock(mtx);
        commit_cv.notify_all();
    }
    // Wait for detached outbound-RPC sender (and snapshot) threads so they
    // never touch a dead `this`.
    while (inflight_rpcs.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    // Safe only here: by this point the gRPC server has fully drained (no
    // propose() call can still be blocked in wait_for_durable), so the
    // flusher's final drain is the last WAL activity before shutdown.
    engine.get_wal_manager().stop_group_commit_flusher();
}

RaftNode::Status RaftNode::get_status() {
    std::lock_guard<std::mutex> lock(mtx);
    return Status{role, current_term, commit_index, last_log_index_locked(), leader_id,
                  last_applied, snap_index, log_.size()};
}

std::pair<uint64_t, std::vector<std::pair<std::string, std::string>>> RaftNode::applied_state() {
    // Entries are applied under mtx, so holding it here gives a state that
    // corresponds exactly to last_applied.
    std::lock_guard<std::mutex> lock(mtx);
    return {last_applied, engine.get_lock_manager().get_all_pairs()};
}

// ---------- persistence ----------

void RaftNode::load_persistent_state() {
    FILE* f = std::fopen(meta_path.c_str(), "rb");
    if (f) {
        unsigned long long term = 0;
        int voted = -1;
        if (std::fscanf(f, "%llu %d", &term, &voted) == 2) {
            current_term = term;
            voted_for = voted;
        }
        std::fclose(f);
    }

    auto& wal = engine.get_wal_manager();
    std::vector<std::pair<std::string, std::string>> pairs;
    bool untagged = false;
    if (wal.read_raft_snapshot(pairs, snap_index, snap_term, &untagged)) {
        for (const auto& [k, v] : pairs) {
            engine.get_lock_manager().set(k, v);
        }
        std::cout << "Loaded " << pairs.size() << " keys from snapshot (raft index "
                  << snap_index << ", term " << snap_term << ")." << std::endl;
    } else if (untagged) {
        // A pre-compaction SNAPSHOT_EXPORT file: the WAL still holds the
        // complete log, so the log alone rebuilds the state.
        std::cout << "[Raft] Ignoring untagged snapshot file; the WAL holds the full log." << std::endl;
    }

    // The WAL may still contain entries the snapshot covers (a crash between
    // writing the snapshot and compacting the WAL) - skip those.
    uint64_t expected = snap_index + 1;
    for (auto& rec : wal.read_all_records()) {
        if (rec.index <= snap_index) continue;
        if (rec.index != expected) {
            std::cerr << "[Raft] WAL gap: expected index " << expected << ", found "
                      << rec.index << "; discarding the rest of the log." << std::endl;
            wal.truncate_from(expected);
            break;
        }
        log_.push_back(std::move(rec));
        expected++;
    }

    // Only the snapshot is known to be committed. Entries after it may be
    // uncommitted leftovers (e.g. writes a partitioned leader accepted but
    // never replicated), so they must NOT be applied until a leader says
    // they're committed. The pre-snapshot code set commit_index to the end
    // of the log here and applied everything - which could expose writes
    // that the cluster later discards.
    commit_index = snap_index;
    last_applied = snap_index;
}

void RaftNode::restore_state_machine_locked() {
    auto& lm = engine.get_lock_manager();
    lm.clear();
    std::vector<std::pair<std::string, std::string>> pairs;
    uint64_t idx = 0, term = 0;
    if (engine.get_wal_manager().read_raft_snapshot(pairs, idx, term)) {
        for (const auto& [k, v] : pairs) lm.set(k, v);
    }
    uint64_t end = std::min(commit_index, last_log_index_locked());
    for (uint64_t i = snap_index + 1; i <= end; ++i) {
        const auto& rec = entry_at_locked(i);
        if (rec.op == 'S' || rec.op == 'D') {
            engine.apply(rec.op, rec.key, rec.val);
        }
    }
    last_applied = std::max(end, snap_index);
}

void RaftNode::persist_meta_locked() {
    std::string tmp = meta_path + ".tmp";
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f) {
        std::cerr << "[Raft] FATAL: cannot persist meta to " << tmp << std::endl;
        return;
    }
    std::fprintf(f, "%llu %d\n", static_cast<unsigned long long>(current_term), voted_for);
    std::fflush(f);
    fsync(fileno(f));
    std::fclose(f);
    std::rename(tmp.c_str(), meta_path.c_str());
}

// ---------- role transitions ----------

void RaftNode::reset_election_timer_locked() {
    std::uniform_int_distribution<int> dist(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
    election_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(rng));
}

void RaftNode::become_follower_locked(uint64_t term) {
    bool term_changed = (term != current_term);
    current_term = term;
    if (term_changed) voted_for = -1;
    if (role != RaftRole::Follower || term_changed) {
        std::cout << "[Raft] Node " << node_id << " -> FOLLOWER (term "
                  << current_term << ")" << std::endl;
    }
    role = RaftRole::Follower;
    persist_meta_locked();
    reset_election_timer_locked();
    commit_cv.notify_all(); // waiting proposers/readers must see we lost leadership
}

void RaftNode::step_down_locked(const char* reason) {
    std::cout << "[Raft] Node " << node_id << " stepping down from LEADER (term "
              << current_term << "): " << reason << std::endl;
    role = RaftRole::Follower;
    leader_id = -1;
    reset_election_timer_locked();
    commit_cv.notify_all();
}

void RaftNode::become_leader_locked() {
    role = RaftRole::Leader;
    leader_id = node_id;

    // Initialize leader state tracking variables for peers
    next_index.assign(peers.size(), last_log_index_locked() + 1);
    match_index.assign(peers.size(), 0);
    peer_busy.assign(peers.size(), 0);
    // Grace period for CheckQuorum: count every peer as freshly heard from.
    // (ReadIndex is unaffected - it only counts acks of rounds sent after a
    // read began, and every read begins after this.)
    peer_ack_sent_at.assign(peers.size(), std::chrono::steady_clock::now());

    // Raft leader election safety: leader commits a no-op entry in its own term to establish local log consistency
    uint64_t entry_term = current_term;
    uint64_t entry_index = last_log_index_locked() + 1;
    try {
        engine.get_wal_manager().append_entry(entry_term, entry_index, 'N', "", "");
        log_.push_back(WalRecord{entry_term, entry_index, 'N', "", ""});
        std::cout << "[Raft] Leader appended no-op entry at term " << entry_term << ", index " << entry_index << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[Raft] Failed to append no-op entry on leadership transition: " << e.what() << std::endl;
    }

    last_heartbeat_sent = std::chrono::steady_clock::now() -
                          std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS);
    std::cout << "[Raft] Node " << node_id << " became LEADER for term "
              << current_term << std::endl;
}

uint64_t RaftNode::last_log_index_locked() const {
    return log_.empty() ? snap_index : log_.back().index;
}

uint64_t RaftNode::last_log_term_locked() const {
    return log_.empty() ? snap_term : log_.back().term;
}

uint64_t RaftNode::term_at_locked(uint64_t idx) const {
    if (idx == 0) return 0;
    if (idx == snap_index) return snap_term;
    if (idx < snap_index || idx > last_log_index_locked()) return 0;
    return entry_at_locked(idx).term;
}

std::string RaftNode::get_leader_address() {
    std::lock_guard<std::mutex> lock(mtx);
    if (leader_id == -1) return "unknown";
    if (leader_id == node_id) return "localhost:" + std::to_string(listen_port - 1000);
    for (const auto& peer : peers) {
        if (peer.id == leader_id) {
            return peer.host + ":" + std::to_string(peer.client_port);
        }
    }
    return "unknown";
}

RaftNode::ProposeResult RaftNode::propose(char op, const std::string& key, const std::string& val, int timeout_ms) {
    uint64_t entry_index = 0;
    uint64_t entry_term = 0;
    uint64_t flush_ticket = 0;
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (role != RaftRole::Leader) {
            return {ProposeStatus::NOT_LEADER, leader_id};
        }

        entry_term = current_term;
        entry_index = last_log_index_locked() + 1;

        try {
            // Buffered, not fsynced yet - mtx is released below so concurrent
            // propose() calls can append too, letting the group-commit
            // flusher batch multiple writers' entries into one fsync
            // (Tier 1 write-throughput optimization).
            flush_ticket = engine.get_wal_manager().append_entry_buffered(entry_term, entry_index, op, key, val);
        } catch (const std::exception& e) {
            std::cerr << "[Raft] Failed to write WAL for proposal: " << e.what() << std::endl;
            return {ProposeStatus::TIMEOUT, leader_id};
        }

        log_.push_back(WalRecord{entry_term, entry_index, op, key, val});
    } // mtx released - other propose()/RPC-handling calls can now proceed

    // Don't tell followers about this entry until it's durably on disk here
    // first - same guarantee the old synchronous append_entry() gave.
    engine.get_wal_manager().wait_for_durable(flush_ticket);

    {
        std::lock_guard<std::mutex> lock(mtx);
        broadcast_append_entries_locked(); // no-ops internally if we're no longer leader
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lock(mtx);
    while (commit_index < entry_index && running && role == RaftRole::Leader &&
           current_term == entry_term) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return {ProposeStatus::TIMEOUT, leader_id};
        }
        if (commit_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            if (commit_index < entry_index) {
                return {ProposeStatus::TIMEOUT, leader_id};
            }
        }
    }

    if (!running) {
        return {ProposeStatus::TIMEOUT, leader_id};
    }
    // Committed is only meaningful if the entry at that index is still ours
    // (a new leader may have overwritten it). (Still leading in the same
    // term means it's ours even if already compacted.) Otherwise the entry
    // is in our log but its fate is unknown - a new leader may or may not
    // keep it - so this is TIMEOUT ("outcome unknown"), never NOT_LEADER:
    // NOT_LEADER promises the write was not appended at all, which is what
    // lets a client treat it as a clean rejection.
    bool ours = (role == RaftRole::Leader && current_term == entry_term) ||
                term_at_locked(entry_index) == entry_term;
    if (commit_index < entry_index || !ours) {
        return {ProposeStatus::TIMEOUT, leader_id};
    }

    // Dashboard visibility: snapshot how many nodes (self + peers whose
    // match_index has reached this entry) had actually replicated it by the
    // moment it committed. This is informational only, computed after the
    // commit-wait above already resolved via the real majority-commit path.
    int replicated = 1; // self
    for (size_t i = 0; i < match_index.size(); ++i) {
        if (match_index[i] >= entry_index) replicated++;
    }
    int cluster_size = static_cast<int>(peers.size()) + 1;

    return {ProposeStatus::OK, leader_id, replicated, cluster_size};
}

RaftNode::ReadStatus RaftNode::read_barrier(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lock(mtx);
    if (role != RaftRole::Leader) return ReadStatus::NOT_LEADER;
    const uint64_t term = current_term;

    // 1. A fresh leader doesn't yet know which entries are committed until
    //    its own no-op commits; reading before that could miss writes that
    //    the previous leader acknowledged.
    while (term_at_locked(commit_index) != term) {
        if (!running || role != RaftRole::Leader || current_term != term) return ReadStatus::NOT_LEADER;
        if (commit_cv.wait_until(lock, deadline) == std::cv_status::timeout &&
            term_at_locked(commit_index) != term) {
            return ReadStatus::TIMEOUT;
        }
    }
    const uint64_t read_index = commit_index;

    // 2. Prove we are still the leader: a majority must acknowledge a round
    //    of AppendEntries sent *after* this read began. An isolated leader
    //    can never collect that, so it times out instead of serving stale data.
    const auto started = std::chrono::steady_clock::now();
    pending_reads++;
    if (started > newest_read_start) newest_read_start = started;
    broadcast_append_entries_locked(); // peers with an RPC in flight re-send when it returns

    ReadStatus result = ReadStatus::OK;
    while (true) {
        if (!running || role != RaftRole::Leader || current_term != term) {
            result = ReadStatus::NOT_LEADER;
            break;
        }
        int acks = 1; // self
        for (const auto& t : peer_ack_sent_at) {
            if (t >= started) acks++;
        }
        if (acks >= majority()) break;
        if (commit_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            acks = 1;
            for (const auto& t : peer_ack_sent_at) {
                if (t >= started) acks++;
            }
            if (acks < majority()) result = ReadStatus::TIMEOUT;
            break;
        }
    }
    pending_reads--;

    // 3. The leader applies synchronously on commit, so this holds already;
    //    kept explicit because it is the condition the read relies on.
    if (result == ReadStatus::OK && last_applied < read_index) {
        result = ReadStatus::TIMEOUT;
    }
    return result;
}

// ---------- timers ----------

void RaftNode::ticker_loop() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(TICK_MS));
        bool do_election = false;
        bool do_heartbeat = false;
        {
            std::lock_guard<std::mutex> lock(mtx);
            auto now = std::chrono::steady_clock::now();
            if (role == RaftRole::Leader) {
                check_quorum_locked(now);
            }
            if (role == RaftRole::Leader) {
                if (now - last_heartbeat_sent >=
                    std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS)) {
                    last_heartbeat_sent = now;
                    do_heartbeat = true;
                }
            } else if (now >= election_deadline) {
                do_election = true;
            }
            maybe_start_snapshot_locked();
        }
        if (do_election) start_pre_vote();
        if (do_heartbeat) broadcast_append_entries();
    }
}

void RaftNode::check_quorum_locked(std::chrono::steady_clock::time_point now) {
    int active = 1; // self
    for (const auto& t : peer_ack_sent_at) {
        if (now - t <= std::chrono::milliseconds(CHECK_QUORUM_WINDOW_MS)) active++;
    }
    if (active < majority()) {
        step_down_locked("no majority acknowledged a heartbeat recently (CheckQuorum)");
    }
}

// ---------- elections ----------

void RaftNode::start_pre_vote() {
    uint64_t term, lli, llt, round;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (role == RaftRole::Leader) return;
        reset_election_timer_locked();
        leader_id = -1;
        term = current_term;
        lli = last_log_index_locked();
        llt = last_log_term_locked();
        round = ++prevote_round;
    }

    // Pre-vote for self; a majority of the full cluster lets us run a real election.
    auto votes = std::make_shared<std::atomic<int>>(1);
    const int needed = majority();

    for (size_t i = 0; i < peers.size(); ++i) {
        inflight_rpcs++;
        std::thread([this, i, term, lli, llt, round, votes, needed]() {
            aether::raft::RequestVoteRequest req;
            req.set_term(term + 1);
            req.set_candidate_id(node_id);
            req.set_last_log_index(lli);
            req.set_last_log_term(llt);
            req.set_pre_vote(true);

            aether::raft::RequestVoteResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(RPC_TIMEOUT_MS));
            grpc::Status status = peer_stubs_[i]->RequestVote(&ctx, req, &resp);

            bool proceed = false;
            if (status.ok() && running) {
                std::lock_guard<std::mutex> lock(mtx);
                if (resp.term() > current_term) {
                    become_follower_locked(resp.term());
                } else if (resp.vote_granted() && role != RaftRole::Leader &&
                           current_term == term && prevote_round == round) {
                    // == (not >=): exactly one responder triggers the election.
                    proceed = (++(*votes) == needed);
                }
            }
            if (proceed) start_election();
            inflight_rpcs--;
        }).detach();
    }
}

void RaftNode::start_election() {
    uint64_t term;
    uint64_t lli, llt;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (role == RaftRole::Leader) return;
        role = RaftRole::Candidate;
        current_term++;
        voted_for = node_id;
        leader_id = -1;
        persist_meta_locked();
        reset_election_timer_locked();
        term = current_term;
        lli = last_log_index_locked();
        llt = last_log_term_locked();
        std::cout << "[Raft] Node " << node_id << " starting election for term "
                  << term << std::endl;
    }

    // Vote for self; majority of the full cluster (peers + self) wins.
    auto votes = std::make_shared<std::atomic<int>>(1);
    const int needed = majority();

    for (size_t i = 0; i < peers.size(); ++i) {
        inflight_rpcs++;
        std::thread([this, i, term, lli, llt, votes, needed]() {
            aether::raft::RequestVoteRequest req;
            req.set_term(term);
            req.set_candidate_id(node_id);
            req.set_last_log_index(lli);
            req.set_last_log_term(llt);

            aether::raft::RequestVoteResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(RPC_TIMEOUT_MS));
            grpc::Status status = peer_stubs_[i]->RequestVote(&ctx, req, &resp);

            if (status.ok() && running) {
                bool won = false;
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    if (resp.term() > current_term) {
                        become_follower_locked(resp.term());
                    } else if (resp.vote_granted() && role == RaftRole::Candidate &&
                               current_term == term) {
                        int v = ++(*votes);
                        if (v >= needed) {
                            become_leader_locked();
                            won = true;
                        }
                    }
                }
                if (won) broadcast_append_entries();
            }
            inflight_rpcs--;
        }).detach();
    }
}

RaftNode::RequestVoteResult RaftNode::handle_request_vote(uint64_t term, int candidate_id,
                                          uint64_t candidate_lli, uint64_t candidate_llt,
                                          bool pre_vote) {
    std::lock_guard<std::mutex> lock(mtx);

    // Election restriction (Raft §5.4.1): only vote for candidates whose
    // log is at least as up-to-date as ours.
    uint64_t my_llt = last_log_term_locked();
    uint64_t my_lli = last_log_index_locked();
    bool log_ok = candidate_llt > my_llt ||
                  (candidate_llt == my_llt && candidate_lli >= my_lli);

    if (pre_vote) {
        // Never changes our own state. Refused while we can still hear a
        // leader: that is exactly the case of a node rejoining after a
        // partition, which must not be able to force a new election.
        bool leader_alive = role == RaftRole::Leader ||
            (leader_id != -1 && std::chrono::steady_clock::now() - last_leader_contact <
                                    std::chrono::milliseconds(ELECTION_TIMEOUT_MIN_MS));
        return {current_term, term > current_term && log_ok && !leader_alive};
    }

    if (term > current_term) {
        become_follower_locked(term);
    }

    bool grant = false;
    if (term == current_term && (voted_for == -1 || voted_for == candidate_id) && log_ok) {
        grant = true;
        voted_for = candidate_id;
        persist_meta_locked();
        reset_election_timer_locked();
        std::cout << "[Raft] Node " << node_id << " voted for node "
                  << candidate_id << " in term " << term << std::endl;
    }

    return {current_term, grant};
}

// ---------- log replication & heartbeats ----------

void RaftNode::broadcast_append_entries() {
    std::lock_guard<std::mutex> lock(mtx);
    broadcast_append_entries_locked();
}

void RaftNode::broadcast_append_entries_locked() {
    if (role != RaftRole::Leader) return;
    for (size_t i = 0; i < peers.size(); ++i) {
        send_append_entries_to_locked(i);
    }
}

void RaftNode::send_append_entries_to_locked(size_t peer_idx) {
    if (peer_idx >= peers.size()) return;
    if (role != RaftRole::Leader) return;
    if (peer_busy[peer_idx]) return;

    // The entries this follower needs were compacted into our snapshot.
    if (next_index[peer_idx] <= snap_index) {
        send_install_snapshot_locked(peer_idx);
        return;
    }

    uint64_t term = current_term;
    uint64_t commit = commit_index;
    uint64_t prev_index = next_index[peer_idx] - 1;
    uint64_t prev_term = term_at_locked(prev_index);

    std::vector<WalRecord> entries;
    uint64_t last = last_log_index_locked();
    for (uint64_t idx = next_index[peer_idx];
         idx <= last && entries.size() < MAX_ENTRIES_PER_APPEND; ++idx) {
        entries.push_back(entry_at_locked(idx));
    }

    peer_busy[peer_idx] = 1;
    inflight_rpcs++;
    int peer_id = peers[peer_idx].id;
    auto sent_at = std::chrono::steady_clock::now();

    std::thread([this, peer_idx, peer_id, term, prev_index, prev_term, commit, sent_at,
                 entries = std::move(entries)]() {
        // Chaos-testing hook only: simulates a slow/degraded peer link by
        // delaying the leader's send to one specific configured peer ID.
        // No-op unless both env vars are set; never used in normal operation.
        if (const char* delay_peer = std::getenv("AETHER_CHAOS_DELAY_PEER_ID")) {
            if (std::atoi(delay_peer) == peer_id) {
                if (const char* delay_ms = std::getenv("AETHER_CHAOS_DELAY_MS")) {
                    int ms = std::atoi(delay_ms);
                    if (ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
                    }
                }
            }
        }

        aether::raft::AppendEntriesRequest req;
        req.set_term(term);
        req.set_leader_id(node_id);
        req.set_prev_log_index(prev_index);
        req.set_prev_log_term(prev_term);
        req.set_leader_commit(commit);
        for (const auto& entry : entries) {
            auto* pe = req.add_entries();
            pe->set_term(entry.term);
            pe->set_index(entry.index);
            pe->set_op(std::string(1, entry.op));
            pe->set_key(entry.key);
            pe->set_val(entry.val);
        }

        aether::raft::AppendEntriesResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(RPC_TIMEOUT_MS));
        grpc::Status status = peer_stubs_[peer_idx]->AppendEntries(&ctx, req, &resp);

        // Always clear the busy flag, even on RPC failure — otherwise this
        // peer would never receive entries again.
        std::lock_guard<std::mutex> lock(mtx);
        peer_busy[peer_idx] = 0;
        if (status.ok() && running) {
            if (resp.term() > current_term) {
                become_follower_locked(resp.term());
            } else if (role == RaftRole::Leader && current_term == term) {
                // Any same-term reply proves this peer still follows us.
                if (sent_at > peer_ack_sent_at[peer_idx]) peer_ack_sent_at[peer_idx] = sent_at;
                if (pending_reads > 0) commit_cv.notify_all();

                bool resend = false;
                if (resp.success()) {
                    // resp.match_index is the last index this RPC verified
                    // (prev_index + entries), never anything beyond it.
                    if (resp.match_index() > match_index[peer_idx]) {
                        match_index[peer_idx] = resp.match_index();
                    }
                    next_index[peer_idx] = match_index[peer_idx] + 1;
                    advance_commit_locked();
                    resend = next_index[peer_idx] <= last_log_index_locked(); // more to catch up
                } else {
                    // Back up, jumping straight to the follower's hint when
                    // it is further back (e.g. its log simply ends earlier).
                    uint64_t next = next_index[peer_idx] > 1 ? next_index[peer_idx] - 1 : 1;
                    next = std::min(next, resp.match_index() + 1);
                    next_index[peer_idx] = std::max<uint64_t>(next, match_index[peer_idx] + 1);
                    resend = true;
                }
                // A read waiting on proof of leadership needs a round that
                // started after it did.
                if (pending_reads > 0 && sent_at < newest_read_start) resend = true;
                if (resend) send_append_entries_to_locked(peer_idx);
            }
        }
        inflight_rpcs--;
    }).detach();
}

void RaftNode::send_install_snapshot_locked(size_t peer_idx) {
    uint64_t term = current_term;
    int peer_id = peers[peer_idx].id;
    peer_busy[peer_idx] = 1;
    inflight_rpcs++;
    auto sent_at = std::chrono::steady_clock::now();

    std::thread([this, peer_idx, peer_id, term, sent_at]() {
        // Read the snapshot file rather than copying state under mtx: the
        // file is replaced atomically, and its own header says which index
        // it covers, so whichever version we read is self-consistent.
        std::vector<std::pair<std::string, std::string>> pairs;
        uint64_t idx = 0, sterm = 0;
        bool ok = engine.get_wal_manager().read_raft_snapshot(pairs, idx, sterm);

        grpc::Status status;
        aether::raft::InstallSnapshotResponse resp;
        if (ok) {
            aether::raft::InstallSnapshotRequest req;
            req.set_term(term);
            req.set_leader_id(node_id);
            req.set_last_included_index(idx);
            req.set_last_included_term(sterm);
            for (const auto& [k, v] : pairs) {
                auto* kv = req.add_data();
                kv->set_key(k);
                kv->set_val(v);
            }
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(INSTALL_SNAPSHOT_TIMEOUT_MS));
            status = peer_stubs_[peer_idx]->InstallSnapshot(&ctx, req, &resp);
            std::cout << "[Raft] Node " << node_id << " sent InstallSnapshot to node " << peer_id
                      << " (index " << idx << ", " << pairs.size() << " keys): "
                      << (status.ok() ? "ok" : status.error_message()) << std::endl;
        } else {
            std::cerr << "[Raft] Node " << node_id << " has no snapshot file to send to node "
                      << peer_id << std::endl;
        }

        std::lock_guard<std::mutex> lock(mtx);
        peer_busy[peer_idx] = 0;
        if (ok && status.ok() && running) {
            if (resp.term() > current_term) {
                become_follower_locked(resp.term());
            } else if (role == RaftRole::Leader && current_term == term) {
                if (sent_at > peer_ack_sent_at[peer_idx]) peer_ack_sent_at[peer_idx] = sent_at;
                if (pending_reads > 0) commit_cv.notify_all();
                if (resp.last_included_index() > match_index[peer_idx]) {
                    match_index[peer_idx] = resp.last_included_index();
                }
                next_index[peer_idx] = match_index[peer_idx] + 1;
                advance_commit_locked();
                send_append_entries_to_locked(peer_idx); // continue with the log tail
            }
        }
        inflight_rpcs--;
    }).detach();
}

void RaftNode::advance_commit_locked() {
    if (role != RaftRole::Leader) return;
    uint64_t last = last_log_index_locked();
    uint64_t n = commit_index + 1;
    while (n <= last) {
        // Only entries from the current term are committed by counting
        // replicas (Raft §5.4.2); earlier ones commit along with them.
        if (term_at_locked(n) == current_term) {
            int count = 1; // self
            for (size_t i = 0; i < peers.size(); ++i) {
                if (match_index[i] >= n) {
                    count++;
                }
            }
            if (count >= majority()) {
                commit_index = n;
            } else {
                break;
            }
        }
        n++;
    }
    if (commit_index > last_applied) {
        apply_committed_locked();
    }
}

void RaftNode::apply_committed_locked() {
    uint64_t last = last_log_index_locked();
    while (commit_index > last_applied && last_applied < last) {
        last_applied++;
        const auto& rec = entry_at_locked(last_applied);
        if (rec.op == 'S' || rec.op == 'D') {
            engine.apply(rec.op, rec.key, rec.val);
        }
    }
    commit_cv.notify_all();
}

RaftNode::AppendEntriesResult RaftNode::handle_append_entries(uint64_t term, int leader,
                                            uint64_t prev_log_index, uint64_t prev_log_term,
                                            uint64_t leader_commit,
                                            std::vector<WalRecord> entries) {
    std::lock_guard<std::mutex> lock(mtx);
    if (term < current_term) {
        return {current_term, false, 0};
    }

    if (term > current_term || role != RaftRole::Follower) {
        become_follower_locked(term);
    }
    leader_id = leader;
    last_leader_contact = std::chrono::steady_clock::now();
    reset_election_timer_locked();

    // Entries at or before our snapshot are committed and therefore
    // identical on every node - skip them and check consistency from the
    // snapshot point instead.
    if (prev_log_index < snap_index) {
        size_t skip = static_cast<size_t>(std::min<uint64_t>(entries.size(), snap_index - prev_log_index));
        entries.erase(entries.begin(), entries.begin() + skip);
        prev_log_index = snap_index;
        prev_log_term = snap_term;
    }

    uint64_t my_last_idx = last_log_index_locked();
    if (prev_log_index > my_last_idx) {
        return {current_term, false, my_last_idx};
    }

    if (prev_log_index > 0 && term_at_locked(prev_log_index) != prev_log_term) {
        return {current_term, false, prev_log_index - 1};
    }

    uint64_t next_idx = prev_log_index + 1;
    bool appended_new = false;
    for (size_t i = 0; i < entries.size(); ++i) {
        uint64_t curr_idx = next_idx + i;
        if (curr_idx <= last_log_index_locked()) {
            if (term_at_locked(curr_idx) == entries[i].term) continue; // already have it
            // Conflict: drop this entry and everything after it.
            if (curr_idx <= commit_index) {
                // Impossible if Raft's guarantees hold; loud, not silent.
                std::cerr << "[Raft] SAFETY VIOLATION: leader " << leader
                          << " overwrites committed index " << curr_idx << std::endl;
            }
            if (!engine.get_wal_manager().truncate_from(curr_idx)) {
                std::cerr << "[Raft] FATAL: WAL Log truncation failed at index " << curr_idx << "! Disk and memory are out of sync." << std::endl;
                std::exit(1);
            }
            log_.erase(log_.begin() + (curr_idx - snap_index - 1), log_.end());
            if (commit_index >= curr_idx) {
                commit_index = curr_idx - 1;
                restore_state_machine_locked();
            }
        }
        // Buffered, not fsynced per-entry - flush_now() below batches
        // every new entry from this one AppendEntries RPC into a single
        // fsync (Tier 1 write-throughput optimization). Still fully
        // inside this function's mtx lock, unlike the leader's propose()
        // path - a follower only ever processes one RPC at a time here,
        // so there's no cross-call concurrency to unlock.
        engine.get_wal_manager().append_entry_buffered(
            entries[i].term, entries[i].index, entries[i].op, entries[i].key, entries[i].val
        );
        log_.push_back(entries[i]);
        appended_new = true;
    }
    if (appended_new) {
        engine.get_wal_manager().flush_now();
    }

    // Only what this RPC verified counts: our log may run past it with
    // stale entries from an old term that the leader hasn't overwritten yet
    // (Raft Figure 2: "index of last new entry", not our last index).
    uint64_t last_verified = prev_log_index + entries.size();
    if (leader_commit > commit_index) {
        uint64_t new_commit = std::min(leader_commit, last_verified);
        if (new_commit > commit_index) {
            commit_index = new_commit;
            apply_committed_locked();
        }
    }

    return {current_term, true, last_verified};
}

// ---------- snapshots / log compaction ----------

void RaftNode::maybe_start_snapshot_locked() {
    if (snapshot_threshold == 0 || last_applied - snap_index < snapshot_threshold) return;
    if (snapshot_running.exchange(true)) return;
    inflight_rpcs++; // so stop() waits for it
    std::thread([this]() {
        take_snapshot();
        snapshot_running = false;
        inflight_rpcs--;
    }).detach();
}

bool RaftNode::snapshot_now() {
    return take_snapshot();
}

bool RaftNode::take_snapshot() {
    std::lock_guard<std::mutex> snap_lock(snapshot_mtx);
    uint64_t idx, term;
    std::vector<std::pair<std::string, std::string>> pairs;
    {
        // Copy the applied state in memory under mtx (cheap), then do the
        // slow file write without blocking replication or elections.
        std::lock_guard<std::mutex> lock(mtx);
        if (last_applied <= snap_index) return true; // nothing new to cover
        idx = last_applied;
        term = term_at_locked(idx);
        pairs = engine.get_lock_manager().get_all_pairs();
    }

    auto& wal = engine.get_wal_manager();
    if (!wal.write_raft_snapshot(pairs, idx, term)) {
        std::cerr << "[Raft] Node " << node_id << " failed to write snapshot at index " << idx << std::endl;
        return false;
    }

    size_t remaining;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (idx <= snap_index) return true;
        // Entries <= idx are applied, hence committed, hence never truncated
        // by a conflicting leader - they're all still at the front of log_.
        log_.erase(log_.begin(), log_.begin() + (idx - snap_index));
        snap_index = idx;
        snap_term = term;
        remaining = log_.size();
    }
    // Rewriting the file takes wal_mutex, which serializes it against
    // concurrent appends/truncations; a crash before it completes just
    // leaves a prefix that load_persistent_state() skips.
    wal.compact_prefix(idx);
    std::cout << "[Raft] Node " << node_id << " snapshot at index " << idx << " (term " << term
              << ", " << pairs.size() << " keys); log compacted to " << remaining
              << " entries" << std::endl;
    return true;
}

RaftNode::InstallSnapshotResult RaftNode::handle_install_snapshot(
    uint64_t term, int leader, uint64_t last_index, uint64_t last_term,
    std::vector<std::pair<std::string, std::string>> pairs) {
    std::lock_guard<std::mutex> snap_lock(snapshot_mtx);
    std::lock_guard<std::mutex> lock(mtx);
    if (term < current_term) {
        return {current_term, 0};
    }
    if (term > current_term || role != RaftRole::Follower) {
        become_follower_locked(term);
    }
    leader_id = leader;
    last_leader_contact = std::chrono::steady_clock::now();
    reset_election_timer_locked();

    if (last_index <= commit_index) {
        // We already have everything it covers.
        return {current_term, last_index};
    }

    auto& wal = engine.get_wal_manager();
    if (!wal.write_raft_snapshot(pairs, last_index, last_term)) {
        std::cerr << "[Raft] Node " << node_id << " failed to persist installed snapshot" << std::endl;
        return {current_term, 0};
    }

    // If our log already contains the snapshot's last entry, keep whatever
    // follows it; otherwise the snapshot supersedes the whole log.
    if (last_index <= last_log_index_locked() && term_at_locked(last_index) == last_term) {
        log_.erase(log_.begin(), log_.begin() + (last_index - snap_index));
        wal.compact_prefix(last_index);
    } else {
        log_.clear();
        wal.compact_prefix(std::numeric_limits<uint64_t>::max());
    }

    auto& lm = engine.get_lock_manager();
    lm.clear();
    for (const auto& [k, v] : pairs) lm.set(k, v);
    snap_index = last_index;
    snap_term = last_term;
    commit_index = last_index;
    last_applied = last_index;
    commit_cv.notify_all();

    std::cout << "[Raft] Node " << node_id << " installed snapshot from leader " << leader
              << " (index " << last_index << ", term " << last_term << ", "
              << pairs.size() << " keys)" << std::endl;
    return {current_term, last_index};
}
