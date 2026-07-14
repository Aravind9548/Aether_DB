#include "Raft.hpp"
#include <cstdio>
#include <sstream>
#include <iostream>
#include <memory>
#include <unistd.h> // fsync (persist_meta_locked)

// Tunables. Election timeout is deliberately demo-friendly (visible in logs);
// heartbeats must be several times shorter than the minimum election timeout.
static constexpr int ELECTION_TIMEOUT_MIN_MS = 800;
static constexpr int ELECTION_TIMEOUT_MAX_MS = 1600;
static constexpr int HEARTBEAT_INTERVAL_MS = 250;
static constexpr int TICK_MS = 25;
static constexpr int RPC_TIMEOUT_MS = 300;

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
                                                  request->last_log_index(), request->last_log_term());
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

    // Persistent per-peer channel + stub, created once and reused for the
    // node's lifetime (unlike the old per-RPC tcp_connect). CreateChannel is
    // lazy — it doesn't actually connect until the first RPC.
    peer_channels_.reserve(peers.size());
    peer_stubs_.reserve(peers.size());
    for (const auto& peer : peers) {
        std::string target = peer.host + ":" + std::to_string(peer.raft_port);
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        peer_channels_.push_back(channel);
        peer_stubs_.push_back(aether::raft::RaftPeer::NewStub(channel));
    }

    service_impl_ = std::make_unique<RaftServiceImpl>(this);
    std::string server_addr = "0.0.0.0:" + std::to_string(listen_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl_.get());
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
              << " (term " << current_term << ", log size " << log_.size() << ")"
              << std::endl;
    return true;
}

void RaftNode::stop() {
    if (!running.exchange(false)) return;
    if (grpc_server_) {
        grpc_server_->Shutdown(); // drains in-flight inbound RPCs, then Wait() returns
    }
    if (grpc_server_thread_.joinable()) grpc_server_thread_.join();
    if (ticker_thread.joinable()) ticker_thread.join();
    // Wait for detached outbound-RPC sender threads so they never touch a dead `this`.
    while (inflight_rpcs.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

RaftNode::Status RaftNode::get_status() {
    std::lock_guard<std::mutex> lock(mtx);
    return Status{role, current_term, commit_index, last_log_index_locked(), leader_id};
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
    
    // Load snapshot first if it exists
    engine.get_wal_manager().load_snapshot(engine.get_lock_manager());
    
    // Load WAL records
    log_ = engine.get_wal_manager().read_all_records();
    
    // Replay WAL into engine state machine
    for (const auto& rec : log_) {
        if (rec.op == 'S' || rec.op == 'D') {
            engine.apply(rec.op, rec.key, rec.val);
        }
    }
    
    commit_index = log_.size();
    last_applied = log_.size();
}

void RaftNode::restore_state_machine_locked() {
    engine.get_lock_manager().clear();
    engine.get_wal_manager().load_snapshot(engine.get_lock_manager());
    uint64_t end = std::min(commit_index, (uint64_t)log_.size());
    for (uint64_t i = 1; i <= end; ++i) {
        const auto& rec = log_[i - 1];
        if (rec.op == 'S' || rec.op == 'D') {
            engine.apply(rec.op, rec.key, rec.val);
        }
    }
    last_applied = end;
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
}

void RaftNode::become_leader_locked() {
    role = RaftRole::Leader;
    leader_id = node_id;
    
    // Initialize leader state tracking variables for peers
    next_index.assign(peers.size(), last_log_index_locked() + 1);
    match_index.assign(peers.size(), 0);
    peer_busy.assign(peers.size(), 0);
    
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
    return log_.empty() ? 0 : log_.back().index;
}

uint64_t RaftNode::last_log_term_locked() const {
    return log_.empty() ? 0 : log_.back().term;
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
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (role != RaftRole::Leader) {
            return {ProposeStatus::NOT_LEADER, leader_id};
        }

        uint64_t entry_term = current_term;
        entry_index = last_log_index_locked() + 1;

        try {
            engine.get_wal_manager().append_entry(entry_term, entry_index, op, key, val);
        } catch (const std::exception& e) {
            std::cerr << "[Raft] Failed to write WAL for proposal: " << e.what() << std::endl;
            return {ProposeStatus::TIMEOUT, leader_id};
        }

        log_.push_back(WalRecord{entry_term, entry_index, op, key, val});
        broadcast_append_entries_locked();
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lock(mtx);
    while (commit_index < entry_index && running && role == RaftRole::Leader) {
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
    if (role != RaftRole::Leader) {
        return {ProposeStatus::NOT_LEADER, leader_id};
    }
    return {ProposeStatus::OK, leader_id};
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
                if (now - last_heartbeat_sent >=
                    std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS)) {
                    last_heartbeat_sent = now;
                    do_heartbeat = true;
                }
            } else if (now >= election_deadline) {
                do_election = true;
            }
        }
        if (do_election) start_election();
        if (do_heartbeat) broadcast_append_entries();
    }
}

// ---------- elections ----------

void RaftNode::start_election() {
    uint64_t term;
    uint64_t lli, llt;
    {
        std::lock_guard<std::mutex> lock(mtx);
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
    const int majority = static_cast<int>(peers.size() + 1) / 2 + 1;

    for (size_t i = 0; i < peers.size(); ++i) {
        inflight_rpcs++;
        std::thread([this, i, term, lli, llt, votes, majority]() {
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
                        if (v >= majority) {
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
                                          uint64_t candidate_lli, uint64_t candidate_llt) {
    std::lock_guard<std::mutex> lock(mtx);
    if (term > current_term) {
        become_follower_locked(term);
    }

    bool grant = false;
    if (term == current_term && (voted_for == -1 || voted_for == candidate_id)) {
        // Election restriction (Raft §5.4.1): only vote for candidates whose
        // log is at least as up-to-date as ours.
        uint64_t my_llt = last_log_term_locked();
        uint64_t my_lli = last_log_index_locked();
        if (candidate_llt > my_llt ||
            (candidate_llt == my_llt && candidate_lli >= my_lli)) {
            grant = true;
            voted_for = candidate_id;
            persist_meta_locked();
            reset_election_timer_locked();
            std::cout << "[Raft] Node " << node_id << " voted for node "
                      << candidate_id << " in term " << term << std::endl;
        }
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
    if (peer_busy[peer_idx]) return;

    uint64_t term = current_term;
    uint64_t commit = commit_index;
    uint64_t prev_index = next_index[peer_idx] - 1;
    uint64_t prev_term = 0;
    if (prev_index > 0 && prev_index <= log_.size()) {
        prev_term = log_[prev_index - 1].term;
    }

    std::vector<WalRecord> entries;
    for (uint64_t idx = next_index[peer_idx]; idx <= log_.size(); ++idx) {
        entries.push_back(log_[idx - 1]);
    }

    peer_busy[peer_idx] = 1;
    inflight_rpcs++;

    std::thread([this, peer_idx, term, prev_index, prev_term, commit, entries = std::move(entries)]() {
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
                if (resp.success()) {
                    next_index[peer_idx] = resp.match_index() + 1;
                    match_index[peer_idx] = resp.match_index();
                    advance_commit_locked();
                } else {
                    if (next_index[peer_idx] > 1) {
                        next_index[peer_idx]--;
                    }
                    // Retry sending immediately
                    send_append_entries_to_locked(peer_idx);
                }
            }
        }
        inflight_rpcs--;
    }).detach();
}

void RaftNode::advance_commit_locked() {
    if (role != RaftRole::Leader) return;
    uint64_t n = commit_index + 1;
    while (n <= log_.size()) {
        if (log_[n - 1].term == current_term) {
            int count = 1; // self
            for (size_t i = 0; i < peers.size(); ++i) {
                if (match_index[i] >= n) {
                    count++;
                }
            }
            int majority = static_cast<int>(peers.size() + 1) / 2 + 1;
            if (count >= majority) {
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
    while (commit_index > last_applied && last_applied < log_.size()) {
        last_applied++;
        const auto& rec = log_[last_applied - 1];
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
    reset_election_timer_locked();

    uint64_t my_last_idx = last_log_index_locked();
    if (prev_log_index > my_last_idx) {
        return {current_term, false, my_last_idx};
    }

    if (prev_log_index > 0 && log_[prev_log_index - 1].term != prev_log_term) {
        return {current_term, false, prev_log_index - 1};
    }

    uint64_t next_idx = prev_log_index + 1;
    for (size_t i = 0; i < entries.size(); ++i) {
        uint64_t curr_idx = next_idx + i;
        if (curr_idx <= log_.size()) {
            if (log_[curr_idx - 1].term != entries[i].term) {
                if (engine.get_wal_manager().truncate_from(curr_idx)) {
                    log_.erase(log_.begin() + (curr_idx - 1), log_.end());
                    if (commit_index >= curr_idx) {
                        commit_index = curr_idx - 1;
                    }
                    restore_state_machine_locked();
                } else {
                    std::cerr << "[Raft] FATAL: WAL Log truncation failed at index " << curr_idx << "! Disk and memory are out of sync." << std::endl;
                    std::exit(1);
                }
            }
        }
        if (curr_idx > log_.size()) {
            engine.get_wal_manager().append_entry(
                entries[i].term, entries[i].index, entries[i].op, entries[i].key, entries[i].val
            );
            log_.push_back(entries[i]);
        }
    }

    if (leader_commit > commit_index) {
        commit_index = std::min(leader_commit, last_log_index_locked());
        apply_committed_locked();
    }

    return {current_term, true, last_log_index_locked()};
}
