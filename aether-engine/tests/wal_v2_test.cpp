// Step-1 verification for the v2 (Raft-log) WAL format:
// append_entry / read_all_records / truncate_from, plus v1 back-compat.
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
#include "Persistence.hpp"

// Always-evaluated check (unlike assert, which NDEBUG compiles out).
#define CHECK(expr)                                                          \
    do {                                                                     \
        if (!(expr)) {                                                       \
            std::cerr << "CHECK failed at " << __FILE__ << ":" << __LINE__  \
                      << ": " #expr << std::endl;                            \
            std::exit(1);                                                    \
        }                                                                    \
    } while (0)

static void cleanup(const std::string& wal, const std::string& snap) {
    std::remove(wal.c_str());
    std::remove(snap.c_str());
}

static void test_v2_roundtrip() {
    const std::string wal = "test_v2.wal", snap = "test_v2.snapshot";
    cleanup(wal, snap);
    {
        WalManager wm(wal, snap);
        CHECK(wm.open_wal_v2());
        wm.append_entry(1, 1, 'S', "alpha", "value-1");
        wm.append_entry(1, 2, 'S', "binary", std::string("has\nnewline\0and-nul", 19));
        wm.append_entry(2, 3, 'S', "empty-val", "");
        wm.append_entry(2, 4, 'D', "alpha", "");
    }
    {
        WalManager wm(wal, snap);
        auto recs = wm.read_all_records();
        CHECK(recs.size() == 4);
        CHECK(recs[0].term == 1 && recs[0].index == 1 && recs[0].op == 'S');
        CHECK(recs[0].key == "alpha" && recs[0].val == "value-1");
        CHECK(recs[1].val == std::string("has\nnewline\0and-nul", 19));
        CHECK(recs[2].term == 2 && recs[2].val.empty());
        CHECK(recs[3].op == 'D' && recs[3].index == 4);
    }
    cleanup(wal, snap);
    std::cout << "test_v2_roundtrip OK" << std::endl;
}

static void test_truncate_from() {
    const std::string wal = "test_trunc.wal", snap = "test_trunc.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);
    CHECK(wm.open_wal_v2());
    for (uint64_t i = 1; i <= 5; ++i) {
        wm.append_entry(1, i, 'S', "k" + std::to_string(i), "v" + std::to_string(i));
    }
    CHECK(wm.truncate_from(3)); // keep indices 1,2
    auto recs = wm.read_all_records();
    CHECK(recs.size() == 2);
    CHECK(recs[0].index == 1 && recs[1].index == 2);

    // The file must still accept appends after truncation.
    wm.append_entry(2, 3, 'S', "k3b", "v3b");
    recs = wm.read_all_records();
    CHECK(recs.size() == 3);
    CHECK(recs[2].term == 2 && recs[2].index == 3 && recs[2].key == "k3b");

    cleanup(wal, snap);
    std::cout << "test_truncate_from OK" << std::endl;
}

static void test_v1_compat() {
    const std::string wal = "test_v1.wal", snap = "test_v1.snapshot";
    cleanup(wal, snap);
    {
        WalManager wm(wal, snap);
        CHECK(wm.open_wal());
        wm.append('S', "a", "1");
        wm.append('S', "b", "2");
        wm.append('D', "a", "");
    }
    {
        // v1 records surface with term 0 and synthetic 1-based indices.
        WalManager wm(wal, snap);
        auto recs = wm.read_all_records();
        CHECK(recs.size() == 3);
        CHECK(recs[0].term == 0 && recs[0].index == 1);
        CHECK(recs[2].op == 'D' && recs[2].index == 3);

        // Standalone replay path is unchanged.
        LockManager lm;
        CHECK(wm.replayInto(lm) == 3);
        CHECK(!lm.get("a").has_value());
        CHECK(lm.get("b").value() == "2");

        // A v1 file must be rejected by the v2 opener and truncate.
        CHECK(!wm.open_wal_v2());
        CHECK(!wm.truncate_from(1));
    }
    cleanup(wal, snap);
    std::cout << "test_v1_compat OK" << std::endl;
}

static void test_v2_replay() {
    const std::string wal = "test_v2r.wal", snap = "test_v2r.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);
    CHECK(wm.open_wal_v2());
    wm.append_entry(1, 1, 'S', "x", "10");
    wm.append_entry(1, 2, 'S', "y", "20");
    wm.append_entry(1, 3, 'D', "x", "");

    LockManager lm;
    CHECK(wm.replayInto(lm) == 3);
    CHECK(!lm.get("x").has_value());
    CHECK(lm.get("y").value() == "20");

    cleanup(wal, snap);
    std::cout << "test_v2_replay OK" << std::endl;
}

// Isolated test of the group-commit primitives (Tier 1 write-throughput
// optimization), before trusting the full Raft integration test to catch a
// bug in them: many threads append concurrently and each waits on its own
// ticket; every entry must land on disk correctly with none lost, and
// flush_now() must work as a synchronous, non-flusher-thread alternative.
static void test_group_commit_concurrent() {
    const std::string wal = "test_gc.wal", snap = "test_gc.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);
    CHECK(wm.open_wal_v2());
    CHECK(wm.start_group_commit_flusher(5, 64));

    constexpr int NUM_THREADS = 20;
    constexpr int WRITES_PER_THREAD = 25;
    std::vector<std::thread> threads;
    std::atomic<int> completed{0};

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < WRITES_PER_THREAD; ++i) {
                uint64_t index = static_cast<uint64_t>(t) * 1000 + i + 1; // disjoint per thread
                std::string key = "gc_t" + std::to_string(t) + "_" + std::to_string(i);
                std::string val = "val_" + std::to_string(t) + "_" + std::to_string(i);
                uint64_t ticket = wm.append_entry_buffered(1, index, 'S', key, val);
                wm.wait_for_durable(ticket); // must return - this is the property under test
            }
            completed++;
        });
    }
    for (auto& th : threads) th.join();
    CHECK(completed.load() == NUM_THREADS);

    wm.stop_group_commit_flusher();

    auto recs = wm.read_all_records();
    CHECK(recs.size() == static_cast<size_t>(NUM_THREADS * WRITES_PER_THREAD));
    for (int t = 0; t < NUM_THREADS; ++t) {
        for (int i = 0; i < WRITES_PER_THREAD; ++i) {
            std::string want_key = "gc_t" + std::to_string(t) + "_" + std::to_string(i);
            std::string want_val = "val_" + std::to_string(t) + "_" + std::to_string(i);
            auto it = std::find_if(recs.begin(), recs.end(), [&](const WalRecord& r) {
                return r.key == want_key;
            });
            CHECK(it != recs.end());
            CHECK(it->val == want_val);
        }
    }

    cleanup(wal, snap);
    std::cout << "test_group_commit_concurrent OK (" << recs.size() << " entries, "
              << NUM_THREADS << " concurrent writers, zero loss)" << std::endl;
}

static void test_group_commit_flush_now() {
    const std::string wal = "test_gcf.wal", snap = "test_gcf.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);
    CHECK(wm.open_wal_v2());
    // No flusher thread started - flush_now() must work standalone, as used
    // by the follower's handle_append_entries (batches one RPC's entries
    // into one synchronous fsync, no background thread involved).
    wm.append_entry_buffered(1, 1, 'S', "f1", "v1");
    wm.append_entry_buffered(1, 2, 'S', "f2", "v2");
    wm.append_entry_buffered(1, 3, 'D', "f1", "");
    wm.flush_now();

    auto recs = wm.read_all_records();
    CHECK(recs.size() == 3);
    CHECK(recs[0].key == "f1" && recs[0].val == "v1");
    CHECK(recs[2].op == 'D');

    cleanup(wal, snap);
    std::cout << "test_group_commit_flush_now OK" << std::endl;
}

// Log compaction: compact_prefix drops the covered prefix, keeps the tail
// appendable, and composes with truncate_from.
static void test_compact_prefix() {
    const std::string wal = "test_compact.wal", snap = "test_compact.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);
    CHECK(wm.open_wal_v2());
    for (uint64_t i = 1; i <= 10; ++i) {
        wm.append_entry(1, i, 'S', "k" + std::to_string(i), "v" + std::to_string(i));
    }
    CHECK(wm.compact_prefix(6)); // keep 7..10
    auto recs = wm.read_all_records();
    CHECK(recs.size() == 4);
    CHECK(recs.front().index == 7 && recs.back().index == 10);

    wm.append_entry(2, 11, 'S', "k11", "v11"); // still appendable after the rewrite
    CHECK(wm.truncate_from(9));                // keep 7,8
    recs = wm.read_all_records();
    CHECK(recs.size() == 2 && recs[0].index == 7 && recs[1].index == 8);

    CHECK(wm.compact_prefix(UINT64_MAX)); // empty log, header intact
    CHECK(wm.read_all_records().empty());
    wm.append_entry(3, 12, 'N', "", "");
    recs = wm.read_all_records();
    CHECK(recs.size() == 1 && recs[0].index == 12 && recs[0].op == 'N');
    cleanup(wal, snap);
    std::cout << "test_compact_prefix OK" << std::endl;
}

// Raft snapshots carry their log position; untagged (standalone) snapshots
// are reported as such, and load_snapshot() accepts both formats.
static void test_raft_snapshot() {
    const std::string wal = "test_rsnap.wal", snap = "test_rsnap.snapshot";
    cleanup(wal, snap);
    WalManager wm(wal, snap);

    std::vector<std::pair<std::string, std::string>> pairs = {
        {"a", "1"}, {"bin", std::string("x\ny\0z", 5)}, {"empty", ""}};
    CHECK(wm.write_raft_snapshot(pairs, 4242, 7));

    std::vector<std::pair<std::string, std::string>> got;
    uint64_t idx = 0, term = 0;
    bool untagged = true;
    CHECK(wm.read_raft_snapshot(got, idx, term, &untagged));
    CHECK(!untagged && idx == 4242 && term == 7);
    std::sort(got.begin(), got.end());
    std::sort(pairs.begin(), pairs.end());
    CHECK(got == pairs);

    LockManager lm(4);
    CHECK(wm.load_snapshot(lm)); // standalone loader skips the header
    CHECK(lm.get("bin").value() == std::string("x\ny\0z", 5));
    CHECK(lm.get("empty").value().empty());

    // An untagged snapshot is not mistaken for a Raft one.
    CHECK(wm.write_snapshot_only(lm));
    got.clear();
    CHECK(!wm.read_raft_snapshot(got, idx, term, &untagged));
    CHECK(untagged);
    cleanup(wal, snap);
    std::cout << "test_raft_snapshot OK" << std::endl;
}

int main() {
    test_compact_prefix();
    test_raft_snapshot();
    test_v2_roundtrip();
    test_truncate_from();
    test_v1_compat();
    test_v2_replay();
    test_group_commit_concurrent();
    test_group_commit_flush_now();
    std::cout << "All WAL v2 tests passed." << std::endl;
    return 0;
}
