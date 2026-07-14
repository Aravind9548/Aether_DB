// Step-1 verification for the v2 (Raft-log) WAL format:
// append_entry / read_all_records / truncate_from, plus v1 back-compat.
#include <cstdlib>
#include <cstdio>
#include <iostream>
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

int main() {
    test_v2_roundtrip();
    test_truncate_from();
    test_v1_compat();
    test_v2_replay();
    std::cout << "All WAL v2 tests passed." << std::endl;
    return 0;
}
