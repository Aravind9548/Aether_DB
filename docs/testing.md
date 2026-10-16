# Testing

AetherDB is tested at four levels: unit tests for the write-ahead log,
crash tests for Raft, network-partition tests with a linearizability
checker, and an end-to-end failover demo through the gateway.

## Test inventory

| Test | What it proves | Result (clean pass, 4 Oct 2026) |
|---|---|---|
| `wal_v2_test` | WAL format, truncation, compaction, Raft snapshots, group commit (500 writes, 20 threads, zero loss) | 8/8 |
| `raft_safety_test.py` | Leader killed mid-load (sequential, and 8 concurrent writers): zero acknowledged-write loss; follower catch-up | All phases pass |
| `chaos_suite.py` | Follower killed mid-load, majority killed, killed mid-snapshot, slow peer, restart storm | 5/5 |
| `partition_test.py` | Network partitions, linearizability, convergence, snapshot catch-up | 5/5 on the fixed build |
| `raft_demo.py` | Failover through the gateway, with timing | Pass (2,270 ms) |

## Partition testing

`aether-engine/scripts/partition_test.py` runs a throwaway cluster (its own
Docker project, so real data is never touched) and cuts links with
**iptables rules inside the containers**. Packets really drop, and the
engine doesn't know it's being tested. Client traffic still reaches every
node, including one that's cut off, which is where stale reads would show
up.

| Scenario | What happens | What must hold |
|---|---|---|
| `leader-isolation` | Leader cut off for 10 s under a 6-client workload | History is linearizable; all nodes converge |
| `follower-isolation` | A follower cut off for 10 s | Its term doesn't climb; the leader keeps its term and role |
| `minority-writes` | Writes sent to a cut-off leader, which is then crash-restarted while still cut off | No minority write survives; the restart doesn't commit them; every majority write survives |
| `snapshot-catchup` | A follower cut off while 1,500 writes land, then reconnected | It's rebuilt via InstallSnapshot; the WAL stays small; state is identical after a full restart |
| `nemesis` | 45 s of a random node being cut off every few seconds | History is linearizable; all nodes converge |

```bash
docker build -t aetherdb-engine:dev aether-engine
python aether-engine/scripts/partition_test.py --all
python aether-engine/scripts/partition_test.py --scenario leader-isolation
```

## Linearizability checking

Each client records every operation with its start and end time. The
history is checked by **[Porcupine](https://github.com/anishathalye/porcupine)**,
the checker used by MIT's distributed systems course and etcd's robustness
tests (`scripts/linearizability/`, run in a `golang:1.22` container).

- Writes that time out are recorded as **unknown outcome**: they may or may
  not have happened, and the checker considers both.
- Each key is checked on its own, and each key's verdict is reported.

A result is trusted only if all of these hold, and the harness enforces
them:

1. The checker container's **exit code** and **out-of-memory flag** are read
   from Docker after it stops, not inferred from its output.
2. **Operations recorded = operations the checker read**, and for a pass,
   every one of them was verified.
3. Histories over **20,000 operations** are refused as "no verdict" rather
   than risking an incomplete check.

Three repeat runs of the 45 s nemesis:

| Run | Recorded = read = verified | Exit code / killed for memory | Peak memory |
|---|---|---|---|
| 1 | 7,063 | 0 / no | 34 MB |
| 2 | 7,898 | 0 / no | 32 MB |
| 3 | 7,604 | 0 / no | 32 MB |

## The clean pass

`aether-engine/scripts/clean_pass.sh` runs the whole suite in one
uninterrupted run: partition tests on the fixed build and on the original
code, the chaos suite, the failover demo, the WAL tests and the Raft safety
test. A **sentinel container** started at the beginning must still be
running, with the same start time, at the end. Any Docker or VM restart
would kill it, which would void the run.

```
start: Sun Oct  4 20:50:38 IST 2026 | Docker VM uptime 2869.68s
1 partition suite, fixed build:            exit=0
2 leader-isolation, pre-fix build:     exit=1 (expected 1)
2 follower-isolation, pre-fix build:     exit=1 (expected 1)
2 minority-writes, pre-fix build:     exit=1 (expected 1)
3 chaos_suite --all:                       exit=0
4 raft_demo (gateway failover):            exit=0
5 wal_v2_test (unit):                      exit=0
6 raft_safety_test (local 3-node):         exit=0
RUNTIME STABLE: sentinel ran uninterrupted and VM uptime never reset
```

Full logs and step-by-step evidence are in
[`CHAOS_TESTING.md`](https://github.com/Aravind9548/Aether_DB/blob/branch2/CHAOS_TESTING.md).
