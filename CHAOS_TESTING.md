# AetherDB Chaos Testing & Disaster Recovery

This document records the adversarial scenarios run against the live 3-node
Docker cluster, beyond the happy-path failover already proven by
`raft_safety_test.py` (leader killed mid-load, sequential and concurrent;
follower catch-up after downtime). Every result below is from a real run
against the live cluster — not derived from reading the code.

## Disaster Recovery: Backup / Restore

**Why this is a separate safety net from Raft**: Raft protects against
losing a *minority* of nodes — the cluster keeps working, no committed data
is lost. It does **not** protect against losing a *majority* at once (e.g.
all three node volumes on one dev machine's disk failing simultaneously).
Backup/restore is an independent safety net for exactly that scenario.

### How it works
- `SNAPSHOT_EXPORT` (new engine command, `aether-engine/src/Server.cpp`) triggers
  a **non-destructive** snapshot write — same atomic temp-then-rename as the
  existing periodic compaction, but it never touches or truncates the WAL.
  This matters: the existing compaction mechanism *does* truncate the WAL,
  which is safe in standalone mode but would silently desync a cluster node
  from Raft's log-tracking state (`RaftNode::log_` mirrors the WAL's
  term/index exactly) without an `InstallSnapshot` RPC — which is why
  compaction is already disabled in cluster mode. `SNAPSHOT_EXPORT` avoids
  this entirely rather than working around it.
- `aether-engine/scripts/backup.py`: triggers `SNAPSHOT_EXPORT` on any node
  (leader or follower — a follower's committed state is equally valid),
  then `docker cp`s out the snapshot, the full WAL, and `raft.meta` into a
  local, timestamped backup directory.
- `aether-engine/scripts/restore.py`: copies those three files into a
  target container's `/data/` before it starts. Zero new recovery code —
  the engine's existing startup path (load snapshot, then replay WAL) does
  all the work.

### A real finding: restore into a *majority* of nodes, not just one
The original plan assumed restoring into **one** node and letting the other
two (fresh, empty) catch up via normal replication would work — and would
be a *stronger* proof than restoring into all three identically. Tested
for real, this turned out to be wrong: with only one node restored, the
other two empty nodes can achieve majority (2 of 3) **between themselves**
(mutually "equally up to date" since both are empty) and elect an empty
leader — which then correctly forces the restored node to truncate its
entire history to match. This is not a bug: it's Raft's safety property
working exactly as designed (a minority, even with superior data, cannot
overrule a majority). It just means the correct disaster-recovery
procedure is to restore the backup into a **majority** of nodes, so the
restored data itself constitutes the new quorum.

### The proof, run for real
1. Live 3-node cluster, `restore_test_key=restore_test_value_12345` written and confirmed present (commit=1880, all 3 nodes agreeing).
2. `backup.py` against the leader — snapshot + full WAL + raft.meta copied out.
3. All three engine volumes destroyed: `docker volume rm atherdb_c_pro_aether_data_1 atherdb_c_pro_aether_data_2 atherdb_c_pro_aether_data_3` (deliberately not `docker compose down -v`, which would also wipe Postgres/Redis and pull auth-database recovery into a test about the KV engine specifically).
4. Fresh, empty containers recreated (`docker compose up --no-start`).
5. **First attempt** (restore into node 1 only): failed as described above — node 1's history got truncated to match an empty 2-node majority. Confirmed via `docker logs`, cleaned up, retried correctly.
6. **Corrected attempt**: same backup restored into **both** node 1 and node 2 (a majority), node 3 left genuinely empty. Started all three:
   ```
   engine-1: role=FOLLOWER term=55 commit=1882 last_log=1882 leader=aether-engine-2:8080
   engine-2: role=LEADER   term=55 commit=1882 last_log=1882 leader=localhost:8080
   engine-3: role=FOLLOWER term=55 commit=0    last_log=0    leader=aether-engine-2:8080
   ```
   Node 3 converged to `commit=1882` within the first poll (a few seconds) via the existing `AppendEntries` catch-up mechanism — the same mechanism already proven in `raft_safety_test.py`'s follower-catch-up phase.
7. `GET restore_test_key` on the new leader: `restore_test_value_12345` — **the pre-disaster data survived**.
8. `SET post_recovery_key it_works` + `GET`: `it_works` — **the cluster is fully functional after recovery**, not just holding stale data.
9. Final state: all 3 nodes at `commit=1883`, one leader, two synced followers.

**Result: PASS.**

---

## Chaos Scenarios

Run via `aether-engine/scripts/chaos_suite.py --scenario N` (or `--all`).

### Scenario 1: Kill a follower mid-load
**Expected**: writes continue uninterrupted (leader + remaining follower
still form a majority); killed follower catches up on restart.
**Actual**: 60/60 writes acknowledged during the kill. Leader never lost
leadership (a follower dying is a non-event for availability, confirmed
rather than assumed). All 60 writes verified correct afterward. Follower
caught up to `commit=1945` after restart.
**Result: PASS.**

### Scenario 3: Kill a majority simultaneously (the important negative test)
**Expected**: the surviving minority node must **never** elect itself
leader and must correctly refuse writes.
**Actual**: leader + one follower killed together, survivor observed for
12 seconds straight:
```
t=0s:  role=CANDIDATE term=58  commit=1945 leader=unknown
t=5s:  role=CANDIDATE term=65  commit=1945 leader=unknown
t=11s: role=CANDIDATE term=71  commit=1945 leader=unknown
```
Term climbed every election-timeout cycle (never enough votes), role never
left `CANDIDATE`. A `SET` attempt during this window returned
`ERR NOT_LEADER unknown` — correctly refused, not hung silently and not
incorrectly accepted.
**Result: PASS.** This is the single most important result in this suite:
proof that the system correctly *refuses* to make progress when it would
be unsafe to do so.

### Scenario 2: Kill a node mid-snapshot-write
A test-only instrumentation hook, `AETHER_CHAOS_SNAPSHOT_DELAY_MS` (inert
unless set), inserts an artificial delay between records during
`write_snapshot_only`'s write loop, making it possible to reliably land a
`SIGKILL` mid-write instead of racing a write that normally completes in
milliseconds.

**A gap found and fixed on re-verification**: the first version of
`scenario_2()` assumed the delay env var had already been set on the target
container by hand before running the script, and printed a note saying so
— but never automated that setup itself. Run as committed, on a normal
cluster, it printed `SCENARIO 2: PASS` without ever actually triggering the
delay, meaning the kill landed well after the (near-instant, undelayed)
write had already completed — a materially weaker, different test than
claimed, passing for the wrong reason. Caught by deliberately re-running
`chaos_suite.py --scenario 2` standalone (not the manual commands used for
the original verification) and noticing the "PASS" carried no evidence the
delay was ever active. Fixed by making the script recreate the target
container with the env var itself (`recreate_with_env`), then revert
afterward (`recreate_normal`) — fully self-contained, no manual setup step.

**Expected**: recovery falls back to the previous snapshot (or none) plus
full WAL replay — no corruption, no crash-loop.
**Actual** (corrected, self-contained run): node recreated with a 3-second
delay by the script itself, `SNAPSHOT_EXPORT` triggered, killed 1 second
into the write (mid-write, before the temp-then-rename could complete).
Restarted with the delay removed:
```
Loaded 1923 keys from snapshot.   <- the OLD snapshot, correctly untouched
[Raft] Node 1 started ... log size 2038    <- full WAL replay, unaffected
```
Node rejoined and converged with the rest of the cluster at `commit=2040`
within seconds. No corruption, no crash-loop.
**Result: PASS.**

### Scenario 4: Simulated slow/degraded peer link
A test-only instrumentation hook, `AETHER_CHAOS_DELAY_PEER_ID` +
`AETHER_CHAOS_DELAY_MS` (inert unless both are set), delays the leader's
outbound `AppendEntries` to one specific configured peer ID — the
"simpler approach" over Linux `tc`, avoiding both a bigger production
Docker image (no `iproute2` needed) and container-level network
manipulation privileges.

**Same gap as Scenario 2, found and fixed the same way**: the first version
of `scenario_4()` also assumed external setup and, run standalone on a
normal cluster, printed `SCENARIO 4: PASS` with the "slow" peer's commit
index identical to the leader's — i.e. no delay was ever actually active,
and the assertions it did check were trivially true regardless. Fixed the
same way: the script now recreates all 3 nodes itself with the env vars
set (`recreate_with_env`), waits for a stable single leader, runs the load,
then reverts (`recreate_normal`) and confirms catch-up — no manual step
required, and the check now explicitly asserts the term stayed unchanged
throughout (so a leadership-churn variant, see below, can't be mistaken
for a pass).

Run twice during development (both real, both worth keeping), revealing
two distinct, both-legitimate behaviors:
- **3000ms delay** (exceeds the ~800–1600ms election timeout): the delayed
  peer never receives a timely heartbeat, so it correctly calls its own
  elections repeatedly — this caused **cluster-wide leadership churn**,
  not just that one peer quietly lagging. Not a bug: a peer that genuinely
  can't hear from the leader within its timeout *should* call an election.
  This is what happens when "slow" becomes "indistinguishable from dead."
- **400ms delay** (under the election timeout — the intended test, and what
  `chaos_suite.py --scenario 4` runs): cluster stayed on a single stable
  leader throughout, term unchanged (`192`) across the whole run. 20/20
  writes succeeded. The delayed peer genuinely lagged under load
  (`commit=2060` vs `2061` for the leader) and caught up to `commit=2062`
  once the delay was removed.
**Result: PASS** for the intended scenario (now the only one the script
runs, verified self-contained), plus an honest, separate finding about
what happens when a "slow" link becomes slower than the election timeout
itself.

### Scenario 5: Restart storm
**Expected**: no corruption, no crash, correct re-sync on every cycle.
**Actual**: same follower killed and restarted 5 times in quick succession
while writing one key per round. 4/5 writes acknowledged (one transient
timeout, not a safety issue). Follower converged correctly to the leader's
commit index after the storm, and **all 4 acknowledged writes were
confirmed present and correct** — zero loss across five rapid kill/restart
cycles.
**Result: PASS.**

---

## Network Partitions & Linearizability

### The headline: the "verified" Raft had three real bugs

Before this work, the Raft implementation had passed every test written for
it: leader killed mid-load with zero acknowledged-write loss, follower
catch-up, concurrent writers, the five crash scenarios above. It was
*demoed*. It was not *verified*: every one of those tests is a crash, and
none checked what clients actually observe while nodes are up but unable to
reach each other.

The first time the cluster was tested under real network partitions, with a
linearizability checker, it failed. Three real bugs, classified precisely
because the difference matters:

| # | Bug | Kind | What it violated |
|---|-----|------|------------------|
| 1 | Restart applied uncommitted log entries | **Consensus safety** | Raft's *State Machine Safety* property: a node applied entries at indexes 22–26 that the rest of the cluster later filled with different entries |
| 2 | Cut-off leader served stale reads | **Linearizability** (read path) | The replicated log stayed correct, but clients were told something false. Raft §8 requires a leader to confirm leadership before serving reads; this code didn't |
| 3 | Isolated follower inflated its term | **Availability** | No data at risk; one bad link forced a needless election when it healed |

What it did **not** have: no acknowledged write was ever lost, in any run. That
claim was checked directly, not assumed (see "harness mistakes" below; one run
falsely suggested otherwise).

### How the harness works

- **Real packet-level partitions.** `iptables` rules inside each container
  drop all traffic between chosen node IPs, in both directions, on both ends.
  The engine doesn't know it's being tested. Client traffic from the host is
  untouched, so the test can still talk to a cut-off node, which is where
  stale reads happen.
- **Isolated from real data.** A throwaway compose project
  (`-p aetherdb-chaos`) plus the `docker-compose.chaos.yml` overlay adds
  `NET_ADMIN`, publishes the engines on `127.0.0.1:18081-18083`, and sets a low
  snapshot threshold. The real cluster's volumes are never touched.
- **Linearizability checking with Porcupine**, the checker used by MIT
  6.5840's Raft labs and etcd's robustness tests (`scripts/linearizability/`,
  run in a `golang:1.22` container). Concurrent clients read and write unique
  values; every operation is recorded with call/return times. Writes with an
  unknown outcome (timeouts) may or may not have happened and are modelled
  that way; failed reads are dropped. Porcupine checks each key's history
  independently and reports each key's verdict, then the whole history.
- **A verdict is only accepted from a checker that provably finished:** the
  container's own exit code and `OOMKilled` flag are read back with
  `docker inspect` (2 GB limit), and the run must report
  `N/N keys verified, X/X operations covered`.
- **Convergence** is checked exactly with a `DIGEST` command
  (`applied=<index> keys=<n> hash=<fnv64>` over the applied state), not by
  sampling keys.
- **Before/after on identical settings.** The pre-fix code is `git archive
  HEAD` built with the same Dockerfile (`aetherdb-engine:legacy`); the fixed
  code is `aetherdb-engine:dev`.

```bash
docker build -t aetherdb-engine:dev aether-engine
python aether-engine/scripts/partition_test.py --all                  # fixed build, ~4 min
bash   aether-engine/scripts/clean_pass.sh                             # the full verification pass below
```

### Verification protocol: one clean pass on a stable runtime

Docker Desktop crashed twice earlier in this work (once from a duplicate
launch, once when a checker exhausted the VM's memory on an oversized
history). Results gathered around a flapping runtime are suspect, so
everything below comes from **one uninterrupted end-to-end pass**
(`clean_pass.sh`) after those were fixed, with proof the runtime stayed up:

```
start: Sun Oct  4 20:50:38 IST 2026 | Docker VM uptime 2869.68s | sentinel started 2026-10-04T15:20:36.295213354Z
1 partition suite, fixed build:            exit=0
2 leader-isolation, pre-fix build:     exit=1 (expected 1)
2 follower-isolation, pre-fix build:     exit=1 (expected 1)
2 minority-writes, pre-fix build:     exit=1 (expected 1)
3 chaos_suite --all:                       exit=0
4 raft_demo (gateway failover):            exit=0
5 wal_v2_test (unit):                      exit=0
6 raft_safety_test (local 3-node):         exit=0
end: Sun Oct  4 20:59:43 IST 2026 | Docker VM uptime 3360.99s | wall 542s | sentinel: true 2026-10-04T15:20:36.295213354Z
RUNTIME STABLE: sentinel ran uninterrupted and VM uptime never reset
```

The sentinel is a container started at the beginning that must still be
running, with the same start time, at the end; any daemon or VM restart
kills it.

### Did the checker actually finish? Completeness and memory

A "LINEARIZABLE" line on its own proves nothing if the checker could have
died partway through. A verdict is accepted only when all of these hold,
enforced by the harness (`check_linearizable`):

- the checker container's **exit code** and **`OOMKilled` flag**, read from
  Docker after it stops (`docker inspect`), are `0` / `false`;
- the number of operations the checker **read** equals the number the run
  **recorded**;
- for a pass, the operations in keys that individually returned Ok cover
  **all** of them (Porcupine returns Ok only after finding a valid order;
  a timeout gives Unknown, never Ok).

The pass-#2 nemesis history file, counted independently: 6,922 records
(3,494 ok puts, 4 unknown puts, 3,424 reads) across 8 keys, matching the
checker's `8/8 keys verified, 6922/6922 operations covered`.

**Why the original out-of-memory crash can't recur at this scale, and what
did and didn't change:**

| | First run (crashed) | Now |
|---|---|---|
| History size | 40,227 ops, unpaced clients | ~7–8k ops (paced); **hard cap 20,000**: anything larger is refused with "no verdict", never sent to the checker |
| Unknown-outcome writes (what makes the search expensive) | 1,311 (pre-fix code) | 0–10 (fixed code: CheckQuorum turns most into clean `NOT_LEADER` rejections) |
| Checker memory limit | none (shared the whole VM) | 2 GB container cap, so it can't take down the VM; if it's hit, `OOMKilled=true` means "no verdict" |
| Docker VM memory | 3.9 GB | 3.9 GB (unchanged) |

Three repeat runs of the 45 s nemesis on the fixed build, after these checks
were added. Peak memory is the container's **resident** peak from cgroup
`memory.peak` (it includes the `go run` toolchain), not the Go runtime's
virtual reservation:

| Run | Recorded | Checker read | Covered | Exit / OOMKilled | Peak resident |
|---|---|---|---|---|---|
| 1 | 7,063 | 7,063 | 7,063/7,063 | 0 / false | 34 MB |
| 2 | 7,898 | 7,898 | 7,898/7,898 | 0 / false | 32 MB |
| 3 | 7,604 | 7,604 | 7,604/7,604 | 0 / false | 32 MB |

About 60× headroom under the cap, with the history size bounded: a
repeatable margin, not luck. One honest caveat: *pre-fix* histories are far
more expensive to check, because proving a violation means exhausting every
order (one reserved 2.8 GB virtual and needed 10 minutes). The 2 GB cap and
the "no verdict" rule mean that can fail closed, never pass by accident.

### Bug 2: stale reads (before → after)

Pre-fix, leader cut off from both peers for 10 s under a 6-client workload.
The old leader still says `role=LEADER`, and clients still talking to it
read old values:

```
  during partition: old leader node 3 says role=LEADER; majority side leader = node 2
  stale read: client 2 read lin_k3='c3-89' from node 3 at +14.672s, but put('c5-98')
              had completed at +9.750s (after put('c3-89') finished at +4.172s)
  porcupine: history: 2951 operations (2916 with a definite result, 35 writes with unknown outcome, 1445 reads)
  porcupine:   key lin_k3     630 ops: VIOLATION
  porcupine:   key lin_k4     573 ops: VIOLATION
  porcupine: RESULT: NOT LINEARIZABLE (checked in 1.32s)
  checker container: exit=1 OOMKilled=false (2 GB limit)
```

Fixed, same scenario and settings:

```
  during partition: old leader node 3 says role=FOLLOWER; majority side leader = node 2
  porcupine: history: 4462 operations (4458 with a definite result, 4 writes with unknown outcome, 2208 reads)
  porcupine: per-key: 5/5 keys verified, 4462/4462 operations covered
  porcupine: RESULT: LINEARIZABLE (checked in 33ms)
  checker container: exit=0 OOMKilled=false (2 GB limit)
```

And under 45 s of a random node being cut off every few seconds:

```
  porcupine: history: 6922 operations (6918 with a definite result, 4 writes with unknown outcome, 3424 reads)
  porcupine: per-key: 8/8 keys verified, 6922/6922 operations covered
  porcupine: RESULT: LINEARIZABLE (checked in 43ms)
  checker container: exit=0 OOMKilled=false (2 GB limit)
```

**Fix: ReadIndex** (Raft thesis §6.4). `GET` calls `RaftNode::read_barrier()`:
the leader must have committed an entry from its own term, then a majority
must acknowledge a heartbeat round sent *after* the read began. A cut-off
leader can never collect that, so it answers `ERR TIMEOUT` instead of stale
data. **Plus CheckQuorum:** a leader that no majority has acknowledged within
the election-timeout window steps down, which is why the old leader now
reports `FOLLOWER`, and new writes to it are rejected at once with
`NOT_LEADER` instead of hanging (unknown-outcome writes: 35 → 4).

### Bug 3: term inflation (before → after)

Pre-fix, a follower cut off for 10 s:

```
  isolated follower's term, sampled each second while cut off: ['2','3','4','4','5','6','7','7','8','9']
  after heal: node 1 role=LEADER term=12          # leader was in term 1; a needless election ran
```

Fixed:

```
  isolated follower's term, sampled each second while cut off: ['1','1','1','1','1','1','1','1','1','1']
  PASS: node 1 is still leader in term 1 after the heal
```

**Fix: PreVote** (thesis §9.6). Before raising its term, a node asks "would
you vote for me?" without changing anyone's state. Peers that can still hear a
leader refuse, so a cut-off node never raises its term.

### Bug 1: restart applied uncommitted entries (before → after)

The leader is cut off, receives 5 writes it can't replicate (correctly never
acknowledged: `ERR TIMEOUT` ×5), and is crash-restarted while still cut off.
Pre-fix:

```
  cut-off node 2 before restart: commit=21 last_log=26; after restart: commit=26 last_log=26
```

`load_persistent_state()` set `commit_index` to the end of the log and
applied everything. The node's state machine now held 5 writes the cluster
never agreed to, at indexes 22–26, where the majority had already committed
and applied *different* entries (its no-op and the majority's writes). Two
nodes applying different entries at the same index is exactly what Raft's
State Machine Safety property forbids. After the heal, reads through the
leader showed none of the 5 writes and every majority write (`leaked []`,
`missing []`), so clients never saw them through the leader. (The pre-fix
build has no `DIGEST` command, so the old node's own state after the heal
wasn't checked directly.) Fixed:

```
  cut-off node 3 before restart: commit=21 last_log=22; after restart: commit=0 last_log=22
  PASS: none of the minority's writes survived the heal; every majority-acknowledged write survived
```

**Fix:** on startup only the snapshot is known committed; later entries are
applied only once a leader confirms they're committed. (Only one minority
write reached the log here, because CheckQuorum stepped the old leader down
and the other four were rejected before being appended.)

### Log compaction & InstallSnapshot

Pre-fix, cluster mode never compacted. Truncating the WAL out from under Raft
without a way to ship state to lagging followers would desync them, so the log
grew forever: **41,698 bytes per node after 1,500 writes** (pre-fix
`snapshot-catchup` run, re-measured on the stable stack, 21:01).

Now, once `AETHER_SNAPSHOT_THRESHOLD` (default 1000) applied entries pile up
past the last snapshot, a node writes a **Raft-indexed snapshot** (`RS2` header +
last included index/term; temp-then-rename, file *and* directory fsynced),
then drops that WAL prefix; a crash between the two is harmless. A follower
that needs compacted entries gets an **`InstallSnapshot` RPC**, then the log
tail. Fixed build, follower cut off while 1,500 writes land (threshold 200):

```
  node 1: [Raft] Node 1 installed snapshot from leader 2 (index 1432, term 1, 1431 keys)
  PASS: follower caught up and converged {applied 1501, keys 1500, hash cbfef21bf6007fd5 on all 3}
  WAL bytes per node after 1500 writes: {1: 2073, 2: 2073, 3: 2073}; log entries kept: 69 each
  PASS: state identical after full-cluster restart (hash cbfef21bf6007fd5 before and after)
```

`SNAPSHOT_EXPORT` (used by `backup.py`) now takes a real Raft snapshot in
cluster mode, and `backup.py` copies the WAL *before* the snapshot (the
snapshot index only grows, so the later copy always covers where the WAL
begins; the other order can leave a gap if auto-compaction runs mid-copy).

**Upgrade path, verified on the real cluster's existing volumes:** the new
build logged `Ignoring untagged snapshot file; the WAL holds the full log`,
replayed the 2,169-entry WAL, compacted at index 2170, and all three nodes
reported the same `keys=1962 hash=297edb7784b99732`.

### Other changes made alongside

- **`NOT_LEADER` is now unambiguous.** A leader that loses leadership *after*
  appending a write returns `ERR TIMEOUT` (outcome unknown), never
  `NOT_LEADER`, so `NOT_LEADER` always means "nothing was written" and clients
  can treat it as a clean rejection.
- **gRPC reconnect tuning.** Peer channels use 100 ms–1 s reconnect backoff plus
  keepalive (gRPC's default backoff grows to 120 s), so a healed link starts
  working again promptly.
- **Follower match index** now reports only what the AppendEntries call
  verified (prev index + entries), not the follower's whole log, per Raft
  Figure 2. This is a latent issue, made reachable by the new 256-entry batch
  cap. It was not observed failing, so it is not counted among the bugs above.

### Harness mistakes, caught before they were reported

The test tooling was wrong five times. Each one was found by re-checking
results rather than trusting them, and none made it into a claim:

1. **"Every acknowledged write lost."** A post-heal check counted any
   non-matching reply as missing, including `ERR NOT_LEADER` during
   leadership churn. Fixed to follow leader redirects and retry until a
   definite answer: leaked `[]`, missing `[]`.
2. **"Nemesis not linearizable"** was a crash, not a verdict: an unpaced 45 s
   pre-fix run produced 40,227 operations (1,311 unknown-outcome writes) and
   the checker exhausted the Docker VM's memory. Linearizability checking is
   NP-hard, and proving a *violation* means exhausting every ordering. Fixed:
   paced clients, memory-capped checker, exit code + `OOMKilled` read back,
   only exit 1 counts as a violation.
3. **A partition that leaked.** Re-partitioning flushed *every* node's rules
   before re-adding them, so for a few hundred ms the whole cluster was
   connected. In one pre-fix run the "cut off" node synced with the majority
   mid-test (`last_log` 26 → 42), which made that run meaningless. Fixed:
   each node flushes and re-adds in one step, and every link is blocked
   from both ends. That run was discarded and the whole pass re-run.
4. **A false "VM restarted" warning.** The first version of `clean_pass.sh`
   required VM uptime to keep pace with wall time; WSL2's clock ran ~111 s
   slow over 20 min under load. Replaced with the sentinel container.
5. **Missing unit-test output.** Through `wsl.exe`, stdout and stderr reach
   a Windows file through separate pipes; `> file 2>&1` let stderr overwrite
   the first lines of stdout, hiding `test_compact_prefix OK` and
   `test_raft_snapshot OK`. Fixed by merging inside WSL; re-run (outside the
   sentinel window, a pure unit test): 8/8 with visible output.

### Regression: the pre-existing suites (same clean pass)

- `chaos_suite.py --all`: scenarios 1–5 **PASS**. In scenario 3 (majority
  killed), the survivor now stays `FOLLOWER` at a fixed term instead of cycling
  as a `CANDIDATE`; that's PreVote, and it's the better behaviour.
- `raft_safety_test.py` (local 3-process cluster): 120 writes with leader
  killed, follower catch-up (160 keys), 8 concurrent writers (200/200):
  **ALL PASS**.
- `raft_demo.py` through the gateway: failover **2269.5 ms**, 30 pre-kill + 15
  downtime writes verified (its check now reads snapshot + WAL tail, since
  compacted keys live in the snapshot).
- `wal_v2_test`: **8/8**, including `test_compact_prefix` and `test_raft_snapshot`.

---

## Summary

| # | Scenario | Result |
|---|----------|--------|
| — | Disaster recovery (destroy 3 volumes, restore into a majority) | PASS |
| 1 | Kill a follower mid-load | PASS |
| 3 | Kill a majority simultaneously (negative test) | PASS |
| 2 | Kill mid-snapshot-write | PASS |
| 4 | Simulated slow peer | PASS (two variants tested) |
| 5 | Restart storm (5x) | PASS |
| P1 | Leader cut off mid-workload (Porcupine) | Pre-fix: NOT LINEARIZABLE → fixed: LINEARIZABLE (4462/4462 ops) |
| P2 | Follower cut off (term inflation) | Pre-fix: forced election, term 1→12 → fixed: term stays 1 |
| P3 | Minority writes + restart while cut off | Pre-fix: restart committed 5 uncommitted entries → fixed: PASS |
| P4 | 45s random-partition nemesis (Porcupine) | Fixed: LINEARIZABLE, 6922/6922 ops, 8/8 keys |
| P5 | Follower behind compacted log (InstallSnapshot) + full restart | Fixed: PASS (no cluster compaction pre-fix) |

Two chaos-testing-only instrumentation hooks were added to support this
suite (`AETHER_CHAOS_SNAPSHOT_DELAY_MS`, `AETHER_CHAOS_DELAY_PEER_ID` /
`AETHER_CHAOS_DELAY_MS`) — both are environment variables, both default to
unset/inert, and neither is exercised in normal operation or by any other
script in this repository.
