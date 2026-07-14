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
**Expected**: recovery falls back to the previous snapshot (or none) plus
full WAL replay — no corruption, no crash-loop.
**Actual**: node recreated with a 3-second delay, `SNAPSHOT_EXPORT`
triggered, killed 1 second into the write (mid-write, before the
temp-then-rename could complete). Restarted with the delay removed:
```
Loaded 1818 keys from snapshot.   <- the OLD snapshot, correctly untouched
[Raft] Node 1 started ... log size 1947    <- full WAL replay, unaffected
```
Node rejoined and converged with the rest of the cluster at `commit=1949`
within seconds. No corruption, no crash-loop.
**Result: PASS.**

### Scenario 4: Simulated slow/degraded peer link
A test-only instrumentation hook, `AETHER_CHAOS_DELAY_PEER_ID` +
`AETHER_CHAOS_DELAY_MS` (inert unless both are set), delays the leader's
outbound `AppendEntries` to one specific configured peer ID — the
"simpler approach" over Linux `tc`, avoiding both a bigger production
Docker image (no `iproute2` needed) and container-level network
manipulation privileges.

Run twice, revealing two distinct, both-legitimate behaviors:
- **3000ms delay** (exceeds the ~800–1600ms election timeout): the delayed
  peer never receives a timely heartbeat, so it correctly calls its own
  elections repeatedly — this caused **cluster-wide leadership churn**,
  not just that one peer quietly lagging. Not a bug: a peer that genuinely
  can't hear from the leader within its timeout *should* call an election.
  This is what happens when "slow" becomes "indistinguishable from dead."
- **400ms delay** (under the election timeout — the intended test): cluster
  stayed on a single stable leader throughout, term unchanged (`181`) across
  the whole run. 20/20 writes succeeded. The delayed peer lagged slightly
  under load (`commit=2007` vs `2009` for the other two) and fully caught up
  within ~2 seconds once the load stopped, before the delay was removed.
**Result: PASS** for the intended scenario, plus an honest, separate finding
about what happens when a "slow" link becomes slower than the election
timeout itself.

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

## Summary

| # | Scenario | Result |
|---|----------|--------|
| — | Disaster recovery (destroy 3 volumes, restore into a majority) | PASS |
| 1 | Kill a follower mid-load | PASS |
| 3 | Kill a majority simultaneously (negative test) | PASS |
| 2 | Kill mid-snapshot-write | PASS |
| 4 | Simulated slow peer | PASS (two variants tested) |
| 5 | Restart storm (5x) | PASS |

Two chaos-testing-only instrumentation hooks were added to support this
suite (`AETHER_CHAOS_SNAPSHOT_DELAY_MS`, `AETHER_CHAOS_DELAY_PEER_ID` /
`AETHER_CHAOS_DELAY_MS`) — both are environment variables, both default to
unset/inert, and neither is exercised in normal operation or by any other
script in this repository.
