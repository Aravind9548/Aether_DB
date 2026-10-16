# Findings: three Raft bugs

## Summary

By July 2026 the Raft implementation passed every test written for it:
the leader killed mid-load with zero acknowledged-write loss, follower
catch-up, concurrent writers, and five chaos scenarios. **Every one of
those tests was a crash.** None checked what clients see when nodes stay up
but can't reach each other.

The first time the cluster was tested under real network partitions with
an independent linearizability checker, it failed. It had been demoed, not
verified.

| # | Bug | Kind |
|---|---|---|
| 1 | A restart applied uncommitted log entries | **Consensus safety:** violates Raft's State Machine Safety property |
| 2 | A cut-off leader served stale reads | **Linearizability:** the log stayed correct, but clients were told something false |
| 3 | An isolated node inflated its term | **Availability:** forced needless elections; no data at risk |

**No acknowledged write was lost** in any run, before or after the fixes.
That was checked directly.

Every bug was reproduced on the unmodified code (built from `git archive`)
before anything was fixed, and the before and after runs used identical
test settings.

## Bug 1: a restart applied uncommitted entries

On startup, `load_persistent_state()` set the commit index to the end of
the log and applied every entry, including entries that were never
committed.

**Reproduced.** The leader is cut off and receives 5 writes it can't
replicate (all correctly refused with `ERR TIMEOUT`). It is then
crash-restarted while still cut off:

```
before restart: commit=21 last_log=26; after restart: commit=26 last_log=26
```

It applied 5 entries at log indexes 22–26, where the majority had already
committed and applied *different* entries. Two nodes applying different
entries at the same index is exactly what Raft's State Machine Safety
property forbids.

**Fix.** On startup, only the snapshot is known to be committed. The rest
of the log is applied once a leader confirms it.

```
after the fix: commit=21 last_log=22; after restart: commit=0 last_log=22
```

## Bug 2: stale reads from a cut-off leader

A GET only checked "am I the leader?" locally. A leader cut off by a
partition kept believing it while the other two nodes elected a new leader
and accepted newer writes.

**Reproduced:**

```
during partition: old leader node 3 says role=LEADER; majority side leader = node 2
stale read: read lin_k3='c3-89' from node 3 at +14.672s,
            but put('c5-98') had completed at +9.750s
porcupine:   key lin_k3     630 ops: VIOLATION
porcupine:   key lin_k4     573 ops: VIOLATION
RESULT: NOT LINEARIZABLE
```

**Fix.** ReadIndex (a read waits for a majority to acknowledge a heartbeat
sent after it began) plus CheckQuorum (a leader that can't hear a majority
steps down).

```
during partition: old leader node 3 says role=FOLLOWER; majority side leader = node 2
per-key: 5/5 keys verified, 4462/4462 operations covered
RESULT: LINEARIZABLE
```

Under 45 s of random partitions: **6,922 / 6,922 operations, 8/8 keys,
linearizable.**

## Bug 3: term inflation

A follower cut off from the others kept timing out and starting elections,
raising its term each time. When it reconnected, its higher term forced an
unnecessary election.

```
before: isolated follower's term, each second: 2 3 4 4 5 6 7 7 8 9
        after heal: cluster at term 12
after:  isolated follower's term, each second: 1 1 1 1 1 1 1 1 1 1
        after heal: same leader, term 1
```

**Fix.** PreVote: a node asks whether it could win before raising its
term.

## Log compaction

Not a bug, but a missing feature found alongside: the cluster never
compacted its log, so the WAL grew forever (41,698 bytes after 1,500
writes). With snapshots and InstallSnapshot it stays at 2,073 bytes, and a
follower that falls behind the compacted log is rebuilt from a snapshot.

## The test tooling's own mistakes

The harness was wrong five times. Each was caught by re-checking a result
rather than trusting it, and none became a claim:

1. A check counted `NOT_LEADER` replies as missing data and briefly
   suggested every write had been lost. Nothing was.
2. A "not linearizable" result was really the checker crashing Docker by
   running out of memory. A crash is not a verdict.
3. Re-applying a partition briefly reconnected the whole cluster, which
   spoiled one run. It was fixed, and the whole pass was repeated.
4. A clock-based "Docker restarted" check gave a false alarm; it was
   replaced with a sentinel container.
5. Error output overwrote test output, hiding two passing unit tests.

## What this shows

"It passes my tests" only meant "it survives the failures I thought of".
Verified means an independent checker, real faults, every operation
accounted for, and evidence that the test run itself was sound.
