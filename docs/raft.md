# Consensus (Raft)

Raft keeps the three nodes in agreement on one ordered log of changes, even
when some of them crash or can't reach each other. AetherDB's
implementation is written from scratch in `aether-engine/src/Raft.cpp`.

## Roles and terms

Each node is a **follower**, a **candidate** or the **leader**. Time is
divided into **terms**, and there is at most one leader per term. A node
that sees a higher term than its own steps down to follower.

| Setting | Value |
|---|---|
| Heartbeat interval | 250 ms |
| Election timeout | random, 800–1,600 ms |
| Peer RPC timeout | 300 ms |
| Majority (3 nodes) | 2 |

## Leader election

A follower that hears nothing from a leader within its election timeout
tries to become leader:

1. **PreVote.** It asks its peers "would you vote for me in the next
   term?" without changing anyone's state. A peer says no if it has heard
   from a leader recently, or if the candidate's log is behind its own.
2. If a majority says yes, it raises its term and asks for real votes
   (**RequestVote**).
3. A node votes for at most one candidate per term, and only for one whose
   log is at least as up to date as its own (the *election restriction*,
   Raft §5.4.1). This guarantees a new leader already has every committed
   entry.
4. The winner appends a **no-op entry** in its new term. Committing it also
   commits any earlier entries it inherited.

**Why PreVote matters.** Without it, a node cut off by a partition keeps
timing out and raising its term. When it reconnects, its high term forces a
healthy leader to step down. In testing, the original code went from term
1 to 12 this way. With PreVote, the term stays at 1.

## Log replication

1. The leader appends the entry to its WAL (group-commit `fsync`).
2. It sends **AppendEntries** to each follower, including the index and
   term of the entry just before the new ones.
3. A follower accepts only if its log matches at that point. Otherwise it
   says no, and the leader backs up and retries. If a follower has
   conflicting entries, it deletes them and everything after.
4. Once a majority has the entry, the leader marks it **committed**,
   applies it to memory, and replies to the client.
5. Followers learn the commit point from the next AppendEntries and apply
   it too.

Catch-up batches are capped at 256 entries per RPC. A follower reports back
only the index that the RPC actually verified (Raft Figure 2), never the
end of its own log.

## Linearizable reads: ReadIndex

A leader answering reads from its own memory can be wrong: if a partition
cuts it off, the other two nodes elect a new leader and accept newer
writes, while the old one still believes it leads.

`read_barrier()` (Raft thesis §6.4) prevents that. Before a GET is
answered:

1. The leader must have committed an entry from its **own** term (so it
   knows the true commit point).
2. It sends a heartbeat round and waits until a **majority acknowledges a
   heartbeat sent after the read began**.

A cut-off leader never gets those acknowledgements, so it answers
`TIMEOUT` instead of returning stale data.

## CheckQuorum

A leader that hasn't heard back from a majority within the election-timeout
window (1,600 ms) steps down to follower. So a cut-off leader stops
accepting writes and starts answering `NOT_LEADER`, instead of believing
it leads forever.

## Snapshots and log compaction

- Once `AETHER_SNAPSHOT_THRESHOLD` (default 1,000) applied entries have
  piled up since the last snapshot, the node copies its state, writes
  `aether.snapshot` with the last included **index and term**, `fsync`s the
  file and its directory, then drops that part of the WAL.
- A crash between those two steps is harmless: on startup the node skips
  WAL entries that the snapshot already covers.
- If a follower needs entries the leader has already dropped, the leader
  sends **InstallSnapshot** with the whole state, then continues with the
  log.

## Restart recovery

On startup a node:

1. Reads its term and vote from `raft.meta`.
2. Loads the snapshot. Everything in it is committed.
3. Reads the rest of the WAL into memory **without applying it**.

The entries after the snapshot may be uncommitted leftovers, for example
writes a cut-off leader accepted but couldn't replicate, which the new
leader will discard. They are applied only once a leader confirms they're
committed. The original code applied them straight away; that was one of
the [three bugs](./findings.md).

## What the client sees

| Reply | Meaning |
|---|---|
| `OK` | Committed on a majority. Survives any single node failure. |
| `ERR NOT_LEADER <host:port>` | Nothing was written. Retry at the given leader. |
| `ERR TIMEOUT` | Outcome unknown: the write might or might not commit. For reads, the node couldn't confirm it's still the leader. |
