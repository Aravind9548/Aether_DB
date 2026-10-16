# Glossary

| Term | Meaning |
|---|---|
| **Raft** | A consensus algorithm: a way for several servers to agree on one ordered log of changes, even when some of them crash. |
| **Leader / follower / candidate** | One node, the leader, accepts writes and copies them to the others, the followers. A follower that stops hearing from the leader becomes a candidate and asks for votes. |
| **Term** | Raft's election counter. Each election starts a new term, and there is at most one leader per term. |
| **Majority (quorum)** | More than half the nodes: 2 of 3. A write counts once a majority has it, so any single node can fail without losing it. |
| **Commit index** | How far along the log is known to be stored on a majority, and therefore safe to apply. |
| **WAL (write-ahead log)** | Changes are written to disk before they're applied, so a crash can be recovered by replaying the log. |
| **fsync** | Forces data from the operating system's memory onto the physical disk. It's slow, which is why group commit lets several writes share one. |
| **Group commit** | Batching several concurrent writes into a single `fsync`. |
| **Shard** | One slice of the keyspace with its own lock. AetherDB uses 16. |
| **Snapshot / log compaction** | Saving the whole state at one point in the log, so the log before that point can be deleted. |
| **InstallSnapshot** | The leader sends its snapshot to a follower that's too far behind to catch up from the log. |
| **Partition** | A network failure where nodes keep running but can't reach each other. |
| **Linearizable** | Every operation appears to happen at one instant, in one order that all clients agree on. Once a write returns OK, no later read may return an older value. |
| **Stale read** | A read that returns a value older than one already confirmed to a client. |
| **Porcupine** | A tool that checks a recorded history of operations for linearizability. |
| **ReadIndex** | Before answering a read, the leader confirms with a majority that it is still the leader. |
| **CheckQuorum** | A leader that can't hear from a majority steps down instead of believing it still leads. |
| **PreVote** | A node checks it could win an election before raising its term, so a cut-off node can't disrupt the cluster when it reconnects. |
| **State Machine Safety** | A core Raft guarantee: if one node has applied an entry at some log index, no node ever applies a different entry at that index. |
| **Tenant** | One user's isolated namespace of keys. |
| **JWT** | JSON Web Token: a signed token the gateway issues at login and checks on every request. |
