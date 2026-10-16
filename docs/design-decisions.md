# Design decisions

| Decision | Instead of | Why |
|---|---|---|
| 16 shards, one reader-writer lock each | One global lock | Writes to different keys run in parallel; a resize only affects one shard. |
| Log before apply, with `fsync` | Apply first, log later | A crash between the two steps can't lose a write the client was told succeeded. |
| Length-prefixed WAL records | Space- or newline-separated text | Values can contain any bytes without breaking recovery. |
| The WAL is the Raft log | A separate replication log | One mechanism provides both durability and replication. |
| Group commit | One `fsync` per write | Concurrent writes share a disk sync: 90.5 → 194–376 ops/sec. |
| gRPC for peers and clients | Hand-rolled TCP (the first version) | The hand-rolled version was proven correct first, then replaced. The same safety tests passed unchanged before and after. |
| Length-prefixed tenant keys (`len:tenant:key`) | `tenant + ":" + key` | A key containing `:` can't reach another tenant's data. |
| ReadIndex for reads | Leader leases | Correct with no assumption about clock drift. It costs one network round trip per read. |
| PreVote + CheckQuorum | Plain Raft elections | A cut-off node can't disrupt the cluster when it returns, and a cut-off leader stops accepting work. |
| Snapshot tagged with log index and term | Untagged state dump | The WAL prefix can be safely dropped, and a snapshot can be sent to a lagging follower. |
| Kill Leader via a self-kill RPC | Mounting `docker.sock` in the gateway | `docker.sock` access is root on the host; the RPC adds no new privilege. |
| Separate admin gRPC service | Adding admin calls to the client API | Keeps chaos and introspection tools out of the data path. |
| Replication info in response headers | Changing the response body to JSON | Existing clients that read the plain-text body keep working. |
| Batch SET as a loop of single writes | An atomic multi-key write | Kept in scope; each key succeeds or fails on its own and is reported that way. |
| Partitions via iptables in containers | Simulating them inside the engine | Packets really drop, so TCP, gRPC timeouts and reconnects behave as in production. |
| JWT secret from the environment, no default | A value in `application.properties` | The repository is public; a committed secret would let anyone forge tokens. |
