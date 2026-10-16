# Limitations & roadmap

## Known limitations

| Limitation | Impact |
|---|---|
| The gateway turns a refused read (`TIMEOUT`) into an immediate HTTP 500 instead of trying another node | During a partition, errors for up to ~1.6 s. Safe, but less available than it could be. |
| Read throughput hasn't been re-measured since ReadIndex was added | Every GET now costs one network round trip; the size of the slowdown is unknown. |
| The disaster-recovery restore hasn't been re-run since the snapshot format changed | The restore code is unchanged and should work, but it's untested. |
| Snapshots are sent in a single message (256 MB limit) | Once the data exceeds 256 MB, a lagging follower can never catch up. |
| Taking a snapshot copies all data while holding the main lock | A pause that grows with data size. |
| Partition tests cut off one whole node at a time, symmetrically, for 45 s | One-way partitions, packet loss and clock skew aren't tested. |
| No unit tests for the Raft logic itself | Raft is tested end to end only. |
| Fixed 3-node membership | Nodes can't be added or removed while running. |
| All reads go to the leader | No follower reads. |
| Any logged-in user can press Kill Leader | Fine for a demo; production would need an admin role. |
| The engine image includes `iptables` and `iproute2` | Needed by the partition tests. They're inert without the NET_ADMIN permission, which only the test overlay grants. |
| Earlier commits contain a JWT secret that has since been rotated out | That value is no longer used anywhere, so it can't be used to forge tokens. |

## Roadmap

**Next:**

1. Gateway: on `TIMEOUT`, try the next node and return 503 instead of 500.
   Add a partition test that goes through the gateway.
2. Re-measure read and write throughput with the gateway load test and
   record ReadIndex's cost.
3. Re-run the disaster-recovery procedure with the new snapshot format.

**Later:**

- Chunked InstallSnapshot, and snapshots that don't copy everything under
  the lock.
- Leader leases, to skip the read round trip where clock assumptions are
  acceptable.
- Follower reads via ReadIndex.
- One-way partitions and packet loss with `tc netem`.
- Unit tests for Raft logic.
- Dynamic membership changes.
