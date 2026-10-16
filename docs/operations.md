# Configuration & operations

## Engine settings

Environment variables are read first; command-line flags override them.

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `AETHER_PORT` | `--port` | `8080` | Text-protocol port. Raft uses port + 1000, client gRPC port + 2000. |
| `AETHER_NODE_ID` | `--node-id` | `0` | This node's ID in the cluster. |
| `AETHER_PEERS` | `--peers` | none | Peers as `2=host:8080,3=host:8080`. If empty, the node runs standalone (no Raft). |
| `AETHER_DATA_DIR` | `--data-dir` | `.` | Where the WAL, snapshot and `raft.meta` live. |
| `AETHER_PROPOSE_TIMEOUT_MS` | `--propose-timeout-ms` | `5000` | How long a write (and a read barrier) waits before answering `TIMEOUT`. |
| `AETHER_SNAPSHOT_THRESHOLD` | | `1000` | Applied entries since the last snapshot before a new one is taken. `0` disables. |
| | `--shards` | `16` | Number of in-memory shards. |

Chaos-testing hooks, unset and inactive by default:
`AETHER_CHAOS_SNAPSHOT_DELAY_MS` (slows snapshot writes so a test can kill
a node mid-write), and `AETHER_CHAOS_DELAY_PEER_ID` plus
`AETHER_CHAOS_DELAY_MS` (delay the leader's messages to one peer).

## Gateway settings

| Variable | Meaning |
|---|---|
| `AETHER_JWT_SECRET` | **Required.** Signs login tokens. Generate with `openssl rand -hex 32` and keep it in `.env`. |
| `AETHER_ENGINE_NODES` | Engine gRPC addresses, e.g. `aether-engine-1:10080,aether-engine-2:10080,aether-engine-3:10080` |
| `AETHER_ENGINE_HOST`, `AETHER_ENGINE_PORT` | First node to try |
| `SPRING_DATASOURCE_URL`, `_USERNAME`, `_PASSWORD` | PostgreSQL connection |
| `SPRING_DATA_REDIS_HOST`, `_PORT` | Redis connection |
| `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` | Optional, for Google sign-in |

## Checking cluster health

From the Cluster Console, or directly against a node:

```bash
docker exec atherdb_c_pro-aether-engine-1-1 bash -c \
  'exec 3<>/dev/tcp/localhost/8080; printf "STATUS\nDIGEST\n" >&3; timeout 2 head -2 <&3'
```

Healthy: exactly one `LEADER`, every node on the same term, and every node
reporting the same `DIGEST` hash once its `applied` index catches up.

## Backup and restore

Raft protects against losing **one** node. A backup protects against losing
a **majority** at once, for example every volume on one disk.

```bash
# Back up any healthy node (it takes a snapshot first)
python3 aether-engine/scripts/backup.py <container> --out-dir backups

# Restore into a stopped container
python3 aether-engine/scripts/restore.py backups/<timestamp> <container>
```

- A backup is the snapshot plus the WAL entries after it, plus
  `raft.meta`. The WAL is copied before the snapshot, so a compaction that
  happens during the backup can't leave a gap.
- **Restore into a majority of nodes (2 of 3), not just one.** If only one
  node gets the backup, the two empty nodes form a majority by themselves
  and correctly overrule it.

> The restore procedure has not been re-run since the snapshot format
> changed in October 2026. See [Limitations](./limitations.md).

## Upgrading from a version without log compaction

No migration step is needed. Older nodes have an untagged snapshot file and
a complete WAL. The new version logs
`Ignoring untagged snapshot file; the WAL holds the full log`, replays the
WAL, and takes its first tagged snapshot once past the threshold. This was
verified on a live cluster's existing volumes.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `docker compose up` fails with "set AETHER_JWT_SECRET in .env" | Add the secret to `.env`; see [Getting started](./getting-started.md). |
| GETs return `500 … TIMEOUT` | The gateway reached a node that can't confirm it's the leader (a partition or an election). It recovers once leadership settles. |
| A node stays `CANDIDATE`/`FOLLOWER` with `leader=unknown` | It can't reach a majority. With PreVote its term won't climb while it's cut off. |
| Election timing is erratic on a laptop | Host CPU contention. On Docker Desktop, other containers can slow elections noticeably. |
