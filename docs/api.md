# API reference

Base URL: `http://localhost:8080`. Every `/api/...` endpoint needs an
`Authorization: Bearer <token>` header.

## Authentication

### `POST /auth/register`

```json
{ "username": "alice", "password": "choose-a-password" }
```

| Status | Body |
|---|---|
| 200 | `Registration successful` |
| 400 | `Username and password are required` or `Username is already taken` |
| 429 | Rate limit reached (5 requests per minute per client IP) |

### `POST /auth/login`

Same request body. Returns:

```json
{ "token": "<JWT, valid for 24 hours>" }
```

| Status | Meaning |
|---|---|
| 200 | Logged in |
| 401 | `Invalid username or password` |
| 429 | Rate limit reached |

### Google sign-in

`GET /oauth2/authorization/google` starts the OAuth flow. It needs
`GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` to be set.

## Key-value storage

Keys are scoped to the logged-in user: two users can both have a key
called `greeting` without seeing each other's.

### `POST /api/v1/storage?key={key}`

Stores the raw request body (`Content-Type: text/plain`) as the value.

| Status | Body | Headers |
|---|---|---|
| 200 | `OK` | `X-Shard-Id`, `X-Replicated-Count`, `X-Cluster-Size` |
| 500 | Error message (e.g. the write timed out) | |

`X-Replicated-Count` is how many nodes had the write when it committed:
`2` or `3` in a 3-node cluster.

### `GET /api/v1/storage/{key}`

| Status | Body | Headers |
|---|---|---|
| 200 | The value | `X-Shard-Id` |
| 404 | `Key not found in your tenant workspace` | `X-Shard-Id` |
| 500 | Error message, e.g. `Engine operation failed: TIMEOUT` when the node couldn't confirm it's still the leader | |

### `DELETE /api/v1/storage/{key}`

| Status | Body |
|---|---|
| 200 | `Deleted` |
| 404 | `Key not found` |
| 500 | Error message |

### `GET /api/v1/storage/keys`

Lists every key in your namespace:

```json
[ { "key": "greeting", "value": "hello", "shard": 7 } ]
```

This may be served by a follower, so a key written a moment ago can be
missing for about a second. A direct GET always goes to the leader.

### `POST /api/v1/storage/batch`

```json
[ { "key": "a", "value": "1" }, { "key": "b", "value": "2" } ]
```

Returns one result per entry. This is **not atomic**: each key is a
separate write.

```json
[ { "key": "a", "ok": true, "error": null }, { "key": "b", "ok": true, "error": null } ]
```

## Cluster

### `GET /api/v1/cluster/status`

One entry per configured node. An unreachable node shows as unhealthy
without failing the whole response.

```json
[ { "address": "aether-engine-1:10080", "healthy": true, "role": "LEADER",
    "term": 3, "commitIndex": 42, "lastLogIndex": 42, "leaderHint": "localhost:8080" } ]
```

### `POST /api/v1/cluster/kill-leader`

Crashes the current leader with SIGKILL; Docker restarts it.

| Status | Body |
|---|---|
| 200 | `{ "acknowledged": true }` |
| 503 | `{ "acknowledged": false, "error": "No current leader found ..." }` during an election |

## Engine diagnostic protocol

Each engine also accepts newline-terminated text commands on its internal
port 8080. The test scripts use these directly.

| Command | Reply |
|---|---|
| `SET <key> <value>` | `OK`, `ERR NOT_LEADER <host:port>`, `ERR TIMEOUT` |
| `GET <key>` | the value, `(nil)`, `ERR NOT_LEADER ...`, `ERR TIMEOUT` |
| `DEL <key>` | `1`, `ERR NOT_LEADER ...`, `ERR TIMEOUT` |
| `STATUS` | `role=LEADER term=3 commit=42 last_log=42 applied=42 snapshot=0 log_entries=42 leader=...` |
| `DIGEST` | `applied=42 keys=40 hash=<fnv64>`: a hash of the applied state, for checking that nodes agree |
| `SNAPSHOT_EXPORT` | `OK`. Takes a snapshot now (used by backups). |

## gRPC services

Defined in `aether-engine/proto/`:

| Service | File | Calls |
|---|---|---|
| `AetherClient` | `client.proto` | `Set`, `Get`, `Delete` |
| `AetherAdmin` | `admin.proto` | `Status`, `ListKeys`, `KillSelf` |
| `RaftPeer` | `raft.proto` | `RequestVote` (with a `pre_vote` flag), `AppendEntries`, `InstallSnapshot` |
