# AetherDB-Platform

AetherDB-Platform is a production-grade, highly concurrent, secure, and persistent key-value platform. It orchestrates a **3-node Raft-replicated** C++17 sharded database engine, a PostgreSQL user registry, and a Java Spring Boot 3 API Gateway implementing JWT authentication, multi-tenant isolation, non-blocking connection pooling, and automatic leader-failover routing.

---

## 🏗️ System Architecture & Data Flow

```
[HTTP Client] ──(JSON over HTTPS)──> [Spring Boot API Service]
                                              │
                                              ├── (1. Authentication via Spring Security/JWT)
                                              ├── (2. Multi-tenant key prefix isolation)
                                              ▼
                                    [Java Socket Client Pool]
                                     (leader-aware routing)
                                              │
                                       (Raw TCP Bytes)
                                              ▼
                          ┌────────────[Raft Cluster]────────────┐
                          │  [engine-1]   [engine-2]  [engine-3] │
                          │      └── AppendEntries / votes ──┘   │
                          └──────────────────────────────────────┘
```

- **aether-gateway**: Acts as the single entry point. It handles user authentication, generates JWTs, and acts as a secure reverse proxy that sanitizes user queries and routes them to the C++ core engine.
- **aether-engine**: The core in-memory sharded storage database, running inside a private Docker bridge network isolated from the host.
- **postgres**: Stores user registration details and credentials hashed with BCrypt.

---

## 🔒 Production-Grade Design & Security Justifications

### 1. Sharded Locks instead of one Mutex (C++ Engine)
* **What breaks:** Protect a single map with a single lock. Under high concurrent write loads, threads block waiting for the table-wide mutex. Furthermore, dynamic map rehashing can corrupt the bucket pointers and cause segfaults in threads concurrently reading other keys.
* **The Architecture:** The keyspace is sharded across 16 independent map buckets, each guarded by its own read-write `std::shared_mutex`. Rehashes are localized, and contention is minimized.

### 2. Length-Prefixed Binary-Safe Framing (C++ Persistence)
* **What breaks:** Write WAL transactions as raw text separated by spaces or newlines. If a value contains space or newline characters, parsing fails during crash recovery and corrupts the state.
* **The Architecture:** Transactions are stored using length-prefixed headers (`S\n<keylen>\n<key><vallen>\n<val>`). The parser reads the exact number of bytes specified by the length prefixes, allowing raw binary values.

### 3. Log-Before-Apply Ordering
* **What breaks:** Update the in-memory map first, then log to the WAL. If the server crashes in between, the client gets a success confirmation, but the transaction is lost forever on reboot.
* **The Architecture:** Update entries are written and `fsync`'ed to disk *before* updating the memory structure. Replay on boot guarantees correctness.

### 4. Non-Blocking High-Concurrency Gateway Routing (Java Client Pool)
* **What breaks:** Using standard `synchronized` wrapper declarations across Java network clients forces the application to process every database request sequentially. This creates a severe processing bottleneck at the entry gate, rendering the C++ sharded locking engine useless.
* **The Architecture:** We built a dedicated, thread-safe connection pool using an internal `BlockingQueue<Socket>`. Threads check out long-lived, pre-allocated socket lines to run concurrent queries completely in parallel before returning them to the pool queue.

### 5. Length-Prefixed Key Multi-Tenancy Defenses
* **What breaks:** Using a simple character delimiter to split tenant identities (e.g., `tenantId + ":" + key`) allows an attacker to inject matching delimiter patterns inside user inputs. A user could intentionally look up a key like `:tenant_2:secret` from `tenant_1`'s account to escape their database isolation bubble.
* **The Architecture:** We adopted our C++ binary logging strategy directly into our Java layer, encoding keys with an explicit length prefix (`len:tenantId:key`). The system parses keys by measuring character lengths instead of searching for character tokens, neutralizing all injection attack paths.

### 6. Zero-Trust Network Topologies
* **What breaks:** Securing your infrastructure only at the API web gateway leaves internal database ports vulnerable if the core network engine is exposed directly on the host interface. Anyone with raw terminal access (`nc` or `telnet`) can connect to port 8080 and wipe the engine data entirely.
* **The Architecture:** The C++ binary operates inside an isolated Docker bridge network (`aether-mesh`). Sockets bind to internal interfaces, meaning the database cannot be queried directly from the internet, protecting data integrity.

### 7. Raft Consensus Replication (C++ Engine Cluster)
* **What breaks:** A single engine process is a single point of failure — kill it and the platform is down until it restarts, and any un-snapshotted data relies on one disk surviving.
* **The Architecture:** Three engine nodes run the Raft consensus protocol (leader election + majority-commit log replication), implemented from scratch in C++17. The **existing WAL doubles as the Raft log**: records carry a `term` and `index`, the leader streams them to followers via AppendEntries, and each follower persists them through the same fsync'd WAL machinery used for crash recovery — one mechanism, two jobs (durability *and* replication). A write is acknowledged only after a majority of nodes have it on disk, so killing the leader mid-load loses **zero acknowledged writes** (verified by test, including under concurrent multi-writer load — see below).
* **Peer RPC transport: gRPC.** `RequestVote`/`AppendEntries` between engine nodes run over gRPC (`aether-engine/proto/raft.proto`), not a hand-rolled socket protocol — a deliberate transport swap once the hand-rolled version was proven correct, to demonstrate knowing when *not* to hand-roll infrastructure. This was a pure transport migration: zero changes to election rules, the commit rule, log-conflict truncation, or the WAL, verified by rerunning the exact same `raft_demo.py`/`raft_safety_test.py` suites unchanged before/after and confirming identical PASS criteria.
* **Client data-path transport: also gRPC.** The gateway-engine `SET`/`GET`/`DEL` path (`aether-engine/proto/client.proto`, gateway's `AetherEngineClient.java`) migrated the same way, as a deliberate fast-follow once the Raft peer migration proved the pattern. `STATUS` and the raw-TCP text protocol stay on the original port 8080 (a genuinely different concern — introspection/diagnostics, not the client data path — and the interface `raft_demo.py`/`raft_safety_test.py` depend on directly), while `SET`/`GET`/`DEL` moved to a new gRPC service on port 8080+2000. Verified the same way: the unchanged `raft_demo.py` passing end-to-end through the gateway is direct proof the new client gRPC path works correctly under real failover, and `raft_safety_test.py` (which talks the raw-TCP protocol directly) confirms that path is untouched.
* **Measured failover time**: **~1.0–2.4 seconds** across independent runs against the live Docker cluster (best observed: 999ms; typical: ~2.35s), measured precisely from the `docker kill` timestamp to a new leader confirmed via `STATUS`, printed by `raft_demo.py` on every run. Consistent with the 800–1600 ms randomized election timer plus RPC overhead; the high end of the range tracks host CPU contention (this was measured on a dev machine also running Docker Desktop's own Kubernetes control plane, which independently used 40–70% CPU during some runs) rather than anything in the Raft implementation itself.

* Key mechanics, per the Raft paper:
  - Randomized election timeouts (800–1600 ms) with heartbeats every 250 ms; exactly one leader per term.
  - Election restriction (§5.4.1): only candidates with an up-to-date log can win, so a stale rejoining node can never overwrite committed data.
  - Leader appends a no-op entry on election so prior-term entries become committable after failover.
  - Followers detect log conflicts, truncate their WAL from the divergence point, and rebuild in-memory state.
  - Followers answer writes/reads with `ERR NOT_LEADER <host:port>`; the gateway follows the redirect, remembers the leader, and rotates through `AETHER_ENGINE_NODES` seeds when the leader dies.

### Live Failover Demo
With the stack running (`docker compose up --build`), run the scripted demo. It
authenticates and writes real data **through the gateway** (the actual client
path, not a raw engine connection), then:
1. Prints every node's `STATUS` (role/term/commit).
2. Writes N keys via the gateway, `docker kill`s the current leader, and polls
   the survivors' `STATUS` to measure the precise election-to-recovery delta in ms.
3. Verifies every pre-kill write is still correct via the gateway.
4. Writes M more keys **while the old leader is still down**.
5. `docker start`s it back, waits for it to rejoin, then reads its on-disk WAL
   directly (a follower doesn't serve `GET`) to confirm it caught up on
   **both** the pre-kill and the during-downtime writes.
```bash
cd aether-engine/scripts
python3 raft_demo.py            # --num-keys / --downtime-keys to change volume
```
This is a repeatable, rerunnable script — safe to run live in an interview
without typing commands by hand. Sample real output:
```
MEASURED ELECTION-TO-RECOVERY DELTA: 2351.2 ms
PASS: all 30 pre-kill acknowledged writes correct on new leader.
PASS: rejoined node's WAL contains all 45 acknowledged keys (30 pre-kill + 15 written during its downtime).
PASS: full failover demo succeeded.
```
Note: the script reads a follower's WAL directly for step 5, so it reproduces
the gateway's tenant-key encoding (`len(tenantId):tenantId:userKey` — see
`StorageService.buildIsolatedKey`) rather than searching for the raw key.

### Automated Safety Tests
Two Python integration tests exercise the safety properties directly against
local (non-Docker) processes, no manual steps required:
- `aether-engine/scripts/raft_chaos_test.py` — basic write/kill/failover/rewrite smoke test.
- `aether-engine/scripts/raft_safety_test.py` — the rigorous proof:
  1. **Mid-load leader kill** (120 sequential writes, leader `kill -9`'d at write #40): every acknowledged write survives on the new leader.
  2. **Follower catch-up**: a follower killed for 40 writes, then restarted, converges its WAL to match the leader exactly.
  3. **Concurrent writers**: 8 threads (200 writes total) hammering the leader simultaneously, leader killed mid-flight: every acknowledged write survives. This specifically rules out concurrency bugs in overlapping in-flight proposals racing a crash — a different failure mode than sequential loss, though not a different safety guarantee (majority-commit-before-ack is what prevents loss either way).

Run either with:
```bash
cd aether-engine
mkdir -p build && cp <path-to-built-AetherDB-binary> build/
python3 scripts/raft_safety_test.py
```
`aether-engine/scripts/raft_chaos_test.py` runs the same scenario as an automated local test (3 processes, no Docker needed).

### Honest Limitations / Future Work
- **No InstallSnapshot RPC**: snapshot compaction is disabled in cluster mode, so the Raft log grows unbounded; a rejoining node catches up by log replay, not snapshot transfer.
- **No dynamic membership changes**: the 3-node topology is fixed at deploy time.
- **No follower reads**: all reads are served by the leader for consistency; follower (stale) reads would be a throughput win.
- **No pre-vote optimization**: a rejoining node can trigger one unnecessary re-election before settling.
- **`STATUS`/diagnostics stay on the raw-TCP protocol**: only `SET`/`GET`/`DEL` moved to gRPC. `STATUS` and the underlying text protocol are a deliberately separate, unmigrated concern (see above) — used directly by `raft_demo.py`/`raft_safety_test.py` for introspection, not worth forcing onto gRPC (would need a Python `grpcio` dependency) for zero functional benefit.
- **`aether-engine/proto/client.proto` is duplicated**, not shared: once at `aether-engine/proto/` (canonical) and once at `aether-gateway/src/main/proto/` (copy, since the gateway's Docker build context can't see the engine's proto directory without restructuring both Dockerfiles). Manual sync risk on future proto changes — a real, acknowledged trade-off, not an oversight.

---

## 🚀 Setup and Deploy

### Prerequisites
- Docker & Docker Compose

### Step 1: Start the Platform
Navigate to the root directory containing the `docker-compose.yml` and execute:
```bash
docker-compose up --build
```
This builds and starts the C++ database engine, the PostgreSQL instance, and the Spring Boot API Gateway.

### Step 2: Register a Tenant
```bash
curl -X POST http://localhost:8080/auth/register \
     -H "Content-Type: application/json" \
     -d '{"username":"tenant_1","password":"password123"}'
```

### Step 3: Login to Obtain JWT
```bash
JWT=$(curl -X POST http://localhost:8080/auth/login \
     -H "Content-Type: application/json" \
     -d '{"username":"tenant_1","password":"password123"}' | jq -r '.token')
```

### Step 4: Write and Read Isolated Keys
Write key-value pair:
```bash
curl -X POST "http://localhost:8080/api/v1/storage?key=username" \
     -H "Authorization: Bearer $JWT" \
     -H "Content-Type: text/plain" \
     -d "aravind"
```

Read key-value pair:
```bash
curl -H "Authorization: Bearer $JWT" http://localhost:8080/api/v1/storage/username
```
