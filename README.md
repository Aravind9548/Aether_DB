# AetherDB-Platform

AetherDB-Platform is a production-grade, highly concurrent, secure, and persistent key-value platform. It orchestrates a C++17 sharded database engine, a PostgreSQL user registry, and a Java Spring Boot 3 API Gateway implementing JWT authentication, multi-tenant isolation, and non-blocking connection pooling.

---

## 🏗️ System Architecture & Data Flow

```
[HTTP Client] ──(JSON over HTTPS)──> [Spring Boot API Service]
                                              │
                                              ├── (1. Authentication via Spring Security/JWT)
                                              ├── (2. Multi-tenant key prefix isolation)
                                              ▼
                                    [Java Socket Client Pool]
                                              │
                                       (Raw TCP Bytes)
                                              ▼
                                    [AetherDB C++ Engine]
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
