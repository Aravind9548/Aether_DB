# Getting started

## Prerequisites

- **Docker** with Docker Compose (Docker Desktop on Windows or macOS).
- About 2 GB of free memory for the full stack.
- Optional, for development: a Linux toolchain (g++ 13, CMake 3.28,
  gRPC/protobuf dev packages) for building the engine outside Docker,
  Java 21 + Maven for the gateway, and Python 3 for the test scripts.

## 1. Configure secrets

Create a `.env` file in the repository root. It is gitignored, so never
commit it.

```bash
# Required: signs login tokens. The stack refuses to start without it.
AETHER_JWT_SECRET=<output of: openssl rand -hex 32>

# Optional: only needed for "Sign in with Google".
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
```

## 2. Start the platform

```bash
docker compose up -d --build
```

This starts three engine nodes, the gateway, PostgreSQL and Redis. Only the
gateway is published, on port **8080**; everything else is on a private
Docker network.

Check that the cluster has elected a leader:

```bash
docker compose logs aether-engine-1 aether-engine-2 aether-engine-3 | grep "became LEADER"
```

## 3. Use the dashboard

Open **http://localhost:8080** in a browser. Register, log in, then:

- **Console** (`/`): set, get and delete keys. Each write shows which shard
  it landed in and how many nodes had it when it committed.
- **Cluster Console** (`/cluster.html`): each node's role, term and commit
  index, refreshed every 2 seconds. **Kill Leader** crashes the current
  leader so you can watch a new one get elected.

## 4. Use the API

```bash
# Register and log in
curl -s -X POST http://localhost:8080/auth/register \
     -H "Content-Type: application/json" \
     -d '{"username":"alice","password":"choose-a-password"}'

TOKEN=$(curl -s -X POST http://localhost:8080/auth/login \
     -H "Content-Type: application/json" \
     -d '{"username":"alice","password":"choose-a-password"}' | jq -r '.token')

# Write, read, delete
curl -i -X POST "http://localhost:8080/api/v1/storage?key=greeting" \
     -H "Authorization: Bearer $TOKEN" -H "Content-Type: text/plain" -d 'hello'

curl -i http://localhost:8080/api/v1/storage/greeting -H "Authorization: Bearer $TOKEN"

curl -i -X DELETE http://localhost:8080/api/v1/storage/greeting -H "Authorization: Bearer $TOKEN"
```

The `-i` flag shows the response headers, including `X-Shard-Id` and
`X-Replicated-Count`. Full details are in the [API reference](./api.md).

## 5. Watch a failover

```bash
python3 aether-engine/scripts/raft_demo.py
```

This writes through the gateway, kills the leader, measures how long the
new election takes, and verifies that every acknowledged write survived.

## Building the engine without Docker

```bash
cd aether-engine
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j4
./wal_v2_test          # unit tests for the write-ahead log
```

On Windows, build inside WSL; the engine uses POSIX sockets and `fsync`.

## Stopping

```bash
docker compose stop        # keeps data
docker compose down -v     # deletes all data volumes
```
