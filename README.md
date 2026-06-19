# AetherDB

AetherDB is a lightweight, high-performance, sharded concurrent key-value database built in C++17. It supports strict durability guarantees (via WAL fsync), graceful signal-driven shutdown, and automated background snapshot compaction.

---

## 🏗️ Design Decisions

### 1. Sharded Locks instead of one Mutex
**Naive Approach:** Protect a single `std::unordered_map` with a single lock.
**Problem:** In C++, inserting/erasing in `std::unordered_map` can trigger a table rehash. If one thread triggers a rehash while another thread is reading a different key on the same map, it leads to memory corruption or crashes.
**Solution:** We shard the keyspace into 16 independent maps, each protected by its own `std::shared_mutex`. Keys hash to a specific shard. Concurrent operations on different shards run in parallel without contention, and rehashes are completely isolated.

### 2. Length-Prefixed Framing instead of Newline-Delimited Text
**Naive Approach:** Write transactions as raw text delimited by newlines or spaces.
**Problem:** The moment a key or value contains a space, carriage return, or newline, parsing fails and corrupts the database state.
**Solution:** We use length-prefixed binary-safe framing for the Write-Ahead Log (WAL) and snapshots:
```
S\n<keylen>\n<key bytes><vallen>\n<value bytes>
```
The parser reads the exact number of bytes specified by the length prefixes, making it 100% binary-safe.

### 3. Log-Before-Apply Ordering
**Naive Approach:** Apply updates to the in-memory map first, then append to the WAL.
**Problem:** If the database crashes after writing to memory but before writing to the log, the client receives an "OK" confirmation, but the data is lost upon server restart.
**Solution:** We write to the WAL, call `fflush()`, and call `fsync()` before updating the in-memory map. If we crash midway, replay reconstructs the state. If we crash before the WAL write, the client never receives "OK", maintaining strict transaction integrity.

### 4. Atomic Rename for Snapshots instead of Overwriting
**Naive Approach:** Write snapshot data directly to `aether.snapshot`.
**Problem:** A sudden crash or power failure while writing the snapshot leaves a half-written, corrupted file, destroying the database backup.
**Solution:** We write the snapshot to a temporary file `aether.snapshot.tmp`, `fsync()` it, and then call POSIX `rename()` to atomically overwrite the real snapshot. This guarantees we always have either the full previous snapshot or the full new snapshot.

### 5. Signal Handlers & Atomic Flags
**Naive Approach:** Stop sockets, take locks, and print logs directly inside the signal handler.
**Problem:** POSIX signal handlers can interrupt a thread mid-operation (e.g., inside `malloc` or while holding `std::cout` locks). Accessing non-reentrant functions or locks inside signal handlers causes deadlocks and undefined behavior.
**Solution:** The signal handler only sets a `std::atomic<bool> shutdown_flag`. A separate watcher thread polls this flag and executes the cleanup and socket shutdowns safely.

---

## 📊 Benchmark Results

Tests were conducted using `scripts/benchmark.py` running 100 concurrent threads executing 10,000 transactions (50% reads, 50% writes) against AetherDB running under WSL (Ubuntu 24.04):

| Metric | Global Mutex (1 shard) | Sharded Locks (16 shards) |
| :--- | :--- | :--- |
| **Elapsed Time** | 13.11 seconds | 12.76 seconds |
| **Throughput** | 762.98 ops/sec | 783.74 ops/sec |
| **Performance Gain**| Reference | **1.03x Speedup** |

### Why is the sharded speedup minimal?
Because AetherDB enforces strict durability by invoking `fsync()` on every mutating write. Disk write syncs are blocking filesystem operations that take orders of magnitude longer than memory lock acquisitions. The database is bottlenecked by disk I/O, meaning lock contention is a minor fraction of the total execution time. If we ran in-memory only without `fsync`, the sharded lock manager would show massive speedups.

---

## 🚀 Build and Run

### Compilation (WSL/Linux only)
```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TSAN=OFF ..
make
```

### Running the server
```bash
./AetherDB --shards 16
```

### Running Benchmarks
```bash
python3 scripts/benchmark.py
```

### Running Durability Demo
```bash
bash scripts/demo.sh
```
