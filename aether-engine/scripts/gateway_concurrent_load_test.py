#!/usr/bin/env python3
"""
Concurrent load test for the gateway's client-facing HTTP API, exercising
AetherEngineClient (the new gRPC-based gateway->engine client added in
Phase B) under genuinely concurrent multi-client load.

This is distinct from the already-proven Raft-layer concurrent-writer test
(raft_safety_test.py Phase 3): that test proves Raft's own safety property
(majority-commit-before-ack) survives a leader crash under concurrent
proposals. This test instead checks whether AetherEngineClient's
channel-per-node approach - which replaced the old hand-rolled
Socket/PooledConn connection pool - correctly handles many real, concurrent
HTTP client requests hitting the SAME gateway process at once, with no
lost/corrupted writes and no request-routing cross-talk between threads.
Each worker thread uses its own persistent keep-alive HTTP connection (not
a new curl/process per request), so the measured throughput reflects the
gateway + gRPC + engine path, not process-spawn overhead.

Usage: python3 gateway_concurrent_load_test.py [--threads N] [--ops-per-thread N]
"""
import argparse
import http.client
import json
import sys
import threading
import time

GATEWAY_HOST = "localhost"
GATEWAY_PORT = 8080


def register_and_login(username, password):
    body = json.dumps({"username": username, "password": password})

    conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=10)
    conn.request("POST", "/auth/register", body=body, headers={"Content-Type": "application/json"})
    conn.getresponse().read()
    conn.close()

    conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=10)
    conn.request("POST", "/auth/login", body=body, headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    conn.close()
    return data.get("token")


class WorkerResult:
    def __init__(self):
        self.ops = 0
        self.errors = []


def worker(worker_id, ops_per_thread, token, results, lock):
    conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=15)
    r = WorkerResult()
    headers_set = {"Authorization": f"Bearer {token}", "Content-Type": "text/plain"}
    headers_get = {"Authorization": f"Bearer {token}"}
    for i in range(ops_per_thread):
        key = f"loadtest_w{worker_id}_{i}"
        val = f"val_{worker_id}_{i}"
        try:
            conn.request("POST", f"/api/v1/storage?key={key}", body=val, headers=headers_set)
            resp = conn.getresponse()
            body = resp.read().decode()
            if resp.status != 200 or body.strip() != "OK":
                r.errors.append(f"SET {key} failed: status={resp.status} body={body!r}")
                continue

            conn.request("GET", f"/api/v1/storage/{key}", headers=headers_get)
            resp = conn.getresponse()
            body = resp.read().decode()
            if resp.status != 200 or body != val:
                r.errors.append(f"GET {key} mismatch: status={resp.status} body={body!r} expected={val!r}")
                continue

            r.ops += 2  # one SET + one GET
        except Exception as e:
            r.errors.append(f"worker {worker_id} op {i} exception: {e}")
    conn.close()
    with lock:
        results.append(r)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--threads", type=int, default=20)
    ap.add_argument("--ops-per-thread", type=int, default=25)
    args = ap.parse_args()

    username = f"loadtest{int(time.time())}@test.com"
    password = "Passw0rd!123"
    print(f"Registering + logging in as {username}...")
    token = register_and_login(username, password)
    if not token:
        print("FAIL: could not obtain auth token")
        sys.exit(1)
    print("Authenticated.")

    total_planned = args.threads * args.ops_per_thread * 2
    print(f"\nLaunching {args.threads} concurrent clients, {args.ops_per_thread} SET+GET pairs "
          f"each ({total_planned} total ops), each on its own persistent HTTP connection...")

    results = []
    lock = threading.Lock()
    threads = []
    start = time.perf_counter()
    for w in range(args.threads):
        t = threading.Thread(target=worker, args=(w, args.ops_per_thread, token, results, lock))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    total_ops = sum(r.ops for r in results)
    all_errors = [e for r in results for e in r.errors]

    print("\n=== Results ===")
    print(f"Elapsed: {elapsed:.3f}s")
    print(f"Total successful ops (SET+GET combined): {total_ops}/{total_planned}")
    print(f"Throughput: {total_ops / elapsed:.1f} ops/sec")
    print(f"Errors: {len(all_errors)}")
    for e in all_errors[:20]:
        print(f"  {e}")

    if all_errors:
        print("\nFAIL: concurrent load test had errors or value mismatches")
        sys.exit(1)
    print("\nPASS: all concurrent client requests succeeded with correct values, zero errors")


if __name__ == "__main__":
    main()
