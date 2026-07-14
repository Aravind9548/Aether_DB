#!/usr/bin/env python3
"""Raft safety verification:
1. Kill leader mid-load: every OK-acknowledged SET must survive on the new leader.
2. Follower catch-up: a restarted follower's WAL must converge to the leader's log.
3. Concurrent writers: same mid-load leader-kill safety property, but with
   several threads hammering the leader simultaneously instead of one
   sequential writer. This exercises request ordering/serialization under
   real contention; it is not a different safety guarantee than Phase 1
   (majority-commit-before-ack is what actually prevents loss either way),
   but it rules out any concurrency bug specific to overlapping in-flight
   proposals racing a leader crash.
"""
import subprocess, time, socket, shutil, os, signal, threading

BIN = "./build/AetherDB"
PORTS = [8080, 8081, 8082]
DIRS = {8080: "node1", 8081: "node2", 8082: "node3"}
procs = {}

def start_node(port):
    others = ",".join(f"{PORTS.index(p)+1}=127.0.0.1:{p}" for p in PORTS if p != port)
    procs[port] = subprocess.Popen(
        [BIN, "--port", str(port), "--node-id", str(PORTS.index(port)+1),
         "--peers", others, "--data-dir", DIRS[port]],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def send(port, cmd, timeout=2.0):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        s.settimeout(timeout)
        s.sendall((cmd + "\n").encode())
        res = s.recv(4096).decode()
        s.close()
        return res.strip()
    except Exception as e:
        return f"ERR conn {e}"

def find_leader(deadline_s=10):
    end = time.time() + deadline_s
    while time.time() < end:
        for p in PORTS:
            if procs.get(p) and procs[p].poll() is None:
                r = send(p, "GET __probe__")
                if not r.startswith("ERR"):
                    return p
        time.sleep(0.2)
    return None

def robust_set(key, val, deadline_s=10):
    """SET with redirect/retry. Returns True only on an explicit OK."""
    end = time.time() + deadline_s
    while time.time() < end:
        p = find_leader(deadline_s=max(0.5, end - time.time()))
        if p is None:
            continue
        r = send(p, f"SET {key} {val}")
        if r == "OK":
            return True
        time.sleep(0.1)
    return False

def wal_keys(datadir):
    """Parse a v2 WAL and return the set of keys with a final SET."""
    state = {}
    path = os.path.join(datadir, "aether.wal")
    with open(path, "rb") as f:
        data = f.read()
    assert data[:3] == b"R2\n", "not a v2 wal"
    i = 3
    def line():
        nonlocal i
        j = data.index(b"\n", i); s = data[i:j]; i = j + 1; return s
    while i < len(data):
        op = line().decode()
        term = int(line()); idx = int(line())
        klen = int(line()); key = data[i:i+klen].decode(); i += klen
        if op == "S":
            vlen = int(line()); val = data[i:i+vlen].decode(); i += vlen
            state[key] = val
        elif op == "D":
            state.pop(key, None)
    return state

for d in DIRS.values():
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
os.makedirs("build", exist_ok=True)

import atexit
def _cleanup():
    for pr in procs.values():
        if pr.poll() is None:
            pr.kill()
    for pr in procs.values():
        try: pr.wait(timeout=5)
        except Exception: pass
atexit.register(_cleanup)

print("=== Phase 1: kill leader mid-load, verify no acknowledged loss ===")
for p in PORTS: start_node(p)
leader = find_leader()
assert leader, "no leader elected"
print(f"leader: {leader}")

acked = []
killed = False
for n in range(120):
    key = f"k{n}"
    if robust_set(key, f"v{n}", deadline_s=15):
        acked.append(key)
    else:
        print(f"WARN: SET {key} never acknowledged (allowed, but noted)")
    if n == 40 and not killed:
        print(f"kill -9 leader on port {leader} mid-load")
        procs[leader].send_signal(signal.SIGKILL)
        procs[leader].wait()
        killed = True

new_leader = find_leader()
assert new_leader and new_leader != leader, "no failover leader"
print(f"new leader: {new_leader}, acknowledged writes: {len(acked)}")

lost = [k for k in acked if send(new_leader, f"GET {k}") != "v" + k[1:]]
assert not lost, f"LOST ACKNOWLEDGED WRITES: {lost}"
print("PASS: all acknowledged writes survived leader kill")

print("\n=== Phase 2: follower catch-up after restart ===")
print(f"restarting old leader {leader} so the cluster is back to 3 nodes")
start_node(leader)
time.sleep(3)
followers = [p for p in PORTS if p != new_leader and procs[p].poll() is None]
f_port = followers[0]
print(f"killing follower {f_port}")
procs[f_port].send_signal(signal.SIGKILL); procs[f_port].wait()

for n in range(120, 160):
    assert robust_set(f"k{n}", f"v{n}", deadline_s=15), f"SET k{n} failed"
    acked.append(f"k{n}")
print("wrote k120..k159 while follower down; restarting follower")
start_node(f_port)
time.sleep(5)

follower_state = wal_keys(DIRS[f_port])
missing = [k for k in acked if follower_state.get(k) != "v" + k[1:]]
assert not missing, f"follower missing/mismatched acked keys: {missing[:10]}"
print(f"PASS: follower converged ({len(acked)} acked keys present in its WAL)")

print("\n=== Phase 3: concurrent writers, kill leader mid-load ===")
# Cluster is back to 3 healthy nodes after phase 2 (old leader + follower both
# restarted above). Each of N_WRITERS threads owns a disjoint key range, so
# "was this key acknowledged" stays unambiguous even with interleaved writes.
N_WRITERS = 8
WRITES_PER_WRITER = 25
concurrent_acked = []
concurrent_lock = threading.Lock()

def writer_thread(writer_id):
    local_acked = []
    for i in range(WRITES_PER_WRITER):
        key = f"cw{writer_id}_{i}"
        val = f"val_{writer_id}_{i}"
        if robust_set(key, val, deadline_s=20):
            local_acked.append((key, val))
    with concurrent_lock:
        concurrent_acked.extend(local_acked)

leader = find_leader()
assert leader, "no leader before concurrent phase"
print(f"leader before concurrent load: {leader}")

threads = [threading.Thread(target=writer_thread, args=(i,)) for i in range(N_WRITERS)]
for t in threads:
    t.start()

# Let real concurrent write traffic build up, then kill the leader mid-flight
# while all N_WRITERS threads have in-flight or soon-to-be-sent proposals.
time.sleep(0.8)
print(f"kill -9 leader on port {leader} with {N_WRITERS} writers mid-flight")
procs[leader].send_signal(signal.SIGKILL)
procs[leader].wait()

for t in threads:
    t.join(timeout=60)
    assert not t.is_alive(), "writer thread did not finish after leader kill + failover"

print(f"all writer threads finished; {len(concurrent_acked)}/{N_WRITERS * WRITES_PER_WRITER} writes acknowledged")

new_leader = find_leader()
assert new_leader, "no leader elected after concurrent-phase kill"
lost = [(k, v) for k, v in concurrent_acked if send(new_leader, f"GET {k}") != v]
assert not lost, f"LOST ACKNOWLEDGED CONCURRENT WRITES: {lost[:10]}"
print(f"PASS: all {len(concurrent_acked)} concurrently-acknowledged writes survived leader kill")

print("\nALL SAFETY TESTS PASSED")
for p in PORTS:
    if procs.get(p) and procs[p].poll() is None:
        procs[p].terminate()
for p in PORTS:
    if not procs.get(p):
        continue
    try:
        procs[p].wait(timeout=5)
    except subprocess.TimeoutExpired:
        procs[p].kill()
        procs[p].wait(timeout=5)
for d in DIRS.values(): shutil.rmtree(d, ignore_errors=True)
