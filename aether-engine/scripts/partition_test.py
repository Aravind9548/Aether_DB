#!/usr/bin/env python3
"""
Network-partition and linearizability tests for the 3-node AetherDB cluster.

The chaos suite (chaos_suite.py) covers crashes: killed leaders, killed
followers, restart storms. This covers the harder case - nodes that stay
up but can't reach each other. Partitions are real packet drops (iptables
rules inside each container, between node IPs), not a simulation inside
the engine. Client traffic still reaches every node, including one cut
off from its peers - that is where stale reads would happen.

Each run uses a throwaway compose project (aetherdb-chaos) with its own
volumes and network, so the real cluster's data is never touched.

Scenarios:
  leader-isolation   Workload running; cut the leader off from both peers
                     for 10s, then heal. Checks the history with Porcupine
                     and that all nodes converge to the same state.
  follower-isolation Cut a follower off for 10s, then heal. The healthy
                     leader must keep its leadership and term (PreVote).
  minority-writes    Send writes to a leader that's been cut off, restart it
                     while still cut off, then heal. Its writes must vanish
                     everywhere, and the restart must not apply them.
  nemesis            45s of workload with a random node isolated every few
                     seconds (Jepsen-style), then the Porcupine check.
  snapshot-catchup   Cut a follower off while the leader writes far past its
                     snapshot threshold, heal, and check the follower is
                     rebuilt via InstallSnapshot. Then restart every node and
                     check nothing changed.

Usage:
    python partition_test.py --scenario leader-isolation
    python partition_test.py --all
    python partition_test.py --all --image aetherdb-engine:legacy   # pre-fix build
"""
import argparse
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR = os.path.dirname(SCRIPTS_DIR)
REPO_DIR = os.path.dirname(ENGINE_DIR)
RESULTS_DIR = os.path.join(REPO_DIR, "chaos-results")
CHECKER_DIR = os.path.join(SCRIPTS_DIR, "linearizability")

PROJECT = "aetherdb-chaos"
NODES = [1, 2, 3]
PORTS = {1: 18081, 2: 18082, 3: 18083}
GO_IMAGE = "golang:1.22"

COMPOSE = ["docker", "compose", "-p", PROJECT,
           "-f", os.path.join(REPO_DIR, "docker-compose.yml"),
           "-f", os.path.join(REPO_DIR, "docker-compose.chaos.yml")]

IMAGE = "aetherdb-engine:dev"
NOT_LEADER_IS_DEFINITE = False  # set per cluster in cluster_up()
# Largest history handed to the linearizability checker. Checking is
# NP-hard; paced runs here produce 4-8k operations and check in milliseconds.
MAX_CHECKABLE_OPS = 20000
failures = []


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(cond, msg):
    if cond:
        log(f"PASS: {msg}")
    else:
        log(f"FAIL: {msg}")
        failures.append(msg)
    return cond


def sh(cmd, timeout=60, env=None, check_rc=False):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    if check_rc and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {r.stderr.strip()}")
    return r


def container(n):
    return f"{PROJECT}-aether-engine-{n}-1"


# ---------- talking to nodes ----------

class Conn:
    """One persistent text-protocol connection to one node. Any timeout or
    error closes it: a late reply on a reused socket would otherwise be
    read as the answer to the next command."""

    def __init__(self, node, timeout=3.0):
        self.node = node
        self.timeout = timeout
        self.sock = None
        self.buf = b""

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.buf = b""

    def cmd(self, line):
        """Returns the reply line, or None on timeout / connection failure."""
        try:
            if self.sock is None:
                self.sock = socket.create_connection(("127.0.0.1", PORTS[self.node]), timeout=self.timeout)
                self.sock.settimeout(self.timeout)
            self.sock.sendall((line + "\n").encode())
            while b"\n" not in self.buf:
                chunk = self.sock.recv(4096)
                if not chunk:
                    raise OSError("closed")
                self.buf += chunk
            reply, self.buf = self.buf.split(b"\n", 1)
            return reply.decode(errors="replace")
        except (OSError, socket.timeout):
            self.close()
            return None


def one_shot(node, line, timeout=3.0):
    c = Conn(node, timeout)
    try:
        return c.cmd(line)
    finally:
        c.close()


def status(node):
    resp = one_shot(node, "STATUS", timeout=2.0)
    if not resp or resp.startswith("ERR"):
        return None
    return dict(p.split("=", 1) for p in resp.split() if "=" in p)


def digest(node):
    resp = one_shot(node, "DIGEST", timeout=5.0)
    if not resp or resp.startswith("ERR"):
        return None
    return dict(p.split("=", 1) for p in resp.split() if "=" in p)


def leader_hint_to_node(hint):
    # Hints look like "aether-engine-2:8080".
    if hint.startswith("aether-engine-"):
        try:
            return int(hint.split(":")[0].rsplit("-", 1)[1])
        except ValueError:
            return None
    return None


def read_definite(key, deadline_s=20):
    """GET that follows leader redirects and retries until it gets a real
    answer (a value or "(nil)"). Returns None if it never does. An error
    reply must never be mistaken for "the key is missing"."""
    end = time.time() + deadline_s
    node = NODES[0]
    while time.time() < end:
        resp = one_shot(node, f"GET {key}")
        if resp is not None and not resp.startswith("ERR"):
            return resp
        hinted = None
        if resp and resp.startswith("ERR NOT_LEADER"):
            parts = resp.split()
            hinted = leader_hint_to_node(parts[2]) if len(parts) > 2 else None
        node = hinted or NODES[(NODES.index(node) + 1) % len(NODES)]
        time.sleep(0.2)
    return None


def find_leader(deadline_s=20, among=NODES):
    end = time.time() + deadline_s
    while time.time() < end:
        leaders = []
        for n in among:
            st = status(n)
            if st and st.get("role") == "LEADER":
                leaders.append((n, st))
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.3)
    return None, None


# ---------- cluster lifecycle ----------

def cluster_up(env_extra=None):
    env = os.environ.copy()
    env["AETHER_ENGINE_IMAGE"] = IMAGE
    if env_extra:
        env.update(env_extra)
    sh(COMPOSE + ["down", "-v", "--remove-orphans"], timeout=120, env=env)
    r = sh(COMPOSE + ["up", "-d", "--no-build", "aether-engine-1", "aether-engine-2", "aether-engine-3"],
           timeout=180, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"compose up failed: {r.stderr}")
    leader, st = find_leader(40)
    if not leader:
        raise RuntimeError("cluster never elected a leader")
    global NOT_LEADER_IS_DEFINITE
    NOT_LEADER_IS_DEFINITE = one_shot(leader, "DIGEST") not in (None, "ERR unknown command")
    log(f"Cluster up ({IMAGE}); leader is node {leader} (term {st['term']})")
    return leader


def cluster_down():
    env = os.environ.copy()
    env["AETHER_ENGINE_IMAGE"] = IMAGE
    sh(COMPOSE + ["down", "-v", "--remove-orphans"], timeout=120, env=env)


_ip_cache = {}


def node_ip(n):
    if n not in _ip_cache:
        r = sh(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container(n)], check_rc=True)
        _ip_cache[n] = r.stdout.strip()
    return _ip_cache[n]


def partition(*groups):
    """Drops all packets between nodes in different groups, in both
    directions, on both ends. Client traffic (from the Docker host) is untouched.

    Each node replaces its rules in ONE exec (flush + re-add). Never flush
    every node first and re-add afterwards: that opens a window where the
    whole cluster is connected, which once let a node that was meant to stay
    cut off sync with the majority mid-test. Because every link is blocked
    from both ends, the other end's rules keep it cut during one node's
    flush-and-re-add."""
    desc = " | ".join(",".join(str(n) for n in g) for g in groups)
    for g in groups:
        for a in g:
            others = [b for h in groups if h is not g for b in h]
            rules = "; ".join(
                f"iptables -A INPUT -s {node_ip(b)} -j DROP; iptables -A OUTPUT -d {node_ip(b)} -j DROP"
                for b in others)
            sh(["docker", "exec", container(a), "sh", "-c", f"iptables -F; {rules}"], check_rc=True)
    log(f"PARTITION  {desc}")


def heal(quiet=False):
    for n in NODES:
        sh(["docker", "exec", container(n), "iptables", "-F"])
    if not quiet:
        log("HEAL       all links restored")


def logs(n):
    return sh(["docker", "logs", container(n)], timeout=30).stdout


# ---------- workload + history ----------

class Workload:
    """Concurrent clients doing reads and writes on a few keys, recording
    every operation with call/return times for the linearizability check.

    Each client starts on a different node and sticks with whatever node it
    is talking to until told NOT_LEADER (then follows the hint) or it fails
    repeatedly. Clients that were talking to a leader when it was cut off
    therefore keep asking it - exactly the reads that must not be stale."""

    def __init__(self, clients=6, keys=5, write_ratio=0.5, pace_s=0.01):
        # pace_s: pause between a client's operations. Linearizability
        # checking is NP-hard; an unpaced client produces tens of thousands
        # of operations a minute, more than the checker can search.
        self.pace_s = pace_s
        self.clients = clients
        self.keys = [f"lin_k{i}" for i in range(keys)]
        self.write_ratio = write_ratio
        self.history = []
        self.lock = threading.Lock()
        self.stop_flag = threading.Event()
        self.threads = []
        self.counts = {"ok_put": 0, "info_put": 0, "rejected_put": 0, "ok_get": 0, "failed_get": 0}
        # Fixed builds reply NOT_LEADER only when nothing was appended (an
        # appended-but-unresolved write is TIMEOUT), so NOT_LEADER is a
        # definite "did not happen". The pre-fix build could also return it
        # after appending, so there it must stay "unknown".
        self.not_leader_is_definite = NOT_LEADER_IS_DEFINITE

    def _client(self, cid):
        rnd = random.Random(cid * 7919 + int(time.time()))
        node = NODES[cid % len(NODES)]
        conns = {n: Conn(n, timeout=3.0) for n in NODES}
        seq = 0
        failures_here = 0
        while not self.stop_flag.is_set():
            key = rnd.choice(self.keys)
            is_put = rnd.random() < self.write_ratio
            if is_put:
                seq += 1
                value = f"c{cid}-{seq}"
                line = f"SET {key} {value}"
            else:
                line = f"GET {key}"
            call = time.monotonic_ns()
            resp = conns[node].cmd(line)
            ret = time.monotonic_ns()

            redirect = None
            if resp is not None and resp.startswith("ERR NOT_LEADER"):
                parts = resp.split()
                redirect = leader_hint_to_node(parts[2]) if len(parts) > 2 else None

            if is_put and self.not_leader_is_definite and resp is not None                     and resp.startswith("ERR NOT_LEADER"):
                with self.lock:
                    self.counts["rejected_put"] += 1
                definite = False
            elif is_put:
                definite = resp == "OK"
                rec = {"client": cid, "kind": "put", "key": key, "value": value, "call": call,
                       "ret": ret if definite else None, "status": "ok" if definite else "info",
                       "node": node, "reply": resp}
                with self.lock:
                    self.history.append(rec)
                    self.counts["ok_put" if definite else "info_put"] += 1
            else:
                definite = resp is not None and not resp.startswith("ERR")
                if definite:
                    rec = {"client": cid, "kind": "get", "key": key,
                           "value": "" if resp == "(nil)" else resp, "call": call, "ret": ret,
                           "status": "ok", "node": node}
                    with self.lock:
                        self.history.append(rec)
                        self.counts["ok_get"] += 1
                else:
                    with self.lock:
                        self.counts["failed_get"] += 1

            if definite:
                failures_here = 0
                time.sleep(self.pace_s)
            elif redirect:
                node = redirect
            else:
                failures_here += 1
                if resp is not None and resp.startswith("ERR NOT_LEADER"):
                    node = NODES[(NODES.index(node) + 1) % len(NODES)]
                    failures_here = 0
                elif failures_here >= 3:
                    node = NODES[(NODES.index(node) + 1) % len(NODES)]
                    failures_here = 0
                time.sleep(0.05)
        for c in conns.values():
            c.close()

    def start(self):
        for cid in range(self.clients):
            t = threading.Thread(target=self._client, args=(cid,), daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self.stop_flag.set()
        for t in self.threads:
            t.join(timeout=10)


def stale_read_examples(history, limit=3):
    """A quick, conservative scan for the classic stale read: a read returns
    value v even though a later write w2 to the same key had already
    completed before the read began, and v's own write finished before w2
    started. (Values are unique, so v identifies its write.) Porcupine does
    the complete check; this just puts a concrete example on screen."""
    writes = {}
    for op in history:
        if op["kind"] == "put":
            writes[(op["key"], op["value"])] = op
    ok_writes = [op for op in history if op["kind"] == "put" and op["status"] == "ok"]
    found = []
    for r in history:
        if r["kind"] != "get" or r["value"] == "":
            continue
        w1 = writes.get((r["key"], r["value"]))
        if not w1 or w1["status"] != "ok":
            continue
        for w2 in ok_writes:
            if (w2["key"] == r["key"] and w2 is not w1 and w2["call"] > w1["ret"]
                    and w2["ret"] < r["call"]):
                found.append((r, w1, w2))
                break
        if len(found) >= limit:
            break
    return found


def check_linearizable(history, name):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    tag = IMAGE.split(":")[-1]
    hist_path = os.path.join(RESULTS_DIR, f"{name}-{tag}-history.json")
    viz_name = f"{name}-{tag}-porcupine.html"
    with open(hist_path, "w") as f:
        json.dump(history, f)
    recorded = len(history)

    # Hard bound on what we hand the checker. Pacing keeps normal runs far
    # below it; anything larger is refused up front rather than risking a
    # checker that runs out of memory partway and leaves no verdict.
    if recorded > MAX_CHECKABLE_OPS:
        check(False, f"{name}: history has {recorded} operations, over the {MAX_CHECKABLE_OPS} "
                     f"checkable limit - no verdict (shorten the run or pace the clients)")
        return False

    examples = stale_read_examples(history)
    t0 = min(op["call"] for op in history) if history else 0
    for r, w1, w2 in examples:
        log(f"  stale read: client {r['client']} read {r['key']}={r['value']!r} from node {r['node']} "
            f"at +{(r['call'] - t0) / 1e9:.3f}s, but put({w2['value']!r}) had completed at "
            f"+{(w2['ret'] - t0) / 1e9:.3f}s (after put({w1['value']!r}) finished at "
            f"+{(w1['ret'] - t0) / 1e9:.3f}s)")

    # Memory-capped and named: an exhaustive search on a large history can
    # otherwise eat the whole Docker VM, and a timed-out `docker run` client
    # leaves its container running unless we kill it by name. Not --rm: the
    # container's own exit code and OOMKilled flag are read back afterwards,
    # so a verdict is only accepted from a checker that provably finished.
    checker_name = f"aetherdb-lincheck-{os.getpid()}"
    sh(["docker", "rm", "-f", checker_name])
    cmd = ["docker", "run", "--name", checker_name, "--memory", "2g",
            "-v", f"{CHECKER_DIR}:/src", "-v", f"{RESULTS_DIR}:/data",
            "-v", "aetherdb-gomod:/go/pkg/mod", "-v", "aetherdb-gocache:/root/.cache/go-build",
            "-w", "/src", GO_IMAGE, "go", "run", ".",
            f"/data/{os.path.basename(hist_path)}", f"/data/{viz_name}"]
    try:
        r = sh(cmd, timeout=1500)
    except subprocess.TimeoutExpired:
        sh(["docker", "rm", "-f", checker_name])
        check(False, f"{name}: Porcupine gave no verdict within 25 minutes")
        return False
    out = (r.stdout + r.stderr).strip()
    for line in out.splitlines():
        log(f"  porcupine: {line}")
    st = sh(["docker", "inspect", "-f", "{{.State.ExitCode}} {{.State.OOMKilled}}", checker_name]).stdout.split()
    sh(["docker", "rm", "-f", checker_name])
    log(f"  checker container: exit={st[0] if st else '?'} OOMKilled={st[1] if len(st) > 1 else '?'} (2 GB limit)")
    if not st or st[1] != "false" or "per-key:" not in out:
        check(False, f"{name}: checker did not finish cleanly (container state {st}) - no verdict")
        return False

    # Completion, not just "no error": the checker must have read every
    # operation we recorded, and a pass must cover every one of them.
    m_read = re.search(r"history: (\d+) operations", out)
    m_cov = re.search(r"per-key: (\d+)/(\d+) keys verified, (\d+)/(\d+) operations covered", out)
    read_n = int(m_read.group(1)) if m_read else -1
    covered, total = (int(m_cov.group(3)), int(m_cov.group(4))) if m_cov else (-1, -1)
    log(f"  completeness: recorded {recorded} ops | checker read {read_n} | covered {covered}/{total}")
    if read_n != recorded or total != recorded:
        check(False, f"{name}: checker saw {read_n}/{total} operations but {recorded} were recorded - no verdict")
        return False
    if r.returncode == 0 and covered != recorded:
        check(False, f"{name}: exit 0 but only {covered}/{recorded} operations verified - no verdict")
        return False
    if r.returncode not in (0, 1):
        # 2 = search timed out; anything else = the checker itself died
        # (e.g. out of memory). Either way: no verdict, not a violation.
        check(False, f"{name}: Porcupine gave no verdict (rc={r.returncode}) - history too large or checker crashed")
        return False
    return check(r.returncode == 0,
                 f"{name}: history is linearizable (Porcupine; visualization: chaos-results/{viz_name})")


def wait_converged(deadline_s=30):
    """All nodes report the same applied index and state hash."""
    end = time.time() + deadline_s
    last = None
    while time.time() < end:
        ds = {n: digest(n) for n in NODES}
        if any(d is None for d in ds.values()):
            last = ds
            time.sleep(0.5)
            continue
        if len({(d["applied"], d["hash"]) for d in ds.values()}) == 1:
            return True, ds
        last = ds
        time.sleep(0.5)
    return False, last


def check_convergence(name):
    if one_shot(NODES[0], "DIGEST") == "ERR unknown command":
        log(f"  {name}: DIGEST not supported by this build - convergence check skipped")
        return
    ok, ds = wait_converged()
    check(ok, f"{name}: all nodes converged to the same applied state {ds}")


# ---------- scenarios ----------

def scenario_leader_isolation():
    log("=== leader-isolation: cut the leader off mid-workload, then heal ===")
    leader = cluster_up()
    w = Workload()
    w.start()
    time.sleep(3)
    others = [n for n in NODES if n != leader]
    partition([leader], others)
    time.sleep(10)
    st_old = status(leader)
    new_leader, st_new = find_leader(5, among=others)
    log(f"  during partition: old leader node {leader} says role={st_old and st_old.get('role')}; "
        f"majority side leader = node {new_leader}")
    heal()
    time.sleep(5)
    w.stop()
    log(f"  workload: {w.counts}")
    check_linearizable(w.history, "leader-isolation")
    check_convergence("leader-isolation")


def scenario_follower_isolation():
    log("=== follower-isolation: cut a follower off, heal, leader must be undisturbed ===")
    leader = cluster_up()
    time.sleep(2)
    st0 = status(leader)
    follower = [n for n in NODES if n != leader][0]
    log(f"  before: leader node {leader}, term {st0['term']}; isolating follower node {follower}")

    stop = threading.Event()

    def trickle():  # light write load so the cluster is doing real work
        c, i = Conn(leader), 0
        while not stop.is_set():
            c.cmd(f"SET trickle {i}")
            i += 1
            time.sleep(0.1)
        c.close()

    t = threading.Thread(target=trickle, daemon=True)
    t.start()
    partition([follower], [n for n in NODES if n != follower])
    terms = []
    for _ in range(10):
        time.sleep(1)
        st = status(follower)
        terms.append(st["term"] if st else "?")
    log(f"  isolated follower's term, sampled each second while cut off: {terms}")
    heal()
    time.sleep(5)
    stop.set()
    t.join()
    st1 = status(leader)
    log(f"  after heal: node {leader} role={st1 and st1.get('role')} term={st1 and st1.get('term')}")
    check(len(set(terms)) == 1 and terms[0] == st0["term"],
          f"follower-isolation: isolated follower's term stayed at {st0['term']} (no term inflation)")
    check(st1 is not None and st1.get("role") == "LEADER" and st1.get("term") == st0["term"],
          f"follower-isolation: node {leader} is still leader in term {st0['term']} after the heal")
    check_convergence("follower-isolation")


def scenario_minority_writes():
    log("=== minority-writes: writes to a cut-off leader must vanish; restart must not apply them ===")
    leader = cluster_up()
    good = Conn(leader)
    for i in range(20):
        good.cmd(f"SET before_{i} v{i}")
    good.close()
    others = [n for n in NODES if n != leader]
    partition([leader], others)

    replies = []
    iso = Conn(leader, timeout=4.0)
    for i in range(5):
        replies.append(iso.cmd(f"SET minority_{i} lost"))
    iso.close()
    log(f"  replies from the cut-off old leader to 5 writes: {replies}")
    check(all(r != "OK" for r in replies), "minority-writes: no write was acknowledged by the minority side")

    new_leader, _ = find_leader(10, among=others)
    acked = []
    c = Conn(new_leader)
    for i in range(20):
        if c.cmd(f"SET majority_{i} kept") == "OK":
            acked.append(f"majority_{i}")
    c.close()
    log(f"  majority side (leader node {new_leader}) acknowledged {len(acked)}/20 writes")

    # Restart (SIGKILL) the old leader while it is still cut off. Its own
    # iptables rules are lost with the restart, but the peers' rules (which
    # drop its IP in both directions) keep it isolated meanwhile.
    st_before = status(leader)
    ip_before = node_ip(leader)
    sh(["docker", "restart", "-t", "0", container(leader)], timeout=60)
    _ip_cache.pop(leader, None)
    if node_ip(leader) != ip_before:
        raise RuntimeError(f"node {leader} changed IP on restart; partition no longer holds")
    partition([leader], others)
    st_after = None
    for _ in range(20):
        st_after = status(leader)
        if st_after:
            break
        time.sleep(0.5)
    log(f"  cut-off node {leader} before restart: commit={st_before and st_before.get('commit')} "
        f"last_log={st_before and st_before.get('last_log')}; after restart: "
        f"commit={st_after and st_after.get('commit')} last_log={st_after and st_after.get('last_log')}")
    check(st_after is not None and st_before is not None and
          int(st_after["commit"]) <= int(st_before["commit"]),
          "minority-writes: restart did not mark the cut-off node's uncommitted writes as committed")

    heal()
    find_leader(15)
    time.sleep(3)
    reads = {k: read_definite(k) for k in [f"minority_{i}" for i in range(5)] + acked}
    undecided = [k for k, v in reads.items() if v is None]
    leaked = [k for k, v in reads.items() if k.startswith("minority_") and v not in (None, "(nil)")]
    missing = [k for k in acked if reads[k] not in (None, "kept")]
    if undecided:
        log(f"  could not get a definite read for {len(undecided)} keys: {undecided}")
    check(not undecided, "minority-writes: every post-heal read got a definite answer")
    check(not leaked, f"minority-writes: none of the minority's writes survived the heal (leaked: {leaked})")
    check(not missing, f"minority-writes: every majority-acknowledged write survived (missing: {missing})")
    check_convergence("minority-writes")


def scenario_nemesis(duration_s=45):
    log(f"=== nemesis: {duration_s}s workload with random partitions ===")
    cluster_up()
    w = Workload(clients=6, keys=8, pace_s=0.03)
    w.start()
    end = time.time() + duration_s
    rnd = random.Random()
    while time.time() < end:
        time.sleep(2)
        victim = rnd.choice(NODES)
        partition([victim], [n for n in NODES if n != victim])
        time.sleep(rnd.uniform(3, 6))
        heal()
    heal()
    time.sleep(4)
    w.stop()
    log(f"  workload: {w.counts}")
    check_linearizable(w.history, "nemesis")
    check_convergence("nemesis")


def scenario_snapshot_catchup(writes=1500):
    log("=== snapshot-catchup: follower falls behind the leader's compacted log ===")
    leader = cluster_up({"AETHER_SNAPSHOT_THRESHOLD": "200"})
    follower = [n for n in NODES if n != leader][0]
    partition([follower], [n for n in NODES if n != follower])

    acked = []
    lock = threading.Lock()

    def writer(wid):
        c = Conn(leader)
        for i in range(wid, writes, 8):
            if c.cmd(f"SET snap_{i} val_{i}") == "OK":
                with lock:
                    acked.append(i)
        c.close()

    ts = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    st_l = status(leader)
    st_f = status(follower)
    log(f"  {len(acked)}/{writes} writes acknowledged while node {follower} was cut off")
    log(f"  leader: snapshot={st_l.get('snapshot')} log_entries={st_l.get('log_entries')} "
        f"commit={st_l['commit']}; isolated follower: last_log={st_f and st_f.get('last_log')}")
    check(st_f is not None and int(st_l.get("snapshot", 0)) > int(st_f["last_log"]),
          "snapshot-catchup: leader compacted past everything the isolated follower has")

    heal()
    ok, ds = wait_converged(45)
    check(ok, f"snapshot-catchup: follower caught up and converged {ds}")
    installed = "installed snapshot" in logs(follower)
    check(installed, f"snapshot-catchup: node {follower} was rebuilt via InstallSnapshot (log line present)")
    for line in logs(follower).splitlines():
        if "installed snapshot" in line:
            log(f"  node {follower}: {line.strip()}")
            break

    sizes = {}
    for n in NODES:
        r = sh(["docker", "exec", container(n), "sh", "-c", "wc -c < /data/aether.wal"])
        sizes[n] = int(r.stdout.strip() or 0)
    st_all = {n: status(n) for n in NODES}
    log(f"  WAL bytes per node after {len(acked)} writes: {sizes}; log entries kept: "
        f"{ {n: s and s.get('log_entries') for n, s in st_all.items()} }")
    check(all(st and int(st["log_entries"]) <= 2 * 200 + 300 for st in st_all.values()),
          "snapshot-catchup: every node's in-memory log is bounded by the snapshot threshold")

    log("  restarting all three nodes (recovery from snapshot + WAL tail)...")
    before = {n: digest(n) for n in NODES}
    sh(["docker", "restart", "-t", "0", container(1), container(2), container(3)], timeout=120)
    nl, _ = find_leader(40)
    # After a restart only the snapshot is known committed; the rest is
    # re-committed once the new leader's no-op commits. Wait for that.
    ok, after = wait_converged(45)
    check(ok and int(after[1]["applied"]) >= int(before[1]["applied"]) and
          {d["hash"] for d in after.values()} == {before[1]["hash"]},
          f"snapshot-catchup: state identical after full-cluster restart (before {before[1]}, after {after and after[1]})")
    sample = random.sample(sorted(acked), min(50, len(acked)))
    wrong = [i for i in sample if read_definite(f"snap_{i}") != f"val_{i}"]
    check(not wrong, f"snapshot-catchup: 50 sampled acknowledged writes readable after restart (wrong: {wrong})")


SCENARIOS = {
    "leader-isolation": scenario_leader_isolation,
    "follower-isolation": scenario_follower_isolation,
    "minority-writes": scenario_minority_writes,
    "nemesis": scenario_nemesis,
    "snapshot-catchup": scenario_snapshot_catchup,
}


def main():
    global IMAGE
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=sorted(SCENARIOS))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--image", default="aetherdb-engine:dev",
                    help="engine image to test (build it first: docker build -t aetherdb-engine:dev aether-engine)")
    ap.add_argument("--keep", action="store_true", help="leave the test cluster running afterwards")
    args = ap.parse_args()
    IMAGE = args.image
    names = sorted(SCENARIOS) if args.all else [args.scenario] if args.scenario else None
    if not names:
        ap.error("pass --scenario NAME or --all")

    order = ["leader-isolation", "follower-isolation", "minority-writes", "snapshot-catchup", "nemesis"]
    names = [n for n in order if n in names]
    try:
        for name in names:
            _ip_cache.clear()
            before = len(failures)
            try:
                SCENARIOS[name]()
            except Exception as e:  # report and continue with the next scenario
                check(False, f"{name}: aborted with {type(e).__name__}: {e}")
            log(f"--- {name}: {'PASS' if len(failures) == before else 'FAIL'} ---\n")
    finally:
        if not args.keep:
            cluster_down()

    if failures:
        print(f"\n{len(failures)} check(s) FAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nALL PARTITION SCENARIOS PASSED")


if __name__ == "__main__":
    main()
