#!/usr/bin/env python3
"""
Live Raft failover demo against the Docker Compose cluster (3 engine nodes +
gateway) — the scripted deliverable named in the improvement plan as "the
artifact you show in an interview." Repeatable, no manual steps.

Flow:
  1. Print each engine node's STATUS (role, term, commitIndex).
  2. Write N keys THROUGH THE GATEWAY (authenticated HTTP, the real client path).
  3. Record a precise timestamp, `docker kill` the current leader container.
  4. Poll STATUS on the surviving nodes until a new leader is confirmed;
     print the measured election-to-recovery delta in milliseconds.
  5. Verify every previously-acknowledged key is still correct via the gateway.
  6. Write M more keys via the gateway WHILE the old leader is still down.
  7. `docker start` the old leader; poll until it rejoins.
  8. Verify the rejoined node's own WAL (read directly via `docker exec cat`,
     since a follower does not serve GET) now has all N+M acknowledged keys,
     including the ones written during its downtime.
  9. Print a clear PASS/FAIL summary.

Usage:
    cd aether-engine/scripts
    python3 raft_demo.py [--compose-project atherdb_c_pro] [--num-keys 30] [--downtime-keys 15]
"""
import argparse
import json
import subprocess
import sys
import time

ENGINE_SERVICES = ["aether-engine-1", "aether-engine-2", "aether-engine-3"]
CLIENT_PORT = 8080
GATEWAY_URL = "http://localhost:8080"
WAL_PATH_IN_CONTAINER = "/data/aether.wal"


def tenant_key(tenant_id, user_key):
    """Mirrors StorageService.buildIsolatedKey: len(tenantId):tenantId:userKey.
    The gateway stores every key under this encoding to defend against
    tenant-prefix injection, so a direct WAL read must reproduce it to find
    the same record the gateway itself would look up."""
    return f"{len(tenant_id)}:{tenant_id}:{user_key}"


def sh(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def sh_bytes(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, timeout=timeout)


def container_name(project, service):
    r = sh(["docker", "compose", "-p", project, "ps", "-q", service])
    cid = r.stdout.strip()
    if not cid:
        raise RuntimeError(f"service {service} not found (is `docker compose up -d` running?)")
    r2 = sh(["docker", "inspect", "-f", "{{.Name}}", cid])
    return r2.stdout.strip().lstrip("/")


# ---------- direct engine access (STATUS + docker kill/start target only) ----------

def send_engine_cmd(container, cmd, timeout=3.0):
    """Runs a raw TCP command against a container's client port via docker exec,
    using /dev/tcp (bash builtin) so no extra tooling is needed inside the image.
    Used ONLY for STATUS/leader-discovery — actual data writes go through the
    gateway, per the real client path."""
    script = (
        f'exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; '
        f'printf "{cmd}\\n" >&3; '
        f'timeout {timeout} head -1 <&3'
    )
    try:
        r = sh(["docker", "exec", container, "bash", "-c", script], timeout=timeout + 2)
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""


def get_status(container):
    resp = send_engine_cmd(container, "STATUS")
    if not resp or resp.startswith("ERR"):
        return None
    fields = {}
    for part in resp.split():
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    return fields or None


def print_all_status(containers, label):
    print(f"--- STATUS ({label}) ---")
    for c in containers:
        st = get_status(c)
        if st:
            print(f"  {c}: role={st.get('role')} term={st.get('term')} commit={st.get('commit')}")
        else:
            print(f"  {c}: unreachable")


def find_leader(containers, deadline_s=10):
    end = time.time() + deadline_s
    while time.time() < end:
        for c in containers:
            st = get_status(c)
            if st and st.get("role") == "LEADER":
                return c, st
        time.sleep(0.2)
    return None, None


def wait_for_new_leader(candidates, deadline_s=15):
    """Polls the surviving containers until one reports LEADER.
    Returns (container, perf_counter_timestamp_of_confirmation)."""
    end = time.time() + deadline_s
    while time.time() < end:
        for c in candidates:
            st = get_status(c)
            if st and st.get("role") == "LEADER":
                return c, time.perf_counter()
        time.sleep(0.02)
    return None, time.perf_counter()


def wait_for_rejoin(container, deadline_s=15):
    end = time.time() + deadline_s
    while time.time() < end:
        st = get_status(container)
        if st and st.get("role") in ("FOLLOWER", "LEADER"):
            return st
        time.sleep(0.3)
    return None


# ---------- gateway HTTP client (the real path data actually travels) ----------

def _post_json(path, payload, retries=3):
    """POSTs JSON with a short retry loop: /auth endpoints are rate-limited
    (5 req/60s per IP), so a demo run shortly after manual testing can get a
    transient 429 — worth riding out rather than failing the whole demo."""
    for attempt in range(retries):
        r = sh(["curl", "-s", "-m", "10", "-w", "\n%{http_code}",
                "-X", "POST", f"{GATEWAY_URL}{path}",
                "-H", "Content-Type: application/json",
                "-d", json.dumps(payload)])
        lines = r.stdout.rsplit("\n", 1)
        body, code = (lines[0], lines[1]) if len(lines) == 2 else (r.stdout, "")
        if code == "429":
            wait_s = 65
            print(f"  rate-limited on {path}, waiting {wait_s}s for the bucket to refill...")
            time.sleep(wait_s)
            continue
        return code, body
    return code, body


def gateway_register(username, password):
    _post_json("/auth/register", {"username": username, "password": password})


def gateway_login(username, password):
    code, body = _post_json("/auth/login", {"username": username, "password": password})
    if code != "200":
        return None
    try:
        return json.loads(body).get("token")
    except Exception:
        return None


def gateway_set(token, key, val, timeout=15):
    r = sh(["curl", "-s", "-m", str(timeout), "-o", "/tmp/raft_demo_body", "-w", "%{http_code}",
            "-X", "POST", f"{GATEWAY_URL}/api/v1/storage?key={key}",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: text/plain", "-d", val], timeout=timeout + 3)
    if r.stdout.strip() != "200":
        return False
    with open("/tmp/raft_demo_body") as f:
        return f.read().strip() == "OK"


def gateway_get(token, key, timeout=15):
    r = sh(["curl", "-s", "-m", str(timeout), "-o", "/tmp/raft_demo_body", "-w", "%{http_code}",
            f"{GATEWAY_URL}/api/v1/storage/{key}",
            "-H", f"Authorization: Bearer {token}"], timeout=timeout + 3)
    if r.stdout.strip() != "200":
        return None
    with open("/tmp/raft_demo_body") as f:
        return f.read()


def write_keys_via_gateway(token, keys_vals, label):
    acked = []
    for key, val in keys_vals:
        if gateway_set(token, key, val):
            acked.append((key, val))
        else:
            print(f"  WARN: SET {key} via gateway not acknowledged ({label})")
    return acked


# ---------- WAL read (only way to check a follower's data; it doesn't serve GET) ----------

def read_wal_final_state(container, path=WAL_PATH_IN_CONTAINER):
    r = sh_bytes(["docker", "exec", container, "cat", path])
    data = r.stdout
    if data[:3] != b"R2\n":
        raise RuntimeError(f"{container}:{path} is not a v2 WAL (got {data[:20]!r})")
    state = {}
    i = 3

    def line():
        nonlocal i
        j = data.index(b"\n", i)
        s = data[i:j]
        i = j + 1
        return s

    while i < len(data):
        op = line().decode()
        line(); line()  # term, index (unused here)
        klen = int(line())
        key = data[i:i + klen].decode(); i += klen
        if op == "S":
            vlen = int(line())
            val = data[i:i + vlen].decode(); i += vlen
            state[key] = val
        elif op == "D":
            state.pop(key, None)
    return state


def fail(msg):
    print(f"\nFAIL: {msg}")
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--compose-project", default="atherdb_c_pro")
    ap.add_argument("--num-keys", type=int, default=30,
                     help="keys written before the kill (default 30)")
    ap.add_argument("--downtime-keys", type=int, default=15,
                     help="keys written while the old leader is down (default 15)")
    args = ap.parse_args()

    print(f"Resolving containers for compose project '{args.compose_project}'...")
    containers = [container_name(args.compose_project, svc) for svc in ENGINE_SERVICES]
    for svc, c in zip(ENGINE_SERVICES, containers):
        print(f"  {svc} -> {c}")

    print()
    print_all_status(containers, "initial")

    username = f"raftdemo{int(time.time())}@test.com"
    password = "Passw0rd!123"
    print(f"\nRegistering + logging in as {username} via gateway...")
    gateway_register(username, password)
    token = gateway_login(username, password)
    if not token:
        fail("gateway login did not return a token")
    print("Authenticated.")

    print(f"\n=== Step 1: write {args.num_keys} keys through the gateway ===")
    keys_before = [(f"demo{i}", f"val{i}") for i in range(args.num_keys)]
    acked_before = write_keys_via_gateway(token, keys_before, "pre-kill")
    print(f"Acknowledged {len(acked_before)}/{args.num_keys} pre-kill writes.")

    leader, st = find_leader(containers)
    if not leader:
        fail("no leader found before kill")
    print(f"Current leader (engine side): {leader} (term={st['term']}, commit={st['commit']})")

    print(f"\n=== Step 2: kill the leader ({leader}) and measure failover ===")
    survivors = [c for c in containers if c != leader]
    kill_t0 = time.perf_counter()
    sh(["docker", "kill", leader])
    new_leader, confirm_t = wait_for_new_leader(survivors, deadline_s=15)
    if not new_leader:
        fail("no new leader elected within 15s")
    delta_ms = (confirm_t - kill_t0) * 1000
    print(f"New leader: {new_leader}")
    print(f"MEASURED ELECTION-TO-RECOVERY DELTA: {delta_ms:.1f} ms "
          f"(docker kill -> new leader confirmed via STATUS)")
    print_all_status(survivors, "post-failover")

    print("\n=== Step 3: verify every pre-kill acknowledged write survived (via gateway) ===")
    lost = []
    for key, val in acked_before:
        got = gateway_get(token, key)
        if got != val:
            lost.append((key, val, got))
    if lost:
        fail(f"{len(lost)} pre-kill acknowledged writes lost or wrong: {lost[:5]}")
    print(f"PASS: all {len(acked_before)} pre-kill acknowledged writes correct on new leader.")

    print(f"\n=== Step 4: write {args.downtime_keys} keys while the old leader ({leader}) is down ===")
    keys_downtime = [(f"downtime{i}", f"dval{i}") for i in range(args.downtime_keys)]
    acked_downtime = write_keys_via_gateway(token, keys_downtime, "downtime")
    print(f"Acknowledged {len(acked_downtime)}/{args.downtime_keys} downtime writes.")

    print(f"\n=== Step 5: restart the old leader ({leader}) and wait for it to rejoin ===")
    sh(["docker", "start", leader])
    st = wait_for_rejoin(leader, deadline_s=15)
    if not st:
        fail("killed node did not rejoin the cluster within 15s")
    print(f"Rejoined as {st.get('role')} (term={st.get('term')}).")

    print("\n=== Step 6: verify the rejoined node's own WAL has ALL keys (pre-kill + downtime) ===")
    # Poll rather than a fixed sleep: AppendEntries catch-up time depends on
    # log size and host load (this machine's Docker Desktop Kubernetes control
    # plane has been observed to spike CPU and visibly slow both elections and
    # replication — see README), so a fixed short sleep is fragile by design.
    # Keys are looked up under the gateway's tenant-isolated encoding
    # (len(tenantId):tenantId:userKey — see StorageService.buildIsolatedKey),
    # since that is the actual key the engine persists, not the raw user key.
    all_acked = [(tenant_key(username, k), v) for k, v in (acked_before + acked_downtime)]
    catchup_deadline = time.time() + 20
    wal_state, missing = {}, all_acked
    while time.time() < catchup_deadline:
        wal_state = read_wal_final_state(leader)
        missing = [(k, v) for k, v in all_acked if wal_state.get(k) != v]
        if not missing:
            break
        time.sleep(0.5)
    if missing:
        fail(f"rejoined node's WAL missing/mismatched {len(missing)} keys after 20s "
             f"(including any written during its downtime): {missing[:10]}")
    print(f"PASS: rejoined node's WAL contains all {len(acked_before) + len(acked_downtime)} acknowledged keys "
          f"({len(acked_before)} pre-kill + {len(acked_downtime)} written during its downtime).")

    print("\n" + "=" * 70)
    print("PASS: full failover demo succeeded.")
    print(f"  Election-to-recovery delta: {delta_ms:.1f} ms")
    print(f"  Pre-kill writes verified:   {len(acked_before)}")
    print(f"  Downtime writes verified:   {len(acked_downtime)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
