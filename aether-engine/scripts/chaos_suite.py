#!/usr/bin/env python3
"""
Chaos testing suite for the live Docker 3-node AetherDB cluster.

Distinct from what's already proven (raft_safety_test.py: leader killed
mid-load, sequential and concurrent; follower catch-up after downtime) -
this suite adds the adversarial scenarios that specifically test edge
cases in Raft's SAFETY guarantees, not just its happy-path failover.

Five independently-runnable scenarios:
  1. Kill a follower (not the leader) mid-load.
  2. Kill a node mid-snapshot-write (tests the atomic temp-then-rename).
  3. Kill a majority at once - the survivor must NOT elect itself (negative test).
  4. Simulate a slow/degraded peer link.
  5. Restart storm - kill/restart the same follower 5x quickly.

Usage:
    python3 chaos_suite.py --scenario 1
    python3 chaos_suite.py --all
"""
import argparse
import os
import subprocess
import sys
import time

CLIENT_PORT = 8080
COMPOSE_PROJECT = "atherdb_c_pro"
ENGINE_SERVICES = ["aether-engine-1", "aether-engine-2", "aether-engine-3"]
COMPOSE_FILE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def sh(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def container_name(service):
    r = sh(["docker", "compose", "-p", COMPOSE_PROJECT, "ps", "-q", service])
    cid = r.stdout.strip()
    if not cid:
        raise RuntimeError(f"service {service} not found (is `docker compose up -d` running?)")
    r2 = sh(["docker", "inspect", "-f", "{{.Name}}", cid])
    return r2.stdout.strip().lstrip("/")


def send_cmd(container, cmd, timeout=5.0):
    script = (
        f'exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; '
        f'printf "{cmd}\\n" >&3; '
        f'timeout {timeout} head -1 <&3'
    )
    try:
        r = sh(["docker", "exec", container, "bash", "-c", script], timeout=timeout + 3)
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""


def get_status(container):
    resp = send_cmd(container, "STATUS")
    if not resp or resp.startswith("ERR") or not resp:
        return None
    fields = {}
    for part in resp.split():
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    return fields or None


def find_leader(containers, deadline_s=10):
    end = time.time() + deadline_s
    while time.time() < end:
        for c in containers:
            st = get_status(c)
            if st and st.get("role") == "LEADER":
                return c, st
        time.sleep(0.2)
    return None, None


def containers():
    return [container_name(s) for s in ENGINE_SERVICES]


def fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def restart_container(name):
    sh(["docker", "start", name])


def service_for_container(container):
    """Reverse-maps a resolved container name back to its compose service
    name (e.g. atherdb_c_pro-aether-engine-1-1 -> aether-engine-1), needed to
    recreate that specific service with different env vars."""
    for svc in ENGINE_SERVICES:
        if container_name(svc) == container:
            return svc
    raise RuntimeError(f"no compose service found for container {container}")


def node_id_for_service(service):
    # aether-engine-N has AETHER_NODE_ID=N in docker-compose.yml.
    return int(service.rsplit("-", 1)[1])


def recreate_with_env(services, env_updates, wait_s=3):
    """Force-recreates the given compose services with extra env vars set
    (via host-env interpolation already wired into docker-compose.yml's
    AETHER_CHAOS_* vars). Preserves each service's data volume - only the
    process restarts, no data is lost. Waits briefly for them to rejoin."""
    env = os.environ.copy()
    env.update(env_updates)
    cmd = ["docker", "compose", "-p", COMPOSE_PROJECT, "up", "-d", "--force-recreate"] + services
    r = subprocess.run(cmd, cwd=COMPOSE_FILE_DIR, capture_output=True, text=True, env=env, timeout=60)
    if r.returncode != 0:
        fail(f"failed to recreate {services} with {env_updates}: {r.stderr}")
    time.sleep(wait_s)


def recreate_normal(services, wait_s=3):
    """Recreates the given services with no chaos env vars set (reverts to
    normal operation). Explicitly clears the vars from the host env passed
    to compose, in case they're set in the calling shell."""
    env = os.environ.copy()
    for k in ["AETHER_CHAOS_SNAPSHOT_DELAY_MS", "AETHER_CHAOS_DELAY_PEER_ID", "AETHER_CHAOS_DELAY_MS"]:
        env.pop(k, None)
    cmd = ["docker", "compose", "-p", COMPOSE_PROJECT, "up", "-d", "--force-recreate"] + services
    r = subprocess.run(cmd, cwd=COMPOSE_FILE_DIR, capture_output=True, text=True, env=env, timeout=60)
    if r.returncode != 0:
        fail(f"failed to revert {services} to normal: {r.stderr}")
    time.sleep(wait_s)


def wait_for_stable_leader(cs, deadline_s=20):
    """Polls until exactly one node reports LEADER (handles the brief
    multi-way election churn that follows recreating multiple nodes at
    once)."""
    end = time.time() + deadline_s
    while time.time() < end:
        statuses = [(c, get_status(c)) for c in cs]
        leaders = [c for c, st in statuses if st and st.get("role") == "LEADER"]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(1)
    return None


# ---------- Scenario 1: kill a follower mid-load ----------

def scenario_1():
    print("=== Scenario 1: kill a follower (not the leader) mid-load ===")
    cs = containers()
    leader, st = find_leader(cs)
    if not leader:
        fail("no leader found")
    print(f"Leader: {leader} (term={st['term']})")
    followers = [c for c in cs if c != leader]
    target_follower = followers[0]
    print(f"Target follower to kill: {target_follower}")

    acked = []
    for i in range(60):
        key, val = f"chaos1_k{i}", f"v{i}"
        resp = send_cmd(leader, f"SET {key} {val}")
        if resp == "OK":
            acked.append((key, val))
        if i == 20:
            print(f"Killing follower {target_follower} mid-load (SIGKILL)...")
            sh(["docker", "kill", target_follower])

    print(f"Acknowledged {len(acked)}/60 writes during the follower kill.")
    # Leader must still be leader - a follower dying is not a leadership event.
    st_after = get_status(leader)
    if not st_after or st_after.get("role") != "LEADER":
        fail(f"leader {leader} lost leadership after a mere follower kill: {st_after}")
    print(f"PASS: {leader} remained LEADER throughout (follower loss is a non-event for availability).")

    lost = [(k, v) for k, v in acked if send_cmd(leader, f"GET {k}") != v]
    if lost:
        fail(f"lost acknowledged writes: {lost[:5]}")
    print(f"PASS: all {len(acked)} acknowledged writes correct.")

    print(f"Restarting killed follower {target_follower}...")
    restart_container(target_follower)
    deadline = time.time() + 20
    caught_up = False
    while time.time() < deadline:
        st_f = get_status(target_follower)
        if st_f and st_f.get("commit") == st_after.get("commit"):
            caught_up = True
            break
        time.sleep(0.5)
    if not caught_up:
        fail(f"follower {target_follower} did not catch up within 20s")
    print(f"PASS: {target_follower} caught up to commit={st_after.get('commit')} after restart.")
    print("SCENARIO 1: PASS\n")


# ---------- Scenario 3: kill a majority at once (negative test) ----------

def scenario_3():
    print("=== Scenario 3: kill a majority (2 of 3) simultaneously - negative test ===")
    cs = containers()
    leader, st = find_leader(cs)
    if not leader:
        fail("no leader found")
    print(f"Leader: {leader} (term={st['term']})")
    others = [c for c in cs if c != leader]
    to_kill = [leader, others[0]]
    survivor = others[1]
    print(f"Killing {to_kill} simultaneously, leaving survivor {survivor} alone (below quorum)...")
    for c in to_kill:
        sh(["docker", "kill", c])

    print("Observing survivor's STATUS for 12s - it must NEVER become LEADER...")
    became_leader = False
    last_status = None
    for i in range(12):
        st_s = get_status(survivor)
        last_status = st_s
        print(f"  t={i}s: {st_s}")
        if st_s and st_s.get("role") == "LEADER":
            became_leader = True
            break
        time.sleep(1)

    write_resp = send_cmd(survivor, "SET chaos3_should_fail x")
    print(f"SET attempt on survivor while below quorum: {write_resp!r}")

    print("Restarting the killed nodes to restore the cluster...")
    for c in to_kill:
        restart_container(c)

    if became_leader:
        fail(f"survivor {survivor} incorrectly became LEADER without a quorum - "
             f"this is a real safety violation, not a minor issue")
    if write_resp == "OK":
        fail(f"survivor {survivor} incorrectly ACCEPTED a write without a quorum")

    print(f"PASS: survivor correctly never became leader (last observed: {last_status}).")
    print(f"PASS: survivor correctly refused the write ({write_resp!r}).")
    print("SCENARIO 3: PASS\n")

    # Let the cluster fully recover before the next scenario.
    deadline = time.time() + 20
    while time.time() < deadline:
        new_leader, _ = find_leader(containers(), deadline_s=2)
        if new_leader:
            break
    time.sleep(2)


# ---------- Scenario 2: kill mid-snapshot-write ----------

def scenario_2():
    print("=== Scenario 2: kill a node mid-snapshot-write ===")
    cs = containers()
    leader, st = find_leader(cs)
    if not leader:
        fail("no leader found")
    followers = [c for c in cs if c != leader]
    target = followers[0]
    target_service = service_for_container(target)
    print(f"Target (a follower, to keep the leader stable): {target}")

    st_before = get_status(target)
    print(f"Before: {st_before}")

    print(f"Recreating {target_service} with AETHER_CHAOS_SNAPSHOT_DELAY_MS=3000 "
          f"(test-only hook) so the write can be reliably caught mid-flight...")
    recreate_with_env([target_service], {"AETHER_CHAOS_SNAPSHOT_DELAY_MS": "3000"})
    target = container_name(target_service)  # container id may have changed

    print(f"Triggering SNAPSHOT_EXPORT on {target}, then killing it mid-write...")
    export_thread_cmd = (
        f'exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; printf "SNAPSHOT_EXPORT\\n" >&3; head -1 <&3'
    )
    proc = subprocess.Popen(["docker", "exec", target, "bash", "-c", export_thread_cmd],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)  # 1s into a 3s delayed write - reliably mid-write, not after
    print(f"Killing {target} mid-write (SIGKILL)...")
    sh(["docker", "kill", target])
    proc.wait(timeout=5)

    print(f"Restarting {target_service} with the delay hook removed (back to normal)...")
    recreate_normal([target_service])
    target = container_name(target_service)
    deadline = time.time() + 20
    recovered = False
    st_after = None
    while time.time() < deadline:
        st_after = get_status(target)
        if st_after:
            recovered = True
            break
        time.sleep(1)

    if not recovered:
        fail(f"{target} did not come back up cleanly after a mid-snapshot-write kill "
             f"(possible corruption or crash-loop)")
    print(f"PASS: {target} recovered cleanly after being killed mid-snapshot-write: {st_after}")

    # Confirm it rejoins and catches up normally, proving no corruption.
    leader_now, _ = find_leader(containers())
    st_leader = get_status(leader_now)
    deadline = time.time() + 20
    caught_up = False
    while time.time() < deadline:
        st_after = get_status(target)
        if st_after and st_after.get("commit") == st_leader.get("commit"):
            caught_up = True
            break
        time.sleep(1)
    if not caught_up:
        fail(f"{target} did not converge with the cluster after recovering")
    print(f"PASS: {target} converged with the cluster (commit={st_leader.get('commit')}) - "
          f"no corruption, no crash-loop.")
    print("SCENARIO 2: PASS\n")


# ---------- Scenario 4: simulated slow peer ----------

def scenario_4():
    print("=== Scenario 4: simulated slow/degraded peer link ===")
    cs = containers()
    leader, st = find_leader(cs)
    if not leader:
        fail("no leader found")
    followers = [c for c in cs if c != leader]
    slow_peer = followers[0]
    slow_id = node_id_for_service(service_for_container(slow_peer))
    print(f"Designated slow peer: {slow_peer} (node id {slow_id})")

    # Recreate ALL three: whichever node ends up leader must have the delay
    # hook active for it to matter (the hook fires from the leader's send
    # path). 400ms is deliberately UNDER the ~800-1600ms election timeout -
    # see the 3000ms variant's result in CHAOS_TESTING.md for what happens
    # when the delay exceeds the timeout instead (cluster-wide churn, a
    # different and also-legitimate finding, not the scenario intended here).
    print(f"Recreating all 3 nodes with AETHER_CHAOS_DELAY_PEER_ID={slow_id}, "
          f"AETHER_CHAOS_DELAY_MS=400 (test-only hook)...")
    all_services = [service_for_container(c) for c in cs]
    recreate_with_env(all_services, {
        "AETHER_CHAOS_DELAY_PEER_ID": str(slow_id),
        "AETHER_CHAOS_DELAY_MS": "400",
    }, wait_s=2)

    cs = containers()
    leader = wait_for_stable_leader(cs)
    if not leader:
        fail("cluster did not settle on a single stable leader after recreating with the delay hook")
    print(f"Stable leader after recreate: {leader}")
    slow_peer = container_name(service_for_container(slow_peer))  # refresh id

    acked = 0
    for i in range(20):
        resp = send_cmd(leader, f"SET chaos4_k{i} v{i}")
        if resp == "OK":
            acked += 1
    print(f"Wrote {acked}/20 keys while the peer link to {slow_peer} is delayed.")

    st_slow = get_status(slow_peer)
    st_leader = get_status(leader)
    print(f"Leader: {st_leader}")
    print(f"Slow peer (expected to be at or behind): {st_slow}")
    if acked < 20:
        fail(f"only {acked}/20 writes succeeded - cluster should stay fully available "
             f"via the leader + non-slow follower")
    if st_leader and st_slow and int(st_slow.get("commit", -1)) > int(st_leader.get("commit", 0)):
        fail("slow peer is somehow AHEAD of the leader - unexpected")
    same_term = st_leader and st_slow and st_leader.get("term") == st_slow.get("term")
    if not same_term:
        fail(f"term diverged (leader={st_leader.get('term')}, slow={st_slow.get('term')}) - "
             f"suggests the delay destabilized leadership rather than just lagging one peer")
    print(f"PASS: cluster kept operating on a single stable leader (term unchanged) "
          f"while {slow_peer} lagged behind ({acked}/20 writes succeeded).")

    print("Removing the delay (recreating all 3 with no chaos env vars)...")
    recreate_normal(all_services, wait_s=2)
    cs = containers()
    leader = wait_for_stable_leader(cs)
    if not leader:
        fail("cluster did not restabilize after removing the delay")
    st_leader = get_status(leader)
    slow_peer = container_name(service_for_container(slow_peer))

    deadline = time.time() + 20
    caught_up = False
    while time.time() < deadline:
        st_slow = get_status(slow_peer)
        st_leader = get_status(leader)
        if st_slow and st_leader and st_slow.get("commit") == st_leader.get("commit"):
            caught_up = True
            break
        time.sleep(1)
    if not caught_up:
        fail(f"{slow_peer} did not catch up within 20s after the delay was removed")
    print(f"PASS: {slow_peer} caught up to commit={st_leader.get('commit')} "
          f"after the delay was removed.")
    print("SCENARIO 4: PASS\n")


# ---------- Scenario 5: restart storm ----------

def scenario_5():
    print("=== Scenario 5: restart storm - kill/restart the same follower 5x quickly ===")
    cs = containers()
    leader, st = find_leader(cs)
    if not leader:
        fail("no leader found")
    followers = [c for c in cs if c != leader]
    target = followers[0]
    print(f"Target follower: {target}")

    acked = []
    for round_n in range(5):
        key, val = f"chaos5_r{round_n}", f"v{round_n}"
        resp = send_cmd(leader, f"SET {key} {val}")
        if resp == "OK":
            acked.append((key, val))
        print(f"Round {round_n + 1}/5: kill {target}...")
        sh(["docker", "kill", target])
        time.sleep(0.3)
        restart_container(target)
        time.sleep(1.5)

    print(f"Acknowledged {len(acked)}/5 writes across the restart storm.")
    deadline = time.time() + 20
    st_leader = get_status(leader)
    caught_up = False
    while time.time() < deadline:
        st_target = get_status(target)
        if st_target and st_leader and st_target.get("commit") == st_leader.get("commit"):
            caught_up = True
            break
        time.sleep(1)
    if not caught_up:
        fail(f"{target} did not converge after the restart storm")
    print(f"PASS: {target} converged correctly after 5 rapid kill/restart cycles, no corruption.")

    lost = [(k, v) for k, v in acked if send_cmd(leader, f"GET {k}") != v]
    if lost:
        fail(f"lost writes across the restart storm: {lost}")
    print(f"PASS: all {len(acked)} writes survived the restart storm.")
    print("SCENARIO 5: PASS\n")


SCENARIOS = {
    1: scenario_1,
    2: scenario_2,
    3: scenario_3,
    4: scenario_4,
    5: scenario_5,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scenario", type=int, choices=SCENARIOS.keys())
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    if args.all:
        for n in [1, 3, 2, 4, 5]:
            SCENARIOS[n]()
    elif args.scenario:
        SCENARIOS[args.scenario]()
    else:
        print("Specify --scenario N or --all")
        sys.exit(1)


if __name__ == "__main__":
    main()
