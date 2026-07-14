#!/usr/bin/env python3
"""
Backs up a running AetherDB node: triggers a non-destructive snapshot
export (SNAPSHOT_EXPORT), then copies the snapshot, the full WAL, and
raft.meta out of the container into a local backup directory.

This is independent of Raft: Raft protects against losing a MINORITY of
nodes (the cluster keeps working, no committed data lost). It does NOT
protect against losing a MAJORITY at once (e.g. all node volumes on one
dev machine's disk failing simultaneously) - that's what this is for.

Any node works as the backup source - a follower's committed state is
just as valid to back up from as the leader's.

Usage:
    python3 backup.py <container-name> [--out-dir ./backups]
"""
import argparse
import datetime
import json
import os
import subprocess
import sys

CLIENT_PORT = 8080
DATA_DIR_IN_CONTAINER = "/data"


def sh(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def send_engine_cmd(container, cmd, timeout=10.0):
    """Raw-TCP admin command via docker exec + /dev/tcp, same pattern as
    raft_demo.py's STATUS polling. SNAPSHOT_EXPORT is an admin command on
    the text protocol, not part of the gRPC client data path."""
    script = (
        f'exec 3<>/dev/tcp/localhost/{CLIENT_PORT}; '
        f'printf "{cmd}\\n" >&3; '
        f'timeout {timeout} head -1 <&3'
    )
    r = sh(["docker", "exec", container, "bash", "-c", script], timeout=timeout + 5)
    return r.stdout.strip()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("container", help="Docker container name to back up (any node)")
    ap.add_argument("--out-dir", default="./backups", help="Local backup root directory")
    args = ap.parse_args()

    print(f"Triggering SNAPSHOT_EXPORT on {args.container}...")
    resp = send_engine_cmd(args.container, "SNAPSHOT_EXPORT")
    if resp != "OK":
        print(f"FAIL: SNAPSHOT_EXPORT returned: {resp!r}")
        sys.exit(1)
    print("Snapshot export succeeded (WAL untouched - non-destructive).")

    timestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    backup_dir = f"{args.out_dir}/{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)

    files_to_copy = ["aether.snapshot", "aether.wal", "raft.meta"]
    copied = []
    for fname in files_to_copy:
        src = f"{args.container}:{DATA_DIR_IN_CONTAINER}/{fname}"
        dst = f"{backup_dir}/{fname}"
        r = sh(["docker", "cp", src, dst])
        if r.returncode == 0:
            copied.append(fname)
            print(f"  copied {fname}")
        else:
            print(f"  skipped {fname} (not present: {r.stderr.strip()})")

    if "aether.wal" not in copied:
        print("FAIL: aether.wal is required for a valid backup and was not copied.")
        sys.exit(1)

    manifest = {
        "source_container": args.container,
        "timestamp": timestamp,
        "files": copied,
    }
    with open(f"{backup_dir}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nBackup complete: {backup_dir}")
    print(f"  files: {copied}")


if __name__ == "__main__":
    main()
