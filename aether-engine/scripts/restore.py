#!/usr/bin/env python3
"""
Restores a backup (produced by backup.py) into a target container's data
directory. The target container must exist but not yet be running its
AetherDB process with conflicting data - either freshly `docker create`d
(never started) or already stopped.

No new recovery code needed: the engine's existing startup path (load
snapshot, then replay WAL on top) does all the actual recovery work once
the right files are in the right place - this script's only job is placing
them there before the process starts.

Usage:
    python3 restore.py <backup-dir> <target-container>
"""
import argparse
import subprocess
import sys

DATA_DIR_IN_CONTAINER = "/data"


def sh(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("backup_dir", help="Backup directory produced by backup.py")
    ap.add_argument("target_container", help="Container to restore into")
    args = ap.parse_args()

    # Stop the container if it's running - never docker cp into a live
    # process's open files. Idempotent: harmless if already stopped/not started.
    print(f"Stopping {args.target_container} (if running)...")
    sh(["docker", "stop", args.target_container])

    files = ["aether.snapshot", "aether.wal", "raft.meta"]
    for fname in files:
        src = f"{args.backup_dir}/{fname}"
        dst = f"{args.target_container}:{DATA_DIR_IN_CONTAINER}/{fname}"
        r = sh(["docker", "cp", src, dst])
        if r.returncode != 0:
            print(f"  WARN: could not copy {fname}: {r.stderr.strip()}")
        else:
            print(f"  restored {fname}")

    print(f"\nStarting {args.target_container}...")
    r = sh(["docker", "start", args.target_container])
    if r.returncode != 0:
        print(f"FAIL: could not start {args.target_container}: {r.stderr.strip()}")
        sys.exit(1)

    print(f"Restore complete. {args.target_container} started with restored data.")


if __name__ == "__main__":
    main()
