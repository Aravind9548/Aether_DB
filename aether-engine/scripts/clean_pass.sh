#!/usr/bin/env bash
# One clean end-to-end verification pass, in a single uninterrupted run:
#   1. partition suite (all 5 scenarios) on the fixed build
#   2. the three deterministic partition scenarios on the pre-fix build
#   3. chaos_suite.py --all and raft_demo.py on the real stack
# Proof the runtime stayed up: a sentinel container is started first and must
# still be running, with the same start time, at the end - any Docker daemon
# or VM restart kills it. The VM's uptime is recorded too (a VM restart
# resets it to ~0, so it must end above both its start value and the
# elapsed time; it can tick a little slower than the wall clock under load).
#
# Usage (from the repo root, Git Bash):  bash aether-engine/scripts/clean_pass.sh
set -u
cd "$(dirname "$0")/../.."
OUT="chaos-results/clean-pass-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
vm_uptime() { docker run --rm golang:1.22 cut -d' ' -f1 /proc/uptime; }

docker rm -f aetherdb-sentinel > /dev/null 2>&1
docker run -d --name aetherdb-sentinel --restart no golang:1.22 sleep infinity > /dev/null
S0=$(docker inspect -f '{{.State.StartedAt}}' aetherdb-sentinel)
T0=$(date +%s); U0=$(vm_uptime)
echo "start: $(date) | Docker VM uptime ${U0}s | sentinel started ${S0}" | tee "$OUT/summary.txt"

docker compose -p atherdb_c_pro stop > /dev/null 2>&1
python -u aether-engine/scripts/partition_test.py --all --image aetherdb-engine:dev > "$OUT/1-partition-fixed.log" 2>&1
echo "1 partition suite, fixed build:            exit=$?" | tee -a "$OUT/summary.txt"
for s in leader-isolation follower-isolation minority-writes; do
  python -u aether-engine/scripts/partition_test.py --scenario "$s" --image aetherdb-engine:legacy > "$OUT/2-legacy-$s.log" 2>&1
  echo "2 $s, pre-fix build:     exit=$? (expected 1)" | tee -a "$OUT/summary.txt"
done

docker compose -p atherdb_c_pro start > /dev/null 2>&1
sleep 15
python -u aether-engine/scripts/chaos_suite.py --all > "$OUT/3-chaos-suite.log" 2>&1
echo "3 chaos_suite --all:                       exit=$?" | tee -a "$OUT/summary.txt"
python -u aether-engine/scripts/raft_demo.py > "$OUT/4-raft-demo.log" 2>&1
echo "4 raft_demo (gateway failover):            exit=$?" | tee -a "$OUT/summary.txt"

# Local (WSL) tests on the freshly built binary. The stack is stopped first:
# its gateway publishes port 8080, which the local 3-node cluster also uses.
docker compose -p atherdb_c_pro stop > /dev/null 2>&1
wsl -e bash -lc 'cd "/mnt/c/Users/Admin/Desktop/my dellinsperion/atherdb_c++_pro/aether-engine/build-wsl" \
  && make -j4 AetherDB wal_v2_test > /dev/null && ./wal_v2_test 2>&1' > "$OUT/5-wal-unit-tests.log"
echo "5 wal_v2_test (unit):                      exit=$?" | tee -a "$OUT/summary.txt"
wsl -e bash -lc 'pkill -9 -x AetherDB; rm -rf /tmp/rst && mkdir -p /tmp/rst/build \
  && cp "/mnt/c/Users/Admin/Desktop/my dellinsperion/atherdb_c++_pro/aether-engine/build-wsl/AetherDB" /tmp/rst/build/ \
  && cd /tmp/rst && timeout 900 python3 -u "/mnt/c/Users/Admin/Desktop/my dellinsperion/atherdb_c++_pro/aether-engine/scripts/raft_safety_test.py"; rc=$?; pkill -9 -x AetherDB; exit $rc' \
  > "$OUT/6-raft-safety-test.log" 2>&1
echo "6 raft_safety_test (local 3-node):         exit=$?" | tee -a "$OUT/summary.txt"
docker compose -p atherdb_c_pro start > /dev/null 2>&1

T1=$(date +%s); U1=$(vm_uptime)
ELAPSED=$((T1 - T0))
S1=$(docker inspect -f '{{.State.Running}} {{.State.StartedAt}}' aetherdb-sentinel 2>/dev/null)
docker rm -f aetherdb-sentinel > /dev/null 2>&1
echo "end: $(date) | Docker VM uptime ${U1}s | wall ${ELAPSED}s | sentinel: ${S1}" | tee -a "$OUT/summary.txt"
UP_OK=$(python -c "print(float('$U1') > float('$U0') and float('$U1') > $ELAPSED)")
if [ "$S1" = "true $S0" ] && [ "$UP_OK" = "True" ]; then
  echo "RUNTIME STABLE: sentinel ran uninterrupted and VM uptime never reset" | tee -a "$OUT/summary.txt"
else
  echo "WARNING: Docker daemon or VM restarted during the pass - results void" | tee -a "$OUT/summary.txt"
fi
echo "logs: $OUT"
