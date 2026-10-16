#!/usr/bin/env bash
# GCE startup script for the AetherDB backend VM. Runs on every boot and is
# safe to re-run: installs Docker once, clones the repo once, creates .env
# once (fresh random JWT secret, generated on the VM, never leaves it),
# then (re)starts the stack.
set -euo pipefail
exec > >(tee -a /var/log/aetherdb-startup.log) 2>&1
echo "=== aetherdb startup $(date -Is) ==="

REPO_URL="https://github.com/Aravind9548/Aether_DB.git"
BRANCH="${AETHER_BRANCH:-branch2}"
DIR=/opt/aetherdb

if ! command -v docker >/dev/null; then
  echo "installing docker"
  curl -fsSL https://get.docker.com | sh
fi

if [ ! -d "$DIR/.git" ]; then
  git clone --branch "$BRANCH" "$REPO_URL" "$DIR"
fi
cd "$DIR"
git fetch origin "$BRANCH" && git checkout "$BRANCH" && git pull --ff-only origin "$BRANCH"

if [ ! -f .env ]; then
  IP=$(curl -s -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip)
  umask 077
  {
    echo "AETHER_JWT_SECRET=$(openssl rand -hex 32)"
    echo "SITE_ADDRESS=${IP//./-}.sslip.io"
  } > .env
  echo "created .env for ${IP//./-}.sslip.io"
fi

docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml up -d --build
docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml ps
echo "=== aetherdb startup done $(date -Is) ==="
