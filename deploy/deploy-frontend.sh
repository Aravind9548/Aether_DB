#!/usr/bin/env bash
# Deploys the AetherDB dashboard (the gateway's static files) to Vercel.
# Vercel serves the HTML/JS and forwards /api, /auth and OAuth paths to the
# backend server-side, so the browser only ever talks to one HTTPS origin.
#
# Usage:  BACKEND_HOST=34-100-1-2.sslip.io bash deploy/deploy-frontend.sh
set -euo pipefail
: "${BACKEND_HOST:?set BACKEND_HOST to the backend hostname, e.g. 34-100-1-2.sslip.io}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/deploy/.frontend-build"

rm -rf "$OUT"
mkdir -p "$OUT"
cp "$ROOT"/aether-gateway/src/main/resources/static/* "$OUT"/
sed "s/__BACKEND_HOST__/$BACKEND_HOST/g" "$ROOT/deploy/vercel.template.json" > "$OUT/vercel.json"
# Keep the Vercel project link between deploys.
if [ -d "$ROOT/deploy/.vercel" ]; then cp -r "$ROOT/deploy/.vercel" "$OUT/"; fi

cd "$OUT"
npx --yes vercel@latest link --yes --project aetherdb >/dev/null
npx --yes vercel@latest deploy --prod --yes
rm -rf "$ROOT/deploy/.vercel" && cp -r .vercel "$ROOT/deploy/"
