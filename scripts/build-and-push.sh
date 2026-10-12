#!/usr/bin/env bash
# Builds ingester/consumer/api for linux/arm64 (the Oracle Ampere target — this machine
# is natively arm64 too, so this is a real, non-emulated build, not QEMU) and pushes them
# to ghcr.io, tagged exactly as compose.prod.yaml's image: fields expect. Run from
# anywhere: ./scripts/build-and-push.sh
#
# Prerequisite, not done by this script: `docker login ghcr.io -u ayomide1x` with a
# GitHub personal access token (packages:write scope). Authentication is a one-time,
# interactive, credential-handling step — scripting it would mean either prompting for a
# token here or holding one in a file, neither of which this script does on your behalf.
#
# Images are pushed public (no credential needed to pull on the VM) — nothing sensitive
# is baked into any of them; every DSN/origin/secret-shaped value is a runtime env var,
# confirmed throughout Stage 9. See DECISIONS.md.
set -euo pipefail
cd "$(dirname "$0")/.."

REGISTRY="ghcr.io/ayomide1x"

docker buildx version >/dev/null 2>&1 || { echo "docker buildx not available"; exit 1; }

# Checks whether a ghcr.io credential is actually recorded, not just that Docker itself
# is reachable (the two are unrelated — an earlier version of this check tested the
# wrong thing).
grep -q '"ghcr.io"' ~/.docker/config.json 2>/dev/null || {
  echo "Not logged into ghcr.io — run: docker login ghcr.io -u ayomide1x   (packages:write PAT)"
  exit 1
}

for svc in ingester consumer api; do
  echo "=== building ${svc} for linux/arm64 ==="
  docker buildx build --platform linux/arm64 -t "${REGISTRY}/mdp-${svc}:latest" --push "./${svc}"

  # A single-platform push can still come back wrapped in a manifest index (BuildKit
  # attaches provenance/SBOM attestations by default, which ride alongside the real
  # manifest as sibling entries with platform "unknown" — confirmed by inspecting a
  # known multi-arch image's own shape before trusting this parsing logic). This checks
  # both the plain single-manifest shape and the indexed one, filtering out anything
  # that isn't a real, named architecture.
  ARCH=$(docker buildx imagetools inspect "${REGISTRY}/mdp-${svc}:latest" --format '{{json .}}' 2>/dev/null \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
m = d.get('manifest', {})
direct = m.get('platform', {}).get('architecture')
if direct:
    print(direct)
else:
    archs = {e.get('platform', {}).get('architecture') for e in m.get('manifests', [])}
    archs.discard('unknown')
    archs.discard(None)
    print(','.join(sorted(archs)) or '?')
" 2>/dev/null || echo "?")
  echo "=== ${svc} pushed, remote manifest architecture: ${ARCH} ==="
done

echo ""
echo "Done. On the VM: docker compose -f compose.yaml -f compose.prod.yaml -f compose.deploy.yaml pull"
