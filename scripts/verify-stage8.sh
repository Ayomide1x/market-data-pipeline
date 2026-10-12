#!/usr/bin/env bash
# Mechanical checks for Stage 8 (the React frontend).
# Run from anywhere: ./scripts/verify-stage8.sh
#
# There is no headless-browser tooling established in this project, so the mechanical
# ceiling here is: the frontend type-checks and builds cleanly, and the dev server
# actually boots and serves index.html. Visual/interactive behavior (does the flash
# look right, does the badge actually go connecting -> connected -> reconnecting ->
# connected across a real api restart, do toasts appear, does the alert form work end
# to end) is a manual checklist printed at the end, not automated — same precedent as
# Stage 3's explicit manual step for the kill-9 crash-recovery test.
set -uo pipefail
cd "$(dirname "$0")/.."

PASS=0
FAIL=0

report() {
  local status=$1; shift
  if [ "$status" -eq 0 ]; then
    echo "PASS: $*"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $*"
    FAIL=$((FAIL + 1))
  fi
}

find_pids() {
  ps aux | grep "$1" | grep -v grep | awk '{print $2}'
}

kill_frontend() {
  for pid in $(find_pids "vite"); do
    kill -TERM "$pid" 2>/dev/null
  done
}

trap kill_frontend EXIT

echo "=== Stage 8 verification ==="

(cd frontend && npm install --no-audit --no-fund) >/tmp/verify-stage8-install.log 2>&1
report $? "frontend dependencies install (see /tmp/verify-stage8-install.log on failure)"

(cd frontend && npm run lint) >/tmp/verify-stage8-lint.log 2>&1
report $? "lint passes with no errors (warnings are fine — see /tmp/verify-stage8-lint.log)"

(cd frontend && npm run build) >/tmp/verify-stage8-build.log 2>&1
report $? "type-checks and builds cleanly (see /tmp/verify-stage8-build.log on failure)"

[ -f frontend/.env ] || cp frontend/.env.example frontend/.env
kill_frontend
sleep 1
(cd frontend && npm run dev) >/tmp/verify-stage8-dev.log 2>&1 &
for i in $(seq 1 30); do
  curl -sf http://localhost:5173 >/dev/null 2>&1 && break
  sleep 1
done
STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5173)
[ "$STATUS" = "200" ]
report $? "dev server boots and serves index.html on :5173 (got $STATUS)"

BODY=$(curl -s http://localhost:5173)
echo "$BODY" | grep -q "<div id=\"root\">"
report $? "served HTML has the root mount point"

echo ""
echo "=== $PASS passed, $FAIL failed ==="
echo ""
echo "Manual checklist, not automated by this script (open http://localhost:5173"
echo "against the real stack and check by eye):"
echo "  1. Table shows price + EMA per configured symbol, updating live with a"
echo "     directional (green/red) flash on price change."
echo "  2. Connection badge: starts 'Connecting…', goes 'Live' once connected."
echo "  3. Stop the api: badge goes 'Reconnecting…' (not 'Connecting…' again)."
echo "     Restart it: badge returns to 'Live' and the table resyncs."
echo "  4. Load the page with the api stopped from the start: badge shows"
echo "     'Connecting…' with an explanatory message, not a blank/broken-looking"
echo "     table — this is the 'never connected' case, distinct from a mid-session drop."
echo "  5. Create an alert near the live price; confirm it appears under 'My alerts'"
echo "     as armed, then flips to fired with a toast when it crosses."
echo "  6. Delete an alert; confirm it disappears from 'My alerts' and the delete"
echo "     call succeeds even if clicked twice (second is a 404, handled quietly)."
echo "  7. Clear localStorage and reload: 'My alerts' is empty, but any"
echo "     previously-created alert is still armed/evaluated server-side (check via"
echo "     redis-cli or the api directly) — losing the id doesn't affect the alert."

[ "$FAIL" -eq 0 ]
exit $?
