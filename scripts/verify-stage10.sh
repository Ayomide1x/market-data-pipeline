#!/usr/bin/env bash
# Live checks against the REAL deployed stack. Run from anywhere, after deploying:
#   ./scripts/verify-stage10.sh yourdomain.dev
#
# There is no local mode — unlike every previous stage's script, most of what Stage 10
# actually is (provisioning the Oracle instance, the security list, the instance's own
# iptables, DNS, Vercel's dashboard, Sentry project creation) has no local-dev-machine
# equivalent to rehearse against. This script only exercises what's genuinely
# observable from outside, against the real public URL, once the real thing exists:
#   - TLS is real (Caddy's automatic cert), not self-signed.
#   - The SSE stream shows a snapshot followed by live deltas through the real Caddy
#     proxy — the actual thing Stage 7's local testing couldn't prove (no proxy, no
#     flush_interval, in that loop at all).
#   - X-Forwarded-For spoofed from an untrusted peer (this machine, hitting the public
#     URL — not loopback, not inside Caddy's trusted range) is ignored, not honored —
#     the half of Stage 5's design that was structurally impossible to test locally.
#   - alertLookupMicros is readable from the real consumer's logs, so the Stage 6/9
#     re-measurement this stage owes can actually be read off, printed here as
#     instructions (this script doesn't have SSH access to the VM to fetch it itself).
set -uo pipefail

DOMAIN="${1:-}"
if [ -z "$DOMAIN" ]; then
  echo "Usage: $0 <yourdomain.dev>"
  echo "Requires the real stack already deployed and DNS already pointing at it."
  exit 1
fi

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

BASE="https://${DOMAIN}"

echo "=== Stage 10 verification against ${BASE} ==="

echo ""
echo "--- TLS is real, not self-signed ---"
CERT_ISSUER=$(curl -s -v "${BASE}/api/symbols" 2>&1 | grep -i "issuer:" | head -1)
echo "  ${CERT_ISSUER:-<none found>}"
echo "$CERT_ISSUER" | grep -qi "Let's Encrypt\|ZeroSSL"
report $? "certificate issuer looks like a real ACME CA, not a self-signed/internal one"

echo ""
echo "--- REST through Caddy ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/api/symbols")
[ "$STATUS" = "200" ]
report $? "GET /api/symbols through Caddy returns 200 (got $STATUS)"

echo ""
echo "--- SSE through Caddy: snapshot followed by live deltas ---"
curl -N -s --max-time 8 "${BASE}/api/stream" > /tmp/verify-stage10-stream.log 2>&1
SNAPSHOT_COUNT=$(grep -c "^event:snapshot" /tmp/verify-stage10-stream.log)
[ "$SNAPSHOT_COUNT" -gt 0 ]
report $? "snapshot event(s) arrived through the real proxy (found: ${SNAPSHOT_COUNT:-0})"

TICK_COUNT=$(grep -c "^event:tick" /tmp/verify-stage10-stream.log)
[ "$TICK_COUNT" -gt 0 ]
report $? "live tick delta(s) arrived through the real proxy within 8s (found: ${TICK_COUNT:-0}) — this is the thing flush_interval -1 exists for; a buffering proxy would show only the snapshot and nothing else until the connection times out"

echo ""
echo "--- X-Forwarded-For: spoofed from an untrusted peer (this machine, over the public internet — not loopback) ---"
echo "This is the half Stage 5 could never test locally: every local connection is"
echo "loopback, always inside Tomcat's trusted range. Hitting the real public URL from"
echo "here is a genuinely untrusted direct peer from Tomcat's perspective."
echo "The actual attack this guards against is varying the spoofed IP on every request"
echo "to get a fresh rate-limit bucket each time — a DIFFERENT fake value per request,"
echo "not the same one repeated. If the header were honored from here, this loop would"
echo "never hit 429, no matter how many requests it sends."
echo "Real alerts on the live symbol get created by whichever requests succeed before"
echo "the limit kicks in — this is a real side effect on the real system, not a"
echo "read-only check; every created id is deleted again immediately after."
LAST=""
CREATED_IDS=()
for i in $(seq 1 16); do
  FAKE_IP="203.0.113.$((RANDOM % 254 + 1))"
  RESP=$(curl -s -w '\n%{http_code}' -X POST "${BASE}/api/alerts" \
      -H 'Content-Type: application/json' -H "X-Forwarded-For: ${FAKE_IP}" \
      -d '{"symbol":"BTC-USD","direction":"above","threshold":"1"}')
  LAST=$(echo "$RESP" | tail -1)
  BODY=$(echo "$RESP" | sed '$d')
  if [ "$LAST" = "201" ]; then
    ID=$(echo "$BODY" | python3 -c "import json,sys; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
    [ -n "$ID" ] && CREATED_IDS+=("$ID")
  fi
done
echo "  sent 16 requests, each with a DIFFERENT spoofed X-Forwarded-For — last response: HTTP $LAST (${#CREATED_IDS[@]} succeeded and created a real alert)"
for id in "${CREATED_IDS[@]:-}"; do
  [ -n "$id" ] && curl -s -o /dev/null -X DELETE "${BASE}/api/alerts/${id}"
done
[ -n "${CREATED_IDS[*]:-}" ] && echo "  cleaned up ${#CREATED_IDS[@]} test alert(s)"
[ "$LAST" = "429" ]
report $? "varying the spoofed IP on every request still got rate-limited — a different fake value per request did not grant a fresh bucket each time, which is exactly what spoofing would achieve if the header were trusted from here. The real, unspoofable request origin (this machine's actual IP, as Caddy itself reports it) is what's actually being keyed on."
echo "  NOTE: this proves the header is being IGNORED from this untrusted peer, not that"
echo "  it's correctly honored from a trusted one — that half was already verified"
echo "  locally in Stage 5 (loopback is always inside the trusted range)."

echo ""
echo "=== $PASS passed, $FAIL failed ==="
echo ""
echo "Not scriptable from here — read these directly, once:"
echo "  1. alertLookupMicros re-measurement (owed since Stage 6/9): SSH into the VM and run"
echo "       docker compose -f compose.yaml -f compose.prod.yaml -f compose.deploy.yaml logs consumer | grep alertLookupMicros | tail -50"
echo "     Compare against the dev-laptop numbers already on record in DECISIONS.md."
echo "  2. docker compose ps on the VM shows every service healthy, and has for a"
echo "     while — this script only proves it's up right now, not that it's been"
echo "     stable (CLAUDE.md's own done-when wants hours, not a moment)."
echo "  3. Reboot the VM once (deliberately) and confirm everything comes back with no"
echo "     manual steps — the one thing this whole stage's restart-policy work was for,"
echo "     and the one thing that can only really be proven by actually doing it."

[ "$FAIL" -eq 0 ]
exit $?
