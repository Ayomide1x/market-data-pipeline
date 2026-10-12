#!/usr/bin/env bash
# Mechanical + live checks for Stage 7 (live push over SSE).
# Run after `make reset`, from anywhere: ./scripts/verify-stage7.sh
#
# Starts ingester + consumer + api, then exercises CLAUDE.md's own "done when" directly:
# curl -N shows a snapshot followed by deltas; two concurrent streams stay byte-for-byte
# in sync; an alert firing shows up as an alert-fired event on the same stream; heartbeat
# comments interleave; killing the api drops open connections and a fresh connection
# after restart gets a correct new snapshot. Also confirms the tick payload omits ageMs
# (a pushed value would go stale sitting in a per-client buffer — see DECISIONS.md) and
# that a client that disappears mid-stream gets evicted without disturbing anyone else.
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

kill_all() {
  for pattern in "src/index.ts" "com.marketdatapipeline.consumer.ConsumerApplication" "com.marketdatapipeline.api.ApiApplication"; do
    for pid in $(find_pids "$pattern"); do
      kill -TERM "$pid" 2>/dev/null
    done
  done
}

trap kill_all EXIT

API=http://127.0.0.1:8080

echo "=== Stage 7 verification ==="

docker compose ps kafka 2>/dev/null | grep -q "healthy"
report $? "kafka container healthy"

docker compose ps redis 2>/dev/null | grep -q "healthy"
report $? "redis container healthy"

(cd consumer && ./mvnw -q compile) >/tmp/verify-stage7-consumer-compile.log 2>&1
report $? "consumer compiles (see /tmp/verify-stage7-consumer-compile.log on failure)"

(cd consumer && ./mvnw -q test) >/tmp/verify-stage7-consumer-test.log 2>&1
report $? "consumer unit tests pass (see /tmp/verify-stage7-consumer-test.log on failure)"

(cd api && ./mvnw -q compile) >/tmp/verify-stage7-api-compile.log 2>&1
report $? "api compiles (see /tmp/verify-stage7-api-compile.log on failure)"

(cd api && ./mvnw -q test) >/tmp/verify-stage7-api-test.log 2>&1
report $? "api unit tests pass (see /tmp/verify-stage7-api-test.log on failure)"

SHA=$(redis-cli -h 127.0.0.1 -p 6379 -x SCRIPT LOAD < consumer/src/main/resources/scripts/apply_tick.lua 2>/dev/null)
[ -n "$SHA" ]
report $? "apply_tick.lua loads into Redis"

SYMBOLS_RAW=$(grep '^SYMBOLS=' ingester/.env 2>/dev/null | cut -d= -f2)
IFS=',' read -ra SYMBOLS <<< "$SYMBOLS_RAW"
FIRST_SYMBOL="${SYMBOLS[0]:-}"
[ -n "$FIRST_SYMBOL" ]
report $? "SYMBOLS resolves at least one symbol (found: ${SYMBOLS_RAW:-none})"

kill_all
sleep 2
redis-cli -h 127.0.0.1 -p 6379 KEYS "alert:*" 2>/dev/null | xargs -r -I{} redis-cli -h 127.0.0.1 -p 6379 DEL {} >/dev/null
for s in "${SYMBOLS[@]}"; do redis-cli -h 127.0.0.1 -p 6379 DEL "alerts:$s" >/dev/null 2>&1; done

echo ""
echo "--- starting ingester + consumer + api ---"
(cd ingester && npm run dev) >/tmp/verify-stage7-ingester.log 2>&1 &
sleep 5
(cd consumer && CONSUMER_CLIENT_ID=consumer-stage7 ./mvnw -q spring-boot:run) >/tmp/verify-stage7-consumer.log 2>&1 &
sleep 12
(cd api && SYMBOLS="$SYMBOLS_RAW" ALERT_GLOBAL_CAP=50 ALERT_RATE_LIMIT_PER_MINUTE=50 ./mvnw -q spring-boot:run) >/tmp/verify-stage7-api.log 2>&1 &
sleep 12

echo ""
echo "--- snapshot followed by deltas ---"
curl -N -s --max-time 6 "$API/api/stream" > /tmp/verify-stage7-stream.log 2>&1

SNAPSHOT_COUNT=$(grep -c "^event:snapshot" /tmp/verify-stage7-stream.log)
[ "$SNAPSHOT_COUNT" = "${#SYMBOLS[@]}" ]
report $? "snapshot contains one event per configured symbol (expected ${#SYMBOLS[@]}, got ${SNAPSHOT_COUNT:-0})"

TICK_COUNT=$(grep -c "^event:tick" /tmp/verify-stage7-stream.log)
[ "$TICK_COUNT" -gt 0 ]
report $? "at least one live tick delta arrived after the snapshot (found: ${TICK_COUNT:-0})"

FIRST_SNAPSHOT_LINE=$(grep -n "^event:snapshot" /tmp/verify-stage7-stream.log | head -1 | cut -d: -f1)
FIRST_TICK_LINE=$(grep -n "^event:tick" /tmp/verify-stage7-stream.log | head -1 | cut -d: -f1)
[ -n "$FIRST_SNAPSHOT_LINE" ] && [ -n "$FIRST_TICK_LINE" ] && [ "$FIRST_TICK_LINE" -gt "$FIRST_SNAPSHOT_LINE" ]
report $? "snapshot events precede the first tick delta in the stream"

AGEMS_IN_TICKS=$(grep "^event:tick" -A1 /tmp/verify-stage7-stream.log | grep -c "ageMs")
[ "$AGEMS_IN_TICKS" = "0" ]
report $? "tick events omit ageMs (a pushed value would go stale in the coalescing buffer — see DECISIONS.md)"

echo ""
echo "--- two concurrent clients stay byte-for-byte in sync ---"
curl -N -s --max-time 4 "$API/api/stream" > /tmp/verify-stage7-client-a.log 2>&1 &
CLIENT_A_PID=$!
curl -N -s --max-time 4 "$API/api/stream" > /tmp/verify-stage7-client-b.log 2>&1 &
CLIENT_B_PID=$!
# Explicit PIDs, not a bare `wait`: a bare wait blocks on EVERY background job of this
# shell, including the still-running ingester/consumer/api — it would hang until the
# trap kills them, never here. Confirmed live: this exact bug stalled the first version
# of this script for 7+ minutes before being caught. See DECISIONS.md.
wait "$CLIENT_A_PID" "$CLIENT_B_PID" 2>/dev/null
# ageMs is normalized out before comparing, not left as a strict byte-diff: it's
# deliberately computed fresh per request (Stage 5), so two near-simultaneous but
# separate snapshot reads legitimately differ by a millisecond or two — that's correct
# behavior, not a sync bug. Every actual live field (price/ema/exchangeTs) must still
# match exactly.
diff -q \
  <(sed -E 's/"ageMs":[0-9]+/"ageMs":X/' /tmp/verify-stage7-client-a.log) \
  <(sed -E 's/"ageMs":[0-9]+/"ageMs":X/' /tmp/verify-stage7-client-b.log) \
  >/dev/null 2>&1
report $? "two concurrent streams are identical (ageMs's expected sub-ms jitter normalized out)"

echo ""
echo "--- heartbeat comments interleave ---"
curl -N -s --max-time 20 "$API/api/stream" > /tmp/verify-stage7-heartbeat.log 2>&1
HEARTBEAT_COUNT=$(grep -c "heartbeat" /tmp/verify-stage7-heartbeat.log)
[ "$HEARTBEAT_COUNT" -ge 1 ]
report $? "at least one heartbeat comment arrived within 20s (found: ${HEARTBEAT_COUNT:-0})"

echo ""
echo "--- alert-fired event on the same stream ---"
# One continuous stream connection throughout, so it catches a fire regardless of which
# attempt's alert id eventually crosses. The alert itself is re-anchored to the live
# price every few seconds if the market drifts away without crossing it — a single fixed
# threshold isn't guaranteed to be crossed within any fixed window if price trends
# monotonically away from it, which is a real market-timing flake this script hit once,
# not a code bug. See DECISIONS.md.
rm -f /tmp/verify-stage7-alert-stream.log
curl -N -s --max-time 60 "$API/api/stream" > /tmp/verify-stage7-alert-stream.log 2>&1 &
STREAM_PID=$!
sleep 1

ALERT_ID=""
FIRED_ALERT_ID=""
FIRED_THRESHOLD=""
for attempt in $(seq 1 8); do
  if [ -n "$ALERT_ID" ]; then
    MATCH=$(grep -A1 "^event:alert-fired" /tmp/verify-stage7-alert-stream.log | grep "\"id\":\"$ALERT_ID\"")
    if [ -n "$MATCH" ]; then
      FIRED_ALERT_ID="$ALERT_ID"
      break
    fi
    curl -s -o /dev/null -X DELETE "$API/api/alerts/$ALERT_ID"
  fi
  LIVE_PRICE=$(curl -s "$API/api/symbols/$FIRST_SYMBOL" | python3 -c "import json,sys; print(json.load(sys.stdin)['price'])" 2>/dev/null)
  RESP=$(curl -s -X POST "$API/api/alerts" -H 'Content-Type: application/json' \
      -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"above\",\"threshold\":\"$LIVE_PRICE\"}")
  ALERT_ID=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  FIRED_THRESHOLD="$LIVE_PRICE"
  sleep 5
done
if [ -z "$FIRED_ALERT_ID" ] && [ -n "$ALERT_ID" ]; then
  MATCH=$(grep -A1 "^event:alert-fired" /tmp/verify-stage7-alert-stream.log | grep "\"id\":\"$ALERT_ID\"")
  [ -n "$MATCH" ] && FIRED_ALERT_ID="$ALERT_ID"
fi

kill "$STREAM_PID" 2>/dev/null
wait "$STREAM_PID" 2>/dev/null

[ -n "$FIRED_ALERT_ID" ]
report $? "alert-fired event appeared on the stream, re-anchoring to the live price as needed (fired id: ${FIRED_ALERT_ID:-none})"

ALERT_EVENT=$(grep -A1 "^event:alert-fired" /tmp/verify-stage7-alert-stream.log | grep "\"id\":\"${FIRED_ALERT_ID:-none}\"")
echo "$ALERT_EVENT" | python3 -c "
import json, sys
data = json.loads(sys.stdin.read().split('data:',1)[1])
assert data['direction'] == 'above'
assert data['threshold'] == '$FIRED_THRESHOLD'
assert 'firedAtOffset' in data
" 2>/tmp/verify-stage7-alert-check.log
report $? "alert-fired payload has correct direction/threshold, read live inside apply_tick.lua (see /tmp/verify-stage7-alert-check.log on failure)"

echo ""
echo "--- client eviction on write failure doesn't disturb other clients ---"
curl -N -s "$API/api/stream" > /tmp/verify-stage7-kill-test.log 2>&1 &
KILL_TEST_PID=$!
sleep 3
kill -9 "$KILL_TEST_PID" 2>/dev/null
sleep 3
EVICTED=$(grep -c "SSE client evicted" /tmp/verify-stage7-api.log)
[ "$EVICTED" -ge 1 ]
report $? "the killed client was evicted (found ${EVICTED:-0} eviction log lines)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 4 "$API/api/stream")
[ "$STATUS" = "200" ]
report $? "api still serves new clients after evicting a dead one (got $STATUS)"

echo ""
echo "--- killing the api drops open connections; a fresh connection after restart resyncs ---"
curl -N -s "$API/api/stream" > /tmp/verify-stage7-restart-client.log 2>&1 &
RESTART_CLIENT_PID=$!
sleep 2
API_PID=$(find_pids "com.marketdatapipeline.api.ApiApplication" | head -1)
[ -n "$API_PID" ]
report $? "found the api process to stop (pid: ${API_PID:-none})"
kill -TERM "$API_PID" 2>/dev/null
sleep 5

if kill -0 "$RESTART_CLIENT_PID" 2>/dev/null; then
  kill -9 "$RESTART_CLIENT_PID" 2>/dev/null
  report 1 "open connection dropped when the api stopped (it was still alive)"
else
  report 0 "open connection dropped when the api stopped"
fi

(cd api && SYMBOLS="$SYMBOLS_RAW" ALERT_GLOBAL_CAP=50 ALERT_RATE_LIMIT_PER_MINUTE=50 ./mvnw -q spring-boot:run) >>/tmp/verify-stage7-api.log 2>&1 &
sleep 12

curl -N -s --max-time 4 "$API/api/stream" > /tmp/verify-stage7-resync.log 2>&1
RESYNC_SNAPSHOT_COUNT=$(grep -c "^event:snapshot" /tmp/verify-stage7-resync.log)
[ "$RESYNC_SNAPSHOT_COUNT" = "${#SYMBOLS[@]}" ]
report $? "fresh connection after api restart gets a correct new snapshot (expected ${#SYMBOLS[@]}, got ${RESYNC_SNAPSHOT_COUNT:-0})"

echo ""
echo "=== $PASS passed, $FAIL failed ==="

[ "$FAIL" -eq 0 ]
exit $?
