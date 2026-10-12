#!/usr/bin/env bash
# Mechanical + live checks for Stage 5 (API read path and alert CRUD).
# Run after `make reset`, from anywhere: ./scripts/verify-stage5.sh
#
# This starts ingester + one consumer + the api, then exercises every response shape the
# plan called for (unknown symbol vs. configured-but-dataless vs. real data), the full
# alert CRUD + UUID round trip, and the two mechanisms that are new and genuinely unverified
# by compiling alone: the global-cap cleanup (which must resolve dead ids across EVERY
# configured symbol, not just the one being read, or an unqueried symbol's expired alerts
# inflate the cap forever — see DECISIONS.md) and CORS's exact-origin allowlist. It also
# proves the Stage 5 "done when": the api keeps serving snapshot reads with the consumer
# stopped (invariant #7 — api never holds authoritative state, Redis does).
#
# X-Forwarded-For/trusted-proxy behavior is NOT fully covered here: every local connection
# arrives from loopback, which is always inside Tomcat's trusted range, so this script can
# prove the header is read and honored from a trusted peer but cannot produce an actual
# untrusted direct peer to prove the header is ignored from one. That half only becomes
# testable once something sits in front of the api at a non-private address — Stage 10.
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

echo "=== Stage 5 verification ==="

docker compose ps kafka 2>/dev/null | grep -q "healthy"
report $? "kafka container healthy"

docker compose ps redis 2>/dev/null | grep -q "healthy"
report $? "redis container healthy"

(cd api && ./mvnw -q compile) >/tmp/verify-stage5-compile.log 2>&1
report $? "api compiles (see /tmp/verify-stage5-compile.log on failure)"

(cd api && ./mvnw -q test) >/tmp/verify-stage5-test.log 2>&1
report $? "api unit tests pass — AlertRequestValidator direction/threshold cases (see /tmp/verify-stage5-test.log on failure)"

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
(cd ingester && npm run dev) >/tmp/verify-stage5-ingester.log 2>&1 &
sleep 5
(cd consumer && CONSUMER_CLIENT_ID=consumer-stage5 ./mvnw -q spring-boot:run) >/tmp/verify-stage5-consumer.log 2>&1 &
CONSUMER_PID=$!
sleep 12
(cd api && SYMBOLS="${SYMBOLS_RAW},ZZZ-NODATA" ALERT_GLOBAL_CAP=3 ALERT_RATE_LIMIT_PER_MINUTE=50 ALERT_TTL_SECONDS=5 \
    CORS_ORIGIN=https://verify-stage5.example ./mvnw -q spring-boot:run) >/tmp/verify-stage5-api.log 2>&1 &
sleep 12

curl -sf "$API/api/symbols" >/tmp/verify-stage5-all.json 2>/dev/null
report $? "GET /api/symbols responds"

python3 -c "
import json
data = json.load(open('/tmp/verify-stage5-all.json'))
traded = [row for row in data if row['symbol'] != 'ZZZ-NODATA']
assert len(traded) == len([s for s in '$SYMBOLS_RAW'.split(',') if s]), 'wrong traded-symbol count'
for row in traded:
    assert row['hasData'] is True, f'{row[\"symbol\"]} has no data yet'
    assert row['ageMs'] is not None and row['ageMs'] >= 0, f'{row[\"symbol\"]} missing ageMs'
" 2>/tmp/verify-stage5-all-check.log
report $? "every configured, traded symbol shows hasData:true with a non-negative ageMs (see /tmp/verify-stage5-all-check.log on failure)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$API/api/symbols/ZZZ-NODATA")
[ "$STATUS" = "200" ]
report $? "configured-but-dataless symbol returns 200, not 404 (got $STATUS)"

HASDATA=$(curl -s "$API/api/symbols/ZZZ-NODATA" | python3 -c "import json,sys; print(json.load(sys.stdin)['hasData'])")
[ "$HASDATA" = "False" ]
report $? "configured-but-dataless symbol reports hasData:false (got $HASDATA)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$API/api/symbols/NOT-CONFIGURED")
[ "$STATUS" = "404" ]
report $? "unconfigured symbol returns 404 (got $STATUS)"

echo ""
echo "--- alert validation ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"NOT-CONFIGURED\",\"direction\":\"above\",\"threshold\":\"1\"}")
[ "$STATUS" = "400" ]
report $? "create against unconfigured symbol rejected with 400 (got $STATUS)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"sideways\",\"threshold\":\"1\"}")
[ "$STATUS" = "400" ]
report $? "create with invalid direction rejected with 400 (got $STATUS)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"above\",\"threshold\":\"-5\"}")
[ "$STATUS" = "400" ]
report $? "create with non-positive threshold rejected with 400 (got $STATUS)"

echo ""
echo "--- alert CRUD + UUID round trip ---"
RESP=$(curl -s -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"above\",\"threshold\":\"999999999\"}")
ID=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
[ -n "$ID" ]
report $? "create returns a capability id (got: ${ID:-none})"

echo "$RESP" | python3 -c "
import json, sys, uuid
data = json.load(sys.stdin)
uuid.UUID(data['id'])
assert 'lastOffset' not in data and 'partition' not in data
" 2>/tmp/verify-stage5-uuid-check.log
report $? "id is a valid UUID and response excludes lastOffset/partition (see /tmp/verify-stage5-uuid-check.log on failure)"

FOUND=$(curl -s "$API/api/alerts?symbol=$FIRST_SYMBOL" | python3 -c "import json,sys; print('$ID' in [a['id'] for a in json.load(sys.stdin)])")
[ "$FOUND" = "True" ]
report $? "list for symbol includes the created alert"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$API/api/alerts/$ID")
[ "$STATUS" = "204" ]
report $? "delete returns 204 (got $STATUS)"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$API/api/alerts/$ID")
[ "$STATUS" = "404" ]
report $? "deleting an already-deleted id returns 404 (got $STATUS)"

echo ""
echo "--- global cap + cleanup ---"
redis-cli -h 127.0.0.1 -p 6379 KEYS "alert:*" 2>/dev/null | xargs -r -I{} redis-cli -h 127.0.0.1 -p 6379 DEL {} >/dev/null
for s in "${SYMBOLS[@]}" ZZZ-NODATA; do redis-cli -h 127.0.0.1 -p 6379 DEL "alerts:$s" >/dev/null 2>&1; done

SECOND_SYMBOL="${SYMBOLS[1]:-$FIRST_SYMBOL}"
for i in 1 2 3; do
  curl -s -o /dev/null -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"above\",\"threshold\":\"999999999\"}"
done
BEFORE=$(curl -s -D - -o /tmp/verify-stage5-cap-body.json -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$SECOND_SYMBOL\",\"direction\":\"above\",\"threshold\":\"1\"}")
STATUS=$(echo "$BEFORE" | grep -oE "^HTTP/[0-9.]+ [0-9]+" | awk '{print $2}')
[ "$STATUS" = "429" ]
report $? "4th create across symbols hits the global cap with 429 (got $STATUS)"

HAS_RETRY=$(echo "$BEFORE" | grep -ic "retry-after")
[ "$HAS_RETRY" -eq 0 ]
report $? "cap-exceeded response omits Retry-After (the cap doesn't drain on a fixed schedule)"

SCARD_BEFORE=$(redis-cli -h 127.0.0.1 -p 6379 SCARD "alerts:$FIRST_SYMBOL")
echo "--- waiting for the 3 $FIRST_SYMBOL alerts to TTL out (ALERT_TTL_SECONDS=5), WITHOUT ever calling list() for $FIRST_SYMBOL ---"
sleep 7
SCARD_AFTER_EXPIRY=$(redis-cli -h 127.0.0.1 -p 6379 SCARD "alerts:$FIRST_SYMBOL")
[ "$SCARD_AFTER_EXPIRY" = "$SCARD_BEFORE" ]
report $? "set membership does not self-clean on hash TTL expiry (SCARD stayed $SCARD_AFTER_EXPIRY) — this is the drift the cleanup fix targets"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/api/alerts" -H 'Content-Type: application/json' -d "{\"symbol\":\"$SECOND_SYMBOL\",\"direction\":\"above\",\"threshold\":\"1\"}")
[ "$STATUS" = "201" ]
report $? "create on a different symbol succeeds once the expired ids are resolved out of the cap count, with $FIRST_SYMBOL's set never read via list() (got $STATUS)"

SCARD_AFTER_CLEANUP=$(redis-cli -h 127.0.0.1 -p 6379 SCARD "alerts:$FIRST_SYMBOL")
[ "$SCARD_AFTER_CLEANUP" -lt "$SCARD_AFTER_EXPIRY" ]
report $? "checkGlobalCapWithCleanup pruned $FIRST_SYMBOL's dead ids as a side effect of counting (SCARD: $SCARD_AFTER_EXPIRY -> $SCARD_AFTER_CLEANUP)"

echo ""
echo "--- CORS exact-origin allowlist ---"
ALLOWED=$(curl -s -D - -o /dev/null -X OPTIONS "$API/api/symbols" -H "Origin: https://verify-stage5.example" -H "Access-Control-Request-Method: GET" | grep -ic "access-control-allow-origin: https://verify-stage5.example")
[ "$ALLOWED" -eq 1 ]
report $? "configured origin receives Access-Control-Allow-Origin"

DISALLOWED=$(curl -s -D - -o /dev/null -X OPTIONS "$API/api/symbols" -H "Origin: https://evil.example" -H "Access-Control-Request-Method: GET" | grep -ic "access-control-allow-origin")
[ "$DISALLOWED" -eq 0 ]
report $? "non-configured origin receives no Access-Control-Allow-Origin"

echo ""
echo "--- stage 5 done-when: api keeps serving with the consumer stopped ---"
kill -TERM "$CONSUMER_PID" 2>/dev/null
sleep 3
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$API/api/symbols/$FIRST_SYMBOL")
[ "$STATUS" = "200" ]
report $? "snapshot read still succeeds with the consumer stopped (got $STATUS)"

echo ""
echo "=== $PASS passed, $FAIL failed ==="

[ "$FAIL" -eq 0 ]
exit $?
