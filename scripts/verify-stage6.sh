#!/usr/bin/env bash
# Mechanical + live checks for Stage 6 (alert evaluation in the consumer).
# Run after `make reset`, from anywhere: ./scripts/verify-stage6.sh
#
# Starts ingester + consumer + api, creates an alert right at the live price (so ordinary
# market jitter crosses it within seconds — no synthetic ticks needed for this check, see
# DECISIONS.md on why synthetic kcat injection stays a one-off manual reproduction
# technique, not something this repeatable script does), waits for it to fire exactly
# once, then proves the Stage 6 "done when": kill -9 the consumer right after firing,
# restart it, and confirm the alert does not fire again. Finishes with tools/verify-ema
# as a regression check, same as Stage 3/4/5's scripts.
#
# The negative test the design review required (temporarily removing AlertStore's
# armed-only filter, confirming a double-fire, restoring it, confirming a single fire)
# was done by hand against this exact scenario during development, not automated here —
# same precedent as Stage 4's clearForPartitions negative test. See DECISIONS.md for the
# observed evidence from both runs.
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

# "Stopped" isn't "drained": after the ingester stops, the consumer can still be sitting
# on a real backlog (e.g. whatever accumulated during a kill -9 window), and a fixed sleep
# can't know how long that takes. Polls kafka-consumer-groups --describe until every
# partition that actually carries data (LOG-END-OFFSET > 0) shows LAG 0, or fails loudly
# on timeout rather than letting the caller run verify-ema against an undrained group. See
# DECISIONS.md — this is the second time a script running verify-ema against a live/
# not-yet-drained stack produced a false FAIL.
wait_for_lag_zero() {
  local timeout_seconds=$1
  local waited=0
  local output=""
  while [ "$waited" -lt "$timeout_seconds" ]; do
    output=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --describe --group ema-consumers 2>/dev/null)
    local datacarrying undrained
    datacarrying=$(echo "$output" | awk '$1=="ema-consumers" && $5+0 > 0 {print}')
    # An empty result here is NOT the same as "drained" — it also matches "the group has
    # no members yet" and "describe's output didn't parse at all." Only lag computed over
    # at least one real data-carrying partition row counts as evidence of draining.
    if [ -n "$datacarrying" ]; then
      undrained=$(echo "$datacarrying" | awk '$6+0 != 0 {print}')
      if [ -z "$undrained" ]; then
        return 0
      fi
    fi
    sleep 2
    waited=$((waited + 2))
  done
  echo "FAIL: lag never reached 0 within ${timeout_seconds}s — last describe output:" >&2
  echo "$output" | sed 's/^/  /' >&2
  return 1
}

trap kill_all EXIT

API=http://127.0.0.1:8080

echo "=== Stage 6 verification ==="

docker compose ps kafka 2>/dev/null | grep -q "healthy"
report $? "kafka container healthy"

docker compose ps redis 2>/dev/null | grep -q "healthy"
report $? "redis container healthy"

(cd consumer && ./mvnw -q compile) >/tmp/verify-stage6-compile.log 2>&1
report $? "consumer compiles (see /tmp/verify-stage6-compile.log on failure)"

(cd consumer && ./mvnw -q test) >/tmp/verify-stage6-test.log 2>&1
report $? "consumer unit tests pass — EmaCalculator + CrossingDetector (see /tmp/verify-stage6-test.log on failure)"

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
(cd ingester && npm run dev) >/tmp/verify-stage6-ingester.log 2>&1 &
sleep 5
INGESTER_PID=$(find_pids "src/index.ts" | head -1)
(cd consumer && CONSUMER_CLIENT_ID=consumer-stage6 ./mvnw -q spring-boot:run) >/tmp/verify-stage6-consumer.log 2>&1 &
sleep 12
(cd api && SYMBOLS="$SYMBOLS_RAW" ALERT_GLOBAL_CAP=50 ALERT_RATE_LIMIT_PER_MINUTE=50 ./mvnw -q spring-boot:run) >/tmp/verify-stage6-api.log 2>&1 &
sleep 12

echo ""
echo "--- creating an alert right at the live price, waiting for ordinary market jitter to cross it ---"
LIVE_PRICE=$(curl -s "$API/api/symbols/$FIRST_SYMBOL" | python3 -c "import json,sys; print(json.load(sys.stdin)['price'])" 2>/dev/null)
[ -n "$LIVE_PRICE" ] && [ "$LIVE_PRICE" != "None" ]
report $? "got a live price for $FIRST_SYMBOL to anchor the alert threshold to (got: ${LIVE_PRICE:-none})"

RESP=$(curl -s -X POST "$API/api/alerts" -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"$FIRST_SYMBOL\",\"direction\":\"above\",\"threshold\":\"$LIVE_PRICE\"}")
ALERT_ID=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
[ -n "$ALERT_ID" ]
report $? "alert created at threshold=$LIVE_PRICE for $FIRST_SYMBOL (id: ${ALERT_ID:-none})"

FIRED=1
FIRED_AT_OFFSET_1=""
for i in $(seq 1 30); do
  STATUS_FIELD=$(redis-cli -h 127.0.0.1 -p 6379 HGET "alert:$ALERT_ID" status 2>/dev/null)
  if [ "$STATUS_FIELD" = "fired" ]; then
    FIRED=0
    FIRED_AT_OFFSET_1=$(redis-cli -h 127.0.0.1 -p 6379 HGET "alert:$ALERT_ID" firedAtOffset 2>/dev/null)
    break
  fi
  sleep 1
done
report $FIRED "alert fired exactly once within 30s of ordinary market movement (firedAtOffset=${FIRED_AT_OFFSET_1:-none})"

FIRE_COUNT=$(grep -c "\"alertId\":\"$ALERT_ID\"" /tmp/verify-stage6-consumer.log 2>/dev/null)
[ "$FIRE_COUNT" = "1" ]
report $? "exactly one \"alert fired\" log line for this id (found: ${FIRE_COUNT:-0})"

echo ""
echo "--- stage 6 done-when: kill -9 right after firing, restart, confirm no re-fire ---"
CONSUMER_PID=$(find_pids "com.marketdatapipeline.consumer.ConsumerApplication" | head -1)
[ -n "$CONSUMER_PID" ]
report $? "found the consumer process to kill -9 (pid: ${CONSUMER_PID:-none})"
kill -9 "$CONSUMER_PID" 2>/dev/null
sleep 3

(cd consumer && CONSUMER_CLIENT_ID=consumer-stage6-restart ./mvnw -q spring-boot:run) >>/tmp/verify-stage6-consumer.log 2>&1 &
sleep 15
CONSUMER_PID=$(find_pids "com.marketdatapipeline.consumer.ConsumerApplication" | head -1)

STATUS_AFTER=$(redis-cli -h 127.0.0.1 -p 6379 HGET "alert:$ALERT_ID" status 2>/dev/null)
[ "$STATUS_AFTER" = "fired" ]
report $? "alert status is still \"fired\" after restart (got: ${STATUS_AFTER:-none})"

FIRED_AT_OFFSET_2=$(redis-cli -h 127.0.0.1 -p 6379 HGET "alert:$ALERT_ID" firedAtOffset 2>/dev/null)
[ "$FIRED_AT_OFFSET_1" = "$FIRED_AT_OFFSET_2" ]
report $? "firedAtOffset unchanged across the restart ($FIRED_AT_OFFSET_1 -> $FIRED_AT_OFFSET_2) — no re-fire"

FIRE_COUNT_AFTER=$(grep -c "\"alertId\":\"$ALERT_ID\"" /tmp/verify-stage6-consumer.log 2>/dev/null)
[ "$FIRE_COUNT_AFTER" = "1" ]
report $? "still exactly one \"alert fired\" log line for this id after restart (found: ${FIRE_COUNT_AFTER:-0})"

echo ""
echo "--- freezing the comparison point: stop producing, drain, then stop the consumer cleanly ---"
[ -n "$INGESTER_PID" ] && kill -TERM "$INGESTER_PID" 2>/dev/null
for pid in $(find_pids "com.marketdatapipeline.api.ApiApplication"); do kill -TERM "$pid" 2>/dev/null; done
sleep 3

wait_for_lag_zero 60
DRAINED=$?
report $DRAINED "consumer group lag reached 0 on every data-carrying partition before stopping the consumer"

# Only stop the consumer — cleanly, with SIGTERM — once it's actually caught up;
# stopping it undrained is exactly the false-FAIL this is here to prevent. If it never
# drained, still stop it (nothing to gain from leaving it running) but verify-ema is
# expected to disagree with Redis in that case, not a regression.
[ -n "$CONSUMER_PID" ] && kill -TERM "$CONSUMER_PID" 2>/dev/null
sleep 3

echo ""
echo "--- regression: tools/verify-ema still matches after alert evaluation changes ---"
./tools/verify-ema >/tmp/verify-stage6-verify-ema.log 2>&1
report $? "tools/verify-ema matches Redis (see /tmp/verify-stage6-verify-ema.log on failure)"
grep -E "^(PASS|FAIL)" /tmp/verify-stage6-verify-ema.log 2>/dev/null | sed 's/^/  /'

echo ""
echo "=== $PASS passed, $FAIL failed ==="

[ "$FAIL" -eq 0 ]
exit $?
