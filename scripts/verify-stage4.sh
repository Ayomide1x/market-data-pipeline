#!/usr/bin/env bash
# Mechanical + live checks for Stage 4 (consumer group scaling and rebalancing).
# Run after `make reset`, from anywhere: ./scripts/verify-stage4.sh
#
# This starts two consumer instances, confirms partitions actually split between them (not
# just that one happens to be idle), kills one, and confirms the survivor reabsorbs the
# exact partition numbers that moved — not just that lag returns to 0, which would also be
# true if the survivor silently never picked them up. It then runs tools/verify-ema as a
# regression test for the cache-clear-on-revocation fix: a symbol whose partition moved
# away and back is the only way this test can actually exercise that code path (see
# DECISIONS.md — this exact script, with the clear temporarily disabled, is what confirmed
# the test fails without the fix before it shipped).
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
  for pattern in "src/index.ts" "com.marketdatapipeline.consumer.ConsumerApplication"; do
    for pid in $(find_pids "$pattern"); do
      kill -TERM "$pid" 2>/dev/null
    done
  done
}

trap kill_all EXIT

# Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
partitions_for_client() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ema-consumers 2>/dev/null \
    | awk -v cid="$1" '$9 == cid {print $3}' | sort -n
}

contains() {
  local needle=$1; shift
  for x in "$@"; do [ "$x" = "$needle" ] && return 0; done
  return 1
}

echo "=== Stage 4 verification ==="

docker compose ps kafka 2>/dev/null | grep -q "healthy"
report $? "kafka container healthy"

docker compose ps redis 2>/dev/null | grep -q "healthy"
report $? "redis container healthy"

(cd consumer && ./mvnw -q compile) >/tmp/verify-stage4-compile.log 2>&1
report $? "consumer compiles (see /tmp/verify-stage4-compile.log on failure)"

(cd consumer && ./mvnw -q test) >/tmp/verify-stage4-test.log 2>&1
report $? "consumer unit tests pass (see /tmp/verify-stage4-test.log on failure)"

SHA=$(redis-cli -h 127.0.0.1 -p 6379 -x SCRIPT LOAD < consumer/src/main/resources/scripts/apply_tick.lua 2>/dev/null)
[ -n "$SHA" ]
report $? "apply_tick.lua loads into Redis"

SYMBOLS_RAW=$(grep '^SYMBOLS=' ingester/.env 2>/dev/null | cut -d= -f2)
IFS=',' read -ra SYMBOLS <<< "$SYMBOLS_RAW"
[ "${#SYMBOLS[@]}" -ge 2 ]
report $? "SYMBOLS configures at least 2 symbols (found: ${SYMBOLS_RAW:-none}) — a single symbol leaves 5 of 6 partitions empty and can't prove a rebalance moved anything (see DECISIONS.md)"

kill_all
sleep 2

echo ""
echo "--- starting ingester + instance A ---"
(cd ingester && npm run dev) >/tmp/verify-stage4-ingester.log 2>&1 &
INGESTER_PID=$!
sleep 5

(cd consumer && CONSUMER_CLIENT_ID=consumer-a ./mvnw -q spring-boot:run) >/tmp/verify-stage4-a.log 2>&1 &
A_PID=$!
sleep 12

A_PARTITIONS_SOLO=($(partitions_for_client "consumer-a-0"))
[ "${#A_PARTITIONS_SOLO[@]}" -eq 6 ]
report $? "instance A alone owns all 6 partitions (found: ${A_PARTITIONS_SOLO[*]:-none})"

DISTINCT_PARTITIONS=()
for s in "${SYMBOLS[@]}"; do
  p=$(redis-cli -h 127.0.0.1 -p 6379 HGET "state:$s" partition 2>/dev/null)
  [ -n "$p" ] && ! contains "$p" "${DISTINCT_PARTITIONS[@]}" && DISTINCT_PARTITIONS+=("$p")
done
[ "${#DISTINCT_PARTITIONS[@]}" -ge 2 ]
report $? "configured symbols actually span more than one partition (found: ${DISTINCT_PARTITIONS[*]:-none})"

echo ""
echo "--- starting instance B ---"
(cd consumer && CONSUMER_CLIENT_ID=consumer-b ./mvnw -q spring-boot:run) >/tmp/verify-stage4-b.log 2>&1 &
B_PID=$!
sleep 12

A_PARTITIONS=($(partitions_for_client "consumer-a-0"))
B_PARTITIONS=($(partitions_for_client "consumer-b-0"))
[ "${#A_PARTITIONS[@]}" -ge 1 ] && [ "${#B_PARTITIONS[@]}" -ge 1 ]
report $? "partitions actually split between the two instances (A: ${A_PARTITIONS[*]:-none}, B: ${B_PARTITIONS[*]:-none}) — not all 6 still on one"

MOVED_SYMBOLS=()
for s in "${SYMBOLS[@]}"; do
  p=$(redis-cli -h 127.0.0.1 -p 6379 HGET "state:$s" partition 2>/dev/null)
  contains "$p" "${B_PARTITIONS[@]}" && MOVED_SYMBOLS+=("$s")
done
[ "${#MOVED_SYMBOLS[@]}" -ge 1 ]
report $? "at least one configured symbol's partition moved to instance B (moved: ${MOVED_SYMBOLS[*]:-none}) — this is what the cache-clear regression test below actually exercises"

echo ""
echo "--- letting instance B trade, then killing it ---"
sleep 8
kill -TERM "$B_PID"
sleep 10

A_PARTITIONS_AFTER=($(partitions_for_client "consumer-a-0"))
ALL_BACK=0
for p in "${B_PARTITIONS[@]}"; do
  contains "$p" "${A_PARTITIONS_AFTER[@]}" || ALL_BACK=1
done
report $ALL_BACK "instance A reabsorbed the exact partition numbers B held (B had: ${B_PARTITIONS[*]:-none}; A now has: ${A_PARTITIONS_AFTER[*]:-none}) — not just that lag returned to 0"

echo ""
echo "--- letting instance A process a few more ticks, then freezing for comparison ---"
sleep 5
kill -TERM "$INGESTER_PID"
sleep 5
kill -TERM "$A_PID"
sleep 4

./tools/verify-ema >/tmp/verify-stage4-verify-ema.log 2>&1
report $? "tools/verify-ema matches Redis after the rebalance — cache-clear regression test (see /tmp/verify-stage4-verify-ema.log on failure)"
grep -E "^(PASS|FAIL)" /tmp/verify-stage4-verify-ema.log 2>/dev/null | sed 's/^/  /'

echo ""
echo "=== $PASS passed, $FAIL failed ==="

[ "$FAIL" -eq 0 ]
exit $?
