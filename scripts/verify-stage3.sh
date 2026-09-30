#!/usr/bin/env bash
# Mechanical checks for Stage 3. Run from anywhere: ./scripts/verify-stage3.sh
#
# The kill -9 crash-recovery test is NOT automated here — it's printed as a manual step
# at the end, per instructions (verifying that one yourself, not from this script's own
# test runs, is the point).
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
  # Every matching PID, not just the first — a pattern can match more than one process
  # (a stray from an earlier run, or from manual testing), and killing only one leaves
  # the other holding the consumer group, blocking things like an offset reset.
  ps aux | grep "$1" | grep -v grep | awk '{print $2}'
}

kill_all() {
  for pattern in "src/index.ts" "com.marketdatapipeline.consumer.ConsumerApplication"; do
    for pid in $(find_pids "$pattern"); do
      kill -TERM "$pid" 2>/dev/null
    done
  done
}

# Guaranteed cleanup: fires on normal completion, on any error under set -e-less linear
# flow, and on most signals (SIGTERM/SIGINT) — though not on SIGKILL, which no process can
# trap. Without this, a script that dies partway (a bug, an interrupt, this session's own
# sandbox killing a long-running command) orphans whatever it started at the "live smoke
# test" step below, with nothing left to stop them.
trap kill_all EXIT

echo "=== Stage 3 verification ==="

docker compose ps kafka 2>/dev/null | grep -q "healthy"
report $? "kafka container healthy"

docker compose ps redis 2>/dev/null | grep -q "healthy"
report $? "redis container healthy"

(cd consumer && ./mvnw -q compile) >/tmp/verify-stage3-compile.log 2>&1
report $? "consumer compiles (see /tmp/verify-stage3-compile.log on failure)"

(cd consumer && ./mvnw -q test) >/tmp/verify-stage3-test.log 2>&1
report $? "consumer unit tests pass — EmaCalculator (see /tmp/verify-stage3-test.log on failure)"

redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q PONG
report $? "redis reachable on 127.0.0.1:6379"

SHA=$(redis-cli -h 127.0.0.1 -p 6379 -x SCRIPT LOAD < consumer/src/main/resources/scripts/apply_tick.lua 2>/dev/null)
[ -n "$SHA" ]
report $? "apply_tick.lua loads into Redis (sha: ${SHA:-none})"

# Kill any already-running ingester/consumer before the live smoke test, so this
# doesn't accidentally verify a stray process instead of what it just started.
kill_all
sleep 2

SYMBOL=$(grep '^SYMBOLS=' ingester/.env 2>/dev/null | cut -d= -f2 | cut -d, -f1)
SYMBOL=${SYMBOL:-BTC-USD}

# Deliberately not deleting state:$SYMBOL here: it would let this "clean start" diverge
# from tools/verify-ema, which always recomputes from the topic's true offset 0 — a
# mid-history reset makes the two disagree by construction, not by bug (see below).

(cd ingester && npm run dev) >/tmp/verify-stage3-ingester.log 2>&1 &
(cd consumer && ./mvnw -q spring-boot:run) >/tmp/verify-stage3-consumer.log 2>&1 &

sleep 15

INGESTER_PID=$(find_pids "src/index.ts" | head -1)
CONSUMER_PID=$(find_pids "com.marketdatapipeline.consumer.ConsumerApplication" | head -1)

[ -n "$INGESTER_PID" ]
report $? "ingester started"
[ -n "$CONSUMER_PID" ]
report $? "consumer started"

HASH_LEN=$(redis-cli -h 127.0.0.1 -p 6379 HLEN "state:$SYMBOL" 2>/dev/null)
[ "$HASH_LEN" = "5" ]
report $? "state:$SYMBOL has all 5 fields after a live run (found ${HASH_LEN:-0})"

EMA_VAL=$(redis-cli -h 127.0.0.1 -p 6379 HGET "state:$SYMBOL" ema 2>/dev/null)
[[ "$EMA_VAL" =~ ^-?[0-9]+\.?[0-9]*([eE][-+]?[0-9]+)?$ ]]
report $? "ema field is a parseable number ($EMA_VAL)"

OFFSET_VAL=$(redis-cli -h 127.0.0.1 -p 6379 HGET "state:$SYMBOL" lastOffset 2>/dev/null)
[[ "$OFFSET_VAL" =~ ^[0-9]+$ ]]
report $? "lastOffset field is a non-negative integer ($OFFSET_VAL)"

PARTITION_VAL=$(redis-cli -h 127.0.0.1 -p 6379 HGET "state:$SYMBOL" partition 2>/dev/null)
[[ "$PARTITION_VAL" =~ ^[0-9]+$ ]]
report $? "partition field is a non-negative integer ($PARTITION_VAL)"

INCONSISTENCY_COUNT=$(grep -c "inconsistency" /tmp/verify-stage3-consumer.log 2>/dev/null)
INCONSISTENCY_COUNT=${INCONSISTENCY_COUNT:-0}
[ "$INCONSISTENCY_COUNT" = "0" ]
report $? "no false topic-recreation inconsistency alarms during the smoke test"

PARTITION_MISMATCH_COUNT=$(grep -c "partition mismatch" /tmp/verify-stage3-consumer.log 2>/dev/null)
PARTITION_MISMATCH_COUNT=${PARTITION_MISMATCH_COUNT:-0}
[ "$PARTITION_MISMATCH_COUNT" = "0" ]
report $? "no partition mismatch fired during the smoke test"

# Freeze the comparison point: stop producing, let the consumer fully drain, then stop
# it cleanly (SIGTERM, not -9) so Redis and the committed offset land in the same place
# tools/verify-ema will read.
[ -n "$INGESTER_PID" ] && kill -TERM "$INGESTER_PID" 2>/dev/null
sleep 5
[ -n "$CONSUMER_PID" ] && kill -TERM "$CONSUMER_PID" 2>/dev/null
sleep 3

./tools/verify-ema >/tmp/verify-stage3-verify-ema.log 2>&1
report $? "tools/verify-ema matches Redis (see /tmp/verify-stage3-verify-ema.log on failure)"
grep -E "^(PASS|FAIL)" /tmp/verify-stage3-verify-ema.log 2>/dev/null | sed 's/^/  /'

echo ""
echo "=== $PASS passed, $FAIL failed ==="
echo ""
echo "Manual step, not automated by this script (see CLAUDE.local.md — verify this one"
echo "yourself, not from a test run reported by the agent that wrote the code):"
echo "  1. With the stack up, start the ingester and consumer."
echo "  2. Find the consumer's real PID (ps aux | grep ConsumerApplication) and: kill -9 <pid>"
echo "  3. Restart it: (cd consumer && ./mvnw spring-boot:run)"
echo "  4. In its logs: no crash on startup; any offset already reflected in Redis is"
echo "     logged with \"applied\":false and an unchanged ema, not reprocessed into it."
echo "  5. Run ./tools/verify-ema and confirm PASS."

[ "$FAIL" -eq 0 ]
exit $?
