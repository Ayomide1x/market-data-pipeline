#!/usr/bin/env bash
# Mechanical + live checks for Stage 9 (production hardening, run locally first).
# Run from anywhere: SYMBOLS=BTC-USD,ETH-USD,SOL-USD CORS_ORIGIN=https://example.com ./scripts/verify-stage9.sh
#
# Builds all three app images explicitly for linux/arm64 (the Oracle Ampere target) and
# inspects each manifest's actual architecture rather than trusting the --platform flag;
# brings up compose.prod.yaml locally; confirms every service reports healthy; confirms
# docker stats shows each under its configured memory limit; confirms log rotation,
# bounded restart policies, and Redis's maxmemory/noeviction are actually set on the
# running containers, not just declared in the file; confirms real data actually flows
# end to end through the containerized stack.
#
# What this does NOT do, by design (see DECISIONS.md): the real alertLookupMicros
# re-measurement on the actual Oracle instance, and supplying real Sentry DSNs / the
# real CORS origin — those are Stage 10 deploy-time steps, printed as a reminder at the
# end, not fabricated here. It also doesn't run the multi-hour soak CLAUDE.md's own
# done-when asks for ("runs locally for several hours, disk usage flat") — that's a
# separate, manual, long-running check, same precedent as Stage 3's manual kill-9 test.
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

: "${SYMBOLS:=BTC-USD,ETH-USD,SOL-USD}"
: "${CORS_ORIGIN:=https://verify-stage9.example}"
export SYMBOLS CORS_ORIGIN

COMPOSE="docker compose -f compose.yaml -f compose.prod.yaml"

cleanup() {
  $COMPOSE down >/dev/null 2>&1
}
trap cleanup EXIT

echo "=== Stage 9 verification ==="

docker buildx version >/dev/null 2>&1
report $? "docker buildx available"

echo ""
echo "--- building all three images for linux/arm64, inspecting actual manifests ---"
for svc in ingester consumer api; do
  docker buildx build --platform linux/arm64 -t "mdp-${svc}:verify-stage9" --load "./${svc}" >/tmp/verify-stage9-build-${svc}.log 2>&1
  report $? "${svc} image builds for linux/arm64 (see /tmp/verify-stage9-build-${svc}.log on failure)"

  ARCH=$(docker image inspect "mdp-${svc}:verify-stage9" --format '{{.Architecture}}' 2>/dev/null)
  [ "$ARCH" = "arm64" ]
  report $? "${svc} image manifest reports arm64 (got: ${ARCH:-none})"
done

echo ""
echo "--- bringing up compose.prod.yaml ---"
$COMPOSE down >/dev/null 2>&1
$COMPOSE up -d --build >/tmp/verify-stage9-up.log 2>&1
report $? "compose.prod.yaml stack starts (see /tmp/verify-stage9-up.log on failure)"

echo "waiting for every service to report healthy (up to 90s)..."
ALL_HEALTHY=1
for i in $(seq 1 18); do
  STATUSES=$($COMPOSE ps --format '{{.Service}} {{.Health}}' 2>/dev/null)
  UNHEALTHY=$(echo "$STATUSES" | grep -v "healthy$" | grep -v "^$")
  if [ -z "$UNHEALTHY" ] && [ -n "$STATUSES" ]; then
    ALL_HEALTHY=0
    break
  fi
  sleep 5
done
report $ALL_HEALTHY "every service reports healthy within 90s"
$COMPOSE ps --format '{{.Service}}: {{.Health}}' 2>/dev/null | sed 's/^/  /'

echo ""
echo "--- memory limits actually enforced, not just declared ---"
for svc in kafka redis ingester consumer api; do
  CID=$($COMPOSE ps -q "$svc" 2>/dev/null)
  LIMIT=$(docker inspect "$CID" --format '{{.HostConfig.Memory}}' 2>/dev/null)
  [ -n "$LIMIT" ] && [ "$LIMIT" != "0" ]
  report $? "$svc has a nonzero enforced memory limit (got: ${LIMIT:-none} bytes)"
done

echo ""
echo "--- docker stats: every service under its configured limit ---"
STATS=$(docker stats --no-stream --format '{{.Name}} {{.MemPerc}}' 2>/dev/null)
echo "$STATS" | sed 's/^/  /'
OVER_LIMIT=$(echo "$STATS" | awk '{gsub("%","",$2); if ($2+0 >= 100) print}')
[ -z "$OVER_LIMIT" ]
report $? "no container is at or over 100% of its memory limit"

echo ""
echo "--- log rotation set on every app service ---"
for svc in ingester consumer api kafka redis; do
  CID=$($COMPOSE ps -q "$svc" 2>/dev/null)
  # index, not dot-access: Go templates can't dot-reference a map key containing a
  # hyphen (confirmed live — dot-access silently returned empty, not an error).
  LOG_CONFIG=$(docker inspect "$CID" --format '{{.HostConfig.LogConfig.Type}} {{index .HostConfig.LogConfig.Config "max-size"}}' 2>/dev/null)
  [ "$LOG_CONFIG" = "json-file 10m" ]
  report $? "$svc has json-file logging with max-size=10m (got: ${LOG_CONFIG:-none})"
done

echo ""
echo "--- restart policies bounded, not always/unbounded ---"
for svc in ingester consumer; do
  CID=$($COMPOSE ps -q "$svc" 2>/dev/null)
  POLICY=$(docker inspect "$CID" --format '{{.HostConfig.RestartPolicy.Name}} {{.HostConfig.RestartPolicy.MaximumRetryCount}}' 2>/dev/null)
  NAME=$(echo "$POLICY" | awk '{print $1}')
  COUNT=$(echo "$POLICY" | awk '{print $2}')
  [ "$NAME" = "on-failure" ] && [ "$COUNT" -gt 0 ] 2>/dev/null
  report $? "$svc restart policy is bounded on-failure (got: ${POLICY:-none}) — see DECISIONS.md: the counter resets on a daemon restart, this isn't a permanent fix"
done

echo ""
echo "--- Redis maxmemory/noeviction actually applied ---"
REDIS_CID=$($COMPOSE ps -q redis 2>/dev/null)
MAXMEM=$(docker exec "$REDIS_CID" redis-cli CONFIG GET maxmemory 2>/dev/null | tail -1)
[ "$MAXMEM" = "268435456" ]
report $? "redis maxmemory is 256mb (got: ${MAXMEM:-none} bytes)"

MAXMEM_POLICY=$(docker exec "$REDIS_CID" redis-cli CONFIG GET maxmemory-policy 2>/dev/null | tail -1)
[ "$MAXMEM_POLICY" = "noeviction" ]
report $? "redis maxmemory-policy is noeviction (got: ${MAXMEM_POLICY:-none})"

echo ""
echo "--- real data flows end to end through the containerized stack ---"
sleep 10
FIRST_SYMBOL=$(echo "$SYMBOLS" | cut -d, -f1)
STATE=$(docker exec "$REDIS_CID" redis-cli HGET "state:$FIRST_SYMBOL" price 2>/dev/null)
[ -n "$STATE" ]
report $? "state:$FIRST_SYMBOL has a real price in Redis (got: ${STATE:-none})"

CONSUMER_CID=$($COMPOSE ps -q consumer 2>/dev/null)
ALERT_LOOKUP_PRESENT=$(docker logs "$CONSUMER_CID" 2>&1 | grep -c "alertLookupMicros")
[ "$ALERT_LOOKUP_PRESENT" -gt 0 ]
report $? "alertLookupMicros is present in the consumer's logs (found $ALERT_LOOKUP_PRESENT lines) — the field Stage 10 will read to do the real arm64 re-measurement"

echo ""
echo "=== $PASS passed, $FAIL failed ==="
echo ""
echo "NOT covered by this script, by design — do these separately:"
echo "  1. The multi-hour soak CLAUDE.md's own done-when asks for: leave this stack up"
echo "     for several hours and confirm docker stats stays flat and disk usage doesn't"
echo "     grow unbounded."
echo "  2. The real alertLookupMicros re-measurement — this script only confirmed the"
echo "     field is present and logging; the actual numbers need the real Oracle Ampere"
echo "     instance, not this machine (different silicon, different storage backend —"
echo "     see DECISIONS.md)."
echo "  3. Real SENTRY_DSN and CORS_ORIGIN values — both wired as mechanisms here,"
echo "     both deliberately placeholder/blank; the real values are Stage 10's."

[ "$FAIL" -eq 0 ]
exit $?
