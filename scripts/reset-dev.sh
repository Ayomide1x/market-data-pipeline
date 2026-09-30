#!/usr/bin/env bash
# Full reset of the local dev stack. Destructive by design: wipes the kafka-data and
# redis-data volumes (docker compose down -v), brings the stack back up fresh, and
# confirms the topic and Redis actually landed in the expected clean state rather than
# just assuming `up` worked. Run via: make reset
#
# Why this exists: synthetic records produced directly into `ticks` for testing (e.g. via
# kcat, to reproduce a specific offset/partition scenario) persist in the topic exactly
# like real data — there is no "test topic" separate from the real one in this project's
# design. tools/verify-ema recomputes from the topic's actual start, so leftover synthetic
# records get replayed as if they were real, silently skewing the result. See DECISIONS.md
# for the rule this script exists to make cheap to follow: reset after any such test.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Resetting dev stack (docker compose down -v) ==="
docker compose down -v

echo "=== Bringing the stack back up ==="
docker compose up -d

echo "=== Waiting for kafka and redis to report healthy ==="
KAFKA_HEALTHY=0
REDIS_HEALTHY=0
for _ in $(seq 1 30); do
  KAFKA_HEALTHY=$(docker compose ps kafka 2>/dev/null | grep -c "healthy")
  REDIS_HEALTHY=$(docker compose ps redis 2>/dev/null | grep -c "healthy")
  [ "$KAFKA_HEALTHY" = "1" ] && [ "$REDIS_HEALTHY" = "1" ] && break
  sleep 2
done

if [ "$KAFKA_HEALTHY" != "1" ] || [ "$REDIS_HEALTHY" != "1" ]; then
  echo "FAIL: kafka/redis did not both report healthy within 60s" >&2
  exit 1
fi
echo "kafka and redis healthy"

echo "=== Confirming topic ticks exists with 6 partitions ==="
DESCRIBE=""
for _ in $(seq 1 30); do
  if DESCRIBE=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --describe --topic ticks 2>/dev/null); then
    [ -n "$DESCRIBE" ] && break
  fi
  sleep 2
done

PARTITION_COUNT=$(echo "$DESCRIBE" | grep -o "PartitionCount: [0-9]*" | awk '{print $2}')
if [ "$PARTITION_COUNT" != "6" ]; then
  echo "FAIL: expected topic ticks to have 6 partitions, found '${PARTITION_COUNT:-none}'" >&2
  echo "$DESCRIBE" >&2
  exit 1
fi
echo "topic ticks confirmed with 6 partitions"

echo "=== Confirming Redis is empty ==="
DB_SIZE=$(redis-cli -h 127.0.0.1 -p 6379 DBSIZE 2>/dev/null)
if [ "$DB_SIZE" != "0" ]; then
  echo "FAIL: expected an empty Redis, found $DB_SIZE key(s)" >&2
  exit 1
fi
echo "redis confirmed empty"

echo ""
echo "=== Dev stack reset complete: fresh topic (6 partitions), empty Redis ==="
