-- Makes atomic: comparing the incoming record's partition against this symbol's stored
-- partition (checked first — offsets from different partitions are not comparable at
-- all, invariant #1), then, if it matches, comparing the incoming Kafka offset against
-- this symbol's stored lastOffset and, if greater, writing price/ema/exchangeTs/
-- lastOffset/partition in one operation (invariant #2), marking any alerts the caller
-- determined crossed as fired in that same write (invariant #5), and PUBLISHing a tick
-- update plus one alert-fired notification per alert actually marked — all inside the
-- same atomic execution, only when applied. PUBLISH from EVAL verified live against this
-- project's actual (standalone, no replica) Redis 8.2.9 before relying on it: delivers
-- correctly to a real subscriber, no restriction/warning, and — as expected, since it
-- touches no key — is not written to the AOF. See DECISIONS.md for that verification,
-- the Java-side alternative it was weighed against, and why the general "semantics
-- differ across replication modes" caveat doesn't bite this specific deployment.
-- Returns the resulting state either way, so the caller can resync its in-memory cache
-- from Redis's actual current state even on a skip (see DECISIONS.md — the
-- cache-staleness bug this fixes, which Stage 6's crossing detection also depends on).
--
-- KEYS[1] = state:{symbol}
-- KEYS[2..] = alert:{id} for each alert the caller (Java) already determined crossed on
--   this tick — zero or more. Only ever marked fired if the tick is actually applied
--   below (never on skipped/partition_mismatch — this is what makes firing replay-safe,
--   see DECISIONS.md), and only if the key still exists: an alert deleted by the api
--   between Java's read and this call is silently never fired, not resurrected as a
--   malformed hash (see DECISIONS.md for the race this guards against).
-- ARGV[1..5] = newPrice, newEma, newExchangeTs, newOffset, newPartition
-- ARGV[6] = symbol, bare (for the ticks:updates/alerts:fired payloads — an explicit
--   value, not parsed back out of KEYS[1], so a future key-naming change can't silently
--   desync the publish payload from the key it was derived from).
-- ARGV[7..] = bare alert ids, one per extra KEYS entry, in the same order as KEYS[2..] —
--   lets the alerts:fired payload include "id" without parsing it out of a key name.
-- Returns: {status, price, ema, exchangeTs, lastOffset, partition} — unchanged shape;
-- the caller already knows which ids it submitted, so "applied" alone tells it they fired.
-- status is "applied", "skipped", or "partition_mismatch"; the five trailing fields are
-- always the hash's resulting current state (unchanged on partition_mismatch or skipped).

local key = KEYS[1]
local newPrice = ARGV[1]
local newEma = ARGV[2]
local newExchangeTs = ARGV[3]
local newOffset = tonumber(ARGV[4])
local newPartition = ARGV[5]

local storedPartition = redis.call('HGET', key, 'partition')

if storedPartition and storedPartition ~= newPartition then
  local price = redis.call('HGET', key, 'price')
  local ema = redis.call('HGET', key, 'ema')
  local exchangeTs = redis.call('HGET', key, 'exchangeTs')
  local lastOffset = redis.call('HGET', key, 'lastOffset')
  return {'partition_mismatch', price, ema, exchangeTs, lastOffset, storedPartition}
end

local storedOffset = tonumber(redis.call('HGET', key, 'lastOffset')) or -1

if newOffset <= storedOffset then
  local price = redis.call('HGET', key, 'price')
  local ema = redis.call('HGET', key, 'ema')
  local exchangeTs = redis.call('HGET', key, 'exchangeTs')
  return {'skipped', price, ema, exchangeTs, tostring(storedOffset), storedPartition or newPartition}
end

redis.call('HSET', key,
  'price', newPrice, 'ema', newEma, 'exchangeTs', newExchangeTs,
  'lastOffset', ARGV[4], 'partition', newPartition)

redis.call('PUBLISH', 'ticks:updates', cjson.encode({
  symbol = ARGV[6], price = newPrice, ema = tonumber(newEma), exchangeTs = tonumber(newExchangeTs)
}))

for i = 2, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    -- direction/threshold read here, not passed in from Java: Java never had them in
    -- hand (only the key and offset) — a small cost (one HMGET) paid only on the rare
    -- branch where an alert actually fires (at most once per alert, ever, per invariant
    -- #5), not on every tick. See DECISIONS.md for the rejected alternative (publish a
    -- bare {id, symbol, offset} and make the client fetch details separately).
    local fields = redis.call('HMGET', KEYS[i], 'direction', 'threshold')
    redis.call('HSET', KEYS[i], 'status', 'fired', 'firedAtOffset', ARGV[4])
    redis.call('PUBLISH', 'alerts:fired', cjson.encode({
      id = ARGV[5 + i], symbol = ARGV[6], direction = fields[1], threshold = fields[2],
      firedAtOffset = tonumber(ARGV[4])
    }))
  end
end

return {'applied', newPrice, newEma, newExchangeTs, ARGV[4], newPartition}
