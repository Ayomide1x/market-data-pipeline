-- Makes atomic: comparing the incoming record's partition against this symbol's stored
-- partition (checked first — offsets from different partitions are not comparable at
-- all, invariant #1), then, if it matches, comparing the incoming Kafka offset against
-- this symbol's stored lastOffset and, if greater, writing price/ema/exchangeTs/
-- lastOffset/partition in one operation (invariant #2), and marking any alerts the
-- caller determined crossed as fired, in that same write (invariant #5). Returns the
-- resulting state either way, so the caller can resync its in-memory cache from Redis's
-- actual current state even on a skip (see DECISIONS.md — the cache-staleness bug this
-- fixes, which Stage 6 now also depends on for correct crossing detection).
--
-- KEYS[1] = state:{symbol}
-- KEYS[2..] = alert:{id} for each alert the caller (Java) already determined crossed on
--   this tick — zero or more. Only ever marked fired if the tick is actually applied
--   below (never on skipped/partition_mismatch — this is what makes firing replay-safe,
--   see DECISIONS.md), and only if the key still exists: an alert deleted by the api
--   between Java's read and this call is silently never fired, not resurrected as a
--   malformed hash (see DECISIONS.md for the race this guards against).
-- ARGV[1..5] = newPrice, newEma, newExchangeTs, newOffset, newPartition
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

for i = 2, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    redis.call('HSET', KEYS[i], 'status', 'fired', 'firedAtOffset', ARGV[4])
  end
end

return {'applied', newPrice, newEma, newExchangeTs, ARGV[4], newPartition}
