-- Makes atomic: comparing the incoming record's partition against this symbol's stored
-- partition (checked first — offsets from different partitions are not comparable at
-- all, invariant #1), then, if it matches, comparing the incoming Kafka offset against
-- this symbol's stored lastOffset and, if greater, writing price/ema/exchangeTs/
-- lastOffset/partition in one operation (invariant #2). Returns the resulting state
-- either way, so the caller can resync its in-memory cache from Redis's actual current
-- state even on a skip (see DECISIONS.md — the cache-staleness bug this fixes).
--
-- KEYS[1] = state:{symbol}
-- ARGV[1..5] = newPrice, newEma, newExchangeTs, newOffset, newPartition
-- Returns: {status, price, ema, exchangeTs, lastOffset, partition}
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
return {'applied', newPrice, newEma, newExchangeTs, ARGV[4], newPartition}
