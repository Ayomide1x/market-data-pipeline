-- Makes atomic: incrementing a fixed-window request counter and setting its expiry only
-- on the first increment of the window. A plain INCR-then-EXPIRE from the client has a
-- gap where a crash between the two calls leaves the counter with no TTL at all —
-- permanently locking out whoever it belongs to, not a transient blip. See DECISIONS.md.
--
-- KEYS[1] = ratelimit:create:{clientId}
-- ARGV[1] = window length in seconds
-- Returns: the counter's new value after this increment.

local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
