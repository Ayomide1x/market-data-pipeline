package com.marketdatapipeline.api;

import java.util.Map;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

// Reads only from Redis, holds no cache of its own (invariant #7) — every call is a fresh
// HGETALL, never a reused in-memory value. See DECISIONS.md.
@Component
public class SnapshotService {

  private final StringRedisTemplate redisTemplate;

  public SnapshotService(StringRedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  // Assumes symbol is already known to be configured — callers (SymbolController) are
  // responsible for that distinction (unknown symbol vs. configured-but-no-data-yet are
  // different things, see DECISIONS.md).
  public SymbolSnapshot snapshotFor(String symbol) {
    Map<String, String> hash = redisTemplate.<String, String>opsForHash().entries("state:" + symbol);
    if (hash.isEmpty()) {
      return new SymbolSnapshot(symbol, false, null, null, null, null);
    }

    long exchangeTs = Long.parseLong(hash.get("exchangeTs"));
    // Wall clock here measures staleness for display, not the EMA itself (invariant #6 is
    // about the EMA computation, not this) — the one place in this project wall-clock time
    // is the right tool, not the wrong one. See DECISIONS.md.
    long ageMs = System.currentTimeMillis() - exchangeTs;

    return new SymbolSnapshot(
        symbol, true, hash.get("price"), Double.parseDouble(hash.get("ema")), exchangeTs, ageMs);
  }
}
