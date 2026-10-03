package com.marketdatapipeline.consumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

// Wraps the state:{symbol} Redis hash behind an in-memory cache — Redis/Kafka wiring,
// separate from the pure EMA math (see DECISIONS.md).
@Component
public class TickStateStore {

  private final StringRedisTemplate redisTemplate;
  private final RedisScript<List> applyTickScript;
  // Never keyed by partition (see DECISIONS.md) — the symbol set is already fully known
  // from config, and clearForPartitions resolves partition -> symbol by scanning values.
  private final Map<String, SymbolState> cache = new ConcurrentHashMap<>();

  public TickStateStore(StringRedisTemplate redisTemplate, RedisScript<List> applyTickScript) {
    this.redisTemplate = redisTemplate;
    this.applyTickScript = applyTickScript;
  }

  /** Lazily loads from Redis on first sight of this symbol; null means never seen at all. */
  public SymbolState load(String symbol) {
    return cache.computeIfAbsent(symbol, this::readFromRedis);
  }

  /**
   * Calls apply_tick.lua and always resyncs the cache from its returned state, whether
   * applied or skipped — a skip means Redis is ahead of what Java computed, not behind
   * (see DECISIONS.md for the cache-staleness bug this fixes).
   */
  public ApplyResult applyTick(String symbol, SymbolState candidate) {
    List<String> result = redisTemplate.execute(
        applyTickScript,
        List.of(stateKey(symbol)),
        candidate.price(),
        Double.toString(candidate.ema()),
        Long.toString(candidate.exchangeTs()),
        Long.toString(candidate.lastOffset()),
        Integer.toString(candidate.partition()));

    String status = result.get(0);
    SymbolState currentState = new SymbolState(
        result.get(1),
        Double.parseDouble(result.get(2)),
        Long.parseLong(result.get(3)),
        Long.parseLong(result.get(4)),
        Integer.parseInt(result.get(5)));

    if ("partition_mismatch".equals(status)) {
      throw new PartitionMismatchException(symbol, candidate.partition(), currentState.partition());
    }

    cache.put(symbol, currentState);
    return new ApplyResult("applied".equals(status), currentState);
  }

  /** Direct single-symbol clear — used by clearForPartitions below. */
  public void clear(String symbol) {
    cache.remove(symbol);
  }

  /**
   * Stage 4's revocation hook: clears every cached symbol whose partition is in
   * {@code revokedPartitions}. Also clears any entry still at partition -1 (written
   * before this field existed, never yet refreshed by a successful applyTick) — its real
   * partition is unknown, so it might be one of the revoked ones; -1 can't be excluded
   * just because it isn't literally in the revoked set. See DECISIONS.md.
   */
  public void clearForPartitions(Set<Integer> revokedPartitions) {
    cache.entrySet()
        .removeIf(entry -> {
          int partition = entry.getValue().partition();
          return partition == -1 || revokedPartitions.contains(partition);
        });
  }

  private SymbolState readFromRedis(String symbol) {
    HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
    Map<String, String> hash = hashOps.entries(stateKey(symbol));
    if (hash.isEmpty()) {
      return null;
    }
    String partition = hash.get("partition");
    return new SymbolState(
        hash.get("price"),
        Double.parseDouble(hash.get("ema")),
        Long.parseLong(hash.get("exchangeTs")),
        Long.parseLong(hash.get("lastOffset")),
        partition == null ? -1 : Integer.parseInt(partition));
  }

  private static String stateKey(String symbol) {
    return "state:" + symbol;
  }

  public record ApplyResult(boolean applied, SymbolState currentState) {}
}
