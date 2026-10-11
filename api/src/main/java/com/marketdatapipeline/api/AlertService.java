package com.marketdatapipeline.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

@Component
public class AlertService {

  private static final long RATE_LIMIT_WINDOW_SECONDS = 60L;

  private final StringRedisTemplate redisTemplate;
  private final RedisScript<Long> rateLimitScript;
  private final List<String> configuredSymbols;
  private final long ttlSeconds;
  private final int globalCap;
  private final long rateLimitPerMinute;

  public AlertService(
      StringRedisTemplate redisTemplate,
      RedisScript<Long> rateLimitScript,
      List<String> configuredSymbols,
      @Value("${app.alert.ttl-seconds}") long ttlSeconds,
      @Value("${app.alert.global-cap}") int globalCap,
      @Value("${app.alert.rate-limit-per-minute}") long rateLimitPerMinute) {
    this.redisTemplate = redisTemplate;
    this.rateLimitScript = rateLimitScript;
    this.configuredSymbols = configuredSymbols;
    this.ttlSeconds = ttlSeconds;
    this.globalCap = globalCap;
    this.rateLimitPerMinute = rateLimitPerMinute;
  }

  public AlertView create(String symbol, String direction, String threshold, String clientId) {
    if (!configuredSymbols.contains(symbol)) {
      throw new InvalidAlertRequestException("unknown symbol: " + symbol);
    }
    AlertRequestValidator.requireDirection(direction);
    AlertRequestValidator.parseThreshold(threshold);

    checkRateLimit(clientId);
    checkGlobalCapWithCleanup();

    String id = UUID.randomUUID().toString();
    long createdAt = System.currentTimeMillis();
    String key = alertKey(id);

    HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
    hashOps.putAll(
        key,
        Map.of(
            "symbol", symbol,
            "direction", direction,
            "threshold", threshold,
            "status", "armed",
            "createdAt", String.valueOf(createdAt)));
    redisTemplate.expire(key, Duration.ofSeconds(ttlSeconds));

    SetOperations<String, String> setOps = redisTemplate.opsForSet();
    setOps.add(alertsSetKey(symbol), id);

    return new AlertView(id, symbol, direction, threshold, "armed", createdAt);
  }

  public List<AlertView> list(String symbol) {
    if (!configuredSymbols.contains(symbol)) {
      throw new InvalidAlertRequestException("unknown symbol: " + symbol);
    }

    SetOperations<String, String> setOps = redisTemplate.opsForSet();
    Set<String> ids = setOps.members(alertsSetKey(symbol));
    if (ids == null || ids.isEmpty()) {
      return List.of();
    }

    HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
    List<AlertView> result = new ArrayList<>();
    for (String id : ids) {
      Map<String, String> hash = hashOps.entries(alertKey(id));
      if (hash.isEmpty()) {
        // Lazy cleanup: the hash's own TTL already expired it, but nothing removed its id
        // from this set — do that now rather than return a dangling reference.
        setOps.remove(alertsSetKey(symbol), id);
        continue;
      }
      result.add(
          new AlertView(
              id,
              hash.get("symbol"),
              hash.get("direction"),
              hash.get("threshold"),
              hash.get("status"),
              Long.parseLong(hash.get("createdAt"))));
    }
    return result;
  }

  public void delete(String id) {
    HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
    String key = alertKey(id);
    Map<String, String> hash = hashOps.entries(key);
    if (hash.isEmpty()) {
      throw new AlertNotFoundException(id);
    }
    redisTemplate.delete(key);
    redisTemplate.opsForSet().remove(alertsSetKey(hash.get("symbol")), id);
  }

  private void checkRateLimit(String clientId) {
    Long count =
        redisTemplate.execute(
            rateLimitScript,
            List.of("ratelimit:create:" + clientId),
            String.valueOf(RATE_LIMIT_WINDOW_SECONDS));
    if (count != null && count > rateLimitPerMinute) {
      throw new RateLimitExceededException(RATE_LIMIT_WINDOW_SECONDS);
    }
  }

  // Resolves every configured symbol's alert set when counting toward the cap, pruning
  // any id whose hash has already expired. Lazy cleanup on read (list()) only cleans a
  // symbol someone actually queries — a symbol nobody ever lists would accumulate dead
  // ids forever otherwise. This path runs on every create, for every symbol, regardless
  // of which one the new alert is for, so it's the one place cleanup can't be skipped by
  // staying quiet. See DECISIONS.md.
  private void checkGlobalCapWithCleanup() {
    SetOperations<String, String> setOps = redisTemplate.opsForSet();
    int total = 0;
    for (String symbol : configuredSymbols) {
      String setKey = alertsSetKey(symbol);
      Set<String> ids = setOps.members(setKey);
      if (ids == null) {
        continue;
      }
      for (String id : ids) {
        if (Boolean.TRUE.equals(redisTemplate.hasKey(alertKey(id)))) {
          total++;
        } else {
          setOps.remove(setKey, id);
        }
      }
    }
    if (total >= globalCap) {
      throw new GlobalCapExceededException(globalCap);
    }
  }

  private static String alertKey(String id) {
    return "alert:" + id;
  }

  private static String alertsSetKey(String symbol) {
    return "alerts:" + symbol;
  }
}
