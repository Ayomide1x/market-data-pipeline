package com.marketdatapipeline.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

// Redis wiring for alert evaluation, separate from the pure crossing math (see
// DECISIONS.md). Always a fresh read, no cache of its own — api is the only writer of
// alert data and Redis is the source of truth for it, the same stance api's own
// SnapshotService takes.
@Component
public class AlertStore {

  private final StringRedisTemplate redisTemplate;

  public AlertStore(StringRedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  /**
   * Armed alerts for {@code symbol} only — a fired or lazily-dead alert is never
   * returned, so a caller iterating this list can't re-evaluate one that's already done.
   * Two Redis round trips total regardless of alert count: one SMEMBERS, then every
   * HGETALL pipelined together into the second. See DECISIONS.md for the measured cost
   * and why N+1 serial round trips were rejected.
   */
  public List<Alert> loadArmed(String symbol) {
    Set<String> ids = redisTemplate.opsForSet().members("alerts:" + symbol);
    if (ids == null || ids.isEmpty()) {
      return List.of();
    }

    List<String> idList = new ArrayList<>(ids);
    // connection here is a JDK dynamic proxy (RedisTemplate's own close-suppressing
    // wrapper), not a StringRedisConnection directly — a plain cast throws
    // ClassCastException, confirmed live. DefaultStringRedisConnection wraps it properly.
    List<Object> rawHashes = redisTemplate.executePipelined(
        (RedisCallback<Object>) connection -> {
          StringRedisConnection stringConnection = new DefaultStringRedisConnection(connection);
          for (String id : idList) {
            stringConnection.hGetAll(alertKey(id));
          }
          return null;
        });

    List<Alert> armed = new ArrayList<>();
    for (int i = 0; i < idList.size(); i++) {
      @SuppressWarnings("unchecked")
      Map<String, String> hash = (Map<String, String>) rawHashes.get(i);
      if (hash == null || hash.isEmpty() || !"armed".equals(hash.get("status"))) {
        continue;
      }
      armed.add(new Alert(idList.get(i), hash.get("direction"), Double.parseDouble(hash.get("threshold"))));
    }
    return armed;
  }

  public static String alertKey(String id) {
    return "alert:" + id;
  }
}
