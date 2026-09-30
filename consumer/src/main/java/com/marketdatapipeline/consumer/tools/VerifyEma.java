package com.marketdatapipeline.consumer.tools;

import com.marketdatapipeline.consumer.EmaCalculator;
import com.marketdatapipeline.consumer.SymbolState;
import com.marketdatapipeline.consumer.TickPayload;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.jackson.databind.ObjectMapper;

/**
 * Recomputes each symbol's EMA independently from the start of the `ticks` topic — reusing
 * EmaCalculator directly, not reimplementing the formula (see DECISIONS.md) — and compares
 * it against Redis's current state:{symbol}. Uses manual partition assignment, not a
 * consumer group, so it never commits and never appears in `kafka-consumer-groups
 * --describe` for `ema-consumers`.
 *
 * <p>Run via: tools/verify-ema
 */
public final class VerifyEma {

  private static final String TOPIC = "ticks";

  private VerifyEma() {}

  public static void main(String[] args) {
    String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092");
    String redisHost = env("REDIS_HOST", "127.0.0.1");
    int redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));
    long tauMs = Long.parseLong(env("EMA_TAU_MS", "300000"));

    Map<String, SymbolState> recomputed = recomputeFromTopicStart(bootstrapServers, tauMs);

    RedisClient redisClient = RedisClient.create(RedisURI.create(redisHost, redisPort));
    boolean allMatch = true;
    try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
      RedisCommands<String, String> redis = connection.sync();

      if (recomputed.isEmpty()) {
        System.out.println("No records found on topic " + TOPIC + " — nothing to verify.");
      }

      for (Map.Entry<String, SymbolState> entry : recomputed.entrySet()) {
        allMatch &= compare(entry.getKey(), entry.getValue(), redis.hgetall("state:" + entry.getKey()));
      }
    } finally {
      redisClient.shutdown();
    }

    System.exit(allMatch ? 0 : 1);
  }

  private static Map<String, SymbolState> recomputeFromTopicStart(String bootstrapServers, long tauMs) {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Map<String, SymbolState> stateBySymbol = new HashMap<>();
    Map<String, Long> lastOffsetBySymbol = new HashMap<>();
    Map<String, Integer> partitionBySymbol = new HashMap<>();
    // Mirrors the real consumer fatally exiting on a partition mismatch: once hit, stop
    // updating that symbol's state here too, so the frozen recomputed value matches
    // whatever Redis holds from right before the real process would have stopped.
    Set<String> halted = new HashSet<>();

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      List<TopicPartition> partitions = consumer.partitionsFor(TOPIC).stream()
          .map(p -> new TopicPartition(p.topic(), p.partition()))
          .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

      while (!caughtUp(consumer, endOffsets)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        for (ConsumerRecord<String, String> record : records) {
          String symbol = record.key();
          if (symbol == null || halted.contains(symbol)) {
            continue;
          }
          long offset = record.offset();
          int partition = record.partition();

          Integer storedPartition = partitionBySymbol.get(symbol);
          if (storedPartition != null && storedPartition != partition) {
            System.out.printf(
                "%s: partition mismatch during replay (stored=%d, incoming=%d at offset=%d) —"
                    + " halting recomputation for this symbol here, matching the real consumer's"
                    + " fatal exit%n",
                symbol, storedPartition, partition, offset);
            halted.add(symbol);
            continue;
          }

          Long storedOffset = lastOffsetBySymbol.get(symbol);
          if (storedOffset != null && offset <= storedOffset) {
            continue; // mirrors apply_tick.lua's offset CAS
          }

          TickPayload tick = objectMapper.readValue(record.value(), TickPayload.class);
          double price = Double.parseDouble(tick.price());
          SymbolState previous = stateBySymbol.get(symbol);
          double ema = EmaCalculator.compute(previous, price, tick.exchangeTs(), tauMs);

          stateBySymbol.put(symbol, new SymbolState(tick.price(), ema, tick.exchangeTs(), offset, partition));
          lastOffsetBySymbol.put(symbol, offset);
          partitionBySymbol.put(symbol, partition);
        }
      }
    }

    return stateBySymbol;
  }

  private static boolean caughtUp(KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> endOffsets) {
    for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
      if (consumer.position(entry.getKey()) < entry.getValue()) {
        return false;
      }
    }
    return true;
  }

  private static boolean compare(String symbol, SymbolState expected, Map<String, String> actual) {
    if (actual.isEmpty()) {
      System.out.println("FAIL " + symbol + ": no state:" + symbol + " hash in Redis at all");
      return false;
    }

    String actualPrice = actual.get("price");
    double actualEma = Double.parseDouble(actual.get("ema"));
    long actualOffset = Long.parseLong(actual.get("lastOffset"));
    int actualPartition = actual.containsKey("partition") ? Integer.parseInt(actual.get("partition")) : -1;

    boolean priceMatches = expected.price().equals(actualPrice);
    boolean emaMatches = Math.abs(expected.ema() - actualEma) < 1e-6;
    boolean offsetMatches = expected.lastOffset() == actualOffset;
    boolean partitionMatches = expected.partition() == actualPartition;

    if (priceMatches && emaMatches && offsetMatches && partitionMatches) {
      System.out.printf(
          "PASS %s: price=%s ema=%s lastOffset=%d partition=%d%n",
          symbol, actualPrice, actual.get("ema"), actualOffset, actualPartition);
      return true;
    }

    System.out.printf(
        "FAIL %s: recomputed{price=%s ema=%s lastOffset=%d partition=%d} redis{price=%s ema=%s lastOffset=%d partition=%d}%n",
        symbol, expected.price(), expected.ema(), expected.lastOffset(), expected.partition(),
        actualPrice, actual.get("ema"), actualOffset, actualPartition);
    return false;
  }

  private static String env(String key, String fallback) {
    String value = System.getenv(key);
    return value != null ? value : fallback;
  }
}
