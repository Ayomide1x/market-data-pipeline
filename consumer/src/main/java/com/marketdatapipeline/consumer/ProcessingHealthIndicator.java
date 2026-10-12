package com.marketdatapipeline.consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

// Actuator's own default health check (with no custom indicator) only reports whether
// the Spring context is alive — that stays UP for a consumer whose listener has
// stopped entirely (lost all partitions in a rebalance, a stuck thread, unbounded lag)
// as long as the JVM itself hasn't crashed. This is deliberately NOT exhaustive: it
// catches "this instance isn't processing records" (via ProcessingHeartbeat, updated
// on every onTick invocation, applied or not), not "the consumer group overall has
// lag" (would need a Kafka AdminClient call this doesn't make) and not a deadlock in
// something other than the listener thread. See DECISIONS.md for this stated limit.
@Component
public class ProcessingHealthIndicator implements HealthIndicator {

  private final ProcessingHeartbeat heartbeat;
  private final long staleAfterMs;

  public ProcessingHealthIndicator(
      ProcessingHeartbeat heartbeat, @Value("${app.processing.stale-after-ms}") long staleAfterMs) {
    this.heartbeat = heartbeat;
    this.staleAfterMs = staleAfterMs;
  }

  @Override
  public Health health() {
    long millisSinceLastProcessed = heartbeat.millisSinceLastProcessed();
    if (millisSinceLastProcessed > staleAfterMs) {
      return Health.down()
          .withDetail("millisSinceLastProcessed", millisSinceLastProcessed)
          .withDetail("staleAfterMs", staleAfterMs)
          .withDetail("reason", "no record processed within the configured window — "
              + "lost partitions, a stuck listener, or a genuinely silent exchange "
              + "all look identical from here; check kafka-consumer-groups --describe "
              + "and the consumer's own logs")
          .build();
    }
    return Health.up().withDetail("millisSinceLastProcessed", millisSinceLastProcessed).build();
  }
}
