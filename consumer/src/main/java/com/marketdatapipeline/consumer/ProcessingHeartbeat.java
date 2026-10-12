package com.marketdatapipeline.consumer;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

// Updated at the top of every onTick invocation, regardless of outcome (applied,
// skipped, or even a malformed/partition-mismatch record that gets retried) — this
// records that the listener is actually being invoked, not that state is advancing.
// Read by ProcessingHealthIndicator. See DECISIONS.md: Actuator's own default health
// check only reports whether the Spring context is alive, which stays UP for a
// consumer that has stopped consuming entirely (lost partitions, a stuck listener) —
// this exists specifically to catch that case instead.
@Component
public class ProcessingHeartbeat {

  private final AtomicLong lastProcessedAtMs = new AtomicLong(System.currentTimeMillis());

  public void recordProcessed() {
    lastProcessedAtMs.set(System.currentTimeMillis());
  }

  public long millisSinceLastProcessed() {
    return System.currentTimeMillis() - lastProcessedAtMs.get();
  }
}
