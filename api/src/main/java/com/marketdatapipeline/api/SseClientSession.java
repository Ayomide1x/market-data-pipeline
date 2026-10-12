package com.marketdatapipeline.api;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

// One per connected SSE client. Tick updates coalesce by symbol (latest wins, via a plain
// Map put) — correct for a value with a "current" (price). A fired alert is a one-time
// fact, not a value with a "current," so it queues instead and is never overwritten or
// dropped. See DECISIONS.md.
public class SseClientSession {

  private final SseEmitter emitter;
  private final Map<String, TickUpdate> pendingTicks = new ConcurrentHashMap<>();
  private final Queue<AlertFired> pendingAlerts = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public SseClientSession(SseEmitter emitter) {
    this.emitter = emitter;
  }

  public SseEmitter emitter() {
    return emitter;
  }

  public Map<String, TickUpdate> pendingTicks() {
    return pendingTicks;
  }

  public Queue<AlertFired> pendingAlerts() {
    return pendingAlerts;
  }

  /** True only for the caller that actually transitions this session to closed. */
  public boolean markClosed() {
    return closed.compareAndSet(false, true);
  }
}
