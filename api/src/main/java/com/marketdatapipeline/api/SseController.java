package com.marketdatapipeline.api;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class SseController {

  // Generous relative to the 15s heartbeat — four missed heartbeats' worth of silence
  // means something is actually wrong, not a transient blip, and Spring's own timeout
  // machinery should close it via onTimeout, feeding the same eviction path as a write
  // failure. See DECISIONS.md.
  private static final long EMITTER_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

  private final SseRegistry registry;
  private final SnapshotService snapshotService;
  private final List<String> configuredSymbols;

  public SseController(SseRegistry registry, SnapshotService snapshotService, List<String> configuredSymbols) {
    this.registry = registry;
    this.snapshotService = snapshotService;
    this.configuredSymbols = configuredSymbols;
  }

  // Register-then-snapshot, in that exact order: anything published between
  // registration and the snapshot read lands in this client's pending map immediately
  // and goes out on the next scheduled flush, not lost. See DECISIONS.md for exactly
  // where the gap is if this order were reversed.
  @GetMapping(path = "/api/stream", produces = "text/event-stream")
  public SseEmitter stream() {
    SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
    SseClientSession session = new SseClientSession(emitter);

    emitter.onCompletion(() -> registry.evict(session, new IllegalStateException("client completed")));
    emitter.onTimeout(() -> registry.evict(session, new TimeoutException("client timed out")));
    emitter.onError(e -> registry.evict(session, e));

    registry.register(session);

    try {
      for (String symbol : configuredSymbols) {
        emitter.send(SseEmitter.event().name("snapshot").data(snapshotService.snapshotFor(symbol)));
      }
    } catch (Exception e) {
      registry.evict(session, e);
    }

    return emitter;
  }
}
