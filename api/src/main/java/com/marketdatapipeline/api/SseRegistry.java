package com.marketdatapipeline.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.ObjectMapper;

// Fans Redis pub/sub messages out to every connected SSE client, in-process. The
// Redis-level subscription (wired in AppConfig) is standing for the api's whole
// lifetime, regardless of client count — fan-out itself is a plain in-memory map/list
// operation, not a Redis concern. See DECISIONS.md.
//
// The actual blocking writes to each client's socket never run on this bean's own
// scheduler threads — they're submitted to a shared executor, so one slow or stalled
// connection can't delay the flush decision (cheap, in-memory) for every other client
// queued behind it in the same scheduled tick. A write that throws is the natural
// eviction signal for a dead connection; see evict() and DECISIONS.md.
@Component
public class SseRegistry implements MessageListener {

  private static final Logger log = LoggerFactory.getLogger(SseRegistry.class);
  private static final String TICKS_CHANNEL = "ticks:updates";
  private static final String ALERTS_CHANNEL = "alerts:fired";

  private final List<SseClientSession> sessions = new CopyOnWriteArrayList<>();
  private final ObjectMapper objectMapper;
  private final ExecutorService writeExecutor;

  public SseRegistry(ObjectMapper objectMapper, ExecutorService sseWriteExecutor) {
    this.objectMapper = objectMapper;
    this.writeExecutor = sseWriteExecutor;
  }

  public void register(SseClientSession session) {
    sessions.add(session);
  }

  @Override
  public void onMessage(Message message, byte[] pattern) {
    String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
    try {
      if (TICKS_CHANNEL.equals(channel)) {
        TickUpdate update = objectMapper.readValue(message.getBody(), TickUpdate.class);
        for (SseClientSession session : sessions) {
          session.pendingTicks().put(update.symbol(), update);
        }
      } else if (ALERTS_CHANNEL.equals(channel)) {
        AlertFired fired = objectMapper.readValue(message.getBody(), AlertFired.class);
        for (SseClientSession session : sessions) {
          session.pendingAlerts().add(fired);
        }
      }
    } catch (Exception e) {
      log.atError().addKeyValue("channel", channel).setCause(e).log("failed to decode pub/sub message");
    }
  }

  // Throttle interval is deliberately short (SSE_THROTTLE_MS, default 100ms) relative to
  // the heartbeat's 15s — this is the data path, not the keep-alive path.
  @Scheduled(fixedRateString = "${app.sse.throttle-ms}")
  public void flush() {
    for (SseClientSession session : sessions) {
      List<TickUpdate> ticks = drainTicks(session);
      List<AlertFired> alerts = drainAlerts(session);
      if (ticks.isEmpty() && alerts.isEmpty()) {
        continue;
      }
      writeExecutor.submit(() -> sendSafely(session, () -> {
        for (TickUpdate tick : ticks) {
          session.emitter().send(SseEmitter.event().name("tick").data(tick));
        }
        for (AlertFired alert : alerts) {
          session.emitter().send(SseEmitter.event().name("alert-fired").data(alert));
        }
      }));
    }
  }

  // A bare SSE comment, invisible to EventSource's onmessage/named-event listeners — see
  // DECISIONS.md for why a comment and not a fake data event.
  @Scheduled(fixedRate = 15000)
  public void heartbeat() {
    for (SseClientSession session : sessions) {
      writeExecutor.submit(() -> sendSafely(session,
          () -> session.emitter().send(SseEmitter.event().comment("heartbeat"))));
    }
  }

  private void sendSafely(SseClientSession session, SseWrite write) {
    try {
      write.run();
    } catch (Exception e) {
      evict(session, e);
    }
  }

  /**
   * The natural eviction signal: a write to a dead connection throws. Also called
   * directly from the emitter's own onCompletion/onTimeout/onError callbacks, so
   * markClosed()'s CAS is what prevents a double-complete() race between "a write just
   * failed" and "Spring's own lifecycle callback fired" — whichever gets here first wins,
   * the other is a no-op. See DECISIONS.md.
   */
  public void evict(SseClientSession session, Throwable cause) {
    if (!session.markClosed()) {
      return;
    }
    sessions.remove(session);
    try {
      // complete(), not completeWithError(cause): the latter dispatches the Throwable
      // through Spring MVC's exception-resolution machinery to write an error response
      // body — on an SSE response whose headers (and often some event bytes) are already
      // committed, that has nothing sane to write and only produces a harmless-looking
      // but real warning ("No converter for ... with preset Content-Type
      // 'text/event-stream'"), confirmed live. cause is for our own log line below, not
      // something the client needs to receive.
      session.emitter().complete();
    } catch (Exception ignored) {
      // Already completed by Spring itself via onTimeout/onCompletion racing ahead of us
      // — nothing further to do.
    }
    log.atInfo().addKeyValue("reason", cause.toString()).log("SSE client evicted");
  }

  private List<TickUpdate> drainTicks(SseClientSession session) {
    Map<String, TickUpdate> snapshot = new HashMap<>(session.pendingTicks());
    List<TickUpdate> result = new ArrayList<>(snapshot.size());
    for (Map.Entry<String, TickUpdate> entry : snapshot.entrySet()) {
      // Conditional remove: only clears the entry if nothing newer overwrote it since the
      // snapshot above. A genuinely newer update landing in this exact window rides to
      // the next flush instead of being silently dropped. See DECISIONS.md.
      session.pendingTicks().remove(entry.getKey(), entry.getValue());
      result.add(entry.getValue());
    }
    return result;
  }

  private List<AlertFired> drainAlerts(SseClientSession session) {
    List<AlertFired> result = new ArrayList<>();
    AlertFired next;
    while ((next = session.pendingAlerts().poll()) != null) {
      result.add(next);
    }
    return result;
  }

  @FunctionalInterface
  private interface SseWrite {
    void run() throws Exception;
  }
}
