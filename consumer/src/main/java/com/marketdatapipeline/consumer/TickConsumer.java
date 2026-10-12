package com.marketdatapipeline.consumer;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class TickConsumer {

  private static final Logger log = LoggerFactory.getLogger(TickConsumer.class);
  private static final String TOPIC = "ticks";

  private final ObjectMapper objectMapper;
  private final TickStateStore stateStore;
  private final AlertStore alertStore;
  private final long tauMs;

  public TickConsumer(
      ObjectMapper objectMapper,
      TickStateStore stateStore,
      AlertStore alertStore,
      @Value("${app.ema.tau-ms}") long tauMs) {
    this.objectMapper = objectMapper;
    this.stateStore = stateStore;
    this.alertStore = alertStore;
    this.tauMs = tauMs;
  }

  @KafkaListener(topics = TOPIC)
  public void onTick(ConsumerRecord<String, String> record) {
    String symbol = record.key();
    if (symbol == null) {
      // Unprocessable, not transient — retrying five times can't produce a key that
      // isn't there. Caught here explicitly rather than left to throw a bare NPE out of
      // ConcurrentHashMap.computeIfAbsent(null, ...), which addNotRetryableExceptions
      // wouldn't recognize. See DECISIONS.md.
      throw new MalformedTickException(
          "record has a null key at partition=" + record.partition() + " offset=" + record.offset());
    }

    TickPayload tick;
    double price;
    try {
      tick = objectMapper.readValue(record.value(), TickPayload.class);
      price = Double.parseDouble(tick.price());
    } catch (RuntimeException e) {
      throw new MalformedTickException(
          "malformed tick at partition=" + record.partition() + " offset=" + record.offset(), e);
    }

    // previous feeds both the EMA and crossing detection below — both depend on
    // TickStateStore's resync-on-skip (the cache always reflects Redis's actual current
    // state, even after a skip), not just the EMA alone. See DECISIONS.md.
    SymbolState previous = stateStore.load(symbol);
    double newEma = EmaCalculator.compute(previous, price, tick.exchangeTs(), tauMs);
    SymbolState candidate = new SymbolState(tick.price(), newEma, tick.exchangeTs(), record.offset(), record.partition());

    long alertLookupStartNanos = System.nanoTime();
    List<Alert> armedAlerts = alertStore.loadArmed(symbol);
    long alertLookupMicros = (System.nanoTime() - alertLookupStartNanos) / 1000;

    // Evaluated optimistically against whatever `previous` currently is, before knowing
    // whether this tick will even be applied — safe because apply_tick.lua only acts on
    // this candidate list inside its own applied branch (see DECISIONS.md: Java's read
    // isn't atomic with Lua's write, but Lua's offset CAS is the actual gate either way).
    Double previousPrice = previous == null ? null : Double.parseDouble(previous.price());
    List<String> crossedAlertIds = new ArrayList<>();
    List<String> crossedAlertKeys = new ArrayList<>();
    for (Alert alert : armedAlerts) {
      if (CrossingDetector.crossed(previousPrice, price, alert.direction(), alert.threshold())) {
        crossedAlertIds.add(alert.id());
        crossedAlertKeys.add(AlertStore.alertKey(alert.id()));
      }
    }

    TickStateStore.ApplyResult result = stateStore.applyTick(symbol, candidate, crossedAlertKeys);

    // "applied" means every id in crossedAlertIds just got marked fired by the same
    // write — the script never touches them on skipped/partition_mismatch (invariant
    // #5's replay safety), and the candidate list itself already excluded anything not
    // currently armed, so this can't double-log a previously-fired alert.
    if (result.applied()) {
      for (String alertId : crossedAlertIds) {
        log.atInfo()
            .addKeyValue("symbol", symbol)
            .addKeyValue("partition", record.partition())
            .addKeyValue("offset", record.offset())
            .addKeyValue("alertId", alertId)
            .log("alert fired");
      }
    }

    // A skip whose own exchangeTs is newer than what's stored can only happen if the
    // topic was recreated while Redis state wasn't reset (see DECISIONS.md) — an ordinary
    // replay redelivers records chronologically at-or-before the stored state, never after.
    if (!result.applied() && tick.exchangeTs() > result.currentState().exchangeTs()) {
      log.atError()
          .addKeyValue("symbol", symbol)
          .addKeyValue("partition", record.partition())
          .addKeyValue("offset", record.offset())
          .addKeyValue("tickExchangeTs", tick.exchangeTs())
          .addKeyValue("storedExchangeTs", result.currentState().exchangeTs())
          .addKeyValue("storedOffset", result.currentState().lastOffset())
          .log(
              "offset/timestamp inconsistency: a skipped record is chronologically newer than the"
                  + " stored state — the topic may have been recreated while Redis state was not"
                  + " reset; manual intervention needed (see DECISIONS.md)");
    }

    log.atInfo()
        .addKeyValue("symbol", symbol)
        .addKeyValue("partition", record.partition())
        .addKeyValue("offset", record.offset())
        .addKeyValue("price", tick.price())
        .addKeyValue("ema", result.currentState().ema())
        .addKeyValue("applied", result.applied())
        .addKeyValue("alertLookupMicros", alertLookupMicros)
        .log("tick processed");
  }
}
