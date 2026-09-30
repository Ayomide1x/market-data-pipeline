package com.marketdatapipeline.consumer;

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
  private final long tauMs;

  public TickConsumer(
      ObjectMapper objectMapper, TickStateStore stateStore, @Value("${app.ema.tau-ms}") long tauMs) {
    this.objectMapper = objectMapper;
    this.stateStore = stateStore;
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

    SymbolState previous = stateStore.load(symbol);
    double newEma = EmaCalculator.compute(previous, price, tick.exchangeTs(), tauMs);
    SymbolState candidate = new SymbolState(tick.price(), newEma, tick.exchangeTs(), record.offset(), record.partition());

    TickStateStore.ApplyResult result = stateStore.applyTick(symbol, candidate);

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
        .log("tick processed");
  }
}
