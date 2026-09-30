package com.marketdatapipeline.consumer;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ConsumerConfig {

  private static final Logger log = LoggerFactory.getLogger(ConsumerConfig.class);

  private final ApplicationContext applicationContext;

  public ConsumerConfig(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Bean
  @SuppressWarnings({"rawtypes", "unchecked"})
  public RedisScript<List> applyTickScript() {
    DefaultRedisScript<List> script = new DefaultRedisScript<>();
    script.setLocation(new ClassPathResource("scripts/apply_tick.lua"));
    script.setResultType((Class) List.class);
    return script;
  }

  // Malformed records (bad JSON, invalid fields, a null key) are never retried — retrying
  // can't help. A partition mismatch is also never retried — it's a deterministic fact,
  // not a transient condition — but unlike a malformed record, it's fatal: it means
  // Redis's state for this symbol references an offset space the incoming record's
  // partition can't be compared against at all (invariant #1). Everything else gets a
  // bounded retry, then a fatal exit rather than a silent skip-and-commit, since Redis is
  // this record's only path to durability and there's no backfill. See DECISIONS.md.
  @Bean
  public DefaultErrorHandler errorHandler() {
    DefaultErrorHandler handler = new DefaultErrorHandler(this::recover, new FixedBackOff(1000L, 5));
    handler.addNotRetryableExceptions(MalformedTickException.class, PartitionMismatchException.class);
    return handler;
  }

  private void recover(ConsumerRecord<?, ?> record, Exception exception) {
    if (unwraps(exception, MalformedTickException.class)) {
      log.atError()
          .addKeyValue("symbol", record.key())
          .addKeyValue("partition", record.partition())
          .addKeyValue("offset", record.offset())
          .addKeyValue("error", exception.getMessage())
          .log("malformed tick, not retrying, committing past it");
      return;
    }

    if (unwraps(exception, PartitionMismatchException.class)) {
      log.atError()
          .addKeyValue("symbol", record.key())
          .addKeyValue("partition", record.partition())
          .addKeyValue("offset", record.offset())
          .addKeyValue("error", exception.getMessage())
          .log("fatal: partition mismatch — Redis state for this symbol references a"
              + " different partition's offsets, which are not comparable; exiting rather"
              + " than guessing (see DECISIONS.md)");
      exitFatally(1);
      return;
    }

    log.atError()
        .addKeyValue("symbol", record.key())
        .addKeyValue("partition", record.partition())
        .addKeyValue("offset", record.offset())
        .addKeyValue("error", exception.toString())
        .log("fatal: retries exhausted on a presumed-transient failure, exiting rather than continuing with an unrepaired gap");
    exitFatally(1);
  }

  private static boolean unwraps(Throwable exception, Class<? extends Throwable> type) {
    for (Throwable current = exception; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  private void exitFatally(int exitCode) {
    // Closing the context here, synchronously on the listener thread, would deadlock:
    // it has to stop this same listener container, which has to wait for this thread to
    // return from the very call it's blocked in. Close it from a separate thread instead,
    // and just return normally so the container's own loop gets there on its own. See
    // DECISIONS.md.
    Thread shutdownThread = new Thread(
        () -> System.exit(SpringApplication.exit(applicationContext, () -> exitCode)), "fatal-shutdown");
    shutdownThread.start();
  }
}
