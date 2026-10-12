package com.marketdatapipeline.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

// Retries RedisMessageListenerContainer's own start() on a schedule, since AppConfig sets
// setAutoStartup(false) on it specifically so a slow-to-become-reachable Redis can never
// fail the whole Spring context. See DECISIONS.md for the incident this fixes.
//
// isRunning() cannot be trusted as "did the subscribe actually succeed" — verified
// directly against this exact dependency version: a failed start() still leaves
// isRunning() true, and calling start() again while it reports running is a silent
// no-op, not a real retry. stop() before every attempt resets that internal state so
// each retry genuinely re-attempts the subscription rather than quietly doing nothing.
//
// This only covers the initial, at-boot subscribe race. A connection that drops after a
// successful subscribe is a different problem, left to Spring Data Redis's own existing
// recovery for an established subscription — not touched here, not verified here.
@Component
public class RedisSubscriptionStarter {

  private static final Logger log = LoggerFactory.getLogger(RedisSubscriptionStarter.class);

  private final RedisMessageListenerContainer container;
  private volatile boolean subscribed = false;

  public RedisSubscriptionStarter(RedisMessageListenerContainer container) {
    this.container = container;
  }

  @Scheduled(fixedDelay = 5000)
  public void ensureSubscribed() {
    if (subscribed) {
      return;
    }
    try {
      container.stop();
      container.start();
      subscribed = true;
      log.atInfo().log("redis pub/sub subscription established");
    } catch (Exception e) {
      log.atWarn().setCause(e).log("redis pub/sub subscription attempt failed, will retry");
    }
  }
}
