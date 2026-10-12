package com.marketdatapipeline.api;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableScheduling
public class AppConfig implements WebMvcConfigurer {

  private final String corsOrigin;

  public AppConfig(@Value("${app.cors.origin}") String corsOrigin) {
    this.corsOrigin = corsOrigin;
  }

  @Bean
  public List<String> configuredSymbols(@Value("${app.symbols}") String symbolsRaw) {
    return Arrays.stream(symbolsRaw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  // Exact-origin allowlist from env, per CLAUDE.md — the frontend is cross-origin from
  // Stage 8 on. No wildcard: allowedOrigins takes the literal configured origin only.
  @Override
  public void addCorsMappings(CorsRegistry registry) {
    registry
        .addMapping("/api/**")
        .allowedOrigins(corsOrigin)
        .allowedMethods("GET", "POST", "DELETE")
        .allowedHeaders("*");
  }

  @Bean
  @SuppressWarnings({"rawtypes", "unchecked"})
  public RedisScript<Long> rateLimitScript() {
    DefaultRedisScript<Long> script = new DefaultRedisScript<>();
    script.setLocation(new ClassPathResource("scripts/rate_limit.lua"));
    script.setResultType((Class) Long.class);
    return script;
  }

  // destroyMethod=shutdown: a cached pool's default (non-daemon) threads would otherwise
  // keep the JVM alive on SIGTERM until their 60s idle keep-alive expires — this makes
  // shutdown prompt instead, matching "graceful shutdown everywhere" (CLAUDE.md).
  @Bean(destroyMethod = "shutdown")
  public ExecutorService sseWriteExecutor() {
    return Executors.newCachedThreadPool();
  }

  // Standing for the api's whole lifetime, regardless of connected-client count — see
  // SseRegistry and DECISIONS.md.
  @Bean
  public RedisMessageListenerContainer redisMessageListenerContainer(
      RedisConnectionFactory connectionFactory, SseRegistry sseRegistry) {
    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    container.addMessageListener(sseRegistry, new ChannelTopic("ticks:updates"));
    container.addMessageListener(sseRegistry, new ChannelTopic("alerts:fired"));
    return container;
  }
}
