package com.marketdatapipeline.api;

import io.sentry.Sentry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApiApplication {

  public static void main(String[] args) {
    // Plain SDK init, not the Spring Boot starter — see pom.xml and DECISIONS.md: the
    // starter crashes fatally against Boot 4.1.1. A null dsn throws
    // IllegalArgumentException (confirmed live) — the SDK requires an explicit empty
    // string to disable cleanly, not an absent env var passed straight through. A real
    // DSN is a Stage 10 deploy-time value.
    String dsn = System.getenv("SENTRY_DSN");
    Sentry.init(options -> options.setDsn(dsn == null ? "" : dsn));
    SpringApplication.run(ApiApplication.class, args);
  }
}
