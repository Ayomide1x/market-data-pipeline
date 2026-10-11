package com.marketdatapipeline.api;

// Per-client creation rate exceeded — 429, with Retry-After, since this window drains on
// a fixed schedule (see DECISIONS.md — unlike the global cap, which doesn't).
public class RateLimitExceededException extends RuntimeException {

  private final long retryAfterSeconds;

  public RateLimitExceededException(long retryAfterSeconds) {
    super("rate limit exceeded; retry after " + retryAfterSeconds + "s");
    this.retryAfterSeconds = retryAfterSeconds;
  }

  public long retryAfterSeconds() {
    return retryAfterSeconds;
  }
}
