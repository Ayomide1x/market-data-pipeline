package com.marketdatapipeline.api;

// The global alert cap has been reached — 429, no Retry-After: this doesn't drain on a
// fixed schedule the way the per-client rate limit does, only as existing alerts expire
// or get deleted (see DECISIONS.md).
public class GlobalCapExceededException extends RuntimeException {
  public GlobalCapExceededException(int cap) {
    super(
        "the global alert cap ("
            + cap
            + ") has been reached; it frees up as existing alerts expire or are deleted, not on a"
            + " fixed schedule");
  }
}
