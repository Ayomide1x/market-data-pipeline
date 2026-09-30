package com.marketdatapipeline.consumer;

// Thrown for a record that can never succeed no matter how many times it's retried (bad
// JSON, missing/invalid fields) — classified as non-retryable in ConsumerApplication's
// error handler. See DECISIONS.md.
public class MalformedTickException extends RuntimeException {

  public MalformedTickException(String message, Throwable cause) {
    super(message, cause);
  }

  public MalformedTickException(String message) {
    super(message);
  }
}
