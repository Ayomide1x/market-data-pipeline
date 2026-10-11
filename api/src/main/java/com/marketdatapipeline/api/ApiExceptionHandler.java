package com.marketdatapipeline.api;

import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

  @ExceptionHandler(UnknownSymbolException.class)
  public ResponseEntity<Map<String, String>> handle(UnknownSymbolException e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
  }

  @ExceptionHandler(InvalidAlertRequestException.class)
  public ResponseEntity<Map<String, String>> handle(InvalidAlertRequestException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
  }

  @ExceptionHandler(AlertNotFoundException.class)
  public ResponseEntity<Map<String, String>> handle(AlertNotFoundException e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
  }

  // Retry-After included: this window drains on a fixed schedule.
  @ExceptionHandler(RateLimitExceededException.class)
  public ResponseEntity<Map<String, String>> handle(RateLimitExceededException e) {
    return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(e.retryAfterSeconds()))
        .body(Map.of("error", e.getMessage()));
  }

  // No Retry-After: the global cap doesn't drain on a fixed schedule, only as existing
  // alerts expire or get deleted — a Retry-After header here would imply a wait time that
  // isn't real. See DECISIONS.md.
  @ExceptionHandler(GlobalCapExceededException.class)
  public ResponseEntity<Map<String, String>> handle(GlobalCapExceededException e) {
    return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("error", e.getMessage()));
  }
}
