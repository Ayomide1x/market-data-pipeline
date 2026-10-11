package com.marketdatapipeline.api;

public class AlertNotFoundException extends RuntimeException {
  public AlertNotFoundException(String id) {
    super("no alert with id: " + id);
  }
}
