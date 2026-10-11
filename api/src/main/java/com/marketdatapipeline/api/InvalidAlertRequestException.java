package com.marketdatapipeline.api;

// Bad symbol/direction/threshold on a create request — 400.
public class InvalidAlertRequestException extends RuntimeException {
  public InvalidAlertRequestException(String message) {
    super(message);
  }
}
