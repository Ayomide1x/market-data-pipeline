package com.marketdatapipeline.api;

// Not one of the configured SYMBOLS at all (see DECISIONS.md — distinct from "configured
// but no data yet," which is a normal 200, not an error).
public class UnknownSymbolException extends RuntimeException {
  public UnknownSymbolException(String symbol) {
    super("symbol is not configured: " + symbol);
  }
}
