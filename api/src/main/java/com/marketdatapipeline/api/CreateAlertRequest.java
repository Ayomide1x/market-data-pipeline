package com.marketdatapipeline.api;

// threshold is a price-like value — travels as a string, same reason prices do elsewhere
// in this project (avoids float-precision surprises).
public record CreateAlertRequest(String symbol, String direction, String threshold) {}
