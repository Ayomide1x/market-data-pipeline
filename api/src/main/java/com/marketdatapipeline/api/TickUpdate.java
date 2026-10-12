package com.marketdatapipeline.api;

// Shape of a ticks:updates pub/sub message, published by apply_tick.lua only when it
// applied a tick — and the shape of the SSE "tick" event sent to browser clients, same
// record, no transformation needed. No ageMs here deliberately: a value computed at
// publish time would go stale while sitting in a client's coalescing buffer waiting for
// the next flush. The client gets raw exchangeTs and computes its own age, continuously.
// See DECISIONS.md.
public record TickUpdate(String symbol, String price, double ema, long exchangeTs) {}
