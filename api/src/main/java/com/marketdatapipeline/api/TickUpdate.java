package com.marketdatapipeline.api;

// Shape of a ticks:updates pub/sub message, published by apply_tick.lua only when it
// applied a tick — and the shape of the SSE "tick" event sent to browser clients, same
// record, no transformation needed. No ageMs here deliberately: a value computed at
// publish time would go stale while sitting in a client's coalescing buffer waiting for
// the next flush. The client gets raw exchangeTs and computes its own age, continuously.
// See DECISIONS.md. exchangeSeq (the exchange's per-product trade id) lets the client
// compute trades/min from the difference between two updates, correct even when this
// api merged several ticks into one. It's a nullable Long so a message from a consumer
// that predates the field still parses, with null meaning "not known".
public record TickUpdate(
    String symbol, String price, double ema, long exchangeTs, Long exchangeSeq) {}
