package com.marketdatapipeline.api;

// price travels as a string (same convention as the Kafka message/state hash); ema is a
// computed indicator, a plain number is fine for it (see DECISIONS.md). ageMs is null
// exactly when hasData is false — there's no exchangeTs to measure age from yet.
public record SymbolSnapshot(
    String symbol, boolean hasData, String price, Double ema, Long exchangeTs, Long ageMs) {}
