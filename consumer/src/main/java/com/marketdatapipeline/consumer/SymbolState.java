package com.marketdatapipeline.consumer;

// Mirrors the state:{symbol} Redis hash exactly (see DECISIONS.md). partition is -1 for
// a hash written before this field existed, or for a symbol never seen at all — meaning
// "no partition recorded yet," not a value to compare against.
public record SymbolState(String price, double ema, long exchangeTs, long lastOffset, int partition) {}
