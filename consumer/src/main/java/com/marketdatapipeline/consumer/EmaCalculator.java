package com.marketdatapipeline.consumer;

// Pure logic, no Kafka/Redis wiring (see DECISIONS.md for why this stays in Java rather
// than in apply_tick.lua) — also reused by tools/verify-ema so there is exactly one
// implementation of this formula to be wrong in.
public final class EmaCalculator {

  private EmaCalculator() {}

  /**
   * @param previous the symbol's current state, or null if this is the first tick ever
   *     seen for it (no prior EMA to blend with).
   * @param newPrice the incoming tick's price.
   * @param newExchangeTs the incoming tick's exchangeTs (ms) — never wall-clock time, so a
   *     replay computes identically (invariant #6).
   * @param tauMs the EMA time constant.
   */
  public static double compute(SymbolState previous, double newPrice, long newExchangeTs, long tauMs) {
    if (previous == null) {
      return newPrice;
    }

    // Clamped to 0, not left negative: a tick chronologically behind the cached state only
    // reaches here for a record that's about to be skipped by apply_tick.lua's offset CAS
    // anyway (see DECISIONS.md) — this value is computed regardless but discarded on skip.
    // Same Δt=0 semantics apply: no elapsed time, no decay.
    long deltaMs = Math.max(0, newExchangeTs - previous.exchangeTs());
    // StrictMath, not Math: Math.exp is explicitly permitted to differ by up to 1 ULP
    // across JVM runs/JIT states (only StrictMath guarantees bit-identical results
    // everywhere), which otherwise contradicts "so replays compute identically." Caught by
    // tools/verify-ema actually disagreeing with a live process in the 8th decimal place.
    double alpha = 1 - StrictMath.exp(-((double) deltaMs) / tauMs);
    return alpha * newPrice + (1 - alpha) * previous.ema();
  }
}
