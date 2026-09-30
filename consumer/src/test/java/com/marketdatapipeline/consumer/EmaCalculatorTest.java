package com.marketdatapipeline.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EmaCalculatorTest {

  private static final long TAU_MS = 300_000L;

  @Test
  void firstTickSeedsEmaDirectlyToPrice() {
    double ema = EmaCalculator.compute(null, 100.0, 1_000L, TAU_MS);
    assertThat(ema).isEqualTo(100.0);
  }

  @Test
  void sameTimestampBurstLeavesEmaUnchanged() {
    SymbolState previous = new SymbolState("100", 100.0, 1_000L, 5L, 3);
    double ema = EmaCalculator.compute(previous, 150.0, 1_000L, TAU_MS);
    assertThat(ema).isEqualTo(100.0);
  }

  @Test
  void negativeDeltaIsClampedToZeroNotNegativeDecay() {
    SymbolState previous = new SymbolState("100", 100.0, 5_000L, 5L, 3);
    // exchangeTs behind the cached state's — only reachable for a record that
    // apply_tick.lua's offset CAS will end up skipping anyway.
    double ema = EmaCalculator.compute(previous, 150.0, 1_000L, TAU_MS);
    assertThat(ema).isEqualTo(100.0);
  }

  @Test
  void blendsByAlphaForAnOrdinaryElapsedTime() {
    SymbolState previous = new SymbolState("100", 100.0, 0L, 5L, 3);
    long deltaMs = 60_000L;
    double expectedAlpha = 1 - StrictMath.exp(-((double) deltaMs) / TAU_MS);
    double expectedEma = expectedAlpha * 200.0 + (1 - expectedAlpha) * 100.0;

    double ema = EmaCalculator.compute(previous, 200.0, deltaMs, TAU_MS);

    assertThat(ema).isEqualTo(expectedEma);
    assertThat(ema).isStrictlyBetween(100.0, 200.0);
  }

  @Test
  void veryLargeDeltaConvergesToNewPrice() {
    SymbolState previous = new SymbolState("100", 100.0, 0L, 5L, 3);
    double ema = EmaCalculator.compute(previous, 200.0, TAU_MS * 1000, TAU_MS);
    assertThat(ema).isCloseTo(200.0, org.assertj.core.data.Offset.offset(1e-9));
  }
}
