package com.marketdatapipeline.consumer;

// Pure logic, no Redis/Kafka wiring (same precedent as EmaCalculator — see DECISIONS.md).
// A single, memoryless answer to "did price move from one side of the threshold to the
// other between these two points." It has no notion of "already fired" and will happily
// report true again for a second, later excursion past the same threshold — see
// DECISIONS.md for why that's AlertStore's armed-only filter's job, not this class's.
public final class CrossingDetector {

  private CrossingDetector() {}

  /**
   * @param previousPrice the symbol's price before this tick, or null if this is the
   *     first tick ever seen for it (no prior point, so no crossing is possible).
   * @param newPrice the incoming tick's price.
   * @param direction "above" or "below" — which side a crossing must land on to count.
   * @param threshold the alert's configured threshold.
   */
  public static boolean crossed(Double previousPrice, double newPrice, String direction, double threshold) {
    if (previousPrice == null) {
      return false;
    }
    if ("above".equals(direction)) {
      return previousPrice <= threshold && newPrice > threshold;
    }
    if ("below".equals(direction)) {
      return previousPrice >= threshold && newPrice < threshold;
    }
    // Unreachable in practice: AlertRequestValidator (api) rejects any other direction at
    // creation time, so a stored alert's direction is always "above" or "below".
    throw new IllegalArgumentException("unknown direction: " + direction);
  }
}
