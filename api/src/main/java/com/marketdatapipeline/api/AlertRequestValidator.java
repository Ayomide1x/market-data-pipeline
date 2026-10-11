package com.marketdatapipeline.api;

// Pure logic, no Redis/HTTP wiring — kept separate from AlertService so direction/threshold
// validation has exactly one implementation to be wrong in, and can be unit tested without
// a Redis connection. See DECISIONS.md.
public final class AlertRequestValidator {

  private AlertRequestValidator() {}

  public static void requireDirection(String direction) {
    if (!"above".equals(direction) && !"below".equals(direction)) {
      throw new InvalidAlertRequestException("direction must be \"above\" or \"below\"");
    }
  }

  public static double parseThreshold(String threshold) {
    double parsed;
    try {
      parsed = Double.parseDouble(threshold);
    } catch (NumberFormatException | NullPointerException e) {
      throw new InvalidAlertRequestException("threshold must be a valid positive number");
    }
    // Double.isFinite, not just parsed > 0: Double.parseDouble accepts the literal strings
    // "NaN" and "Infinity" as valid doubles, and Infinity > 0 is true — neither is a sane
    // price threshold, so both must be rejected explicitly, not just non-positive values.
    if (!Double.isFinite(parsed) || parsed <= 0) {
      throw new InvalidAlertRequestException("threshold must be a valid positive number");
    }
    return parsed;
  }
}
