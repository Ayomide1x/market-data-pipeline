package com.marketdatapipeline.consumer;

// Thrown when a symbol's incoming record.partition() disagrees with what apply_tick.lua
// has on record for it — a deterministic fact, never retryable: offsets between two
// different partitions are not comparable at all (invariant #1). See DECISIONS.md.
public class PartitionMismatchException extends RuntimeException {

  public PartitionMismatchException(String symbol, int incomingPartition, int storedPartition) {
    super(
        "symbol="
            + symbol
            + " arrived on partition="
            + incomingPartition
            + " but state:"
            + symbol
            + " was last written from partition="
            + storedPartition
            + " — offsets between these partitions are not comparable (invariant #1 violated)");
  }
}
