package com.marketdatapipeline.consumer;

// Kafka message contract, schema v1 (see DECISIONS.md and the project's data-contracts
// section) — mirrors the ingester's Tick interface field-for-field.
public record TickPayload(
    int v,
    String symbol,
    String price,
    String size,
    long exchangeTs,
    long ingestTs,
    long exchangeSeq) {}
