package com.marketdatapipeline.api;

// id is a random v4 UUID, handed back on create — knowing it is the only "ownership" check
// there is (see DECISIONS.md: no accounts, nothing personal in an alert, worst case of a
// leaked id is someone deletes it).
public record AlertView(
    String id, String symbol, String direction, String threshold, String status, long createdAt) {}
