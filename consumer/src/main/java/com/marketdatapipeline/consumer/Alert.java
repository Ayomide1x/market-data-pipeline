package com.marketdatapipeline.consumer;

// An armed alert as read from alert:{id} — only the fields crossing detection needs.
public record Alert(String id, String direction, double threshold) {}
