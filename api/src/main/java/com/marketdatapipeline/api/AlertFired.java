package com.marketdatapipeline.api;

// Shape of an alerts:fired pub/sub message. direction/threshold are read inside
// apply_tick.lua via HMGET on the alert's own hash — Java never had them in hand at
// publish time, only the key and offset. See DECISIONS.md for that cost and the
// rejected alternative (publish bare {id, symbol, firedAtOffset}, make the client fetch
// details separately).
public record AlertFired(String id, String symbol, String direction, String threshold, long firedAtOffset) {}
