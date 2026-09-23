import type {
  CoinbaseHeartbeatMessage,
  CoinbaseLastMatchMessage,
  CoinbaseMatchMessage,
  CoinbaseSubscriptionsMessage,
} from "./types.js";

export const COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com";

// `heartbeat`, not just `matches`: ticks once a second per subscribed product regardless
// of trade volume, which Stage 2 needs both as a stale-feed signal and as a secondary
// gap-detection source for quiet symbols (see DECISIONS.md).
export function buildSubscribeMessage(symbols: string[]): string {
  return JSON.stringify({
    type: "subscribe",
    product_ids: symbols,
    channels: ["matches", "heartbeat"],
  });
}

// Runtime boundary check for data arriving over the WebSocket — TypeScript's static
// types don't apply to network input (see DECISIONS.md). Anything that fails this
// is logged and dropped, never produced to Kafka. `match` and `last_match` share the
// same field shape on the wire (see DECISIONS.md), so both guards share this check.
function isTradeMessage(
  msg: unknown,
  type: "match" | "last_match",
  configuredSymbols: string[],
): msg is CoinbaseMatchMessage | CoinbaseLastMatchMessage {
  if (typeof msg !== "object" || msg === null) return false;
  const m = msg as Record<string, unknown>;

  return (
    m.type === type &&
    typeof m.price === "string" &&
    typeof m.size === "string" &&
    typeof m.trade_id === "number" &&
    typeof m.product_id === "string" &&
    configuredSymbols.includes(m.product_id) &&
    typeof m.time === "string" &&
    !Number.isNaN(Date.parse(m.time))
  );
}

export function isMatchMessage(msg: unknown, configuredSymbols: string[]): msg is CoinbaseMatchMessage {
  return isTradeMessage(msg, "match", configuredSymbols);
}

export function isLastMatchMessage(msg: unknown, configuredSymbols: string[]): msg is CoinbaseLastMatchMessage {
  return isTradeMessage(msg, "last_match", configuredSymbols);
}

export function isHeartbeatMessage(msg: unknown, configuredSymbols: string[]): msg is CoinbaseHeartbeatMessage {
  if (typeof msg !== "object" || msg === null) return false;
  const m = msg as Record<string, unknown>;

  return (
    m.type === "heartbeat" &&
    typeof m.last_trade_id === "number" &&
    typeof m.product_id === "string" &&
    configuredSymbols.includes(m.product_id)
  );
}

export function isSubscriptionsMessage(msg: unknown): msg is CoinbaseSubscriptionsMessage {
  if (typeof msg !== "object" || msg === null) return false;
  const m = msg as Record<string, unknown>;
  return m.type === "subscriptions" && Array.isArray(m.channels);
}

// Config error, not a network error (see DECISIONS.md): a symbol missing from either
// channel's confirmed product_ids means Coinbase silently isn't tracking it there — a
// typo'd or delisted symbol, not a transient failure a reconnect would fix. Checked against
// `matches` and `heartbeat` independently: a symbol must be confirmed on *both*, since a
// symbol present only on one channel is exactly as broken as being on neither (missing
// trade data, or missing the stale-feed/gap-detection signal that channel exists for).
export function findMissingSymbols(msg: CoinbaseSubscriptionsMessage, configuredSymbols: string[]): string[] {
  const missing = new Set<string>();
  for (const channelName of ["matches", "heartbeat"]) {
    const channel = msg.channels.find((c) => c.name === channelName);
    const confirmed = new Set(channel?.product_ids ?? []);
    for (const symbol of configuredSymbols) {
      if (!confirmed.has(symbol)) missing.add(symbol);
    }
  }
  return [...missing];
}
