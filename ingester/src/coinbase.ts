import type { CoinbaseMatchMessage } from "./types.js";

export const COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com";

export function buildSubscribeMessage(symbols: string[]): string {
  return JSON.stringify({
    type: "subscribe",
    product_ids: symbols,
    channels: ["matches"],
  });
}

// Runtime boundary check for data arriving over the WebSocket — TypeScript's static
// types don't apply to network input (see DECISIONS.md). Anything that fails this
// is logged and dropped, never produced to Kafka.
export function isMatchMessage(msg: unknown, configuredSymbols: string[]): msg is CoinbaseMatchMessage {
  if (typeof msg !== "object" || msg === null) return false;
  const m = msg as Record<string, unknown>;

  return (
    m.type === "match" &&
    typeof m.price === "string" &&
    typeof m.size === "string" &&
    typeof m.trade_id === "number" &&
    typeof m.product_id === "string" &&
    configuredSymbols.includes(m.product_id) &&
    typeof m.time === "string" &&
    !Number.isNaN(Date.parse(m.time))
  );
}
