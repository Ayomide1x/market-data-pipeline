// Kafka message contract, schema v1 (see DECISIONS.md and the project's data-contracts section).
export interface Tick {
  v: 1;
  symbol: string;
  price: string;
  size: string;
  exchangeTs: number;
  ingestTs: number;
  exchangeSeq: number;
}

// Coinbase's `matches` channel message shapes we distinguish by `type`.
// Fields beyond what we read are present on the wire but not modeled here.

export interface CoinbaseMatchMessage {
  type: "match";
  trade_id: number;
  product_id: string;
  price: string;
  size: string;
  time: string;
  sequence: number;
}

export interface CoinbaseLastMatchMessage {
  type: "last_match";
  trade_id: number;
  product_id: string;
}

export interface CoinbaseSubscriptionsMessage {
  type: "subscriptions";
  channels: Array<{ name: string; product_ids: string[] }>;
}

export interface CoinbaseErrorMessage {
  type: "error";
  message: string;
  reason?: string;
}
