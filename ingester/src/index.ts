import { WebSocket } from "ws";
import confluentKafka from "@confluentinc/kafka-javascript";
import { loadConfig } from "./config.js";
import { COINBASE_WS_URL, buildSubscribeMessage, isMatchMessage } from "./coinbase.js";
import type { Tick } from "./types.js";

const { KafkaJS } = confluentKafka;
const { Kafka } = KafkaJS;

const TOPIC = "ticks";

function log(level: "info" | "warn" | "error", msg: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ level, msg, ts: new Date().toISOString(), ...fields });
  if (level === "error") {
    console.error(line);
  } else {
    console.log(line);
  }
}

async function main(): Promise<void> {
  const config = loadConfig();
  log("info", "starting ingester", { symbols: config.symbols, kafkaBootstrapServers: config.kafkaBootstrapServers });

  const kafka = new Kafka();
  const producer = kafka.producer({
    "bootstrap.servers": config.kafkaBootstrapServers,
  });
  await producer.connect();
  log("info", "kafka producer connected");

  const ws = new WebSocket(COINBASE_WS_URL);

  ws.on("open", () => {
    log("info", "coinbase websocket connected");
    ws.send(buildSubscribeMessage(config.symbols));
  });

  ws.on("message", (raw: Buffer) => {
    let msg: unknown;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      log("warn", "dropped non-JSON message from coinbase", { raw: raw.toString().slice(0, 200) });
      return;
    }

    const type = (msg as Record<string, unknown>)?.type;

    if (type === "match") {
      if (!isMatchMessage(msg, config.symbols)) {
        log("warn", "dropped match message that failed validation", { msg });
        return;
      }

      const tick: Tick = {
        v: 1,
        symbol: msg.product_id,
        price: msg.price,
        size: msg.size,
        exchangeTs: Date.parse(msg.time),
        ingestTs: Date.now(),
        exchangeSeq: msg.trade_id,
      };

      producer
        .send({
          topic: TOPIC,
          messages: [{ key: tick.symbol, value: JSON.stringify(tick) }],
        })
        .then((results) => {
          const result = results[0];
          log("info", "produced tick", {
            symbol: tick.symbol,
            price: tick.price,
            exchangeSeq: tick.exchangeSeq,
            partition: result?.partition,
            offset: result?.baseOffset,
          });
        })
        .catch((err: unknown) => {
          log("error", "failed to produce tick", { symbol: tick.symbol, error: String(err) });
        });
      return;
    }

    if (type === "last_match") {
      // Deliberately not produced — see DECISIONS.md. Its trade_id is a candidate
      // baseline for Stage 2 gap detection, not data to forward now.
      log("info", "received last_match snapshot, skipping", { msg });
      return;
    }

    if (type === "subscriptions") {
      log("info", "coinbase subscriptions confirmed", { msg });
      return;
    }

    if (type === "error") {
      log("error", "coinbase sent an error message", { msg });
      return;
    }

    log("warn", "unrecognized coinbase message type", { type });
  });

  ws.on("error", (err) => {
    log("error", "coinbase websocket error", { error: String(err) });
  });

  ws.on("close", (code, reason) => {
    log("warn", "coinbase websocket closed", { code, reason: reason.toString() });
  });

  const shutdown = async (signal: string): Promise<void> => {
    log("info", "shutting down", { signal });
    ws.close();
    await producer.disconnect();
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  log("error", "ingester crashed", { error: String(err) });
  process.exit(1);
});
