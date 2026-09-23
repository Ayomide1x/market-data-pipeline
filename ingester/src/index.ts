import { WebSocket } from "ws";
import confluentKafka from "@confluentinc/kafka-javascript";
import { loadConfig } from "./config.js";
import {
  COINBASE_WS_URL,
  buildSubscribeMessage,
  isMatchMessage,
  isLastMatchMessage,
  isHeartbeatMessage,
  isSubscriptionsMessage,
  findMissingSymbols,
} from "./coinbase.js";
import type { Tick } from "./types.js";

const { KafkaJS } = confluentKafka;
const { Kafka } = KafkaJS;

const TOPIC = "ticks";
const EX_CONFIG = 78;

const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;
const HEARTBEAT_STALE_MS = 10_000;
const BACKOFF_BASE_MS = 500;
const BACKOFF_CAP_MS = 30_000;
const PRODUCER_MESSAGE_TIMEOUT_MS = 30_000;
const FLUSH_TIMEOUT_MS = 5_000;

function log(level: "info" | "warn" | "error", msg: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ level, msg, ts: new Date().toISOString(), ...fields });
  if (level === "error") {
    console.error(line);
  } else {
    console.log(line);
  }
}

// Routes the Kafka client's own logs (both its JS-level events and native librdkafka log
// callbacks) through our structured JSON, instead of its default `console.info({...})`
// util.inspect-formatted output. `setLogLevel`/`namespace` are required by the client's
// internal calls even though we don't use them (see DECISIONS.md).
const kafkaLogger = {
  info: (message: string, extra?: object) => log("info", message, extra as Record<string, unknown> | undefined),
  warn: (message: string, extra?: object) => log("warn", message, extra as Record<string, unknown> | undefined),
  error: (message: string, extra?: object) => log("error", message, extra as Record<string, unknown> | undefined),
  debug: (message: string, extra?: object) => log("info", message, extra as Record<string, unknown> | undefined),
  setLogLevel: (_level: unknown) => {},
  namespace: (_namespace: string) => kafkaLogger,
};

function fullJitterDelayMs(attempt: number): number {
  const cap = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt);
  return Math.random() * cap;
}

async function main(): Promise<void> {
  const config = loadConfig();
  log("info", "starting ingester", { symbols: config.symbols, kafkaBootstrapServers: config.kafkaBootstrapServers });

  const kafka = new Kafka();
  const producer = kafka.producer({
    "bootstrap.servers": config.kafkaBootstrapServers,
    "message.timeout.ms": PRODUCER_MESSAGE_TIMEOUT_MS,
    kafkaJS: {
      idempotent: true,
      acks: -1,
      maxInFlightRequests: 5,
      retry: { retries: 2147483647 },
      logger: kafkaLogger,
    },
  });
  await producer.connect();
  log("info", "kafka producer connected");

  let stopping = false;
  let ws: WebSocket | null = null;
  let reconnectAttempt = 0;
  let connectCount = 0;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let pingTimer: NodeJS.Timeout | null = null;
  let pongTimeoutTimer: NodeJS.Timeout | null = null;
  let staleCheckTimer: NodeJS.Timeout | null = null;

  // Gap-detection baseline: what trade_id we expect next, per symbol. Reset from
  // `last_match` on every (re)connect.
  const lastTradeId = new Map<string, number>();
  // Duplicate-suppression watermark: highest trade_id actually produced, per symbol.
  // Never reset across reconnects — see DECISIONS.md for why this is a separate map.
  const highestProducedTradeId = new Map<string, number>();
  const firstMatchSeenSinceConnect = new Set<string>();
  const lastHeartbeatAt = new Map<string, number>();
  // A heartbeat can report a trade_id slightly ahead of that trade's own `match` message
  // still being in flight — not a real gap. Stores the *suspected* trade_id from the first
  // sighting per symbol; only confirmed (against that stored id, not a fresher one) if the
  // baseline is still behind it on the next heartbeat. See DECISIONS.md.
  const pendingHeartbeatGapTradeId = new Map<string, number>();

  // `lastTradeId` must never move backwards — match, last_match, and heartbeat handling all
  // advance it independently and can race, so every update goes through this. See DECISIONS.md.
  function advanceLastTradeId(symbol: string, tradeId: number): void {
    const current = lastTradeId.get(symbol);
    if (current === undefined || tradeId > current) {
      lastTradeId.set(symbol, tradeId);
    }
  }

  function clearConnectionTimers(): void {
    if (pingTimer) clearInterval(pingTimer);
    if (pongTimeoutTimer) clearTimeout(pongTimeoutTimer);
    if (staleCheckTimer) clearInterval(staleCheckTimer);
    pingTimer = null;
    pongTimeoutTimer = null;
    staleCheckTimer = null;
  }

  function clearReconnectTimer(): void {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  // Single funnel for every reconnect trigger (dead socket, missed pong, stale heartbeat,
  // an ordinary close): all of them just terminate the socket, which raises `close`, which
  // is the only place that decides whether to schedule the next attempt. See DECISIONS.md.
  function scheduleReconnect(): void {
    if (stopping) return;
    const delay = fullJitterDelayMs(reconnectAttempt);
    reconnectAttempt += 1;
    log("warn", "scheduling coinbase reconnect", { attempt: reconnectAttempt, delayMs: Math.round(delay) });
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (stopping) return;
      connect();
    }, delay);
  }

  // Any producer.send() rejection — final delivery-timeout failure or a full local queue —
  // is fatal: continuing would leave a hole in Kafka that Coinbase-side gap detection can't
  // see. See DECISIONS.md.
  async function terminateFatally(exitCode: number): Promise<void> {
    if (stopping) return;
    stopping = true;
    clearConnectionTimers();
    clearReconnectTimer();
    ws?.terminate();
    try {
      await producer.flush({ timeout: FLUSH_TIMEOUT_MS });
    } catch (err) {
      log("error", "flush failed during fatal shutdown", { error: String(err) });
    }
    await producer.disconnect().catch(() => {});
    process.exit(exitCode);
  }

  function produceTick(tick: Tick): void {
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
        log("error", "fatal: failed to produce tick, exiting to avoid a silent gap", {
          symbol: tick.symbol,
          exchangeSeq: tick.exchangeSeq,
          error: String(err),
        });
        void terminateFatally(1);
      });
  }

  function connect(): void {
    connectCount += 1;
    const isReconnect = connectCount > 1;
    const socket = new WebSocket(COINBASE_WS_URL);
    ws = socket;
    firstMatchSeenSinceConnect.clear();
    pendingHeartbeatGapTradeId.clear();

    socket.on("open", () => {
      log("info", "coinbase websocket connected");
      socket.send(buildSubscribeMessage(config.symbols));

      const now = Date.now();
      for (const symbol of config.symbols) lastHeartbeatAt.set(symbol, now);

      pingTimer = setInterval(() => {
        if (pongTimeoutTimer) return; // already waiting on a previous ping
        socket.ping();
        pongTimeoutTimer = setTimeout(() => {
          log("warn", "coinbase pong timeout, forcing reconnect");
          socket.terminate();
        }, PONG_TIMEOUT_MS);
      }, PING_INTERVAL_MS);

      staleCheckTimer = setInterval(() => {
        const checkedAt = Date.now();
        for (const symbol of config.symbols) {
          const last = lastHeartbeatAt.get(symbol) ?? checkedAt;
          if (checkedAt - last > HEARTBEAT_STALE_MS) {
            log("warn", "coinbase heartbeat stale for symbol, forcing reconnect", {
              symbol,
              msSinceLastHeartbeat: checkedAt - last,
            });
            socket.terminate();
            return;
          }
        }
      }, HEARTBEAT_STALE_MS);
    });

    socket.on("pong", () => {
      if (pongTimeoutTimer) {
        clearTimeout(pongTimeoutTimer);
        pongTimeoutTimer = null;
      }
    });

    socket.on("message", (raw: Buffer) => {
      let msg: unknown;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        log("warn", "dropped non-JSON message from coinbase", { raw: raw.toString().slice(0, 200) });
        return;
      }

      const type = (msg as Record<string, unknown>)?.type;

      if (type === "match") {
        if (!isMatchMessage(msg, config.symbols)) {
          log("warn", "dropped match message that failed validation", { msg });
          return;
        }

        const symbol = msg.product_id;
        const tradeId = msg.trade_id;

        const watermark = highestProducedTradeId.get(symbol) ?? -1;
        if (tradeId <= watermark) {
          log("warn", "dropped duplicate match (trade_id already produced)", { symbol, tradeId, watermark });
          return;
        }

        const expected = lastTradeId.get(symbol);
        if (expected !== undefined && tradeId > expected + 1) {
          log("warn", "gap detected in trade_id sequence", {
            symbol,
            expectedTradeId: expected + 1,
            gotTradeId: tradeId,
            gapSize: tradeId - expected - 1,
            detectedVia: "match",
            sinceReconnect: !firstMatchSeenSinceConnect.has(symbol),
          });
        }
        firstMatchSeenSinceConnect.add(symbol);
        advanceLastTradeId(symbol, tradeId);
        highestProducedTradeId.set(symbol, tradeId);

        const tick: Tick = {
          v: 1,
          symbol,
          price: msg.price,
          size: msg.size,
          exchangeTs: Date.parse(msg.time),
          ingestTs: Date.now(),
          exchangeSeq: tradeId,
        };
        produceTick(tick);
        return;
      }

      if (type === "heartbeat") {
        if (!isHeartbeatMessage(msg, config.symbols)) {
          log("warn", "dropped heartbeat message that failed validation", { msg });
          return;
        }

        lastHeartbeatAt.set(msg.product_id, Date.now());
        const symbol = msg.product_id;

        // Resolve any suspicion from the *previous* heartbeat first, against the trade_id
        // it actually suspected — not this fresh heartbeat's, which in a busy market is
        // almost always a different, higher value (see DECISIONS.md for the bug this fixes).
        const pendingTradeId = pendingHeartbeatGapTradeId.get(symbol);
        if (pendingTradeId !== undefined) {
          const expected = lastTradeId.get(symbol);
          if (expected !== undefined && expected < pendingTradeId) {
            log("warn", "gap detected via heartbeat (no match message revealed it)", {
              symbol,
              expectedTradeId: expected + 1,
              coinbaseLastTradeId: pendingTradeId,
              gapSize: pendingTradeId - expected,
              detectedVia: "heartbeat",
            });
            advanceLastTradeId(symbol, pendingTradeId);
          }
          pendingHeartbeatGapTradeId.delete(symbol);
        }

        // Now evaluate this heartbeat as a new, independent suspicion.
        const expectedNow = lastTradeId.get(symbol);
        if (expectedNow !== undefined && msg.last_trade_id > expectedNow) {
          pendingHeartbeatGapTradeId.set(symbol, msg.last_trade_id);
        }
        return;
      }

      if (type === "last_match") {
        if (!isLastMatchMessage(msg, config.symbols)) {
          log("warn", "dropped last_match message that failed validation", { msg });
          return;
        }

        const symbol = msg.product_id;
        const tradeId = msg.trade_id;
        const previousExpected = lastTradeId.get(symbol);

        if (isReconnect) {
          // A jump here means trades happened during the outage window this last_match
          // snapshot now spans — permanent, unrecoverable (no backfill), logged as such.
          if (previousExpected !== undefined && tradeId > previousExpected + 1) {
            log("warn", "gap detected across reconnect (outage window)", {
              symbol,
              expectedTradeId: previousExpected + 1,
              gotTradeId: tradeId,
              gapSize: tradeId - previousExpected - 1,
              detectedVia: "reconnect",
            });
          }
          advanceLastTradeId(symbol, tradeId);

          // Produce the trade itself too — it happened during the outage and would
          // otherwise never reach Kafka at all. Watermark-checked like any other trade,
          // since it may be one we already produced just before the disconnect.
          const watermark = highestProducedTradeId.get(symbol) ?? -1;
          if (tradeId > watermark) {
            highestProducedTradeId.set(symbol, tradeId);
            produceTick({
              v: 1,
              symbol,
              price: msg.price,
              size: msg.size,
              exchangeTs: Date.parse(msg.time),
              ingestTs: Date.now(),
              exchangeSeq: tradeId,
            });
          }
        } else {
          // First connect of this process: nothing to compare against, and this trade may
          // already be sitting in Kafka from before this process started. See DECISIONS.md.
          advanceLastTradeId(symbol, tradeId);
        }

        log("info", "received last_match snapshot", { msg });
        return;
      }

      if (type === "subscriptions") {
        if (!isSubscriptionsMessage(msg)) {
          log("error", "coinbase subscriptions confirmation had an unexpected shape", { msg });
          return;
        }

        const missing = findMissingSymbols(msg, config.symbols);
        if (missing.length > 0) {
          log("error", "coinbase did not confirm all configured symbols on every channel — config error, not retrying", {
            missing,
            confirmed: msg,
          });
          void terminateFatally(EX_CONFIG);
          return;
        }

        reconnectAttempt = 0;
        log("info", "coinbase subscriptions confirmed", { msg });
        return;
      }

      if (type === "error") {
        log("error", "coinbase sent an error message", { msg });
        return;
      }

      log("warn", "unrecognized coinbase message type", { type });
    });

    socket.on("error", (err) => {
      log("error", "coinbase websocket error", { error: String(err) });
    });

    socket.on("close", (code, reason) => {
      log("warn", "coinbase websocket closed", { code, reason: reason.toString() });
      clearConnectionTimers();
      if (!stopping) scheduleReconnect();
    });
  }

  connect();

  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    log("info", "shutting down", { signal });
    clearConnectionTimers();
    clearReconnectTimer();
    ws?.close();

    let exitCode = 0;
    try {
      await producer.flush({ timeout: FLUSH_TIMEOUT_MS });
    } catch (err) {
      log("error", "shutdown: flush failed, some produced ticks may be lost", { error: String(err) });
      exitCode = 1;
    }
    await producer.disconnect().catch(() => {});
    process.exit(exitCode);
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  log("error", "ingester crashed", { error: String(err) });
  process.exit(1);
});
