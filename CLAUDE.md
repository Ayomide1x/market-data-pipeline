# Real-Time Market Data & Alerting Pipeline

## What this is

Streams live trades from one public crypto exchange into Kafka, computes a per-symbol exponential moving average (EMA) and evaluates user-defined price alerts in a Java consumer, caches current state in Redis, and pushes live updates to a React frontend over Server-Sent Events. Publicly deployed: Docker Compose stack on one Azure VM behind Caddy, frontend on Vercel.

```
exchange WS ──▶ ingester (Node) ──▶ Kafka topic `ticks` (key = symbol)
                                          │
                                          ▼
                          consumer (Spring Boot, group `ema-consumers`, N instances)
                                          │  apply_tick.lua (CAS on offset) + PUBLISH
                                          ▼
                                        Redis ◀── alert CRUD ── api (Spring Boot)
                                          │  pub/sub                 │ REST + SSE
                                          └──────────────▶ api ──────▶ frontend (React/Vite, Vercel)
```

## Stack

- **ingester/** — current Node.js LTS. The Kafka client choice is a Stage 1 DECISIONS entry. KafkaJS has had little maintenance; Confluent's JavaScript client is the supported alternative. Evaluate before choosing.
- **consumer/** — Java (LTS), Spring Boot, Spring for Apache Kafka. Computes EMA, evaluates alerts. Never serves HTTP beyond health checks.
- **api/** — Java, Spring Boot. REST + SSE. Reads only from Redis; never touches Kafka.
- **frontend/** — React + Vite. Deployed separately to Vercel.
- **Kafka** — single broker, KRaft mode (no ZooKeeper). Replication factor 1 (single broker; accepted for this project).
- **Redis** — AOF persistence on.
- **Caddy** — production only; automatic TLS, reverse proxy to `api`.
- **Sentry** — ingester, consumer, api, frontend.
- Pin versions in the build files and record why in DECISIONS.md.

**Exchange:** Coinbase or Kraken public WebSocket. **Not Binance global**: it blocks US IPs, and the VM will be in a US region. Before writing ingester code, confirm the chosen exchange's public trade/ticker channel requires no API key.

## Configuration

- Symbols come from config (`SYMBOLS=BTC-USD,ETH-USD,SOL-USD`), never hardcoded anywhere, including tests and frontend. Dev: 3 symbols. Production: 10–20.
- EMA time constant, alert caps, throttle rates, Kafka/Redis addresses, CORS origin: all from env. Commit `.env.example`, never `.env`.
- `compose.yaml` for local dev; `compose.prod.yaml` override for production (memory limits, Caddy, no published Kafka/Redis ports).

## Data contracts (initial proposal; changes go in DECISIONS.md)

**Kafka message** (topic `ticks`, key = symbol, JSON value):
`{ "v": 1, "symbol": "BTC-USD", "price": "64012.55", "size": "0.012", "exchangeTs": 1726600000123, "ingestTs": 1726600000140, "exchangeSeq": 12345 }`
Prices travel as strings. EMA is computed in `double` (it's an indicator, not a ledger); record this.

**Redis:**
- `state:{symbol}` (hash): `price`, `ema`, `exchangeTs`, `lastOffset`
- `alert:{id}` (hash): `symbol`, `direction` (above|below), `threshold`, `status` (armed|fired), `firedAtOffset`, `createdAt`. TTL applied.
- `alerts:{symbol}` (set of alert ids)
- Pub/sub channels: `ticks:updates`, `alerts:fired`

## Code conventions

- Structured JSON logs. Every consumer log line about a record includes `symbol`, `partition`, `offset`.
- Pure logic (EMA math, crossing detection) lives in plain classes with unit tests, separate from Kafka/Redis wiring.
- Lua scripts live in their own files, loaded at startup, with a header comment stating what they make atomic.
- Graceful shutdown everywhere: ingester flushes the producer on SIGTERM; consumer finishes the in-flight record and commits before leaving the group.
- Comments must not contradict module docstrings. When behavior changes, update both.

## DECISIONS.md

Every non-obvious choice gets an entry, written when the decision is made, not after:

```
## <title>
Chose: …
Over: …
Because: …
Breaks if: …
```

## Build order

One stage at a time. Do not start a stage until the previous one is verified. Each stage ends with a verification the user runs themselves.

**Stage 0 — Infrastructure skeleton.** Compose with Kafka (KRaft) and Redis (AOF). `auto.create.topics.enable=false`. An explicit topic-creation step creates `ticks` with a fixed partition count and explicit retention/segment configs (see "Always-on").
*Done when:* `kafka-topics --describe` shows the intended partitions and configs; `redis-cli CONFIG GET appendonly` returns yes.

**Stage 1 — Vertical slice.** Ingester subscribes to one symbol and produces normalized ticks keyed by symbol. Consumer logs symbol, partition, offset, and price. Nothing else.
*Done when:* `kafka-console-consumer --property print.key=true` shows keyed ticks, and consumer logs match.

**Stage 2 — Ingester hardening.**
- All symbols from config; schema v1.
- Idempotent producer with `acks=all`, so retries can't reorder or duplicate within a partition.
- Reconnect with full-jitter exponential backoff.
- Stale-connection detection: no message for N seconds triggers a reconnect.
- Log exchange sequence gaps. There is no backfill; log and move on.
- Flush on SIGTERM.

*Done when:* killing the network forces a reconnect that recovers on its own, gaps are logged, and SIGTERM loses nothing already received.

**Stage 3 — EMA + durable state + commit-after-write.**
- Time-decayed EMA: `alpha = 1 - exp(-Δt/τ)`, with Δt taken from `exchangeTs`, never wall clock, so replays compute identically.
- In-memory state per symbol, lazily loaded from Redis on first tick.
- `apply_tick.lua` writes state only if the incoming offset is greater than the stored `lastOffset`. It returns applied or skipped.
- Manual offset commit happens only after the script returns.
- Build `tools/verify-ema`, which recomputes EMA from the start of the topic and compares it to Redis.

*Done when:* `kill -9` the consumer mid-stream and restart it. Logs show replayed offsets being skipped, and `verify-ema` matches.

**Stage 4 — Consumer group & rebalancing.**
- Run 2–3 consumer instances with the cooperative-sticky assignor.
- On partition revocation, clear the in-memory state for those partitions' symbols.
- Log assignments and revocations.

*Done when:* killing one instance moves its partitions to the others, `kafka-consumer-groups --describe` shows lag returning to ~0, and `verify-ema` still matches.

**Stage 5 — API read path + alert CRUD.**
- `api` service with REST snapshot endpoints (all symbols, one symbol) read from Redis.
- Alert create/list/delete, validated against configured symbols.
- Global alert cap and per-client rate limit on creation.
- Alerts carry a TTL.

*Done when:* the endpoints work via curl, the cap is enforced, and the API keeps serving with the consumer stopped.

**Stage 6 — Alert evaluation.**
- Consumer evaluates crossings: previous price on one side of the threshold, new price on the other. A price that is simply above the threshold does not count.
- Firing is recorded inside `apply_tick.lua`, in the same atomic write as the state and offset.

*Done when:* an alert set near the current price fires exactly once. `kill -9` the consumer right after firing and restart it; the alert does not fire again.

**Stage 7 — Live push over SSE.**
- `apply_tick.lua` PUBLISHes only when it applied a tick. The api subscribes and fans out to SSE clients.
- On connect, subscribe first, then send the snapshot, so no update falls in the gap.
- Per-client coalescing/throttle per symbol.
- Heartbeat comment every ~15s.
- Alert-fired events go on the same stream.

*Done when:* `curl -N` shows a snapshot followed by deltas; two browsers stay in sync; killing the api makes clients reconnect and resync.

**Stage 8 — Frontend.**
- Live table of price + EMA per symbol.
- Alert create/delete UI, with toasts when alerts fire.
- Visible connection-status indicator; reconnect on drop.
- API base URL from env. Handle an empty/loading state.

*Done when:* it works against the local stack, and killing the api shows disconnected → reconnected in the UI.

**Stage 9 — Production hardening, run locally first.**
- `compose.prod.yaml`: container memory limits, explicit JVM heaps, Docker log rotation, restart policies, health checks.
- Sentry in all four components.
- CORS allowlist set to the exact frontend origin.
- Redis `maxmemory` with `noeviction`.

*Done when:* the prod stack runs locally for several hours with `docker stats` under limits and disk usage flat.

**Stage 10 — Deploy.**
- Azure VM; network security group allowing only 443/80, plus 22 from your own IP.
- Docker starts on boot. Caddy with the .dev domain.
- Frontend on Vercel. Azure budget alert configured.

*Done when:* the public URL works on a phone over cellular, and survives a VM reboot with no manual steps.

## Invariants

1. Every tick for a symbol goes to exactly one partition (key = symbol). The partition count of `ticks` never changes after creation. Changing it remaps keys and breaks both per-symbol ordering and offset-based dedupe.
2. A symbol's EMA, last price, and `lastOffset` are written atomically. A tick whose offset is ≤ the stored `lastOffset` changes nothing.
3. The consumer commits an offset only after the Redis write for that record succeeded.
4. The in-memory state cache never outlives partition ownership. It is cleared on revocation.
5. An alert fires at most once per crossing. Firing is recorded in the same atomic write as the tick that caused it.
6. EMA depends only on tick data (`exchangeTs`, price), never on wall-clock or processing time.
7. The api never reads Kafka and never holds authoritative state. Redis is the source of truth for everything it serves.
8. No symbol is hardcoded anywhere.
9. Every topic has explicit `retention.ms`, `retention.bytes`, and segment settings. No environment runs on broker defaults.
10. Kafka and Redis are never reachable from the public internet.

## Out of scope

- Multiple exchanges
- Historical backfill or charts beyond the live session
- Any database besides Redis
- Kubernetes
- User accounts/auth; alerts are anonymous, capped, and expiring
- Kafka transactions / exactly-once semantics; the offset-CAS gives effectively-once state without them
- Schema registry / Avro
- Multi-broker Kafka or replication
- Order books / level-2 data
- Trading of any kind

## Always-on vs local dev

- **Kafka retention works per segment.** Only closed segments are deleted, and the default segment roll is 7 days. `retention.ms` alone won't protect the disk on a low-volume topic. Set `segment.ms`/`segment.bytes` small (on the order of an hour / tens of MB) alongside `retention.ms` (hours, not days) and a per-partition `retention.bytes`.
- **Consumer group offsets expire** (`offsets.retention.minutes`, default 7 days). After a long outage, `auto.offset.reset` decides where consumers restart. Choose it deliberately and record it.
- **Docker container logs** grow without bound by default. Configure json-file `max-size`/`max-file`.
- **Memory:** three JVMs (Kafka, consumer, api) plus Redis, Node, and Caddy on 4GB. Every JVM gets an explicit heap well under its container limit.
- **Kafka `advertised.listeners`:** internal Compose network only in prod.
- **Exchanges drop long-lived WebSockets routinely.** Reconnect must be tested, not assumed.
- **SSE through proxies:** heartbeats prevent idle-timeout disconnects. Confirm Caddy flushes `text/event-stream` immediately.
- **Redis:** AOF rewrite enabled; `maxmemory` set with `noeviction`, so a full Redis errors loudly instead of silently evicting state.
- **Cost:** check the VM's monthly cost against the credit and set a budget alert.
