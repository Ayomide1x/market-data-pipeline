# Decisions

## Kafka distribution / image

Chose: `apache/kafka:4.3.1`, the official upstream image, pinned to an exact tag.
Over: `bitnami/kafka` and `confluentinc/cp-kafka`.
Because: Bitnami removed versioned tags from its free Docker Hub catalog on 2025-08-28 (Broadcom's catalog restructuring) — the free tier is now `latest`-only, which is incompatible with pinning versions in the build files. `cp-kafka` is versioned against Confluent Platform releases, not Kafka releases, which obscures the actual broker version for anyone reading this file later. `apache/kafka` has been KRaft-native since Kafka 3.7, and as of Kafka 4.0 ZooKeeper mode was removed entirely, so it's also the only mode the upstream project still ships.
Breaks if: Apache stops publishing this image or changes its tagging scheme; re-evaluate at that point rather than assuming Bitnami's free tier comes back. Verified: `docker pull apache/kafka:4.3.1` succeeds (digest `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`).

## Kafka storage formatting: rely on the image's own on-start format, no separate step

Chose: set `CLUSTER_ID` as a plain (non-`KAFKA_`-prefixed) environment variable on the `kafka` service; no separate `kafka-storage.sh format` step anywhere in the compose files.
Over: running `kafka-storage.sh format --cluster-id ... --config server.properties` as an init step (e.g. a second one-shot service, or a custom entrypoint wrapper).
Because: inspecting the image's actual startup path (`/etc/kafka/docker/launch`) shows it already calls `kafka.docker.KafkaDockerWrapper setup`, which formats storage using `CLUSTER_ID` on every start, and detects an already-formatted log directory to skip reformatting (matching on `"already formatted"` in the setup command's output). A separate manual formatting step would be redundant with what the entrypoint already does, and would fight over CLUSTER_ID sourcing.
Breaks if: nothing, as long as `CLUSTER_ID` stays a fixed value across restarts and the named volume isn't dropped — both already required by the fixed-cluster-ID decision below. Verified (partially): after `docker compose up`, `docker compose restart kafka` logs `Log directory /var/lib/kafka/data is already formatted. Use --ignore-formatted to ignore this directory and format the others.` — confirming the format-and-skip codepath runs. Note: `restart` only restarts the existing container's process, it does not recreate the container, so this only proves the skip-on-already-formatted logic fires — it does not by itself prove the data survives *container recreation* (`down`/`up`, or a container replaced by an image update). That's a stronger, separate claim; verify it with an actual `down` (no `-v`) then `up`.

## KRaft topology: single combined node

Chose: one node running both roles (`process.roles=broker,controller`), one-node quorum (`controller.quorum.voters=1@kafka:9093`).
Over: separate broker and controller containers.
Because: this project is single-broker, RF=1, multi-broker explicitly out of scope. A separate controller node is the production topology for multi-broker clusters where the controller quorum needs to survive a broker failure independently — with one broker there is nothing to fail over to, so it's pure extra config and an extra container for no benefit.
Breaks if: multi-broker is ever added later — combined mode would need to be split into a real controller quorum before adding brokers, not incrementally.

## Explicit topic creation, auto-create disabled

Chose: `auto.create.topics.enable=false` on the broker, plus a one-shot `kafka-init` Compose service, gated on `condition: service_healthy` for `kafka`, that on every run: (1) `kafka-topics --create --if-not-exists` with the full topic config, (2) `kafka-topics --describe` and checks the reported partition count equals the expected 6, exiting non-zero if it doesn't, (3) unconditionally runs `kafka-configs --alter --add-config` with the same retention/segment settings.
Over: a manual `docker compose run` step invoked by hand; leaving auto-create on; stopping at `--create --if-not-exists` alone.
Because: auto-create would create `ticks` with broker defaults (wrong partition count, no explicit retention — violates invariant #9) the first time anything produces to it. A manual step is easy to forget and isn't part of "the stack," which matters once the same compose shape needs to come up unattended after a VM reboot (Stage 10). `--if-not-exists` alone is not sufficient, though: it makes topic *creation* idempotent, but on every subsequent `up` it silently no-ops against an existing topic — so a retention/segment value changed in this file would never reach the already-running topic, which is itself a violation of invariant #9 (the running topic's config would drift from what's declared here). Re-running `kafka-configs --alter` every time closes that gap; it's a safe no-op when nothing changed. Partition count can't be fixed the same way — it's immutable after creation — so `kafka-init` checks it and fails loudly instead of silently running against a topic with the wrong shape.
Breaks if: someone bumps the partition count in this file for an already-created topic — `kafka-init` will refuse to proceed (by design; the topic must be deliberately recreated, which loses data, so that's a decision a human makes, not something automated). `restart: "no"` on `kafka-init` means a failed check stops there rather than retry-looping — confirmed by inspecting `compose.yaml`, no `restart` policy is set that would restart it.

## Partition count: 6, one topic definition for dev and prod

Chose: 6 partitions for `ticks`, same partition count and topic config used in both `compose.yaml` (dev) and `compose.prod.yaml` (prod) — one topic definition, not two.
Over: 3 (one partition per dev symbol), 12+, or separate dev/prod partition counts sized to each environment's symbol count.
Because: partition count doesn't scale with symbol count — each symbol's key always hashes to exactly one partition (invariant #1), so adding symbols never requires more partitions. What partition count actually caps is consumer parallelism: 6 divides evenly across the 2 or 3 consumer instances Stage 4 runs, so every instance gets an equal partition share with no leftover. Since partition count can never change after creation, it's set once for the topic's whole life rather than per-environment.
Breaks if: consumer instance count ever needs to exceed 6 — parallelism is hard-capped at the partition count regardless of how many instances are running. Note for Stage 4: with only 3 active symbol keys across 6 partitions, at least 3 partitions carry no traffic at any given time — the rebalance test must deliberately kill the instance that owns a partition with live traffic, not an arbitrary one, or the test will look like nothing happened.

## Retention and segment settings

Chose: `segment.ms=3600000` (1h), `segment.bytes=52428800` (50MB), `retention.ms=21600000` (6h), `retention.bytes=209715200` per partition (200MB), `cleanup.policy=delete`.
Over: leaving segment roll at the 7-day default and relying on `retention.ms` alone.
Because: retention only reclaims closed segments; a 7-day default segment roll means `retention.ms=6h` would be a no-op on a low-volume topic until a segment happens to fill or age out. Small `segment.ms`/`segment.bytes` forces segments to close often enough for time-based retention to actually execute, satisfying invariant #9.
Breaks if: **once segments are deleted under this policy, replaying from topic start no longer reproduces the full history behind the current EMA state in Redis** — `tools/verify-ema` (Stage 3) cannot assume "replay from earliest offset" reconstructs the same EMA from zero once retention has trimmed the topic. Stage 3's `verify-ema` will need to compare against Redis with a tolerance after a warm-up of several EMA time constants, rather than expecting an exact match from a cold replay. Recorded here for Stage 3; not building this now.

## Kafka listener topology

Chose: three listeners, with bind address separated from advertised address.
- `listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093` — what the broker process binds to inside the container. Always `0.0.0.0`: the container's own network namespace is already isolated by Docker, so there's nothing gained by binding narrower, and 0.0.0.0 is required for `EXTERNAL` to be reachable via a published port at all.
- `advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://127.0.0.1:29092` — what the broker tells clients to reconnect to after the initial connection. `INTERNAL` for containers on the Compose network (the consumer, `kafka-init`, anything else running inside Compose); `EXTERNAL` for host-side clients (CLI tools like `kafka-console-consumer`/`kcat`, and the ingester when run via `npm run dev` outside a container). No advertised address for `CONTROLLER` — KRaft controller traffic isn't a client-facing listener. `EXTERNAL` uses the literal `127.0.0.1`, not `localhost`: Docker publishes the port on the IPv4 loopback only, but `localhost` resolves to both `::1` and `127.0.0.1`, and a client that tries the IPv6 address first gets connection-refused before ever falling back to IPv4 — this is exactly what happened when testing from macOS (below).
- `listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT` — required once listener names are custom; the broker has no built-in mapping for names it doesn't recognize.
- `inter.broker.listener.name=INTERNAL` — broker-to-broker (and this broker's own controller-adjacent traffic) uses `INTERNAL`, never `EXTERNAL`. Without this, the broker defaults to looking for a listener literally named `PLAINTEXT` and refuses to start.
- `controller.listener.names=CONTROLLER` — tells the broker which listener carries KRaft quorum traffic.

Over: a single `kafka:9092` listener; letting `listeners` derive from `advertised.listeners` by the image's own default substitution.
Because: `kafka:9092` only resolves inside the Compose network, so a single-listener setup leaves host-run tools and the ingester (when iterated on outside a container) unable to reach the broker at all. Setting `listeners` explicitly (rather than relying on the image's `configureDefaults` substitution of hosts with `0.0.0.0`) keeps bind and advertised addresses visibly distinct in the compose file instead of one being implicit.
Breaks if: `EXTERNAL`'s port mapping ships in `compose.prod.yaml` by mistake — that would expose Kafka to the public internet, violating invariant #10 (see the compose file structure decision below for how the file split prevents this).

Verified from macOS itself (not from inside a container — `docker run --network host` on Docker Desktop for Mac only reaches the Docker VM, not the Mac's own network namespace, so that earlier check proved nothing about host reachability):
- First attempt, `kcat -b localhost:29092 -L`, failed: `localhost` resolved to `[::1]:29092` first and kcat got connection-refused, because `lsof -iTCP -sTCP:LISTEN -P` showed Docker's proxy listening on `127.0.0.1:29092` only — no `[::1]:29092` listener exists, matching the `127.0.0.1:29092:29092` binding in `compose.override.yaml`.
- After changing `advertised.listeners` to `EXTERNAL://127.0.0.1:29092`, `kcat -b 127.0.0.1:29092 -L` succeeded from macOS: metadata listed broker `1 at 127.0.0.1:29092` and topic `ticks`.
- Topic survives container recreation, not just an in-place restart: `docker compose down` (no `-v`) followed by `docker compose up` recreated the `kafka` container from scratch, `kafka` reported healthy, and `kafka-init`'s describe step showed `ticks` still at 6 partitions with the same `TopicId` — the named volume and fixed `CLUSTER_ID` did what the earlier "Fixed cluster ID, named data volume" decision claimed, this time actually proven across a real recreation rather than a same-container restart.

## Fixed cluster ID, named data volume

Chose: a fixed, hardcoded KRaft cluster ID (`CLUSTER_ID=0vdKmALERBCX9QAvTQWdcg`, generated once with `kafka-storage.sh random-uuid` and committed to `compose.yaml`) and a named volume (`kafka-data`) mounted at `/var/lib/kafka/data` (overriding the image's default `log.dirs=/tmp/kraft-combined-logs` for a clearer, explicitly-owned path).
Over: generating a random cluster ID at container startup, or using an anonymous/bind-mount-free volume.
Because: a cluster ID generated fresh on every startup would fail the image's own format-on-start step against an already-formatted volume (see the storage-formatting decision above) — the log directory would be stamped with the volume's original cluster ID forever, and a new random ID on each boot would mismatch it. Compose's default anonymous volumes also get orphaned (and eventually pruned) independently of the container — either problem can silently wipe the topic and all consumer offsets on a routine `docker compose up` after a container replacement.
Breaks if: the named volume is removed with `docker compose down -v` — that's a deliberate, visible reset, not an accidental one, which is the point.

## Exchange: Coinbase, `trade_id` (not `sequence`) as `exchangeSeq`

Chose: Coinbase's public Exchange WebSocket feed (`ws-feed.exchange.coinbase.com`), `matches` channel, using `trade_id` for the Kafka message's `exchangeSeq`.
Over: Kraken's public WebSocket v2 (`wss://ws.kraken.com/v2`); using Coinbase's `sequence` field for `exchangeSeq`.
Because: both Coinbase and Kraken confirmed no-auth for public trade/ticker channels — Coinbase docs: *"Coinbase Market Data is our traditional feed which is available without authentication"*; Kraken docs: *"public feeds (ticker, order book (L2), and trade streams) are available without a Kraken account."* Coinbase was chosen for the `matches` channel's `trade_id`, a per-`product_id` transaction identifier. `sequence`, by contrast, is shared across all message types Coinbase sends on a connection (heartbeats, `l2update`, `matches`, etc.), so it advances on every message type, not just trades — using it for gap detection on the `matches` channel alone would report a "gap" on every non-match message, firing constantly and telling us nothing real.
Breaks if: Coinbase's own docs are explicit that *messages can be dropped from the matches channel* and recommend cross-checking against the `heartbeat` channel's `last_trade_id` (or REST) to detect misses — `trade_id` gap-logging from the `matches` channel alone can under-detect drops the channel itself doesn't deliver. Since backfill is out of scope, this is acceptable (log what the gap in `trade_id` reveals, move on) but worth revisiting at Stage 2: subscribing to `heartbeat` alongside `matches` is a cheap, independent second source for the same check.

## Node Kafka client: `@confluentinc/kafka-javascript`, not KafkaJS

Chose: `@confluentinc/kafka-javascript` (Confluent's GA client, built on librdkafka).
Over: KafkaJS.
Because: KafkaJS has had no release since February 2023, and Kafka 4.0 removed several old client protocol API versions (KIP-896: Produce V0–V2, Fetch V0–V3, ListOffset V0, OffsetCommit V0–V1 — the last explicitly called out as affecting KafkaJS). This isn't theoretical: [tulios/kafkajs#1752](https://github.com/tulios/kafkajs/issues/1752) reports KafkaJS consumers failing to join a consumer group against Kafka 4.0.0 in KRaft mode with "the group coordinator is not available," with no fix available since the project is unmaintained. `@confluentinc/kafka-javascript` is the actively maintained, vendor-supported replacement CLAUDE.md flagged for evaluation.
Breaks if: Confluent's client has a materially different config surface than KafkaJS (it does — it's not a drop-in), so ingester code should target this client's API directly rather than write against a KafkaJS-shaped abstraction "just in case."

## Kafka healthcheck: `/opt/kafka/bin/kafka-broker-api-versions.sh`, not curl

Chose: `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092` (full path) as the Compose healthcheck command for the `kafka` service.
Over: a `curl`-based HTTP check; the bare script name without its path.
Because: `apache/kafka:4.3.1` is not built on an image that includes `curl` or `nc`. It does ship the full Kafka distribution's scripts, but under `/opt/kafka/bin/`, which is **not** on `PATH` (`PATH` inside the image is `/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin` — checked directly with `docker run --entrypoint sh apache/kafka:4.3.1 -c 'which kafka-broker-api-versions.sh'`, which fails). The script itself does a real protocol-level round trip (an ApiVersionsRequest) rather than a bare TCP connect, so a healthy result actually means the broker is answering Kafka's protocol, not just that the port is open.
Breaks if: the script's location or output format changes across major versions — worth re-checking the path on any future Kafka version bump. Verified: `docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092` exits 0 and prints the broker's supported API version ranges against the running compose stack; `docker compose up` independently reports the `kafka` container reaching `healthy` using this same command.

## Internal topic replication factor: 1, set explicitly

Chose: `offsets.topic.replication.factor=1`, `transaction.state.log.replication.factor=1`, `transaction.state.log.min.isr=1` on the broker.
Over: leaving these at their broker defaults (3, 3, 2 respectively).
Because: Kafka's internal topics (`__consumer_offsets`, and `__transaction_state` if a transactional producer is ever used) are created with these replication factors the first time they're needed — for `__consumer_offsets`, that's the first consumer group offset commit, i.e. Stage 3. With one broker, a default of 3 makes that internal topic creation fail outright (not enough brokers to satisfy the replica count), which would surface as a confusing consumer-side error in Stage 3 with nothing wrong in the code. This follows directly from the project's own single-broker/RF=1 decision (stated in CLAUDE.md's stack section) — it just applies it to Kafka's internal topics too, not only `ticks`.
Breaks if: a second broker is ever added — these internal topics would still be stuck at RF=1 unless explicitly reconfigured with `kafka-reassign-partitions` or similar; out of scope per "multi-broker Kafka" being excluded from this project.

## Compose file structure: no ports in the base file

Chose: `compose.yaml` (base, shared by dev and prod, no `ports:` published anywhere) + `compose.override.yaml` (dev-only, loaded automatically by `docker compose up` with no `-f` flags, adds host port publishing) + `compose.prod.yaml` (prod override, applied explicitly as `docker compose -f compose.yaml -f compose.prod.yaml up`).
Over: publishing dev ports directly in `compose.yaml` and having `compose.prod.yaml` attempt to remove them.
Because: Compose merges `ports:` lists across files — it has no mechanism for one file to *remove* a port another file published. If dev ports lived in the base file, `compose.prod.yaml` could add to them but never strip them, making it structurally impossible to keep Kafka/Redis off the public internet in prod (invariant #10) while using the same base file. Putting dev's ports in the auto-loaded override instead means the base file is inherently port-free, and prod (which never loads the override) simply never has ports to strip in the first place. Dev ports are further bound to `127.0.0.1` specifically (`127.0.0.1:29092:29092`, `127.0.0.1:6379:6379`), not just published — so even on the dev machine nothing on the LAN can reach them.
Breaks if: someone runs `docker compose -f compose.yaml -f compose.override.yaml -f compose.prod.yaml up` by hand — mixing all three would republish dev's ports on top of prod. Prod must be brought up with exactly `-f compose.yaml -f compose.prod.yaml`, never including the override.

## Redis image and data volume

Chose: `redis:8.2.9-alpine`, pinned to the exact patch version, with a named volume (`redis-data`) mounted at `/data` (the image's default `dir`).
Over: an unpinned or minor-pinned tag (`redis:8.2-alpine`, `redis:alpine`); an anonymous volume.
Because: `redis:8.2-alpine` and `redis:8.2.9-alpine` currently resolve to the identical image (same digest, confirmed by pulling both), but only the patch-pinned tag stays reproducible if `8.2.10` etc. is released later — consistent with the same pin-everything reasoning applied to Kafka. A named volume is needed for the same reason as Kafka's: an anonymous volume can be orphaned by container recreation, silently losing the AOF file and defeating the whole point of `--appendonly yes`.
Breaks if: nothing specific; re-pin when intentionally upgrading Redis. Verified: `docker run --rm redis:8.2.9-alpine redis-server --version` reports `v=8.2.9`; `redis-cli CONFIG GET appendonly` returns `yes` and `/data/appendonlydir` exists after `docker compose up`.

## Ingester Node.js version: `engines` range `>=24 <25`, exact `24.21.0` pinned via `.nvmrc`

Chose: `package.json` declares `"engines": { "node": ">=24 <25" }` (accepts any Node 24.x, rejects 23 or 26); `.nvmrc` pins the exact patch, `24.21.0`, for local version managers (`fnm`/`nvm`) to install and switch to.
Over: pinning the exact `24.21.0` in `engines` too; pinning to `26.8.2`, the version actually installed on this machine before this decision (`node --version` → `v26.8.2`, via Homebrew).
Because: CLAUDE.md specifies "current Node.js LTS" for the ingester — as of today Node 24 is Active LTS and Node 26 is still Current (becomes LTS 2026-10-28), so 24.x is the correct line. `engines` is an npm-enforced compatibility check across whoever/whatever runs this package (CI later, a teammate, a future you) — an exact-version `engines` pin is needlessly strict for that purpose and would reject a routine 24.21.1 patch release for no reason; the range expresses "must be on the LTS-24 line" without pinning a moment in time. `.nvmrc`, by contrast, exists specifically to pin an exact, reproducible version for local dev tooling to install — that's where the exact patch belongs.
Breaks if: development on this machine runs on the globally-installed 26.8.2 day to day while these files declare 24.x — installing 24.21.0 via `fnm` and confirming `node --version` before `npm install` avoids `@confluentinc/kafka-javascript`'s native module build happening against the wrong Node ABI. This isn't hypothetical: see the `engine-strict` decision below.

## `ingester/.npmrc`: `engine-strict=true`

Chose: `engine-strict=true` in `ingester/.npmrc`, making the `engines` range in `package.json` a hard install-time gate instead of an advisory.
Over: leaving `engines` as documentation only (npm's default behavior — it warns, but installs anyway).
Because: this already happened once. Writing this code, the shell running `npm install` didn't have `fnm`'s environment sourced (a fresh non-interactive shell that predated adding `fnm` to `.zshrc`), so `which node` silently resolved to Homebrew's `v26.8.2` instead of the pinned `24.21.0` — `npm install` proceeded without complaint and compiled `@confluentinc/kafka-javascript`'s native addon against the wrong Node ABI. It was caught only by checking `node --version` immediately before the install and noticing the mismatch, then killing the running install and rebuilding from a clean `node_modules`. `engine-strict` turns that class of mistake into a loud `EBADENGINE` failure at `npm install` time instead of a silently-wrong native build discovered later (or not discovered at all, if it happened to work) — confirmed by re-running `npm install` under Homebrew's `v26.8.2`: it now refuses immediately (`Required: {"node":">=24 <25"} / Actual: {"node":"v26.8.2"}`) instead of installing.
Breaks if: someone needs to install with a Node version outside the declared range for a one-off reason — `engine-strict` has no per-command override short of editing `.npmrc` or passing `--engine-strict=false`, which is the point: it should be inconvenient to bypass, not silently skipped.

## Ingester language: TypeScript, not plain JavaScript — plus a hand-written runtime guard for WebSocket data

Chose: TypeScript, run directly in dev via `tsx` (no separate compile step during development). **Correction to an earlier version of this entry:** TypeScript only checks code that flows through statically-typed values — it catches us mismapping our own fields when *constructing* the outgoing Kafka message (e.g. writing `exchangeTs` as a string, or transposing two fields), because that code is ours and fully typed. It does **not** validate the data actually arriving over the Coinbase WebSocket: `JSON.parse()` of a network message returns `any`/`unknown`, and nothing stops us from asserting a type onto it that the real payload doesn't match — Coinbase sending a malformed or unexpected message wouldn't be caught by the type system at all, it would just be silently mis-cast.
Over: plain JavaScript (no build-time checking on our own code either); reaching for a validation library (`zod`, `ajv`, etc.) for the runtime side.
Because: TypeScript still earns its place for the code we write (the message-construction path, the config loader) — schema v1 is a contract other services depend on, and a typo'd key or wrong-typed field in *our own* code is worth catching at edit time. But the WebSocket boundary needs an explicit, hand-written runtime check, not type annotations: a small type guard function (`isMatchMessage(msg: unknown): msg is CoinbaseMatchMessage`) that checks `typeof price === 'string'`, `typeof size === 'string'`, `typeof trade_id === 'number'`, `!Number.isNaN(Date.parse(time))`, and `configuredSymbols.includes(product_id)`. Anything that fails is logged and dropped, not produced to Kafka. A validation library is unnecessary weight for one message shape with five fields to check — hand-written conditionals are shorter than the equivalent schema definition and just as clear.
Breaks if: the guard's checks and the `CoinbaseMatchMessage` type ever drift apart (someone changes the type without updating the guard, or vice versa) — the guard is the actual boundary enforcement, the type is documentation for code past that boundary. `tsx` remains a dev-time convenience; whether the eventual Dockerfile runs compiled `dist/*.js` or `tsx` directly against `src/` is a Stage-9-adjacent decision, not one to make now.

## Ingester dev run mode: host via `npm`, not containerized yet

Chose: run the ingester directly on the host during Stages 1–2 (`npm run dev`, connecting to Kafka at `127.0.0.1:29092`, the `EXTERNAL` listener), no Dockerfile yet.
Over: containerizing the ingester now and running it against `kafka:9092` (the `INTERNAL` listener) from the start, for parity with how it'll run in prod.
Because: `@confluentinc/kafka-javascript` is a native module — containerizing now means every code change costs an image rebuild (or a bind-mount + reinstall-in-container dance) for zero benefit while the code is still this small and single-instance. The `EXTERNAL` listener exists specifically for this case (see the listener topology decision above). Containerizing is cheap to add later and doesn't need deciding now — consistent with implementing one component at a time rather than scaffolding ahead of the current stage.
Breaks if: this is revisited once the ingester needs to run unattended (Stage 9/10) or needs to join `compose.yaml` so `docker compose up` brings up the whole pipeline in one command — at that point it gets a Dockerfile and switches to `kafka:9092`.

## Stage 1 tick message: full schema v1 now, `trade_id` mapped to `exchangeSeq`

Chose: produce the full schema v1 shape from Stage 1 (`{ v: 1, symbol, price, size, exchangeTs, ingestTs, exchangeSeq }`), not a stripped-down interim shape.
Over: a minimal ad hoc shape for Stage 1 (e.g. just `{ symbol, price }`) and expanding to full schema v1 in Stage 2.
Because: schema v1's fields are already fully specified in this file's data contracts, and the `exchangeSeq` field's source (`trade_id`, not `sequence`) is already decided. There's nothing left to design at Stage 2 that would change the shape — Stage 2 adds *behavior* (idempotent producer config, reconnect, gap logging using the `exchangeSeq` field this stage already writes), not new fields. Writing the real shape now avoids a schema migration for a field set that's already settled.
Field mapping from Coinbase's `match` message (verified shape: `{type, trade_id, maker_order_id, taker_order_id, side, size, price, product_id, sequence, time}`, `trade_id`/`sequence` as JSON numbers, `price`/`size` as strings, `time` as an ISO 8601 string):
- `symbol` ← `product_id` (already in our `SYMBOLS` format, e.g. `"BTC-USD"` — no translation needed)
- `price` ← `price` (already a string — pass through, no parsing)
- `size` ← `size` (already a string — pass through)
- `exchangeTs` ← `Date.parse(time)` (ISO 8601 → epoch ms; Coinbase's microsecond precision is truncated to ms, which is what our field is)
- `ingestTs` ← `Date.now()` at the point the ingester constructs the record
- `exchangeSeq` ← `trade_id` (per the earlier exchange decision — not `sequence`)
Breaks if: Coinbase ever changes `matches` channel field names or types without a version bump on their side (nothing to do differently now — this is the same risk any integration with an external feed carries, not something Stage 1 needs to guard against).

## WebSocket client: `ws`, not Node's built-in global `WebSocket`

Chose: the `ws` npm package.
Over: Node's built-in global `WebSocket` (available since Node 21, backed by `undici`).
Because: the built-in `WebSocket` deliberately implements the WHATWG browser spec, which never exposes control frames to application code — no `.ping()` method, no `'ping'`/`'pong'` events; the browser (and Node's spec-compliant implementation of it) handles those transparently below the API surface script can reach. `ws` is a Node-native implementation that exposes ping/pong directly (`socket.ping()`, `socket.on('pong', ...)`). Stage 2's stale-connection detection is likely to want exactly that: sending an application-level ping on an interval and treating a missing pong as the "no message for N seconds" signal, rather than only inferring staleness from the absence of data messages (which `matches` won't send at all if a symbol is simply quiet — not the same thing as a dead connection).
Breaks if: nothing yet — this is a Stage 1 dependency choice made now because it's the kind of thing that's annoying to swap later, not because Stage 1 itself uses ping/pong (it doesn't; that's Stage 2).

## Stage 2 note (not solved now): `ws` ping/pong and Coinbase's `heartbeat` channel are complementary, not redundant

Chose: not deciding the stale-connection design yet — recording that Stage 2 likely wants both mechanisms, for different failure modes.
Because: `ws`'s ping/pong proves the TCP/TLS socket itself is alive — the transport is up and Coinbase's server is responding to frames at all. It says nothing about whether the *feed* is alive per product: a socket can be perfectly healthy while a specific product's `matches` channel goes quiet because Coinbase stopped sending for that product_id (the exact "bad `product_id`" silent-failure shape already handled by logging `error`/`subscriptions` — see above — but also relevant to an established connection). Coinbase's `heartbeat` channel, subscribed per product, proves the feed is alive per product and — already noted above — carries `last_trade_id`, which Stage 2 also wants as the gap-detection baseline. So the two mechanisms answer different questions: "is the socket up" (ping/pong) vs. "is this product's data still flowing, and what's the last trade_id I should expect continuity from" (heartbeat).
Breaks if: this is not designed yet — deferred to Stage 2 by explicit instruction. Recorded now so Stage 2 doesn't have to rediscover that one mechanism doesn't substitute for the other.
## Coinbase message types: handle `match`, `subscriptions`, and `error` explicitly; skip `last_match` deliberately

Chose: an explicit switch on the WebSocket message's `type` field — produce to Kafka only for `type === "match"`; log the `subscriptions` confirmation at info level once, right after connecting; log `error` messages loudly (not swallowed); explicitly recognize and skip `last_match` (Coinbase sends one per product immediately after subscribing, containing the most recent trade from before the subscription); log a warning and drop anything else unrecognized.
Over: only handling `match` and ignoring everything else silently.
Because: a bad `product_id` (typo in `SYMBOLS`, or a delisted/renamed pair) doesn't error the WebSocket connection — Coinbase just never sends `match` messages for it, which looks identical to "market is quiet" from the ingester's point of view. Without logging `error` messages loudly and confirming `subscriptions`, that failure mode is silent: the vertical slice "works" (connects, stays open) while producing nothing, and the "Done when" `kcat` check would just show an empty topic with no clue why.
Breaks if: nothing now — `last_match` is skipped, not produced, but it's not thrown away conceptually: its `trade_id` is the natural baseline for Stage 2's gap detection (the first real `match` after subscribing should be compared against `last_match`'s `trade_id`, not treated as if there's no prior state at all). Recorded here so Stage 2 doesn't have to rediscover this message exists.

## Coinbase subscription scope: the full configured `SYMBOLS` list, not a first-symbol special case

Chose: `config.ts` reads all of `SYMBOLS` and subscribes to the full list in one `product_ids` array.
Over: subscribing only to the first configured symbol as a Stage-1-specific simplification.
Because: `product_ids` already accepts an array — there's no per-symbol subscription call to fan out, so handling the full list costs nothing extra in code. Special-casing "just the first one" would be an artificial restriction that adds a code path (and a TODO) purely to look more like a "vertical slice," when the real vertical-slice scope reduction is behavioral (no reconnect, no idempotence config, no gap logging — Stage 2's job), not about how many symbols are subscribed. Dev's `.env` sets `SYMBOLS=BTC-USD` for Stage 1, so in practice exactly one symbol flows regardless — but the code path handles N from the start, consistent with invariant #8.
Breaks if: nothing — this removes a special case rather than adding one.

## Environment variables: Node's built-in `--env-file`, not `dotenv`

Chose: `node --env-file=.env` (via the `dev` npm script), no `dotenv` dependency.
Over: `dotenv`.
Because: `--env-file` has been stable in Node since well before 24 (confirmed present in `node --help` on this machine), does the same job (load `KEY=value` pairs from a file into `process.env` before the script runs) as `dotenv`'s core use case, and needs zero dependencies for it. `dotenv` earns its keep when something needs `.env`-loading as a library call (e.g. loading it conditionally, or from a non-standard path at runtime) — Stage 1 doesn't need that, just "load `.env` before the script starts."
Breaks if: a later stage needs `.env` loading behavior `--env-file` doesn't offer (e.g. variable expansion/interpolation between `.env` entries) — `dotenv` supports that, `--env-file` doesn't. Not needed yet.

## Ingester base image: `node:<lts>-slim`, not Alpine

Chose: `node:<lts>-slim` (Debian-based) as the eventual ingester container's base image.
Over: `node:<lts>-alpine`.
Because: `@confluentinc/kafka-javascript` is a native addon built on librdkafka (prebuilt binaries per platform, or a node-gyp/CMake build as a fallback). Alpine's musl libc is a common source of native-module breakage — either missing prebuilt binaries for the musl target (forcing a from-source build that then needs build tooling installed in the image) or subtle runtime issues that don't show up on glibc. `slim` keeps the image reasonably small while staying on glibc, avoiding that whole class of problem.
Breaks if: not verified yet — this is a Stage 1 note, decided now (while other base-image-adjacent decisions are being made) so it doesn't surprise us mid-stage; confirm `@confluentinc/kafka-javascript` actually installs and runs cleanly on `node:<lts>-slim` before writing the ingester Dockerfile.

## Open question for Stage 3 (not solved now): simultaneous matches from one taker order all get Δt=0

Chose: not addressing this now — recorded so Stage 3's EMA implementation doesn't discover it mid-stage.
Over: designing around it in the ingester or the message schema today (e.g. synthesizing a sub-timestamp tiebreaker, or coalescing same-timestamp matches).
Because: a single taker order that matches against several resting orders produces multiple `match` messages sharing the exact same `time` value (each maker fill is a separate trade with its own `trade_id`, but Coinbase timestamps the whole taker order's fills identically). Stage 3's EMA update takes `Δt` from `exchangeTs`, and `alpha = 1 - exp(-Δt/τ)`. When `Δt = 0`, `alpha = 0`, so the EMA update is a no-op — the second, third, etc. match at an identical timestamp changes `lastOffset` and gets stored, but has zero effect on the EMA value, even though each is a real, distinct trade at (possibly) a different price. The first match in the group is the only one that actually moves the EMA.
Breaks if: this becomes visible as "the EMA doesn't seem to react to some trades" during Stage 3 verification, especially during volatile bursts where large taker orders sweeping the book are common — worth deciding then whether that's acceptable (arguably it's a defensible reading of "time-decayed" — no time passed, so no decay applies) or whether same-timestamp matches should be coalesced (e.g. volume-weighted average price across the batch) before feeding the EMA. Not a Stage 1/2 concern.
