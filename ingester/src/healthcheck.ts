import { statSync } from "fs";

// Run by Docker's HEALTHCHECK, not imported by the app itself. Checks the liveness
// file's own age, not "is the Node process running" — checking that from inside a
// container whose only process IS Node tells Docker nothing it doesn't already know
// (if Node exits, the container exits; no HEALTHCHECK needed to discover that). This
// checks the thing that can actually go wrong while the process stays alive: the
// connection going silently stuck despite the event loop still running. See index.ts
// (touchLiveness, wired to the same signal the internal stale-connection detector
// uses) and DECISIONS.md.
const LIVENESS_FILE = process.env.LIVENESS_FILE ?? "/tmp/ingester-liveness";
// Deliberately larger than index.ts's own HEARTBEAT_STALE_MS (10s): give the internal
// reconnect logic a real chance to notice and self-heal before Docker intervenes.
const LIVENESS_STALE_MS = Number(process.env.LIVENESS_STALE_MS ?? 30_000);

try {
  const stat = statSync(LIVENESS_FILE);
  const age = Date.now() - stat.mtimeMs;
  if (age > LIVENESS_STALE_MS) {
    console.error(`liveness file is ${age}ms old (stale after ${LIVENESS_STALE_MS}ms)`);
    process.exit(1);
  }
  process.exit(0);
} catch (err) {
  // Missing file: either this is a fresh start that hasn't connected yet (expected
  // briefly, which is why HEALTHCHECK has its own start-period), or the write itself
  // failed — either way, not something to call healthy.
  console.error(`liveness file not found: ${String(err)}`);
  process.exit(1);
}
