// Vite only exposes env vars prefixed VITE_ to client code — anything else silently
// resolves to undefined at build time, not a runtime error. Both read independently of
// each other and of the other three services' own SYMBOLS reads (ingester/consumer/api)
// — a fourth instance of the already-accepted drift risk, not a new one. See DECISIONS.md.

export const API_BASE_URL: string = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080';

// Static, not sourced from the live stream: the alert form must stay usable while the
// api is down, which the stream-derived symbol set cannot guarantee. See DECISIONS.md.
export const CONFIGURED_SYMBOLS: string[] = (import.meta.env.VITE_SYMBOLS ?? 'BTC-USD')
  .split(',')
  .map((s: string) => s.trim())
  .filter((s: string) => s.length > 0);
