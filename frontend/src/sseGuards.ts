import type { AlertFired, SymbolSnapshot, TickUpdate } from './types';

// Hand-written runtime checks at the actual untrusted boundary — same reasoning as the
// ingester's isMatchMessage: JSON.parse returns unknown/any, and nothing stops asserting
// a type onto it that the real payload doesn't match. TypeScript only checks the code we
// write; it says nothing about what actually arrives over the wire. Anything that fails
// a guard is logged and dropped, not rendered. See DECISIONS.md.

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

export function isSymbolSnapshot(value: unknown): value is SymbolSnapshot {
  if (!isRecord(value)) return false;
  return (
    typeof value.symbol === 'string' &&
    typeof value.hasData === 'boolean' &&
    (value.price === null || typeof value.price === 'string') &&
    (value.ema === null || typeof value.ema === 'number') &&
    (value.exchangeTs === null || typeof value.exchangeTs === 'number') &&
    (value.ageMs === null || typeof value.ageMs === 'number')
  );
}

export function isTickUpdate(value: unknown): value is TickUpdate {
  if (!isRecord(value)) return false;
  return (
    typeof value.symbol === 'string' &&
    typeof value.price === 'string' &&
    typeof value.ema === 'number' &&
    typeof value.exchangeTs === 'number'
  );
}

export function isAlertFired(value: unknown): value is AlertFired {
  if (!isRecord(value)) return false;
  return (
    typeof value.id === 'string' &&
    typeof value.symbol === 'string' &&
    (value.direction === 'above' || value.direction === 'below') &&
    typeof value.threshold === 'string' &&
    typeof value.firedAtOffset === 'number'
  );
}

/** Parses and guards in one step; returns null (logged) rather than throwing. */
export function parseSseData<T>(raw: string, guard: (value: unknown) => value is T, eventName: string): T | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    console.error(`malformed SSE "${eventName}" payload (not valid JSON)`, raw, e);
    return null;
  }
  if (!guard(parsed)) {
    console.error(`malformed SSE "${eventName}" payload (failed shape check)`, parsed);
    return null;
  }
  return parsed;
}
