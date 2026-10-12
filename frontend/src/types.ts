// Shapes mirrored from the api's own records (SymbolSnapshot, TickUpdate, AlertFired,
// AlertView) — see DECISIONS.md for why the table's rows and the alert form's symbol
// choices deliberately come from different sources despite both being "symbols."

export interface SymbolSnapshot {
  symbol: string;
  hasData: boolean;
  price: string | null;
  ema: number | null;
  exchangeTs: number | null;
  ageMs: number | null;
}

export interface TickUpdate {
  symbol: string;
  price: string;
  ema: number;
  exchangeTs: number;
}

export interface AlertFired {
  id: string;
  symbol: string;
  direction: 'above' | 'below';
  threshold: string;
  firedAtOffset: number;
}

export interface AlertView {
  id: string;
  symbol: string;
  direction: 'above' | 'below';
  threshold: string;
  status: 'armed' | 'fired';
  createdAt: number;
}

// What's actually persisted in localStorage for an alert this browser created — the
// full create response, not a bare id, so a cached row still renders something real if
// a later status refresh fails. See DECISIONS.md.
export interface StoredAlert {
  id: string;
  symbol: string;
  direction: 'above' | 'below';
  threshold: string;
  createdAt: number;
}

// A StoredAlert plus whatever the frontend currently knows about its live status.
// 'unknown' specifically means "couldn't reach the server to check," not "doesn't
// exist" — those are different and must render differently. See DECISIONS.md.
export type AlertRowStatus = 'armed' | 'fired' | 'unknown';

export interface AlertRow extends StoredAlert {
  status: AlertRowStatus;
}

// Four states, not two — see DECISIONS.md on why "disconnected" alone was wrong.
export type ConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'failed';

// In-memory row state, deliberately without ageMs — age is always derived live from
// exchangeTs + a ticking clock (see age.ts), never stored as a fetched/pushed value.
// decimals is the most decimal places this symbol has shown in any price seen so far —
// derived from the data, not hardcoded per symbol — so the displayed column stays at a
// stable width instead of jittering as trailing zeros come and go. See decimals.ts and
// DECISIONS.md.
export interface SymbolRow {
  symbol: string;
  hasData: boolean;
  price: string | null;
  ema: number | null;
  exchangeTs: number | null;
  decimals: number;
}
