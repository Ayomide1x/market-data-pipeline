import { computeAgeMs } from './age';
import { tradesPerMinute } from './tradeRate';
import type { SymbolRow } from './types';

/**
 * Click-to-sort for the symbol table. The order is recomputed on every render, so the
 * table re-sorts live as updates arrive. Sorting by price barely moves anything, while
 * sorting by Age or Trades/min will move rows around; that's what the viewer asked for
 * when they picked that column. Two rules keep it readable:
 * - Ties fall back to symbol A→Z, so equal values don't swap places on every update.
 * - Rows with no value yet ("—") always go last, in either direction, so an empty row
 *   never takes the top spot just because the sort is ascending.
 * See DECISIONS.md.
 */

export type SortKey = 'symbol' | 'price' | 'ema' | 'age' | 'rate';
export type SortDirection = 'asc' | 'desc';
export interface SortState {
  key: SortKey;
  direction: SortDirection;
}

export const DEFAULT_SORT: SortState = { key: 'symbol', direction: 'asc' };

/** First click on a new column: text and age read best ascending, numbers biggest-first. */
const FIRST_DIRECTION: Record<SortKey, SortDirection> = {
  symbol: 'asc',
  price: 'desc',
  ema: 'desc',
  age: 'asc',
  rate: 'desc',
};

export function nextSort(current: SortState, clicked: SortKey): SortState {
  if (current.key !== clicked) return { key: clicked, direction: FIRST_DIRECTION[clicked] };
  return { key: clicked, direction: current.direction === 'asc' ? 'desc' : 'asc' };
}

function sortValue(row: SymbolRow, key: Exclude<SortKey, 'symbol'>, now: number): number | null {
  if (!row.hasData) return null;
  switch (key) {
    case 'price':
      return row.price === null ? null : Number(row.price);
    case 'ema':
      return row.ema;
    case 'age':
      return computeAgeMs(row.exchangeTs, now);
    case 'rate':
      return tradesPerMinute(row.rateSamples);
  }
}

export function sortSymbols(rows: Record<string, SymbolRow>, sort: SortState, now: number): string[] {
  const sign = sort.direction === 'asc' ? 1 : -1;
  return Object.keys(rows).sort((a, b) => {
    if (sort.key === 'symbol') return sign * a.localeCompare(b);
    const va = sortValue(rows[a], sort.key, now);
    const vb = sortValue(rows[b], sort.key, now);
    if (va === null && vb === null) return a.localeCompare(b);
    if (va === null) return 1;
    if (vb === null) return -1;
    return va === vb ? a.localeCompare(b) : sign * (va - vb);
  });
}

// Remembered per browser only, as a convenience. Any storage failure (private window,
// blocked storage) just falls back to the default sort.
const STORAGE_KEY = 'mdp.tableSort';

export function loadSort(): SortState {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? 'null');
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'key' in parsed &&
      'direction' in parsed &&
      typeof parsed.key === 'string' &&
      parsed.key in FIRST_DIRECTION &&
      (parsed.direction === 'asc' || parsed.direction === 'desc')
    ) {
      return { key: parsed.key as SortKey, direction: parsed.direction };
    }
  } catch {
    // fall through to the default
  }
  return DEFAULT_SORT;
}

export function saveSort(sort: SortState): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(sort));
  } catch {
    // not remembered this time; nothing else depends on it
  }
}
