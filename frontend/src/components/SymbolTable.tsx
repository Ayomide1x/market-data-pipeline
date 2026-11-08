import { memo, useEffect, useRef, useState } from 'react';
import { computeAgeMs, formatAge } from '../age';
import { formatPrice } from '../decimals';
import { loadSort, nextSort, saveSort, sortSymbols, type SortKey, type SortState } from '../tableSort';
import { formatRate, tradesPerMinute } from '../tradeRate';
import { Sparkline } from './Sparkline';
import type { SymbolRow } from '../types';

type FlashClass = '' | 'flash-up' | 'flash-down';
const FLASH_DURATION_MS = 500;

const SymbolTableRow = memo(function SymbolTableRow({ row, now }: { row: SymbolRow; now: number }) {
  const prevPriceRef = useRef<string | null>(null);
  const [flash, setFlash] = useState<FlashClass>('');

  useEffect(() => {
    const prev = prevPriceRef.current;
    prevPriceRef.current = row.price;
    if (prev === null || row.price === null || prev === row.price) return;
    // Directional, not a neutral flash: the color itself carries real information
    // (which way the price moved), which a viewer actually wants — see DECISIONS.md.
    const direction: FlashClass = parseFloat(row.price) > parseFloat(prev) ? 'flash-up' : 'flash-down';
    setFlash(direction);
    const timeout = setTimeout(() => setFlash(''), FLASH_DURATION_MS);
    return () => clearTimeout(timeout);
  }, [row.price]);

  if (!row.hasData) {
    return (
      <tr>
        <td>{row.symbol}</td>
        <td colSpan={5} className="symbol-table__no-data">
          waiting for first trade
        </td>
      </tr>
    );
  }

  // Same decimals for EMA as price — EMA is a derived float with no "natural" string
  // precision of its own, so matching it to whatever this symbol's actual trades have
  // shown keeps the two columns visually consistent instead of EMA staying fixed at 2
  // while price's observed width grows or shrinks. See DECISIONS.md.
  const rate = tradesPerMinute(row.rateSamples);
  return (
    <tr>
      <td>{row.symbol}</td>
      <td className={flash}>{row.price !== null ? formatPrice(row.price, row.decimals) : '—'}</td>
      <td className="symbol-table__trend">
        <Sparkline history={row.history} />
      </td>
      <td>{row.ema !== null ? row.ema.toFixed(row.decimals) : '—'}</td>
      <td className="symbol-table__rate">{rate !== null ? formatRate(rate) : '—'}</td>
      <td className="symbol-table__age">{formatAge(computeAgeMs(row.exchangeTs, now))}</td>
    </tr>
  );
});

// A header that sorts the table. The click target is a <button> inside the <th>, so it
// works from the keyboard; aria-sort on the <th> tells screen readers the current order.
function SortHeader({
  label,
  sortKey,
  sort,
  onSort,
  title,
}: {
  label: string;
  sortKey: SortKey;
  sort: SortState;
  onSort: (key: SortKey) => void;
  title?: string;
}) {
  const active = sort.key === sortKey;
  const ariaSort = active ? (sort.direction === 'asc' ? 'ascending' : 'descending') : 'none';
  return (
    <th aria-sort={ariaSort} title={title}>
      <button type="button" className="symbol-table__sort" onClick={() => onSort(sortKey)}>
        {label}
        <span className="symbol-table__sort-icon" aria-hidden="true">
          {active ? (sort.direction === 'asc' ? '▲' : '▼') : ''}
        </span>
      </button>
    </th>
  );
}

export function SymbolTable({ rows, now }: { rows: Record<string, SymbolRow>; now: number }) {
  const [sort, setSort] = useState<SortState>(loadSort);
  const symbols = sortSymbols(rows, sort, now);
  const onSort = (key: SortKey) => {
    const next = nextSort(sort, key);
    setSort(next);
    saveSort(next);
  };

  return (
    <table className="symbol-table">
      <thead>
        <tr>
          <SortHeader label="Symbol" sortKey="symbol" sort={sort} onSort={onSort} />
          <SortHeader label="Price" sortKey="price" sort={sort} onSort={onSort} />
          <th title="Last 40 ticks received for this symbol — a tick count, not a fixed time window, since ticks aren't evenly spaced. Resets on every reconnect: a snapshot only ever gives one current price, never history.">
            Trend ⓘ
          </th>
          <SortHeader
            label="EMA ⓘ"
            sortKey="ema"
            sort={sort}
            onSort={onSort}
            title="5-minute decay-weighted average — starts at the first price and diverges over time. Nearly equal to price early on is expected, not broken."
          />
          <SortHeader
            label="Trades/min ⓘ"
            sortKey="rate"
            sort={sort}
            onSort={onSort}
            title="Trades on the exchange per minute over the last 5 minutes, counted from Coinbase trade ids, so merged updates don't undercount it. Shows — until 10 seconds of data have arrived. Keeps its last value if trading stops; check Age."
          />
          <SortHeader label="Age" sortKey="age" sort={sort} onSort={onSort} />
        </tr>
      </thead>
      <tbody>
        {symbols.map((symbol) => (
          <SymbolTableRow key={symbol} row={rows[symbol]} now={now} />
        ))}
      </tbody>
    </table>
  );
}
