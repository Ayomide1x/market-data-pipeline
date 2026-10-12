import { memo, useEffect, useRef, useState } from 'react';
import { computeAgeMs, formatAge } from '../age';
import { formatPrice } from '../decimals';
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
        <td colSpan={4} className="symbol-table__no-data">
          waiting for first trade
        </td>
      </tr>
    );
  }

  // Same decimals for EMA as price — EMA is a derived float with no "natural" string
  // precision of its own, so matching it to whatever this symbol's actual trades have
  // shown keeps the two columns visually consistent instead of EMA staying fixed at 2
  // while price's observed width grows or shrinks. See DECISIONS.md.
  return (
    <tr>
      <td>{row.symbol}</td>
      <td className={flash}>{row.price !== null ? formatPrice(row.price, row.decimals) : '—'}</td>
      <td className="symbol-table__trend">
        <Sparkline history={row.history} />
      </td>
      <td>{row.ema !== null ? row.ema.toFixed(row.decimals) : '—'}</td>
      <td className="symbol-table__age">{formatAge(computeAgeMs(row.exchangeTs, now))}</td>
    </tr>
  );
});

export function SymbolTable({ rows, now }: { rows: Record<string, SymbolRow>; now: number }) {
  const symbols = Object.keys(rows).sort();

  return (
    <table className="symbol-table">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Price</th>
          <th title="Last 40 ticks received for this symbol — a tick count, not a fixed time window, since ticks aren't evenly spaced. Resets on every reconnect: a snapshot only ever gives one current price, never history.">
            Trend ⓘ
          </th>
          <th title="5-minute decay-weighted average — starts at the first price and diverges over time. Nearly equal to price early on is expected, not broken.">
            EMA ⓘ
          </th>
          <th>Age</th>
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
