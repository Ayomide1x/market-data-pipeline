/**
 * Price precision varies per symbol and isn't fixed up front — BTC trades report two
 * decimals, SOL sometimes one, depending on what the exchange actually sent for that
 * specific trade (the api passes the price string straight through, unmodified, all
 * the way from Coinbase). Rendering the raw string directly means the column jitters
 * as trailing digits appear and disappear, and different symbols show different
 * precision side by side, which reads as broken rather than "that's just the data."
 *
 * The fix tracks, per symbol, the most decimal places seen in any price so far, and
 * pads every displayed value to at least that width — derived from observed data, not
 * a hardcoded per-symbol table, so a symbol's display precision is whatever its own
 * real ticks have actually shown, including if that later turns out to need more
 * places than the first few ticks showed. See DECISIONS.md.
 */
export function decimalPlaces(priceStr: string): number {
  const dot = priceStr.indexOf('.');
  return dot === -1 ? 0 : priceStr.length - dot - 1;
}

export function formatPrice(priceStr: string, decimals: number): string {
  const value = Number(priceStr);
  if (Number.isNaN(value)) return priceStr;
  return value.toFixed(decimals);
}
