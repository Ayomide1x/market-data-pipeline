/**
 * Trades per minute, computed from the exchange's own per-product trade id
 * (`exchangeSeq`, Coinbase's trade_id), not by counting SSE updates.
 *
 * Counting updates would measure the api's delivery rather than the market: the api
 * merges every tick that arrives within one throttle interval (100ms) into a single
 * update per symbol, so a burst of 30 BTC trades can reach the browser as 3 updates.
 * Trade ids still advance by one per trade, so the difference between two updates'
 * exchangeSeq values is the number of trades between them, however many were merged.
 * Both ends of the measurement are exchange data (trade id and exchangeTs), so neither
 * this browser's clock nor processing delay changes the number. See DECISIONS.md.
 *
 * Known limit: the rate only changes when an update arrives. A symbol that stops
 * trading keeps showing its last rate. The Age column is what shows that it's gone quiet.
 */

export interface RateSample {
  exchangeTs: number;
  exchangeSeq: number;
}

/** Trailing window, in exchange time. Matches the EMA's 5-minute time constant. */
export const RATE_WINDOW_MS = 5 * 60 * 1000;

/**
 * Minimum time between the oldest and newest sample before a rate is shown. Below
 * this, two trades 50ms apart would show as 1,200 trades/min.
 */
export const MIN_RATE_SPAN_MS = 10 * 1000;

const SAMPLE_SPACING_MS = 1000;

/**
 * Appends a sample and drops those older than the window, keeping one sample at or
 * just past the window's edge so the measured span covers the whole window. Resets to
 * the new sample alone if the sequence or timestamp went backwards. That shouldn't
 * happen, since apply_tick.lua only publishes ticks it applied and its offset check
 * keeps those in order per symbol, but a negative difference can't be a trade count.
 */
export function pushRateSample(samples: RateSample[], next: RateSample): RateSample[] {
  const last = samples[samples.length - 1];
  if (last && (next.exchangeSeq < last.exchangeSeq || next.exchangeTs < last.exchangeTs)) {
    return [next];
  }
  // At most one kept sample per second: a newer sample within a second of the last one
  // replaces it instead of being appended. The rate only uses the window's two ends, so
  // nothing is lost, and BTC's window stays around 300 samples instead of ~3,000.
  const replaceLast =
    samples.length >= 2 && next.exchangeTs - last.exchangeTs < SAMPLE_SPACING_MS;
  const all = replaceLast ? [...samples.slice(0, -1), next] : [...samples, next];
  const cutoff = next.exchangeTs - RATE_WINDOW_MS;
  let firstKept = 0;
  while (firstKept + 1 < all.length && all[firstKept + 1].exchangeTs <= cutoff) {
    firstKept++;
  }
  return firstKept === 0 ? all : all.slice(firstKept);
}

/** Trades per minute across the samples, or null if they don't span enough time yet. */
export function tradesPerMinute(samples: RateSample[]): number | null {
  if (samples.length < 2) return null;
  const first = samples[0];
  const last = samples[samples.length - 1];
  const spanMs = last.exchangeTs - first.exchangeTs;
  if (spanMs < MIN_RATE_SPAN_MS) return null;
  return ((last.exchangeSeq - first.exchangeSeq) / spanMs) * 60_000;
}

export function formatRate(rate: number): string {
  return rate < 10 ? rate.toFixed(1) : Math.round(rate).toString();
}
