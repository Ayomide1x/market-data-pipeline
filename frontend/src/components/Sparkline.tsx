const WIDTH = 80;
const HEIGHT = 24;
const PAD = 2; // keeps the stroke from clipping at the top/bottom edge

// Scaled to this symbol's OWN window min/max, not a shared scale across symbols — BTC
// and SOL differ by three orders of magnitude, so a shared scale would flatten
// everything but the highest-value symbol into an invisible line. See DECISIONS.md.
export function Sparkline({ history }: { history: number[] }) {
  // Fewer than 2 points can't draw a line segment at all — rendered as genuinely
  // empty, not a placeholder dash or a flat line, which would imply a signal ("no
  // movement yet") that doesn't exist. Showing less is better than showing something
  // invented. See DECISIONS.md.
  if (history.length < 2) {
    return <svg width={WIDTH} height={HEIGHT} className="sparkline" aria-hidden="true" />;
  }

  const min = Math.min(...history);
  const max = Math.max(...history);
  const range = max - min;

  const points = history
    .map((value, i) => {
      const x = (i / (history.length - 1)) * WIDTH;
      // range === 0: every point in the window is identical (a genuinely quiet
      // moment) — a flat line at vertical center, not a division by zero.
      const y = range === 0 ? HEIGHT / 2 : HEIGHT - PAD - ((value - min) / range) * (HEIGHT - 2 * PAD);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');

  // Reused directly from the window's own last two points — the same comparison the
  // row's flash animation already makes, just read off history instead of a separate
  // ref, so this doesn't duplicate state to stay in sync with. Cheap: one comparison.
  const last = history[history.length - 1];
  const prev = history[history.length - 2];
  const direction = last > prev ? 'up' : last < prev ? 'down' : 'flat';

  return (
    <svg width={WIDTH} height={HEIGHT} viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="sparkline" aria-hidden="true">
      <polyline points={points} className={`sparkline__line sparkline__line--${direction}`} />
    </svg>
  );
}
