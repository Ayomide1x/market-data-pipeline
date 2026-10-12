/**
 * ageMs — whether it arrives via the initial REST-shaped snapshot or is computed here —
 * is only ever correct at the instant it's computed. Storing a fetched ageMs in React
 * state and rendering it verbatim would mean it's wrong the moment a re-render doesn't
 * happen to coincide with a refetch — the exact same staleness argument that's why the
 * api never pushes ageMs over SSE at all (see DECISIONS.md). So this derives age fresh
 * on every render from exchangeTs and a locally ticking clock, never from a stored
 * ageMs value.
 */
export function computeAgeMs(exchangeTs: number | null, now: number): number | null {
  if (exchangeTs === null) return null;
  return Math.max(0, now - exchangeTs);
}

export function formatAge(ageMs: number | null): string {
  if (ageMs === null) return '—';
  const totalSeconds = Math.floor(ageMs / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s ago`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}m ${seconds.toString().padStart(2, '0')}s ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${(minutes % 60).toString().padStart(2, '0')}m ago`;
}
