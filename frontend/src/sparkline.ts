// SPARKLINE_WINDOW is a *tick count*, not a time window — the server coalesces to one
// update per symbol per flush (100ms, Stage 7), but ticks still aren't evenly spaced in
// wall-clock time (a quiet symbol's window spans longer than a busy one's). 40 is sized
// against the sparkline's own rendered width (~80px in this layout): past roughly one
// point per 2px, more points stop being visually distinguishable at all, so a larger
// window would just be unused state, not a better picture. It's also few enough that a
// burst of ticks reads as a real, visible trend rather than dense noise filling the row.
export const SPARKLINE_WINDOW = 40;

export function pushHistory(history: number[], price: number): number[] {
  const next = [...history, price];
  return next.length > SPARKLINE_WINDOW ? next.slice(next.length - SPARKLINE_WINDOW) : next;
}
