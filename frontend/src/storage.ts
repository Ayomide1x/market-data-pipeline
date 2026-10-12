import type { StoredAlert } from './types';

// The only record, anywhere, of which alerts this browser created — the api has no
// account/session concept at all (GET /api/alerts?symbol= returns everyone's alerts for
// that symbol). Losing this doesn't affect the alert server-side in any way: it stays
// armed/evaluated exactly as before, it just can't be managed through this UI anymore
// and rides out its TTL. See DECISIONS.md.
const STORAGE_KEY = 'mdp:my-alerts';

export function loadStoredAlerts(): StoredAlert[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (a): a is StoredAlert =>
        a &&
        typeof a.id === 'string' &&
        typeof a.symbol === 'string' &&
        (a.direction === 'above' || a.direction === 'below') &&
        typeof a.threshold === 'string' &&
        typeof a.createdAt === 'number',
    );
  } catch {
    return [];
  }
}

function save(alerts: StoredAlert[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(alerts));
  } catch {
    // Storage disabled/full/private-mode — the alert was still created server-side;
    // this browser just won't remember it past this page load. Nothing to recover here.
  }
}

export function addStoredAlert(alert: StoredAlert): StoredAlert[] {
  const next = [...loadStoredAlerts(), alert];
  save(next);
  return next;
}

export function removeStoredAlert(id: string): StoredAlert[] {
  const next = loadStoredAlerts().filter((a) => a.id !== id);
  save(next);
  return next;
}

/** Drops every id in staleIds — used when a status refresh confirms they no longer exist. */
export function pruneStoredAlerts(staleIds: Set<string>): StoredAlert[] {
  const next = loadStoredAlerts().filter((a) => !staleIds.has(a.id));
  save(next);
  return next;
}
