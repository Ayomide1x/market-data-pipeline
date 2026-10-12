import { useCallback, useEffect, useState } from 'react';
import { ApiError, createAlert, deleteAlert, listAlertsForSymbol } from './api';
import { addStoredAlert, loadStoredAlerts, pruneStoredAlerts, removeStoredAlert } from './storage';
import type { AlertRow, AlertRowStatus, StoredAlert } from './types';

// Catches TTL expiry, which pub/sub never announces (only a fire gets published) — an
// alert-fired SSE event updates status immediately via markFired; this periodic check
// is only for the "quietly expired, nobody told us" case. See DECISIONS.md.
const REFRESH_INTERVAL_MS = 30_000;

export function useMyAlerts() {
  const [stored, setStored] = useState<StoredAlert[]>(() => loadStoredAlerts());
  const [statusById, setStatusById] = useState<Record<string, AlertRowStatus>>({});
  const [createError, setCreateError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    const current = loadStoredAlerts();
    if (current.length === 0) return;

    const bySymbol = new Map<string, StoredAlert[]>();
    for (const a of current) {
      const list = bySymbol.get(a.symbol) ?? [];
      list.push(a);
      bySymbol.set(a.symbol, list);
    }

    const nextStatus: Record<string, AlertRowStatus> = {};
    const staleIds = new Set<string>();

    for (const [symbol, alertsForSymbol] of bySymbol) {
      try {
        const live = await listAlertsForSymbol(symbol);
        const liveById = new Map(live.map((a) => [a.id, a]));
        for (const a of alertsForSymbol) {
          const match = liveById.get(a.id);
          if (match) {
            nextStatus[a.id] = match.status;
          } else {
            // Absent from a successful list response — gone (TTL expired, or deleted
            // from elsewhere), not a 404: the list endpoint doesn't 404, it just omits
            // ids that no longer exist. See DECISIONS.md.
            staleIds.add(a.id);
          }
        }
      } catch {
        // Couldn't reach the server for this symbol's check. "unknown" is deliberately
        // distinct from "doesn't exist" — keep showing the cached row, don't prune it
        // on a guess. See DECISIONS.md.
        for (const a of alertsForSymbol) {
          nextStatus[a.id] = 'unknown';
        }
      }
    }

    if (staleIds.size > 0) {
      setStored(pruneStoredAlerts(staleIds));
    }
    setStatusById((prev) => ({ ...prev, ...nextStatus }));
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [refresh]);

  const create = useCallback(async (symbol: string, direction: 'above' | 'below', threshold: string) => {
    setCreateError(null);
    try {
      const created = await createAlert(symbol, direction, threshold);
      setStored(addStoredAlert(created));
      setStatusById((prev) => ({ ...prev, [created.id]: created.status }));
    } catch (e) {
      setCreateError(e instanceof ApiError ? e.message : 'could not reach the server');
    }
  }, []);

  const remove = useCallback(async (id: string) => {
    try {
      await deleteAlert(id);
    } catch {
      // Couldn't confirm the delete server-side, but there's nothing more useful to do
      // than stop tracking it locally — the user asked to remove it from their view.
    }
    setStored(removeStoredAlert(id));
  }, []);

  // Called from the SSE alert-fired handler — only acts if this id is actually one of
  // ours (the stream is global, every connected client sees every fire).
  const markFired = useCallback(
    (id: string) => {
      if (!stored.some((a) => a.id === id)) return;
      setStatusById((prev) => ({ ...prev, [id]: 'fired' }));
    },
    [stored],
  );

  const rows: AlertRow[] = stored.map((a) => ({ ...a, status: statusById[a.id] ?? 'unknown' }));

  return { rows, createError, create, remove, markFired };
}
