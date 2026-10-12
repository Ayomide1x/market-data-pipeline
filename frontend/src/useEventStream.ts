import { useEffect, useRef, useState } from 'react';
import { API_BASE_URL } from './config';
import { isAlertFired, isSymbolSnapshot, isTickUpdate, parseSseData } from './sseGuards';
import type { AlertFired, ConnectionStatus, SymbolRow } from './types';

interface UseEventStreamResult {
  rows: Record<string, SymbolRow>;
  status: ConnectionStatus;
}

/**
 * Four connection states, not two: `connecting` (never successfully connected yet,
 * the browser is retrying on its own) is a materially different situation from
 * `reconnecting` (was connected, just dropped) — a visitor loading the page while the
 * api is down should see "connecting," not an unexplained "disconnected" badge next to
 * an empty table, which looks broken rather than starting up. `failed` (readyState
 * CLOSED) is different again: native EventSource only reaches CLOSED for a fatal cause
 * (a non-200/wrong-content-type response, or this code calling .close() itself) — for
 * an ordinary dropped connection it keeps retrying forever at CONNECTING, it does not
 * give up on its own. See DECISIONS.md.
 */
export function useEventStream(onAlertFired: (fired: AlertFired) => void): UseEventStreamResult {
  const [rows, setRows] = useState<Record<string, SymbolRow>>({});
  const [status, setStatus] = useState<ConnectionStatus>('connecting');
  const hasEverConnected = useRef(false);

  // Always-current callback without the setup effect depending on it — this component
  // mounts exactly one EventSource for its lifetime; re-running that effect every time
  // the caller's callback identity changes would mean tearing down and recreating the
  // connection for no reason. Assigned in its own effect, not during render, since a
  // ref's .current is a side effect, not a render output.
  const onAlertFiredRef = useRef(onAlertFired);
  useEffect(() => {
    onAlertFiredRef.current = onAlertFired;
  }, [onAlertFired]);

  useEffect(() => {
    const es = new EventSource(`${API_BASE_URL}/api/stream`);

    es.addEventListener('open', () => {
      hasEverConnected.current = true;
      setStatus('connected');
    });

    es.addEventListener('error', () => {
      if (es.readyState === EventSource.CLOSED) {
        setStatus('failed');
      } else {
        // readyState CONNECTING: the browser has already scheduled a retry on its own.
        setStatus(hasEverConnected.current ? 'reconnecting' : 'connecting');
      }
    });

    es.addEventListener('snapshot', (e: MessageEvent<string>) => {
      const snap = parseSseData(e.data, isSymbolSnapshot, 'snapshot');
      if (!snap) return;
      setRows((prev) => ({
        ...prev,
        [snap.symbol]: {
          symbol: snap.symbol,
          hasData: snap.hasData,
          price: snap.price,
          ema: snap.ema,
          exchangeTs: snap.exchangeTs,
        },
      }));
    });

    es.addEventListener('tick', (e: MessageEvent<string>) => {
      const tick = parseSseData(e.data, isTickUpdate, 'tick');
      if (!tick) return;
      setRows((prev) => {
        // Stage 7's rule, enforced here: a tick for a symbol this connection's snapshot
        // never listed is ignored, not turned into a new row. See DECISIONS.md.
        if (!(tick.symbol in prev)) return prev;
        return {
          ...prev,
          [tick.symbol]: { symbol: tick.symbol, hasData: true, price: tick.price, ema: tick.ema, exchangeTs: tick.exchangeTs },
        };
      });
    });

    es.addEventListener('alert-fired', (e: MessageEvent<string>) => {
      const fired = parseSseData(e.data, isAlertFired, 'alert-fired');
      if (!fired) return;
      onAlertFiredRef.current(fired);
    });

    return () => {
      es.close();
    };
  }, []);

  return { rows, status };
}
