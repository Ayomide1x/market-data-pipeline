import { API_BASE_URL } from './config';
import type { AlertView } from './types';

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function createAlert(symbol: string, direction: 'above' | 'below', threshold: string): Promise<AlertView> {
  const res = await fetch(`${API_BASE_URL}/api/alerts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol, direction, threshold }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiError(res.status, body.error ?? `request failed with ${res.status}`);
  }
  return res.json();
}

/** Resolves even on a 404 (already gone) — the caller treats that the same as "not found". */
export async function deleteAlert(id: string): Promise<{ found: boolean }> {
  const res = await fetch(`${API_BASE_URL}/api/alerts/${id}`, { method: 'DELETE' });
  if (res.status === 404) return { found: false };
  if (!res.ok) {
    throw new ApiError(res.status, `delete failed with ${res.status}`);
  }
  return { found: true };
}

/** Everyone's alerts for this symbol, not just this browser's — the caller filters. */
export async function listAlertsForSymbol(symbol: string): Promise<AlertView[]> {
  const res = await fetch(`${API_BASE_URL}/api/alerts?symbol=${encodeURIComponent(symbol)}`);
  if (!res.ok) {
    throw new ApiError(res.status, `list failed with ${res.status}`);
  }
  return res.json();
}
