import type { ConnectionStatus } from '../types';

const LABELS: Record<ConnectionStatus, string> = {
  connecting: 'Connecting…',
  connected: 'Live',
  reconnecting: 'Reconnecting…',
  failed: 'Connection error',
};

export function ConnectionBadge({ status }: { status: ConnectionStatus }) {
  return (
    <div className={`connection-badge connection-badge--${status}`}>
      <span className="connection-badge__dot" />
      {LABELS[status]}
      {status === 'failed' && (
        <button type="button" className="connection-badge__reload" onClick={() => window.location.reload()}>
          Reload
        </button>
      )}
    </div>
  );
}
