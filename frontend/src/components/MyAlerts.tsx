import type { AlertRow } from '../types';

interface Props {
  rows: AlertRow[];
  onDelete: (id: string) => void;
}

const STATUS_LABEL: Record<AlertRow['status'], string> = {
  armed: 'armed',
  fired: 'fired',
  unknown: 'status unknown — couldn’t reach server',
};

export function MyAlerts({ rows, onDelete }: Props) {
  if (rows.length === 0) {
    return <p className="my-alerts__empty">No alerts created from this browser yet.</p>;
  }

  return (
    <ul className="my-alerts">
      {rows.map((a) => (
        <li key={a.id} className={`my-alerts__row my-alerts__row--${a.status}`}>
          <span>
            {a.symbol} {a.direction} {a.threshold}
          </span>
          <span className="my-alerts__status">{STATUS_LABEL[a.status]}</span>
          <button type="button" onClick={() => onDelete(a.id)}>
            Delete
          </button>
        </li>
      ))}
    </ul>
  );
}
