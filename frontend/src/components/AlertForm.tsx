import { useState } from 'react';
import { CONFIGURED_SYMBOLS } from '../config';

interface Props {
  createError: string | null;
  onCreate: (symbol: string, direction: 'above' | 'below', threshold: string) => void;
}

// Symbol choices come from CONFIGURED_SYMBOLS (a build-time env var), not from the
// live stream's known symbols — the form must stay usable while the api is down,
// which is exactly the scenario the stream can't help with. See DECISIONS.md.
export function AlertForm({ createError, onCreate }: Props) {
  const [symbol, setSymbol] = useState(CONFIGURED_SYMBOLS[0] ?? '');
  const [direction, setDirection] = useState<'above' | 'below'>('above');
  const [threshold, setThreshold] = useState('');

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!threshold.trim()) return;
    onCreate(symbol, direction, threshold.trim());
    setThreshold('');
  }

  return (
    <form className="alert-form" onSubmit={handleSubmit}>
      <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
        {CONFIGURED_SYMBOLS.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </select>
      <select value={direction} onChange={(e) => setDirection(e.target.value as 'above' | 'below')}>
        <option value="above">above</option>
        <option value="below">below</option>
      </select>
      <input
        type="number"
        inputMode="decimal"
        step="any"
        placeholder="threshold"
        value={threshold}
        onChange={(e) => setThreshold(e.target.value)}
        required
      />
      <button type="submit">Create alert</button>
      {createError && <span className="alert-form__error">{createError}</span>}
    </form>
  );
}
