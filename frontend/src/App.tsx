import { useCallback, useEffect, useState } from 'react';
import './App.css';
import { AlertForm } from './components/AlertForm';
import { ConnectionBadge } from './components/ConnectionBadge';
import { MyAlerts } from './components/MyAlerts';
import { SymbolTable } from './components/SymbolTable';
import { ToastStack } from './components/ToastStack';
import { useEventStream } from './useEventStream';
import { useMyAlerts } from './useMyAlerts';
import { useToasts } from './useToasts';
import type { AlertFired } from './types';

function App() {
  const { toasts, push: pushToast } = useToasts();
  const { rows: myAlerts, createError, create, remove, markFired } = useMyAlerts();

  const onAlertFired = useCallback(
    (fired: AlertFired) => {
      markFired(fired.id);
      // Filtered to "my alerts" only: the stream is global, every connected browser
      // sees every fire. Toasting for a stranger's threshold on a shared public demo
      // would be noise with nothing actionable in it. See DECISIONS.md.
      if (myAlerts.some((a) => a.id === fired.id)) {
        pushToast(`${fired.symbol} crossed ${fired.direction} ${fired.threshold}`);
      }
    },
    [markFired, myAlerts, pushToast],
  );

  const { rows, status } = useEventStream(onAlertFired);

  // A single shared clock for the whole table rather than N per-row timers — age only
  // needs 1s granularity for display, and every row derives it from this plus its own
  // exchangeTs, never from a stored ageMs value. See DECISIONS.md / age.ts.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);

  // "Loading" ends the moment the first row arrives, not when every configured symbol
  // has — each pops in individually as its own snapshot event lands. See DECISIONS.md.
  const loading = Object.keys(rows).length === 0;

  return (
    <div className="app">
      <header className="app__header">
        <h1>Market Data Pipeline</h1>
        <ConnectionBadge status={status} />
      </header>

      <section className="app__table">
        {loading ? (
          <p className="app__loading">
            {status === 'failed' ? 'Could not reach the server.' : 'Connecting to the server…'}
          </p>
        ) : (
          <SymbolTable rows={rows} now={now} />
        )}
      </section>

      <section className="app__alerts">
        <h2>Create an alert</h2>
        <AlertForm createError={createError} onCreate={create} />

        <h2>My alerts</h2>
        <MyAlerts rows={myAlerts} onDelete={remove} />
      </section>

      <ToastStack toasts={toasts} />
    </div>
  );
}

export default App;
