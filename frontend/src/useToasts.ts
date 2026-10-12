import { useCallback, useRef, useState } from 'react';

export interface Toast {
  id: number;
  message: string;
}

const DISMISS_AFTER_MS = 6000;

// Hand-rolled, not a library — this is a well-understood, small pattern that doesn't
// earn an added dependency. See DECISIONS.md.
export function useToasts() {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(0);

  const push = useCallback((message: string) => {
    const id = nextId.current++;
    setToasts((prev) => [...prev, { id, message }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, DISMISS_AFTER_MS);
  }, []);

  return { toasts, push };
}
