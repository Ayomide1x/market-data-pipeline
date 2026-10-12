import * as Sentry from '@sentry/react'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

// Undefined dsn no-ops cleanly, same as the ingester's @sentry/node (both JS SDKs;
// unlike the Java SDK, which throws on null — see consumer/api and DECISIONS.md). A
// real DSN is a Stage 10 deploy-time value, supplied via Vercel's env config, not
// committed here.
Sentry.init({ dsn: import.meta.env.VITE_SENTRY_DSN })

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
