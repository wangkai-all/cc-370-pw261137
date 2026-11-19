import { resolve } from 'node:path';

function int(name, fallback) {
  const v = process.env[name];
  if (v === undefined) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

export const config = {
  dbPath: process.env.METRICS_DB || resolve(process.cwd(), 'data', 'metrics.db'),
  ingestPort: int('INGEST_PORT', 4101),
  schedulerPort: int('SCHEDULER_PORT', 4102),
  queryPort: int('QUERY_PORT', 4103),
  webPort: int('WEB_PORT', 4100),
  schedulerPollMs: int('SCHEDULER_POLL_MS', 1500),
  // Builds are operator-driven from the console by default so a change can be
  // inspected, cancelled and resumed. Set SCHEDULER_AUTORUN=1 for unattended
  // operation.
  schedulerAutoRun: process.env.SCHEDULER_AUTORUN === '1',
  queryServiceUrl: process.env.QUERY_SERVICE_URL || `http://127.0.0.1:${int('QUERY_PORT', 4103)}`,
  ingestServiceUrl: process.env.INGEST_SERVICE_URL || `http://127.0.0.1:${int('INGEST_PORT', 4101)}`,
  schedulerServiceUrl: process.env.SCHEDULER_SERVICE_URL || `http://127.0.0.1:${int('SCHEDULER_PORT', 4102)}`,
};
