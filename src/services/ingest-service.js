// Ingest service (port 4101). Accepts events and corrections, writes only to
// the append-only input tables and the change queue. It never computes metrics.
import { openDb, initSchema } from '../lib/db.js';
import { createService } from '../lib/http.js';
import { ingestEvents, submitCorrection, submitReassignment, submitReparent, METRICS } from '../lib/ingest.js';
import { config } from '../config.js';

export function createIngestService({ dbPath = config.dbPath } = {}) {
  const db = openDb(dbPath);
  initSchema(db);

  const routes = [
    {
      method: 'GET', path: '/health',
      handler: () => ({ ok: true, service: 'ingest', metrics: METRICS }),
    },
    {
      // Append new events. Body: { events: [...] } or a bare array.
      method: 'POST', path: '/events',
      handler: ({ body, req }) => ingestEvents(db, body, {
        actor: req.headers['x-actor'] ?? 'ingest', requestId: req.headers['x-request-id'],
      }),
    },
    {
      method: 'POST', path: '/corrections',
      handler: ({ body, req }) => submitCorrection(db, body, {
        actor: req.headers['x-actor'] ?? 'ingest', requestId: req.headers['x-request-id'],
      }),
    },
    {
      method: 'POST', path: '/reassignments',
      handler: ({ body, req }) => submitReassignment(db, body, {
        actor: req.headers['x-actor'] ?? 'ingest', requestId: req.headers['x-request-id'],
      }),
    },
    {
      method: 'POST', path: '/reparents',
      handler: ({ body, req }) => submitReparent(db, body, {
        actor: req.headers['x-actor'] ?? 'ingest', requestId: req.headers['x-request-id'],
      }),
    },
    {
      method: 'GET', path: '/orgs',
      handler: () => ({ orgs: db.prepare('SELECT * FROM orgs ORDER BY level, org_id').all() }),
    },
    {
      method: 'GET', path: '/events/:eventId',
      handler: ({ params }) => {
        const versions = db.prepare('SELECT * FROM event_versions WHERE event_id = ? ORDER BY version').all(params.eventId);
        const assignments = db.prepare('SELECT * FROM event_org_assignments WHERE event_id = ? ORDER BY valid_from').all(params.eventId);
        return { event_id: params.eventId, versions, assignments };
      },
    },
    {
      method: 'GET', path: '/log',
      handler: ({ query }) => ({
        entries: db.prepare('SELECT * FROM ingest_log ORDER BY id DESC LIMIT ?').all(Number(query.limit) || 50)
          .map((r) => ({ ...r, detail: JSON.parse(r.detail || '{}') })),
      }),
    },
    {
      method: 'GET', path: '/changes',
      handler: ({ query }) => ({
        changes: db.prepare(`SELECT * FROM change_requests ${query.status ? 'WHERE status = ?' : ''} ORDER BY created_at DESC LIMIT ?`)
          .all(...(query.status ? [query.status] : []), Number(query.limit) || 50)
          .map((r) => ({ ...r, payload: JSON.parse(r.payload || '{}') })),
      }),
    },
  ];

  return { db, ...createService({ name: 'ingest', port: config.ingestPort, routes }) };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const svc = createIngestService();
  const port = await svc.listen();
  console.log(`ingest service listening on http://127.0.0.1:${port}`);
}
