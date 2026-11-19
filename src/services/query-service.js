// Query service (port 4103). Read-only. Every response is pinned to exactly one
// generation so it can never mix daily rows from one build with weekly rows
// from another. Serves both the derived metrics and the lineage/explain views.
import { openDb, initSchema } from '../lib/db.js';
import { createService } from '../lib/http.js';
import {
  resolveGeneration, listGenerations, orgTree, dailySeries, weeklySeries,
  orgComparison, lineage, explain, cellProvenance,
} from '../lib/query.js';
import { listReports, getReport, verifyReport } from '../lib/reports.js';
import { config } from '../config.js';

export function createQueryService({ dbPath = config.dbPath } = {}) {
  const db = openDb(dbPath, { readOnly: false });
  initSchema(db);

  const routes = [
    { method: 'GET', path: '/health', handler: () => ({ ok: true, service: 'query' }) },

    { method: 'GET', path: '/generations', handler: ({ query }) => ({ generations: listGenerations(db, { limit: Number(query.limit) || 50 }) }) },
    {
      method: 'GET', path: '/generations/:id',
      handler: ({ params }) => {
        const g = resolveGeneration(db, params.id);
        return { generation: { ...g, plan: JSON.parse(g.plan || '{}'), verification: g.verification ? JSON.parse(g.verification) : null } };
      },
    },
    { method: 'GET', path: '/orgs', handler: ({ query }) => orgTree(db, query.generation) },
    { method: 'GET', path: '/daily', handler: ({ query }) => dailySeries(db, query) },
    { method: 'GET', path: '/weekly', handler: ({ query }) => weeklySeries(db, query) },
    { method: 'GET', path: '/comparison', handler: ({ query }) => orgComparison(db, query) },
    { method: 'GET', path: '/lineage', handler: ({ query }) => lineage(db, query) },
    { method: 'GET', path: '/explain', handler: ({ query }) => explain(db, query) },
    { method: 'GET', path: '/provenance', handler: ({ query }) => cellProvenance(db, query) },

    { method: 'GET', path: '/reports', handler: () => ({ reports: listReports(db) }) },
    { method: 'GET', path: '/reports/:id', handler: ({ params }) => getReport(db, Number(params.id)) },
    { method: 'GET', path: '/reports/:id/verify', handler: ({ params }) => verifyReport(db, Number(params.id)) },

    {
      // The dashboard's single consistent read: one generation id, all layers
      // fetched in one request so the client cannot assemble a mixed view.
      method: 'GET', path: '/snapshot',
      handler: ({ query }) => {
        const g = resolveGeneration(db, query.generation);
        const week = query.week_id ?? null;
        return {
          generation: { id: g.id, status: g.status, digest: g.digest, as_of: g.as_of, reason: g.reason, verification: g.verification ? JSON.parse(g.verification) : null },
          daily: dailySeries(db, { generation: g.id, date_from: query.date_from, date_to: query.date_to, metric: query.metric }).rows,
          weekly: weeklySeries(db, { generation: g.id, week_id: week, metric: query.metric }).rows,
          comparison: orgComparison(db, { generation: g.id, week_id: week, metric: query.metric }).rows,
          orgs: orgTree(db, g.id).orgs,
        };
      },
    },
    {
      // Aggregate summary for the header tiles, still from one generation.
      method: 'GET', path: '/summary',
      handler: ({ query }) => {
        const g = resolveGeneration(db, query.generation);
        const daily = db.prepare('SELECT metric, SUM(value) AS value, SUM(event_count) AS events, COUNT(DISTINCT date) AS days FROM daily_metrics WHERE generation = ? GROUP BY metric')
          .all(g.id);
        const weeks = db.prepare('SELECT DISTINCT week_id FROM weekly_metrics WHERE generation = ? ORDER BY week_id').all(g.id).map((r) => r.week_id);
        const range = db.prepare('SELECT MIN(date) AS from_date, MAX(date) AS to_date FROM daily_metrics WHERE generation = ?').get(g.id);
        return { generation: g.id, status: g.status, totals: daily, weeks, range, digest: g.digest };
      },
    },
  ];

  return { db, ...createService({ name: 'query', port: config.queryPort, routes }) };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const svc = createQueryService();
  const port = await svc.listen();
  console.log(`query service listening on http://127.0.0.1:${port}`);
}
