// Shared test helpers: a fresh in-memory-ish DB per test and a small fixed
// dataset with hand-computed expected values.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openDb, initSchema, transaction } from '../src/lib/db.js';
import { nextSeq } from '../src/lib/seq.js';
import { ingestEvents } from '../src/lib/ingest.js';
import { buildAndRun } from '../src/lib/scheduler.js';

export function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'metrics-test-'));
  const db = openDb(join(dir, 'test.db'));
  initSchema(db);
  const cleanup = () => {
    try { db.close(); } catch { /* already closed */ }
    rmSync(dir, { recursive: true, force: true });
  };
  return { db, dir, cleanup };
}

/**
 * Two-branch org tree:
 *   root
 *   ├── north
 *   │   ├── n1
 *   │   └── n2
 *   └── south
 *       └── s1
 */
export const TREE = [
  { org_id: 'root', name: 'Root', parent_id: null, level: 'group' },
  { org_id: 'north', name: 'North', parent_id: 'root', level: 'region' },
  { org_id: 'south', name: 'South', parent_id: 'root', level: 'region' },
  { org_id: 'n1', name: 'N1', parent_id: 'north', level: 'branch' },
  { org_id: 'n2', name: 'N2', parent_id: 'north', level: 'branch' },
  { org_id: 's1', name: 'S1', parent_id: 'south', level: 'branch' },
];

export function addOrgs(db, orgs = TREE) {
  transaction(db, () => {
    for (const o of orgs) {
      db.prepare('INSERT OR IGNORE INTO orgs(org_id, name, parent_id, level) VALUES (?,?,?,?)')
        .run(o.org_id, o.name, o.parent_id, o.level);
      const seq = nextSeq(db, 'org');
      db.prepare('INSERT INTO org_history(org_id, name, parent_id, level, recorded_at, seq) VALUES (?,?,?,?,?,?)')
        .run(o.org_id, o.name, o.parent_id, o.level, new Date().toISOString(), seq);
    }
  });
}

/** A tiny, fully hand-checkable dataset: one week (2026-09-14..2026-09-20). */
export const BASE_EVENTS = [
  { event_id: 'e1', occurred_on: '2026-09-14', org_id: 'n1', metric: 'signups', amount: 10 },
  { event_id: 'e2', occurred_on: '2026-09-14', org_id: 'n1', metric: 'signups', amount: 5 },
  { event_id: 'e3', occurred_on: '2026-09-15', org_id: 'n2', metric: 'signups', amount: 20 },
  { event_id: 'e4', occurred_on: '2026-09-16', org_id: 's1', metric: 'revenue', amount: 100 },
  { event_id: 'e5', occurred_on: '2026-09-20', org_id: 's1', metric: 'signups', amount: 7 },
];

export function seedBase(db, events = BASE_EVENTS) {
  addOrgs(db);
  const res = ingestEvents(db, { events }, { actor: 'test' });
  return res;
}

export function build(db, opts) {
  return buildAndRun(db, { actor: 'test', ...opts });
}

export function q1(db, sql, ...params) {
  return db.prepare(sql).get(...params);
}
export function qAll(db, sql, ...params) {
  return db.prepare(sql).all(...params);
}
