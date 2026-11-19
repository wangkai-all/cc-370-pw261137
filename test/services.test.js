// Integration: the three services run as real, separate processes against one
// shared SQLite file. Proves cross-process line-of-sight (ingest -> scheduler
// -> query) without sharing memory.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { openDb, initSchema } from '../src/lib/db.js';
import { addOrgs, BASE_EVENTS } from './helpers.js';

const root = resolve(import.meta.dirname, '..');
let dir, dbPath, procs = [], ports = {};

async function freePort() {
  const net = await import('node:net');
  return new Promise((res, rej) => {
    const s = net.createServer();
    s.listen(0, () => { const p = s.address().port; s.close(() => res(p)); });
    s.on('error', rej);
  });
}

function startService(file, port, env) {
  const child = spawn(process.execPath, [resolve(root, file)], {
    cwd: root, env: { ...process.env, METRICS_DB: dbPath, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.on('data', (d) => { log += d; });
  child.stderr.on('data', (d) => { log += d; });
  return { child, getLog: () => log, port };
}

async function waitFor(port, path = '/health', tries = 60) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}${path}`);
      if (r.ok) return true;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`service on ${port} did not start`);
}

async function api(port, method, path, body) {
  const r = await fetch(`http://127.0.0.1:${port}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await r.json();
  if (!r.ok) throw new Error(`${method} ${path} -> ${r.status}: ${JSON.stringify(json)}`);
  return json;
}

before(async () => {
  dir = mkdtempSync(join(tmpdir(), 'metrics-it-'));
  dbPath = join(dir, 'it.db');
  const db = openDb(dbPath);
  initSchema(db);
  addOrgs(db);
  db.close();

  ports.ingest = await freePort();
  ports.scheduler = await freePort();
  ports.query = await freePort();

  procs.push(startService('src/services/ingest-service.js', ports.ingest, { INGEST_PORT: String(ports.ingest) }));
  procs.push(startService('src/services/scheduler-service.js', ports.scheduler, {
    SCHEDULER_PORT: String(ports.scheduler), SCHEDULER_AUTORUN: '0',
  }));
  procs.push(startService('src/services/query-service.js', ports.query, { QUERY_PORT: String(ports.query) }));

  await Promise.all([waitFor(ports.ingest), waitFor(ports.scheduler), waitFor(ports.query)]);
});

after(async () => {
  for (const p of procs) p.child.kill('SIGTERM');
  await new Promise((r) => setTimeout(r, 200));
  rmSync(dir, { recursive: true, force: true });
});

test('ingest service accepts events; query sees nothing until scheduler builds', async () => {
  const res = await api(ports.ingest, 'POST', '/events', { events: BASE_EVENTS });
  assert.equal(res.accepted, BASE_EVENTS.length);
  // No generation yet.
  const empty = await fetch(`http://127.0.0.1:${ports.query}/summary`);
  assert.equal(empty.status, 404);
});

test('scheduler builds across the process boundary and query reads the result', async () => {
  const plan = await api(ports.scheduler, 'GET', '/plan');
  assert.ok(plan.plan.affected.dates.length > 0);

  const run = await api(ports.scheduler, 'POST', '/builds/run', {});
  assert.equal(run.job.status, 'done');

  const summary = await api(ports.query, 'GET', '/summary');
  assert.equal(summary.generation, run.generation_id);
  const snap = await api(ports.query, 'GET', '/snapshot?metric=signups');
  assert.ok(snap.daily.length > 0);
  // All layers in one response belong to one generation.
  assert.ok(snap.generation.id === run.generation_id);
});

test('ingest correction is planned, then built, updating weekly and rollup together', async () => {
  const before = await api(ports.query, 'GET', '/comparison?week_id=2026-W38&metric=signups');
  const rootBefore = before.rows.find((r) => r.org_id === 'root').value;
  assert.equal(rootBefore, 42);

  const corr = await api(ports.ingest, 'POST', '/corrections', {
    event_id: 'e1', amount: 100, reason: 'integration restatement',
  });
  assert.equal(corr.to_version, 2);

  const plan = await api(ports.scheduler, 'GET', '/plan');
  assert.deepEqual(plan.plan.affected.weeks, ['2026-W38']);

  const run = await api(ports.scheduler, 'POST', '/builds/run', {});
  assert.equal(run.job.status, 'done');
  assert.equal(run.plan.mode, 'incremental');

  const after = await api(ports.query, 'GET', `/comparison?generation=${run.generation_id}&week_id=2026-W38&metric=signups`);
  assert.equal(after.rows.find((r) => r.org_id === 'root').value, 132);
  const n1 = after.rows.find((r) => r.org_id === 'n1');
  assert.equal(n1.value, 105);
});

test('job control endpoints: advance, cancel, resume', async () => {
  await api(ports.ingest, 'POST', '/corrections', { event_id: 'e3', amount: 33, reason: 'another fix' });
  const started = await api(ports.scheduler, 'POST', '/builds', {});
  const jobId = started.job_id;

  const stepped = await api(ports.scheduler, 'POST', `/jobs/${jobId}/advance`, { max_steps: 2 });
  assert.equal(stepped.job.status, 'running');
  assert.equal(stepped.ran, 2);

  const cancelled = await api(ports.scheduler, 'POST', `/jobs/${jobId}/cancel`, {});
  assert.equal(cancelled.status, 'cancelled');

  const resumed = await api(ports.scheduler, 'POST', `/jobs/${jobId}/resume`, {});
  assert.equal(resumed.status, 'done');

  const gen = await api(ports.query, 'GET', `/generations/${started.generation_id}`);
  assert.equal(gen.generation.status, 'ready');
  assert.equal(gen.generation.verification.ok, true);
});

test('approve + publish as one release, then verify the report is single-generation', async () => {
  const gens = await api(ports.query, 'GET', '/generations');
  const ready = gens.generations.find((g) => g.status === 'ready');
  assert.ok(ready, 'expected a ready generation');

  const rel = await api(ports.scheduler, 'POST', '/releases', {
    generation_ids: [ready.id], note: 'integration publish',
  });
  assert.equal(rel.report_ids.length, 1);

  const verify = await api(ports.query, 'GET', `/reports/${rel.report_ids[0]}/verify`);
  assert.equal(verify.ok, true);
  assert.deepEqual(verify.generations, [ready.id]);

  const report = await api(ports.query, 'GET', `/reports/${rel.report_ids[0]}`);
  assert.equal(report.generation, ready.id);
});

test('lineage and explain are served over HTTP from the pinned generation', async () => {
  const gens = await api(ports.query, 'GET', '/generations');
  const gen = gens.generations[0];
  const lin = await api(ports.query, 'GET', `/lineage?generation=${gen.id}&layer=rollup&id=2026-W38@root&direction=upstream`);
  assert.ok(lin.edges.length > 0);
  const ex = await api(ports.query, 'GET', `/explain?generation=${gen.id}&layer=weekly&id=2026-W38`);
  assert.ok(ex.steps.length > 0);
});
