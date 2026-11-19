import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshDb, seedBase, build } from './helpers.js';
import { submitCorrection, ingestEvents } from '../src/lib/ingest.js';
import { approveGeneration } from '../src/lib/scheduler.js';
import { publishGeneration, publishRelease, getReport, verifyReport } from '../src/lib/reports.js';
import { resolveGeneration } from '../src/lib/query.js';

function publishReady(db, generationId, actor = 'test') {
  approveGeneration(db, generationId, { actor });
  return publishGeneration(db, generationId, { actor, title: `gen ${generationId}` });
}

test('a published report never changes after later corrections', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    const r1 = publishReady(db, g1);
    const before = getReport(db, r1.report_id);
    // root has no own-line weekly row; its value lives in the rollup layer.
    const beforeRow = before.rollup.find((w) => w.week_id === '2026-W38' && w.org_id === 'root' && w.metric === 'signups');
    assert.equal(beforeRow.value, 42);

    // A big correction downstream.
    submitCorrection(db, { event_id: 'e1', amount: 1000, reason: 'restate' });
    const g2 = build(db).generationId;
    publishReady(db, g2);

    const after = getReport(db, r1.report_id);
    const afterRow = after.rollup.find((w) => w.week_id === '2026-W38' && w.org_id === 'root' && w.metric === 'signups');
    assert.equal(afterRow.value, 42, 'old report row must be frozen');
    assert.deepEqual(after, before, 'old report is byte-for-byte identical');
    assert.equal(verifyReport(db, r1.report_id).ok, true);
  } finally { cleanup(); }
});

test('each report pins exactly one generation across all layers', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    const g2 = (() => { submitCorrection(db, { event_id: 'e2', amount: 1, reason: 'x' }); return build(db).generationId; })();
    const rel = publishRelease(db, [g1, g2], { actor: 'test', note: 'two baselines' });
    assert.equal(rel.report_ids.length, 2);
    for (const id of rel.report_ids) {
      const v = verifyReport(db, id);
      assert.equal(v.ok, true);
      assert.equal(v.generations.length, 1);
    }
    const rep1 = getReport(db, rel.report_ids[0]);
    const rep2 = getReport(db, rel.report_ids[1]);
    assert.equal(rep1.generation, g1);
    assert.equal(rep2.generation, g2);
    assert.notEqual(rep1.digest, rep2.digest);
  } finally { cleanup(); }
});

test('publishing an unapproved or unverified generation is refused', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    assert.throws(() => publishGeneration(db, g1, { actor: 'x' }), /approved/);
    approveGeneration(db, g1);
    assert.ok(publishGeneration(db, g1, { actor: 'x' }).report_id);
  } finally { cleanup(); }
});

test('a release requires every generation to be ready', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    approveGeneration(db, g1);
    assert.throws(() => publishRelease(db, [g1, 9999]), /unknown generation/);
  } finally { cleanup(); }
});

test('queries always resolve one generation, even as new ones appear', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    const resolved1 = resolveGeneration(db);
    assert.equal(resolved1.id, g1);
    submitCorrection(db, { event_id: 'e1', amount: 3, reason: 'x' });
    const g2 = build(db).generationId;
    // Explicit pin still returns the old generation.
    assert.equal(resolveGeneration(db, g1).id, g1);
    // Unpinned returns the newest.
    assert.equal(resolveGeneration(db).id, g2);
  } finally { cleanup(); }
});

test('a correction after a build does not retroactively alter that generation', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    const dailyBefore = db.prepare('SELECT * FROM daily_metrics WHERE generation=? ORDER BY date, org_id, metric').all(g1);
    submitCorrection(db, { event_id: 'e1', amount: 99999, reason: 'later fix' });
    const dailyAfter = db.prepare('SELECT * FROM daily_metrics WHERE generation=? ORDER BY date, org_id, metric').all(g1);
    assert.deepEqual(dailyAfter, dailyBefore);
  } finally { cleanup(); }
});

test('multi-generation release verifies every report independently', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const ids = [build(db).generationId];
    ingestEvents(db, { events: [{ event_id: 'x1', occurred_on: '2026-09-25', org_id: 'n2', metric: 'signups', amount: 4 }] });
    ids.push(build(db).generationId);
    const rel = publishRelease(db, ids, { actor: 'approver', note: 'batch' });
    for (const id of rel.report_ids) {
      assert.equal(verifyReport(db, id).ok, true);
    }
    const gens = new Set(rel.report_ids.map((id) => getReport(db, id).generation));
    assert.deepEqual([...gens].sort(), ids.sort());
  } finally { cleanup(); }
});
