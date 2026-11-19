import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshDb, seedBase, build, q1, qAll } from './helpers.js';
import { submitCorrection, submitReassignment, submitReparent, ingestEvents } from '../src/lib/ingest.js';
import { verifyGeneration } from '../src/lib/generation.js';
import { publishGeneration, verifyReport } from '../src/lib/reports.js';
import { approveGeneration } from '../src/lib/scheduler.js';

function rollup(db, gen, week, org, metric) {
  return q1(db, 'SELECT value, event_count, member_orgs FROM weekly_org_rollup WHERE generation=? AND week_id=? AND org_id=? AND metric=?', gen, week, org, metric);
}
function weeklyRow(db, gen, week, org, metric) {
  return q1(db, 'SELECT value, event_count, day_count FROM weekly_metrics WHERE generation=? AND week_id=? AND org_id=? AND metric=?', gen, week, org, metric);
}

test('amount correction updates daily AND the whole weekly/rollup chain', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    assert.equal(weeklyRow(db, g1, '2026-W38', 'n1', 'signups').value, 15);
    assert.equal(rollup(db, g1, '2026-W38', 'root', 'signups').value, 42);

    // e1: 10 -> 100. Daily n1 becomes 105; root weekly becomes 132.
    submitCorrection(db, { event_id: 'e1', amount: 100, reason: 'fix' });
    const res = build(db);
    assert.equal(res.plan.mode, 'incremental');
    const g2 = res.generationId;

    assert.equal(q1(db, "SELECT value FROM daily_metrics WHERE generation=? AND date='2026-09-14' AND org_id='n1' AND metric='signups'", g2).value, 105);
    assert.equal(weeklyRow(db, g2, '2026-W38', 'n1', 'signups').value, 105);
    assert.equal(rollup(db, g2, '2026-W38', 'north', 'signups').value, 125); // 105 + 20
    assert.equal(rollup(db, g2, '2026-W38', 'root', 'signups').value, 132); // 105 + 20 + 7
    // Unrelated revenue is untouched but present (copied forward).
    assert.equal(rollup(db, g2, '2026-W38', 'root', 'revenue').value, 100);
    assert.equal(verifyGeneration(db, g2).ok, true);
  } finally { cleanup(); }
});

test('cross-week date move removes from source week and adds to target week', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    // e5 (7 signups) on 2026-09-20 (W38, Sunday) -> 2026-09-21 (W39, Monday).
    const corr = submitCorrection(db, { event_id: 'e5', occurred_on: '2026-09-21', reason: 'wrong week' });
    assert.deepEqual(corr.moved_week, { from: '2026-09-20', to: '2026-09-21' });
    const res = build(db);
    const g2 = res.generationId;
    assert.deepEqual(res.plan.affected.changed_weeks, ['2026-W38', '2026-W39']);

    // W38 root signups drops 42 -> 35; W39 gains 7.
    assert.equal(rollup(db, g2, '2026-W38', 'root', 'signups').value, 35);
    assert.equal(rollup(db, g2, '2026-W39', 'root', 'signups').value, 7);
    // The source day is fully cleaned up.
    assert.equal(q1(db, "SELECT COUNT(*) AS n FROM daily_metrics WHERE generation=? AND date='2026-09-20' AND org_id='s1'", g2).n, 0);
    // Old generation is untouched.
    assert.equal(rollup(db, g1, '2026-W38', 'root', 'signups').value, 42);
    assert.equal(verifyGeneration(db, g2).ok, true);
  } finally { cleanup(); }
});

test('org attribution revision moves value between subtrees and both ancestors update', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    // e4: revenue 100 on s1 -> reassign to n2 for 2026-09-16 onwards.
    const r = submitReassignment(db, { event_id: 'e4', org_id: 'n2', valid_from: '2026-09-16', reason: 'branch move' });
    assert.equal(r.status, 'accepted');
    const res = build(db);
    const g2 = res.generationId;

    // south loses 100, north gains 100, root stays 100.
    assert.equal(rollup(db, g2, '2026-W38', 'south', 'revenue')?.value ?? 0, 0);
    assert.equal(rollup(db, g2, '2026-W38', 'north', 'revenue').value, 100);
    assert.equal(rollup(db, g2, '2026-W38', 'root', 'revenue').value, 100);
    assert.deepEqual(rollup(db, g2, '2026-W38', 'root', 'revenue').member_orgs, JSON.stringify(['n2']));
    assert.equal(verifyGeneration(db, g2).ok, true);
    // Historical generation still shows s1.
    assert.equal(rollup(db, g1, '2026-W38', 'south', 'revenue').value, 100);
  } finally { cleanup(); }
});

test('reparenting a subtree re-derives every week rollup up the new tree', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    // Add a second week so the structural change spans more than one week.
    ingestEvents(db, { events: [{ event_id: 'e6', occurred_on: '2026-09-22', org_id: 'n1', metric: 'signups', amount: 3 }] });
    build(db);
    const gBase = q1(db, 'SELECT MAX(id) AS id FROM generations').id;

    // Move n1 from north to south.
    submitReparent(db, { org_id: 'n1', parent_id: 'south', reason: 'reorg' });
    const res = build(db);
    const g = res.generationId;
    assert.equal(res.plan.mode, 'structural');
    assert.ok(res.plan.affected.weeks.includes('2026-W38'));
    assert.ok(res.plan.affected.weeks.includes('2026-W39'));

    // W38: n1 had 15 signups; now south = n1(15) + s1(7) = 22, north = n2(20), root = 42.
    assert.equal(rollup(db, g, '2026-W38', 'south', 'signups').value, 22);
    assert.equal(rollup(db, g, '2026-W38', 'north', 'signups').value, 20);
    assert.equal(rollup(db, g, '2026-W38', 'root', 'signups').value, 42);
    // W39: n1 had 3 signups, now under south.
    assert.equal(rollup(db, g, '2026-W39', 'south', 'signups').value, 3);
    assert.equal(verifyGeneration(db, g).ok, true);
    // Prior generation untouched.
    assert.equal(rollup(db, gBase, '2026-W38', 'north', 'signups').value, 35);
  } finally { cleanup(); }
});

test('a correction dated into an untouched week still updates that week', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    // Move e1 from W38 into a brand-new W40 by changing its date.
    const corr = submitCorrection(db, { event_id: 'e1', occurred_on: '2026-10-01', reason: 'late posting' });
    assert.equal(corr.from_version, 1);
    const res = build(db);
    const g = res.generationId;
    assert.equal(rollup(db, g, '2026-W40', 'root', 'signups').value, 10);
    assert.equal(rollup(db, g, '2026-W38', 'n1', 'signups').value, 5); // 15 - 10
    assert.equal(verifyGeneration(db, g).ok, true);
  } finally { cleanup(); }
});

test('weekly values always equal the sum of their days within one generation', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    submitCorrection(db, { event_id: 'e3', amount: 55, reason: 'tweak' });
    const g = build(db).generationId;
    const dailySum = q1(db, `SELECT SUM(value) AS v FROM daily_metrics WHERE generation=? AND metric='signups'`, g).v;
    const weeklySum = q1(db, `SELECT SUM(value) AS v FROM weekly_metrics WHERE generation=? AND metric='signups'`, g).v;
    const rollupRoot = q1(db, `SELECT SUM(value) AS v FROM weekly_org_rollup WHERE generation=? AND metric='signups' AND org_id='root'`, g).v;
    assert.equal(weeklySum, dailySum);
    assert.equal(rollupRoot, weeklySum);
  } finally { cleanup(); }
});
