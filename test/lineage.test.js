import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshDb, seedBase, build } from './helpers.js';
import { submitCorrection } from '../src/lib/ingest.js';
import { verifyGeneration } from '../src/lib/generation.js';
import { lineage, explain, cellProvenance, orgComparison, weeklySeries, dailySeries } from '../src/lib/query.js';

test('verification detects a weekly layer that reads a stale daily contribution', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g = build(db).generationId;
    assert.equal(verifyGeneration(db, g).ok, true);

    // Simulate the exact bug the brief warns about: daily was corrected but the
    // weekly row was left reading the old contribution.
    db.prepare("UPDATE daily_metrics SET value = value + 500 WHERE generation=? AND date='2026-09-15' AND org_id='n2'").run(g);
    const v = verifyGeneration(db, g);
    assert.equal(v.ok, false);
    // The daily layer itself mismatches the frozen events...
    assert.equal(v.checks.daily.ok, false);
    assert.ok(v.checks.daily.mismatches.length > 0);
    // ...and the weekly-total cross-check no longer equals the daily total.
    assert.equal(v.checks.weekly_total_matches_daily, false);
  } finally { cleanup(); }
});

test('verification detects a rollup that diverged from its subtree', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g = build(db).generationId;
    db.prepare("UPDATE weekly_org_rollup SET value = value + 1 WHERE generation=? AND week_id='2026-W38' AND org_id='root' AND metric='signups'").run(g);
    const v = verifyGeneration(db, g);
    assert.equal(v.ok, false);
    assert.equal(v.checks.rollup.ok, false);
  } finally { cleanup(); }
});

test('lineage links events -> daily -> weekly -> rollup and can focus a node', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g = build(db).generationId;
    const full = lineage(db, { generation: g });
    assert.ok(full.edges.some((e) => e.from_layer === 'event' && e.to_layer === 'daily'));
    assert.ok(full.edges.some((e) => e.from_layer === 'daily' && e.to_layer === 'weekly'));
    assert.ok(full.edges.some((e) => e.from_layer === 'weekly' && e.to_layer === 'rollup'));

    const up = lineage(db, { generation: g, layer: 'rollup', id: '2026-W38@root', direction: 'upstream' });
    assert.ok(up.edges.length > 0);
    assert.ok(up.edges.every((e) => e.to_layer === 'rollup' && e.to_id === 'rollup:2026-W38@root'));
    // Both leaf orgs that have rows feed root's rollup.
    const members = new Set(up.edges.filter((e) => e.reason.includes('ancestor')).map((e) => e.from_id));
    assert.ok(members.has('weekly:2026-W38@n1'));
    assert.ok(members.has('weekly:2026-W38@n2'));
  } finally { cleanup(); }
});

test('explain returns the job steps and change requests that produced a partition', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e5', occurred_on: '2026-09-21', reason: 'cross-week' });
    const g = build(db).generationId;
    const ex = explain(db, { generation: g, layer: 'weekly', id: '2026-W39' });
    assert.ok(ex.steps.length > 0);
    assert.ok(ex.requests.some((r) => r.type === 'correction'));
    assert.ok(ex.steps.some((s) => /cross-week|re-derived/.test(s.reason)));
  } finally { cleanup(); }
});

test('cell provenance lists the event versions that built one daily cell', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    submitCorrection(db, { event_id: 'e1', amount: 100, reason: 'restate' });
    const g2 = build(db).generationId;

    const p1 = cellProvenance(db, { generation: g1, date: '2026-09-14', org_id: 'n1', metric: 'signups' });
    assert.equal(p1.cell.value, 15);
    assert.equal(p1.events.length, 2);
    // Generation 1's pinned snapshot used version 1 of e1.
    assert.equal(p1.events.find((e) => e.event_id === 'e1').version, 1);

    const p2 = cellProvenance(db, { generation: g2, date: '2026-09-14', org_id: 'n1', metric: 'signups' });
    assert.equal(p2.cell.value, 105);
    assert.equal(p2.events.find((e) => e.event_id === 'e1').version, 2);
    // Rollups above n1 include north and root.
    assert.equal(p2.rollups.length, 2);
  } finally { cleanup(); }
});

test('comparison and series reads are pinned and filterable', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g = build(db).generationId;
    const cmp = orgComparison(db, { generation: g, week_id: '2026-W38', metric: 'signups' });
    assert.ok(cmp.rows.every((r) => r.week_id === '2026-W38' && r.metric === 'signups'));
    const names = cmp.rows.map((r) => r.org_id);
    assert.ok(names.includes('root') && names.includes('n1'));
    const root = cmp.rows.find((r) => r.org_id === 'root');
    assert.equal(root.value, 42);
    assert.equal(root.name, 'Root');

    const week = weeklySeries(db, { generation: g, week_id: '2026-W38' });
    assert.ok(week.rows.length > 0);
    const daily = dailySeries(db, { generation: g, date_from: '2026-09-14', date_to: '2026-09-15' });
    assert.ok(daily.rows.every((r) => r.date >= '2026-09-14' && r.date <= '2026-09-15'));
  } finally { cleanup(); }
});
