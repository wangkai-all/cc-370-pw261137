import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshDb, seedBase, build, q1, qAll } from './helpers.js';
import { isoWeekLabel, weekStart, addDays, isDateString } from '../src/lib/time.js';
import { aggregateDaily, aggregateWeekly, aggregateRollup } from '../src/lib/compute.js';

test('ISO week boundaries use the Thursday rule across year ends', () => {
  assert.equal(isoWeekLabel('2026-09-14'), '2026-W38');
  assert.equal(isoWeekLabel('2026-09-20'), '2026-W38');
  assert.equal(isoWeekLabel('2026-09-21'), '2026-W39');
  // 2027-01-01 is a Friday -> belongs to the week of 2026-12-28.
  assert.equal(isoWeekLabel('2027-01-01'), '2026-W53');
  assert.equal(isoWeekLabel('2026-01-01'), '2026-W01');
  assert.equal(isoWeekLabel('2021-01-01'), '2020-W53');
  assert.equal(weekStart('2026-09-17'), '2026-09-14');
  assert.equal(addDays('2026-12-31', 1), '2027-01-01');
  assert.equal(isDateString('2026-02-30'), false);
});

test('pure aggregation: daily, weekly, rollup totals agree', () => {
  const events = [
    { occurred_on: '2026-09-14', effective_org: 'n1', metric: 's', amount: 10 },
    { occurred_on: '2026-09-14', effective_org: 'n1', metric: 's', amount: 5 },
    { occurred_on: '2026-09-15', effective_org: 'n2', metric: 's', amount: 20 },
    { occurred_on: '2026-09-21', effective_org: 's1', metric: 's', amount: 7 },
  ];
  const daily = aggregateDaily(events);
  assert.equal(daily.get('2026-09-14|n1|s').value, 15);
  const weekly = aggregateWeekly([...daily.values()]);
  assert.equal(weekly.get('2026-W38|n1|s').value, 15);
  assert.equal(weekly.get('2026-W38|n1|s').day_count, 1);
  assert.equal(weekly.get('2026-W38|n2|s').value, 20);

  const orgRows = [
    { org_id: 'root', parent_id: null }, { org_id: 'north', parent_id: 'root' },
    { org_id: 'n1', parent_id: 'north' }, { org_id: 'n2', parent_id: 'north' },
    { org_id: 's1', parent_id: 'root' },
  ];
  const rollup = aggregateRollup([...weekly.values()], orgRows);
  assert.equal(rollup.get('2026-W38|root|s').value, 35);
  assert.equal(rollup.get('2026-W38|north|s').value, 35);
  assert.deepEqual(rollup.get('2026-W38|root|s').member_orgs, ['n1', 'n2']);
  // s1 has no events in W38 at all, so it contributes no rollup row.
  assert.equal(rollup.has('2026-W38|s1|s'), false);
});

test('initial build materializes exact hand-computed values', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const res = build(db);
    assert.equal(res.job.status, 'done');
    const gen = res.generationId;

    // Daily: two events on n1/2026-09-14 merge.
    const d = q1(db, "SELECT value, event_count FROM daily_metrics WHERE generation=? AND date='2026-09-14' AND org_id='n1' AND metric='signups'", gen);
    assert.equal(d.value, 15);
    assert.equal(d.event_count, 2);

    // Weekly W38 for root signups = 15 (n1) + 20 (n2) + 7 (s1) = 42.
    const w = q1(db, "SELECT value, event_count FROM weekly_org_rollup WHERE generation=? AND week_id='2026-W38' AND org_id='root' AND metric='signups'", gen);
    assert.equal(w.value, 42);
    assert.equal(w.event_count, 4);

    // Revenue only lives under south.
    const rev = q1(db, "SELECT value FROM weekly_org_rollup WHERE generation=? AND week_id='2026-W38' AND org_id='south' AND metric='revenue'", gen);
    assert.equal(rev.value, 100);
    const rootRev = q1(db, "SELECT value FROM weekly_org_rollup WHERE generation=? AND week_id='2026-W38' AND org_id='root' AND metric='revenue'", gen);
    assert.equal(rootRev.value, 100);
  } finally { cleanup(); }
});

test('every built generation passes independent verification', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const res = build(db);
    const g = q1(db, 'SELECT verification, status FROM generations WHERE id=?', res.generationId);
    assert.equal(g.status, 'ready');
    const v = JSON.parse(g.verification);
    assert.equal(v.ok, true);
    assert.deepEqual(v.checks.daily.mismatches, []);
    assert.deepEqual(v.checks.weekly.mismatches, []);
    assert.deepEqual(v.checks.rollup.mismatches, []);
  } finally { cleanup(); }
});
