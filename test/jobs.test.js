import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshDb, seedBase, build, q1, qAll } from './helpers.js';
import { openDb, initSchema } from '../src/lib/db.js';
import { submitCorrection } from '../src/lib/ingest.js';
import { buildPending, approveGeneration, requestRebuild } from '../src/lib/scheduler.js';
import { advanceJob, cancelJob, pauseJob, resumeJob, getJob } from '../src/lib/generation.js';

test('a job can be advanced step by step, cancelled mid-way, and resumed to completion', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db); // generation 1
    submitCorrection(db, { event_id: 'e1', amount: 50, reason: 'fix' });
    const { jobId, generationId } = buildPending(db, { actor: 'test' });

    const first = advanceJob(db, jobId, 2);
    assert.equal(first.job.status, 'running');
    assert.equal(first.ran, 2);
    assert.ok(first.job.done === 2);

    const paused = cancelJob(db, jobId);
    assert.equal(paused.status, 'cancelled');
    const genMid = q1(db, 'SELECT status FROM generations WHERE id=?', generationId);
    assert.equal(genMid.status, 'building');

    const resumed = resumeJob(db, jobId);
    assert.equal(resumed.status, 'done');
    assert.equal(resumed.done, resumed.total_steps);
    const genEnd = q1(db, 'SELECT status, verification FROM generations WHERE id=?', generationId);
    assert.equal(genEnd.status, 'ready');
    assert.equal(JSON.parse(genEnd.verification).ok, true);
  } finally { cleanup(); }
});

test('pause halts between steps and resume continues without redoing work', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e2', amount: 99, reason: 'fix' });
    const { jobId } = buildPending(db, { actor: 'test' });
    advanceJob(db, jobId, 3);
    const paused = pauseJob(db, jobId);
    assert.equal(paused.status, 'paused');
    const doneBefore = paused.done;
    assert.equal(advanceJob(db, jobId, 1).ran, 0, 'paused job must not advance');
    const resumed = resumeJob(db, jobId);
    assert.equal(resumed.status, 'done');
    assert.ok(resumed.done > doneBefore);
  } finally { cleanup(); }
});

test('a cancelled generation is never silently published; a completed one verifies', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e1', amount: 1, reason: 'fix' });
    const { jobId, generationId } = buildPending(db, { actor: 'test' });
    advanceJob(db, jobId, 1);
    cancelJob(db, jobId, { abandonGeneration: true });
    const g = q1(db, 'SELECT status FROM generations WHERE id=?', generationId);
    assert.equal(g.status, 'abandoned');
    assert.throws(() => approveGeneration(db, generationId), /abandoned/);
  } finally { cleanup(); }
});

test('jobs expose a readable reason for every step', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e5', occurred_on: '2026-09-21', amount: 70, reason: 'cross-week fix' });
    const { jobId } = buildPending(db, { actor: 'test' });
    const job = advanceJob(db, jobId, Infinity).job;
    assert.ok(job.steps.length > 0);
    for (const step of job.steps) {
      assert.ok(step.reason && step.reason.length > 0, `step ${step.seq} (${step.layer}) has no reason`);
      assert.equal(step.status, 'done');
    }
    const weeklySteps = job.steps.filter((s) => s.layer === 'weekly');
    assert.ok(weeklySteps.some((s) => /cross-week/.test(s.reason)), 'cross-week move should be explained');
  } finally { cleanup(); }
});

test('explicit rebuild recomputes every layer from raw inputs', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    const g1 = build(db).generationId;
    requestRebuild(db, { actor: 'test', reason: 'audit' });
    const res = build(db);
    assert.equal(res.plan.mode, 'rebuild');
    assert.equal(res.job.status, 'done');
    // Same data -> identical digest.
    const d1 = q1(db, 'SELECT digest FROM generations WHERE id=?', g1).digest;
    const d2 = q1(db, 'SELECT digest FROM generations WHERE id=?', res.generationId).digest;
    assert.equal(d1, d2);
    assert.equal(JSON.parse(q1(db, 'SELECT verification FROM generations WHERE id=?', res.generationId).verification).ok, true);
  } finally { cleanup(); }
});

test('a second build is refused while a cancelled job is still resumable', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e1', amount: 50, reason: 'fix' });
    const { jobId } = buildPending(db, { actor: 'test' });
    advanceJob(db, jobId, 1);
    cancelJob(db, jobId); // resumable, requests still planned
    assert.throws(() => buildPending(db, { actor: 'test' }), /resume it before starting another/);
    // Abandoning releases the inputs back to the queue, so a new build works.
    cancelJob(db, jobId, { abandonGeneration: true });
    const pending = qAll(db, "SELECT id FROM change_requests WHERE status='pending'");
    assert.ok(pending.length >= 1);
    const res = build(db);
    assert.equal(res.job.status, 'done');
  } finally { cleanup(); }
});

test('abandoning a job returns its change requests to the queue', () => {
  const { db, cleanup } = freshDb();
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e2', amount: 9, reason: 'x' });
    const { jobId } = buildPending(db, { actor: 'test' });
    advanceJob(db, jobId, 1);
    const planned = qAll(db, "SELECT id FROM change_requests WHERE status='planned'");
    assert.equal(planned.length, 1);
    cancelJob(db, jobId, { abandonGeneration: true });
    const back = qAll(db, "SELECT id FROM change_requests WHERE status='pending'");
    assert.equal(back.length, 1);
    assert.equal(back[0].id, planned[0].id);
  } finally { cleanup(); }
});

test('two scheduler connections cannot both open a build', () => {
  const { db, dir, cleanup } = freshDb();
  const other = openDb(`${dir}/test.db`);
  initSchema(other);
  try {
    seedBase(db);
    build(db);
    submitCorrection(db, { event_id: 'e1', amount: 50, reason: 'fix' });

    // Second connection sees the same inputs and tries to start a build while
    // the first one's job is open. Exactly one may win.
    const first = buildPending(db, { actor: 'test' });
    assert.throws(() => buildPending(other, { actor: 'test' }), /resume it before starting another/);
    const open = qAll(db, "SELECT id FROM jobs WHERE status IN ('running','paused','cancelled')");
    assert.equal(open.length, 1);
    assert.equal(open[0].id, first.jobId);
  } finally {
    other.close();
    cleanup();
  }
});
