// Builds one generation. A generation is a complete, immutable materialization
// of every layer. Unaffected partitions are copied forward from a base
// generation; affected partitions are patched (incremental) or recomputed from
// scratch (rebuild). Every step is separately committed and recorded, so a job
// can be paused/cancelled and resumed exactly where it stopped.
import { randomUUID } from 'node:crypto';
import { transaction, nowIso } from './db.js';
import { inputBoundaries } from './seq.js';
import { snapshotEvents, orgsAt } from './snapshot.js';
import { aggregateDaily, aggregateWeekly, aggregateRollup, buildOrgIndex, digestRows, money } from './compute.js';
import { isoWeekLabel, weekStart } from './time.js';
import { ValidationError, ConflictError, NotFoundError } from './errors.js';

const insertRows = {
  daily(db, gen, rows) {
    const st = db.prepare(`INSERT OR REPLACE INTO daily_metrics(generation, date, org_id, metric, value, event_count)
                           VALUES (?,?,?,?,?,?)`);
    for (const r of rows) st.run(gen, r.date, r.org_id, r.metric, r.value, r.event_count);
  },
  weekly(db, gen, rows) {
    const st = db.prepare(`INSERT OR REPLACE INTO weekly_metrics(generation, week_id, week_start, org_id, metric, value, event_count, day_count)
                           VALUES (?,?,?,?,?,?,?,?)`);
    for (const r of rows) st.run(gen, r.week_id, r.week_start, r.org_id, r.metric, r.value, r.event_count, r.day_count);
  },
  rollup(db, gen, rows) {
    const st = db.prepare(`INSERT OR REPLACE INTO weekly_org_rollup(generation, week_id, week_start, org_id, metric, value, event_count, member_orgs)
                           VALUES (?,?,?,?,?,?,?,?)`);
    for (const r of rows) st.run(gen, r.week_id, r.week_start, r.org_id, r.metric, r.value, r.event_count, JSON.stringify(r.member_orgs));
  },
};

const TABLE = { daily: 'daily_metrics', weekly: 'weekly_metrics', rollup: 'weekly_org_rollup' };

/** Create a job + generation skeleton and persist the step plan. */
export function createGenerationJob(db, { plan, baseGeneration, requestIds, triggerType, reason, strategy, actor = 'scheduler' }) {
  return transaction(db, () => {
    const at = nowIso();
    const bounds = inputBoundaries(db);
    const gen = db.prepare(`
      INSERT INTO generations(status, trigger_type, reason, base_generation, as_of, event_seq, assign_seq, org_seq, started_at, plan)
      VALUES ('building', ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(triggerType, reason, baseGeneration ?? null, at, bounds.eventSeq, bounds.assignSeq, bounds.orgSeq, at, JSON.stringify(plan));

    const generationId = Number(gen.lastInsertRowid);
    const jobId = `job_${randomUUID()}`;
    db.prepare(`INSERT INTO jobs(id, generation, status, strategy, reason, base_generation, as_of, created_at, updated_at, plan, request_ids)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
      .run(jobId, generationId, 'running', strategy, reason, baseGeneration ?? null, at, at, at,
        JSON.stringify(plan), JSON.stringify(requestIds));

    const steps = buildSteps(plan, baseGeneration, generationId);
    const st = db.prepare(`INSERT INTO job_steps(job_id, seq, layer, partition_id, label, reason, meta)
                           VALUES (?,?,?,?,?,?,?)`);
    steps.forEach((s, i) => st.run(jobId, i, s.layer, s.partition_id, s.label, s.reason, JSON.stringify(s.meta ?? {})));
    db.prepare('UPDATE jobs SET total_steps = ? WHERE id = ?').run(steps.length, jobId);

    return { generationId, jobId, steps: steps.length };
  });
}

function buildSteps(plan, baseGeneration, generationId) {
  const steps = [];
  const fresh = baseGeneration === null || baseGeneration === undefined;
  const fullRebuild = Boolean(plan.full_rebuild);

  steps.push({
    layer: 'snapshot',
    partition_id: String(generationId),
    label: 'Pin inputs & materialize event snapshot',
    reason: 'Freeze the exact event versions each layer will read',
    meta: { fresh },
  });

  if (fresh || fullRebuild) {
    // Everything is built from the snapshot: initial load, or an explicit rebuild.
    const why = fresh ? 'initial load: no base generation to copy forward' : 'explicit rebuild: every layer recomputed from raw inputs';
    steps.push({ layer: 'daily', partition_id: '*', label: 'Build all daily partitions', reason: why, meta: { full: true } });
    steps.push({ layer: 'weekly', partition_id: '*', label: 'Build all weekly partitions', reason: why, meta: { full: true } });
    steps.push({ layer: 'rollup', partition_id: '*', label: 'Build org comparison table', reason: why, meta: { full: true } });
  } else {
    const structural = Boolean(plan.structural_change);
    if (!structural) {
      steps.push({
        layer: 'seed', partition_id: 'daily', label: 'Copy unaffected daily partitions forward',
        reason: `reuse all but ${plan.affected ? plan.affected.dates.length : 0} affected date(s)`, meta: {},
      });
    } else {
      // Daily and weekly values are untouched by a reparent; copy them wholesale.
      steps.push({
        layer: 'seed', partition_id: 'daily', label: 'Copy daily partitions forward',
        reason: 'organization tree change does not alter daily values', meta: {},
      });
    }
    steps.push({
      layer: 'seed', partition_id: 'weekly', label: 'Copy unaffected weekly partitions forward',
      reason: structural ? 'organization tree change does not alter weekly values' : 'only touched weeks are re-derived',
      meta: {},
    });
    steps.push({
      layer: 'seed', partition_id: 'rollup', label: 'Copy unaffected org rollups forward',
      reason: structural ? 'all rollups are re-derived after the tree change' : 'only touched weeks are re-rolled up',
      meta: {},
    });
    for (const s of plan.dailySteps) {
      steps.push({ layer: 'daily', partition_id: s.partition_id, label: s.label, reason: s.reason, meta: { strategy: s.strategy, orgs: s.orgs } });
    }
    for (const s of plan.weeklySteps) {
      steps.push({ layer: 'weekly', partition_id: s.partition_id, label: s.label, reason: s.reason, meta: {} });
    }
    for (const weekId of plan.rollupWeeks ?? []) {
      const changed = plan.weeklySteps.some((w) => w.partition_id === weekId);
      steps.push({
        layer: 'rollup', partition_id: weekId, label: `Org comparison ${weekId}`,
        reason: changed
          ? `re-roll week ${weekId} up the organization tree`
          : `week ${weekId} rollups re-derived after organization tree change`,
        meta: {},
      });
    }
  }

  steps.push({
    layer: 'lineage', partition_id: String(generationId),
    label: 'Record dependency edges', reason: 'link each recomputed partition to its inputs', meta: {},
  });
  steps.push({
    layer: 'finalize', partition_id: String(generationId),
    label: 'Verify & finalize generation', reason: 'independently recompute every layer and compare', meta: {},
  });
  return steps;
}

/** Execute the job until it completes, fails, or is cancelled. */
export function runJob(db, jobId, { onStep } = {}) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job "${jobId}"`);
  if (job.status === 'done') return getJob(db, jobId);
  if (job.status === 'cancelled') throw new ConflictError('job is cancelled; resume it first');
  db.prepare("UPDATE jobs SET status='running', updated_at=?, error=NULL WHERE id=? AND status IN ('running','paused')")
    .run(nowIso(), jobId);
  const result = advanceJob(db, jobId, Infinity, onStep);
  return result.job;
}

/**
 * Run up to `maxSteps` remaining steps of a job, committing each step in its
 * own transaction. Returns the job plus whether it stopped early. This is the
 * single step-execution path shared by the CLI, the scheduler service and the
 * UI's step-by-step advance.
 */
export function advanceJob(db, jobId, maxSteps = Infinity, onStep) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job ${jobId}`);
  let ran = 0;
  let stoppedAt = null;

  const steps = db.prepare('SELECT * FROM job_steps WHERE job_id = ? ORDER BY seq').all(jobId);
  for (const step of steps) {
    if (ran >= maxSteps) { stoppedAt = step.label; break; }
    const fresh = db.prepare('SELECT status FROM job_steps WHERE job_id = ? AND seq = ?').get(jobId, step.seq);
    if (fresh.status === 'done') continue;
    const current = db.prepare('SELECT status FROM jobs WHERE id = ?').get(jobId);
    if (current.status === 'cancelled' || current.status === 'paused') { stoppedAt = step.label; break; }

    const outcome = runSingleStep(db, jobId, job, step);
    if (outcome === 'failed') { stoppedAt = step.label; break; }
    ran += 1;
    onStep?.(step);
  }

  const updated = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  const pending = db.prepare("SELECT COUNT(*) AS n FROM job_steps WHERE job_id = ? AND status != 'done'").get(jobId).n;
  if (pending === 0 && updated.status === 'running') {
    transaction(db, () => {
      db.prepare("UPDATE jobs SET status='done', updated_at=? WHERE id=?").run(nowIso(), jobId);
      const ids = JSON.parse(updated.request_ids || '[]');
      if (ids.length) {
        db.prepare(`UPDATE change_requests SET status='done', processed_at=?, generation=? WHERE id IN (${ids.map(() => '?').join(',')})`)
          .run(nowIso(), updated.generation, ...ids);
      }
    });
  }
  return { job: getJob(db, jobId), ran, stopped_at: stoppedAt };
}

function runSingleStep(db, jobId, job, step) {
  try {
    transaction(db, () => {
      executeStep(db, job, step);
      db.prepare("UPDATE job_steps SET status='done', note=? WHERE job_id=? AND seq=?")
        .run(`completed ${nowIso()}`, jobId, step.seq);
    });
    return 'done';
  } catch (err) {
    transaction(db, () => {
      db.prepare("UPDATE jobs SET status='failed', error=?, updated_at=? WHERE id=?").run(err.message, nowIso(), jobId);
      db.prepare("UPDATE job_steps SET status='failed', note=? WHERE job_id=? AND seq=?").run(err.message, jobId, step.seq);
      db.prepare("UPDATE generations SET status='failed', error=?, finished_at=? WHERE id=?").run(err.message, nowIso(), job.generation);
      // A failed build produced nothing usable: put its inputs back in the queue
      // so the next build (which snapshots all current inputs) can recover.
      requeueRequests(db, job);
    });
    return 'failed';
  }
}

function executeStep(db, job, step) {
  const gen = job.generation;
  const meta = JSON.parse(step.meta || '{}');
  switch (step.layer) {
    case 'snapshot': return stepSnapshot(db, gen, job);
    case 'seed': return stepSeed(db, job, step.partition_id);
    case 'daily': return stepDaily(db, job, step);
    case 'weekly': return stepWeekly(db, job, step);
    case 'rollup': return stepRollup(db, job, step);
    case 'lineage': return stepLineage(db, gen);
    case 'finalize': return stepFinalize(db, gen);
    default: throw new Error(`unknown step layer ${step.layer}`);
  }
}

export function generationSnapshot(db, generationId) {
  const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(generationId);
  if (!g) throw new NotFoundError(`unknown generation ${generationId}`);
  const events = db.prepare('SELECT * FROM generation_events WHERE generation = ? ORDER BY event_id').all(generationId);
  const orgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ? ORDER BY org_id').all(generationId);
  return { generation: g, events, orgs };
}

function stepSnapshot(db, gen, job) {
  const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(gen);
  db.prepare('DELETE FROM generation_events WHERE generation = ?').run(gen);
  db.prepare('DELETE FROM generation_orgs WHERE generation = ?').run(gen);
  const events = snapshotEvents(db, { eventSeq: g.event_seq, assignSeq: g.assign_seq });
  const st = db.prepare(`INSERT INTO generation_events(generation, event_id, version, occurred_on, org_id, effective_org, metric, amount)
                         VALUES (?,?,?,?,?,?,?,?)`);
  for (const e of events) st.run(gen, e.event_id, e.version, e.occurred_on, e.org_id, e.effective_org, e.metric, e.amount);
  const orgs = orgsAt(db, g.org_seq);
  const so = db.prepare('INSERT INTO generation_orgs(generation, org_id, name, parent_id, level) VALUES (?,?,?,?,?)');
  for (const o of orgs) so.run(gen, o.org_id, o.name, o.parent_id, o.level);
}

function copyLayer(db, fromGen, toGen, layer) {
  const table = TABLE[layer];
  const cols = layer === 'rollup'
    ? 'week_id, week_start, org_id, metric, value, event_count, member_orgs'
    : layer === 'weekly'
      ? 'week_id, week_start, org_id, metric, value, event_count, day_count'
      : 'date, org_id, metric, value, event_count';
  db.prepare(`INSERT OR REPLACE INTO ${table}(generation, ${cols})
              SELECT ?, ${cols} FROM ${table} WHERE generation = ?`).run(toGen, fromGen);
}

function stepSeed(db, job, layer) {
  const base = job.base_generation;
  if (base === null) throw new Error('seed step requires a base generation');
  db.prepare(`DELETE FROM ${TABLE[layer]} WHERE generation = ?`).run(job.generation);
  copyLayer(db, base, job.generation, layer);
}

function allDailyFromSnapshot(db, gen) {
  const events = db.prepare('SELECT occurred_on, effective_org, metric, amount FROM generation_events WHERE generation = ?').all(gen);
  return [...aggregateDaily(events).values()];
}

function stepDaily(db, job, step) {
  const gen = job.generation;
  const meta = JSON.parse(step.meta || '{}');
  if (meta.full) {
    db.prepare('DELETE FROM daily_metrics WHERE generation = ?').run(gen);
    insertRows.daily(db, gen, allDailyFromSnapshot(db, gen));
    return;
  }
  const date = step.partition_id;
  const strategy = meta.strategy ?? 'incremental';
  if (strategy === 'rebuild') {
    // Recompute the whole partition from the snapshot.
    db.prepare('DELETE FROM daily_metrics WHERE generation = ? AND date = ?').run(gen, date);
    const events = db.prepare(`SELECT occurred_on, effective_org, metric, amount FROM generation_events
                               WHERE generation = ? AND occurred_on = ?`).all(gen, date);
    insertRows.daily(db, gen, [...aggregateDaily(events).values()]);
    return;
  }
  // Incremental: patch only the affected (org, metric) cells for this date.
  const touched = new Set(meta.orgs ?? []);
  const events = db.prepare(`SELECT occurred_on, effective_org, metric, amount FROM generation_events
                             WHERE generation = ? AND occurred_on = ?`).all(gen, date);
  const desired = aggregateDaily(events);
  const existing = db.prepare('SELECT org_id, metric, value, event_count FROM daily_metrics WHERE generation = ? AND date = ?')
    .all(gen, date);
  for (const row of existing) {
    if (touched.size && !touched.has(row.org_id)) continue;
    const key = `${date}|${row.org_id}|${row.metric}`;
    if (!desired.has(key)) {
      db.prepare('DELETE FROM daily_metrics WHERE generation = ? AND date = ? AND org_id = ? AND metric = ?')
        .run(gen, date, row.org_id, row.metric);
    }
  }
  const upsert = db.prepare(`INSERT OR REPLACE INTO daily_metrics(generation, date, org_id, metric, value, event_count)
                             VALUES (?,?,?,?,?,?)`);
  for (const [key, row] of desired) {
    if (touched.size && !touched.has(row.org_id)) continue;
    upsert.run(gen, row.date, row.org_id, row.metric, row.value, row.event_count);
  }
}

function weeklyForWeek(db, gen, weekId, orgFilter) {
  const rows = db.prepare('SELECT * FROM daily_metrics WHERE generation = ?').all(gen)
    .filter((r) => isoWeekLabel(r.date) === weekId)
    .filter((r) => !orgFilter || orgFilter.has(r.org_id));
  return [...aggregateWeekly(rows).values()];
}

function stepWeekly(db, job, step) {
  const gen = job.generation;
  const meta = JSON.parse(step.meta || '{}');
  const weekId = step.partition_id;
  if (meta.full) {
    db.prepare('DELETE FROM weekly_metrics WHERE generation = ?').run(gen);
    const rows = db.prepare('SELECT * FROM daily_metrics WHERE generation = ?').all(gen);
    insertRows.weekly(db, gen, [...aggregateWeekly(rows).values()]);
    return;
  }
  db.prepare('DELETE FROM weekly_metrics WHERE generation = ? AND week_id = ?').run(gen, weekId);
  // Re-derive the week strictly from this generation's daily rows.
  insertRows.weekly(db, gen, weeklyForWeek(db, gen, weekId, null));
}

function stepRollup(db, job, step) {
  const gen = job.generation;
  const meta = JSON.parse(step.meta || '{}');
  db.prepare('DELETE FROM weekly_org_rollup WHERE generation = ?' + (meta.full ? '' : ' AND week_id = ?'))
    .run(...(meta.full ? [gen] : [gen, step.partition_id]));
  const orgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ?').all(gen);
  let weekly;
  if (meta.full) {
    weekly = db.prepare('SELECT * FROM weekly_metrics WHERE generation = ?').all(gen);
  } else {
    weekly = db.prepare('SELECT * FROM weekly_metrics WHERE generation = ? AND week_id = ?').all(gen, step.partition_id);
  }
  const rows = [...aggregateRollup(weekly, orgs).values()];
  insertRows.rollup(db, gen, meta.full ? rows : rows.filter((r) => r.week_id === step.partition_id));
}

function stepLineage(db, gen) {
  const st = db.prepare(`INSERT OR IGNORE INTO dependencies(generation, from_layer, from_id, to_layer, to_id, reason)
                         VALUES (?,?,?,?,?,?)`);
  // Event -> daily, daily -> weekly, weekly -> rollup.
  const events = db.prepare('SELECT event_id, occurred_on, effective_org FROM generation_events WHERE generation = ?').all(gen);
  const dailyDates = new Set(db.prepare('SELECT DISTINCT date FROM daily_metrics WHERE generation = ?').all(gen).map((r) => r.date));
  for (const e of events) {
    if (!dailyDates.has(e.occurred_on)) continue;
    st.run(gen, 'event', e.event_id, 'daily', `daily:${e.occurred_on}`, 'occurred on this date');
  }
  for (const date of dailyDates) {
    st.run(gen, 'daily', `daily:${date}`, 'weekly', `weekly:${isoWeekLabel(date)}`, 'day belongs to week');
  }
  const weeklyOrgs = db.prepare('SELECT DISTINCT week_id, org_id FROM weekly_metrics WHERE generation = ?').all(gen);
  const orgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ?').all(gen);
  const index = buildOrgIndex(orgs);
  for (const w of weeklyOrgs) {
    st.run(gen, 'weekly', `weekly:${w.week_id}@${w.org_id}`, 'rollup', `rollup:${w.week_id}@${w.org_id}`, 'org own value feeds its rollup');
    for (const ancestor of index.ancestorsOf(w.org_id)) {
      st.run(gen, 'weekly', `weekly:${w.week_id}@${w.org_id}`, 'rollup', `rollup:${w.week_id}@${ancestor}`, 'subtree member feeds ancestor rollup');
    }
  }
}

function computeDigest(db, gen) {
  const daily = db.prepare('SELECT date, org_id, metric, value, event_count FROM daily_metrics WHERE generation = ? ORDER BY date, org_id, metric').all(gen);
  const weekly = db.prepare('SELECT week_id, org_id, metric, value, event_count, day_count FROM weekly_metrics WHERE generation = ? ORDER BY week_id, org_id, metric').all(gen);
  const rollup = db.prepare('SELECT week_id, org_id, metric, value, event_count, member_orgs FROM weekly_org_rollup WHERE generation = ? ORDER BY week_id, org_id, metric').all(gen);
  return digestRows([
    ...daily.map((r) => ({ layer: 'daily', ...r })),
    ...weekly.map((r) => ({ layer: 'weekly', ...r })),
    ...rollup.map((r) => ({ layer: 'rollup', ...r })),
  ]);
}

/**
 * Independent verification: recompute every layer from the generation's own
 * frozen snapshot and compare with what was stored. Any mismatch means the
 * generation mixed inputs from different points in time.
 */
export function verifyGeneration(db, gen) {
  const events = db.prepare('SELECT occurred_on, effective_org, metric, amount FROM generation_events WHERE generation = ?').all(gen);
  const orgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ?').all(gen);

  const expectedDaily = aggregateDaily(events);
  const actualDaily = new Map(db.prepare('SELECT date, org_id, metric, value, event_count FROM daily_metrics WHERE generation = ?').all(gen)
    .map((r) => [`${r.date}|${r.org_id}|${r.metric}`, r]));
  const dailyMismatch = diffCells(expectedDaily, actualDaily, (r) => ({ value: r.value, event_count: r.event_count }));

  const expectedWeekly = aggregateWeekly([...expectedDaily.values()]);
  const actualWeekly = new Map(db.prepare('SELECT week_id, org_id, metric, value, event_count, day_count FROM weekly_metrics WHERE generation = ?').all(gen)
    .map((r) => [`${r.week_id}|${r.org_id}|${r.metric}`, r]));
  const weeklyMismatch = diffCells(expectedWeekly, actualWeekly, (r) => ({ value: r.value, event_count: r.event_count, day_count: r.day_count }));

  const expectedRollup = aggregateRollup([...expectedWeekly.values()], orgs);
  const actualRollup = new Map(db.prepare('SELECT week_id, org_id, metric, value, event_count, member_orgs FROM weekly_org_rollup WHERE generation = ?').all(gen)
    .map((r) => [`${r.week_id}|${r.org_id}|${r.metric}`, r]));
  const canonicalMembers = (v) => JSON.stringify([...(typeof v === 'string' ? JSON.parse(v) : v)].sort());
  const rollupMismatch = diffCells(expectedRollup, actualRollup, (r) => ({
    value: r.value,
    event_count: r.event_count,
    member_orgs: canonicalMembers(r.member_orgs),
  }));

  const totalEvents = (m) => [...m.values()].reduce((a, r) => a + (r.event_count ?? 0), 0);
  const totalValue = (m) => Math.round([...m.values()].reduce((a, r) => a + (r.value ?? 0), 0) * 1e6) / 1e6;
  const checks = {
    daily: { ok: dailyMismatch.length === 0, cells: actualDaily.size, mismatches: dailyMismatch.slice(0, 10) },
    weekly: { ok: weeklyMismatch.length === 0, cells: actualWeekly.size, mismatches: weeklyMismatch.slice(0, 10) },
    rollup: { ok: rollupMismatch.length === 0, cells: actualRollup.size, mismatches: rollupMismatch.slice(0, 10) },
    // Weekly totals must equal the sum of daily rows, and rollup totals must
    // equal weekly totals: catches a week left reading a stale daily layer.
    weekly_total_matches_daily: totalValue(actualWeekly) === totalValue(actualDaily),
    event_counts_agree: totalEvents(actualWeekly) === totalEvents(actualDaily),
  };
  return {
    ok: checks.daily.ok && checks.weekly.ok && checks.rollup.ok
      && checks.weekly_total_matches_daily && checks.event_counts_agree,
    checks,
    verified_at: nowIso(),
  };
}

function diffCells(expected, actual, pick) {
  const out = [];
  for (const [key, exp] of expected) {
    const act = actual.get(key);
    const e = pick(exp);
    if (!act) { out.push({ key, problem: 'missing', expected: e }); continue; }
    const a = pick(act);
    if (JSON.stringify(e) !== JSON.stringify(a)) out.push({ key, problem: 'value', expected: e, actual: a });
  }
  for (const key of actual.keys()) {
    if (!expected.has(key)) out.push({ key, problem: 'unexpected', actual: pick(actual.get(key)) });
  }
  return out;
}

async function stepFinalize(db, gen) {
  const verification = verifyGeneration(db, gen);
  const digest = computeDigest(db, gen);
  const ok = verification.ok;
  db.prepare(`UPDATE generations SET status=?, finished_at=?, digest=?, verification=?, error=? WHERE id=?`)
    .run(ok ? 'ready' : 'failed', nowIso(), digest, JSON.stringify(verification),
      ok ? null : 'verification failed: layers do not agree', gen);
  if (!ok) throw new Error('generation verification failed');
}

/** Steps in the current job, with human-readable state for the UI. */
export function getJob(db, jobId) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job "${jobId}"`);
  const steps = db.prepare('SELECT * FROM job_steps WHERE job_id = ? ORDER BY seq').all(jobId)
    .map((s) => ({ ...s, meta: JSON.parse(s.meta || '{}') }));
  const done = steps.filter((s) => s.status === 'done').length;
  return { ...job, plan: JSON.parse(job.plan || '{}'), request_ids: JSON.parse(job.request_ids || '[]'), steps, done, progress: steps.length ? done / steps.length : 0 };
}

export function pauseJob(db, jobId) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job "${jobId}"`);
  if (job.status !== 'running') throw new ConflictError(`job is ${job.status}, not running`);
  db.prepare("UPDATE jobs SET status='paused', updated_at=? WHERE id=?").run(nowIso(), jobId);
  return getJob(db, jobId);
}

/** Change requests belonging to a job go back to the queue when it is abandoned. */
function requeueRequests(db, job) {
  const ids = JSON.parse(job.request_ids || '[]');
  if (!ids.length) return;
  db.prepare(`UPDATE change_requests SET status='pending', generation=NULL
              WHERE id IN (${ids.map(() => '?').join(',')}) AND status='planned'`).run(...ids);
}

/**
 * Cancel a job. By default it stays resumable (its change requests remain
 * attached to it). With `abandonGeneration`, the half-built generation is
 * abandoned and its change requests return to the queue so a later build —
 * which will include every current input — can pick them up.
 */
export function cancelJob(db, jobId, { abandonGeneration = false } = {}) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job "${jobId}"`);
  if (job.status === 'done') throw new ConflictError('job already done');
  transaction(db, () => {
    db.prepare("UPDATE jobs SET status='cancelled', updated_at=? WHERE id=?").run(nowIso(), jobId);
    if (abandonGeneration) {
      db.prepare("UPDATE generations SET status='abandoned', finished_at=?, error='cancelled by user' WHERE id=? AND status='building'")
        .run(nowIso(), job.generation);
      requeueRequests(db, job);
    }
  });
  return getJob(db, jobId);
}

export function resumeJob(db, jobId, opts) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) throw new NotFoundError(`unknown job "${jobId}"`);
  if (job.status !== 'paused' && job.status !== 'cancelled') {
    throw new ConflictError(`job is ${job.status}; only paused or cancelled jobs can resume`);
  }
  transaction(db, () => {
    db.prepare("UPDATE jobs SET status='running', updated_at=?, error=NULL WHERE id=?").run(nowIso(), jobId);
    db.prepare("UPDATE generations SET status='building', error=NULL WHERE id=? AND status='abandoned'").run(job.generation);
  });
  return runJob(db, jobId, opts);
}
