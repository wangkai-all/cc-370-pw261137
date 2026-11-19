// The scheduler turns pending change requests into generations. It runs in its
// own process and only ever reads raw inputs and writes derived generations;
// it never mutates the event store or published reports.
import { transaction, nowIso } from './db.js';
import { inputBoundaries } from './seq.js';
import { snapshotEvents, orgsAt } from './snapshot.js';
import { planRecompute } from './planner.js';
import { createGenerationJob, runJob } from './generation.js';
import { weekIdOf } from './time.js';
import { ConflictError, NotFoundError } from './errors.js';

export function latestApprovedGeneration(db) {
  return db.prepare("SELECT * FROM generations WHERE status = 'approved' ORDER BY id DESC LIMIT 1").get() ?? null;
}

export function latestReadyGeneration(db) {
  return db.prepare("SELECT * FROM generations WHERE status IN ('ready','approved') ORDER BY id DESC LIMIT 1").get() ?? null;
}

/**
 * The base for a new build: the newest generation that was built from the
 * current inputs, regardless of approval. Approval gates *publishing*, not
 * internal lineage, so a build can chain from a ready-but-unapproved parent.
 */
export function baseGeneration(db) {
  return db.prepare("SELECT * FROM generations WHERE status IN ('ready','approved') ORDER BY id DESC LIMIT 1").get() ?? null;
}

export function pendingChanges(db) {
  return db.prepare("SELECT * FROM change_requests WHERE status = 'pending' ORDER BY created_at, id").all();
}

/**
 * Compare the base generation's frozen snapshot with the current inputs and
 * produce the plan. This is where cross-week moves and org revisions widen
 * the recompute set.
 */
export function planFromBase(db, { base, changes }) {
  const bounds = inputBoundaries(db);
  const nextEvents = snapshotEvents(db, bounds);
  const orgs = orgsAt(db, bounds.orgSeq);
  const previousEvents = base
    ? db.prepare('SELECT * FROM generation_events WHERE generation = ?').all(base.id)
    : [];
  const allWeeks = base
    ? db.prepare('SELECT DISTINCT week_id FROM weekly_metrics WHERE generation = ? ORDER BY week_id').all(base.id).map((r) => r.week_id)
    : [];
  const allDates = base
    ? db.prepare('SELECT DISTINCT date FROM daily_metrics WHERE generation = ? ORDER BY date').all(base.id).map((r) => r.date)
    : [];
  const plan = planRecompute({ changes, previousEvents, nextEvents, orgs, allWeeks, allDates });
  plan.input_boundaries = bounds;
  plan.base_generation = base?.id ?? null;
  return plan;
}

/**
 * Only one build may be open at a time. A cancelled job is still resumable, so
 * it counts as open until it is finished or its change requests are requeued.
 *
 * This runs inside the build transaction (not before it): BEGIN IMMEDIATE
 * serializes writers, so two scheduler processes cannot both observe "no open
 * job" and each create one. That is the cross-process guard; no separate lock
 * table is needed.
 */
function assertNoOpenJob(db) {
  const open = db.prepare(`SELECT id, status, generation FROM jobs
                           WHERE status IN ('running','paused','cancelled')
                             AND EXISTS (SELECT 1 FROM change_requests c
                                         WHERE c.status='planned'
                                           AND c.id IN (SELECT value FROM json_each(jobs.request_ids)))
                           ORDER BY created_at DESC LIMIT 1`).get();
  if (open) {
    throw new ConflictError(
      `job ${open.id} (generation ${open.generation}) is ${open.status}; finish or resume it before starting another build`,
      { job_id: open.id, status: open.status, generation: open.generation },
    );
  }
}

/**
 * Build a generation for all pending changes. Returns the job so the caller
 * (CLI/HTTP) can drive or stream it.
 */
export function buildPending(db, { actor = 'scheduler', triggerType = 'change', reason, plan } = {}) {
  const base = baseGeneration(db);
  const changes = pendingChanges(db);
  if (!plan) plan = planFromBase(db, { base, changes });

  const strategy = plan.mode;
  const cleanReason = reason ?? summarizeChanges(changes, plan);
  const { generationId, jobId } = transaction(db, () => {
    // Checked first: "you have an unresolved job" is more actionable than
    // "nothing pending" when both are true. Re-read pending inside the
    // transaction so the requests attached are exactly the ones planned.
    assertNoOpenJob(db);
    const pending = pendingChanges(db);
    if (plan.affected.dates.length === 0 && !plan.structural_change && pending.length === 0) {
      throw new ConflictError('nothing to compute: no pending changes');
    }
    const created = createGenerationJob(db, {
      plan, baseGeneration: base?.id ?? null, requestIds: pending.map((c) => c.id),
      triggerType, reason: cleanReason, strategy, actor,
    });
    for (const c of pending) {
      db.prepare("UPDATE change_requests SET status='planned' WHERE id=?").run(c.id);
    }
    return created;
  });
  return { generationId, jobId, plan };
}

export function summarizeChanges(changes, plan) {
  if (changes.length === 1) {
    const c = changes[0];
    return `${c.type}: ${c.summary}`;
  }
  const types = [...new Set(changes.map((c) => c.type))];
  return `${changes.length} changes (${types.join(', ')}); ${plan.affected.dates.length} daily and ${plan.affected.weeks.length} weekly partitions affected`;
}

/** Build and run to completion synchronously (used by CLI/tests). */
export function buildAndRun(db, opts = {}) {
  const { generationId, jobId, plan } = buildPending(db, opts);
  const job = runJob(db, jobId, opts);
  return { generationId, jobId, job, plan };
}

/**
 * Manually triggered full rebuild, e.g. after a suspected inconsistency.
 */
export function requestRebuild(db, { actor = 'api', reason = 'manual rebuild' } = {}) {
  return transaction(db, () => {
    const id = `chg_rebuild_${Date.now()}`;
    db.prepare(`INSERT INTO change_requests(id, type, payload, summary, status, created_at, requested_by, strategy)
                VALUES (?,?,?,?,?,?,?,?)`)
      .run(id, 'rebuild', '{}', reason, 'pending', nowIso(), actor, 'rebuild');
    return id;
  });
}

/**
 * Detect that inputs changed without any pending change request (e.g. a
 * process crashed between ingesting and enqueueing). Used by the scheduler
 * poll loop.
 */
export function inputsAheadOfLatestGeneration(db) {
  const latest = db.prepare('SELECT * FROM generations ORDER BY id DESC LIMIT 1').get();
  const bounds = inputBoundaries(db);
  if (!latest) return bounds.eventSeq + bounds.assignSeq + bounds.orgSeq > 0;
  return bounds.eventSeq > latest.event_seq
    || bounds.assignSeq > latest.assign_seq
    || bounds.orgSeq > latest.org_seq;
}

export function approveGeneration(db, generationId, { actor = 'approver' } = {}) {
  return transaction(db, () => {
    const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(generationId);
    if (!g) throw new NotFoundError(`unknown generation ${generationId}`);
    if (g.status !== 'ready') throw new ConflictError(`generation is ${g.status}, only ready generations can be approved`);
    const verification = g.verification ? JSON.parse(g.verification) : null;
    if (!verification?.ok) throw new ConflictError('generation failed verification and cannot be approved');
    const at = nowIso();
    db.prepare("UPDATE generations SET status='approved', approved_at=?, approved_by=? WHERE id=?")
      .run(at, actor, generationId);
    return db.prepare('SELECT * FROM generations WHERE id = ?').get(generationId);
  });
}

/** Which week a correction's target date falls into, for the UI. */
export function affectedWeeksForDate(date) {
  return weekIdOf(date).weekId;
}
