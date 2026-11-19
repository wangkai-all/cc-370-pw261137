// Scheduler service (port 4102). Owns the compute jobs: it plans the closure of
// pending changes, builds generations step by step, and exposes pause / cancel /
// resume. The poll loop lets it run unattended; auto-run can be disabled when
// the UI wants to drive jobs step-by-step.
import { openDb, initSchema, transaction, nowIso } from '../lib/db.js';
import { createService } from '../lib/http.js';
import {
  buildPending, planFromBase, baseGeneration, pendingChanges, requestRebuild,
  inputsAheadOfLatestGeneration, approveGeneration,
} from '../lib/scheduler.js';
import { runJob, getJob, pauseJob, cancelJob, resumeJob, verifyGeneration, advanceJob } from '../lib/generation.js';
import { publishRelease, verifyReport, publishGeneration } from '../lib/reports.js';
import { NotFoundError, ConflictError } from '../lib/errors.js';
import { config } from '../config.js';

export function createSchedulerService({ dbPath = config.dbPath, autoRun = config.schedulerAutoRun, pollMs = config.schedulerPollMs } = {}) {
  const db = openDb(dbPath);
  initSchema(db);
  let timer = null;
  let busy = false;

  const state = { auto_run: autoRun, last_poll: null, last_result: null };

  function autoBuildOnce() {
    if (!state.auto_run || busy) return null;
    busy = true;
    try {
      const pending = pendingChanges(db);
      if (pending.length === 0) return null;
      const { jobId } = buildPending(db, { actor: 'scheduler', triggerType: 'auto' });
      const job = runJob(db, jobId);
      state.last_result = { job_id: jobId, generation: job.generation, status: job.status, at: nowIso() };
      return state.last_result;
    } catch (err) {
      state.last_result = { error: err.message, at: nowIso() };
      console.error('[scheduler] auto build failed:', err.message);
      return state.last_result;
    } finally {
      busy = false;
      state.last_poll = nowIso();
    }
  }

  function startPolling() {
    if (timer) return;
    timer = setInterval(() => {
      try {
        autoBuildOnce();
      } catch (err) {
        console.error('[scheduler] poll error', err);
      }
    }, pollMs);
    timer.unref?.();
  }

  function stopPolling() {
    if (timer) clearInterval(timer);
    timer = null;
  }

  const routes = [
    { method: 'GET', path: '/health', handler: () => ({ ok: true, service: 'scheduler', ...state, running: Boolean(timer) }) },

    {
      method: 'GET', path: '/status',
      handler: () => ({
        ...state,
        base_generation: baseGeneration(db)?.id ?? null,
        pending_changes: pendingChanges(db).length,
        inputs_ahead: inputsAheadOfLatestGeneration(db),
        jobs: db.prepare('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 20').all()
          .map((j) => ({ ...j, plan: JSON.parse(j.plan || '{}'), request_ids: JSON.parse(j.request_ids || '[]') })),
      }),
    },

    {
      // Dry run: what would a build do, given the pending changes?
      method: 'GET', path: '/plan',
      handler: () => {
        const base = baseGeneration(db);
        const changes = pendingChanges(db);
        const plan = planFromBase(db, { base, changes });
        return { base_generation: base?.id ?? null, changes: changes.map((c) => ({ id: c.id, type: c.type, summary: c.summary })), plan };
      },
    },

    {
      // Start a build but do NOT run it, so the caller can step through it.
      method: 'POST', path: '/builds',
      handler: ({ body }) => {
        const { generationId, jobId, plan } = buildPending(db, { actor: body.actor ?? 'api', triggerType: body.trigger_type ?? 'manual' });
        return { generation_id: generationId, job_id: jobId, plan };
      },
    },

    {
      // Build and run to completion (or first failure). Synchronous so CLI use
      // gets a deterministic result; the UI normally uses jobs below.
      method: 'POST', path: '/builds/run',
      handler: ({ body }) => {
        const { generationId, jobId, plan } = buildPending(db, { actor: body.actor ?? 'api', triggerType: body.trigger_type ?? 'manual' });
        const job = runJob(db, jobId);
        return { generation_id: generationId, job_id: jobId, job, plan };
      },
    },

    {
      method: 'POST', path: '/jobs/:jobId/run',
      handler: ({ params }) => {
        const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(params.jobId);
        if (!job) throw new NotFoundError(`unknown job ${params.jobId}`);
        return getJob(db, params.jobId);
      },
    },

    {
      // Execute remaining steps of a job. body.max_steps=N runs at most N
      // steps then returns, which is what the UI uses to animate and to stop
      // mid-way. Omit max_steps to run to completion.
      method: 'POST', path: '/jobs/:jobId/advance',
      handler: ({ params, body }) => {
        const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(params.jobId);
        if (!job) throw new NotFoundError(`unknown job ${params.jobId}`);
        if (job.status === 'cancelled') throw new ConflictError('job is cancelled; resume it first');
        const max = body.max_steps === undefined ? Infinity : Number(body.max_steps);
        return advanceJob(db, params.jobId, max);
      },
    },

    { method: 'GET', path: '/jobs/:jobId', handler: ({ params }) => getJob(db, params.jobId) },
    { method: 'GET', path: '/jobs', handler: () => ({ jobs: db.prepare('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50').all().map((j) => ({ ...j, plan: JSON.parse(j.plan || '{}') })) }) },

    { method: 'POST', path: '/jobs/:jobId/pause', handler: ({ params }) => pauseJob(db, params.jobId) },
    { method: 'POST', path: '/jobs/:jobId/cancel', handler: ({ params, body }) => cancelJob(db, params.jobId, { abandonGeneration: Boolean(body.abandon_generation) }) },
    { method: 'POST', path: '/jobs/:jobId/resume', handler: ({ params }) => resumeJob(db, params.jobId) },

    {
      method: 'POST', path: '/generations/:id/approve',
      handler: ({ params, body }) => approveGeneration(db, Number(params.id), { actor: body.actor ?? 'approver' }),
    },

    {
      method: 'GET', path: '/generations/:id/verify',
      handler: ({ params }) => {
        const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(Number(params.id));
        if (!g) throw new NotFoundError(`unknown generation ${params.id}`);
        return { generation: g.id, stored: g.verification ? JSON.parse(g.verification) : null, fresh: verifyGeneration(db, g.id) };
      },
    },

    {
      // Approve several ready generations and publish them as one release.
      method: 'POST', path: '/releases',
      handler: ({ body }) => publishRelease(db, body.generation_ids, {
        actor: body.actor ?? 'approver', note: body.note ?? '', title: body.title,
      }),
    },

    {
      method: 'POST', path: '/reports',
      handler: ({ body }) => publishGeneration(db, Number(body.generation_id), { title: body.title, actor: body.actor ?? 'publisher' }),
    },

    { method: 'GET', path: '/reports/:id/verify', handler: ({ params }) => verifyReport(db, Number(params.id)) },

    {
      method: 'POST', path: '/rebuild',
      handler: ({ body }) => {
        const changeId = requestRebuild(db, { actor: body.actor ?? 'api', reason: body.reason ?? 'manual rebuild' });
        const { generationId, jobId, plan } = buildPending(db, { actor: body.actor ?? 'api', triggerType: 'rebuild' });
        return { change_id: changeId, generation_id: generationId, job_id: jobId, plan };
      },
    },

    {
      method: 'POST', path: '/poll/start',
      handler: () => {
        state.auto_run = true;
        startPolling();
        return { running: true, auto_run: true };
      },
    },
    {
      method: 'POST', path: '/poll/stop',
      handler: () => {
        state.auto_run = false;
        stopPolling();
        return { running: false, auto_run: false };
      },
    },
    {
      method: 'POST', path: '/poll/once',
      handler: () => ({ result: autoBuildOnce(), ...state }),
    },
  ];

  const svc = createService({ name: 'scheduler', port: config.schedulerPort, routes });
  return {
    db,
    ...svc,
    startPolling,
    stopPolling,
    autoBuildOnce,
    listen: async () => {
      const port = await svc.listen();
      if (state.auto_run) startPolling();
      return port;
    },
    close: async () => {
      stopPolling();
      await svc.close();
    },
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const svc = createSchedulerService();
  const port = await svc.listen();
  console.log(`scheduler service listening on http://127.0.0.1:${port}`);
}
