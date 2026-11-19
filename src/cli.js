#!/usr/bin/env node
// Operator CLI. Every command opens the shared DB directly (the services are
// separate processes but all speak to the same SQLite file in WAL mode).
import { openDb, initSchema, nowIso } from './lib/db.js';
import { ingestEvents, submitCorrection, submitReassignment, submitReparent } from './lib/ingest.js';
import { seedOrgs, generateEvents } from './lib/seed.js';
import { buildAndRun, buildPending, planFromBase, baseGeneration, pendingChanges, requestRebuild, approveGeneration } from './lib/scheduler.js';
import { advanceJob, getJob, verifyGeneration } from './lib/generation.js';
import { publishGeneration, publishRelease, listReports, getReport } from './lib/reports.js';
import { resolveGeneration, orgComparison, lineage } from './lib/query.js';
import { config } from './config.js';

const [cmd, ...args] = process.argv.slice(2);

function db() {
  const d = openDb(config.dbPath);
  initSchema(d);
  return d;
}

function parseFlags(argv) {
  const flags = {};
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const [k, v] = a.slice(2).split('=');
      if (v !== undefined) flags[k] = v;
      else if (argv[i + 1] && !argv[i + 1].startsWith('--')) flags[k] = argv[++i];
      else flags[k] = true;
    } else positional.push(a);
  }
  return { flags, positional };
}

const commands = {
  init() {
    const d = db();
    console.log(`database ready at ${config.dbPath}`);
    return d;
  },

  async seed() {
    const d = db();
    const { flags } = parseFlags(args);
    const weeks = Number(flags.weeks ?? 6);
    seedOrgs(d);
    const end = flags.end ?? new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const from = shiftDays(end, -(weeks * 7 - 1));
    const events = generateEvents({ from, to: end, seed: Number(flags.seed ?? 20261120) });
    const res = ingestEvents(d, { events }, { actor: 'seed' });
    console.log(`seeded ${res.accepted} events over ${from}..${end} (${weeks} weeks)`);
    const build = buildAndRun(d, { triggerType: 'initial', reason: `initial load ${from}..${end}` });
    console.log(`generation ${build.generationId} ${build.job.status}; digest ${d.prepare('SELECT substr(digest,1,16) AS d FROM generations WHERE id=?').get(build.generationId).d}`);
    if (flags.publish) {
      approveGeneration(d, build.generationId, { actor: 'cli' });
      const rep = publishGeneration(d, build.generationId, { actor: 'cli', title: `Initial report ${from}..${end}` });
      console.log(`published report ${rep.report_id}`);
    }
  },

  async correct() {
    const d = db();
    const { flags } = parseFlags(args);
    const payload = {
      event_id: flags.event,
      amount: flags.amount !== undefined ? Number(flags.amount) : undefined,
      occurred_on: flags.date,
      org_id: flags.org,
      metric: flags.metric,
      reason: flags.reason ?? 'manual correction',
      org_valid_from: flags['org-valid-from'],
      org_valid_to: flags['org-valid-to'],
    };
    const res = submitCorrection(d, payload, { actor: flags.actor ?? 'cli' });
    console.log(JSON.stringify(res, null, 2));
    if (!flags['no-build']) await commands.build({ implicit: true });
  },

  async reassign() {
    const d = db();
    const { flags } = parseFlags(args);
    const res = submitReassignment(d, {
      event_id: flags.event, org_id: flags.org, valid_from: flags['valid-from'],
      valid_to: flags['valid-to'], reason: flags.reason ?? 'manual reassignment',
    }, { actor: flags.actor ?? 'cli' });
    console.log(JSON.stringify(res, null, 2));
    if (!flags['no-build']) await commands.build({ implicit: true });
  },

  async reparent() {
    const d = db();
    const { flags } = parseFlags(args);
    const res = submitReparent(d, { org_id: flags.org, parent_id: flags.parent ?? null, reason: flags.reason ?? 'manual reparent' }, { actor: flags.actor ?? 'cli' });
    console.log(JSON.stringify(res, null, 2));
    if (!flags['no-build']) await commands.build({ implicit: true });
  },

  async plan() {
    const d = db();
    const base = baseGeneration(d);
    const changes = pendingChanges(d);
    console.log(JSON.stringify({ base_generation: base?.id ?? null, changes: changes.map((c) => ({ id: c.id, type: c.type, summary: c.summary })), plan: planFromBase(d, { base, changes }) }, null, 2));
  },

  async build() {
    const d = db();
    const { flags } = parseFlags(args);
    const build = buildAndRun(d, { triggerType: flags.rebuild ? 'rebuild' : 'change' });
    const g = d.prepare('SELECT * FROM generations WHERE id=?').get(build.generationId);
    console.log(JSON.stringify({
      generation: build.generationId,
      job: build.jobId,
      status: build.job.status,
      steps: build.job.steps.length,
      mode: build.plan.mode,
      affected: build.plan.affected,
      digest: g.digest,
      verification: g.verification ? JSON.parse(g.verification).ok : null,
    }, null, 2));
    if (build.job.status === 'failed') process.exitCode = 1;
  },

  async job() {
    const d = db();
    const { flags, positional } = parseFlags(args);
    const id = positional[0];
    const action = flags.action ?? 'show';
    if (action === 'advance') console.log(JSON.stringify(advanceJob(d, id, Number(flags.steps ?? Infinity)), null, 2));
    else if (action === 'resume') console.log(JSON.stringify(advanceJob(d, id), null, 2));
    else console.log(JSON.stringify(getJob(d, id), null, 2));
  },

  async verify() {
    const d = db();
    const { positional } = parseFlags(args);
    const g = resolveGeneration(d, positional[0]);
    console.log(JSON.stringify({ generation: g.id, ...verifyGeneration(d, g.id) }, null, 2));
  },

  async approve() {
    const d = db();
    const { positional, flags } = parseFlags(args);
    const g = approveGeneration(d, Number(positional[0]), { actor: flags.actor ?? 'cli' });
    console.log(`generation ${g.id} approved by ${g.approved_by}`);
  },

  async publish() {
    const d = db();
    const { positional, flags } = parseFlags(args);
    const ids = positional.map(Number);
    if (ids.length > 1) {
      const res = publishRelease(d, ids, { actor: flags.actor ?? 'cli', note: flags.note ?? '' });
      console.log(JSON.stringify(res, null, 2));
    } else {
      approveGeneration(d, ids[0], { actor: flags.actor ?? 'cli' });
      const res = publishGeneration(d, ids[0], { actor: flags.actor ?? 'cli', title: flags.title });
      console.log(JSON.stringify(res, null, 2));
    }
  },

  async reports() {
    const d = db();
    console.log(JSON.stringify(listReports(d), null, 2));
  },

  async report() {
    const d = db();
    const { positional, flags } = parseFlags(args);
    const rep = getReport(d, Number(positional[0]));
    if (flags.summary) {
      console.log(JSON.stringify({ ...rep, daily: `${rep.daily.length} rows`, weekly: `${rep.weekly.length} rows`, rollup: `${rep.rollup.length} rows` }, null, 2));
    } else console.log(JSON.stringify(rep, null, 2));
  },

  async compare() {
    const d = db();
    const { flags } = parseFlags(args);
    const g = resolveGeneration(d, flags.generation);
    console.log(JSON.stringify(orgComparison(d, { generation: g.id, week_id: flags.week, metric: flags.metric }), null, 2));
  },

  async lineage() {
    const d = db();
    const { flags } = parseFlags(args);
    const g = resolveGeneration(d, flags.generation);
    console.log(JSON.stringify(lineage(d, { generation: g.id, layer: flags.layer, id: flags.id }), null, 2));
  },

  async rebuild() {
    const d = db();
    const { flags } = parseFlags(args);
    requestRebuild(d, { actor: 'cli', reason: flags.reason ?? 'manual rebuild' });
    const build = buildAndRun(d, { triggerType: 'rebuild' });
    console.log(`generation ${build.generationId} rebuilt: ${build.job.status}, verify=${JSON.parse(d.prepare('SELECT verification FROM generations WHERE id=?').get(build.generationId).verification).ok}`);
  },

  async serve() {
    const { startAll } = await import('./start.js');
    await startAll({ withWeb: true });
  },

  async 'start-services'() {
    const { startAll } = await import('./start.js');
    await startAll({ withWeb: false });
  },

  help() {
    console.log(`metrics lineage console — commands:
  init                         create the database
  seed --weeks N [--publish]   generate fictional data and build a generation
  correct --event ID [--amount N] [--date D] [--org O] [--reason R]
  reassign --event ID --org O --valid-from D [--valid-to D]
  reparent --org O --parent P
  plan                         show what the next build would recompute
  build                        build all pending changes
  rebuild                      force a full rebuild from raw inputs
  job <id> [--action show|advance|resume] [--steps N]
  verify [generation]          independently recompute a generation
  approve <generation>         mark ready generation approved
  publish <generation> [more..]  publish one, or a group as one release
  reports                      list published reports
  report <id> [--summary]      show a published report
  compare [--week W] [--metric M]  org comparison table
  lineage [--layer L --id I]   dependency edges
  serve                        run ingest + scheduler + query + web
  start-services               run the three services only
`);
  },
};

function shiftDays(date, n) {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

const fn = commands[cmd];
if (!fn) {
  commands.help();
  process.exitCode = cmd ? 1 : 0;
} else {
  await fn();
}
