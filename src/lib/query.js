// Read models. Every query is pinned to exactly one generation id, so a
// response can never mix daily rows from one build with weekly rows from
// another. Callers either name a generation or get the newest ready/approved
// one, which is chosen in a single statement and then used everywhere.
import { NotFoundError } from './errors.js';
import { isoWeekLabel } from './time.js';
import { buildOrgIndex } from './compute.js';

export function resolveGeneration(db, generationId) {
  if (generationId !== undefined && generationId !== null && generationId !== '') {
    const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(Number(generationId));
    if (!g) throw new NotFoundError(`unknown generation ${generationId}`);
    return g;
  }
  const g = db.prepare("SELECT * FROM generations WHERE status IN ('ready','approved') ORDER BY id DESC LIMIT 1").get();
  if (!g) throw new NotFoundError('no completed generation yet');
  return g;
}

export function listGenerations(db, { limit = 50 } = {}) {
  return db.prepare('SELECT * FROM generations ORDER BY id DESC LIMIT ?').all(limit).map((g) => ({
    ...g,
    plan: JSON.parse(g.plan || '{}'),
    verification: g.verification ? JSON.parse(g.verification) : null,
  }));
}

export function orgTree(db, generationId) {
  const gen = resolveGeneration(db, generationId);
  const orgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ? ORDER BY org_id').all(gen.id);
  return { generation: gen.id, orgs };
}

export function dailySeries(db, { generation, date_from, date_to, org_id, metric } = {}) {
  const gen = resolveGeneration(db, generation);
  const where = ['generation = ?'];
  const params = [gen.id];
  if (date_from) { where.push('date >= ?'); params.push(date_from); }
  if (date_to) { where.push('date <= ?'); params.push(date_to); }
  if (org_id) { where.push('org_id = ?'); params.push(org_id); }
  if (metric) { where.push('metric = ?'); params.push(metric); }
  const rows = db.prepare(`SELECT date, org_id, metric, value, event_count FROM daily_metrics
                           WHERE ${where.join(' AND ')} ORDER BY date, org_id, metric`).all(...params);
  return { generation: gen.id, rows };
}

export function weeklySeries(db, { generation, week_id, org_id, metric } = {}) {
  const gen = resolveGeneration(db, generation);
  const where = ['generation = ?'];
  const params = [gen.id];
  if (week_id) { where.push('week_id = ?'); params.push(week_id); }
  if (org_id) { where.push('org_id = ?'); params.push(org_id); }
  if (metric) { where.push('metric = ?'); params.push(metric); }
  const rows = db.prepare(`SELECT week_id, week_start, org_id, metric, value, event_count, day_count
                           FROM weekly_metrics WHERE ${where.join(' AND ')}
                           ORDER BY week_id, org_id, metric`).all(...params);
  return { generation: gen.id, rows };
}

export function orgComparison(db, { generation, week_id, metric } = {}) {
  const gen = resolveGeneration(db, generation);
  const where = ['r.generation = ?'];
  const params = [gen.id];
  if (week_id) { where.push('r.week_id = ?'); params.push(week_id); }
  if (metric) { where.push('r.metric = ?'); params.push(metric); }
  const rows = db.prepare(`
    SELECT r.week_id, r.week_start, r.org_id, o.name, o.level, r.metric, r.value, r.event_count, r.member_orgs
    FROM weekly_org_rollup r
    LEFT JOIN generation_orgs o ON o.generation = r.generation AND o.org_id = r.org_id
    WHERE ${where.join(' AND ')}
    ORDER BY r.week_id, o.level, r.org_id, r.metric`).all(...params);
  return {
    generation: gen.id,
    rows: rows.map((r) => ({ ...r, member_orgs: JSON.parse(r.member_orgs) })),
  };
}

/** Full lineage graph of a generation, optionally focused on one node.
 * `layer` + `id` identify a node (e.g. layer='rollup', id='2026-W38@root');
 * returns edges where that node is upstream (downstream), downstream
 * (upstream), or either end (both). */
export function lineage(db, { generation, layer, id, direction = 'both' } = {}) {
  const gen = resolveGeneration(db, generation);
  let edges = db.prepare('SELECT from_layer, from_id, to_layer, to_id, reason FROM dependencies WHERE generation = ?')
    .all(gen.id);
  if (layer && id) {
    // Stored ids already carry their layer prefix (e.g. "rollup:2026-W38@root").
    const node = `${layer}:${id}`;
    const isFrom = (e) => e.from_layer === layer && e.from_id === node;
    const isTo = (e) => e.to_layer === layer && e.to_id === node;
    if (direction === 'downstream') edges = edges.filter(isFrom);
    else if (direction === 'upstream') edges = edges.filter(isTo);
    else edges = edges.filter((e) => isFrom(e) || isTo(e));
  }
  return { generation: gen.id, node: layer && id ? `${layer}:${id}` : null, edges };
}

/**
 * "Why was this recomputed?" — the job steps that touched a partition, with
 * their recorded reasons, plus the change requests that caused the build.
 */
export function explain(db, { generation, layer, id } = {}) {
  const gen = resolveGeneration(db, generation);
  const jobs = db.prepare('SELECT * FROM jobs WHERE generation = ? ORDER BY created_at').all(gen.id);
  const explanations = [];
  for (const job of jobs) {
    const steps = db.prepare('SELECT * FROM job_steps WHERE job_id = ? ORDER BY seq').all(job.id);
    for (const s of steps) {
      if (layer && id) {
        const matches = s.layer === layer && (s.partition_id === id || s.partition_id === '*');
        if (!matches) continue;
      }
      explanations.push({
        job_id: job.id,
        strategy: job.strategy,
        step: s.seq,
        layer: s.layer,
        partition_id: s.partition_id,
        label: s.label,
        reason: s.reason,
        status: s.status,
        meta: JSON.parse(s.meta || '{}'),
      });
    }
  }
  const requestIds = jobs.flatMap((j) => JSON.parse(j.request_ids || '[]'));
  const requests = requestIds.length
    ? db.prepare(`SELECT id, type, summary, status, created_at FROM change_requests WHERE id IN (${requestIds.map(() => '?').join(',')})`).all(...requestIds)
    : [];
  return {
    generation: gen.id,
    generation_reason: gen.reason,
    trigger_type: gen.trigger_type,
    base_generation: gen.base_generation,
    inputs: { event_seq: gen.event_seq, assign_seq: gen.assign_seq, org_seq: gen.org_seq },
    steps: explanations,
    requests,
  };
}

/**
 * Which events feed one daily cell, with their versions. Lets the UI show a
 * value's provenance and prove it came from this generation's snapshot.
 */
export function cellProvenance(db, { generation, date, org_id, metric }) {
  const gen = resolveGeneration(db, generation);
  if (!date || !org_id || !metric) {
    throw new NotFoundError('date, org_id and metric are required');
  }
  const events = db.prepare(`SELECT event_id, version, occurred_on, org_id, effective_org, metric, amount
                             FROM generation_events
                             WHERE generation = ? AND occurred_on = ? AND effective_org = ? AND metric = ?
                             ORDER BY event_id`).all(gen.id, date, org_id, metric);
  const versions = events.length
    ? db.prepare(`SELECT event_id, version, amount, org_id, reason, recorded_at, is_correction
                  FROM event_versions WHERE event_id IN (${events.map(() => '?').join(',')})
                  ORDER BY event_id, version`).all(...events.map((e) => e.event_id))
    : [];
  const cell = db.prepare(`SELECT value, event_count FROM daily_metrics
                           WHERE generation = ? AND date = ? AND org_id = ? AND metric = ?`)
    .get(gen.id, date, org_id, metric);
  const weekly = db.prepare(`SELECT week_id, week_start, value, event_count, day_count FROM weekly_metrics
                             WHERE generation = ? AND week_id = ? AND org_id = ? AND metric = ?`)
    .get(gen.id, isoWeekLabel(date), org_id, metric);
  const rollupOrgs = db.prepare('SELECT * FROM generation_orgs WHERE generation = ?').all(gen.id);
  const index = buildOrgIndex(rollupOrgs);
  const ancestors = index.ancestorsOf(org_id);
  const rollups = ancestors.length
    ? db.prepare(`SELECT org_id, value, event_count, member_orgs FROM weekly_org_rollup
                  WHERE generation = ? AND week_id = ? AND metric = ?
                    AND org_id IN (${ancestors.map(() => '?').join(',')})`)
      .all(gen.id, isoWeekLabel(date), metric, ...ancestors)
    : [];
  return { generation: gen.id, cell: cell ?? null, weekly: weekly ?? null, events, versions, rollups };
}
