// Publishing freezes one generation's layers into report_* tables. Reports are
// append-only: an older report's numbers can never change, and a report is
// always internally consistent because it is copied inside one transaction
// from a single generation that was itself verified as a whole.
import { transaction, nowIso } from './db.js';
import { digestRows } from './compute.js';
import { ConflictError, NotFoundError, ValidationError } from './errors.js';

function reportRows(db, gen, table, cols) {
  return db.prepare(`SELECT ${cols} FROM ${table} WHERE generation = ?`).all(gen);
}

/**
 * Publish one generation as a report. Multiple generations can be published
 * together inside a release, but each report row is still tagged with the
 * single generation it came from — the browser shows "the latest report"
 * only via the newest report id, never by mixing layers.
 */
export function publishGeneration(db, generationId, { title, actor = 'publisher', releaseId = null } = {}) {
  return transaction(db, () => {
    const g = db.prepare('SELECT * FROM generations WHERE id = ?').get(generationId);
    if (!g) throw new NotFoundError(`unknown generation ${generationId}`);
    if (g.status !== 'approved') {
      throw new ConflictError(`generation ${generationId} is ${g.status}; only approved generations can be published`);
    }
    const verification = g.verification ? JSON.parse(g.verification) : null;
    if (!verification?.ok) throw new ConflictError('refusing to publish a generation that did not verify');

    const daily = reportRows(db, generationId, 'daily_metrics', 'date, org_id, metric, value, event_count');
    const weekly = reportRows(db, generationId, 'weekly_metrics', 'week_id, org_id, metric, value, event_count, day_count');
    const rollup = db.prepare('SELECT week_id, org_id, metric, value, event_count, member_orgs FROM weekly_org_rollup WHERE generation = ?').all(generationId);
    const orgs = db.prepare('SELECT org_id, name, parent_id, level FROM generation_orgs WHERE generation = ?').all(generationId);

    if (daily.length === 0 && weekly.length === 0) {
      throw new ConflictError('generation has no metrics to publish');
    }

    const dates = daily.map((r) => r.date).sort();
    const weeks = weekly.map((r) => r.week_id).sort();
    const summary = {
      generation: generationId,
      digest: g.digest,
      daily_rows: daily.length,
      weekly_rows: weekly.length,
      rollup_rows: rollup.length,
      date_from: dates[0] ?? null,
      date_to: dates.at(-1) ?? null,
      weeks,
      totals: totalsOf(daily),
    };

    const info = db.prepare(`INSERT INTO reports(generation, release_id, title, published_at, published_by, date_from, date_to, digest, summary)
                             VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(generationId, releaseId, title ?? `Metrics generation ${generationId}`, nowIso(), actor,
        summary.date_from ?? '', summary.date_to ?? '', g.digest, JSON.stringify(summary));
    const reportId = Number(info.lastInsertRowid);

    const sd = db.prepare(`INSERT INTO report_daily(report_id, date, org_id, metric, value, event_count, generation)
                           VALUES (?,?,?,?,?,?,?)`);
    for (const r of daily) sd.run(reportId, r.date, r.org_id, r.metric, r.value, r.event_count, generationId);
    const sw = db.prepare(`INSERT INTO report_weekly(report_id, week_id, org_id, metric, value, event_count, day_count, generation)
                           VALUES (?,?,?,?,?,?,?,?)`);
    for (const r of weekly) sw.run(reportId, r.week_id, r.org_id, r.metric, r.value, r.event_count, r.day_count, generationId);
    const sr = db.prepare(`INSERT INTO report_rollup(report_id, week_id, org_id, metric, value, event_count, member_orgs, generation)
                           VALUES (?,?,?,?,?,?,?,?)`);
    for (const r of rollup) sr.run(reportId, r.week_id, r.org_id, r.metric, r.value, r.event_count, r.member_orgs, generationId);
    const so = db.prepare('INSERT INTO report_orgs(report_id, org_id, name, parent_id, level) VALUES (?,?,?,?,?)');
    for (const o of orgs) so.run(reportId, o.org_id, o.name, o.parent_id, o.level);

    return { report_id: reportId, generation: generationId, summary };
  });
}

function totalsOf(rows) {
  const totals = {};
  for (const r of rows) totals[r.metric] = Math.round(((totals[r.metric] ?? 0) + r.value) * 1e6) / 1e6;
  return totals;
}

/**
 * Approve a group of ready generations and publish them as one release. The
 * "approve the whole group" flow: every generation must be ready and verified,
 * otherwise nothing is published.
 */
export function publishRelease(db, generationIds, { actor = 'approver', note = '', title } = {}) {
  if (!Array.isArray(generationIds) || generationIds.length === 0) {
    throw new ValidationError('generation_ids must be a non-empty array');
  }
  const unique = [...new Set(generationIds.map(Number))];
  return transaction(db, () => {
    const gens = unique.map((id) => db.prepare('SELECT * FROM generations WHERE id = ?').get(id));
    for (const [i, g] of gens.entries()) {
      if (!g) throw new NotFoundError(`unknown generation ${unique[i]}`);
      if (!['ready', 'approved'].includes(g.status)) {
        throw new ConflictError(`generation ${g.id} is ${g.status}; every generation in a release must be ready or approved`);
      }
      const v = g.verification ? JSON.parse(g.verification) : null;
      if (!v?.ok) throw new ConflictError(`generation ${g.id} did not verify`);
    }
    const at = nowIso();
    const rel = db.prepare("INSERT INTO releases(status, created_at, approved_at, approved_by, note) VALUES ('approved',?,?,?,?)")
      .run(at, at, actor, note);
    const releaseId = Number(rel.lastInsertRowid);
    for (const g of gens) {
      db.prepare("UPDATE generations SET status='approved', approved_at=?, approved_by=? WHERE id=?")
        .run(at, actor, g.id);
    }
    const reports = gens.map((g) => publishGeneration(db, g.id, {
      title: title ? `${title} — generation ${g.id}` : undefined, actor, releaseId,
    }));
    db.prepare("UPDATE releases SET status='published' WHERE id=?").run(releaseId);
    return { release_id: releaseId, report_ids: reports.map((r) => r.report_id), reports };
  });
}

export function listReports(db) {
  return db.prepare('SELECT * FROM reports ORDER BY id DESC').all().map((r) => ({
    ...r, summary: JSON.parse(r.summary || '{}'),
  }));
}

export function getReport(db, reportId) {
  const report = db.prepare('SELECT * FROM reports WHERE id = ?').get(reportId);
  if (!report) throw new NotFoundError(`unknown report ${reportId}`);
  const daily = db.prepare('SELECT * FROM report_daily WHERE report_id = ? ORDER BY date, org_id, metric').all(reportId);
  const weekly = db.prepare('SELECT * FROM report_weekly WHERE report_id = ? ORDER BY week_id, org_id, metric').all(reportId);
  const rollup = db.prepare('SELECT * FROM report_rollup WHERE report_id = ? ORDER BY week_id, org_id, metric').all(reportId);
  const orgs = db.prepare('SELECT * FROM report_orgs WHERE report_id = ? ORDER BY org_id').all(reportId);
  return {
    ...report,
    summary: JSON.parse(report.summary || '{}'),
    daily, weekly, rollup, orgs,
  };
}

/**
 * Confirm a stored report is byte-for-byte consistent with the generation it
 * pins, i.e. every row still belongs to that one generation.
 */
export function verifyReport(db, reportId) {
  const report = db.prepare('SELECT * FROM reports WHERE id = ?').get(reportId);
  if (!report) throw new NotFoundError(`unknown report ${reportId}`);
  const gens = new Set();
  for (const table of ['report_daily', 'report_weekly', 'report_rollup']) {
    for (const r of db.prepare(`SELECT DISTINCT generation FROM ${table} WHERE report_id = ?`).all(reportId)) {
      gens.add(r.generation);
    }
  }
  const mixed = gens.size !== 1 || !gens.has(report.generation);
  return { report_id: reportId, ok: !mixed, generations: [...gens], pinned: report.generation };
}
