// Builds the effective event snapshot for a generation at pinned input
// boundaries, so a later correction can never leak into an older generation.
import { weekIdOf } from './time.js';

/**
 * Latest version of every event whose seq <= bound.
 * Returns Map<event_id, versionRow>.
 */
export function latestVersionsAt(db, eventSeqBound) {
  const rows = db.prepare(`
    SELECT v.* FROM event_versions v
    JOIN (SELECT event_id, MAX(version) AS version
          FROM event_versions WHERE seq <= ? GROUP BY event_id) latest
      ON latest.event_id = v.event_id AND latest.version = v.version
    WHERE v.seq <= ?
  `).all(eventSeqBound, eventSeqBound);
  return new Map(rows.map((r) => [r.event_id, r]));
}

/** All attribution assignments whose seq <= bound, grouped by event_id. */
export function assignmentsAt(db, assignSeqBound) {
  const rows = db.prepare(`
    SELECT * FROM event_org_assignments WHERE seq <= ? ORDER BY event_id, valid_from ASC, seq ASC
  `).all(assignSeqBound);
  const grouped = new Map();
  for (const r of rows) {
    if (!grouped.has(r.event_id)) grouped.set(r.event_id, []);
    grouped.get(r.event_id).push(r);
  }
  return grouped;
}

/**
 * Effective org = the assignment covering the event's business date, taking
 * the most recently recorded one when windows overlap. Falls back to the
 * version's own org when no assignment covers the date.
 */
export function effectiveOrg(versionRow, assignments) {
  if (!assignments?.length) return versionRow.org_id;
  let best = null;
  for (const a of assignments) {
    if (a.valid_from <= versionRow.occurred_on && (a.valid_to === null || versionRow.occurred_on <= a.valid_to)) {
      if (!best || a.seq > best.seq || (a.seq === best.seq && a.valid_from > best.valid_from)) best = a;
    }
  }
  return best ? best.org_id : versionRow.org_id;
}

export function orgsAt(db, orgSeqBound) {
  // Latest history row per org at the bound; orgs never deleted, so use
  // history when present, else the current row.
  const hist = db.prepare(`
    SELECT h.* FROM org_history h
    JOIN (SELECT org_id, MAX(seq) AS seq FROM org_history WHERE seq <= ? GROUP BY org_id) latest
      ON latest.org_id = h.org_id AND latest.seq = h.seq
  `).all(orgSeqBound);
  const histByOrg = new Map(hist.map((h) => [h.org_id, h]));
  const current = db.prepare('SELECT * FROM orgs').all();
  return current.map((o) => {
    const h = histByOrg.get(o.org_id);
    return h
      ? { org_id: o.org_id, name: h.name, parent_id: h.parent_id ?? null, level: h.level }
      : { org_id: o.org_id, name: o.name, parent_id: o.parent_id ?? null, level: o.level };
  });
}

/** The full effective event snapshot: one row per event with effective org. */
export function snapshotEvents(db, { eventSeq, assignSeq }) {
  const versions = latestVersionsAt(db, eventSeq);
  const assigns = assignmentsAt(db, assignSeq);
  const out = [];
  for (const v of versions.values()) {
    out.push({
      event_id: v.event_id,
      version: v.version,
      occurred_on: v.occurred_on,
      org_id: v.org_id,
      effective_org: effectiveOrg(v, assigns.get(v.event_id)),
      metric: v.metric,
      amount: v.amount,
    });
  }
  out.sort((a, b) => (a.event_id < b.event_id ? -1 : a.event_id > b.event_id ? 1 : 0));
  return out;
}

/** Convenience: which week does a business date fall in. */
export function weekOf(date) {
  return weekIdOf(date).weekId;
}
