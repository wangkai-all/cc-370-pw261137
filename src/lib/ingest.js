import { randomUUID } from 'node:crypto';
import { transaction, nowIso } from './db.js';
import { nextSeq } from './seq.js';
import { assertDate, isDateString } from './time.js';
import { ValidationError, NotFoundError, ConflictError } from './errors.js';

export const METRICS = ['signups', 'revenue', 'sessions'];

function validateEvent(raw, index) {
  const where = `events[${index}]`;
  if (!raw || typeof raw !== 'object') throw new ValidationError(`${where} must be an object`);
  const eventId = raw.event_id ?? raw.eventId;
  if (typeof eventId !== 'string' || !eventId.trim()) {
    throw new ValidationError(`${where}.event_id is required`, { index });
  }
  if (!isDateString(raw.occurred_on ?? raw.occurredOn)) {
    throw new ValidationError(`${where}.occurred_on must be YYYY-MM-DD`, { index });
  }
  const orgId = raw.org_id ?? raw.orgId;
  if (typeof orgId !== 'string' || !orgId.trim()) {
    throw new ValidationError(`${where}.org_id is required`, { index });
  }
  const metric = raw.metric;
  if (typeof metric !== 'string' || !metric.trim()) {
    throw new ValidationError(`${where}.metric is required`, { index });
  }
  const amount = Number(raw.amount);
  if (!Number.isFinite(amount)) {
    throw new ValidationError(`${where}.amount must be a finite number`, { index });
  }
  return {
    eventId: eventId.trim(),
    occurredOn: raw.occurred_on ?? raw.occurredOn,
    orgId: orgId.trim(),
    metric: metric.trim(),
    amount,
    payload: raw.payload && typeof raw.payload === 'object' ? raw.payload : {},
  };
}

function insertVersion(db, { eventId, version, occurredOn, orgId, metric, amount, payload, isCorrection, correctionId, reason, seq, recordedAt }) {
  db.prepare(`INSERT INTO event_versions
      (event_id, version, occurred_on, org_id, metric, amount, payload, is_correction, correction_id, reason, recorded_at, seq)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(eventId, version, occurredOn, orgId, metric, amount, JSON.stringify(payload), isCorrection ? 1 : 0,
      correctionId ?? null, reason ?? null, recordedAt, seq);
}

/**
 * Ingest brand-new events. Event ids that already exist are rejected rather
 * than silently updated: a change to an existing event must go through
 * /corrections so its lineage and history are preserved.
 */
export function ingestEvents(db, body, { actor = 'ingest', requestId = randomUUID() } = {}) {
  const list = Array.isArray(body) ? body : body?.events;
  if (!Array.isArray(list) || list.length === 0) {
    throw new ValidationError('body must contain a non-empty "events" array');
  }
  const validated = list.map(validateEvent);

  const seen = new Set();
  for (const e of validated) {
    if (seen.has(e.eventId)) throw new ValidationError(`duplicate event_id "${e.eventId}" inside one request`);
    seen.add(e.eventId);
  }

  return transaction(db, () => {
    const at = nowIso();
    const rejected = [];
    const acceptedIds = [];
    const knownOrgs = new Set(db.prepare('SELECT org_id FROM orgs').all().map((r) => r.org_id));

    for (const e of validated) {
      const existing = db.prepare('SELECT event_id FROM events WHERE event_id = ?').get(e.eventId);
      if (existing) {
        rejected.push({ event_id: e.eventId, error: 'already exists; use a correction' });
        continue;
      }
      if (!knownOrgs.has(e.orgId)) {
        rejected.push({ event_id: e.eventId, error: `unknown org_id "${e.orgId}"` });
        continue;
      }
      const seq = nextSeq(db, 'event');
      db.prepare(`INSERT INTO events(event_id, occurred_on, org_id, metric, amount, payload, received_at, seq)
                  VALUES (?,?,?,?,?,?,?,?)`)
        .run(e.eventId, e.occurredOn, e.orgId, e.metric, e.amount, JSON.stringify(e.payload), at, seq);
      insertVersion(db, {
        eventId: e.eventId, version: 1, occurredOn: e.occurredOn, orgId: e.orgId,
        metric: e.metric, amount: e.amount, payload: e.payload, isCorrection: false,
        correctionId: null, reason: null, seq, recordedAt: at,
      });
      acceptedIds.push(e.eventId);
    }

    const changeId = `chg_${randomUUID()}`;
    let changeCreated = false;
    if (acceptedIds.length > 0) {
      db.prepare(`INSERT INTO change_requests(id, type, payload, summary, status, created_at, requested_by, strategy)
                  VALUES (?,?,?,?,?,?,?,?)`)
        .run(changeId, 'ingest', JSON.stringify({ eventIds: acceptedIds }), `ingest ${acceptedIds.length} event(s)`,
          'pending', at, actor, 'auto');
      changeCreated = true;
    }

    const status = rejected.length === 0 ? 'accepted' : acceptedIds.length === 0 ? 'rejected' : 'partial';
    db.prepare(`INSERT INTO ingest_log(request_id, at, kind, accepted, rejected, status, detail)
                VALUES (?,?,?,?,?,?,?)`)
      .run(requestId, at, 'events', acceptedIds.length, rejected.length, status,
        JSON.stringify({ rejected, acceptedIds, changeId: changeCreated ? changeId : null }));

    return {
      request_id: requestId,
      accepted: acceptedIds.length,
      rejected: rejected.length,
      status,
      change_id: changeCreated ? changeId : null,
      conflicts: rejected,
    };
  });
}

/**
 * Correct one historical event. The payload may change amount, metric, org
 * attribution and/or the business date. Changing the date can move the event
 * between weeks; changing the org can move it between org partitions — both
 * are surfaced explicitly so the scheduler can widen the recompute set.
 */
export function submitCorrection(db, body, { actor = 'ingest', requestId = randomUUID() } = {}) {
  if (!body || typeof body !== 'object') throw new ValidationError('correction body required');
  const eventId = body.event_id ?? body.eventId;
  if (typeof eventId !== 'string' || !eventId) throw new ValidationError('event_id is required');
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (!reason) throw new ValidationError('reason is required for a correction');

  return transaction(db, () => {
    const at = nowIso();
    const current = db.prepare('SELECT * FROM event_versions WHERE event_id = ? ORDER BY version DESC LIMIT 1')
      .get(eventId);
    if (!current) throw new NotFoundError(`unknown event_id "${eventId}"`);
    if (!db.prepare('SELECT 1 AS ok FROM events WHERE event_id = ?').get(eventId)) {
      throw new NotFoundError(`unknown event_id "${eventId}"`);
    }

    const next = {
      occurredOn: body.occurred_on ?? body.occurredOn ?? current.occurred_on,
      orgId: body.org_id ?? body.orgId ?? current.org_id,
      metric: body.metric ?? current.metric,
      amount: body.amount !== undefined ? Number(body.amount) : current.amount,
      payload: body.payload && typeof body.payload === 'object'
        ? { ...JSON.parse(current.payload || '{}'), ...body.payload }
        : JSON.parse(current.payload || '{}'),
    };
    assertDate(next.occurredOn, 'occurred_on');
    if (!Number.isFinite(next.amount)) throw new ValidationError('amount must be a finite number');
    if (!db.prepare('SELECT 1 AS ok FROM orgs WHERE org_id = ?').get(next.orgId)) {
      throw new ValidationError(`unknown org_id "${next.orgId}"`);
    }

    const changes = {};
    for (const [From, to] of [
      ['occurred_on', next.occurredOn], ['org_id', next.orgId],
      ['metric', next.metric], ['amount', next.amount],
    ]) {
      if (current[From] !== to) changes[From] = { from: current[From], to };
    }
    if (current.org_id === next.orgId && !body.org_valid_from) { /* org unchanged */ }

    const correctionId = `cor_${randomUUID()}`;
    const seq = nextSeq(db, 'event');
    insertVersion(db, {
      eventId, version: current.version + 1, occurredOn: next.occurredOn, orgId: next.orgId,
      metric: next.metric, amount: next.amount, payload: next.payload, isCorrection: true,
      correctionId, reason, seq, recordedAt: at,
    });

    // A correction may also revise attribution for a period. Record the
    // attribution change as its own input so the scheduler can widen scope
    // independently of value changes.
    let assignment = null;
    if (body.org_valid_from) {
      assertDate(body.org_valid_from, 'org_valid_from');
      const validTo = body.org_valid_to ?? null;
      if (validTo) assertDate(validTo, 'org_valid_to');
      if (validTo && validTo < body.org_valid_from) {
        throw new ValidationError('org_valid_to must not precede org_valid_from');
      }
      const prior = db.prepare(`SELECT * FROM event_org_assignments WHERE event_id = ? AND valid_to IS NULL
                                ORDER BY valid_from DESC LIMIT 1`).get(eventId);
      if (prior) {
        db.prepare('UPDATE event_org_assignments SET valid_to = ? WHERE id = ?')
          .run(body.org_valid_from, prior.id);
      }
      const assignSeq = nextSeq(db, 'assignment');
      db.prepare(`INSERT INTO event_org_assignments(event_id, org_id, valid_from, valid_to, reason, recorded_at, seq)
                  VALUES (?,?,?,?,?,?,?)`)
        .run(eventId, next.orgId, body.org_valid_from, validTo, reason, at, assignSeq);
      assignment = { event_id: eventId, org_id: next.orgId, valid_from: body.org_valid_from, valid_to: validTo };
    }

    const changeId = `chg_${randomUUID()}`;
    const summary = `${eventId} v${current.version}->v${current.version + 1}`;
    db.prepare(`INSERT INTO change_requests(id, type, payload, summary, status, created_at, requested_by, strategy)
                VALUES (?,?,?,?,?,?,?,?)`)
      .run(changeId, 'correction', JSON.stringify({
        eventId, correctionId, previous: {
          occurred_on: current.occurred_on, org_id: current.org_id,
          metric: current.metric, amount: current.amount, version: current.version,
        },
        current: { occurred_on: next.occurredOn, org_id: next.orgId, metric: next.metric, amount: next.amount, version: current.version + 1 },
        changes, assignment,
      }), summary, 'pending', at, actor, 'auto');

    db.prepare(`INSERT INTO ingest_log(request_id, at, kind, accepted, rejected, status, detail)
                VALUES (?,?,?,?,?,?,?)`)
      .run(requestId, at, 'correction', 1, 0, 'accepted',
        JSON.stringify({ eventId, correctionId, changeId, changes }));

    return {
      request_id: requestId,
      status: 'accepted',
      change_id: changeId,
      correction_id: correctionId,
      event_id: eventId,
      from_version: current.version,
      to_version: current.version + 1,
      changes,
      org_assignment: assignment,
      moved_week: changes.occurred_on
        ? { from: changes.occurred_on.from, to: changes.occurred_on.to }
        : null,
    };
  });
}

/**
 * Pure attribution change: move an event's org for a validity window without
 * altering its value. Used for "organization attribution revisions".
 */
export function submitReassignment(db, body, { actor = 'ingest', requestId = randomUUID() } = {}) {
  const eventId = body?.event_id ?? body?.eventId;
  if (typeof eventId !== 'string' || !eventId) throw new ValidationError('event_id is required');
  const orgId = body?.org_id ?? body?.orgId;
  if (typeof orgId !== 'string' || !orgId) throw new ValidationError('org_id is required');
  const validFrom = body.valid_from;
  assertDate(validFrom, 'valid_from');
  const validTo = body.valid_to ?? null;
  if (validTo) assertDate(validTo, 'valid_to');
  if (validTo && validTo < validFrom) throw new ValidationError('valid_to must not precede valid_from');
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (!reason) throw new ValidationError('reason is required for a reassignment');

  return transaction(db, () => {
    const at = nowIso();
    if (!db.prepare('SELECT 1 AS ok FROM events WHERE event_id = ?').get(eventId)) {
      throw new NotFoundError(`unknown event_id "${eventId}"`);
    }
    if (!db.prepare('SELECT 1 AS ok FROM orgs WHERE org_id = ?').get(orgId)) {
      throw new ValidationError(`unknown org_id "${orgId}"`);
    }
    // Overlapping open-ended assignment for the same event: close it at validFrom.
    const prior = db.prepare(`SELECT * FROM event_org_assignments
                              WHERE event_id = ? AND (valid_to IS NULL OR valid_to >= ?)
                              ORDER BY valid_from ASC`).all(eventId, validFrom);
    for (const p of prior) {
      if (p.valid_from >= validFrom && (validTo === null || p.valid_from <= validTo)) {
        throw new ConflictError(
          `an assignment for ${eventId} starting ${p.valid_from} already covers this window`,
          { existing: { org_id: p.org_id, valid_from: p.valid_from, valid_to: p.valid_to } },
        );
      }
      if (p.valid_from < validFrom && (p.valid_to === null || p.valid_to >= validFrom)) {
        db.prepare('UPDATE event_org_assignments SET valid_to = ? WHERE id = ?').run(validFrom, p.id);
      }
    }
    const seq = nextSeq(db, 'assignment');
    db.prepare(`INSERT INTO event_org_assignments(event_id, org_id, valid_from, valid_to, reason, recorded_at, seq)
                VALUES (?,?,?,?,?,?,?)`)
      .run(eventId, orgId, validFrom, validTo, reason, at, seq);

    const changeId = `chg_${randomUUID()}`;
    db.prepare(`INSERT INTO change_requests(id, type, payload, summary, status, created_at, requested_by, strategy)
                VALUES (?,?,?,?,?,?,?,?)`)
      .run(changeId, 'reassign', JSON.stringify({ eventId, orgId, validFrom, validTo, reason }),
        `reassign ${eventId} to ${orgId}`, 'pending', at, actor, 'auto');
    db.prepare(`INSERT INTO ingest_log(request_id, at, kind, accepted, rejected, status, detail)
                VALUES (?,?,?,?,?,?,?)`)
      .run(requestId, at, 'reassign', 1, 0, 'accepted', JSON.stringify({ eventId, changeId }));

    return { request_id: requestId, status: 'accepted', change_id: changeId, event_id: eventId, org_id: orgId, valid_from: validFrom, valid_to: validTo };
  });
}

/** Move an org (and by implication its whole subtree) under a new parent. */
export function submitReparent(db, body, { actor = 'ingest', requestId = randomUUID() } = {}) {
  const orgId = body?.org_id ?? body?.orgId;
  if (typeof orgId !== 'string' || !orgId) throw new ValidationError('org_id is required');
  const parentId = body.parent_id ?? body.parentId ?? null;
  if (parentId !== null && (typeof parentId !== 'string' || !parentId)) {
    throw new ValidationError('parent_id must be a string or null');
  }
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (!reason) throw new ValidationError('reason is required for a reparent');

  return transaction(db, () => {
    const at = nowIso();
    const org = db.prepare('SELECT * FROM orgs WHERE org_id = ?').get(orgId);
    if (!org) throw new NotFoundError(`unknown org_id "${orgId}"`);
    if (parentId === orgId) throw new ValidationError('an org cannot be its own parent');
    if (parentId && !db.prepare('SELECT 1 AS ok FROM orgs WHERE org_id = ?').get(parentId)) {
      throw new ValidationError(`unknown parent_id "${parentId}"`);
    }
    if (parentId && isDescendant(db, parentId, orgId)) {
      throw new ValidationError(`reparenting ${orgId} under ${parentId} would create a cycle`);
    }
    db.prepare('UPDATE orgs SET parent_id = ? WHERE org_id = ?').run(parentId, orgId);
    const seq = nextSeq(db, 'org');
    db.prepare(`INSERT INTO org_history(org_id, name, parent_id, level, recorded_at, seq)
                VALUES (?,?,?,?,?,?)`)
      .run(org.org_id, org.name, parentId, org.level, at, seq);

    const changeId = `chg_${randomUUID()}`;
    db.prepare(`INSERT INTO change_requests(id, type, payload, summary, status, created_at, requested_by, strategy)
                VALUES (?,?,?,?,?,?,?,?)`)
      .run(changeId, 'reparent', JSON.stringify({ orgId, parentId, previous: org.parent_id, reason }),
        `reparent ${orgId} -> ${parentId ?? '(root)'}`, 'pending', at, actor, 'auto');
    db.prepare(`INSERT INTO ingest_log(request_id, at, kind, accepted, rejected, status, detail)
                VALUES (?,?,?,?,?,?,?)`)
      .run(requestId, at, 'reparent', 1, 0, 'accepted', JSON.stringify({ orgId, changeId }));

    return { request_id: requestId, status: 'accepted', change_id: changeId, org_id: orgId, parent_id: parentId };
  });
}

function isDescendant(db, candidate, ancestor) {
  let cur = candidate;
  const seen = new Set();
  while (cur) {
    if (cur === ancestor) return true;
    if (seen.has(cur)) return false;
    seen.add(cur);
    const row = db.prepare('SELECT parent_id FROM orgs WHERE org_id = ?').get(cur);
    cur = row?.parent_id ?? null;
  }
  return false;
}
