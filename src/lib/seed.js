// Deterministic fictional seed data. Fixed seed -> identical numbers on every
// run, which lets tests assert exact values.
import { transaction, nowIso } from './db.js';
import { nextSeq } from './seq.js';
import { addDays, dateRange, isoWeekday } from './time.js';

/** Mulberry32: tiny deterministic PRNG. */
export function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const ORG_SEED = [
  { org_id: 'acme', name: 'Acme Holdings', parent_id: null, level: 'group' },
  { org_id: 'acme-north', name: 'Acme North', parent_id: 'acme', level: 'region' },
  { org_id: 'acme-south', name: 'Acme South', parent_id: 'acme', level: 'region' },
  { org_id: 'acme-north-oslo', name: 'Oslo Branch', parent_id: 'acme-north', level: 'branch' },
  { org_id: 'acme-north-bergen', name: 'Bergen Branch', parent_id: 'acme-north', level: 'branch' },
  { org_id: 'acme-south-lisbon', name: 'Lisbon Branch', parent_id: 'acme-south', level: 'branch' },
  { org_id: 'acme-south-porto', name: 'Porto Branch', parent_id: 'acme-south', level: 'branch' },
];

/** Leaf orgs actually receive events. */
export const LEAF_ORGS = ['acme-north-oslo', 'acme-north-bergen', 'acme-south-lisbon', 'acme-south-porto'];

export const METRIC_MIX = [
  { metric: 'signups', base: 40, spread: 25, weekend: 0.55 },
  { metric: 'revenue', base: 1800, spread: 900, weekend: 0.8 },
  { metric: 'sessions', base: 900, spread: 400, weekend: 0.7 },
];

/** Insert the org tree (idempotent). */
export function seedOrgs(db) {
  return transaction(db, () => {
    const at = nowIso();
    for (const o of ORG_SEED) {
      const exists = db.prepare('SELECT 1 AS ok FROM orgs WHERE org_id = ?').get(o.org_id);
      if (exists) continue;
      db.prepare('INSERT INTO orgs(org_id, name, parent_id, level) VALUES (?,?,?,?)')
        .run(o.org_id, o.name, o.parent_id, o.level);
      const seq = nextSeq(db, 'org');
      db.prepare('INSERT INTO org_history(org_id, name, parent_id, level, recorded_at, seq) VALUES (?,?,?,?,?,?)')
        .run(o.org_id, o.name, o.parent_id, o.level, at, seq);
    }
    return ORG_SEED.length;
  });
}

/**
 * Generate deterministic fictional events across a date window.
 * Returns the events as plain objects (not yet inserted), so the caller can
 * feed them through the normal ingest path.
 */
export function generateEvents({ from, to, seed = 20261120 } = {}) {
  const rand = rng(seed);
  const events = [];
  let n = 0;
  for (const date of dateRange(from, to)) {
    const weekend = isoWeekday(date) >= 6;
    for (const org of LEAF_ORGS) {
      for (const m of METRIC_MIX) {
        // 0-3 events per org/metric/day, shaped by weekday.
        const rolls = Math.floor(rand() * 3.4) + (weekend ? 0 : 1);
        for (let i = 0; i < rolls; i++) {
          const noise = 0.5 + rand() * 1.5;
          let amount = m.base * noise * (weekend ? m.weekend : 1);
          amount = m.metric === 'revenue' ? Math.round(amount * 100) / 100 : Math.round(amount);
          n += 1;
          events.push({
            event_id: `evt_${date.replaceAll('-', '')}_${org}_${m.metric}_${i}`,
            occurred_on: date,
            org_id: org,
            metric: m.metric,
            amount,
            payload: { channel: rand() < 0.5 ? 'direct' : 'referral', fictional: true },
          });
        }
      }
    }
  }
  return events;
}

export function seedRange({ weeks = 6, end } = {}) {
  const endDate = end ?? addDays(new Date().toISOString().slice(0, 10), -1);
  // Snap back to the previous Sunday so the window ends on a full week.
  const start = addDays(endDate, -(weeks * 7 - 1));
  return { from: start, to: endDate };
}
