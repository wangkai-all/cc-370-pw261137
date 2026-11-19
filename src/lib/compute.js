// Pure, deterministic aggregation. Nothing here touches the database, which
// makes it directly testable and safe to use both when building a generation
// and when independently verifying one.
import { createHash } from 'node:crypto';
import { isoWeekLabel, weekStart } from './time.js';

/** Round to 6 decimals so floating-point noise never creates spurious diffs. */
export function money(n) {
  return Math.round(n * 1e6) / 1e6;
}

/**
 * Aggregate events into daily rows.
 * events: [{ occurred_on, effective_org, metric, amount }]
 * Returns Map key = `${date}|${org}|${metric}`.
 */
export function aggregateDaily(events) {
  const out = new Map();
  for (const e of events) {
    const key = `${e.occurred_on}|${e.effective_org}|${e.metric}`;
    const row = out.get(key) ?? { date: e.occurred_on, org_id: e.effective_org, metric: e.metric, value: 0, event_count: 0 };
    row.value += e.amount;
    row.event_count += 1;
    out.set(key, row);
  }
  for (const row of out.values()) row.value = money(row.value);
  return out;
}

/** Derive weekly rows from daily rows. Key = `${week_id}|${org}|${metric}`. */
export function aggregateWeekly(dailyRows) {
  const out = new Map();
  for (const d of dailyRows) {
    const weekId = isoWeekLabel(d.date);
    const key = `${weekId}|${d.org_id}|${d.metric}`;
    const row = out.get(key) ?? {
      week_id: weekId,
      week_start: weekStart(d.date),
      org_id: d.org_id,
      metric: d.metric,
      value: 0,
      event_count: 0,
      days: new Set(),
    };
    row.value += d.value;
    row.event_count += d.event_count ?? 0;
    row.days.add(d.date);
    out.set(key, row);
  }
  for (const row of out.values()) {
    row.value = money(row.value);
    row.day_count = row.days.size;
    delete row.days;
  }
  return out;
}

/** Build child -> parent maps from org rows. */
export function buildOrgIndex(orgRows) {
  const parentOf = new Map();
  const childrenOf = new Map();
  const byId = new Map();
  for (const o of orgRows) {
    byId.set(o.org_id, o);
    parentOf.set(o.org_id, o.parent_id ?? null);
    if (o.parent_id) {
      if (!childrenOf.has(o.parent_id)) childrenOf.set(o.parent_id, []);
      childrenOf.get(o.parent_id).push(o.org_id);
    }
  }
  const ancestorsOf = (orgId) => {
    const chain = [];
    let cur = parentOf.get(orgId) ?? null;
    const seen = new Set([orgId]);
    while (cur && !seen.has(cur)) {
      chain.push(cur);
      seen.add(cur);
      cur = parentOf.get(cur) ?? null;
    }
    return chain;
  };
  const subtreeOf = (orgId) => {
    const out = [];
    const stack = [orgId];
    const seen = new Set();
    while (stack.length) {
      const cur = stack.pop();
      if (seen.has(cur)) continue;
      seen.add(cur);
      out.push(cur);
      for (const child of childrenOf.get(cur) ?? []) stack.push(child);
    }
    return out.sort();
  };
  return { parentOf, childrenOf, byId, ancestorsOf, subtreeOf };
}

/**
 * Org comparison table. For every (week, org) we sum the weekly rows of the
 * org's whole subtree, so a manager sees the total for what they own.
 * Key = `${week_id}|${org}|${metric}`.
 */
export function aggregateRollup(weeklyRows, orgRows) {
  const index = buildOrgIndex(orgRows);
  const orgs = [...index.byId.keys()].sort();

  // week -> org -> metric -> { value, count }
  const byWeek = new Map();
  for (const w of weeklyRows) {
    if (!byWeek.has(w.week_id)) byWeek.set(w.week_id, new Map());
    const perOrg = byWeek.get(w.week_id);
    if (!perOrg.has(w.org_id)) perOrg.set(w.org_id, new Map());
    perOrg.get(w.org_id).set(w.metric, { value: w.value, count: w.event_count });
  }

  const out = new Map();
  for (const [weekId, perOrg] of byWeek) {
    const metrics = new Set();
    for (const m of perOrg.values()) for (const k of m.keys()) metrics.add(k);
    const weekStartDate = weeklyRows.find((w) => w.week_id === weekId)?.week_start ?? null;

    for (const orgId of orgs) {
      const members = index.subtreeOf(orgId);
      for (const metric of metrics) {
        let value = 0;
        let count = 0;
        const contributing = [];
        for (const member of members) {
          const cell = perOrg.get(member)?.get(metric);
          if (cell) {
            value += cell.value;
            count += cell.count;
            contributing.push(member);
          }
        }
        if (contributing.length === 0) continue;
        out.set(`${weekId}|${orgId}|${metric}`, {
          week_id: weekId,
          week_start: weekStartDate,
          org_id: orgId,
          metric,
          value: money(value),
          event_count: count,
          member_orgs: contributing.sort(),
        });
      }
    }
  }
  return out;
}

/** Canonical, order-independent digest of a set of rows. */
export function digestRows(rows) {
  const canonical = rows.map((r) => JSON.stringify(r)).sort().join('\n');
  return createHash('sha256').update(canonical).digest('hex');
}
