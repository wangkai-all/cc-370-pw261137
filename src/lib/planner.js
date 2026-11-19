// The planner decides what must be recomputed for a set of change requests,
// and whether each layer uses a local incremental update or a rebuild of the
// affected partition. Two facts drive it:
//
//   1. A change to a daily partition invalidates its week's weekly row AND the
//      org-rollup rows of every ancestor of the affected orgs.
//   2. A date change can move an event between weeks, and an org change can
//      move it between org subtrees — both sides of the move must be refreshed.
import { weekIdOf } from './time.js';
import { buildOrgIndex } from './compute.js';

/**
 * @param {object} args
 * @param {Array}  args.changes   change_requests rows (pending)
 * @param {Array}  args.previousEvents  effective snapshot rows of the base generation
 * @param {Array}  args.nextEvents      effective snapshot rows including the changes
 * @param {Array}  args.orgs            org rows at the new generation
 * @param {Set}    args.previouslyCoveredDates  dates already materialized
 * @param {number} args.previouslyCoveredThroughWeek  highest covered week id
 */
export function planRecompute({ changes, previousEvents, nextEvents, orgs, allWeeks = [], allDates = [] }) {
  const prevById = new Map(previousEvents.map((e) => [e.event_id, e]));
  const nextById = new Map(nextEvents.map((e) => [e.event_id, e]));

  const impactedEventIds = new Set();
  const corrections = [];
  let structuralChange = false; // org tree changes require a full rebuild
  let fullRebuild = false;

  for (const change of changes) {
    const payload = typeof change.payload === 'string' ? JSON.parse(change.payload) : change.payload;
    if (change.type === 'ingest' || change.type === 'correction') {
      const ids = change.type === 'ingest' ? payload.eventIds : [payload.eventId];
      for (const id of ids) impactedEventIds.add(id);
      if (change.type === 'correction') {
        const prev = prevById.get(payload.eventId);
        const next = nextById.get(payload.eventId);
        corrections.push({
          change_id: change.id,
          event_id: payload.eventId,
          from: prev ? { date: prev.occurred_on, week: weekIdOf(prev.occurred_on).weekId, org: prev.effective_org, metric: prev.metric, amount: prev.amount } : null,
          to: next ? { date: next.occurred_on, week: weekIdOf(next.occurred_on).weekId, org: next.effective_org, metric: next.metric, amount: next.amount } : null,
          moved_week: Boolean(prev && next && weekIdOf(prev.occurred_on).weekId !== weekIdOf(next.occurred_on).weekId),
          moved_org: Boolean(prev && next && prev.effective_org !== next.effective_org),
        });
      }
    } else if (change.type === 'reassign') {
      impactedEventIds.add(payload.eventId);
      const prev = prevById.get(payload.eventId);
      const next = nextById.get(payload.eventId);
      corrections.push({
        change_id: change.id,
        event_id: payload.eventId,
        from: prev ? { date: prev.occurred_on, week: weekIdOf(prev.occurred_on).weekId, org: prev.effective_org, metric: prev.metric, amount: prev.amount } : null,
        to: next ? { date: next.occurred_on, week: weekIdOf(next.occurred_on).weekId, org: next.effective_org, metric: next.metric, amount: next.amount } : null,
        moved_week: false,
        moved_org: Boolean(prev && next && prev.effective_org !== next.effective_org),
      });
    } else if (change.type === 'reparent') {
      // The daily/weekly *values* are unchanged, but every ancestor rollup for
      // every week must be re-derived along the new tree.
      structuralChange = true;
    } else if (change.type === 'rebuild') {
      // Explicit full rebuild: recompute every layer from the raw inputs.
      fullRebuild = true;
      structuralChange = true;
    }
  }

  // ---- Decide the affected daily partitions -------------------------------
  const daily = new Map(); // date -> Set<org>
  const addDaily = (date, org) => {
    if (!daily.has(date)) daily.set(date, new Set());
    daily.get(date).add(org);
  };

  for (const id of impactedEventIds) {
    const before = prevById.get(id);
    const after = nextById.get(id);
    if (before) addDaily(before.occurred_on, before.effective_org);
    if (after) addDaily(after.occurred_on, after.effective_org);
  }

  const index = buildOrgIndex(orgs);
  const weekly = new Map(); // week_id -> Set<org> (orgs whose own weekly row changed)
  for (const [date, orgSet] of daily) {
    const { weekId } = weekIdOf(date);
    if (!weekly.has(weekId)) weekly.set(weekId, new Set());
    for (const org of orgSet) weekly.get(weekId).add(org);
  }

  // ---- Per-layer strategy -------------------------------------------------
  // Daily: incremental only if every affected date already exists; a brand new
  // date, or a reparent/rebuild request, forces a rebuild of that partition.
  const dailySteps = [];
  const affectedDates = new Set([...daily.keys()]);
  const dailyStrategy = structuralChange ? 'rebuild' : 'incremental';

  for (const date of [...affectedDates].sort()) {
    const orgsForDate = [...daily.get(date)];
    const touched = new Set(orgsForDate);
    // Include already-present org/metric cells at that date so a moved-out
    // event leaves no stale value behind.
    for (const e of previousEvents) {
      if (e.occurred_on === date) touched.add(e.effective_org);
    }
    dailySteps.push({
      layer: 'daily',
      partition_id: date,
      label: `Daily ${date}`,
      reason: describeDailyReason(date, orgsForDate, corrections),
      strategy: dailyStrategy,
      orgs: [...touched].sort(),
    });
  }

  // ---- Weekly: recompute every week touched by an affected date -----------
  const weeklySteps = [];
  for (const weekId of [...new Set([...affectedDates].map((d) => weekIdOf(d).weekId))].sort()) {
    const orgsForWeek = new Set();
    for (const [date, orgSet] of daily) {
      if (weekIdOf(date).weekId === weekId) for (const o of orgSet) orgsForWeek.add(o);
    }
    // Weeks whose daily partitions changed must re-derive from daily, and the
    // org-reassignment case must include both old and new org branches.
    for (const c of corrections) {
      if (c.from?.week === weekId) orgsForWeek.add(c.from.org);
      if (c.to?.week === weekId) orgsForWeek.add(c.to.org);
    }
    const rollupOrgs = new Set();
    for (const o of orgsForWeek) {
      rollupOrgs.add(o);
      for (const a of index.ancestorsOf(o)) rollupOrgs.add(a);
    }
    weeklySteps.push({
      layer: 'weekly',
      partition_id: weekId,
      label: `Weekly ${weekId}`,
      reason: weeklyReason(weekId, corrections, structuralChange),
      strategy: 'incremental',
      orgs: [...orgsForWeek].sort(),
      rollup_orgs: [...rollupOrgs].sort(),
    });
  }

  // A structural change (reparent / explicit rebuild) leaves weekly *values*
  // intact but invalidates the org rollups, so every existing week must be
  // re-rolled up along the new tree — even weeks with no event-level change.
  let rollupWeeks = [...new Set(weeklySteps.map((s) => s.partition_id))].sort();
  if (structuralChange) {
    rollupWeeks = [...new Set([...rollupWeeks, ...allWeeks])].sort();
  }

  const dates = [...new Set([...daily.keys()])].sort();
  return {
    mode: fullRebuild ? 'rebuild' : structuralChange ? 'structural' : 'incremental',
    affected: {
      event_count: impactedEventIds.size,
      dates,
      weeks: rollupWeeks,
      changed_weeks: weeklySteps.map((w) => w.partition_id),
      orgs: [...new Set([...daily.values()].flatMap((s) => [...s]))].sort(),
    },
    corrections,
    dailySteps,
    weeklySteps,
    rollupWeeks,
    structural_change: structuralChange,
    full_rebuild: fullRebuild,
  };
}

function describeDailyReason(date, orgs, corrections) {
  const relevant = corrections.filter((c) => c.from?.date === date || c.to?.date === date);
  const bits = [];
  for (const c of relevant) {
    if (c.from?.date === date && c.to?.date === date) bits.push(`${c.event_id} value/org changed on this date`);
    else if (c.to?.date === date && !c.from) bits.push(`${c.event_id} newly covers this date`);
    else if (c.from?.date === date && c.to?.date !== date) bits.push(`${c.event_id} no longer falls on this date`);
    else if (c.to?.date === date) bits.push(`${c.event_id} moved into this date from ${c.from?.date ?? 'ingest'}`);
  }
  if (bits.length === 0) bits.push(`events for ${orgs.length} org(s) on ${date}`);
  return bits.join('; ');
}

function weeklyReason(weekId, corrections, structural) {
  if (structural) return `week ${weekId} re-derived after organization tree change`;
  const moved = corrections.filter((c) => c.moved_week && (c.from?.week === weekId || c.to?.week === weekId));
  if (moved.length) return `week ${weekId} affected by cross-week move (${moved.map((c) => c.event_id).join(', ')})`;
  const orgMoved = corrections.filter((c) => c.moved_org && (c.from?.week === weekId || c.to?.week === weekId));
  if (orgMoved.length) return `week ${weekId} affected by org attribution change (${orgMoved.map((c) => c.event_id).join(', ')})`;
  return `week ${weekId} re-derived from its daily rows`;
}
