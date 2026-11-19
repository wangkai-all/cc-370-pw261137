// Front-end for the lineage console. It talks to the three services through the
// same-origin proxy (/api/ingest, /api/scheduler, /api/query). Every metric
// panel is rendered from a single /snapshot response so it can never assemble a
// view that mixes two generations.
const $ = (sel) => document.querySelector(sel);
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = text;
  return n;
};

const state = {
  generation: null,      // pinned generation id for all read panels
  week: null,
  metric: 'signups',
  direction: 'upstream',
  selected: null,        // { org_id, week_id, metric }
  job: null,
  pollTimer: null,
  approving: new Set(),
  snapshot: null,
};

const api = {
  async call(service, method, path, body) {
    const res = await fetch(`/api/${service}${path}`, {
      method,
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json = {};
    try { json = text ? JSON.parse(text) : {}; } catch { json = { error: text }; }
    if (!res.ok) throw new Error(json.error || `${res.status} ${res.statusText}`);
    return json;
  },
  ingest: (m, p, b) => api.call('ingest', m, p, b),
  scheduler: (m, p, b) => api.call('scheduler', m, p, b),
  query: (m, p, b) => api.call('query', m, p, b),
};

function toast(message, isError = false) {
  const t = $('#toast');
  t.textContent = message;
  t.className = `toast show${isError ? ' err' : ''}`;
  t.hidden = false;
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.className = 'toast'; t.hidden = true; }, 3800);
}

const fmt = (n) => {
  if (n === null || n === undefined) return '—';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
  return String(Math.round(n * 100) / 100);
};

// ---------------------------------------------------------------- bootstrap
async function init() {
  bindControls();
  try {
    const summary = await api.query('GET', '/summary');
    state.generation = summary.generation;
    renderTiles(summary);
    await loadWorkbench();
  } catch (err) {
    if (/no completed generation/.test(err.message)) {
      renderEmptyState();
    } else {
      toast(err.message, true);
    }
  }
  refreshRecentReports();
  setInterval(refreshBuildStatus, 2500);
}

function renderEmptyState() {
  $('#gen-label').textContent = 'no generation yet';
  $('#tiles').innerHTML = '';
  $('#compare-table tbody').innerHTML = '<tr><td colspan="6" class="empty">No generation yet. Ingest events with <code>npm run seed</code> then press “Recompute pending changes”.</td></tr>';
}

function renderTiles(summary) {
  const tiles = $('#tiles');
  tiles.innerHTML = '';
  const totalEvents = summary.totals.reduce((a, t) => a + t.events, 0);
  tiles.append(
    tile('Generation', `#${summary.generation}`, `${summary.status} · ${summary.digest?.slice(0, 10)}…`),
    tile('Days covered', summary.range?.from_date ? `${summary.range.from_date} → ${summary.range.to_date}` : '—', `${summary.weeks.length} ISO weeks`),
    tile('Events', fmt(totalEvents), `${summary.totals.length} metrics`),
    ...summary.totals.map((t) => tile(t.metric, fmt(t.value), `${t.events} events · ${t.days} days`)),
  );
  $('#gen-label').textContent = `generation #${summary.generation} · ${summary.status}`;
}

function tile(k, v, m) {
  const n = el('div', 'tile');
  n.append(el('div', 'k', k), el('div', 'v', v), el('div', 'm', m));
  return n;
}

// ---------------------------------------------------------------- workbench
async function loadWorkbench(opts = {}) {
  if (!state.generation && !opts.allowNull) throw new Error('no generation');
  const params = new URLSearchParams();
  if (state.generation) params.set('generation', state.generation);
  if (opts.metric) params.set('metric', opts.metric);
  const snap = await api.query('GET', `/snapshot?${params}`);
  state.snapshot = snap;
  state.generation = snap.generation.id;
  $('#gen-label').textContent = `generation #${snap.generation.id} · ${snap.generation.status}`;

  const weeks = [...new Set(snap.weekly.map((w) => w.week_id))].sort();
  populateSelect($('#week-select'), weeks, state.week ?? weeks.at(-1));
  state.week = $('#week-select').value;
  populateSelect($('#metric-select'), [...new Set(snap.comparison.map((r) => r.metric))].sort(), state.metric);
  state.metric = $('#metric-select').value;

  renderComparison(snap);
  if (state.selected) {
    const still = snap.comparison.find(
      (r) => r.org_id === state.selected.org_id && r.week_id === state.selected.week_id && r.metric === state.selected.metric,
    );
    if (still) selectCell(state.selected);
    else clearSelection();
  }
}

function populateSelect(select, values, current) {
  const keep = values.includes(current) ? current : values[0];
  select.innerHTML = '';
  for (const v of values) {
    const o = el('option', null, v);
    o.value = v;
    select.append(o);
  }
  if (keep) select.value = keep;
}

const LEVEL_ORDER = { group: 0, region: 1, branch: 2 };

function renderComparison(snap) {
  const rows = snap.comparison
    .filter((r) => r.week_id === state.week && r.metric === state.metric)
    .sort((a, b) => (LEVEL_ORDER[a.level] ?? 9) - (LEVEL_ORDER[b.level] ?? 9) || a.org_id.localeCompare(b.org_id));
  const showChildren = $('#show-children').checked;
  const max = Math.max(1, ...rows.map((r) => r.value));

  const thead = $('#compare-table thead');
  thead.innerHTML = '';
  const tr = el('tr');
  for (const h of ['Organization', 'Level', 'Value', 'Events', 'Share', 'Member orgs']) tr.append(el('th', null, h));
  thead.append(tr);

  const tbody = $('#compare-table tbody');
  tbody.innerHTML = '';
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">No values for this week and metric.</td></tr>';
    return;
  }
  for (const r of rows) {
    if (!showChildren && r.level === 'branch') continue;
    const row = el('tr');
    row.dataset.org = r.org_id;
    row.dataset.week = r.week_id;
    row.dataset.metric = r.metric;
    if (state.selected && sameCell(state.selected, r)) row.classList.add('selected');

    const nameCell = el('td');
    const nameWrap = el('div', 'org-name');
    nameWrap.append(el('span', 'org-dot'), el('span', null, r.name ?? r.org_id));
    nameCell.append(nameWrap);
    row.append(nameCell, el('td', null, r.level));

    const valCell = el('td', null, fmt(r.value));
    row.append(valCell);
    row.append(el('td', null, String(r.event_count)));

    const share = el('td', 'bar-cell');
    const bar = el('div', 'mini-bar');
    bar.style.width = `${Math.round((r.value / max) * 78)}%`;
    share.append(bar, el('span', null, `${Math.round((r.value / rows[0].value) * 100)}%`));
    row.append(share);

    row.append(el('td', null, r.member_orgs.length > 1 ? `${r.member_orgs.length} orgs` : r.member_orgs[0] ?? '—'));

    row.addEventListener('click', () => {
      selectCell({ org_id: r.org_id, week_id: r.week_id, metric: r.metric, label: r.name ?? r.org_id });
      renderComparison(state.snapshot);
    });
    tbody.append(row);
  }
}

function sameCell(a, b) {
  return a.org_id === b.org_id && a.week_id === b.week_id && a.metric === b.metric;
}

async function selectCell(cell) {
  state.selected = cell;
  document.querySelectorAll('#compare-table tbody tr').forEach((tr) => {
    tr.classList.toggle('selected', tr.dataset.org === cell.org_id && tr.dataset.week === cell.week);
  });
  await Promise.all([renderLineage(cell), renderReasons(cell)]);
}

function clearSelection() {
  state.selected = null;
  $('#lineage').innerHTML = '<p class="muted small">Pick a node in the comparison table to trace its lineage.</p>';
  $('#reasons').innerHTML = '<p class="muted">No partition selected yet.</p>';
}

// ---------------------------------------------------------------- lineage
async function renderLineage(cell) {
  const box = $('#lineage');
  box.innerHTML = '<p class="muted small">Loading lineage…</p>';
  try {
    const nodeId = `${cell.week_id}@${cell.org_id}`;
    const data = await api.query('GET', `/lineage?generation=${state.generation}&layer=rollup&id=${encodeURIComponent(nodeId)}&direction=${state.direction}`);
    box.innerHTML = '';
    if (data.edges.length === 0) {
      box.innerHTML = '<p class="muted small">No dependency edges for this node in this generation.</p>';
      return;
    }
    // Show a focused slice, grouped by direction, with reasons.
    const seen = new Set();
    for (const e of data.edges.slice(0, 40)) {
      const key = `${e.from_layer}:${e.from_id}->${e.to_layer}:${e.to_id}:${e.reason}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const row = el('div', 'edge');
      const from = el('div', 'node from', shortNode(e.from_id));
      const arrow = el('div', 'arrow', '→');
      const to = el('div', 'node to', shortNode(e.to_id));
      row.append(from, arrow, to, el('div', 'r', e.reason));
      box.append(row);
    }
    if (data.edges.length > 40) box.append(el('p', 'muted small', `…and ${data.edges.length - 40} more edges`));
  } catch (err) {
    box.innerHTML = '';
    box.append(el('p', 'muted small', err.message));
  }
}

function shortNode(id) {
  return id.replace(/^rollup:/, '').replace(/^weekly:/, '').replace(/^daily:/, '');
}

// ---------------------------------------------------------------- reasons
async function renderReasons(cell) {
  const box = $('#reasons');
  box.innerHTML = '<p class="muted small">Loading…</p>';
  try {
    const data = await api.query(
      'GET',
      `/explain?generation=${state.generation}&layer=weekly&id=${encodeURIComponent(cell.week_id)}`,
    );
    box.innerHTML = '';
    if (!data.steps.length) {
      box.innerHTML = '<p class="muted">No recorded steps for this partition.</p>';
      return;
    }
    const header = el('div', 'reason-block');
    header.append(
      el('div', 'rtext', data.generation_reason),
      el('div', 'rmeta', `${data.trigger_type} · base generation ${data.base_generation ?? 'none'} · inputs @ event#${data.inputs.event_seq}`),
    );
    box.append(header);
    for (const s of data.steps) {
      const b = el('div', 'reason-block');
      const chips = el('div');
      chips.append(el('span', 'chip', s.layer), el('span', 'chip', s.strategy ?? ''));
      b.append(
        el('div', 'rid', `${s.partition_id}`),
        chips,
        el('div', 'rtext', s.reason),
        el('div', 'rmeta', `step ${s.step} · ${s.status}`),
      );
      box.append(b);
    }
    for (const r of data.requests) {
      const b = el('div', 'reason-block');
      b.append(el('div', 'rid', r.id.slice(0, 18)), el('div', 'rtext', r.summary), el('div', 'rmeta', `${r.type} · ${r.status}`));
      box.append(b);
    }
  } catch (err) {
    box.innerHTML = '';
    box.append(el('p', 'muted small', err.message));
  }
}

// ---------------------------------------------------------------- build flow
function bindControls() {
  $('#week-select').addEventListener('change', (e) => {
    state.week = e.target.value;
    renderComparison(state.snapshot);
    if (state.selected) selectCell({ ...state.selected, week_id: state.week });
  });
  $('#metric-select').addEventListener('change', (e) => {
    state.metric = e.target.value;
    renderComparison(state.snapshot);
    if (state.selected) selectCell({ ...state.selected, metric: state.metric });
  });
  $('#show-children').addEventListener('change', () => renderComparison(state.snapshot));
  $('#dir-toggle').addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-dir]');
    if (!btn) return;
    state.direction = btn.dataset.dir;
    document.querySelectorAll('#dir-toggle button').forEach((b) => b.classList.toggle('active', b === btn));
    if (state.selected) renderLineage(state.selected);
  });
  $('#btn-build').addEventListener('click', () => startBuild(false));
  $('#btn-rebuild').addEventListener('click', () => startBuild(true));
  $('#btn-advance').addEventListener('click', () => advanceJob(1));
  $('#btn-cancel').addEventListener('click', cancelJob);
  $('#btn-resume').addEventListener('click', resumeJob);
  $('#btn-approve').addEventListener('click', approveSelected);
}

async function startBuild(fullRebuild) {
  try {
    let started;
    if (fullRebuild) {
      const r = await api.scheduler('POST', '/rebuild', { reason: 'full rebuild from UI' });
      started = { job_id: r.job_id, generation_id: r.generation_id, plan: r.plan };
    } else {
      started = await api.scheduler('POST', '/builds', {});
    }
    state.job = { id: started.job_id, generation: started.generation_id, plan: started.plan };
    showBuildArea(started.plan);
    toast(`Planned generation #${started.generation_id}`);
    await refreshJob();
  } catch (err) {
    toast(err.message, true);
  }
}
function showBuildArea(plan) {
  $('#build-area').hidden = false;
  const aff = plan?.affected ?? {};
  $('#build-title').textContent = `Planned generation`;
  $('#build-sub').textContent = `${plan?.mode ?? ''} · ${aff.dates?.length ?? 0} days · ${aff.weeks?.length ?? 0} weeks · ${aff.event_count ?? 0} events`;
  $('#btn-cancel').hidden = false;
  $('#btn-resume').hidden = true;
}

async function refreshJob() {
  if (!state.job) return;
  try {
    const job = await api.scheduler('GET', `/jobs/${state.job.id}`);
    renderJob(job);
    if (['done', 'cancelled', 'failed'].includes(job.status)) {
      if (job.status === 'done') {
        await refreshAfterBuild(job);
      }
    }
  } catch (err) {
    toast(err.message, true);
  }
}

function renderJob(job) {
  $('#build-bar').style.width = `${Math.round(job.progress * 100)}%`;
  $('#build-sub').textContent = `${job.strategy} · ${job.done}/${job.total_steps} steps · ${job.status}`;
  $('#btn-cancel').hidden = job.status !== 'running' && job.status !== 'paused';
  $('#btn-resume').hidden = !(job.status === 'cancelled' || job.status === 'paused');
  $('#btn-advance').disabled = job.status !== 'running' && job.status !== 'paused';

  const list = $('#steps');
  list.innerHTML = '';
  for (const s of job.steps) {
    const li = el('li');
    const mark = el('span', `mark ${s.status === 'done' ? 'done' : s.status === 'failed' ? 'failed' : 'pending'}`, s.status === 'done' ? '✓' : s.status === 'failed' ? '✕' : '·');
    li.append(mark, el('span', 'layer', s.layer), el('span', 'label', s.label));
    if (s.reason) li.append(el('span', 'reason', s.reason));
    list.append(li);
  }
}

async function advanceJob(steps) {
  if (!state.job) return;
  try {
    const r = await api.scheduler('POST', `/jobs/${state.job.id}/advance`, { max_steps: steps });
    renderJob(r.job);
    if (r.job.status === 'done') await refreshAfterBuild(r.job);
  } catch (err) {
    toast(err.message, true);
  }
}

async function cancelJob() {
  if (!state.job) return;
  try {
    const job = await api.scheduler('POST', `/jobs/${state.job.id}/cancel`, {});
    renderJob(job);
    toast(`Job cancelled at ${job.done}/${job.total_steps} steps — resume any time`);
  } catch (err) {
    toast(err.message, true);
  }
}

async function resumeJob() {
  if (!state.job) return;
  try {
    const job = await api.scheduler('POST', `/jobs/${state.job.id}/resume`, {});
    renderJob(job);
    if (job.status === 'done') await refreshAfterBuild(job);
    else toast('Resumed');
  } catch (err) {
    toast(err.message, true);
  }
}

async function refreshAfterBuild(job) {
  toast(`Generation #${job.generation} ready`);
  await loadWorkbench({ metric: state.metric, allowNull: true });
  renderTiles(await api.query('GET', `/summary?generation=${job.generation}`));
  await refreshReadyGenerations();
  await refreshRecentReports();
}

async function refreshBuildStatus() {
  // Show pending work even if the user didn't start a job from this tab.
  try {
    const plan = await api.scheduler('GET', '/plan');
    const pending = plan.changes?.length ?? 0;
    if (pending > 0) {
      if (!$('#build-area').hidden) return;
      showBuildArea(plan.plan);
      $('#build-title').textContent = `${pending} pending change(s)`;
      $('#build-sub').textContent = `planned: ${plan.plan.mode} · ${plan.plan.affected.weeks.length} weeks`;
      // Not started yet: no job to advance.
      state.job = null;
      $('#btn-advance').disabled = true;
      $('#btn-cancel').hidden = true;
      $('#btn-resume').hidden = true;
      renderPlannedSteps(plan.plan);
    }
  } catch { /* scheduler may be down; ignore */ }
}

function renderPlannedSteps(plan) {
  const list = $('#steps');
  list.innerHTML = '';
  const add = (layer, label, reason) => {
    const li = el('li');
    li.append(el('span', 'mark pending', '·'), el('span', 'layer', layer), el('span', 'label', label));
    if (reason) li.append(el('span', 'reason', reason));
    list.append(li);
  };
  for (const d of plan.dailySteps ?? []) add('daily', d.label, d.reason);
  for (const w of plan.weeklySteps ?? []) add('weekly', w.label, w.reason);
  for (const wk of plan.rollupWeeks ?? []) add('rollup', `Org comparison ${wk}`, 're-roll up the organization tree');
}

// ---------------------------------------------------------------- approvals
async function refreshReadyGenerations() {
  const box = $('#ready-gens');
  box.innerHTML = '';
  try {
    const { generations } = await api.query('GET', '/generations');
    const ready = generations.filter((g) => g.status === 'ready' || g.status === 'approved').slice(0, 6);
    if (ready.length === 0) {
      box.innerHTML = '<p class="muted small">No ready generations.</p>';
      $('#btn-approve').disabled = true;
      return;
    }
    for (const g of ready) {
      const row = el('label', 'gen-row');
      const cb = el('input');
      cb.type = 'checkbox';
      cb.value = g.id;
      cb.checked = state.approving.has(g.id);
      cb.addEventListener('change', () => {
        if (cb.checked) state.approving.add(g.id);
        else state.approving.delete(g.id);
        updateApproveButton();
      });
      const badge = el('span', `badge ${g.status}`, g.status);
      row.append(cb, el('span', 'gid', `#${g.id}`), badge,
        el('span', 'gmeta', `${g.reason?.slice(0, 40) ?? ''} · verify ${g.verification?.ok ? 'OK' : '—'}`));
      box.append(row);
    }
    updateApproveButton();
  } catch (err) {
    box.innerHTML = '';
    box.append(el('p', 'muted small', err.message));
  }
}

function updateApproveButton() {
  const btn = $('#btn-approve');
  const n = state.approving.size;
  btn.disabled = n === 0;
  btn.textContent = n > 1 ? `Approve & publish ${n} generations` : 'Approve & publish selected';
}

async function approveSelected() {
  const ids = [...state.approving];
  if (ids.length === 0) return;
  try {
    const res = await api.scheduler('POST', '/releases', {
      generation_ids: ids, note: 'approved from console', title: 'Metrics report',
    });
    toast(`Published ${res.report_ids.length} report(s) as one release`);
    state.approving.clear();
    await refreshReadyGenerations();
    await refreshRecentReports();
    // Approval changes generation status; refresh the header so the pill does
    // not keep claiming "ready" after publishing.
    if (state.generation) {
      renderTiles(await api.query('GET', `/summary?generation=${state.generation}`));
    }
  } catch (err) {
    toast(err.message, true);
  }
}

async function refreshRecentReports() {
  const list = $('#reports');
  try {
    const { reports } = await api.query('GET', '/reports');
    list.innerHTML = '';
    if (reports.length === 0) {
      list.innerHTML = '<li class="empty">No reports published yet.</li>';
      return;
    }
    for (const r of reports.slice(0, 8)) {
      const li = el('li');
      const title = el('div', 'rtitle');
      title.append(el('strong', null, r.title), el('span', 'frozen', 'frozen'));
      const s = r.summary ?? {};
      li.append(
        title,
        el('div', 'rsub', `report #${r.id} · generation #${r.generation} · ${s.daily_rows ?? 0} daily / ${s.weekly_rows ?? 0} weekly rows`),
        el('div', 'rsub', `${s.date_from ?? ''} → ${s.date_to ?? ''} · digest ${r.digest?.slice(0, 12)}…`),
      );
      li.addEventListener('click', async () => {
        try {
          const v = await api.query('GET', `/reports/${r.id}/verify`);
          toast(v.ok ? `Report #${r.id} verified: all layers from generation #${v.pinned}` : `Report #${r.id} MIXES generations!`, !v.ok);
        } catch (err) { toast(err.message, true); }
      });
      list.append(li);
    }
  } catch (err) {
    list.innerHTML = '';
    list.append(el('li', 'empty', err.message));
  }
}

init().then(() => refreshReadyGenerations());
