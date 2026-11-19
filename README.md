# Metrics Lineage Console

A from-scratch metrics pipeline and web console: event-level detail rolls up into
daily metrics, then weekly metrics, then a per-organization comparison table.
Historical event corrections propagate through **every** dependent layer together
— a corrected daily cell can never leave a weekly row reading the old
contribution. Three independent services (ingest, scheduler, query) share one
SQLite database, and the web console lets you expand dependencies, read why each
partition was recomputed, cancel and resume a build mid-way, and approve a whole
group of generations for publishing.

Everything is computed for real from deterministic fictional data. No metrics
values are hand-written.

```
            raw events                 daily                 weekly              comparison
  ┌────────────────────────┐   ┌───────────────┐   ┌────────────────┐   ┌────────────────────┐
  │ events                 │   │ daily_metrics │   │ weekly_metrics │   │ weekly_org_rollup  │
  │ event_versions         │──▶│  (date, org,  │──▶│  (week, org,   │──▶│  (week, org,       │
  │ event_org_assignments  │   │   metric)     │   │   metric)      │   │   metric, members) │
  │ orgs / org_history     │   └───────────────┘   └────────────────┘   └────────────────────┘
  └────────────────────────┘            │                    │                     │
          append-only                   └────────── every row is keyed by ─────────┘
                                                  generation id
```

## Requirements

- Node.js **≥ 22.5** (uses the built-in `node:sqlite`). No npm dependencies are
  needed to run the app or its tests; `playwright-core` is only used by the
  optional headless UI script.

## Quick start

```bash
npm run seed        # deterministic fictional data, builds generation 1
npm run start       # ingest + scheduler + query + web UI
# open http://127.0.0.1:4100
```

`npm run start` spawns three child processes plus a tiny static/proxy server:

| service   | port | responsibility                                                 |
|-----------|------|----------------------------------------------------------------|
| web UI    | 4100 | static assets, same-origin `/api/{ingest,scheduler,query}` proxy |
| ingest    | 4101 | append events, submit corrections / reassignments / reparents   |
| scheduler | 4102 | plan affected partitions, build resumable generations, approve  |
| query     | 4103 | read one pinned generation (all derived layers + lineage)        |

`npm run services` starts only the three services (no web UI).

The database lives at `data/metrics.db` (override with `METRICS_DB`). It is
opened in WAL mode with `busy_timeout=5000`, so the processes read and write the
same file concurrently.

### Unattended operation

By default builds are operator-driven from the console, so you can inspect,
cancel, and resume them. Set `SCHEDULER_AUTORUN=1` to let the scheduler poll for
pending changes and build automatically.

## Try the consistency guarantees

```bash
# 1. Seed six ISO weeks and publish the first report.
npm run seed -- --weeks 6 --publish

# 2. Correct one historical event. With --no-build this only enqueues the
#    change; the console can then show the plan before anything runs.
npm run correct -- --event <event_id> --amount 4000 --date 2026-11-17 --no-build
npm run plan        # see exactly which daily/weekly/rollup partitions move

# 3. Build it. The correction moved an event to the next ISO week, so both the
#    old week and the new week are re-derived, along with every ancestor rollup.
npm run build

# 4. Prove the build is internally consistent: recompute every layer from the
#    generation's frozen input snapshot and compare.
npm run verify
```

Other commands: `reassign`, `reparent`, `job`, `approve`, `publish` (one id, or
several as a single release), `reports`, `report`, `compare`, `lineage`,
`rebuild`, `init`. Run `node src/cli.js help` for the full list.

## How it works

### Raw store is append-only and versioned

Ingest writes `events` (first version), `event_versions` (every version), and
`event_org_assignments` (dated per-event organization attribution). Corrections
never update derived tables; they append a new version and enqueue a
`change_requests` row (`pending`). A correction can change amount, metric, date,
or organization, and can optionally record an org attribution valid over a date
range.

### A generation pins its inputs

A `generations` row records the exact `event_seq`, `assign_seq`, and `org_seq`
boundaries it read. Materializing those into `generation_events` /
`generation_orgs` makes the generation's inputs immutable: a correction arriving
later does not retroactively change what an older generation computed.

Every derived row carries the `generation` column as part of its primary key
(`daily_metrics`, `weekly_metrics`, `weekly_org_rollup`, `dependencies`). A read
always filters by one generation, so the query service cannot assemble a view
that mixes layers from different input points.

### The planner widens the affected set correctly

`planRecompute` compares the base generation's snapshot with the current inputs:

- **value/metric change** → the affected daily partition, its week, and the
  rollup rows of every ancestor org.
- **cross-week move** (date correction) → *both* the source week and destination
  week are re-derived, so the old week loses the contribution as the new one
  gains it.
- **org attribution change** (reassign / dated assignment) → both the old and
  new org subtrees are re-derived, including ancestor rollups on each side.
- **reparent** (org tree change) → daily and weekly *values* are untouched, but
  every week's rollup is re-derived along the new tree; this is a
  `structural` build.

For each partition the planner chooses **local incremental update** (copy
unaffected cells forward, re-derive only the touched ones) or **rebuild of the
affected partition**. A brand-new date or an explicit `rebuild` request forces a
full rebuild (`mode: rebuild`).

### Builds are resumable at step granularity

A build is a `jobs` row plus one `job_steps` row per unit of work
(`snapshot`, `seed`, `daily`, `weekly`, `rollup`, `lineage`, `finalize`). Each
step is committed in its own transaction. The console can therefore advance one
step at a time, cancel mid-way, and resume without redoing completed steps. A
cancelled job keeps its change requests attached and remains resumable; only an
explicit abandon returns those requests to the queue for a fresh build.

### Finalize verifies the whole generation

`verifyGeneration` recomputes daily, weekly, and rollup from
`generation_events` and compares cell-by-cell, then cross-checks that weekly
totals equal daily totals and that event counts agree. A generation that fails
verification is marked `failed` and cannot be approved. Approval requires
`ready` **and** a passing verification.

### Reports are frozen and single-generation

Publishing copies all layers into `report_*` tables inside one transaction from
one approved, verified generation. Each report row carries the generation it came
from, so `verifyReport` can prove a report never mixes entries from two
generations. Older reports are never touched by later corrections. Approving
several ready generations at once publishes them as one `release`, and every
report in it independently verifies.

### The console reads one snapshot

`GET /api/query/snapshot?generation=N` returns daily, weekly, comparison, and org
rows for a single generation in one response, so the client cannot compose a
mixed-generation view. Selecting a comparison cell shows its upstream lineage
edges and the job steps / change requests that caused it to recompute.

## API sketch

Ingest (4101): `POST /events`, `/corrections`, `/reassignments`, `/reparents`;
`GET /orgs`, `/events/:id`, `/log`, `/changes`.

Scheduler (4102): `GET /status`, `/plan`; `POST /builds`, `/builds/run`,
`/jobs/:id/advance` (`max_steps`), `/jobs/:id/{pause,cancel,resume}`,
`/generations/:id/approve`, `/releases`, `/rebuild`.

Query (4103): `GET /generations`, `/daily`, `/weekly`, `/comparison`,
`/lineage`, `/explain`, `/provenance`, `/reports`, `/reports/:id/verify`,
`/snapshot`, `/summary`. All reads accept `generation`; without it they pin the
newest ready-or-approved generation.

## Tests

```bash
npm test
```

The suite (`node --test`, no dependencies) covers ISO-week math and exact
aggregation values, incremental correction chains, cross-week moves, org
attribution moves, reparent rebuilds, resumable/pausable/cancellable jobs, the
one-open-build guard across connections, frozen single-generation reports, and
lineage/explain. It also starts the three services as real subprocesses on free
ports sharing one database and exercises ingest → plan → build → query over HTTP.

## Layout

```
src/lib/         pure + DB logic (time, compute, planner, snapshot, generation,
                 scheduler, reports, query, ingest, seed, http)
src/services/    the three independent HTTP services
src/start.js     launcher (services + static/proxy web server)
src/cli.js       operator CLI
public/          framework-free web console
test/            node:test suites (unit, integration, subprocess services)
```
