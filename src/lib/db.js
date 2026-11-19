import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

export function openDb(path = defaultDbPath(), { readOnly = false } = {}) {
  const file = resolve(path);
  if (!readOnly) mkdirSync(dirname(file), { recursive: true });
  const db = new DatabaseSync(file, { readOnly, timeout: 5000 });
  db.exec('PRAGMA busy_timeout = 5000');
  db.exec('PRAGMA foreign_keys = ON');
  db.exec('PRAGMA journal_mode = WAL');
  if (!readOnly) db.exec('PRAGMA synchronous = FULL');
  return db;
}

export function defaultDbPath() {
  return process.env.METRICS_DB || resolve(process.cwd(), 'data', 'metrics.db');
}

/** Run `fn` inside one write transaction; nested calls join the outer one. */
export function transaction(db, fn, mode = 'IMMEDIATE') {
  if (db.__inTx) return fn();
  db.exec(`BEGIN ${mode}`);
  db.__inTx = true;
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* connection may already have rolled back */
    }
    throw err;
  } finally {
    db.__inTx = false;
  }
}

export const nowIso = () => new Date().toISOString();

export function initSchema(db) {
  db.exec(`
    ------------------------------------------------------------------
    -- Raw event store: append-only ingestion, versioned corrections.
    -- "seq" is the authoritative recency order (timestamps can tie).
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS events (
      event_id     TEXT PRIMARY KEY,
      occurred_on  TEXT NOT NULL,
      org_id       TEXT NOT NULL,
      metric       TEXT NOT NULL,
      amount       REAL NOT NULL,
      payload      TEXT NOT NULL DEFAULT '{}',
      received_at  TEXT NOT NULL,
      seq          INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_date ON events(occurred_on);
    CREATE INDEX IF NOT EXISTS idx_events_org  ON events(org_id);

    CREATE TABLE IF NOT EXISTS event_versions (
      event_id       TEXT NOT NULL,
      version        INTEGER NOT NULL,
      occurred_on    TEXT NOT NULL,
      org_id         TEXT NOT NULL,
      metric         TEXT NOT NULL,
      amount         REAL NOT NULL,
      payload        TEXT NOT NULL DEFAULT '{}',
      is_correction  INTEGER NOT NULL DEFAULT 0,
      correction_id  TEXT,
      reason         TEXT,
      recorded_at    TEXT NOT NULL,
      seq            INTEGER NOT NULL,
      PRIMARY KEY (event_id, version)
    );
    CREATE INDEX IF NOT EXISTS idx_versions_seq ON event_versions(seq);

    -- Dated, per-event organization attribution history.
    CREATE TABLE IF NOT EXISTS event_org_assignments (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id     TEXT NOT NULL,
      org_id       TEXT NOT NULL,
      valid_from   TEXT NOT NULL,
      valid_to     TEXT,
      reason       TEXT NOT NULL DEFAULT '',
      recorded_at  TEXT NOT NULL,
      seq          INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_assign_event ON event_org_assignments(event_id, valid_from);

    ------------------------------------------------------------------
    -- Organization reporting tree (current) plus its append-only history.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS orgs (
      org_id      TEXT NOT NULL PRIMARY KEY,
      name        TEXT NOT NULL,
      parent_id   TEXT,
      level       TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS org_history (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      org_id       TEXT NOT NULL,
      name         TEXT NOT NULL,
      parent_id    TEXT,
      level        TEXT NOT NULL,
      recorded_at  TEXT NOT NULL,
      seq          INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_org_history ON org_history(org_id, seq);

    ------------------------------------------------------------------
    -- Ingest log and the change queue the scheduler polls.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS ingest_log (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      request_id  TEXT NOT NULL,
      at          TEXT NOT NULL,
      kind        TEXT NOT NULL,
      accepted    INTEGER NOT NULL DEFAULT 0,
      rejected    INTEGER NOT NULL DEFAULT 0,
      status      TEXT NOT NULL,
      detail      TEXT NOT NULL DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS change_requests (
      id            TEXT PRIMARY KEY,
      type          TEXT NOT NULL,        -- ingest | correction | reassign | reparent | rebuild
      payload       TEXT NOT NULL,
      summary       TEXT NOT NULL DEFAULT '',
      status        TEXT NOT NULL,        -- pending | planned | done | error | released
      created_at    TEXT NOT NULL,
      processed_at  TEXT,
      generation    INTEGER,
      release_id    INTEGER,
      error         TEXT,
      requested_by  TEXT NOT NULL DEFAULT 'system',
      strategy      TEXT NOT NULL DEFAULT 'auto'
    );
    CREATE INDEX IF NOT EXISTS idx_change_status ON change_requests(status, created_at);

    ------------------------------------------------------------------
    -- Generations: immutable, fully materialized snapshots of all layers.
    -- Input boundaries (event/assign/org seq) pin exactly what was read.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS generations (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      status           TEXT NOT NULL,   -- building | ready | approved | failed | abandoned
      trigger_type     TEXT NOT NULL,
      reason           TEXT NOT NULL,
      base_generation  INTEGER,
      as_of            TEXT NOT NULL,
      event_seq        INTEGER NOT NULL,
      assign_seq       INTEGER NOT NULL,
      org_seq          INTEGER NOT NULL,
      started_at       TEXT NOT NULL,
      finished_at      TEXT,
      approved_at      TEXT,
      approved_by      TEXT,
      plan             TEXT NOT NULL DEFAULT '{}',
      digest           TEXT,
      verification     TEXT,
      error            TEXT
    );

    CREATE TABLE IF NOT EXISTS generation_events (
      generation    INTEGER NOT NULL,
      event_id      TEXT NOT NULL,
      version       INTEGER NOT NULL,
      occurred_on   TEXT NOT NULL,
      org_id        TEXT NOT NULL,       -- version's own org
      effective_org TEXT NOT NULL,       -- after dated attribution history
      metric        TEXT NOT NULL,
      amount        REAL NOT NULL,
      PRIMARY KEY (generation, event_id)
    );
    CREATE INDEX IF NOT EXISTS idx_gen_events_date ON generation_events(generation, occurred_on);

    CREATE TABLE IF NOT EXISTS generation_orgs (
      generation INTEGER NOT NULL,
      org_id     TEXT NOT NULL,
      name       TEXT NOT NULL,
      parent_id  TEXT,
      level      TEXT NOT NULL,
      PRIMARY KEY (generation, org_id)
    );

    ------------------------------------------------------------------
    -- Derived layers, all keyed by generation so a read can never mix.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS daily_metrics (
      generation   INTEGER NOT NULL,
      date         TEXT NOT NULL,
      org_id       TEXT NOT NULL,
      metric       TEXT NOT NULL,
      value        REAL NOT NULL,
      event_count  INTEGER NOT NULL,
      PRIMARY KEY (generation, date, org_id, metric)
    );

    CREATE TABLE IF NOT EXISTS weekly_metrics (
      generation   INTEGER NOT NULL,
      week_id      TEXT NOT NULL,
      week_start   TEXT NOT NULL,
      org_id       TEXT NOT NULL,
      metric       TEXT NOT NULL,
      value        REAL NOT NULL,
      event_count  INTEGER NOT NULL,
      day_count    INTEGER NOT NULL,
      PRIMARY KEY (generation, week_id, org_id, metric)
    );

    CREATE TABLE IF NOT EXISTS weekly_org_rollup (
      generation   INTEGER NOT NULL,
      week_id      TEXT NOT NULL,
      week_start   TEXT NOT NULL,
      org_id       TEXT NOT NULL,
      metric       TEXT NOT NULL,
      value        REAL NOT NULL,
      event_count  INTEGER NOT NULL,
      member_orgs  TEXT NOT NULL,
      PRIMARY KEY (generation, week_id, org_id, metric)
    );

    ------------------------------------------------------------------
    -- Explicit lineage edges within a generation (upstream -> downstream).
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS dependencies (
      generation  INTEGER NOT NULL,
      from_layer  TEXT NOT NULL,
      from_id     TEXT NOT NULL,
      to_layer    TEXT NOT NULL,
      to_id       TEXT NOT NULL,
      reason      TEXT NOT NULL,
      PRIMARY KEY (generation, to_layer, to_id, from_layer, from_id, reason)
    );
    CREATE INDEX IF NOT EXISTS idx_dep_downstream ON dependencies(generation, to_layer, to_id);
    CREATE INDEX IF NOT EXISTS idx_dep_upstream   ON dependencies(generation, from_layer, from_id);

    ------------------------------------------------------------------
    -- Jobs: one generation build, resumable at step granularity.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS jobs (
      id              TEXT PRIMARY KEY,
      generation      INTEGER NOT NULL,
      status          TEXT NOT NULL,     -- running | paused | cancelled | done | failed
      strategy        TEXT NOT NULL,     -- incremental | rebuild | mixed
      reason          TEXT NOT NULL,
      base_generation INTEGER,
      as_of           TEXT NOT NULL,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL,
      total_steps     INTEGER NOT NULL DEFAULT 0,
      plan            TEXT NOT NULL DEFAULT '{}',
      request_ids     TEXT NOT NULL DEFAULT '[]',
      error           TEXT
    );

    CREATE TABLE IF NOT EXISTS job_steps (
      job_id       TEXT NOT NULL,
      seq          INTEGER NOT NULL,
      layer        TEXT NOT NULL,       -- snapshot | seed | daily | weekly | rollup | lineage | finalize
      partition_id TEXT NOT NULL,
      label        TEXT NOT NULL,
      reason       TEXT NOT NULL DEFAULT '',
      status       TEXT NOT NULL DEFAULT 'pending', -- pending | done | failed
      meta         TEXT NOT NULL DEFAULT '{}',
      note         TEXT,
      PRIMARY KEY (job_id, seq)
    );

    ------------------------------------------------------------------
    -- Releases group several ready generations for one approval click.
    -- Membership is recorded by reports.release_id, so no join table is
    -- needed; each published report still pins exactly one generation.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS releases (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      status       TEXT NOT NULL,       -- draft | approved | published
      created_at   TEXT NOT NULL,
      approved_at  TEXT,
      approved_by  TEXT,
      note         TEXT NOT NULL DEFAULT ''
    );

    ------------------------------------------------------------------
    -- Published reports: immutable copies of exactly one generation.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS reports (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      generation     INTEGER NOT NULL,
      release_id     INTEGER,
      title          TEXT NOT NULL,
      published_at   TEXT NOT NULL,
      published_by   TEXT NOT NULL,
      date_from      TEXT NOT NULL,
      date_to        TEXT NOT NULL,
      digest         TEXT NOT NULL,
      summary        TEXT NOT NULL DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS report_daily (
      report_id   INTEGER NOT NULL,
      date        TEXT NOT NULL,
      org_id      TEXT NOT NULL,
      metric      TEXT NOT NULL,
      value       REAL NOT NULL,
      event_count INTEGER NOT NULL,
      generation  INTEGER NOT NULL,
      PRIMARY KEY (report_id, date, org_id, metric)
    );

    CREATE TABLE IF NOT EXISTS report_weekly (
      report_id   INTEGER NOT NULL,
      week_id     TEXT NOT NULL,
      org_id      TEXT NOT NULL,
      metric       TEXT NOT NULL,
      value        REAL NOT NULL,
      event_count  INTEGER NOT NULL,
      day_count    INTEGER NOT NULL,
      generation   INTEGER NOT NULL,
      PRIMARY KEY (report_id, week_id, org_id, metric)
    );

    CREATE TABLE IF NOT EXISTS report_rollup (
      report_id   INTEGER NOT NULL,
      week_id     TEXT NOT NULL,
      org_id      TEXT NOT NULL,
      metric      TEXT NOT NULL,
      value       REAL NOT NULL,
      event_count INTEGER NOT NULL,
      member_orgs TEXT NOT NULL,
      generation  INTEGER NOT NULL,
      PRIMARY KEY (report_id, week_id, org_id, metric)
    );

    CREATE TABLE IF NOT EXISTS report_orgs (
      report_id  INTEGER NOT NULL,
      org_id     TEXT NOT NULL,
      name       TEXT NOT NULL,
      parent_id  TEXT,
      level      TEXT NOT NULL,
      PRIMARY KEY (report_id, org_id)
    );

    ------------------------------------------------------------------
    -- Append-only audit trail.
    ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS audit (
      id     INTEGER PRIMARY KEY AUTOINCREMENT,
      at     TEXT NOT NULL,
      actor  TEXT NOT NULL,
      action TEXT NOT NULL,
      target TEXT NOT NULL,
      detail TEXT NOT NULL DEFAULT '{}'
    );

    -- Single-row counters keep seq allocation atomic across processes.
    CREATE TABLE IF NOT EXISTS sequence (
      name  TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    );
    INSERT OR IGNORE INTO sequence(name, value) VALUES
      ('event', 0), ('assignment', 0), ('org', 0);
  `);
}
