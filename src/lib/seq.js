import { transaction } from './db.js';

/** Allocate a new global sequence number for an append-only input table. */
export function nextSeq(db, name) {
  db.prepare('UPDATE sequence SET value = value + 1 WHERE name = ?').run(name);
  return db.prepare('SELECT value FROM sequence WHERE name = ?').get(name).value;
}

export function currentSeq(db, name) {
  return db.prepare('SELECT value FROM sequence WHERE name = ?').get(name).value;
}

export function inputBoundaries(db) {
  return {
    eventSeq: currentSeq(db, 'event'),
    assignSeq: currentSeq(db, 'assignment'),
    orgSeq: currentSeq(db, 'org'),
  };
}

export function recordAudit(db, { actor = 'system', action, target, detail = {} }) {
  transaction(db, () => {
    db.prepare('INSERT INTO audit(at, actor, action, target, detail) VALUES (?,?,?,?,?)')
      .run(new Date().toISOString(), actor, action, target, JSON.stringify(detail));
  });
}
