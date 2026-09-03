// sqlite-bun.js - Row-per-leaf persistence on bun:sqlite
//
// One database file holds any number of stores. Every leaf path is a row
// in `leaves`, keyed by (store, path): live rows carry the JSON value and
// the timestamp that won it, tombstones carry the timestamp only. A
// commit is one transaction with exactly the upserts and deletes the op
// produced, so the write cost of an edit is a few rows, not the state.
//
// Paths are JSON-encoded arrays, so the descendants of a path share a
// prefix ('["tasks","a",'), and the (store, path) primary key serves
// prefix scans; the merge does its own descendant bookkeeping through
// `deletes`, so the adapter never needs to scan.
//
//   const sqlite = sqliteStorage('data/app.sqlite');
//   const store = createStore({ initial, registers, storage: sqlite.store('team-1') });
import { Database } from 'bun:sqlite';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS leaves (
    store      TEXT    NOT NULL,
    path       TEXT    NOT NULL,
    value      TEXT,
    ts_ms      INTEGER NOT NULL,
    ts_count   INTEGER NOT NULL,
    ts_replica TEXT    NOT NULL,
    deleted    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (store, path)
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS stores (
    store   TEXT    PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 0
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS replicas (
    store   TEXT    NOT NULL,
    replica TEXT    NOT NULL,
    seq     INTEGER NOT NULL,
    seen    INTEGER,
    PRIMARY KEY (store, replica)
  ) WITHOUT ROWID;
`;

/** Databases from before 0.3.0 lack `replicas.seen`; NULL there means unknown */
function migrate(db) {
  const columns = db.query('PRAGMA table_info(replicas)').all().map(c => c.name);
  if (!columns.includes('seen')) db.exec('ALTER TABLE replicas ADD COLUMN seen INTEGER');
}

/**
 * @param {string} [file=':memory:'] - database file (created if missing)
 * @param {{ wal?: boolean }} [options] - write-ahead logging (default on for
 *   files; readers never block a commit)
 */
export function sqliteStorage(file = ':memory:', { wal = true } = {}) {
  const db = new Database(file, { create: true });
  if (wal && file !== ':memory:') db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
  db.exec(SCHEMA);
  migrate(db);

  const q = {
    upsert: db.prepare(`
      INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (store, path) DO UPDATE SET
        value = excluded.value, ts_ms = excluded.ts_ms, ts_count = excluded.ts_count,
        ts_replica = excluded.ts_replica, deleted = excluded.deleted`),
    del: db.prepare('DELETE FROM leaves WHERE store = ? AND path = ?'),
    rows: db.prepare('SELECT path, value, ts_ms, ts_count, ts_replica, deleted FROM leaves WHERE store = ?'),
    version: db.prepare('SELECT version FROM stores WHERE store = ?'),
    setVersion: db.prepare('INSERT INTO stores (store, version) VALUES (?, ?) ON CONFLICT (store) DO UPDATE SET version = excluded.version'),
    replicas: db.prepare('SELECT replica, seq, seen FROM replicas WHERE store = ?'),
    setReplica: db.prepare(`
      INSERT INTO replicas (store, replica, seq, seen) VALUES (?, ?, ?, ?)
      ON CONFLICT (store, replica) DO UPDATE SET seq = excluded.seq, seen = excluded.seen`),
    forgetReplica: db.prepare('DELETE FROM replicas WHERE store = ? AND replica = ?'),
    ids: db.prepare('SELECT store FROM stores ORDER BY store'),
    dropLeaves: db.prepare('DELETE FROM leaves WHERE store = ?'),
    dropReplicas: db.prepare('DELETE FROM replicas WHERE store = ?'),
    dropStore: db.prepare('DELETE FROM stores WHERE store = ?')
  };

  const commit = db.transaction((id, change) => {
    for (const key of change.deletes) q.del.run(id, key);
    for (const [key, row] of change.upserts) {
      q.upsert.run(id, key, row.deleted ? null : JSON.stringify(row.value), row.ts[0], row.ts[1], row.ts[2], row.deleted ? 1 : 0);
    }
    if (change.replica) q.setReplica.run(id, change.replica.id, change.replica.seq, change.replica.seen ?? null);
    for (const replica of change.forgetReplicas ?? []) q.forgetReplica.run(id, replica);
    q.setVersion.run(id, change.version);
  });

  const remove = db.transaction(id => {
    q.dropLeaves.run(id);
    q.dropReplicas.run(id);
    q.dropStore.run(id);
  });

  function assertId(id) {
    if (typeof id !== 'string' || id.length === 0) throw new TypeError('A store id must be a non-empty string');
  }

  return {
    /** The storage adapter for one store (create it on first commit) */
    store(id) {
      assertId(id);
      return {
        load() {
          const meta = q.version.get(id);
          if (!meta) return null;
          const rows = q.rows.all(id).map(r => [
            r.path,
            r.deleted
              ? { ts: [r.ts_ms, r.ts_count, r.ts_replica], deleted: true }
              : { value: JSON.parse(r.value), ts: [r.ts_ms, r.ts_count, r.ts_replica] }
          ]);
          const replicas = Object.fromEntries(q.replicas.all(id).map(r => [r.replica, { seq: r.seq, seen: r.seen ?? null }]));
          return { rows, replicas, version: meta.version };
        },
        commit(change) {
          commit(id, change);
        },
        flush() {}
      };
    },
    /** Ids of every store that has committed at least once */
    ids: () => q.ids.all().map(r => r.store),
    /** Delete a store's rows, replicas, and version */
    remove(id) {
      assertId(id);
      remove(id);
    },
    /** The underlying bun:sqlite Database, for backups or ad-hoc queries */
    db,
    close: () => db.close()
  };
}
