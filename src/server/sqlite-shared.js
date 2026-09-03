// sqlite-shared.js - Row-per-leaf persistence on SQLite, for any driver
//
// One database file holds any number of stores. Every leaf path is a row
// in `leaves`, keyed by (store, path): live rows carry the JSON value and
// the timestamp that won it, tombstones carry the timestamp only. A
// commit is one transaction with exactly the upserts and deletes the op
// produced, so the write cost of an edit is a few rows, not the state.
// The delta log lives alongside (`log`), pruned to the floor the store
// names, so a restart still answers reconnects with deltas.
//
// Paths are JSON-encoded arrays, so the descendants of a path share a
// prefix ('["tasks","a",'), and the (store, path) primary key serves
// prefix scans; the merge does its own descendant bookkeeping through
// `deletes`, so the adapter never needs to scan.
//
// The driver is abstracted to what bun:sqlite and node:sqlite both offer:
// `exec(sql)`, `prepare(sql)` giving a statement with run/get/all, and a
// `transaction(fn)` wrapper. sqlite-bun.js and sqlite-node.js supply them.

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
    version INTEGER NOT NULL DEFAULT 0,
    epoch   TEXT
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS replicas (
    store   TEXT    NOT NULL,
    replica TEXT    NOT NULL,
    seq     INTEGER NOT NULL,
    seen    INTEGER,
    PRIMARY KEY (store, replica)
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS log (
    store TEXT    NOT NULL,
    v     INTEGER NOT NULL,
    diff  TEXT    NOT NULL,
    PRIMARY KEY (store, v)
  ) WITHOUT ROWID;
`;

/**
 * @param {Object} driver
 * @param {(sql: string) => void} driver.exec
 * @param {(sql: string) => { run: Function, get: Function, all: Function }} driver.prepare
 * @param {(fn: Function) => Function} driver.transaction - wraps fn so a call runs in one transaction
 * @param {() => void} driver.close
 * @param {any} driver.db - the driver's database object, exposed as `db`
 * @param {{ file: string, wal: boolean }} options
 */
export function sqliteStorageOn({ exec, prepare, transaction, close, db }, { file, wal }) {
  if (wal && file !== ':memory:') exec('PRAGMA journal_mode = WAL;');
  exec('PRAGMA synchronous = NORMAL;');
  exec(SCHEMA);
  // Databases from before 0.3.0 lack `replicas.seen` (NULL: unknown) and `stores.epoch` (NULL: the store mints one)
  const columns = table => prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  if (!columns('replicas').includes('seen')) exec('ALTER TABLE replicas ADD COLUMN seen INTEGER');
  if (!columns('stores').includes('epoch')) exec('ALTER TABLE stores ADD COLUMN epoch TEXT');

  const q = {
    upsert: prepare(`
      INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (store, path) DO UPDATE SET
        value = excluded.value, ts_ms = excluded.ts_ms, ts_count = excluded.ts_count,
        ts_replica = excluded.ts_replica, deleted = excluded.deleted`),
    del: prepare('DELETE FROM leaves WHERE store = ? AND path = ?'),
    rows: prepare('SELECT path, value, ts_ms, ts_count, ts_replica, deleted FROM leaves WHERE store = ?'),
    version: prepare('SELECT version, epoch FROM stores WHERE store = ?'),
    setVersion: prepare(`
      INSERT INTO stores (store, version, epoch) VALUES (?, ?, ?)
      ON CONFLICT (store) DO UPDATE SET version = excluded.version, epoch = COALESCE(excluded.epoch, stores.epoch)`),
    replicas: prepare('SELECT replica, seq, seen FROM replicas WHERE store = ?'),
    setReplica: prepare(`
      INSERT INTO replicas (store, replica, seq, seen) VALUES (?, ?, ?, ?)
      ON CONFLICT (store, replica) DO UPDATE SET seq = excluded.seq, seen = excluded.seen`),
    forgetReplica: prepare('DELETE FROM replicas WHERE store = ? AND replica = ?'),
    ids: prepare('SELECT store FROM stores ORDER BY store'),
    putLog: prepare('INSERT INTO log (store, v, diff) VALUES (?, ?, ?) ON CONFLICT (store, v) DO UPDATE SET diff = excluded.diff'),
    pruneLog: prepare('DELETE FROM log WHERE store = ? AND v < ?'),
    log: prepare('SELECT v, diff FROM log WHERE store = ? ORDER BY v'),
    dropLeaves: prepare('DELETE FROM leaves WHERE store = ?'),
    dropReplicas: prepare('DELETE FROM replicas WHERE store = ?'),
    dropLog: prepare('DELETE FROM log WHERE store = ?'),
    dropStore: prepare('DELETE FROM stores WHERE store = ?')
  };

  const commit = transaction((id, change) => {
    for (const key of change.deletes) q.del.run(id, key);
    for (const [key, row] of change.upserts) {
      q.upsert.run(id, key, row.deleted ? null : JSON.stringify(row.value), row.ts[0], row.ts[1], row.ts[2], row.deleted ? 1 : 0);
    }
    if (change.replica) q.setReplica.run(id, change.replica.id, change.replica.seq, change.replica.seen ?? null);
    for (const replica of change.forgetReplicas ?? []) q.forgetReplica.run(id, replica);
    if (change.log) q.putLog.run(id, change.log.v, JSON.stringify(change.log.diff));
    if (Number.isInteger(change.logFloor)) q.pruneLog.run(id, change.logFloor);
    q.setVersion.run(id, change.version, change.epoch ?? null);
  });

  const remove = transaction(id => {
    q.dropLeaves.run(id);
    q.dropReplicas.run(id);
    q.dropLog.run(id);
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
          const log = q.log.all(id).map(r => ({ v: r.v, diff: JSON.parse(r.diff) }));
          return { rows, replicas, version: meta.version, epoch: meta.epoch ?? null, log };
        },
        commit(change) {
          commit(id, change);
        },
        flush() {}
      };
    },
    /** Ids of every store that has committed at least once */
    ids: () => q.ids.all().map(r => r.store),
    /** Delete a store's rows, replicas, log, and version */
    remove(id) {
      assertId(id);
      remove(id);
    },
    /** The underlying database object, for backups or ad-hoc queries */
    db,
    close
  };
}
