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
// One process at a time serves a store. Two processes on one file (a
// deploy whose old and new process overlap, a server started twice) would
// each load the store, commit versions that collide, and send their
// clients states that never meet again. So a process takes a LEASE on a
// store when it loads it: a row in `leases` naming it, renewed with every
// commit and on a timer while it holds the store, given up when the store
// is disposed or the file closed. A process that finds a store leased to
// another is refused with code 'store-locked' (a hub tells its clients
// 'unavailable', and they try again), and a commit that finds its lease
// gone is refused too, so a process that stalled past its lease cannot
// write over the one that took the store. A lease left by a process that
// died runs out after `ttl`, or at once when that process was on this
// machine and is gone. Stores are leased one by one, so two processes may
// share a file on purpose by serving different stores.
//
// The driver is abstracted to what bun:sqlite and node:sqlite both offer:
// `exec(sql)`, `prepare(sql)` giving a statement with run/get/all, and a
// `transaction(fn)` wrapper. sqlite-bun.js and sqlite-node.js supply them.

import { hostname } from 'node:os';
import { randomId } from '../core/ids.js';
import { assertDocument, newEpoch } from './storage.js';

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
    epoch   TEXT,
    schema  INTEGER
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS replicas (
    store   TEXT    NOT NULL,
    replica TEXT    NOT NULL,
    seq     INTEGER NOT NULL,
    seen    INTEGER,
    owner   TEXT,
    PRIMARY KEY (store, replica)
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS log (
    store TEXT    NOT NULL,
    v     INTEGER NOT NULL,
    diff  TEXT    NOT NULL,
    PRIMARY KEY (store, v)
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS leases (
    store  TEXT    PRIMARY KEY,
    holder TEXT    NOT NULL,
    host   TEXT,
    pid    INTEGER,
    until  INTEGER NOT NULL
  ) WITHOUT ROWID;
`;

/** Whether a process on this machine is alive (true when that cannot be told) */
function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err?.code !== 'ESRCH';
  }
}

/**
 * @param {Object} driver
 * @param {(sql: string) => void} driver.exec
 * @param {(sql: string) => { run: Function, get: Function, all: Function }} driver.prepare
 * @param {(fn: Function) => Function} driver.transaction - wraps fn so a call runs in one transaction
 * @param {() => void} driver.close
 * @param {any} driver.db - the driver's database object, exposed as `db`
 * @param {{ file: string, wal: boolean, lease?: { ttl?: number } | false }} options
 *   `lease`: how long (ms) a store's lease lasts without renewal (default
 *   30 s); false serves stores without leases (an in-memory database has
 *   no other process to guard against)
 */
export function sqliteStorageOn({ exec, prepare, transaction, close, db }, { file, wal, lease = {} }) {
  if (wal && file !== ':memory:') exec('PRAGMA journal_mode = WAL;');
  exec('PRAGMA synchronous = NORMAL;');
  // Wait out another connection's lock (a backup, an admin script) rather
  // than fail the commit at once with SQLITE_BUSY
  exec('PRAGMA busy_timeout = 5000;');
  exec(SCHEMA);
  // Files from before replicas had owners, or stores a schema, gain the column
  if (!prepare('PRAGMA table_info(replicas)').all().some(c => c.name === 'owner')) exec('ALTER TABLE replicas ADD COLUMN owner TEXT;');
  if (!prepare('PRAGMA table_info(stores)').all().some(c => c.name === 'schema')) exec('ALTER TABLE stores ADD COLUMN schema INTEGER;');

  const q = {
    upsert: prepare(`
      INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (store, path) DO UPDATE SET
        value = excluded.value, ts_ms = excluded.ts_ms, ts_count = excluded.ts_count,
        ts_replica = excluded.ts_replica, deleted = excluded.deleted`),
    del: prepare('DELETE FROM leaves WHERE store = ? AND path = ?'),
    rows: prepare('SELECT path, value, ts_ms, ts_count, ts_replica, deleted FROM leaves WHERE store = ?'),
    version: prepare('SELECT version, epoch, schema FROM stores WHERE store = ?'),
    setVersion: prepare(`
      INSERT INTO stores (store, version, epoch, schema) VALUES (?, ?, ?, ?)
      ON CONFLICT (store) DO UPDATE SET version = excluded.version, epoch = excluded.epoch, schema = COALESCE(excluded.schema, stores.schema)`),
    replicas: prepare('SELECT replica, seq, seen, owner FROM replicas WHERE store = ?'),
    setReplica: prepare(`
      INSERT INTO replicas (store, replica, seq, seen, owner) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (store, replica) DO UPDATE SET seq = excluded.seq, seen = excluded.seen, owner = COALESCE(replicas.owner, excluded.owner)`),
    forgetReplica: prepare('DELETE FROM replicas WHERE store = ? AND replica = ?'),
    ids: prepare('SELECT store FROM stores ORDER BY store'),
    putLog: prepare('INSERT INTO log (store, v, diff) VALUES (?, ?, ?) ON CONFLICT (store, v) DO UPDATE SET diff = excluded.diff'),
    pruneLog: prepare('DELETE FROM log WHERE store = ? AND v < ?'),
    log: prepare('SELECT v, diff FROM log WHERE store = ? ORDER BY v'),
    dropLeaves: prepare('DELETE FROM leaves WHERE store = ?'),
    dropReplicas: prepare('DELETE FROM replicas WHERE store = ?'),
    dropLog: prepare('DELETE FROM log WHERE store = ?'),
    dropStore: prepare('DELETE FROM stores WHERE store = ?'),
    dropLease: prepare('DELETE FROM leases WHERE store = ?')
  };

  // Leases (see the header)
  const leasing = lease !== false && file !== ':memory:';
  const ttl = (lease && lease.ttl) || 30_000;
  const holder = randomId();
  const host = hostname();
  const pid = process.pid;
  const lq = leasing ? {
    get: prepare('SELECT holder, host, pid, until FROM leases WHERE store = ?'),
    take: prepare(`
      INSERT INTO leases (store, holder, host, pid, until) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (store) DO UPDATE SET holder = excluded.holder, host = excluded.host, pid = excluded.pid, until = excluded.until`),
    renew: prepare('UPDATE leases SET until = ? WHERE store = ? AND holder = ?'),
    renewAll: prepare('UPDATE leases SET until = ? WHERE holder = ?'),
    give: prepare('DELETE FROM leases WHERE store = ? AND holder = ?'),
    giveAll: prepare('DELETE FROM leases WHERE holder = ?')
  } : null;
  const held = new Set();
  const acquire = leasing ? transaction(id => {
    const current = lq.get.get(id);
    const now = Date.now();
    const others = current && current.holder !== holder && current.until > now;
    const gone = others && current.host === host && current.pid !== pid && !alive(current.pid);
    if (others && !gone) {
      const err = new Error(`Store "${id}" is open in another process (${current.host ?? 'a host'}, pid ${current.pid}); it is served here once that process lets it go, or ${Math.ceil((current.until - now) / 1000)} s after it stops renewing`);
      err.code = 'store-locked';
      throw err;
    }
    lq.take.run(id, holder, host, pid, now + ttl);
    held.add(id);
  }) : () => {};
  let renewer = null;
  if (leasing) {
    renewer = setInterval(() => {
      try {
        if (held.size) lq.renewAll.run(Date.now() + ttl, holder);
      } catch { /* a busy database: the next tick, or the next commit, renews */ }
    }, Math.max(1000, Math.floor(ttl / 3)));
    if (typeof renewer?.unref === 'function') renewer.unref();
  }

  const commit = transaction((id, change) => {
    if (leasing) {
      // Still ours, or another process has the store now and this one must not write
      const renewed = lq.renew.run(Date.now() + ttl, id, holder);
      if (!renewed || renewed.changes === 0) {
        held.delete(id);
        const err = new Error(`The lease on store "${id}" was lost: another process serves it now`);
        err.code = 'lease-lost';
        throw err;
      }
    }
    for (const key of change.deletes) q.del.run(id, key);
    for (const [key, row] of change.upserts) {
      q.upsert.run(id, key, row.deleted ? null : JSON.stringify(row.value), row.ts[0], row.ts[1], row.ts[2], row.deleted ? 1 : 0);
    }
    if (change.replica) q.setReplica.run(id, change.replica.id, change.replica.seq, change.replica.seen, change.replica.owner ?? null);
    for (const replica of change.forgetReplicas ?? []) q.forgetReplica.run(id, replica);
    if (change.log) q.putLog.run(id, change.log.v, JSON.stringify(change.log.diff));
    if (Number.isInteger(change.logFloor)) q.pruneLog.run(id, change.logFloor);
    q.setVersion.run(id, change.version, change.epoch, Number.isInteger(change.schema) ? change.schema : null);
  });

  const replace = transaction((id, doc) => {
    q.dropLeaves.run(id);
    q.dropReplicas.run(id);
    q.dropLog.run(id);
    for (const [key, row] of doc.rows) {
      q.upsert.run(id, key, row.deleted ? null : JSON.stringify(row.value), row.ts[0], row.ts[1], row.ts[2], row.deleted ? 1 : 0);
    }
    for (const [replica, r] of Object.entries(doc.replicas ?? {})) q.setReplica.run(id, replica, r.seq, r.seen ?? null, r.owner ?? null);
    q.setVersion.run(id, doc.version, newEpoch(), Number.isInteger(doc.schema) ? doc.schema : null);
  });

  const remove = transaction(id => {
    q.dropLeaves.run(id);
    q.dropReplicas.run(id);
    q.dropLog.run(id);
    q.dropStore.run(id);
    q.dropLease.run(id);
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
          acquire(id);
          const meta = q.version.get(id);
          if (!meta) return null;
          const rows = q.rows.all(id).map(r => [
            r.path,
            r.deleted
              ? { ts: [r.ts_ms, r.ts_count, r.ts_replica], deleted: true }
              : { value: JSON.parse(r.value), ts: [r.ts_ms, r.ts_count, r.ts_replica] }
          ]);
          const replicas = Object.fromEntries(q.replicas.all(id).map(r => [r.replica, r.owner === null ? { seq: r.seq, seen: r.seen } : { seq: r.seq, seen: r.seen, owner: r.owner }]));
          const log = q.log.all(id).map(r => ({ v: r.v, diff: JSON.parse(r.diff) }));
          return { rows, replicas, version: meta.version, epoch: meta.epoch, ...(meta.schema === null || meta.schema === undefined ? {} : { schema: meta.schema }), log };
        },
        commit(change) {
          commit(id, change);
        },
        flush() {},
        /**
         * Take a document (store.export(), or another adapter's load()) as
         * this store's whole storage, in one transaction, under a new epoch
         * and without a delta log (see newEpoch in storage.js). Never under a live
         * store: one this process has loaded is refused with code
         * 'store-open' (release it first), one another process serves with
         * 'store-locked'
         */
        replace(doc) {
          assertDocument(doc);
          if (held.has(id)) {
            const err = new Error(`Store "${id}" is loaded here: release it (stores.release(id), or dispose it) before replacing its storage`);
            err.code = 'store-open';
            throw err;
          }
          acquire(id);
          try {
            replace(id, doc);
          } finally {
            this.close();
          }
        },
        /** The store is let go of: another process may serve it now */
        close() {
          if (!leasing || !held.delete(id)) return;
          try {
            lq.give.run(id, holder);
          } catch { /* the lease runs out on its own */ }
        }
      };
    },
    /** Ids of every store that has committed at least once */
    ids: () => q.ids.all().map(r => r.store),
    /** Delete a store's rows, replicas, log, and version */
    remove(id) {
      assertId(id);
      remove(id);
    },
    /**
     * Copy the whole file to `file` (which must not exist yet) as it
     * stands, with VACUUM INTO: consistent while the server runs, and
     * compact. The copy holds no leases, so a server opened on it serves
     * its stores at once, and gives every store a new epoch: by the time
     * it is restored, clients have seen more than it holds (see newEpoch
     * in storage.js)
     */
    backup(file) {
      if (typeof file !== 'string' || !file || file === ':memory:') throw new TypeError('backup needs a file path');
      prepare('VACUUM INTO ?').run(file);
      prepare("ATTACH DATABASE ? AS lazy_backup").run(file);
      try {
        exec('DELETE FROM lazy_backup.leases; UPDATE lazy_backup.stores SET epoch = lower(hex(randomblob(8)));');
      } finally {
        exec('DETACH DATABASE lazy_backup;');
      }
    },
    /** The underlying database object, for backups or ad-hoc queries */
    db,
    /** Give up every lease this process holds and close the file */
    close() {
      clearInterval(renewer);
      if (leasing && held.size) {
        try {
          lq.giveAll.run(holder);
        } catch { /* they run out on their own */ }
      }
      held.clear();
      close();
    }
  };
}
