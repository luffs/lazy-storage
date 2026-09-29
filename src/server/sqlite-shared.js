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
// SQLite writes whole pages, 4 KB each, whatever part of them changed, so
// what a commit costs the disk is the pages it touches, not the size of the
// op: a small op touched six, some 26 KB of WAL for a few hundred bytes of
// change, and a busy store's disk (and the event loop, waiting on its
// checkpoints) was what held it back. So a commit touches no page it need
// not: the delta log is pruned once the floor has moved PRUNE_EVERY entries
// past what was last pruned, not one entry every commit (the rows between are
// read back and dropped by the store on load, which keeps its own last
// `deltaLog`), and the lease is written only when it runs low (see below).
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
// share a file on purpose by serving different stores. A commit reads its
// lease rather than writing it: the timer keeps it fresh, and a commit
// renews it only when less than half of `ttl` is left (the timer fell
// behind), so most commits write no lease page. The read comes after the
// commit's first write, which takes the file's write lock, so it sees the
// lease as it stands: no other process can take it in between.
//
// The driver is abstracted to what bun:sqlite and node:sqlite both offer:
// `exec(sql)`, `prepare(sql)` giving a statement with run/get/all, and a
// `transaction(fn)` wrapper. sqlite-bun.js and sqlite-node.js supply them.

import { hostname } from 'node:os';
import { Worker } from 'node:worker_threads';
import { randomId } from '../core/ids.js';
import { assertDocument, newEpoch } from './storage.js';

/** How far a store's log floor moves before the rows under it are deleted (see the header) */
const PRUNE_EVERY = 100;

/** How often (ms) the worker checkpoints (see checkpointer) */
const CHECKPOINT_EVERY = 100;
/** The log (in pages, some 40 MB) past which the main connection checkpoints after all: a worker fallen far behind */
const BACKSTOP_PAGES = 10_000;
/** SQLite's own: the log past which the committing connection checkpoints, with no worker */
const INLINE_PAGES = 1000;
/** How long (ms) close() waits for the worker to let the file go */
const CLOSE_WAIT = 5000;

/**
 * Checkpoints on a worker thread with a connection of its own (see
 * sqlite-checkpoint.js), and the main connection's own put off to a
 * backstop: the thread that commits (a server's event loop) no longer waits
 * on the disk for them. A worker that cannot start, or fails, hands them
 * back to SQLite on the committing thread, as they were. `close()` has the
 * worker close its connection first, and waits for it: a connection left
 * open would keep the WAL, and on Windows the file
 */
function checkpointer({ file, driver, exec }) {
  let running = true;
  const inline = () => {
    if (!running) return;
    running = false;
    try {
      exec(`PRAGMA wal_autocheckpoint = ${INLINE_PAGES};`);
    } catch { /* the file closed meanwhile */ }
  };
  const done = new SharedArrayBuffer(4);
  let worker;
  try {
    worker = new Worker(new URL('./sqlite-checkpoint.js', import.meta.url), { workerData: { file, driver, every: CHECKPOINT_EVERY, done } });
  } catch {
    return { close() {} };
  }
  exec(`PRAGMA wal_autocheckpoint = ${BACKSTOP_PAGES};`);
  worker.unref();
  worker.on('error', inline);
  worker.on('exit', inline);
  return {
    close() {
      if (!running) return;
      running = false;
      worker.postMessage('close');
      Atomics.wait(new Int32Array(done), 0, 0, CLOSE_WAIT);
    }
  };
}

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
 * @param {'bun'|'node'} [driver.driver] - which driver the checkpoint worker opens the file with
 * @param {{ file: string, wal: boolean, lease?: { ttl?: number } | false, checkpoints?: 'worker'|'inline' }} options
 *   `lease`: how long (ms) a store's lease lasts without renewal (default
 *   30 s); false serves stores without leases (an in-memory database has
 *   no other process to guard against). `checkpoints`: where the WAL is
 *   copied back into the file, on a worker thread (the default, for a file
 *   in WAL mode; see checkpointer) or, 'inline', by SQLite on the thread
 *   that commits
 */
export function sqliteStorageOn({ exec, prepare, transaction, close, db, driver }, { file, wal, lease = {}, checkpoints = 'worker' }) {
  if (checkpoints !== 'worker' && checkpoints !== 'inline') {
    close();   // the driver opened the file already
    throw new TypeError("checkpoints must be 'worker' or 'inline'");
  }
  if (wal && file !== ':memory:') exec('PRAGMA journal_mode = WAL;');
  exec('PRAGMA synchronous = NORMAL;');
  // Wait out another connection's lock (a backup, an admin script) rather
  // than fail the commit at once with SQLITE_BUSY
  exec('PRAGMA busy_timeout = 5000;');
  exec(SCHEMA);
  // Files from before replicas had owners, or stores a schema, gain the column
  if (!prepare('PRAGMA table_info(replicas)').all().some(c => c.name === 'owner')) exec('ALTER TABLE replicas ADD COLUMN owner TEXT;');
  if (!prepare('PRAGMA table_info(stores)').all().some(c => c.name === 'schema')) exec('ALTER TABLE stores ADD COLUMN schema INTEGER;');
  const background = wal && file !== ':memory:' && checkpoints === 'worker' && driver ? checkpointer({ file, driver, exec }) : null;

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
  // Stores loaded here: with leases on, the ones this process holds a lease
  // on; with them off, still the ones replace() must not write under
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
  }) : id => { held.add(id); };
  let renewer = null;
  if (leasing) {
    renewer = setInterval(() => {
      try {
        if (held.size) lq.renewAll.run(Date.now() + ttl, holder);
      } catch { /* a busy database: the next tick, or the next commit, renews */ }
    }, Math.max(1000, Math.floor(ttl / 3)));
    if (typeof renewer?.unref === 'function') renewer.unref();
  }

  /**
   * A store's changes, in one transaction: one commit, or what a turn of
   * the event loop merged (see the store's groupCommit). Each row is written
   * once, as the last of the changes left it: the version is the last
   * change's, the lease is read once, a leaf or a replica several changes
   * wrote (or deleted, or forgot) gets its final row, and the log is pruned
   * once, to `floor`. Every log entry is written, one a version. A page the
   * changes share is written once, and a statement runs once for them all
   * rather than once a change
   */
  const commitChanges = transaction((id, changes, floor) => {
    const last = changes[changes.length - 1];
    // The version first: every commit writes it, and that first write takes
    // the file's write lock, so the lease read below is the latest
    q.setVersion.run(id, last.version, last.epoch, Number.isInteger(last.schema) ? last.schema : null);
    if (leasing) {
      // Still ours, or another process has the store now and this one must not write
      const current = lq.get.get(id);
      if (!current || current.holder !== holder) {
        held.delete(id);
        const err = new Error(`The lease on store "${id}" was lost: another process serves it now`);
        err.code = 'lease-lost';
        throw err;
      }
      const now = Date.now();
      if (current.until - now < ttl / 2) lq.renew.run(now + ttl, id, holder);
    }
    const leaves = new Map();     // path key -> its row as the changes leave it; null: deleted
    const replicas = new Map();   // replica id -> its progress as they leave it; null: forgotten
    for (const change of changes) {
      // In the order one change's own rows go: its deletes, then its upserts
      for (const key of change.deletes) leaves.set(key, null);
      for (const [key, row] of change.upserts) leaves.set(key, row);
      if (change.replica) replicas.set(change.replica.id, change.replica);
      for (const replica of change.forgetReplicas ?? []) replicas.set(replica, null);
      if (change.log) q.putLog.run(id, change.log.v, JSON.stringify(change.log.diff));
    }
    for (const [key, row] of leaves) {
      if (row === null) q.del.run(id, key);
      else q.upsert.run(id, key, row.deleted ? null : JSON.stringify(row.value), row.ts[0], row.ts[1], row.ts[2], row.deleted ? 1 : 0);
    }
    for (const [replica, r] of replicas) {
      if (r === null) q.forgetReplica.run(id, replica);
      else q.setReplica.run(id, replica, r.seq, r.seen, r.owner ?? null);
    }
    if (floor !== null) q.pruneLog.run(id, floor);
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
      // The log floor the rows were last pruned to: null until a commit here
      // prunes, which the first one does
      let pruned = null;
      /** Several changes, in order, in one transaction: all of them or none */
      const commitMany = changes => {
        if (!changes.length) return;
        // The floor the last of them names, pruned to when it has moved far enough (see the header)
        let floor = null;
        for (const change of changes) if (Number.isInteger(change.logFloor)) floor = change.logFloor;
        const prune = floor !== null && (pruned === null || floor - pruned >= PRUNE_EVERY);
        commitChanges(id, changes, prune ? floor : null);
        if (prune) pruned = floor;
      };
      return {
        load() {
          pruned = null;
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
        commit: change => commitMany([change]),
        commitMany,
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
          pruned = null;
          try {
            replace(id, doc);
          } finally {
            this.close();
          }
        },
        /** The store is let go of: another process may serve it now */
        close() {
          if (!held.delete(id) || !leasing) return;
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
    /** Give up every lease this process holds, stop the checkpoint worker, and close the file */
    close() {
      clearInterval(renewer);
      if (leasing && held.size) {
        try {
          lq.giveAll.run(holder);
        } catch { /* they run out on their own */ }
      }
      held.clear();
      // The worker's connection first: the last one to close checkpoints and removes the WAL
      background?.close();
      close();
    }
  };
}
