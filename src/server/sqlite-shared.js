// sqlite-shared.js - Row-per-record persistence on SQLite, for any driver
//
// One database file holds any number of stores. Every record is a row in
// `records`, keyed by (store, collection, id), `id` '' for a singleton:
// its JSON data and its clock (the times by path the merge decides by,
// core/doc.js). A commit is one transaction with exactly the records the
// op touched, so the write cost of an edit is a row or two, not the state.
// The delta log lives alongside (`oplog`), pruned to the floor the store
// names, so a restart still answers reconnects with deltas.
//
// A file written by lazy-storage before 1.0 keeps its rows in `leaves`, one
// per leaf, which this adapter does not read: a store found there and not
// in `records` is refused with code 'storage-format', so that it is
// migrated (`fromLeaves`, below) rather than served empty.
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
import { createShape } from '../core/shape.js';
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
 * on the disk for them. A worker that cannot start, fails, or ends on its
 * own hands them back to SQLite on the committing thread, as they were,
 * and says so through `onError` (code 'checkpoint-worker'): the likeliest
 * cause is a bundle that left sqlite-checkpoint.js out, and its only other
 * sign would be slow answers. `close()` has the worker close its
 * connection first, and waits for it: a connection left open would keep
 * the WAL, and on Windows the file
 */
function checkpointer({ file, driver, exec, onError }) {
  let running = true;
  const inline = cause => {
    if (!running) return;
    running = false;
    try {
      exec(`PRAGMA wal_autocheckpoint = ${INLINE_PAGES};`);
    } catch { /* the file closed meanwhile */ }
    const err = new Error(`SQLite checkpoints are back on the thread that commits: the checkpoint worker failed (${cause?.message ?? cause})`, { cause });
    err.code = 'checkpoint-worker';
    try {
      onError(err);
    } catch { /* a reporter that throws reports nothing more */ }
  };
  const done = new SharedArrayBuffer(4);
  let worker;
  try {
    worker = new Worker(new URL('./sqlite-checkpoint.js', import.meta.url), { workerData: { file, driver, every: CHECKPOINT_EVERY, done } });
  } catch (err) {
    inline(err);
    return { close() {} };
  }
  exec(`PRAGMA wal_autocheckpoint = ${BACKSTOP_PAGES};`);
  worker.unref();
  worker.on('error', inline);
  worker.on('exit', code => inline(new Error(`it exited with code ${code}`)));
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
  CREATE TABLE IF NOT EXISTS records (
    store      TEXT NOT NULL,
    collection TEXT NOT NULL,
    id         TEXT NOT NULL,
    data       TEXT NOT NULL,
    clock      TEXT NOT NULL,
    PRIMARY KEY (store, collection, id)
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
  CREATE TABLE IF NOT EXISTS oplog (
    store   TEXT    NOT NULL,
    v       INTEGER NOT NULL,
    ts      INTEGER NOT NULL,
    changes TEXT    NOT NULL,
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

/** A record's id in its row: '' for a singleton's */
const rowId = id => (id === null || id === undefined ? '' : id);
const EMPTY_CLOCK = { live: {}, gone: {} };

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
 * @param {{ file: string, wal: boolean, lease?: { ttl?: number } | false, checkpoints?: 'worker'|'inline', onError?: (error: any) => void }} options
 *   `lease`: how long (ms) a store's lease lasts without renewal (default
 *   30 s); false serves stores without leases (an in-memory database has
 *   no other process to guard against). `checkpoints`: where the WAL is
 *   copied back into the file, on a worker thread (the default, for a file
 *   in WAL mode; see checkpointer) or, 'inline', by SQLite on the thread
 *   that commits. `onError`: faults of the adapter's own, a checkpoint
 *   worker that failed; default console
 */
export function sqliteStorageOn({ exec, prepare, transaction, close, db, driver }, { file, wal, lease = {}, checkpoints = 'worker', onError = err => console.error('lazy-storage:', err) }) {
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
  // A file from before 1.0 has its stores in `leaves` (see the header)
  const hasLeaves = prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'leaves'").get() != null;
  const leavesOf = hasLeaves ? prepare('SELECT 1 AS one FROM leaves WHERE store = ? LIMIT 1') : null;
  const background = wal && file !== ':memory:' && checkpoints === 'worker' && driver ? checkpointer({ file, driver, exec, onError }) : null;

  const q = {
    upsert: prepare(`
      INSERT INTO records (store, collection, id, data, clock) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (store, collection, id) DO UPDATE SET data = excluded.data, clock = excluded.clock`),
    del: prepare('DELETE FROM records WHERE store = ? AND collection = ? AND id = ?'),
    records: prepare('SELECT collection, id, data, clock FROM records WHERE store = ?'),
    any: prepare('SELECT 1 AS one FROM records WHERE store = ? LIMIT 1'),
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
    putLog: prepare('INSERT INTO oplog (store, v, ts, changes) VALUES (?, ?, ?, ?) ON CONFLICT (store, v) DO UPDATE SET ts = excluded.ts, changes = excluded.changes'),
    pruneLog: prepare('DELETE FROM oplog WHERE store = ? AND v < ?'),
    log: prepare('SELECT v, ts, changes FROM oplog WHERE store = ? ORDER BY v'),
    dropRecords: prepare('DELETE FROM records WHERE store = ?'),
    dropReplicas: prepare('DELETE FROM replicas WHERE store = ?'),
    dropLog: prepare('DELETE FROM oplog WHERE store = ?'),
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
      // On another host (another container, say) whether that process still runs cannot be told from here: one that
      // was killed, or stopped without closing the file, holds the store until its lease runs out, and the message
      // says so, as that is what a deploy's wait usually is
      const elsewhere = current.host !== host
        ? '; from here it cannot be told whether that process still runs: one killed without closing the file holds the store until its lease runs out'
        : '';
      const err = new Error(`Store "${id}" is open in another process (${current.host ?? 'a host'}, pid ${current.pid}); it is served here once that process lets it go, or ${Math.ceil((current.until - now) / 1000)} s after it stops renewing${elsewhere}`);
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
    const records = new Map();    // [collection, id] -> its row as the changes leave it; null: deleted
    const replicas = new Map();   // replica id -> its progress as they leave it; null: forgotten
    for (const change of changes) {
      for (const [c, rid] of change.deletes ?? []) records.set(JSON.stringify([c, rowId(rid)]), null);
      for (const [c, rid, data, clock] of change.upserts ?? []) records.set(JSON.stringify([c, rowId(rid)]), [data, clock]);
      if (change.replica) replicas.set(change.replica.id, change.replica);
      for (const replica of change.forgetReplicas ?? []) replicas.set(replica, null);
      if (change.log) q.putLog.run(id, change.log.v, change.log.ts, JSON.stringify(change.log.changes));
    }
    for (const [key, row] of records) {
      const [c, rid] = JSON.parse(key);
      if (row === null) q.del.run(id, c, rid);
      else q.upsert.run(id, c, rid, JSON.stringify(row[0]), JSON.stringify(row[1] ?? EMPTY_CLOCK));
    }
    for (const [replica, r] of replicas) {
      if (r === null) q.forgetReplica.run(id, replica);
      else q.setReplica.run(id, replica, r.seq, r.seen, r.owner ?? null);
    }
    if (floor !== null) q.pruneLog.run(id, floor);
  });

  const replace = transaction((id, doc) => {
    q.dropRecords.run(id);
    q.dropReplicas.run(id);
    q.dropLog.run(id);
    for (const [c, rid, data, clock] of doc.records) q.upsert.run(id, c, rowId(rid), JSON.stringify(data), JSON.stringify(clock ?? EMPTY_CLOCK));
    for (const [replica, r] of Object.entries(doc.replicas ?? {})) q.setReplica.run(id, replica, r.seq, r.seen ?? null, r.owner ?? null);
    q.setVersion.run(id, doc.version, newEpoch(), Number.isInteger(doc.schema) ? doc.schema : null);
  });

  const remove = transaction(id => {
    q.dropRecords.run(id);
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
          if (leavesOf?.get(id) && !q.any.get(id)) {
            this.close();
            const err = new Error(`Store "${id}" was written by lazy-storage before 1.0, one row per leaf: migrate the file with fromLeaves before serving it`);
            err.code = 'storage-format';
            throw err;
          }
          const records = q.records.all(id).map(r => [r.collection, r.id === '' ? null : r.id, JSON.parse(r.data), JSON.parse(r.clock)]);
          const replicas = Object.fromEntries(q.replicas.all(id).map(r => [r.replica, r.owner === null ? { seq: r.seq, seen: r.seen } : { seq: r.seq, seen: r.seen, owner: r.owner }]));
          const log = q.log.all(id).map(r => ({ v: r.v, ts: r.ts, changes: JSON.parse(r.changes) }));
          return { records, replicas, version: meta.version, epoch: meta.epoch, ...(meta.schema === null || meta.schema === undefined ? {} : { schema: meta.schema }), log };
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
    /** Delete a store's records, replicas, log, and version */
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
    /**
     * Move the stores a file from before 1.0 keeps in `leaves` (one row per
     * leaf) into `records`, one transaction a store, and drop their leaf rows
     * and their delta log (which spoke of leaves). `shapeOf(storeId)` gives the
     * store's shape, as createStore takes it, so its singletons are the ones
     * the store will read (a key listed in `singletons`, or one whose initial
     * value is not an object); `{ singletons }` alone will do too, and null
     * for a store whose every top-level key is a collection. Every other key
     * is a collection whose records are its keys. Each leaf's time becomes
     * its path's time in its record's clock, a deletion inside a living
     * record a deleted path, and a record deleted whole is left out. The
     * stores get a new epoch, so every client takes a snapshot. Run it with no
     * store loaded. Returns the ids it moved
     */
    fromLeaves(shapeOf = () => ({})) {
      if (!hasLeaves) return [];
      const ids = prepare('SELECT DISTINCT store FROM leaves').all().map(r => r.store);
      const rowsOf = prepare('SELECT path, value, ts_ms, deleted FROM leaves WHERE store = ?');
      const dropLeaves = prepare('DELETE FROM leaves WHERE store = ?');
      const dropOldLog = prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'log'").get() ? prepare('DELETE FROM log WHERE store = ?') : null;
      const setEpoch = prepare('UPDATE stores SET epoch = ? WHERE store = ?');
      const moveOne = transaction(id => {
        const declared = shapeOf(id);
        const full = declared?.initial ? createShape(declared) : null;
        const singles = new Set(full ? full.singletons : (declared?.singletons ?? []));
        const records = new Map();   // [c, id] -> { rows: [[inner path, row]] }
        for (const row of rowsOf.all(id)) {
          const path = JSON.parse(row.path);
          if (!Array.isArray(path) || path.length === 0) continue;
          const single = singles.has(path[0]);
          if (!single && path.length < 2) continue;
          const key = JSON.stringify(single ? [path[0], ''] : [path[0], path[1]]);
          if (!records.has(key)) records.set(key, []);
          records.get(key).push([single ? path.slice(1) : path.slice(2), row]);
        }
        for (const [key, rows] of records) {
          const [c, rid] = JSON.parse(key);
          rows.sort((a, b) => a[0].length - b[0].length);
          const whole = rows.find(([inner]) => inner.length === 0);
          if (whole && whole[1].deleted === 1) continue;
          let data = {};
          // A seed record's rows were written over its seed
          if (full && !singles.has(c) && Object.hasOwn(full.initial[c] ?? {}, rid)) data = structuredClone(full.initial[c][rid]);
          const clock = { live: {}, gone: {} };
          for (const [inner, row] of rows) {
            const pk = JSON.stringify(inner);
            if (row.deleted === 1) {
              if (inner.length) {
                let node = data;
                for (const seg of inner.slice(0, -1)) node = node && typeof node === 'object' ? node[seg] : undefined;
                if (node && typeof node === 'object') delete node[inner[inner.length - 1]];
              }
              clock.gone[pk] = row.ts_ms;
              continue;
            }
            const value = row.deleted === 2 ? {} : JSON.parse(row.value);
            if (inner.length === 0) {
              data = value;
            } else {
              if (data === null || typeof data !== 'object' || Array.isArray(data)) data = {};
              let node = data;
              for (const seg of inner.slice(0, -1)) {
                if (node[seg] === null || typeof node[seg] !== 'object' || Array.isArray(node[seg])) node[seg] = {};
                node = node[seg];
              }
              const last = inner[inner.length - 1];
              const container = value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0;
              if (!(container && node[last] && typeof node[last] === 'object')) node[last] = value;
            }
            clock.live[pk] = row.ts_ms;
          }
          q.upsert.run(id, c, rid, JSON.stringify(data), JSON.stringify(clock));
        }
        // A seed record before 1.0 was there unless it was deleted; now it is there only as a row
        if (full) {
          for (const c of full.collections) {
            for (const [rid, seed] of Object.entries(full.initial[c] ?? {})) {
              if (!records.has(JSON.stringify([c, rid]))) q.upsert.run(id, c, rid, JSON.stringify(seed), JSON.stringify({ live: {}, gone: {} }));
            }
          }
        }
        dropLeaves.run(id);
        dropOldLog?.run(id);
        setEpoch.run(newEpoch(), id);
      });
      for (const id of ids) moveOne(id);
      return ids;
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
