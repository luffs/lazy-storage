// storage.js - Server persistence adapters
//
// A store persists as ROWS, one per leaf path: { value, ts, deleted }.
// Live rows carry the value and the timestamp that won it; tombstones
// carry the timestamp only. Alongside the rows: each replica's progress
// (the last sequence number seen from it, and when, and the user it
// belongs to), and the store's version.
//
// An empty object is a leaf too (`assignees: {}` is one row). When such a
// container later gains children, its `{}` row stays next to the child
// rows on purpose: rows are applied shallow-first on load, so the empty
// object is created and then filled, and if every child is later deleted
// the row is what keeps the container in existence — exactly as the live
// state has it. Dropping it would make a restart lose the empty container.
//
// The interface is incremental so a row-oriented backend (SQLite) writes
// only what an op touched:
//
//   load()  -> null | { rows: Array<[pathKey, row]>, replicas: { replicaId: { seq, seen, owner? } }, version, epoch, schema?, log? }
//   commit({ upserts: Array<[pathKey, row]>, deletes: pathKey[],
//            replica?: { id, seq, seen, owner? }, forgetReplicas?: replicaId[], version, epoch, schema,
//            log?: { v, diff }, logFloor?: number })
//   flush() -> void   (write out anything buffered; called on dispose)
//   replace(doc) -> void   optional: take a document as load() gives it
//                          (or store.export() makes it) as the store's
//                          whole storage, for a restore or a move; never
//                          under a live store. It starts a new epoch (see
//                          newEpoch below) and no delta log
//
// `seen` is the store's clock (ms) when the replica's op arrived; a
// `seen` of null means unknown (a document from before it was recorded).
// `owner` is the key (a string) of the user who first spoke for the
// replica, absent for a replica of sessions without a user: another user
// may not take the replica over. An adapter that drops it loses only that.
// `epoch` is a random id the store mints once per storage life, so a
// client can tell whether its cached version means anything here.
// `schema` is how many of the store's `migrations` the rows have been
// through; an adapter that does not keep it has every migration run again
// on each load, so it keeps it.
// `log` in a commit is the accepted diff this op made, at version `v`;
// an adapter may keep these (pruning below `logFloor`) and hand them back
// as `log: [{ v, diff }]` on load, so a restarted server still answers
// reconnects with deltas. The memory and SQLite adapters do; the JSON
// file adapter does not, to keep its document small.
// `null` from load means "never seen": the store starts from `initial`.
// Document-oriented adapters (memory, JSON file) apply commits to an
// in-memory copy and write the whole document.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomId } from '../core/ids.js';

/**
 * The epoch a replaced store starts. A document may be older than what
 * clients have seen (a backup restored after the store moved on); with the
 * epoch it was taken under, a client that was ahead would, once the store
 * passes its version again, be sent a delta from a history it never had.
 * A new epoch sends every client that was connected a snapshot instead
 */
export function newEpoch() {
  return randomId();
}

/** A document replace() may take: rows, a version, and the rest optional */
export function assertDocument(doc) {
  if (!doc || !Array.isArray(doc.rows) || !Number.isInteger(doc.version)) {
    throw new TypeError('A store document has rows and a version (see store.export())');
  }
}

/** The in-memory document shared by the memory and JSON-file adapters */
function document(initial = null, { keepLog = false } = {}) {
  let rows = new Map(initial ? initial.rows : []);
  let log = keepLog && Array.isArray(initial?.log) ? initial.log.map(e => structuredClone(e)) : [];
  let replicas = initial ? structuredClone(initial.replicas ?? {}) : {};
  let version = initial ? initial.version : 0;
  let epoch = initial?.epoch ?? null;
  let schema = Number.isInteger(initial?.schema) ? initial.schema : undefined;
  let seen = initial !== null;
  return {
    load: () => (seen
      ? { rows: [...rows].map(([k, r]) => [k, structuredClone(r)]), replicas: structuredClone(replicas), version, epoch, ...(schema === undefined ? {} : { schema }), ...(keepLog ? { log: structuredClone(log) } : {}) }
      : null),
    commit(change) {
      seen = true;
      for (const key of change.deletes) rows.delete(key);
      for (const [key, row] of change.upserts) rows.set(key, structuredClone(row));
      if (change.replica) {
        const { seq, seen, owner } = change.replica;
        replicas[change.replica.id] = owner === undefined ? { seq, seen } : { seq, seen, owner };
      }
      for (const id of change.forgetReplicas ?? []) delete replicas[id];
      version = change.version;
      if (change.epoch !== undefined) epoch = change.epoch;
      if (Number.isInteger(change.schema)) schema = change.schema;
      if (keepLog) {
        if (change.log) log.push(structuredClone(change.log));
        if (Number.isInteger(change.logFloor)) log = log.filter(e => e.v >= change.logFloor);
      }
    },
    serialize: () => ({ rows: [...rows], replicas, version, epoch, ...(schema === undefined ? {} : { schema }), ...(keepLog ? { log } : {}) }),
    replace(doc) {
      assertDocument(doc);
      rows = new Map(doc.rows.map(([k, r]) => [k, structuredClone(r)]));
      replicas = structuredClone(doc.replicas ?? {});
      version = doc.version;
      epoch = newEpoch();
      schema = Number.isInteger(doc.schema) ? doc.schema : undefined;
      log = [];
      seen = true;
    }
  };
}

/** Keeps the document in memory only (delta log included); the store starts fresh with the process */
export function memoryStorage() {
  const doc = document(null, { keepLog: true });
  return {
    load: doc.load,
    commit: doc.commit,
    flush: () => {},
    replace: doc.replace
  };
}

/**
 * One JSON file per store, written atomically (temp file + rename) and
 * debounced so a burst of ops costs one write. Call `flush()` before exit.
 * A debounced write that fails (a full disk, a permission) runs in a timer,
 * where a throw would take the process down: it goes to `onError`
 * instead, the changes stay pending, and the write is tried again a
 * second later. `flush()` throws to its caller
 * @param {string} file - path of the JSON file
 * @param {{ debounce?: number, onError?: (error: any) => void }} [options]
 */
export function jsonFileStorage(file, { debounce = 200, onError = err => console.error('lazy-storage:', err) } = {}) {
  const path = resolve(file);
  const doc = document(existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : null);
  let dirty = false;
  let timer = null;

  const write = () => {
    if (!dirty) return;
    mkdirSync(dirname(path), { recursive: true });
    const tmp = `${path}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(doc.serialize()));
    renameSync(tmp, path);
    dirty = false;
  };

  const later = ms => {
    clearTimeout(timer);
    timer = setTimeout(background, ms);
    if (typeof timer?.unref === 'function') timer.unref();
  };
  function background() {
    timer = null;
    try {
      write();
    } catch (err) {
      onError(err);
      later(Math.max(debounce, 1000));
    }
  }

  return {
    load: doc.load,
    commit(change) {
      doc.commit(change);
      dirty = true;
      later(debounce);
    },
    flush() {
      clearTimeout(timer);
      timer = null;
      write();
    },
    replace(document) {
      doc.replace(document);
      dirty = true;
      this.flush();
    }
  };
}
