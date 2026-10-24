// storage.js - Server persistence adapters
//
// A store persists as RECORDS, one per collection record or singleton:
// [c, id, data, clock], `id` null for a singleton, `clock` the record's
// timestamps by path ({ live, gone }, core/doc.js). Alongside the records:
// each replica's progress (the last sequence number seen from it, when,
// and the user it belongs to), the store's version, its epoch, how many
// migrations it has run, and the log of its last changes for deltas.
//
// The interface is incremental, so a row-oriented backend (SQLite) writes
// only the records an op touched:
//
//   load()  -> null | { records: [[c, id, data, clock]], replicas: { id: { seq, seen, owner? } },
//                       version, epoch, schema?, log?: [{ v, ts, changes }] }
//   commit({ upserts: [[c, id, data, clock]], deletes: [[c, id]],
//            replica?: { id, seq, seen, owner? }, forgetReplicas?: id[],
//            version, epoch, schema, log?: { v, ts, changes }, logFloor?: number })
//   commitMany(changes)   optional: several commits as one, all or none
//   flush()               write out anything buffered
//   close()               optional: the store let go of it
//   replace(doc)          optional: take a document (store.export()) as the
//                         whole storage, under a new epoch and no log
//
// `null` from load means "never seen": the store starts from `initial`.
// Document-oriented adapters (memory, JSON file) apply commits to an
// in-memory copy and write the whole document.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomId } from '../core/ids.js';

/** The epoch a replaced store starts: clients that were ahead of the document get a snapshot, not a delta of another history */
export function newEpoch() {
  return randomId();
}

/** A document replace() may take: records and a version, the rest optional */
export function assertDocument(doc) {
  if (!doc || !Array.isArray(doc.records) || !Number.isInteger(doc.version)) {
    throw new TypeError('A store document has records and a version (see store.export())');
  }
  for (const record of doc.records) {
    if (!Array.isArray(record) || typeof record[0] !== 'string' || !(record[1] === null || typeof record[1] === 'string') || record.length < 3) {
      throw new TypeError(`A record is [collection, id or null, data, clock]: ${JSON.stringify(record).slice(0, 200)}`);
    }
  }
}

const recordKey = (c, id) => JSON.stringify([c, id ?? null]);

/** The in-memory document shared by the memory and JSON-file adapters */
function document(initial = null, { keepLog = false } = {}) {
  let records = new Map((initial?.records ?? []).map(r => [recordKey(r[0], r[1]), r]));
  let log = keepLog && Array.isArray(initial?.log) ? structuredClone(initial.log) : [];
  let replicas = initial ? structuredClone(initial.replicas ?? {}) : {};
  let version = initial ? initial.version : 0;
  let epoch = initial?.epoch ?? null;
  let schema = Number.isInteger(initial?.schema) ? initial.schema : undefined;
  let seen = initial !== null;
  const extra = () => ({ ...(schema === undefined ? {} : { schema }), ...(keepLog ? { log } : {}) });
  return {
    load: () => (seen ? structuredClone({ records: [...records.values()], replicas, version, epoch, ...extra() }) : null),
    commit(change) {
      seen = true;
      for (const [c, id] of change.deletes ?? []) records.delete(recordKey(c, id));
      for (const record of change.upserts ?? []) records.set(recordKey(record[0], record[1]), structuredClone(record));
      if (change.replica) {
        const { id, seq, seen: at, owner } = change.replica;
        replicas[id] = owner === undefined ? { seq, seen: at } : { seq, seen: at, owner };
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
    serialize: () => ({ records: [...records.values()], replicas, version, epoch, ...extra() }),
    replace(doc) {
      assertDocument(doc);
      records = new Map(doc.records.map(r => [recordKey(r[0], r[1]), structuredClone(r)]));
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
  return { load: doc.load, commit: doc.commit, flush: () => {}, replace: doc.replace };
}

/**
 * One JSON file per store, written atomically (temp file + rename) and
 * debounced so a burst of ops costs one write. Call `flush()` before exit.
 * A debounced write that fails goes to `onError`, and is tried again a
 * second later; `flush()` throws to its caller
 * @param {string} file
 * @param {{ debounce?: number, onError?: (error: any) => void }} [options]
 */
export function jsonFileStorage(file, { debounce = 200, onError = err => console.error('lazy-storage:', err) } = {}) {
  const path = resolve(file);
  const saved = existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : null;
  // A file from before 1.0 keeps rows per leaf: served as no records, its next write would erase them
  let outdated = saved && !Array.isArray(saved.records) && Array.isArray(saved.rows);
  const doc = document(outdated ? null : saved);
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
    load() {
      if (outdated) {
        const err = new Error(`${path} was written by lazy-storage before 1.0, one row per leaf, which 1.0 cannot read: keep it, and start the store from a new file`);
        err.code = 'storage-format';
        throw err;
      }
      return doc.load();
    },
    commit(change) {
      if (outdated) throw new Error(`${path} was written by lazy-storage before 1.0; nothing is written over it`);
      doc.commit(change);
      dirty = true;
      later(debounce);
    },
    flush() {
      clearTimeout(timer);
      timer = null;
      write();
    },
    // Taken whole, it may go over a file from before 1.0: that is the way to move one
    replace(document) {
      doc.replace(document);
      outdated = false;
      dirty = true;
      this.flush();
    }
  };
}
