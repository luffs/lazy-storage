// storage.js - Server persistence adapters
//
// A store persists as ROWS, one per leaf path: { value, ts, deleted }.
// Live rows carry the value and the timestamp that won it; tombstones
// carry the timestamp only. Alongside the rows: the last sequence number
// seen from each replica, and the store's version.
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
//   load()  -> null | { rows: Array<[pathKey, row]>, seqs: { replicaId: seq }, version }
//   commit({ upserts: Array<[pathKey, row]>, deletes: pathKey[], replica?: { id, seq }, version })
//   flush() -> void   (write out anything buffered; called on dispose)
//
// `null` from load means "never seen": the store starts from `initial`.
// Document-oriented adapters (memory, JSON file) apply commits to an
// in-memory copy and write the whole document.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

/** The in-memory document shared by the memory and JSON-file adapters */
function document(initial = null) {
  const rows = new Map(initial ? initial.rows : []);
  const seqs = initial ? { ...initial.seqs } : {};
  let version = initial ? initial.version : 0;
  let seen = initial !== null;
  return {
    load: () => (seen ? { rows: [...rows].map(([k, r]) => [k, structuredClone(r)]), seqs: { ...seqs }, version } : null),
    commit(change) {
      seen = true;
      for (const key of change.deletes) rows.delete(key);
      for (const [key, row] of change.upserts) rows.set(key, structuredClone(row));
      if (change.replica) seqs[change.replica.id] = change.replica.seq;
      version = change.version;
    },
    serialize: () => ({ rows: [...rows], seqs, version })
  };
}

/** Keeps the document in memory only; the store starts fresh with the process */
export function memoryStorage() {
  const doc = document();
  return {
    load: doc.load,
    commit: doc.commit,
    flush: () => {}
  };
}

/**
 * One JSON file per store, written atomically (temp file + rename) and
 * debounced so a burst of ops costs one write. Call `flush()` before exit.
 * @param {string} file - path of the JSON file
 * @param {{ debounce?: number }} [options]
 */
export function jsonFileStorage(file, { debounce = 200 } = {}) {
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

  return {
    load: doc.load,
    commit(change) {
      doc.commit(change);
      dirty = true;
      clearTimeout(timer);
      timer = setTimeout(write, debounce);
    },
    flush() {
      clearTimeout(timer);
      write();
    }
  };
}
