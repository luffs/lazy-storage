// storage.js - Server persistence adapters
//
// A store persists as ROWS, one per leaf path: { value, ts, deleted }.
// Live rows carry the value and the timestamp that won it; tombstones
// carry the timestamp only. Alongside the rows: each replica's progress
// (the last sequence number seen from it, and when), and the store's
// version.
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
//   load()  -> null | { rows: Array<[pathKey, row]>, replicas: { replicaId: { seq, seen } }, version, epoch, log? }
//   commit({ upserts: Array<[pathKey, row]>, deletes: pathKey[],
//            replica?: { id, seq, seen }, forgetReplicas?: replicaId[], version, epoch,
//            log?: { v, diff }, logFloor?: number })
//   flush() -> void   (write out anything buffered; called on dispose)
//
// `seen` is the store's clock (ms) when the replica's op arrived; a
// `seen` of null means unknown (a document from before it was recorded).
// `epoch` is a random id the store mints once per storage life, so a
// client can tell whether its cached version means anything here.
// `log` in a commit is the accepted diff this op made, at version `v`;
// an adapter may keep these (pruning below `logFloor`) and hand them back
// as `log: [{ v, diff }]` on load, so a restarted server still answers
// reconnects with deltas. The memory and SQLite adapters do; the JSON
// file adapter does not, to keep its document small.
// The store also accepts the older `seqs: { replicaId: seq }` from load.
// `null` from load means "never seen": the store starts from `initial`.
// Document-oriented adapters (memory, JSON file) apply commits to an
// in-memory copy and write the whole document.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

/** The in-memory document shared by the memory and JSON-file adapters */
function document(initial = null, { keepLog = false } = {}) {
  const rows = new Map(initial ? initial.rows : []);
  let log = keepLog && Array.isArray(initial?.log) ? initial.log.map(e => structuredClone(e)) : [];
  // A document written before `seen` existed holds `seqs`
  const replicas = initial
    ? Object.fromEntries(initial.replicas
      ? Object.entries(initial.replicas).map(([id, r]) => [id, { ...r }])
      : Object.entries(initial.seqs ?? {}).map(([id, seq]) => [id, { seq, seen: null }]))
    : {};
  let version = initial ? initial.version : 0;
  let epoch = initial?.epoch ?? null;
  let seen = initial !== null;
  return {
    load: () => (seen
      ? { rows: [...rows].map(([k, r]) => [k, structuredClone(r)]), replicas: structuredClone(replicas), version, epoch, ...(keepLog ? { log: structuredClone(log) } : {}) }
      : null),
    commit(change) {
      seen = true;
      for (const key of change.deletes) rows.delete(key);
      for (const [key, row] of change.upserts) rows.set(key, structuredClone(row));
      if (change.replica) replicas[change.replica.id] = { seq: change.replica.seq, seen: change.replica.seen ?? null };
      for (const id of change.forgetReplicas ?? []) delete replicas[id];
      version = change.version;
      if (change.epoch !== undefined) epoch = change.epoch;
      if (keepLog) {
        if (change.log) log.push(structuredClone(change.log));
        if (Number.isInteger(change.logFloor)) log = log.filter(e => e.v >= change.logFloor);
      }
    },
    serialize: () => ({ rows: [...rows], replicas, version, epoch, ...(keepLog ? { log } : {}) })
  };
}

/** Keeps the document in memory only (delta log included); the store starts fresh with the process */
export function memoryStorage() {
  const doc = document(null, { keepLog: true });
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
