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
// The memory adapter keeps the document in memory, for tests and what need
// not survive the process; a file on disk is SQLite's (sqlite-bun.js,
// sqlite-node.js).
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

/** Keeps the document in memory only (delta log included); the store starts fresh with the process */
export function memoryStorage() {
  let records = new Map();
  let log = [];
  let replicas = {};
  let version = 0;
  let epoch = null;
  let schema;
  let seen = false;
  return {
    load: () => (seen ? structuredClone({ records: [...records.values()], replicas, version, epoch, ...(schema === undefined ? {} : { schema }), log }) : null),
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
      if (change.log) log.push(structuredClone(change.log));
      if (Number.isInteger(change.logFloor)) log = log.filter(e => e.v >= change.logFloor);
    },
    flush: () => {},
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
