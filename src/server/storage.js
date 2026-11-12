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
// An adapter may also keep HISTORY, for a store made with `history`: a
// commit then carries `history: [{ c, id, v, ts, user, replica, data,
// changes }]`, a row per record the write wrote, `data` the record as it
// was left, as JSON (null: deleted), and `changes` the changes to it, as
// JSON. It keeps them, by record and version, and answers:
//
//   history({ c, id, before?, upToV?, upToTs?, fromTs?, user?, limit })   a record's rows, newest first:
//                         older than version `before`, no later than version `upToV` or time `upToTs`,
//                         no earlier than time `fromTs`, of one user's writes
//   historyOfVersion(v)   the rows one write left, each with its `c` and `id`
//   activity({ before?, after?, fromTs?, toTs?, user?, c?, deleted?, ascending?, cursor?, limit? })
//                         the rows of every record, each with its `c` and `id`, newest first (oldest
//                         first with `ascending`): of versions under `before` and over `after`, from
//                         time `fromTs` to `toTs`, of one user's writes, of one collection, or the
//                         deletions alone. In order of version, then collection, then id;
//                         `cursor` ({ v, c, id }) is a row to read on after
//   historyCount()        how many rows it holds
//   pruneHistory(before)  forget the rows older than that time, but for each
//                         record's last one before it: what the record was
//                         then (not kept when that is a deletion, which no
//                         row says as well)
//
// A document that replaces the storage starts its history over.
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
  let history = [];   // rows as committed, oldest first
  const sameRecord = (row, c, id) => row.c === c && row.id === (id ?? null);
  return {
    history({ c, id, before = Infinity, upToV = Infinity, upToTs = Infinity, fromTs = -Infinity, user, limit = Infinity }) {
      const out = [];
      for (let i = history.length - 1; i >= 0 && out.length < limit; i--) {
        const row = history[i];
        if (sameRecord(row, c, id) && row.v < before && row.v <= upToV && row.ts <= upToTs && row.ts >= fromTs && (user === undefined || row.user === user)) out.push({ ...row });
      }
      return out;
    },
    historyCount: () => history.length,
    historyOfVersion: v => history.filter(row => row.v === v).map(row => ({ ...row })),
    activity({ before = Infinity, after = -Infinity, fromTs = -Infinity, toTs = Infinity, user, c, deleted = false, ascending = false, cursor, limit = Infinity } = {}) {
      const order = (x, y) => (x.v !== y.v ? (ascending ? x.v - y.v : y.v - x.v) : x.c !== y.c ? (x.c < y.c ? -1 : 1) : (x.id ?? '') < (y.id ?? '') ? -1 : (x.id ?? '') > (y.id ?? '') ? 1 : 0);
      const rows = history.filter(row => row.v < before && row.v > after && row.ts >= fromTs && row.ts <= toTs &&
        (user === undefined || row.user === user) && (c === undefined || row.c === c) && (!deleted || row.data === null) &&
        (!cursor || order(row, cursor) > 0)).sort(order);
      return rows.slice(0, limit).map(row => ({ ...row }));
    },
    historyCount: () => history.length,
    pruneHistory(before) {
      // Each record's last row before the time stays: what the record was then. Unless it was deleted then:
      // no row says the same, and a record made and deleted would otherwise leave one for ever
      const last = new Map();
      for (const row of history) if (row.ts < before) last.set(recordKey(row.c, row.id), row);
      history = history.filter(row => row.ts >= before || (row.data !== null && last.get(recordKey(row.c, row.id)) === row));
    },
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
      for (const row of change.history ?? []) history.push({ ...row });
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
      history = [];
      seen = true;
    }
  };
}
