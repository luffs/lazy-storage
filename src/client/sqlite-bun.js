// sqlite-bun.js - Row persistence on bun:sqlite, for a client that runs in Bun
//
// The same rows the IndexedDB adapter keeps, in three tables: `records`
// ([collection, id] -> the record and its clock), `ops` (seq -> JSON op),
// and `meta` (one row). bun:sqlite is synchronous, so this adapter fits
// createClient() as it is: no openClient() needed. A patch is one
// transaction of the records it touched. One file per client. A file from
// before 1.0 (a `leaves` table) has its rows and ops let go: the client
// takes a snapshot.
//
//   const db = createClient({ store: 'team-1', storage: sqliteClientStorage('mirror.sqlite'), ... });
import { Database } from 'bun:sqlite';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS records (collection TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, clock TEXT NOT NULL, PRIMARY KEY (collection, id)) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS ops (seq INTEGER PRIMARY KEY, op TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
`;

const rowId = id => (id === null || id === undefined ? '' : id);
const EMPTY_CLOCK = { live: {}, gone: {} };

/**
 * @param {string} [file=':memory:'] - database file (created if missing)
 * @param {{ wal?: boolean }} [options] - write-ahead logging (default on for files)
 */
export function sqliteClientStorage(file = ':memory:', { wal = true } = {}) {
  const db = new Database(file, { create: true });
  if (wal && file !== ':memory:') db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
  if (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'leaves'").get()) {
    db.exec('DROP TABLE leaves; DROP TABLE IF EXISTS ops; DROP TABLE IF EXISTS meta;');
  }
  db.exec(SCHEMA);

  const q = {
    put: db.prepare('INSERT INTO records (collection, id, data, clock) VALUES (?, ?, ?, ?) ON CONFLICT (collection, id) DO UPDATE SET data = excluded.data, clock = excluded.clock'),
    del: db.prepare('DELETE FROM records WHERE collection = ? AND id = ?'),
    clear: db.prepare('DELETE FROM records'),
    records: db.prepare('SELECT collection, id, data, clock FROM records'),
    putOp: db.prepare('INSERT INTO ops (seq, op) VALUES (?, ?) ON CONFLICT (seq) DO UPDATE SET op = excluded.op'),
    delOp: db.prepare('DELETE FROM ops WHERE seq = ?'),
    dropOps: db.prepare('DELETE FROM ops WHERE seq <= ?'),
    ops: db.prepare('SELECT op FROM ops ORDER BY seq'),
    meta: db.prepare("SELECT value FROM meta WHERE key = 'meta'"),
    setMeta: db.prepare("INSERT INTO meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value")
  };
  const setMeta = meta => q.setMeta.run(JSON.stringify(meta));
  const put = ([c, id, data, clock]) => q.put.run(c, rowId(id), JSON.stringify(data), JSON.stringify(clock ?? EMPTY_CLOCK));

  const commit = db.transaction(({ puts, deletes, meta }) => {
    for (const [c, id] of deletes) q.del.run(c, rowId(id));
    for (const record of puts) put(record);
    setMeta(meta);
  });
  const replace = db.transaction(({ records, meta }) => {
    q.clear.run();
    for (const record of records) put(record);
    setMeta(meta);
  });
  const saveOp = db.transaction((op, meta) => {
    q.putOp.run(op.seq, JSON.stringify(op));
    setMeta(meta);
  });
  const removeOp = db.transaction((seq, meta) => {
    q.delOp.run(seq);
    setMeta(meta);
  });
  const dropOps = db.transaction((seq, meta) => {
    q.dropOps.run(seq);
    setMeta(meta);
  });

  return {
    load() {
      const row = q.meta.get();
      if (!row) return null;
      return {
        ...JSON.parse(row.value),
        ops: q.ops.all().map(r => JSON.parse(r.op)),
        records: q.records.all().map(r => [r.collection, r.id === '' ? null : r.id, JSON.parse(r.data), JSON.parse(r.clock)])
      };
    },
    commit,
    replace,
    saveOp,
    removeOp,
    dropOps,
    /** The underlying bun:sqlite Database */
    db,
    close: () => db.close()
  };
}
