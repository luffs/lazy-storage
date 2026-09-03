// sqlite-bun.js - Row persistence on bun:sqlite, for a client that runs in Bun
//
// The same rows the IndexedDB adapter keeps, in three tables: `leaves`
// (path key -> JSON leaf), `ops` (seq -> JSON op), and `meta` (one row).
// bun:sqlite is synchronous, so this adapter fits createClient() as it
// is: no openClient() needed. A batch is one transaction of the rows it
// touched; a deletion removes the path and, through a range over the
// descendant prefix, everything under it. One file per client.
//
//   const db = createClient({ store: 'team-1', storage: sqliteClientStorage('mirror.sqlite'), ... });
import { Database } from 'bun:sqlite';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS leaves (path TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS ops (seq INTEGER PRIMARY KEY, op TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
`;

/** The bounds of every strict descendant of a path key: keys share the ancestor's key minus `]`, plus `,` */
const descendants = key => {
  const prefix = key.slice(0, -1) + ',';
  return [prefix, prefix + '￿'];
};

/**
 * @param {string} [file=':memory:'] - database file (created if missing)
 * @param {{ wal?: boolean }} [options] - write-ahead logging (default on for files)
 */
export function sqliteClientStorage(file = ':memory:', { wal = true } = {}) {
  const db = new Database(file, { create: true });
  if (wal && file !== ':memory:') db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
  db.exec(SCHEMA);

  const q = {
    put: db.prepare('INSERT INTO leaves (path, value) VALUES (?, ?) ON CONFLICT (path) DO UPDATE SET value = excluded.value'),
    del: db.prepare('DELETE FROM leaves WHERE path = ?'),
    delRange: db.prepare('DELETE FROM leaves WHERE path >= ? AND path < ?'),
    clear: db.prepare('DELETE FROM leaves'),
    rows: db.prepare('SELECT path, value FROM leaves'),
    putOp: db.prepare('INSERT INTO ops (seq, op) VALUES (?, ?) ON CONFLICT (seq) DO UPDATE SET op = excluded.op'),
    dropOps: db.prepare('DELETE FROM ops WHERE seq <= ?'),
    ops: db.prepare('SELECT op FROM ops ORDER BY seq'),
    meta: db.prepare("SELECT value FROM meta WHERE key = 'meta'"),
    setMeta: db.prepare("INSERT INTO meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value")
  };
  const setMeta = meta => q.setMeta.run(JSON.stringify(meta));

  const commit = db.transaction(({ puts, deletes, meta }) => {
    for (const key of deletes) {
      q.del.run(key);
      q.delRange.run(...descendants(key));
    }
    for (const [key, value] of puts) q.put.run(key, JSON.stringify(value));
    setMeta(meta);
  });
  const replace = db.transaction(({ rows, meta }) => {
    q.clear.run();
    for (const [key, value] of rows) q.put.run(key, JSON.stringify(value));
    setMeta(meta);
  });
  const saveOp = db.transaction((op, meta) => {
    q.putOp.run(op.seq, JSON.stringify(op));
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
        rows: q.rows.all().map(r => [r.path, JSON.parse(r.value)])
      };
    },
    commit,
    replace,
    saveOp,
    dropOps,
    /** The underlying bun:sqlite Database */
    db,
    close: () => db.close()
  };
}
