// sqlite-node.js - Row-per-leaf persistence on node:sqlite (Node 22.13+)
//
//   import { sqliteStorage } from 'lazy-storage/server/sqlite-node';
//   const sqlite = sqliteStorage('data/app.sqlite');
//
// The same schema and rows as the Bun adapter (sqlite-shared.js); the two
// read each other's files. node:sqlite has no transaction helper, so one
// is built from BEGIN/COMMIT here.
import { DatabaseSync } from 'node:sqlite';
import { sqliteStorageOn } from './sqlite-shared.js';

/**
 * @param {string} [file=':memory:'] - database file (created if missing)
 * @param {{ wal?: boolean, lease?: { ttl?: number } | false, checkpoints?: 'worker' | 'inline', onError?: (error: any) => void }} [options] -
 *   write-ahead logging (default on for files); `lease`, `checkpoints` and
 *   `onError` as in sqlite-shared.js (one process per store; the WAL copied
 *   back on a worker thread; a worker that failed, reported)
 */
export function sqliteStorage(file = ':memory:', { wal = true, lease, checkpoints, onError } = {}) {
  const db = new DatabaseSync(file);
  const transaction = fn => (...args) => {
    db.exec('BEGIN');
    try {
      const result = fn(...args);
      db.exec('COMMIT');
      return result;
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  };
  return sqliteStorageOn({
    db,
    driver: 'node',
    exec: sql => db.exec(sql),
    prepare: sql => db.prepare(sql),
    transaction,
    close: () => db.close()
  }, { file, wal, lease, checkpoints, onError });
}
