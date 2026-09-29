// sqlite-bun.js - Row-per-leaf persistence on bun:sqlite
//
//   const sqlite = sqliteStorage('data/app.sqlite');
//   const store = createStore({ initial, registers, storage: sqlite.store('team-1') });
//
// See sqlite-shared.js for the schema and the row model; this file only
// supplies the driver.
import { Database } from 'bun:sqlite';
import { sqliteStorageOn } from './sqlite-shared.js';

/**
 * @param {string} [file=':memory:'] - database file (created if missing)
 * @param {{ wal?: boolean, lease?: { ttl?: number } | false, checkpoints?: 'worker' | 'inline' }} [options] -
 *   write-ahead logging (default on for files; readers never block a
 *   commit); `lease` and `checkpoints` as in sqlite-shared.js (one process
 *   per store; the WAL copied back on a worker thread)
 */
export function sqliteStorage(file = ':memory:', { wal = true, lease, checkpoints } = {}) {
  const db = new Database(file, { create: true });
  return sqliteStorageOn({
    db,
    driver: 'bun',
    exec: sql => db.exec(sql),
    prepare: sql => db.prepare(sql),
    transaction: fn => db.transaction(fn),
    close: () => db.close()
  }, { file, wal, lease, checkpoints });
}
