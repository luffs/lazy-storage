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
 * @param {{ wal?: boolean }} [options] - write-ahead logging (default on for
 *   files; readers never block a commit)
 */
export function sqliteStorage(file = ':memory:', { wal = true } = {}) {
  const db = new Database(file, { create: true });
  return sqliteStorageOn({
    db,
    exec: sql => db.exec(sql),
    prepare: sql => db.prepare(sql),
    transaction: fn => db.transaction(fn),
    close: () => db.close()
  }, { file, wal });
}
