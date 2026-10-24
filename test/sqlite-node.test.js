import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { sqliteStorage } from '../src/server/sqlite-node.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };

function withFile(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-sqlite-'));
  // A failure leaves the file open, and its error, not the cleanup's, is the one to see
  return Promise.resolve(fn(join(dir, 'app.sqlite'))).finally(() => { try { rmSync(dir, { recursive: true, force: true }); } catch { /* left for the OS */ } });
}

test('records, clocks, replicas and the log survive a restart on SQLite', () => withFile(async file => {
  const now = fakeTime();
  let sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  let store = createStore({ ...SHAPE, now, storage: sqlite.store('main') });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now });
  await net.settle();
  a.state.tasks.k = { title: 'Milk', meta: { color: 'red' } };
  a.state.settings.theme = 'dark';
  await net.settle();
  a.state.tasks.k.meta.color = 'blue';
  delete a.state.tasks.k.title;
  await net.settle();
  store.flush();
  const before = store.snapshot();
  const clock = store.clock('tasks', 'k');
  const version = store.version;
  store.dispose();
  sqlite.close();

  sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  store = createStore({ ...SHAPE, now, storage: sqlite.store('main') });
  assert.deepEqual(store.snapshot(), before);
  assert.deepEqual(store.clock('tasks', 'k'), clock);
  assert.equal(store.version, version);
  assert.equal(store.stats().log, 2, 'two batches, two entries');
  // An old write of the deleted title still loses after the restart
  store.apply({ replica: 'b', seq: 1, ts: 1, changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'old' }] });
  assert.equal(store.state.tasks.k.title, undefined);
  store.dispose();
  sqlite.close();
  assert.ok(LazyWatch);
}));

test('a file from before 1.0 is refused until fromLeaves moves it', () => withFile(file => {
  const db = new DatabaseSync(file);
  db.exec(`CREATE TABLE leaves (store TEXT NOT NULL, path TEXT NOT NULL, value TEXT, ts_ms INTEGER NOT NULL, ts_count INTEGER NOT NULL, ts_replica TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, since_ms INTEGER, since_count INTEGER, since_replica TEXT, v INTEGER, made_by TEXT, PRIMARY KEY (store, path)) WITHOUT ROWID;
    CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0, epoch TEXT, schema INTEGER, horizon INTEGER) WITHOUT ROWID;
    INSERT INTO stores VALUES ('main', 7, 'old', NULL, 0);`);
  const leaf = db.prepare('INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted) VALUES (?, ?, ?, ?, 0, ?, ?)');
  leaf.run('main', JSON.stringify(['tasks', 'k', 'title']), JSON.stringify('Milk'), 100, 'a', 0);
  leaf.run('main', JSON.stringify(['tasks', 'k', 'meta', 'color']), JSON.stringify('red'), 110, 'a', 0);
  leaf.run('main', JSON.stringify(['tasks', 'k', 'note']), null, 120, 'a', 1);
  leaf.run('main', JSON.stringify(['tasks', 'gone']), null, 130, 'a', 1);
  leaf.run('main', JSON.stringify(['settings', 'theme']), JSON.stringify('dark'), 140, 'a', 0);
  db.close();

  const now = fakeTime();
  let sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  assert.throws(() => createStore({ ...SHAPE, now, storage: sqlite.store('main') }), err => err.code === 'storage-format');
  assert.deepEqual(sqlite.fromLeaves(() => SHAPE), ['main']);
  sqlite.close();
  sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  const store = createStore({ ...SHAPE, now, storage: sqlite.store('main') });
  assert.deepEqual(store.snapshot(), { tasks: { k: { title: 'Milk', meta: { color: 'red' } } }, settings: { theme: 'dark' } });
  assert.deepEqual(store.clock('tasks', 'k'), { live: { '["title"]': 100, '["meta","color"]': 110 }, gone: { '["note"]': 120 } });
  assert.notEqual(store.epoch, 'old');
  assert.equal(store.version, 7);
  store.dispose();
  sqlite.close();
}));
