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

test('the SQLite adapter refuses what it cannot be given, and has nothing to move in a file of 1.0', () => withFile(file => {
  assert.throws(() => sqliteStorage(file, { checkpoints: 'sometimes' }), /checkpoints/);
  const sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  assert.throws(() => sqlite.store(''), /store id/);
  assert.throws(() => sqlite.backup(':memory:'), /file path/);
  assert.deepEqual(sqlite.fromLeaves(), [], 'no leaves table, nothing to move');
  sqlite.close();
}));

test('a document replaced into SQLite keeps its singletons, replicas and owners, and a compaction forgets what the window no longer needs', () => withFile(file => {
  const now = fakeTime(1_000_000_000);
  const sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  sqlite.store('main').replace({
    records: [
      ['tasks', 'a', { title: 'x' }, { live: { '[]': 1, '["title"]': 1 }, gone: { '["old"]': 1 } }],
      ['settings', null, { theme: 'dark' }, { live: { '[]': 1 }, gone: {} }]
    ],
    replicas: { r1: { seq: 4, seen: 1, owner: 'ann' }, r2: { seq: 1, seen: 1 } },
    version: 7,
    epoch: 'old',
    schema: 0
  });
  let store = createStore({ ...SHAPE, now, retention: 86_400_000, storage: sqlite.store('main') });
  assert.equal(store.state.settings.theme, 'dark');
  assert.equal(store.version, 7);
  assert.equal(store.stats().replicas, 0, 'replicas older than the window were forgotten on load');
  assert.equal(store.clock('tasks', 'a').gone['["old"]'], undefined, 'and so was the deletion');
  store.flush();
  store.dispose();
  store = createStore({ ...SHAPE, now, retention: 86_400_000, storage: sqlite.store('main') });
  assert.equal(store.stats().replicas, 0, 'forgotten in the file too');
  assert.deepEqual(sqlite.ids(), ['main']);
  store.dispose();
  sqlite.remove('main');
  assert.deepEqual(sqlite.ids(), []);
  sqlite.close();
}));

test('fromLeaves reads singletons from a whole shape, a key whose initial value is not an object among them', () => withFile(file => {
  const db = new DatabaseSync(file);
  db.exec(`CREATE TABLE leaves (store TEXT NOT NULL, path TEXT NOT NULL, value TEXT, ts_ms INTEGER NOT NULL, ts_count INTEGER NOT NULL, ts_replica TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, since_ms INTEGER, since_count INTEGER, since_replica TEXT, v INTEGER, made_by TEXT, PRIMARY KEY (store, path)) WITHOUT ROWID;
    CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0, epoch TEXT, schema INTEGER, horizon INTEGER) WITHOUT ROWID;
    INSERT INTO stores VALUES ('core', 3, 'old', NULL, 0), ('tokens', 1, 'old', NULL, 0);`);
  const leaf = db.prepare('INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted) VALUES (?, ?, ?, ?, 0, ?, ?)');
  leaf.run('core', JSON.stringify(['build']), JSON.stringify('b-42'), 100, 'server', 0);
  leaf.run('core', JSON.stringify(['users', 'ann', 'name']), JSON.stringify('Ann'), 110, 'server', 0);
  leaf.run('tokens', JSON.stringify(['tokens', 't1', 'device']), JSON.stringify('D1'), 120, 'server', 0);
  db.close();

  // The shape as an app declares it: `build` a singleton by its value, not by a list
  const core = { initial: { users: {}, build: '' } };
  const tokens = { initial: { tokens: {} } };
  let sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  assert.deepEqual(sqlite.fromLeaves(id => (id === 'core' ? core : null)).sort(), ['core', 'tokens']);
  sqlite.close();
  sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  const store = createStore({ ...core, storage: sqlite.store('core') });
  assert.deepEqual(store.snapshot(), { users: { ann: { name: 'Ann' } }, build: 'b-42' });
  const other = createStore({ ...tokens, storage: sqlite.store('tokens') });
  assert.deepEqual(other.snapshot(), { tokens: { t1: { device: 'D1' } } }, 'a store with no shape: every key a collection');
  store.dispose();
  other.dispose();
  sqlite.close();
}));

test('fromLeaves keeps a seed record before 1.0 kept: untouched, edited over its seed, or deleted', () => withFile(file => {
  const db = new DatabaseSync(file);
  db.exec(`CREATE TABLE leaves (store TEXT NOT NULL, path TEXT NOT NULL, value TEXT, ts_ms INTEGER NOT NULL, ts_count INTEGER NOT NULL, ts_replica TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, since_ms INTEGER, since_count INTEGER, since_replica TEXT, v INTEGER, made_by TEXT, PRIMARY KEY (store, path)) WITHOUT ROWID;
    CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0, epoch TEXT, schema INTEGER, horizon INTEGER) WITHOUT ROWID;
    INSERT INTO stores VALUES ('main', 2, 'old', NULL, 0);`);
  const leaf = db.prepare('INSERT INTO leaves (store, path, value, ts_ms, ts_count, ts_replica, deleted) VALUES (?, ?, ?, ?, 0, ?, ?)');
  leaf.run('main', JSON.stringify(['tasks', 'edited', 'done']), JSON.stringify(true), 100, 'a', 0);
  leaf.run('main', JSON.stringify(['tasks', 'deleted']), null, 110, 'a', 1);
  db.close();
  const shape = { initial: { tasks: { untouched: { title: 'Hi' }, edited: { title: 'Do' }, deleted: { title: 'Bye' } } } };
  let sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  sqlite.fromLeaves(() => shape);
  sqlite.close();
  sqlite = sqliteStorage(file, { checkpoints: 'inline' });
  const store = createStore({ ...shape, storage: sqlite.store('main') });
  assert.deepEqual(store.snapshot(), { tasks: { untouched: { title: 'Hi' }, edited: { title: 'Do', done: true } } });
  store.dispose();
  sqlite.close();
}));
