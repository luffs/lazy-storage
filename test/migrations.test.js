// migrations.test.js - Changing the shape of the state, once per store, before anyone is served
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, createHub, memoryStorage } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };

// Tasks had a `title`; the new shape calls it `name` and adds a priority
const renameTitle = state => ({
  tasks: Object.fromEntries(Object.entries(state.tasks).map(([id, task]) => [id, { name: task.title, title: null }]))
});
const addPriority = state => ({
  tasks: Object.fromEntries(Object.keys(state.tasks).map(id => [id, { priority: 'normal' }]))
});

/** A storage that records what it is asked to commit */
function recorded(storage = memoryStorage()) {
  const commits = [];
  return {
    commits,
    storage: {
      load: () => storage.load(),
      commit(change) { commits.push(change); storage.commit(change); },
      flush: () => storage.flush(),
      close() { this.closed = true; }
    }
  };
}

test('a store stored before its migrations runs them all, in order, once, each with its rows in one commit', () => {
  const disk = memoryStorage();
  const old = createStore({ initial: INITIAL, storage: disk });
  old.patch({ tasks: { a: { id: 'a', title: 'Milk' }, b: { id: 'b', title: 'Eggs' } } });
  old.dispose();

  let runs = 0;
  const counted = state => { runs++; return renameTitle(state); };
  const { storage, commits } = recorded(disk);
  const store = createStore({ initial: INITIAL, storage, migrations: [counted, addPriority] });
  assert.deepEqual(store.state.tasks, {
    a: { id: 'a', name: 'Milk', priority: 'normal' },
    b: { id: 'b', name: 'Eggs', priority: 'normal' }
  });
  assert.equal(store.stats().schema, 2);
  assert.deepEqual(commits.map(c => [c.schema, c.upserts.length > 0]), [[1, true], [2, true]], 'each migration\'s rows and its count go together');
  store.dispose();

  const again = createStore({ initial: INITIAL, storage: disk, migrations: [counted, addPriority] });
  assert.equal(runs, 1, 'not run again on the next load');
  assert.equal(again.state.tasks.a.name, 'Milk');
  again.dispose();
});

test('a new store starts with every migration done; one added later runs alone', () => {
  const disk = memoryStorage();
  let renamed = 0;
  const store = createStore({ initial: INITIAL, storage: disk, migrations: [state => { renamed++; return renameTitle(state); }] });
  assert.equal(renamed, 0, 'initial is in the latest shape already');
  store.patch({ tasks: { a: { id: 'a', name: 'Milk' } } });
  assert.equal(disk.load().schema, 1, 'the count is stored with the first commit');
  store.dispose();

  const later = createStore({ initial: INITIAL, storage: disk, migrations: [state => { renamed++; return renameTitle(state); }, addPriority] });
  assert.equal(renamed, 0);
  assert.deepEqual(later.state.tasks.a, { id: 'a', name: 'Milk', priority: 'normal' });
  later.dispose();
});

test('storage a newer version migrated is not served by an older one, and a migration that throws stops the load', () => {
  const disk = memoryStorage();
  const newer = createStore({ initial: INITIAL, storage: disk, migrations: [renameTitle, addPriority] });
  newer.patch({ tasks: { a: { id: 'a' } } });
  newer.dispose();
  assert.throws(() => createStore({ initial: INITIAL, storage: disk, migrations: [renameTitle] }), err => err.code === 'schema-ahead');

  const { storage } = recorded(disk);
  assert.throws(
    () => createStore({ initial: INITIAL, storage, migrations: [renameTitle, addPriority, () => { throw new Error('bad data'); }] }),
    err => err.message === 'Migration 2 failed: bad data' && err.cause.message === 'bad data'
  );
  assert.equal(storage.closed, true, 'the storage was let go');
  assert.equal(disk.load().schema, 2, 'the count stays where it was');
});

test("clients meet the migrated state: a snapshot for a new one, the migration's patch in a delta for one that was here before", async () => {
  const disk = memoryStorage();
  let current = createStore({ initial: INITIAL, storage: disk });
  const net = createNetwork({ session: ({ send, user }) => createHub(() => current, { send, user }) });
  const client = net.client({ replicaId: 'c', initial: INITIAL });
  current.patch({ tasks: { a: { id: 'a', title: 'Milk' } } });
  await net.settle();
  client.link.goOffline();
  await net.settle();
  current.dispose();

  // A deploy: the next process's store migrates on load
  current = createStore({ initial: INITIAL, storage: disk, migrations: [renameTitle] });
  const fresh = net.client({ replicaId: 'f', initial: INITIAL });
  client.link.goOnline();
  client.connect();
  const answers = [];
  const session = current.session;
  current.session = options => session({ ...options, send: m => { answers.push(m.t); options.send(m); } });
  await net.settle();
  assert.deepEqual({ ...fresh.state.tasks.a }, { id: 'a', name: 'Milk' });
  assert.deepEqual({ ...client.state.tasks.a }, { id: 'a', name: 'Milk' }, 'the client that was here before follows');
  assert.deepEqual(answers.filter(t => t === 'snapshot' || t === 'delta').sort(), ['delta', 'snapshot'], 'the one that was here catches up with a delta');
  current.dispose();
});

test('the count survives a reopen of a SQLite file, which gains the column it is kept in', async t => {
  let sqliteStorage;
  try {
    ({ sqliteStorage } = await import('../src/server/sqlite-node.js'));
  } catch {
    return t.skip('node:sqlite unavailable');
  }
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-migrate-'));
  const file = join(dir, 'm.sqlite');
  try {
    const { DatabaseSync } = await import('node:sqlite');
    const old = new DatabaseSync(file);   // a file from before stores had a schema
    old.exec(`CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0, epoch TEXT) WITHOUT ROWID;
      CREATE TABLE leaves (store TEXT NOT NULL, path TEXT NOT NULL, value TEXT, ts_ms INTEGER NOT NULL, ts_count INTEGER NOT NULL, ts_replica TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (store, path)) WITHOUT ROWID;
      INSERT INTO stores VALUES ('main', 1, 'e1');
      INSERT INTO leaves VALUES ('main', '["tasks","a","id"]', '"a"', 1, 0, 'server', 0);
      INSERT INTO leaves VALUES ('main', '["tasks","a","title"]', '"Milk"', 1, 0, 'server', 0);`);
    old.close();

    let sqlite = sqliteStorage(file);
    const one = createStore({ initial: INITIAL, storage: sqlite.store('main'), migrations: [renameTitle] });
    assert.deepEqual(one.state.tasks.a, { id: 'a', name: 'Milk' });
    one.dispose();
    sqlite.close();

    sqlite = sqliteStorage(file);
    assert.equal(sqlite.store('main').load().schema, 1);
    let ran = false;
    const two = createStore({ initial: INITIAL, storage: sqlite.store('main'), migrations: [state => { ran = true; return renameTitle(state); }] });
    assert.equal(ran, false);
    assert.deepEqual(two.state.tasks.a, { id: 'a', name: 'Milk' });
    two.dispose();
    sqlite.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
