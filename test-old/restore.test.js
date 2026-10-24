// restore.test.js - Putting a copy back while the server runs, and what clients hear
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, createStores, createHub, memoryStorage } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const plain = value => JSON.parse(JSON.stringify(value));

let sqliteStorage = null;
try {
  ({ sqliteStorage } = await import('../src/server/sqlite-node.js'));
} catch { /* node:sqlite missing: those tests skip */ }

/** A registry over `storageFor`, served through hubs on an in-memory network */
function served(storageFor) {
  const stores = createStores(id => (id === 'refused' ? null : createStore({ initial: INITIAL, storage: storageFor(id) })));
  const net = createNetwork({ session: ({ send, user }) => createHub(id => stores.get(id), { send, user }) });
  return { stores, net };
}

/** Deliver until `pred` holds; a client told `unavailable` says hello again within a second */
async function until(net, pred, label) {
  for (let i = 0; i < 300; i++) {
    await net.settle();
    if (pred()) return;
    await sleep(10);
  }
  throw new Error(`timeout: ${label}`);
}

test('stores.restore puts a backup back under a running server: every client hears reset, sees what the server holds, and keeps its unsent edits', { skip: !sqliteStorage }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-restore-'));
  const sqlite = sqliteStorage(join(dir, 'live.sqlite'));
  try {
    const { stores, net } = served(id => sqlite.store(id));
    const a = net.client({ replicaId: 'a', initial: INITIAL });
    const b = net.client({ replicaId: 'b', initial: INITIAL });
    const heard = { a: [], b: [] };
    a.on('reset', r => heard.a.push(r));
    b.on('reset', r => heard.b.push(r));

    a.state.tasks.one = { id: 'one', title: 'before the backup' };
    await net.settle();
    sqlite.backup(join(dir, 'backup.sqlite'));
    a.state.tasks.two = { id: 'two', title: 'after the backup' };
    delete a.state.tasks.one;
    await net.settle();
    b.link.goOffline();
    await net.settle();
    b.state.tasks.three = { id: 'three', title: 'b, offline' };

    const copy = sqliteStorage(join(dir, 'backup.sqlite'));
    const doc = copy.store('main').load();
    copy.close();
    const before = stores.get('main');
    assert.throws(() => stores.restore('main', { rows: [] }), TypeError, 'a document without a version is refused');
    assert.equal(before.disposed, false, '... before anything ends');
    stores.restore('main', doc);
    assert.equal(before.disposed, true);
    assert.equal(stores.has('main'), false, 'the registry let go of the old instance');

    await until(net, () => heard.a.length === 1 && a.status === 'online', 'a heard the reset');
    assert.deepEqual(plain(a.state), { tasks: { one: { id: 'one', title: 'before the backup' } } });
    assert.deepEqual(heard.a[0].previous.state, { tasks: { two: { id: 'two', title: 'after the backup' } } }, 'what a showed, for the app to tell the user');
    assert.equal(heard.a[0].previous.epoch !== heard.a[0].epoch, true);
    assert.equal(heard.a[0].epoch, stores.get('main').epoch);

    b.link.goOnline();
    b.connect();
    await until(net, () => heard.b.length === 1 && b.status === 'online' && b.pending === 0, 'b heard the reset and sent its edit');
    await net.settle();
    const server = stores.get('main').snapshot();
    assert.deepEqual(server.tasks.three, { id: 'three', title: 'b, offline' }, 'the unsent edit was applied on top');
    assert.deepEqual(plain(a.state), server);
    assert.deepEqual(plain(b.state), server);
    assert.equal(heard.a.length, 1, 'once');

    assert.throws(() => stores.restore('refused', doc), err => err.code === 'unknown-store');
    a.dispose();
    b.dispose();
    stores.dispose();
  } finally {
    sqlite.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('store.restore ends the store and replaces its storage; storage that cannot take a document is refused first', () => {
  const disk = memoryStorage();
  const store = createStore({ initial: INITIAL, storage: disk });
  store.patch({ tasks: { a: { id: 'a' } } });
  const doc = store.export();
  store.patch({ tasks: { b: { id: 'b' } } });
  store.restore(doc);
  assert.equal(store.disposed, true);
  assert.throws(() => store.restore(doc), /disposed/);
  const again = createStore({ initial: INITIAL, storage: disk });
  assert.deepEqual(again.snapshot(), { tasks: { a: { id: 'a' } } });
  assert.notEqual(again.epoch, store.epoch);
  again.dispose();

  const bare = createStore({ initial: INITIAL, storage: { load: () => null, commit() {}, flush() {} } });
  assert.throws(() => bare.restore(doc), TypeError);
  assert.equal(bare.disposed, false);
  bare.dispose();
});

test('without leases, SQLite still refuses to replace a store loaded here', { skip: !sqliteStorage }, () => {
  const sqlite = sqliteStorage(':memory:');
  const store = createStore({ initial: INITIAL, storage: sqlite.store('main') });
  store.patch({ tasks: { a: { id: 'a' } } });
  const doc = store.export();
  assert.throws(() => sqlite.store('main').replace(doc), err => err.code === 'store-open');
  store.patch({ tasks: { b: { id: 'b' } } });
  store.restore(doc);
  const again = createStore({ initial: INITIAL, storage: sqlite.store('main') });
  assert.deepEqual(again.snapshot(), { tasks: { a: { id: 'a' } } });
  again.dispose();
  sqlite.close();
});

test('no reset for a client that held nothing: a store that never committed mints an epoch per load', async () => {
  const { stores, net } = served(() => memoryStorage());
  const client = net.client({ replicaId: 'c', initial: INITIAL });
  const heard = [];
  client.on('reset', r => heard.push(r));
  await net.settle();
  const first = stores.get('main').epoch;
  stores.release('main');
  await until(net, () => client.status === 'online' && stores.has('main'), 'back on a fresh load');
  assert.notEqual(stores.get('main').epoch, first, 'a new epoch');
  assert.deepEqual(heard, []);
  client.dispose();
  stores.dispose();
});
