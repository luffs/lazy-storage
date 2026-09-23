// indexeddb.test.js - The IndexedDB adapter, against fake-indexeddb
import 'fake-indexeddb/auto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createClient, openClient } from '../src/client/index.js';
import { indexedDBStorage } from '../src/client/indexeddb.js';
import { rebuild } from '../src/core/model.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const snap = client => LazyWatch.snapshot(client.state);

test('a client on IndexedDB is opened with openClient, writes rows per batch, and comes back from them', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const link = net.link();
  const storage = indexedDBStorage('lazy-storage-test');
  assert.throws(() => createClient({ transport: link.factory, store: 'main', initial: INITIAL, storage }), /openClient/);
  assert.equal(await storage.load(), null, 'a fresh database');

  const a = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'a' });
  a.connect();
  await net.settle();
  a.collection('tasks').add({ id: 't1', title: 'one', sub: { s1: { id: 's1' }, s2: { id: 's2' } } });
  a.state.order.push('t1');
  await net.settle();
  a.collection('tasks').add({ id: 't2', title: 'two' });
  await net.settle();
  await storage.settled();
  let saved = await storage.load();
  assert.deepEqual(rebuild(INITIAL, saved.rows), snap(a));
  assert.deepEqual(saved.ops, [], 'acked');
  assert.equal(saved.seq, 2);
  assert.equal(saved.version, store.version);
  assert.equal(saved.epoch, store.epoch);

  link.goOffline();
  await net.settle();
  delete a.state.tasks.t1;                 // a container: its descendant rows go with it
  a.state.order = ['t2'];
  await net.settle();
  a.state.tasks.t2.title = 'two, edited offline';
  await net.settle();
  await storage.settled();
  saved = await storage.load();
  assert.deepEqual(saved.ops.map(op => op.seq), [3, 4], 'the pending ops are rows too');
  assert.equal(saved.rows.some(([key]) => key.startsWith('["tasks","t1"')), false, 'the range delete took the descendants');
  const before = snap(a);
  assert.deepEqual(rebuild(INITIAL, saved.rows), before);
  a.dispose();
  storage.close();

  const again = indexedDBStorage('lazy-storage-test');
  const b = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage: again });
  assert.equal(b.restored, true);
  assert.equal(b.replicaId, 'a');
  assert.equal(b.pending, 2);
  assert.deepEqual(snap(b), before);
  link.goOnline();
  b.connect();
  await net.settle();
  assert.equal(b.pending, 0);
  assert.deepEqual(store.snapshot().tasks.t2.title, 'two, edited offline');
  await again.settled();
  saved = await again.load();
  assert.deepEqual(saved.ops, []);
  assert.deepEqual(rebuild(INITIAL, saved.rows), store.snapshot());
  b.dispose();

  await again.destroy();
  const gone = indexedDBStorage('lazy-storage-test');
  assert.equal(await gone.load(), null, 'destroy() dropped the database');
  gone.close();
});

test('a snapshot replaces the rows, and a closed adapter reopens for a later write', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  store.patch({ tasks: { real: { id: 'real' } } });
  const net = createNetwork(store);
  const link = net.link();
  const errors = [];
  const seed = indexedDBStorage('lazy-storage-seeded', { onError: err => errors.push(err) });
  seed.replace({ rows: [['["junk"]', 1]], meta: { replicaId: 'a', seq: 0, version: 0, epoch: null } });
  await seed.settled();

  const a = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage: seed });
  assert.equal(a.state.junk, 1);
  a.connect();
  await net.settle();
  await seed.settled();
  const saved = await seed.load();
  assert.deepEqual(rebuild(INITIAL, saved.rows), store.snapshot());
  assert.equal(a.state.junk, undefined);
  assert.deepEqual(errors, []);

  seed.close();
  seed.commit({ puts: [['["x"]', 1]], deletes: [], meta: { replicaId: 'a', seq: 0, version: 1, epoch: 'e' } });
  await seed.settled();
  assert.equal(errors.length, 0, 'a closed adapter reopens for a later write');
  a.dispose();
  await seed.destroy();
});

test('a database another tab deleted is made anew and written whole again: every row, every pending op', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  store.patch({ tasks: { a: { id: 'a', title: 'from the server' } } });
  const net = createNetwork(store);
  const link = net.link();
  const errors = [];
  const storage = indexedDBStorage('lazy-storage-reset', { onError: err => errors.push(err) });
  const client = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'r' });
  client.connect();
  await net.settle();
  link.goOffline();
  await net.settle();
  client.state.tasks.b = { id: 'b', title: 'offline one' };
  await new Promise(resolve => setImmediate(resolve));
  await storage.settled();

  // Another tab deletes the database: this one steps aside, and it is gone
  const other = indexedDBStorage('lazy-storage-reset');
  await other.destroy();

  client.state.tasks.c = { id: 'c', title: 'offline two' };   // the next write opens it anew
  await new Promise(resolve => setImmediate(resolve));
  await storage.settled();
  await new Promise(resolve => setTimeout(resolve, 20));
  await storage.settled();

  const saved = await indexedDBStorage('lazy-storage-reset').load();
  assert.ok(saved, 'there is a database again');
  assert.deepEqual(saved.ops.map(op => op.seq), [1, 2], 'both pending ops, the one from before the deletion too');
  assert.deepEqual(rebuild(INITIAL, saved.rows).tasks, {
    a: { id: 'a', title: 'from the server' },
    b: { id: 'b', title: 'offline one' },
    c: { id: 'c', title: 'offline two' }
  }, 'every row, not only the ones written since');
  assert.deepEqual(errors, []);
  client.dispose();
  await storage.destroy();
});
