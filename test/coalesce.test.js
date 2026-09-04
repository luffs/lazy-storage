// coalesce.test.js - The outbox: a newer op takes over from what older pending ops wrote at or under its paths
import 'fake-indexeddb/auto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient, openClient } from '../src/client/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { indexedDBStorage } from '../src/client/indexeddb.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
/** Lets lazy-watch close the current batch, so the next write is a new op */
const tick = () => new Promise(resolve => setImmediate(resolve));

/** A client on its own link that has not connected yet, so it edits offline first; `outbox()` reads what it persisted */
function offlineClient(net, options = {}) {
  const link = net.link();
  const storage = memoryOutbox();
  const client = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, ...options });
  client.link = link;
  client.outbox = () => storage.load()?.ops ?? [];
  return client;
}

test('typing into one field keeps one op pending beside the record write, and the server merges only those', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const c = offlineClient(net, { replicaId: 'c' });
  c.state.tasks.x = { id: 'x', title: 'h', done: false };
  await tick();
  for (const title of ['he', 'hel', 'hell', 'hello']) {
    c.state.tasks.x.title = title;
    await tick();
  }
  assert.equal(c.pending, 2);
  assert.deepEqual(c.outbox().map(op => op.diff), [
    { tasks: { x: { id: 'x', done: false } } },
    { tasks: { x: { title: 'hello' } } }
  ], 'the record write lost its title but kept its id; the last title stands alone');
  assert.deepEqual(c.outbox().map(op => op.seq), [1, 5], 'superseded ops leave gaps in the sequence');

  const merged = [];
  store.observe('op', e => merged.push(e.seq));
  c.connect();
  await net.settle();
  assert.equal(c.pending, 0);
  assert.deepEqual(merged, [1, 5], 'the server saw two ops, and accepts the gaps');
  assert.deepEqual(store.snapshot().tasks.x, { id: 'x', title: 'hello', done: false });
});

test('edits to different fields stay separate ops with their own stamps, so a remote edit between them still wins', async () => {
  const now = fakeTime();
  const store = createStore({ initial: INITIAL, registers: REGISTERS, now });
  store.patch({ tasks: { x: { id: 'x', title: 'start', done: false } } });
  const net = createNetwork(store);
  const other = net.client({ replicaId: 'o', initial: INITIAL, registers: REGISTERS, now });
  const c = net.client({ replicaId: 'c', initial: INITIAL, registers: REGISTERS, now });
  await net.settle();
  c.link.goOffline();
  await net.settle();

  now.advance(1000);
  c.state.tasks.x.title = 'mine';
  await tick();
  now.advance(1000);
  other.state.tasks.x.title = 'theirs';
  await net.settle();
  now.advance(1000);
  c.state.tasks.x.done = true;
  await tick();
  assert.equal(c.pending, 2, 'another field: nothing to take over');

  c.link.goOnline();
  c.connect();
  await net.settle();
  assert.equal(c.pending, 0);
  assert.deepEqual(store.snapshot().tasks.x, { id: 'x', title: 'theirs', done: true }, 'the older title kept its older stamp and lost');
  assert.equal(c.state.tasks.x.title, 'theirs', 'and the client was corrected');
});

test('deleting a record drops its pending edits; re-adding it keeps the deletion ahead of the new record', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const c = offlineClient(net, { replicaId: 'c' });
  c.state.tasks.x = { id: 'x', title: 'one' };
  await tick();
  c.state.tasks.x.title = 'two';
  await tick();
  c.state.tasks.y = { id: 'y', title: 'stays' };
  await tick();
  delete c.state.tasks.x;
  await tick();
  assert.deepEqual(c.outbox().map(op => op.diff), [
    { tasks: { y: { id: 'y', title: 'stays' } } },
    { tasks: { x: null } }
  ], 'the record write and its edit went with the deletion; the other record is untouched');

  c.state.tasks.x = { id: 'x', title: 'three' };
  await tick();
  assert.equal(c.pending, 3, 'a record write under a pending deletion does not take it over');
  c.connect();
  await net.settle();
  assert.equal(c.pending, 0);
  assert.deepEqual(store.snapshot().tasks, { x: { id: 'x', title: 'three' }, y: { id: 'y', title: 'stays' } });
});

test('a register written again takes over from the earlier write, like any leaf', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const c = offlineClient(net, { replicaId: 'c' });
  c.state.order = ['a'];
  await tick();
  c.state.order.push('b');
  await tick();
  c.state.order.push('c');
  await tick();
  assert.equal(c.pending, 1);
  assert.deepEqual(c.outbox()[0].diff, { order: ['a', 'b', 'c'] });
  c.connect();
  await net.settle();
  assert.deepEqual(store.snapshot().order, ['a', 'b', 'c']);
});

test('an op already sent but not yet acknowledged is taken over as well, and the outcome is unchanged', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const c = net.client({ replicaId: 'c', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  c.state.tasks.x = { id: 'x', title: 'h' };
  await net.settle();
  c.state.tasks.x.title = 'a';
  await tick();
  assert.equal(net.pending, 1, 'the op is on the wire');
  c.state.tasks.x.title = 'b';
  await tick();
  assert.equal(c.pending, 1, 'the one in flight left the outbox');
  await net.settle();
  assert.equal(c.pending, 0, 'the acks for both, one of them for an op no longer held, leave nothing behind');
  assert.equal(store.snapshot().tasks.x.title, 'b');
});

test('a row adapter rewrites a pruned op and removes an emptied one, so a restart loads the smaller outbox', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const link = net.link();
  const storage = indexedDBStorage('lazy-storage-coalesce');
  const open = () => openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'r' });
  const a = await open();
  a.state.tasks.x = { id: 'x', title: 'h' };
  await tick();
  a.state.tasks.x.title = 'he';
  await tick();
  a.state.tasks.x.title = 'hel';
  await tick();
  await storage.settled();
  const saved = await storage.load();
  assert.deepEqual(saved.ops.map(op => [op.seq, op.diff]), [[1, { tasks: { x: { id: 'x' } } }], [3, { tasks: { x: { title: 'hel' } } }]]);
  assert.equal(saved.seq, 3);
  a.dispose();

  const b = await open();
  assert.equal(b.pending, 2);
  assert.equal(b.state.tasks.x.title, 'hel');
  b.connect();
  await net.settle();
  assert.equal(b.pending, 0);
  assert.deepEqual(store.snapshot().tasks.x, { id: 'x', title: 'hel' });
  b.dispose();
  await storage.destroy();
});
