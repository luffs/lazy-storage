// retention.test.js - One window: ops older than it are refused, deletions and replicas older than it forgotten
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const DAY = 86_400_000;
const SHAPE = { initial: { tasks: {} } };

test('an op stamped before the window is refused as expired; the client takes it back', async () => {
  const now = fakeTime(100 * DAY);
  const store = createStore({ ...SHAPE, now, retention: 30 * DAY });
  const net = createNetwork(store);
  const clientNow = fakeTime(100 * DAY);
  const a = net.client({ replicaId: 'a', ...SHAPE, now: clientNow });
  await net.settle();
  a.link.goOffline();
  await net.settle();
  a.state.tasks.old = { n: 1 };
  await net.settle();
  now.advance(40 * DAY);
  clientNow.advance(40 * DAY);
  const rejected = [];
  a.on('rejected', e => rejected.push(e.code));
  a.link.goOnline(); a.connect();
  await net.settle();
  assert.deepEqual(rejected, ['expired']);
  assert.equal(a.state.tasks.old, undefined);
  assert.equal(a.pending, 0);
});

test('deletions inside records and replicas older than the window are forgotten, and nothing they guarded comes back', () => {
  const now = fakeTime(100 * DAY);
  const storage = memoryStorage();
  const store = createStore({ ...SHAPE, now, retention: 30 * DAY, storage });
  store.apply({ replica: 'r1', seq: 1, ts: now(), changes: [{ c: 'tasks', id: 'k', put: { title: 'Milk', note: 'x' } }] });
  now.advance(DAY);
  store.apply({ replica: 'r1', seq: 2, ts: now(), changes: [{ c: 'tasks', id: 'k', path: ['note'], del: true }] });
  assert.deepEqual(Object.keys(store.clock('tasks', 'k').gone), ['["note"]']);
  now.advance(40 * DAY);
  const removed = store.compact();
  assert.deepEqual(removed, { deletions: 1, replicas: 1 });
  assert.deepEqual(store.clock('tasks', 'k').gone, {});
  // A write from before the deletion would have lost to it; it is now older than the window, so it is refused
  assert.throws(() => store.apply({ replica: 'r2', seq: 1, ts: 100 * DAY + 10, changes: [{ c: 'tasks', id: 'k', path: ['note'], set: 'old' }] }), err => err.code === 'expired');
  assert.equal(store.state.tasks.k.note, undefined);
  store.flush();
  assert.deepEqual(Object.keys(storage.load().replicas), [], 'the replica forgotten in storage too');
});

test('retention: Infinity keeps every deletion and replica and takes ops of any age', () => {
  const now = fakeTime(1000 * DAY);
  const store = createStore({ ...SHAPE, now, retention: Infinity });
  store.apply({ replica: 'r1', seq: 1, ts: 1, changes: [{ c: 'tasks', id: 'k', put: { a: 1 } }] });
  store.apply({ replica: 'r1', seq: 2, ts: 2, changes: [{ c: 'tasks', id: 'k', path: ['a'], del: true }] });
  assert.deepEqual(store.compact(), { deletions: 0, replicas: 0 });
  assert.deepEqual(Object.keys(store.clock('tasks', 'k').gone), ['["a"]']);
});
