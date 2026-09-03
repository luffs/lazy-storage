// retention.test.js - Old deletions and idle replicas are forgotten; ops older than the window are refused
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const HOUR = 3_600_000;
const DAY = 24 * HOUR;

const rows = storage => Object.fromEntries(storage.load().rows.map(([k, r]) => [k, r.deleted ? 'tombstone' : r.value]));

test('compaction forgets tombstones and replicas older than the window, runs on load and on schedule, and never forgets the server', () => {
  const storage = memoryStorage();
  const time = fakeTime(100 * DAY);
  const store = createStore({ initial: INITIAL, storage, now: time, retention: 30 * DAY, compactEvery: HOUR });
  store.patch({ tasks: { a: { id: 'a' }, b: { id: 'b' } } });
  store.patch({ tasks: { a: null } });
  const session = store.session({ send: () => {} });
  session.receive({ t: 'op', op: { replicaId: 'idle', seq: 1, ts: [100 * DAY, 0, 'idle'], diff: { tasks: { b: { done: true } } } } });
  assert.deepEqual(store.replicas.sort(), ['idle', 'server']);
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone');

  time.advance(20 * DAY);
  store.patch({ tasks: { c: { id: 'c' } } });
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone', 'day 120: the deletion from day 100 is inside the window');
  assert.deepEqual(store.replicas.sort(), ['idle', 'server']);

  time.advance(20 * DAY);
  store.patch({ tasks: { d: { id: 'd' } } });
  assert.equal(rows(storage)['["tasks","a"]'], undefined, 'day 140: forgotten with the next op');
  assert.deepEqual(store.replicas, ['server']);
  assert.deepEqual(Object.keys(storage.load().replicas), ['server'], 'and gone from storage');
  assert.deepEqual(store.compact(), { tombstones: 0, replicas: 0 }, 'nothing left to do');

  time.advance(40 * DAY);
  assert.deepEqual(store.compact(), { tombstones: 0, replicas: 0 });
  assert.deepEqual(store.replicas, ['server'], 'the server replica stays however long it was quiet');
  store.dispose();

  // A store reopened long after its last write compacts on load
  const reopened = createStore({ initial: INITIAL, storage: seeded(80 * DAY), now: fakeTime(200 * DAY), retention: 30 * DAY });
  assert.deepEqual(reopened.replicas, ['server']);
  assert.equal(reopened.compact().tombstones, 0);
});

/** Storage holding a task, a tombstone, and an idle replica, all from `day` */
function seeded(day) {
  const storage = memoryStorage();
  const store = createStore({ initial: INITIAL, storage, now: fakeTime(day), retention: Infinity });
  store.patch({ tasks: { keep: { id: 'keep' }, gone: { id: 'gone' } } });
  store.patch({ tasks: { gone: null } });
  store.session({ send: () => {} }).receive({ t: 'op', op: { replicaId: 'idle', seq: 1, ts: [day, 0, 'idle'], diff: { tasks: { keep: { done: true } } } } });
  store.dispose();
  assert.equal(rows(storage)['["tasks","gone"]'], 'tombstone');
  assert.ok('idle' in storage.load().replicas);
  return storage;
}

test('an op older than the window is refused with code expired; the client drops it and resyncs', async () => {
  const store = createStore({ initial: INITIAL, now: fakeTime(100 * DAY), retention: 30 * DAY });
  store.patch({ tasks: { t1: { id: 't1', title: 'current' } } });
  const net = createNetwork(store);
  const link = net.link();
  // Stamped on day 60 and never synced since: the client has not heard a
  // newer clock, so its op keeps that date
  const stale = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'stale', now: fakeTime(60 * DAY) });
  stale.collection('tasks').add({ id: 'old', title: 'from day 60' });
  await net.settle();
  const errors = [];
  stale.on('error', err => errors.push([err.code, err.message]));

  stale.connect();
  await net.settle();
  assert.deepEqual(errors, [['expired', 'The op is 40 days old, older than the store keeps history for']]);
  assert.equal(store.snapshot().tasks.old, undefined);
  assert.equal(stale.state.tasks.old, undefined, 'the snapshot removed the local copy');
  assert.equal(stale.state.tasks.t1.title, 'current');
  assert.equal(stale.pending, 0);
  assert.equal(stale.status, 'online');

  stale.state.tasks.t1.title = 'edited after syncing';
  await net.settle();
  assert.equal(store.snapshot().tasks.t1.title, 'edited after syncing', 'once synced, the clock is current and edits flow');
});

test('a compacted deletion cannot be resurrected by a write from before it', async () => {
  const time = fakeTime(100 * DAY);
  const store = createStore({ initial: INITIAL, now: time, retention: 30 * DAY });
  store.patch({ tasks: { t1: { id: 't1', title: 'doomed' } } });
  const net = createNetwork(store);
  const late = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: { tasks: { t1: { id: 't1', title: 'doomed' } } }, replicaId: 'late', now: fakeTime(100 * DAY) });
  late.state.tasks.t1.title = 'edited offline on day 100';
  await net.settle();
  assert.equal(late.pending, 1);

  time.advance(DAY);
  store.patch({ tasks: { t1: null } });
  time.advance(39 * DAY);
  assert.equal(store.compact().tombstones, 1, 'day 140: the deletion from day 101 is forgotten');

  late.connect();
  await net.settle();
  assert.equal(store.snapshot().tasks.t1, undefined, 'the stale edit did not bring the record back');
  assert.equal(late.state.tasks.t1, undefined);
  assert.equal(late.pending, 0);
});

test('retention: Infinity keeps every tombstone and replica and accepts ops of any age', async () => {
  const storage = memoryStorage();
  const time = fakeTime(100 * DAY);
  const store = createStore({ initial: INITIAL, storage, now: time, retention: Infinity });
  store.patch({ tasks: { a: { id: 'a' } } });
  store.patch({ tasks: { a: null } });
  time.advance(400 * DAY);
  store.patch({ tasks: { b: { id: 'b' } } });
  assert.deepEqual(store.compact(), { tombstones: 0, replicas: 0 });
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone');

  const net = createNetwork(store);
  const ancient = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'ancient', now: fakeTime(DAY) });
  ancient.collection('tasks').add({ id: 'c' });
  await net.settle();
  ancient.connect();
  await net.settle();
  assert.equal(store.snapshot().tasks.c.id, 'c');
});
