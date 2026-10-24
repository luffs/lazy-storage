// retention.test.js - Old deletions and idle replicas are forgotten; ops older than the window are refused
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';
import { PROTOCOL } from '../src/core/index.js';

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
  store.flush();   // stored at the end of the turn (see groupCommit), or now
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone');

  time.advance(20 * DAY);
  store.patch({ tasks: { c: { id: 'c' } } });
  store.flush();
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone', 'day 120: the deletion from day 100 is inside the window');
  assert.deepEqual(store.replicas.sort(), ['idle', 'server']);

  time.advance(20 * DAY);
  store.patch({ tasks: { d: { id: 'd' } } });
  store.flush();
  assert.equal(rows(storage)['["tasks","a"]'], undefined, 'day 140: forgotten with the next op');
  assert.deepEqual(store.replicas, ['server']);
  assert.deepEqual(Object.keys(storage.load().replicas), ['server'], 'and gone from storage');
  assert.deepEqual(store.compact(), { tombstones: 0, barriers: 0, replicas: 0 }, 'nothing left to do');

  time.advance(40 * DAY);
  assert.deepEqual(store.compact(), { tombstones: 0, barriers: 0, replicas: 0 });
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
  assert.deepEqual(store.compact(), { tombstones: 0, barriers: 0, replicas: 0 });
  store.flush();
  assert.equal(rows(storage)['["tasks","a"]'], 'tombstone');

  const net = createNetwork(store);
  const ancient = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'ancient', now: fakeTime(DAY) });
  ancient.collection('tasks').add({ id: 'c' });
  await net.settle();
  ancient.connect();
  await net.settle();
  assert.equal(store.snapshot().tasks.c.id, 'c');
});

test('a settled barrier moves the horizon up: an op made in sight of less is refused as expired, after a reload too', async () => {
  const storage = memoryStorage();
  const time = fakeTime(100 * DAY);
  let store = createStore({ initial: INITIAL, storage, now: time, retention: 30 * DAY });
  store.patch({ tasks: { x: { id: 'x', meta: { note: 'first' } } } });
  store.patch({ tasks: { x: { meta: null } } });
  const session = store.session({ send: () => {} });
  // Replica a re-makes the container on day 100, at version 2 of its knowledge: the barrier becomes version 3
  session.receive({ t: 'op', op: { replicaId: 'a', seq: 1, ts: [100 * DAY + 1000, 0, 'a'], v: 2, epoch: store.epoch, diff: { tasks: { x: { meta: { note: 'again' } } } }, made: [['tasks', 'x', 'meta']] } });
  store.flush();
  assert.equal(store.stats().barriers, 1);
  assert.equal(store.stats().horizon, 0);
  // Day 140: the barrier settles, and the horizon is its version
  time.advance(40 * DAY);
  assert.deepEqual(store.compact(), { tombstones: 0, barriers: 1, replicas: 1 }, 'the barrier settled, and a, idle since day 100, is forgotten');
  assert.equal(store.stats().horizon, 3);
  assert.equal(store.export().horizon, 3);
  // b, whose view is from before the re-add, edits the old container on day 140 (a fresh stamp): refused, and told why
  const heard = [];
  const b = store.session({ send: m => heard.push(m) });
  b.receive({ t: 'hello', proto: PROTOCOL, replicaId: 'b', ops: [] });
  b.receive({ t: 'op', op: { replicaId: 'b', seq: 1, ts: [140 * DAY, 0, 'b'], v: 2, epoch: store.epoch, diff: { tasks: { x: { meta: { color: 'red' } } } } } });
  store.flush();
  const refused = heard.find(m => m.t === 'error' && m.seq === 1);
  assert.equal(refused?.code, 'expired');
  assert.match(refused.message, /version 2.*before 3/);
  assert.equal(store.snapshot().tasks.x.meta.color, undefined);
  // In sight of the re-add, the same edit lands; and a never-synced replica (no epoch) is judged by what it writes, as ever
  b.receive({ t: 'op', op: { replicaId: 'b', seq: 2, ts: [140 * DAY, 1, 'b'], v: 3, epoch: store.epoch, diff: { tasks: { x: { meta: { color: 'red' } } } } } });
  store.flush();
  assert.equal(store.snapshot().tasks.x.meta.color, 'red');
  // The horizon survives a reload
  store.dispose();
  store = createStore({ initial: INITIAL, storage, now: time, retention: 30 * DAY });
  assert.equal(store.stats().horizon, 3);
  const c = store.session({ send: m => heard.push(m) });
  c.receive({ t: 'hello', proto: PROTOCOL, replicaId: 'c', ops: [] });
  c.receive({ t: 'op', op: { replicaId: 'c', seq: 1, ts: [140 * DAY, 2, 'c'], v: 1, epoch: store.epoch, diff: { tasks: { x: { meta: { size: 2 } } } } } });
  store.flush();
  assert.equal(heard.filter(m => m.t === 'error' && m.code === 'expired').length, 2);
  store.dispose();
});
