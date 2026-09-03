// clock.test.js - The clock guard: a client running ahead is corrected, not obeyed
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const START = 1_000_000_000;

const rowTs = (storage, key) => storage.load().rows.find(([k]) => k === key)[1].ts;

test('an op stamped an hour ahead is refused, the client adopts the server time, re-stamps, and the edit lands with a sane timestamp', async () => {
  const storage = memoryStorage();
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, storage, now: time });
  const net = createNetwork(store);
  const fast = fakeTime(START + HOUR);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: fast });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now: fakeTime(START) });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err.code));

  a.collection('tasks').add({ id: 't1', title: 'from the future' });
  await net.settle();
  assert.equal(store.snapshot().tasks.t1.title, 'from the future', 'the edit was not lost');
  assert.equal(b.state.tasks.t1.title, 'from the future');
  assert.equal(a.pending, 0);
  assert.deepEqual(errors, [], 'a corrected clock is not an error the app hears about');
  const ts = rowTs(storage, '["tasks","t1","title"]');
  assert.ok(ts[0] >= START && ts[0] <= START + MINUTE, `re-stamped near server time, got ${ts[0] - START} ms after`);

  // The correction sticks: the next op passes first time, and a's clock now
  // runs on server time
  fast.advance(10);
  time.advance(10);
  a.state.tasks.t1.done = true;
  await net.settle();
  assert.equal(store.snapshot().tasks.t1.done, true);
  const ts2 = rowTs(storage, '["tasks","t1","done"]');
  assert.ok(ts2[0] >= START && ts2[0] <= START + MINUTE);

  // The server's own clock was never dragged forward by the refused stamp
  store.patch({ marker: 1 });
  assert.ok(rowTs(storage, '["marker"]')[0] < START + MINUTE);
});

test('offline edits made with a fast clock are all re-stamped and applied once the client connects', async () => {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time });
  const net = createNetwork(store);
  const link = net.link();
  const c = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'c', now: fakeTime(START + 2 * HOUR) });
  c.collection('tasks').add({ id: 'c1', title: 'first' });
  await net.settle();
  c.collection('tasks').add({ id: 'c2', title: 'second' });
  await net.settle();
  assert.equal(c.pending, 2);
  const errors = [];
  c.on('error', err => errors.push(err.code));

  c.connect();
  await net.settle();
  assert.deepEqual(Object.keys(store.snapshot().tasks).sort(), ['c1', 'c2']);
  assert.equal(c.pending, 0);
  assert.equal(c.status, 'online');
  assert.deepEqual(errors, []);
  assert.deepEqual(Object.keys(c.state.tasks).sort(), ['c1', 'c2'], 'the client kept its edits through the resync');
});

test('the refusal names the skew and carries the server time and the refused stamp; a clock within maxSkew passes', () => {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time, maxSkew: 2 * MINUTE });
  const sent = [];
  // Only the answers to the ops: not the presence list a new session gets, nor the broadcast patch
  const session = store.session({ send: m => { if (m.t !== 'presence' && m.t !== 'patch') sent.push(m); } });
  session.receive({ t: 'op', op: { replicaId: 'r', seq: 1, ts: [START + 3 * MINUTE, 0, 'r'], diff: { tasks: { a: { id: 'a' } } } } });
  session.receive({ t: 'op', op: { replicaId: 'r', seq: 2, ts: [START + MINUTE, 0, 'r'], diff: { tasks: { b: { id: 'b' } } } } });
  assert.deepEqual(sent[0], {
    t: 'error', seq: 1, code: 'clock-skew', message: "The op is stamped 180 s ahead of the server's clock", now: START, ts: [START + 3 * MINUTE, 0, 'r']
  });
  assert.equal(sent[1].t, 'ack');
  assert.deepEqual(Object.keys(store.snapshot().tasks), ['b']);
  assert.deepEqual(store.replicas, ['r'], 'a refused op leaves no trace of progress');
});

test('maxSkew: Infinity turns the guard off', async () => {
  const storage = memoryStorage();
  const store = createStore({ initial: INITIAL, storage, now: fakeTime(START), maxSkew: Infinity });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: fakeTime(START + HOUR) });
  await net.settle();
  a.collection('tasks').add({ id: 't1' });
  await net.settle();
  assert.equal(rowTs(storage, '["tasks","t1","id"]')[0], START + HOUR);
});
