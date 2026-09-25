// observe.test.js - Store events for logs and metrics, and stats
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createHub, snapshotResponse } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';
import { LazyWatch } from 'lazy-watch';
import { toJSON } from '../src/server/wire.js';

const INITIAL = { tasks: {} };

test('observe reports every op, refusal, and session; a throwing observer goes to onError and stops nothing', async () => {
  const faults = [];
  const store = createStore({ initial: INITIAL, readOnly: ['locked'], onError: err => faults.push(err.message) });
  const events = [];
  const stop = store.observe('op', e => events.push(['op', e]));
  store.observe('refused', e => events.push(['refused', e]));
  store.observe('session', e => events.push(['session', e]));
  store.observe('op', () => { throw new Error('observer bug'); });
  assert.throws(() => store.observe('nope', () => {}), /Unknown store event/);
  assert.throws(() => store.observe('op', 42), /needs a function/);

  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  a.collection('tasks').add({ id: 't1' });
  await net.settle();
  store.patch({ tasks: { s1: { id: 's1' } } });
  a.state.locked = 1;
  await net.settle();
  a.dispose();
  await net.settle();

  // How long each op took from reaching the store to its patch handed on
  for (const [kind, e] of events) {
    if (kind !== 'op') continue;
    assert.ok(typeof e.ms === 'number' && e.ms >= 0, `ms: ${e.ms}`);
    delete e.ms;
  }
  assert.deepEqual(events, [
    ['session', { event: 'open', user: { id: 'u1' }, replicaId: null, sessions: 1 }],
    ['op', { replicaId: 'a', seq: 1, user: { id: 'u1' }, accepted: true, rejected: 0, version: 1 }],
    ['op', { replicaId: 'server', seq: 1, user: undefined, accepted: true, rejected: 0, version: 2 }],
    ['refused', { replicaId: 'a', seq: 2, user: { id: 'u1' }, code: 'forbidden', message: '"locked" is read-only' }],
    ['session', { event: 'close', user: { id: 'u1' }, replicaId: 'a', sessions: 0 }]
  ]);
  assert.deepEqual(faults, ['observer bug', 'observer bug'], 'once per op, and the other observers still ran');

  stop();
  store.patch({ tasks: { s2: { id: 's2' } } });
  assert.equal(events.filter(([kind]) => kind === 'op').length, 2, 'unsubscribed');
});

test('stats counts what the store holds', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  await net.settle();
  a.collection('tasks').add({ id: 't1', title: 'x' });
  await net.settle();
  a.collection('tasks').remove('t1');
  await net.settle();
  const { sent, ...counts } = store.stats();
  assert.deepEqual(counts, { version: 2, epoch: store.epoch, schema: 0, sessions: 1, replicas: 1, rows: 1, tombstones: 1, log: 2 });
  assert.equal(sent.ack.messages, 2, 'two ops acknowledged');
});

test('stats().sent counts what the store sent, by type, a broadcast once per session it reaches', async () => {
  const store = createStore({ initial: INITIAL });
  const heard = { a: [], b: [] };
  const a = store.session({ send: m => heard.a.push(m) });
  const b = store.session({ send: m => heard.b.push(m) });
  a.receive({ t: 'hello', replicaId: 'a', ops: [] });
  b.receive({ t: 'hello', replicaId: 'b', ops: [] });
  a.receive({ t: 'op', op: { replicaId: 'a', seq: 1, ts: [Date.now(), 0, 'a'], diff: { tasks: { t1: { id: 't1', title: 'x' } } } } });
  store.patch({ tasks: { t2: { id: 't2' } } });
  a.receive({ t: 'nonsense' });

  const expected = {};
  for (const m of [...heard.a, ...heard.b]) {
    const entry = expected[m.t] ??= { messages: 0, bytes: 0 };
    entry.messages++;
    entry.bytes += toJSON(m).length;
  }
  assert.deepEqual(store.stats().sent, expected);
  assert.equal(expected.patch.messages, 4, 'two patches, each to both sessions');
  assert.ok(expected.ack && expected.snapshot && expected.error, JSON.stringify(Object.keys(expected)));

  // The snapshot route counts its responses, compressed, a 304 as nothing sent
  const fetched = await snapshotResponse(store, new Request('http://x/ws/snapshot/main', { headers: { 'accept-encoding': 'gzip' } })).arrayBuffer();
  const etag = snapshotResponse(store, new Request('http://x/ws/snapshot/main')).headers.get('etag');
  assert.equal(snapshotResponse(store, new Request('http://x/ws/snapshot/main', { headers: { 'if-none-match': etag } })).status, 304);
  const route = store.stats().sent['http-snapshot'];
  assert.equal(route.messages, 3);
  assert.ok(route.bytes > fetched.byteLength, 'the gzipped body and the plain one');
  store.dispose();
});

test('a hub hands a throwing store factory to onError instead of the console', () => {
  const faults = [];
  const sent = [];
  const hub = createHub(() => { throw new Error('factory boom'); }, { send: m => sent.push(m), onError: err => faults.push(err.message) });
  hub.receive({ t: 'hello', store: 'x', replicaId: 'r', ops: [] });
  assert.deepEqual(faults, ['factory boom']);
  assert.deepEqual(sent, [{ t: 'closed', store: 'x', code: 'unknown-store', message: 'Store "x" could not be opened: factory boom' }]);
});

test("db.stats(): what is pending and for how long, the last ack's round trip, and how old others' edits are on arrival", async () => {
  const now = fakeTime();
  const store = createStore({ initial: INITIAL, now });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now });
  await net.settle();
  assert.deepEqual(a.stats(), { pending: 0, oldestPendingMs: null, ackMs: null, remoteAgeMs: null });

  a.link.goOffline();
  await net.settle();
  a.collection('tasks').add({ id: 't1', title: 'x' });
  await net.settle();
  now.advance(500);
  assert.equal(a.stats().pending, 1);
  assert.equal(a.stats().oldestPendingMs, 500, 'the edit has waited half a second');

  a.link.goOnline();
  a.connect();
  await net.settle();
  const { pending, oldestPendingMs, ackMs } = a.stats();
  assert.equal(pending, 0);
  assert.equal(oldestPendingMs, null);
  assert.ok(typeof ackMs === 'number' && ackMs >= 0, `ackMs: ${ackMs}`);

  b.collection('tasks').add({ id: 't2' });
  LazyWatch.flush(b.state);   // stamped and sent now
  now.advance(200);           // on its way for 200 ms of the shared clock
  await net.settle();
  assert.equal(a.stats().remoteAgeMs, 200);
  // b last heard a's offline edit, half a second old on arrival; its own
  // patch coming back (200 ms old) is not someone else's and does not count
  assert.equal(b.stats().remoteAgeMs, 500);
  a.dispose();
  b.dispose();
  store.dispose();
});
