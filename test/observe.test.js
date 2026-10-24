// observe.test.js - What a store tells its watchers, what it counts, and what a client says of itself
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };

test('observe reports every op once stored, every refusal, and every session; a throwing observer stops nothing', async () => {
  const errors = [];
  const store = createStore({ ...SHAPE, rules: { settings: false }, onError: err => errors.push(err) });
  const ops = [];
  const refused = [];
  const sessions = [];
  store.observe('op', e => ops.push(e));
  store.observe('op', () => { throw new Error('a broken observer'); });
  store.observe('refused', e => refused.push(e.code));
  store.observe('session', e => sessions.push(e.event));
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE }, { user: { id: 'u1' } });
  await net.settle();
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  a.state.settings.theme = 'dark';
  await net.settle();
  store.patch({ tasks: { s: { from: 'server' } } });
  store.flush();
  assert.deepEqual(ops.map(e => [e.replicaId, e.changes.length]), [['a', 1], ['server', 1]]);
  assert.deepEqual(ops[0].user, { id: 'u1' });
  assert.deepEqual(refused, ['forbidden']);
  assert.deepEqual(sessions, ['open']);
  assert.equal(errors.filter(e => e.message === 'a broken observer').length, 2);
  a.dispose();
  assert.deepEqual(sessions, ['open', 'close']);
});

test('on() hears every change as it lands, with its origin', async () => {
  const store = createStore(SHAPE);
  const heard = [];
  store.on((changes, meta) => heard.push([meta.origin, changes.map(ch => ch.c)]));
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  a.state.tasks.k = { n: 1 };
  await net.settle();
  store.patch({ settings: { theme: 'dark' } });
  assert.deepEqual(heard, [['remote', ['tasks']], ['server', ['settings']]]);
});

test('stats counts what the store holds and what it sent, a broadcast once per session it reaches', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  const b = net.client({ replicaId: 'b', ...SHAPE });
  await net.settle();
  a.state.tasks.k = { n: 1 };
  await net.settle();
  a.state.tasks.j = { n: 2 };
  await net.settle();
  const stats = store.stats();
  assert.equal(stats.records, 2);
  assert.equal(stats.sessions, 2);
  assert.equal(stats.replicas, 1, 'b, without a user, has written nothing to be kept for');
  assert.equal(stats.log, 2);
  assert.equal(stats.version, 2);
  assert.equal(stats.sent.patch.messages, 4, 'two patches, each to both sessions');
  assert.equal(stats.sent.snapshot.messages, 2);
  assert.equal(stats.sent.ack.messages, 2);
  assert.ok(b);
});

test('db.stats(): what is pending, for how long, and the last ack\'s round trip', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now });
  await net.settle();
  assert.deepEqual(a.stats(), { pending: 0, oldestPendingMs: null, ackMs: null });
  a.link.goOffline();
  await net.settle();
  a.state.tasks.k = { n: 1 };
  await net.settle();
  now.advance(500);
  assert.equal(a.stats().pending, 1);
  assert.equal(a.stats().oldestPendingMs, 500);
  a.link.goOnline(); a.connect();
  await net.settle();
  a.state.tasks.k.n = 2;
  await net.settle();
  const after = a.stats();
  assert.equal(after.pending, 0);
  assert.equal(after.oldestPendingMs, null);
  assert.equal(typeof after.ackMs, 'number');
});
