// observe.test.js - Store events for logs and metrics, and stats
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createHub } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

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
  assert.deepEqual(store.stats(), { version: 2, epoch: store.epoch, sessions: 1, replicas: 1, rows: 1, tombstones: 1, log: 2 });
});

test('a hub hands a throwing store factory to onError instead of the console', () => {
  const faults = [];
  const sent = [];
  const hub = createHub(() => { throw new Error('factory boom'); }, { send: m => sent.push(m), onError: err => faults.push(err.message) });
  hub.receive({ t: 'hello', store: 'x', replicaId: 'r', ops: [] });
  assert.deepEqual(faults, ['factory boom']);
  assert.deepEqual(sent, [{ t: 'closed', store: 'x', code: 'unknown-store', message: 'Store "x" could not be opened: factory boom' }]);
});
