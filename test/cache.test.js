// cache.test.js - The outbox is written per op; the state cache is written debounced, and restored safely
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { memoryOutbox, localStorageOutbox } from '../src/client/storage.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** An adapter that records every call */
function recording() {
  const calls = [];
  const adapter = {
    load: () => null,
    save: doc => calls.push(['save', structuredClone(doc)]),
    saveState: cache => calls.push(['saveState', structuredClone(cache)])
  };
  return { adapter, calls };
}

test('a local op writes the outbox at once and the state a moment later, once, without the state in the outbox', async () => {
  const { adapter, calls } = recording();
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: adapter, replicaId: 'a' });
  a.connect();
  await net.settle();
  calls.length = 0;

  a.collection('tasks').add({ id: 't1', title: 'one' });
  a.state.tasks.t1.title = 'two';
  await net.settle();
  const saves = calls.filter(([kind]) => kind === 'save');
  assert.equal(saves.length >= 2, true, 'the outbox is written for the op and again for the ack');
  assert.equal('state' in saves[0][1], false, 'no state in the outbox document');
  assert.deepEqual(Object.keys(saves[0][1]).sort(), ['ops', 'replicaId', 'seq']);
  assert.equal(calls.filter(([kind]) => kind === 'saveState').length, 0, 'the state write is still pending');

  await sleep(80);
  const states = calls.filter(([kind]) => kind === 'saveState');
  assert.equal(states.length, 1, 'one debounced write for the whole burst');
  assert.deepEqual(states[0][1].state.tasks.t1, { id: 't1', title: 'two' });
  assert.equal(states[0][1].version, store.version);
  assert.equal(states[0][1].epoch, store.epoch);
  a.dispose();
});

test('a restore replays the outbox over the cached state, so a state written before the last ops still comes up current', () => {
  const op = (seq, diff) => ({ replicaId: 'a', seq, ts: [1000 + seq, 0, 'a'], diff });
  const storage = {
    load: () => ({
      replicaId: 'a',
      seq: 3,
      ops: [op(2, { tasks: { t2: { id: 't2', title: 'second' } } }), op(3, { tasks: { t1: { done: true } } })],
      state: { tasks: { t1: { id: 't1', title: 'first' } } },   // written when only op 1 had happened
      version: 7,
      epoch: 'e'
    }),
    save: () => {},
    saveState: () => {}
  };
  const client = createClient({ transport: () => ({ send() {}, close() {} }), reconnect: false, store: 'main', initial: INITIAL, storage });
  assert.equal(client.restored, true);
  assert.equal(client.pending, 2);
  assert.equal(client.version, 7);
  assert.deepEqual({ ...client.state.tasks.t1 }, { id: 't1', title: 'first', done: true });
  assert.deepEqual({ ...client.state.tasks.t2 }, { id: 't2', title: 'second' });
  assert.equal(client.canUndo, false, 'the replay is not history');
  client.dispose();
});

test('the localStorage adapter keeps the outbox and the state under separate keys', () => {
  const backing = new Map();
  const previous = globalThis.localStorage;
  globalThis.localStorage = {
    getItem: k => (backing.has(k) ? backing.get(k) : null),
    setItem: (k, v) => backing.set(k, String(v)),
    removeItem: k => backing.delete(k)
  };
  try {
    const adapter = localStorageOutbox('app:outbox');
    assert.equal(adapter.load(), null);
    adapter.save({ replicaId: 'a', seq: 1, ops: [] });
    assert.deepEqual(adapter.load(), { replicaId: 'a', seq: 1, ops: [] });
    adapter.saveState({ state: { tasks: {} }, version: 3, epoch: 'e' });
    assert.deepEqual([...backing.keys()].sort(), ['app:outbox', 'app:outbox:state']);
    assert.deepEqual(adapter.load(), { replicaId: 'a', seq: 1, ops: [], state: { tasks: {} }, version: 3, epoch: 'e' });
    assert.equal(JSON.parse(backing.get('app:outbox')).state, undefined, 'the outbox document does not carry the state');
  } finally {
    if (previous === undefined) delete globalThis.localStorage;
    else globalThis.localStorage = previous;
  }
});

test('memoryOutbox merges the two parts on load', () => {
  const adapter = memoryOutbox();
  assert.equal(adapter.load(), null);
  adapter.save({ replicaId: 'a', seq: 0, ops: [] });
  adapter.saveState({ state: { n: 1 }, version: 2, epoch: 'e' });
  assert.deepEqual(adapter.load(), { replicaId: 'a', seq: 0, ops: [], state: { n: 1 }, version: 2, epoch: 'e' });
});
