// cache.test.js - The outbox is written per op; the state cache is written debounced, and restored safely
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { memoryOutbox, localStorageOutbox } from '../src/client/storage.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const tick = () => new Promise(resolve => setImmediate(resolve));

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
  const a = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: adapter, replicaId: 'a', cacheDelay: 50 });
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
    assert.deepEqual([...backing.keys()].sort(), ['app:outbox', 'app:outbox:lease', 'app:outbox:state']);
    assert.deepEqual(adapter.load(), { replicaId: 'a', seq: 1, ops: [], state: { tasks: {} }, version: 3, epoch: 'e' });
    assert.equal(JSON.parse(backing.get('app:outbox')).state, undefined, 'the outbox document does not carry the state');
    adapter.clear();
    assert.equal(adapter.load(), null, 'clear() forgets the store');
    assert.deepEqual([...backing.keys()], ['app:outbox:lease'], 'both keys removed; the lease is the tab\'s');
    adapter.close();
    assert.deepEqual([...backing.keys()], [], 'close() gives the key up');

    const errors = [];
    const full = localStorageOutbox('app:full', { onError: err => errors.push(err.name) });
    globalThis.localStorage.setItem = () => { throw Object.assign(new Error('quota'), { name: 'QuotaExceededError' }); };
    full.save({ replicaId: 'a', seq: 1, ops: [] });
    full.saveState({ state: {} });
    assert.deepEqual(errors, ['QuotaExceededError', 'QuotaExceededError'], 'a failed write is reported, not swallowed');
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

test('memoryOutbox.clear() forgets the outbox and the cache', () => {
  const adapter = memoryOutbox();
  adapter.save({ replicaId: 'a', seq: 2, ops: [] });
  adapter.saveState({ state: { tasks: {} }, version: 1, epoch: 'e' });
  assert.ok(adapter.load());
  adapter.clear();
  assert.equal(adapter.load(), null);
});

test('localStorageOutbox is one tab\'s at a time: a second tab on the key starts afresh, keeps nothing, and is told', () => {
  const backing = new Map();
  const previous = globalThis.localStorage;
  globalThis.localStorage = {
    getItem: k => (backing.has(k) ? backing.get(k) : null),
    setItem: (k, v) => backing.set(k, String(v)),
    removeItem: k => backing.delete(k)
  };
  try {
    const first = localStorageOutbox('app');
    first.load();
    first.save({ replicaId: 'r1', seq: 4, ops: [] });
    const errors = [];
    const second = localStorageOutbox('app', { onError: err => errors.push(err.code) });
    assert.equal(second.load(), null, 'not the first tab\'s replica: a replica of its own');
    assert.deepEqual(errors, ['storage-in-use']);
    second.save({ replicaId: 'r2', seq: 1, ops: [] });
    second.saveState({ state: {} });
    assert.equal(JSON.parse(backing.get('app')).replicaId, 'r1', 'the first tab\'s outbox is untouched');
    assert.equal(first.load().replicaId, 'r1', 'and still the first tab\'s');

    first.close();   // the first tab is done (pagehide does the same)
    const third = localStorageOutbox('app');
    assert.equal(third.load().replicaId, 'r1', 'a tab opened after it carries on the replica');
    third.close();
  } finally {
    if (previous === undefined) delete globalThis.localStorage;
    else globalThis.localStorage = previous;
  }
});

test('takeOver() holds the key whatever lease a crashed tab left behind', () => {
  const backing = new Map([['app:lease', JSON.stringify({ tab: 'crashed', at: Date.now() })]]);
  const previous = globalThis.localStorage;
  globalThis.localStorage = {
    getItem: k => (backing.has(k) ? backing.get(k) : null),
    setItem: (k, v) => backing.set(k, String(v)),
    removeItem: k => backing.delete(k)
  };
  try {
    backing.set('app', JSON.stringify({ replicaId: 'r1', seq: 2, ops: [] }));
    const leader = localStorageOutbox('app', { onError: () => assert.fail('no error for the lock holder') });
    leader.takeOver();
    assert.equal(leader.load().replicaId, 'r1');
    leader.close();
  } finally {
    if (previous === undefined) delete globalThis.localStorage;
    else globalThis.localStorage = previous;
  }
});

test('the state is written once changes settle, at least every ten delays under steady traffic, and at once when the page is hidden', async () => {
  const handlers = new Map();
  const had = globalThis.addEventListener;
  globalThis.addEventListener = (type, fn) => handlers.set(type, fn);
  globalThis.removeEventListener = (type, fn) => { if (handlers.get(type) === fn) handlers.delete(type); };
  try {
    const { adapter, calls } = recording();
    const store = createStore({ initial: INITIAL });
    const net = createNetwork(store);
    const a = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: adapter, replicaId: 'a', cacheDelay: 20 });
    a.connect();
    await net.settle();
    await sleep(40);
    calls.length = 0;
    const states = () => calls.filter(([kind]) => kind === 'saveState').length;

    // Remote traffic every 5 ms for 300 ms never settles: the ceiling (200 ms) writes once
    const started = Date.now();
    for (let i = 0; Date.now() - started < 300; i++) {
      store.patch({ tasks: { [`r${i % 20}`]: { id: `r${i % 20}`, n: i } } });
      await net.settle();
      await sleep(5);
    }
    assert.equal(states(), 1, 'one write in 300 ms of traffic, not one per 50 ms');
    await sleep(40);
    assert.equal(states(), 2, 'and one once it settled');

    store.patch({ tasks: { late: { id: 'late' } } });
    await net.settle();
    assert.equal(states(), 2);
    handlers.get('pagehide')({ type: 'pagehide' });
    assert.equal(states(), 3, 'a page going away writes what is pending at once');
    assert.equal(calls.at(-1)[1].state.tasks.late.id, 'late');
    a.dispose();
    assert.equal(handlers.has('pagehide'), false, 'dispose lets go of the page');
  } finally {
    if (had === undefined) { delete globalThis.addEventListener; delete globalThis.removeEventListener; }
    else globalThis.addEventListener = had;
  }
});

test('localStorageOutbox keeps the outbox op by op: an op writes itself, not every op pending, and an earlier version\'s document is taken over', async () => {
  const backing = new Map();
  let written = 0;
  const previous = globalThis.localStorage;
  globalThis.localStorage = {
    getItem: k => (backing.has(k) ? backing.get(k) : null),
    setItem: (k, v) => { written += String(v).length; backing.set(k, String(v)); },
    removeItem: k => backing.delete(k)
  };
  try {
    const store = createStore({ initial: INITIAL });
    const net = createNetwork(store);
    const link = net.link();
    const storage = localStorageOutbox('app');
    const a = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, storage, replicaId: 'a', cache: false });
    a.connect();
    await net.settle();
    link.goOffline();
    await net.settle();
    const perOp = [];
    for (let i = 0; i < 300; i++) {
      const before = written;
      a.state.tasks[`t${i}`] = { id: `t${i}`, title: `offline ${i}` };   // each op its own record: none makes another moot
      await tick();
      perOp.push(written - before);
    }
    assert.ok(perOp.at(-1) < perOp[0] * 2, `the 300th op wrote ${perOp.at(-1)} bytes, the first ${perOp[0]}: no growth with the outbox`);
    assert.equal([...backing.keys()].filter(k => k.startsWith('app:op:')).length, 300);
    a.dispose();
    storage.close();

    // Reloaded offline: every op comes back, in order, and goes out when online
    const again = localStorageOutbox('app');
    const b = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, storage: again, cache: false });
    assert.equal(b.replicaId, 'a');
    assert.equal(b.pending, 300);
    link.goOnline();
    b.connect();
    await net.settle();
    assert.equal(b.pending, 0);
    assert.equal(Object.keys(store.snapshot().tasks).length, 300);
    assert.equal([...backing.keys()].filter(k => k.startsWith('app:op:')).length, 0, 'acknowledged ops leave storage');
    b.dispose();
    again.close();

    // A document as 0.13 and earlier wrote it
    backing.clear();
    const op = { replicaId: 'old', seq: 2, ts: [1, 0, 'old'], diff: { tasks: { x: { id: 'x' } } } };
    backing.set('legacy', JSON.stringify({ replicaId: 'old', seq: 2, ops: [op] }));
    const legacy = localStorageOutbox('legacy');
    assert.deepEqual(legacy.load().ops, [op]);
    assert.deepEqual(JSON.parse(backing.get('legacy')), { replicaId: 'old', seq: 2, first: 2 }, 'taken apart into the new form');
    assert.deepEqual(JSON.parse(backing.get('legacy:op:2')), op);
    legacy.clear();
    assert.deepEqual([...backing.keys()].filter(k => !k.endsWith(':lease')), [], 'clear() removes the ops too');
    legacy.close();
  } finally {
    if (previous === undefined) delete globalThis.localStorage;
    else globalThis.localStorage = previous;
  }
});
