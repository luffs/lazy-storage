// rows.test.js - Row persistence on the client: a batch costs the leaves it touched
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createClient, openClient } from '../src/client/index.js';
import { rebuild } from '../src/core/model.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const snap = client => LazyWatch.snapshot(client.state);

/** An in-memory row adapter with the interface IndexedDB and SQLite implement */
function memoryRows({ async = false } = {}) {
  const leaves = new Map();
  const ops = new Map();
  let meta = null;
  const prefixOf = key => key.slice(0, -1) + ',';
  return {
    leaves,
    ops,
    get meta() { return meta; },
    load() {
      const doc = meta
        ? { ...meta, ops: [...ops.keys()].sort((a, b) => a - b).map(k => structuredClone(ops.get(k))), rows: [...leaves].map(([k, v]) => [k, structuredClone(v)]) }
        : null;
      return async ? Promise.resolve(doc) : doc;
    },
    commit({ puts, deletes, meta: m }) {
      for (const key of deletes) {
        leaves.delete(key);
        const prefix = prefixOf(key);
        for (const k of [...leaves.keys()]) if (k.startsWith(prefix)) leaves.delete(k);
      }
      for (const [k, v] of puts) leaves.set(k, structuredClone(v));
      meta = m;
    },
    replace({ rows, meta: m }) {
      leaves.clear();
      for (const [k, v] of rows) leaves.set(k, structuredClone(v));
      meta = m;
    },
    saveOp(op, m) {
      ops.set(op.seq, structuredClone(op));
      meta = m;
    },
    removeOp(seq, m) {
      ops.delete(seq);
      meta = m;
    },
    dropOps(seq, m) {
      for (const k of [...ops.keys()]) if (k <= seq) ops.delete(k);
      meta = m;
    }
  };
}

/** A store whose sessions record what they send, to see how a hello was answered */
function setup(options = {}) {
  const store = createStore({ initial: INITIAL, registers: REGISTERS, ...options });
  const sent = [];
  const session = store.session;
  store.session = opts => session({ ...opts, send: message => { sent.push(message); opts.send(message); } });
  const net = createNetwork(store);
  return { store, net, answers: () => sent.filter(m => m.t === 'snapshot' || m.t === 'delta').map(m => m.t) };
}

const rowsOf = adapter => [...adapter.leaves];
const rebuilt = adapter => rebuild(INITIAL, rowsOf(adapter));

test('local ops, remote patches, deletions, and registers keep the rows equal to the state; op rows follow the outbox', async () => {
  const { store, net } = setup();
  const storage = memoryRows();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, storage });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  assert.deepEqual(rebuilt(storage), snap(a), 'the snapshot filled the rows');
  assert.deepEqual(storage.meta, { replicaId: 'a', seq: 0, version: store.version, epoch: store.epoch });

  a.collection('tasks').add({ id: 't1', title: 'one', sub: { s1: { id: 's1', done: false } } });
  a.state.order.push('t1');
  await net.settle();
  assert.deepEqual(rebuilt(storage), snap(a));
  assert.deepEqual(storage.leaves.get('["order"]'), ['t1'], 'a register is one row');
  assert.ok(storage.leaves.has('["tasks","t1","sub","s1","done"]'), 'a nested leaf is one row');
  assert.equal(storage.ops.size, 0, 'acked ops are gone');
  assert.equal(storage.meta.seq, 1);

  b.collection('tasks').add({ id: 't2', title: 'from b' });
  b.state.tasks.t1.sub.s1.done = true;
  await net.settle();
  assert.deepEqual(rebuilt(storage), snap(a), 'remote patches land as rows too');
  assert.equal(storage.leaves.get('["tasks","t1","sub","s1","done"]'), true);

  a.link.goOffline();
  await net.settle();
  delete a.state.tasks.t1;
  a.state.order = ['t2'];
  await net.settle();
  a.state.tasks.t2.title = 'renamed offline';
  await net.settle();
  assert.equal(a.pending, 2);
  assert.deepEqual([...storage.ops.keys()], [2, 3], 'one row per pending op');
  assert.equal([...storage.leaves.keys()].some(k => k.startsWith('["tasks","t1"')), false, 'deleting a record removed every descendant row');
  assert.deepEqual(rebuilt(storage), snap(a));

  a.link.goOnline();
  a.connect();
  await net.settle();
  assert.equal(a.pending, 0);
  assert.equal(storage.ops.size, 0);
  assert.deepEqual(rebuilt(storage), snap(a));
  assert.deepEqual(rebuilt(storage), store.snapshot());
  assert.equal(storage.meta.version, store.version);
  assert.equal(a.version, store.version);
});

test('a snapshot replaces the rows wholesale, so rows a failed write left behind are healed', async () => {
  const { store, net, answers } = setup();
  store.patch({ tasks: { real: { id: 'real' } } });
  const storage = memoryRows();
  storage.replace({ rows: [['["junk"]', 1], ['["tasks","stale","id"]', 'stale']], meta: { replicaId: 'a', seq: 0, version: 0, epoch: null } });
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, storage });
  assert.equal(a.restored, true);
  assert.equal(a.state.junk, 1, 'started from the rows it had');
  await net.settle();
  assert.deepEqual(answers(), ['snapshot']);
  assert.deepEqual(rebuilt(storage), store.snapshot());
  assert.equal(storage.leaves.has('["junk"]'), false);
  assert.equal(a.state.junk, undefined);
});

test('a client restored from rows has its state, replays its pending ops, and resumes with a delta', async () => {
  const { store, net, answers } = setup();
  const storage = memoryRows();
  const link = net.link();
  const first = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'a' });
  first.connect();
  await net.settle();
  first.collection('tasks').add({ id: 't1', title: 'synced' });
  await net.settle();
  link.goOffline();
  await net.settle();
  first.collection('tasks').add({ id: 't2', title: 'pending' });
  await net.settle();
  assert.equal(first.pending, 1);
  first.dispose();
  store.patch({ tasks: { t3: { id: 't3', title: 'meanwhile' } } });

  const second = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage });
  assert.equal(second.restored, true);
  assert.equal(second.replicaId, 'a');
  assert.equal(second.pending, 1);
  assert.deepEqual(Object.keys(second.state.tasks).sort(), ['t1', 't2']);
  assert.equal(second.canUndo, false, 'the replay is not history');
  link.goOnline();
  second.connect();
  await net.settle();
  assert.deepEqual(answers(), ['snapshot', 'delta']);
  assert.equal(second.pending, 0);
  assert.deepEqual(Object.keys(second.state.tasks).sort(), ['t1', 't2', 't3']);
  assert.deepEqual(rebuilt(storage), store.snapshot());
});

test('cache: false on a row adapter keeps the ops and nothing else', async () => {
  const { net } = setup();
  const storage = memoryRows();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, storage, cache: false });
  await net.settle();
  a.link.goOffline();
  await net.settle();
  a.collection('tasks').add({ id: 't1' });
  await net.settle();
  assert.equal(storage.leaves.size, 0);
  assert.deepEqual([...storage.ops.keys()], [1]);
  assert.equal(storage.meta.replicaId, 'a');
});

test('createClient refuses an adapter that loads asynchronously; openClient awaits it', async () => {
  const { store, net } = setup();
  const storage = memoryRows({ async: true });
  const link = net.link();
  assert.throws(() => createClient({ transport: link.factory, store: 'main', initial: INITIAL, storage }), /openClient/);

  const a = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'a' });
  assert.equal(a.restored, false, 'nothing was stored yet');
  a.connect();
  await net.settle();
  a.collection('tasks').add({ id: 't1' });
  await net.settle();
  a.dispose();

  const b = await openClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage });
  assert.equal(b.restored, true);
  assert.deepEqual(snap(b), store.snapshot());
  assert.equal(b.version, store.version);
});
