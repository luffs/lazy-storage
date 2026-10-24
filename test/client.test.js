// client.test.js - What a client says about itself: its base and clocks,
// what is pending, undo's grouping and history, a declaration the server
// does not share, a patch that went missing, a listener that throws
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createHub } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };

test('the base is the server\'s, the view adds what is pending, and isPending says where', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE, now });
  await net.settle();
  assert.equal(db.epoch, store.epoch);
  db.state.tasks.k = { title: 'x', meta: { color: 'red' } };
  await net.settle();
  assert.deepEqual(db.clockOf('tasks', 'k').live['["title"]'], store.clock('tasks', 'k').live['["title"]'], 'the server\'s clock');
  db.link.goOffline();
  await net.settle();
  db.state.tasks.k.meta.color = 'blue';
  await new Promise(resolve => setImmediate(resolve));   // the batch emits
  assert.equal(db.base.tasks.k.meta.color, 'red', 'the base waits for the server');
  assert.equal(db.state.tasks.k.meta.color, 'blue');
  assert.equal(db.isPending('tasks/k/meta/color'), true);
  assert.equal(db.isPending(['tasks', 'k']), true, 'above it');
  assert.equal(db.isPending('tasks/k/meta/color/deeper'), true, 'beneath it');
  assert.equal(db.isPending('tasks/k/title'), false);
  assert.equal(db.isPending('settings'), false);
  assert.equal(db.shape.isSingleton('settings'), true);
  db.link.goOnline();
  db.connect();
  await net.settle();
  assert.equal(db.isPending('tasks/k/meta/color'), false);
  assert.equal(db.base.tasks.k.meta.color, 'blue');
});

test('undo groups, redoes, and forgets its history on demand', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  const history = [];
  db.on('history', h => history.push(h));
  db.group(() => {
    db.state.settings.theme = 'dark';
    db.state.tasks.k = { title: 'x' };
  });
  await net.settle();
  assert.equal(db.canUndo, true);
  db.undo();
  await net.settle();
  assert.equal(store.state.settings.theme, 'light', 'the group undone as one');
  assert.equal(store.state.tasks.k, undefined);
  assert.equal(db.canRedo, true);
  db.redo();
  await net.settle();
  assert.equal(store.state.settings.theme, 'dark');
  assert.equal(store.state.tasks.k.title, 'x');
  db.clearHistory();
  assert.equal(db.canUndo, false);
  assert.equal(db.canRedo, false);
  assert.deepEqual(history.at(-1), { canUndo: false, canRedo: false });
});

test('a client that declares the store otherwise than the server hears shape-mismatch', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', initial: { tasks: {}, settings: { theme: 'light' }, extra: {} }, singletons: ['settings'] });
  const errors = [];
  db.on('error', err => errors.push(err.code));
  await net.settle();
  assert.deepEqual(errors, ['shape-mismatch']);
});

test('a patch that skips a version has the client catch up with a hello', async () => {
  const store = createStore(SHAPE);
  const sends = [];
  const net = createNetwork({
    session: ({ send, user }) => {
      sends.push(send);
      return createHub(() => store, { send, user });
    }
  });
  const db = net.client({ store: 'main', replicaId: 'a', ...SHAPE });
  await net.settle();
  assert.equal(db.status, 'online');
  // A patch from far ahead, as if two before it went missing
  sends[0]({ t: 'patch', store: 'main', changes: [{ c: 'tasks', id: 'z', put: { title: 'from v+3' } }], v: db.version + 3, ts: store.now() });
  await net.settle();
  assert.equal(db.state.tasks.z, undefined, 'not taken on its own');
  assert.equal(db.status, 'online', 'caught up again');
  assert.equal(db.version, store.version);
});

test('a listener that throws is reported, and the others still hear the event', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE });
  const heard = [];
  const original = console.error;
  const logged = [];
  console.error = (...args) => logged.push(args.join(' '));
  try {
    db.on('status', () => { throw new Error('a bad listener'); });
    db.on('status', s => heard.push(s));
    await net.settle();
  } finally {
    console.error = original;
  }
  assert.ok(heard.includes('online'));
  assert.ok(logged.some(line => line.includes('a bad listener')));
});

test('a storage adapter that loads asynchronously needs openClient', () => {
  const storage = { load: () => Promise.resolve(null), save() {}, saveState() {} };
  assert.throws(() => createClient({ store: 'main', transport: () => ({}), storage }), /openClient/);
});

test('a client without undo, and the collection API, refuse and answer plainly', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE, undo: false });
  await net.settle();
  assert.equal(db.canUndo, false);
  assert.equal(db.canRedo, false);
  assert.equal(db.undo(), false);
  assert.equal(db.redo(), false);
  assert.equal(db.group(() => 'ran'), 'ran', 'a group still runs');
  assert.throws(() => db.on('nope', () => {}), TypeError);
  assert.throws(() => db.collection('settings'), TypeError, 'a singleton is no collection');
  const tasks = db.collection('tasks');
  assert.throws(() => tasks.add('x'), TypeError);
  assert.equal(tasks.update('missing', { a: 1 }), false);
  assert.equal(tasks.update('missing', 'x'), false);
  assert.equal(tasks.remove('missing'), false);
  const id = tasks.add({ title: 'x' });
  assert.equal(tasks.update(id, { done: true }), true);
  assert.equal(tasks.remove(id), true);
});

test('createClient refuses a connection and a transport together, and a row adapter that lacks a method', () => {
  assert.throws(() => createClient({ store: 'main', connection: { attach() {} }, transport: () => ({}) }), /either a connection or a transport/);
  const rows = { load: () => null, commit() {}, replace() {}, saveOp() {}, removeOp() {} };
  assert.throws(() => createClient({ store: 'main', transport: () => ({}), storage: rows }), /dropOps/);
});

test('pending edits stay shown through patches, acks, superseding and a refusal, record by record', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, rules: { tasks: (user, before, after) => after?.title !== 'refused' } });
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE, now });
  const other = net.client({ replicaId: 'b', ...SHAPE, now });
  await net.settle();
  db.link.goOffline();
  await net.settle();
  for (let i = 0; i < 30; i++) {
    db.state.tasks['k' + i] = { title: 'mine ' + i };
    await new Promise(resolve => setImmediate(resolve));
  }
  db.state.tasks.k3.title = 'mine again';          // supersedes the older write of k3's title
  await new Promise(resolve => setImmediate(resolve));
  db.state.tasks.bad = { title: 'refused' };
  await new Promise(resolve => setImmediate(resolve));
  now.advance(1);
  other.state.tasks.k5 = { note: 'theirs' };        // a patch for a record with an edit pending here
  await net.settle();
  assert.equal(db.state.tasks.k3.title, 'mine again');
  db.link.goOnline();
  db.connect();
  await net.settle();
  assert.equal(db.pending, 0);
  assert.equal(db.state.tasks.bad, undefined, 'the refused one taken back');
  assert.equal(db.state.tasks.k3.title, 'mine again');
  assert.deepEqual(db.state.tasks.k5, { title: 'mine 5', note: 'theirs' });
  for (let i = 0; i < 30; i++) assert.ok(db.state.tasks['k' + i], 'k' + i);
  const { LazyWatch } = await import('lazy-watch');
  assert.deepEqual(LazyWatch.snapshot(db.state), store.snapshot());
});
