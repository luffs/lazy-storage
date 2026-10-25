// client.test.js - What a client says about itself: its base and clocks,
// what is pending, undo's grouping and history, a declaration the server
// does not share, a patch that went missing, a listener that throws, and
// a client that takes its snapshots without the records' clocks
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createHub } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createRelay } from '../src/relay/index.js';
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

test('a client behind the server\'s shape passes over what it does not declare, and keeps the rest of the patch', async () => {
  // The server was deployed with a new collection; a client still on the old shape must not fall apart on a patch that touches it
  const store = createStore({ initial: { ...SHAPE.initial, notes: {} }, singletons: ['settings'] });
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...SHAPE });
  const errors = [];
  db.on('error', err => errors.push(err.code ?? err.message));
  await net.settle();
  store.patch({ notes: { n: { text: 'new' } }, tasks: { k: { title: 'kept' } } });
  await net.settle();
  assert.deepEqual(db.state.tasks.k, { title: 'kept' });
  assert.equal(db.state.notes, undefined);
  assert.deepEqual(errors, ['shape-mismatch']);
  db.state.tasks.k.title = 'edited';
  await net.settle();
  assert.equal(store.state.tasks.k.title, 'edited', 'and it goes on syncing');
});

test('a new op supersedes older pending writes of its own record only: not a singleton\'s for a record of that name, nor another id\'s', async () => {
  const shape = { initial: { tasks: {}, settings: {} }, singletons: ['settings'] };
  const store = createStore(shape);
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'a', ...shape });
  await net.settle();
  db.link.goOffline();
  await net.settle();
  db.state.settings.title = 'singleton';
  await new Promise(resolve => setImmediate(resolve));
  db.state.tasks.settings = { title: 'a record named settings' };
  await new Promise(resolve => setImmediate(resolve));
  db.state.tasks.other = { title: 'other' };
  await new Promise(resolve => setImmediate(resolve));
  db.state.tasks.settings.title = 'again';          // supersedes the record's title, nothing else
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.pending, 4);
  assert.ok(db.isPending(['settings', 'title']), 'the singleton\'s write still pending');
  assert.ok(db.isPending(['tasks', 'other', 'title']));
  db.link.goOnline();
  db.connect();
  await net.settle();
  assert.equal(store.state.settings.title, 'singleton');
  assert.equal(store.state.tasks.settings.title, 'again');
  assert.equal(store.state.tasks.other.title, 'other');
});

// --- Without the records' clocks ------------------------------------------------------

/** A link whose transport records what the client is sent and what it sends, and holds back what `hold` picks until `release()` */
function recorded(net, hold = () => false) {
  const link = net.link();
  const received = [];
  const sent = [];
  const held = [];
  const factory = () => {
    const t = link.factory();
    const send = t.send;
    t.send = m => { sent.push(m); send(m); };
    queueMicrotask(() => {
      const onmessage = t.onmessage;
      t.onmessage = m => {
        received.push(m);
        if (hold(m)) held.push(() => onmessage?.(m));
        else onmessage?.(m);
      };
    });
    return t;
  };
  return { link, factory, received, sent, release: () => { for (const deliver of held.splice(0)) deliver(); } };
}

test('a client without clocks shows an edit the server will refuse as landed until the ack; one with them, as lost before it', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now });
  await net.settle();
  a.state.tasks.k = { title: 'old' };
  await net.settle();
  now.advance(20);
  a.state.tasks.k.title = 'theirs';
  await net.settle();

  // Two clients connecting now, whose snapshot takes a while (held back here, as a large one fetched over HTTP does) while the
  // user edits: the edit is stamped behind the server's write of the title (after the record's birth), and sent once the snapshot lands
  const seen = {};
  const lost = {};
  const clients = {};
  const wires = {};
  for (const [name, clocks] of [['judged', true], ['optimistic', false]]) {
    const wire = recorded(net, m => m.t === 'snapshot');
    const db = createClient({ transport: wire.factory, reconnect: false, store: 'main', replicaId: name, ...SHAPE, now: () => now() - 10, clocks });
    db.connect();
    seen[name] = [];
    lost[name] = [];
    db.watch(() => seen[name].push([db.state.tasks.k?.title, db.pending]));
    db.on('lost', e => lost[name].push(e));
    clients[name] = db;
    wires[name] = wire;
  }
  await net.settle();
  for (const db of Object.values(clients)) {
    assert.equal(db.status, 'connecting', 'the snapshot is on its way');
    db.state.tasks.k = { title: 'mine' };
  }
  await new Promise(resolve => setImmediate(resolve));
  for (const db of Object.values(clients)) assert.equal(db.state.tasks.k.title, 'mine', 'the edit shows at once, nothing heard yet');
  for (const wire of Object.values(wires)) wire.release();
  await net.settle();

  for (const [name, db] of Object.entries(clients)) {
    assert.equal(db.pending, 0, name);
    assert.equal(db.state.tasks.k.title, 'theirs', `${name} ends with the server's value`);
    assert.deepEqual(lost[name].map(e => e.lost.map(l => l.why)), [['newer']], `${name} hears why`);
    assert.equal(wires[name].sent.find(m => m.t === 'hello').clocks, name === 'judged' ? true : undefined, 'the hello asks, or not');
    const snapshot = wires[name].received.find(m => m.t === 'snapshot');
    assert.equal('clocks' in snapshot, name === 'judged', 'the snapshot carries them only when asked');
  }
  assert.equal(seen.judged.some(([title, pending]) => title === 'theirs' && pending === 1), true, 'judged by the clocks: the edit falls away on the snapshot, while still pending');
  assert.equal(seen.optimistic.every(([title, pending]) => pending === 0 || title === 'mine'), true, 'without them: shown as landed until the ack');
  assert.deepEqual(clients.optimistic.clockOf('tasks', 'k'), { live: {}, gone: {} }, 'no clock known');
  assert.equal(Object.keys(clients.judged.clockOf('tasks', 'k').live).length > 0, true);
  for (const db of Object.values(clients)) db.dispose();
});

test('a client without clocks still judges by what patches told it, undoes, and reloads a persisted base without them', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  store.patch({ tasks: { k: { title: 'one' } } });
  const storage = memoryOutbox();
  const open = () => {
    const link = net.link();
    const db = createClient({ transport: link.factory, reconnect: false, store: 'main', replicaId: 'a', storage, cacheDelay: 0, ...SHAPE, now, clocks: false });
    db.link = link;
    db.connect();
    return db;
  };
  let db = open();
  await net.settle();
  assert.deepEqual(db.clockOf('tasks', 'k'), { live: {}, gone: {} }, 'from the snapshot: no clock');
  now.advance(5);
  store.patch({ tasks: { j: { title: 'two' } } });
  await net.settle();
  assert.deepEqual(db.clockOf('tasks', 'j'), { at: 1_000_005, live: {}, gone: {} }, 'from a patch: what it stamped');

  // Offline, an edit of what another client writes later; the undo manager works as ever
  db.link.goOffline();
  await net.settle();
  db.state.tasks.k.title = 'mine';
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.state.tasks.k.title, 'mine');
  db.undo();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.state.tasks.k.title, 'one', 'undone locally');
  db.redo();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(db.state.tasks.k.title, 'mine');
  await new Promise(resolve => setTimeout(resolve, 5));   // the base written (cacheDelay 0)
  const saved = storage.load();
  assert.deepEqual(saved.state.tasks.k, { title: 'one' }, 'the base, not the edit');
  assert.deepEqual(saved.clocks, [['tasks', 'j', { at: 1_000_005, live: {}, gone: {} }]], 'only a clock that says something is written');
  assert.equal(saved.ops.length, 1);
  db.dispose();
  const b = net.client({ replicaId: 'b', ...SHAPE, now });
  await net.settle();
  now.advance(5);
  b.state.tasks.k.title = 'three';
  await net.settle();

  db = open();
  assert.equal(db.restored, true);
  assert.equal(db.pending, 1);
  assert.equal(db.state.tasks.k.title, 'mine', 'the pending edit over the reloaded base');
  assert.deepEqual(db.clockOf('tasks', 'k'), { live: {}, gone: {} });
  assert.deepEqual(db.clockOf('tasks', 'j'), { at: 1_000_005, live: {}, gone: {} });
  await net.settle();
  assert.equal(db.pending, 0);
  assert.equal(db.state.tasks.k.title, 'three', 'lost to the newer write: the delta stamped it, and the ack agreed');
  assert.deepEqual(db.base, store.snapshot());
  db.dispose();
});

test('a relay asks the server for the clocks whatever its clients ask: it decides for them', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const central = createNetwork(store, { vouch: credential => credential });
  const up = recorded(central);
  const relay = createRelay({ shapeOf: () => SHAPE, link: up.factory, grace: 0, reconnect: false, keepalive: false, now });
  const site = createNetwork(relay);
  const settle = async () => {
    for (let i = 0; i < 20; i++) {
      await central.settle();
      await site.settle();
      if (central.pending === 0 && site.pending === 0) return;
    }
  };
  const d = site.client({ replicaId: 'd', ...SHAPE, now, clocks: false }, { user: { id: 'd' } });
  await settle();
  assert.equal(d.status, 'online');
  assert.equal(up.sent.find(m => m.t === 'hello').clocks, true, 'the relay\'s own hello');
  assert.equal('clocks' in up.received.find(m => m.t === 'snapshot'), true, 'and its snapshot carries them');
  d.dispose();
  relay.close();
});
