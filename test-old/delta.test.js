// delta.test.js - A reconnect gets what it missed, not the whole state
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {}, team: {} };
const snap = client => LazyWatch.snapshot(client.state);

/** A store whose sessions record what they send, per replica, so a test can see how a hello was answered */
function setup(options = {}) {
  const store = createStore({ initial: INITIAL, ...options });
  const sent = [];
  const session = store.session;
  store.session = opts => session({ ...opts, send: message => { sent.push(message); opts.send(message); } });
  const net = createNetwork(store);
  const answers = () => sent.filter(m => m.t === 'snapshot' || m.t === 'delta');
  const last = () => answers().at(-1);
  return { store, net, sent, answers, last };
}

async function reconnect(net, client) {
  client.link.goOnline();
  client.connect();
  await net.settle();
}

test('a client that missed ops gets them as a delta; one that missed nothing gets an empty one', async () => {
  const { store, net, last, answers } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  await net.settle();
  assert.equal(last().t, 'snapshot', 'the first connect is a snapshot');
  assert.equal(a.version, store.version);

  a.link.goOffline();
  await net.settle();
  b.collection('tasks').add({ id: 't1', title: 'while a was away' });
  await net.settle();
  b.state.tasks.t1.done = true;
  await net.settle();
  assert.equal(a.status, 'offline');

  await reconnect(net, a);
  const answer = last();
  assert.equal(answer.t, 'delta');
  assert.equal(answer.patches.length, 2, 'one accepted diff per op b made');
  assert.equal(answer.v, store.version);
  assert.deepEqual(snap(a), store.snapshot());
  assert.equal(a.version, store.version);
  assert.equal(a.status, 'online');

  a.link.goOffline();
  await net.settle();
  await reconnect(net, a);
  assert.deepEqual(last().patches, [], 'nothing happened: an empty delta');
  assert.equal(answers().filter(m => m.t === 'snapshot').length, 2, 'only the two first connects were snapshots');
});

test('a hello with pending ops gets a delta that includes them, and corrections for the leaves they lost', async () => {
  const { store, net, last } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  await net.settle();
  b.collection('tasks').add({ id: 't1', title: 'original' });
  await net.settle();

  a.link.goOffline();
  await net.settle();
  a.state.tasks.t1.title = 'a, offline';           // will lose: b writes later
  a.collection('tasks').add({ id: 'ta', title: 'a made this' });
  await net.settle();
  b.state.tasks.t1.title = 'b, later';
  b.collection('tasks').add({ id: 'tb', title: 'b made this' });
  await net.settle();
  assert.equal(a.pending, 1);

  await reconnect(net, a);
  assert.equal(last().t, 'delta');
  assert.equal(a.pending, 0);
  assert.equal(store.snapshot().tasks.t1.title, 'b, later');
  assert.equal(a.state.tasks.t1.title, 'b, later', 'the correction put the server value back');
  assert.deepEqual(Object.keys(a.state.tasks).sort(), ['t1', 'ta', 'tb']);
  assert.deepEqual(snap(a), store.snapshot());
  assert.deepEqual(snap(b), store.snapshot());
});

test('a correction in a delta reflects the state after every op of the hello, not the moment one op lost', async () => {
  // Found by the fuzzer: an early register write in the hello loses, its
  // correction is read then, a later write in the same hello wins, and
  // the stale correction applied last put the client on the old value
  const START = 1_000_000;
  const aTime = fakeTime(START);
  const bTime = fakeTime(START);
  const { store, net, last } = setup({ initial: { tasks: {}, order: [] }, registers: ['order'], now: fakeTime(START) });
  const a = net.client({ replicaId: 'a', initial: { tasks: {}, order: [] }, registers: ['order'], now: aTime });
  const b = net.client({ replicaId: 'b', initial: { tasks: {}, order: [] }, registers: ['order'], now: bTime });
  await net.settle();

  a.link.goOffline();
  await net.settle();
  // a writes at START+10 (will lose), b at START+20 (wins over that), a again at START+40 (wins over b)
  aTime.set(START + 10);
  a.state.order = ['first'];
  await net.settle();
  bTime.set(START + 20);
  b.state.order = ['middle'];
  await net.settle();
  aTime.set(START + 40);
  a.state.order = ['last'];
  await net.settle();
  assert.equal(a.pending, 1, 'the second write to the register took over from the first');

  await reconnect(net, a);
  assert.equal(last().t, 'delta');
  assert.deepEqual(store.snapshot().order, ['last']);
  assert.deepEqual(snap(a).order, ['last'], 'the correction was taken after the winning write');
  assert.deepEqual(snap(b).order, ['last']);
  assert.equal(a.pending, 0);
});

test('a refused op in the hello, a short log, or a disabled log all fall back to a snapshot', async () => {
  const refusing = setup({ readOnly: ['team'] });
  const a = refusing.net.client({ replicaId: 'a', initial: INITIAL });
  await refusing.net.settle();
  a.link.goOffline();
  await refusing.net.settle();
  a.state.team.name = 'not allowed';
  await refusing.net.settle();
  await reconnect(refusing.net, a);
  assert.equal(refusing.last().t, 'snapshot', 'a delta would have left the refused edit in place');
  assert.deepEqual(snap(a), refusing.store.snapshot());

  const short = setup({ deltaLog: 2 });
  const c = short.net.client({ replicaId: 'c', initial: INITIAL });
  const d = short.net.client({ replicaId: 'd', initial: INITIAL });
  await short.net.settle();
  c.link.goOffline();
  await short.net.settle();
  for (const id of ['x', 'y', 'z']) {
    d.collection('tasks').add({ id });
    await short.net.settle();
  }
  await reconnect(short.net, c);
  assert.equal(short.last().t, 'snapshot', 'three ops happened, the log holds two');
  assert.deepEqual(Object.keys(c.state.tasks).sort(), ['x', 'y', 'z']);

  const off = setup({ deltaLog: 0 });
  const e = off.net.client({ replicaId: 'e', initial: INITIAL });
  await off.net.settle();
  e.link.goOffline();
  await off.net.settle();
  off.store.patch({ tasks: { t: { id: 't' } } });
  await reconnect(off.net, e);
  assert.deepEqual(off.answers().map(m => m.t), ['snapshot', 'snapshot']);
  e.link.goOffline();
  await off.net.settle();
  await reconnect(off.net, e);
  assert.deepEqual(off.last(), { ...off.last(), t: 'delta', patches: [] }, 'with nothing missed, even a store without a log answers with an empty delta');
});

test('a client restored from its cache asks for what happened since the version it cached', async () => {
  const storage = memoryOutbox();
  const { store, net, last } = setup();
  const link = net.link();
  const first = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, storage, replicaId: 'a' });
  first.connect();
  await net.settle();
  store.patch({ tasks: { t1: { id: 't1', title: 'seen by the first' } } });
  await net.settle();
  first.dispose();
  assert.equal(storage.load().version, store.version, 'the cache carries the version');
  assert.equal(storage.load().epoch, store.epoch, 'and the epoch');

  store.patch({ tasks: { t2: { id: 't2', title: 'after the first left' } } });
  const second = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, storage });
  assert.equal(second.restored, true);
  assert.equal(second.version, store.version - 1);
  second.connect();
  await net.settle();
  assert.equal(last().t, 'delta');
  assert.equal(last().patches.length, 1);
  assert.deepEqual(snap(second), store.snapshot());
});

test('a cached version from another life of the storage is answered with a snapshot', async () => {
  const outbox = memoryOutbox();
  const one = setup();
  const oneClient = createClient({ transport: one.net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: outbox, replicaId: 'a' });
  oneClient.connect();
  await one.net.settle();
  for (let i = 0; i < 3; i++) one.store.patch({ tasks: { [`o${i}`]: { id: `o${i}` } } });
  await one.net.settle();
  oneClient.dispose();
  const cachedVersion = outbox.load().version;

  // Storage wiped and re-seeded: same versions, different history
  const two = setup();
  for (let i = 0; i < 5; i++) two.store.patch({ tasks: { [`n${i}`]: { id: `n${i}` } } });
  assert.ok(two.store.version > cachedVersion, 'the new store\'s log covers the cached version, and would answer with a wrong delta');
  const twoClient = createClient({ transport: two.net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: outbox });
  assert.equal(twoClient.restored, true);
  twoClient.connect();
  await two.net.settle();
  assert.equal(two.last().t, 'snapshot');
  assert.deepEqual(snap(twoClient), two.store.snapshot());
  assert.equal(twoClient.version, two.store.version);
});

test('a restart on storage that keeps the delta log still answers reconnects with deltas; without the log, a snapshot', async () => {
  const storage = memoryStorage();
  const outbox = memoryOutbox();
  let { store, net, last } = setup({ storage });
  const a = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: outbox, replicaId: 'a' });
  a.connect();
  await net.settle();
  store.patch({ tasks: { t1: { id: 't1' } } });
  await net.settle();
  a.dispose();
  store.patch({ tasks: { t2: { id: 't2' } } });   // missed by the client, and persisted in the log
  store.dispose();

  ({ store, net, last } = setup({ storage }));
  assert.equal(store.stats().log, 2, 'the log came back with the rows');
  const b = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: outbox });
  b.connect();
  await net.settle();
  assert.equal(last().t, 'delta');
  assert.equal(last().patches.length, 1, 'just the op made after the client left');
  assert.deepEqual(snap(b), store.snapshot());
  b.dispose();
  store.patch({ tasks: { t3: { id: 't3' } } });   // missed by the client; the restart below forgets the log
  store.dispose();

  // Storage that forgets the log (the JSON file adapter, or a custom one)
  const forgetful = { load: () => { const doc = storage.load(); delete doc.log; return doc; }, commit: change => storage.commit(change), flush() {} };
  ({ store, net, last } = setup({ storage: forgetful }));
  assert.equal(store.stats().log, 0);
  const c = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: INITIAL, storage: outbox });
  c.connect();
  await net.settle();
  assert.equal(last().t, 'snapshot', 'the log does not reach back to the cached version');
  assert.deepEqual(Object.keys(c.state.tasks).sort(), ['t1', 't2', 't3']);
  c.dispose();
});

test('a persisted log is used only where it is contiguous and ends at the current version', () => {
  const rows = [['["tasks","a","id"]', { value: 'a', ts: [1, 0, 'x'] }]];
  const at = (version, log) => createStore({ initial: INITIAL, storage: { load: () => ({ rows, replicas: {}, version, epoch: 'e', log }), commit() {}, flush() {} } });
  const entry = v => ({ v, diff: { tasks: { a: { n: v } } } });
  assert.equal(at(5, [entry(3), entry(4), entry(5)]).stats().log, 3);
  assert.equal(at(5, [entry(2), entry(4), entry(5)]).stats().log, 2, 'the gap cuts the log at 4');
  assert.equal(at(5, [entry(3), entry(4)]).stats().log, 0, 'a log that stops short is useless');
  assert.equal(at(5, [entry(5), entry(3), entry(4)]).stats().log, 3, 'order does not matter');
  assert.equal(at(5, [entry(4), 'junk', entry(5)]).stats().log, 2);
  assert.equal(createStore({ initial: INITIAL, deltaLog: 2, storage: { load: () => ({ rows, replicas: {}, version: 5, epoch: 'e', log: [entry(3), entry(4), entry(5)] }), commit() {}, flush() {} } }).stats().log, 2, 'capped at deltaLog');
});
