import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore, memoryStorage } from '../src/server/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const snap = client => LazyWatch.snapshot(client.state);

// The server gets a fake clock too: a real one would pull every client's
// hybrid clock up to wall time and swamp the fake advances the tests use
// to order concurrent edits
function setup(options = {}) {
  const store = createStore({ initial: INITIAL, registers: REGISTERS, now: fakeTime(1_000_000), ...options });
  const net = createNetwork(store);
  return { store, net };
}

test('a connected client receives the snapshot and live changes from another client', async () => {
  const { store, net } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  assert.equal(a.status, 'online');
  assert.equal(b.status, 'online');

  const id = a.collection('tasks').add({ title: 'Write docs', done: false });
  a.state.order.push(id);
  await net.settle();

  assert.deepEqual(b.collection('tasks').get(id), { title: 'Write docs', done: false, id });
  assert.deepEqual(b.state.order, [id]);
  assert.deepEqual(store.snapshot(), snap(a));
  assert.deepEqual(snap(b), snap(a));
  assert.equal(a.pending, 0);
  assert.equal(b.pending, 0);
});

test('edits made offline queue in the outbox and merge on reconnect', async () => {
  const { store, net } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS });
  await net.settle();

  a.link.goOffline();
  await net.settle();
  assert.equal(a.status, 'offline');
  const id = a.collection('tasks').add({ title: 'Offline task' });
  a.collection('tasks').update(id, { done: true });
  await net.settle();
  assert.equal(a.pending, 1, 'add and update in one tick form one batch, so one op waits');
  assert.equal(b.collection('tasks').has(id), false);

  a.link.goOnline();
  a.connect();
  await net.settle();
  assert.equal(a.pending, 0);
  assert.equal(a.status, 'online');
  assert.deepEqual(b.collection('tasks').get(id), { title: 'Offline task', id, done: true });
  assert.deepEqual(store.snapshot(), snap(a));
  assert.deepEqual(snap(b), snap(a));
});

test('concurrent offline edits to one field resolve by timestamp, in either reconnect order', async () => {
  for (const firstBack of ['a', 'b']) {
    const { store, net } = setup();
    const timeA = fakeTime(1_000_000);
    const timeB = fakeTime(1_000_000);
    const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, now: timeA });
    const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS, now: timeB });
    await net.settle();
    const id = a.collection('tasks').add({ title: 'Draft' });
    await net.settle();

    a.link.goOffline();
    b.link.goOffline();
    await net.settle();
    timeA.advance(10);
    a.collection('tasks').update(id, { title: 'From A (earlier)' });
    timeB.advance(20);
    b.collection('tasks').update(id, { title: 'From B (later)' });
    await net.settle();

    const [first, second] = firstBack === 'a' ? [a, b] : [b, a];
    first.link.goOnline(); first.connect();
    await net.settle();
    second.link.goOnline(); second.connect();
    await net.settle();

    assert.equal(store.state.tasks[id].title, 'From B (later)', `reconnect order ${firstBack} first`);
    assert.equal(a.collection('tasks').get(id).title, 'From B (later)', 'the loser was corrected');
    assert.equal(b.collection('tasks').get(id).title, 'From B (later)');
    assert.deepEqual(snap(a), store.snapshot());
    assert.deepEqual(snap(b), store.snapshot());
  }
});

test('a newer deletion beats an older concurrent edit, and the editor drops the record', async () => {
  const { store, net } = setup();
  const timeA = fakeTime(1_000_000);
  const timeB = fakeTime(1_000_000);
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, now: timeA });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS, now: timeB });
  await net.settle();
  const id = a.collection('tasks').add({ title: 'Doomed', done: false });
  await net.settle();

  a.link.goOffline(); b.link.goOffline();
  await net.settle();
  timeB.advance(10);
  b.collection('tasks').update(id, { done: true });       // older
  timeA.advance(20);
  a.collection('tasks').remove(id);                        // newer
  await net.settle();

  b.link.goOnline(); b.connect(); await net.settle();
  assert.equal(store.state.tasks[id].done, true, 'the edit landed first');
  a.link.goOnline(); a.connect(); await net.settle();

  assert.equal(store.state.tasks[id], undefined, 'the newer deletion removed the record');
  assert.equal(b.collection('tasks').has(id), false, 'the editor was corrected to the deletion');
  assert.deepEqual(snap(a), store.snapshot());
  assert.deepEqual(snap(b), store.snapshot());
});

test('an array outside a register is reverted locally, reported, and never sent', async () => {
  const { store, net } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err));

  a.state.tags = ['urgent'];
  await net.settle();

  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /Arrays are not allowed at "tags"/);
  assert.equal(a.state.tags, undefined, 'the write was rolled back');
  assert.equal(a.pending, 0, 'nothing was queued');
  assert.equal(store.state.tags, undefined);
  assert.equal(a.canUndo, false, 'a rejected batch is not an undo step');
});

test('a register array travels whole and the newest value wins', async () => {
  const { store, net } = setup();
  const timeA = fakeTime(1_000_000);
  const timeB = fakeTime(1_000_000);
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS, now: timeA });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS, now: timeB });
  await net.settle();
  a.state.order.push('x', 'y');
  await net.settle();
  assert.deepEqual(b.state.order, ['x', 'y']);

  a.link.goOffline(); b.link.goOffline(); await net.settle();
  timeA.advance(30);
  a.state.order.reverse();                 // ['y', 'x'] at the later time
  timeB.advance(10);
  b.state.order.push('z');                 // ['x', 'y', 'z'] earlier
  await net.settle();
  a.link.goOnline(); a.connect(); await net.settle();
  b.link.goOnline(); b.connect(); await net.settle();

  assert.deepEqual(store.state.order, ['y', 'x']);
  assert.deepEqual(a.state.order, ['y', 'x']);
  assert.deepEqual(b.state.order, ['y', 'x'], 'the older whole-value write lost entirely');
});

test('undo reverts only this replica\'s edits and survives a teammate\'s insert', async () => {
  const { net } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  const id = a.collection('tasks').add({ title: 'Mine', done: false });
  await net.settle();
  a.collection('tasks').update(id, { done: true });
  await net.settle();
  b.collection('tasks').add({ title: 'Theirs' });        // a keyed insert: no shape change on a's history
  b.collection('tasks').update(id, { title: 'Renamed by B' });
  await net.settle();

  assert.equal(a.canUndo, true);
  assert.equal(a.undo(), true);
  await net.settle();
  assert.equal(a.collection('tasks').get(id).done, false, 'own edit reverted');
  assert.equal(a.collection('tasks').get(id).title, 'Renamed by B', 'remote field edit kept');
  assert.equal(a.collection('tasks').ids().length, 2, 'remote insert kept');
  assert.equal(b.collection('tasks').get(id).done, false, 'the undo reached the other replica');
});

test('an outbox persisted before a reload is sent on the next connect, and duplicates are ignored', async () => {
  const { store, net } = setup();
  const storage = memoryOutbox();
  const link = net.link();
  link.goOffline();
  const { createClient } = await import('../src/client/index.js');
  const first = createClient({ transport: link.factory, reconnect: false, initial: INITIAL, registers: REGISTERS, storage, replicaId: 'a' });
  const id = first.collection('tasks').add({ title: 'Survives reload' });
  await net.settle();
  assert.equal(storage.load().ops.length, 1);
  first.dispose();

  link.goOnline();
  const second = createClient({ transport: link.factory, reconnect: false, initial: INITIAL, registers: REGISTERS, storage });
  assert.equal(second.replicaId, 'a', 'the replica id is restored with the outbox');
  second.connect();
  await net.settle();
  assert.deepEqual(store.state.tasks[id], { title: 'Survives reload', id });
  assert.equal(second.pending, 0);

  // A resent hello with the same ops changes nothing
  const before = store.version;
  store.session({ send: () => {} }).receive({ t: 'hello', replicaId: 'a', ops: storage.load().ops.concat([{ replicaId: 'a', seq: 1, ts: [1, 0, 'a'], diff: { tasks: { [id]: { title: 'stale' } } } }]) });
  assert.equal(store.version, before);
  assert.equal(store.state.tasks[id].title, 'Survives reload');
});

test('the server persists state and clocks through its storage adapter', async () => {
  const storage = memoryStorage();
  const one = createStore({ initial: INITIAL, registers: REGISTERS, storage });
  one.patch({ tasks: { a: { id: 'a', title: 'Kept' } } });
  one.patch({ tasks: { a: null } });
  one.dispose();

  const two = createStore({ initial: INITIAL, registers: REGISTERS, storage });
  assert.deepEqual(two.snapshot(), { tasks: {}, order: [] });
  // The tombstone survived: an older write from a replica that never saw the deletion is refused
  const r = two.apply({ replicaId: 'late', seq: 1, ts: [1, 0, 'late'], diff: { tasks: { a: { title: 'ghost' } } } });
  assert.equal(r.accepted, null);
  assert.deepEqual(r.correction, { tasks: { a: null } });
});

test('a server-side patch reaches connected clients', async () => {
  const { store, net } = setup();
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  store.patch({ tasks: { s: { id: 's', title: 'From the server' } } });
  await net.settle();
  assert.deepEqual(a.collection('tasks').get('s'), { id: 's', title: 'From the server' });
});
