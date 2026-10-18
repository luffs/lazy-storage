// recreate.test.js - A container deleted and made again: its author had nothing there, so what
// it writes is new, not a stale edit of what was deleted, and the server re-adds it (core/merge.js,
// `made`); an edit from a copy that still held the old one stays refused
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { state: {} };
const START = 1_000_000;

function setup() {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time });
  const net = createNetwork(store);
  const aTime = fakeTime(START + 1000);   // a's clock runs ahead: its writes are the newer
  const bTime = fakeTime(START);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: aTime });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now: bTime });
  const heard = { a: [], b: [] };
  a.on('conflict', c => heard.a.push(c));
  b.on('conflict', c => heard.b.push(c));
  return { store, net, a, b, heard };
}

test('a container deleted and made again lands, on the server and the other client, with no conflict', async () => {
  const { store, net, a, b, heard } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  delete a.state.state.lists;
  await net.settle();
  assert.equal(store.snapshot().state.lists, undefined);
  assert.equal(b.state.state.lists, undefined);

  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t2: { text: 'y' } } });
  assert.deepEqual(JSON.parse(JSON.stringify(b.state.state.lists)), { turns: { t2: { text: 'y' } } });
  assert.deepEqual(heard.a, []);
  assert.deepEqual(store.stats().rows, 2, 'the turn and the barrier where the tombstone was');
  assert.deepEqual(store.stats().barriers, 1);
});

test('a container made and filled in while both ops wait lands: the re-add goes with whatever op carries the writes', async () => {
  const { store, net, a } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  delete a.state.state.lists;
  await net.settle();
  a.link.goOffline();
  await net.settle();
  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  a.state.state.lists.turns.t2.text = 'yy';          // takes over the first op's only leaf: the first op goes, its `made` must not
  await net.settle();
  a.link.goOnline();
  a.connect();
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t2: { text: 'yy' } } });
});

test('a stale deletion of what is gone already is no conflict', async () => {
  const { store, net, a, b, heard } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  b.link.goOffline();
  await net.settle();
  delete b.state.state.lists.turns.t1;
  await net.settle();
  delete a.state.state.lists;
  await net.settle();
  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t2: { text: 'y' } } });
  assert.deepEqual(heard.b, [], 'what b deleted is gone: its intent holds');
});

test('an edit from a copy that still held the deleted container is refused, before and after it is made again', async () => {
  const { store, net, a, b, heard } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  b.link.goOffline();
  await net.settle();
  // b, offline, edits the old record and adds a field to it
  b.state.state.lists.turns.t1.text = 'edited while it went';
  b.state.state.lists.turns.t1.color = 'red';
  await net.settle();
  // a deletes the container and makes it again, newer than b's edits
  delete a.state.state.lists;
  await net.settle();
  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t2: { text: 'y' } } }, 'nothing of the old record came back');
  assert.deepEqual(JSON.parse(JSON.stringify(b.state.state.lists)), { turns: { t2: { text: 'y' } } });
  assert.deepEqual(heard.b, [{ seq: 1, lost: [{ path: ['state', 'lists', 'turns', 't1', 'text'], mine: 'edited while it went', theirs: null }, { path: ['state', 'lists', 'turns', 't1', 'color'], mine: 'red', theirs: null }] }]);
});

test('undoing a deletion makes the container again, so it comes back', async () => {
  const { store, net, a } = setup();
  await net.settle();
  a.state.state.settings = { theme: 'dark', size: 2 };
  await net.settle();
  delete a.state.state.settings;
  await net.settle();
  assert.equal(store.snapshot().state.settings, undefined);
  assert.equal(a.undo(), true);
  await net.settle();
  assert.deepEqual(store.snapshot().state.settings, { theme: 'dark', size: 2 });
});

test('a container made by a client that had never seen it lands: what it writes is its own, not an edit of the old one', async () => {
  const { store, net, a, b } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  delete a.state.state.lists;
  await net.settle();
  b.state.state.lists = { turns: { t9: { text: 'from b' } } };
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t9: { text: 'from b' } } });
  assert.deepEqual(JSON.parse(JSON.stringify(a.state.state.lists)), { turns: { t9: { text: 'from b' } } });
});

test('a declared list under the container made again comes with it: its records are its author\'s, not an edit of the deleted ones', async () => {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time, lists: ['state/lists/*'] });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL, lists: ['state/lists/*'], now: fakeTime(START + 1000) });
  const b = net.client({ replicaId: 'b', initial: INITIAL, lists: ['state/lists/*'], now: fakeTime(START) });
  await net.settle();
  a.state.state.lists = { turns: [{ id: 't1', text: 'x' }] };
  await net.settle();
  assert.equal(store.snapshot().state.lists.turns.t1.text, 'x', 'a list on the wire is keyed by id');
  delete a.state.state.lists;
  await net.settle();
  assert.equal(b.state.state.lists, undefined);
  a.state.state.lists = { turns: [{ id: 't2', text: 'y' }] };
  await net.settle();
  assert.equal(store.snapshot().state.lists.turns.t2.text, 'y');
  assert.deepEqual(b.state.state.lists.turns.map(turn => turn.text), ['y']);
  // Emptying the list is not deleting it: nothing to lift, and a record added after lands as ever
  a.state.state.lists.turns = [];
  await net.settle();
  a.state.state.lists.turns.push({ id: 't3', text: 'z' });
  await net.settle();
  assert.deepEqual(b.state.state.lists.turns.map(turn => turn.text), ['z']);
});

test('an edit made offline after the re-add, with a clock that runs ahead, is refused all the same: stale is what was seen, not when', async () => {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: fakeTime(START) });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now: fakeTime(START + 60_000) });   // b's clock runs a minute ahead
  const heard = [];
  b.on('conflict', c => heard.push(c));
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  b.link.goOffline();
  await net.settle();
  // Monday: a deletes the container and makes it again; Tuesday, still offline, b edits the old one, with a timestamp past a's re-add
  delete a.state.state.lists;
  await net.settle();
  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  b.state.state.lists.turns.t1.text = 'edited, later by the clock';
  await net.settle();
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.deepEqual(store.snapshot().state.lists, { turns: { t2: { text: 'y' } } }, 'the edit of the deleted container is refused, however late it was made');
  assert.deepEqual(JSON.parse(JSON.stringify(b.state.state.lists)), { turns: { t2: { text: 'y' } } });
  assert.deepEqual(heard, [{ seq: 1, lost: [{ path: ['state', 'lists', 'turns', 't1', 'text'], mine: 'edited, later by the clock', theirs: null }] }]);
  // Back in sync, b's edit of the re-added container lands as ever
  b.state.state.lists.turns.t2.text = 'z';
  await net.settle();
  assert.equal(store.snapshot().state.lists.turns.t2.text, 'z');
});

test('a version from another storage life says nothing of what was seen here: such an op, or a relay\'s own counting, is judged as made in sight of nothing', async () => {
  const { store, net, a } = setup();
  await net.settle();
  a.state.state.lists = { turns: { t1: { text: 'x' } } };
  await net.settle();
  delete a.state.state.lists;
  await net.settle();
  a.state.state.lists = { turns: { t2: { text: 'y' } } };
  await net.settle();
  const v = store.version;
  const edit = (replicaId, extra) => store.apply({ replicaId, seq: 1, ts: [START + 5000, 0, replicaId], diff: { state: { lists: { turns: { t2: { color: 'red' } } } } }, ...extra });
  assert.equal(edit('r1', { v, epoch: 'another-life' }).accepted, null, 'a restored backup\'s versions are not this store\'s');
  assert.equal(edit('r2', { v, epoch: null }).accepted, null, 'a relay\'s own counting is not either');
  assert.equal(edit('r3', { v }).accepted, null, 'no epoch, no claim');
  assert.deepEqual(edit('r4', { v, epoch: store.epoch }).accepted, { state: { lists: { turns: { t2: { color: 'red' } } } } }, 'this store\'s version: seen');
});
