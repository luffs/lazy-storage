import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], parents: { subtasks: { taskId: 'tasks' } } };
const snap = client => LazyWatch.snapshot(client.state);

function setup(options = {}) {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, ...options });
  const net = createNetwork(store);
  const client = (replicaId, extra = {}) => net.client({ replicaId, ...SHAPE, now, ...extra });
  return { store, net, now, client };
}

test('a client gets the snapshot and another client\'s changes', async () => {
  const { store, net, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  assert.equal(a.status, 'online');
  a.state.tasks.k = { title: 'Milk', meta: { color: 'red' } };
  a.state.settings.theme = 'dark';
  await net.settle();
  assert.deepEqual(snap(b), { tasks: { k: { title: 'Milk', meta: { color: 'red' } } }, subtasks: {}, settings: { theme: 'dark' } });
  assert.deepEqual(store.snapshot(), snap(a));
  assert.equal(a.pending, 0);
});

test('two clients changing different paths deep in one record both keep theirs', async () => {
  const { net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.k = { meta: { color: 'red', size: 'M' } };
  await net.settle();
  a.link.goOffline();
  b.link.goOffline();
  await net.settle();
  a.state.tasks.k.meta.color = 'blue';
  await net.settle();
  now.advance(10);
  b.state.tasks.k.meta.size = 'L';
  await net.settle();
  for (const c of [a, b]) { c.link.goOnline(); c.connect(); }
  await net.settle();
  assert.deepEqual(snap(a).tasks.k.meta, { color: 'blue', size: 'L' });
  assert.deepEqual(snap(b), snap(a));
});

test('on the same path the later edit wins, whoever syncs first', async () => {
  const { net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  a.link.goOffline();
  b.link.goOffline();
  await net.settle();
  b.state.tasks.k.title = 'Oat milk';
  await net.settle();
  now.advance(100);
  a.state.tasks.k.title = 'Skimmed';
  await net.settle();
  a.link.goOnline(); a.connect();
  await net.settle();
  b.link.goOnline(); b.connect();
  await net.settle();
  assert.equal(a.state.tasks.k.title, 'Skimmed');
  assert.equal(b.state.tasks.k.title, 'Skimmed');
});

test('a client whose own edit lost shows the winner once acknowledged, and hears why', async () => {
  const { net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  a.link.goOffline();
  await net.settle();
  a.state.tasks.k.title = 'Mjölk';   // 10:01
  await net.settle();
  now.advance(100);
  b.state.tasks.k.title = 'Havremjölk';   // 10:02
  await net.settle();
  assert.equal(a.state.tasks.k.title, 'Mjölk', 'its own pending edit shows while it waits');
  const lost = [];
  a.on('lost', event => lost.push(event));
  a.link.goOnline(); a.connect();
  await net.settle();
  assert.equal(a.state.tasks.k.title, 'Havremjölk');
  assert.equal(lost.length, 1);
  assert.deepEqual(lost[0].lost, [{ c: 'tasks', id: 'k', path: ['title'], why: 'newer' }]);
});

test('an edit of a record deleted meanwhile is lost; a deletion older than an edit loses', async () => {
  const { net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.x = { title: 'x' };
  a.state.tasks.y = { title: 'y' };
  await net.settle();
  a.link.goOffline();
  await net.settle();
  a.state.tasks.x.title = 'edited offline';
  a.state.tasks.y = null;   // an old deletion
  await net.settle();
  now.advance(50);
  b.state.tasks.x = null;
  b.state.tasks.y.title = 'edited later';
  await net.settle();
  a.link.goOnline(); a.connect();
  await net.settle();
  assert.equal(a.state.tasks.x, undefined);
  assert.equal(a.state.tasks.y.title, 'edited later');
  assert.deepEqual(snap(a), snap(b));
});

test('a child is never left without its parent', async () => {
  const { store, net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.t = { title: 'Shop' };
  a.state.subtasks.s1 = { taskId: 't', title: 'Milk' };
  await net.settle();
  b.link.goOffline();
  await net.settle();
  b.state.subtasks.s2 = { taskId: 't', title: 'Eggs' };
  await net.settle();
  now.advance(10);
  a.state.tasks.t = null;
  await net.settle();
  assert.deepEqual(store.state.subtasks, {}, 'the server deletes the children with the parent');
  b.link.goOnline(); b.connect();
  await net.settle();
  assert.deepEqual(store.state.subtasks, {});
  assert.deepEqual(snap(b).subtasks, {});
  assert.deepEqual(snap(a), snap(b));
});

test('rules judge each record before and after, and a refused op is taken back', async () => {
  const { net, client } = setup({ rules: { settings: false, tasks: (user, before, after) => (after ?? before).owner === user?.id } });
  const a = net.client({ replicaId: 'a', ...SHAPE }, { user: { id: 'u1' } });
  await net.settle();
  const rejected = [];
  a.on('rejected', event => rejected.push(event));
  a.state.tasks.k = { owner: 'u1', title: 'mine' };
  await net.settle();
  assert.equal(rejected.length, 0);
  a.state.tasks.j = { owner: 'u2', title: 'theirs' };
  a.state.settings.theme = 'dark';
  await net.settle();
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].code, 'forbidden');
  assert.equal(a.state.tasks.j, undefined);
  assert.equal(a.state.settings.theme, 'light');
  assert.equal(a.state.tasks.k.title, 'mine');
  assert.ok(client);
});

test('a reconnect after a blip is answered with a delta', async () => {
  const { store, net, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.k = { n: 1 };
  await net.settle();
  b.link.goOffline();
  await net.settle();
  a.state.tasks.k.n = 2;
  a.state.tasks.j = { n: 3 };
  await net.settle();
  const before = store.stats().sent.snapshot?.messages ?? 0;
  b.link.goOnline(); b.connect();
  await net.settle();
  assert.equal(store.stats().sent.snapshot?.messages ?? 0, before, 'no snapshot');
  assert.equal(store.stats().sent.delta.messages, 1);
  assert.deepEqual(snap(b), snap(a));
});

test('typing into one field keeps one op pending', async () => {
  const { net, client } = setup();
  const a = client('a');
  await net.settle();
  a.state.tasks.k = { title: '' };
  await net.settle();
  a.link.goOffline();
  await net.settle();
  for (const text of ['M', 'Mi', 'Mil', 'Milk']) {
    a.state.tasks.k.title = text;
    await net.settle();
  }
  assert.equal(a.pending, 1);
  a.link.goOnline(); a.connect();
  await net.settle();
  assert.equal(a.pending, 0);
  assert.equal(a.state.tasks.k.title, 'Milk');
});

test('undo takes back this client\'s own edit, and syncs', async () => {
  const { net, now, client } = setup();
  const a = client('a');
  const b = client('b');
  await net.settle();
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  await net.settle();
  now.advance(10);
  b.state.tasks.k.done = true;
  await net.settle();
  await net.settle();
  now.advance(10);
  a.state.tasks.k.title = 'Eggs';
  await net.settle();
  a.undo();
  await net.settle();
  assert.deepEqual(snap(b).tasks.k, { title: 'Milk', done: true });
  assert.deepEqual(snap(a), snap(b));
});

test('a register is written and merged whole', async () => {
  const { net, now } = setup();
  const store2 = createStore({ ...SHAPE, registers: ['tasks/*/geo'], now });
  const net2 = createNetwork(store2);
  const a = net2.client({ replicaId: 'a', ...SHAPE, registers: ['tasks/*/geo'], now });
  const b = net2.client({ replicaId: 'b', ...SHAPE, registers: ['tasks/*/geo'], now });
  await net2.settle();
  a.state.tasks.w = { geo: { x: 1, y: 1 } };
  await net2.settle();
  a.link.goOffline();
  await net2.settle();
  a.state.tasks.w.geo.x = 5;
  await net2.settle();
  now.advance(10);
  b.state.tasks.w.geo.y = 9;
  await net2.settle();
  a.link.goOnline(); a.connect();
  await net2.settle();
  assert.deepEqual(snap(a).tasks.w.geo, { x: 1, y: 9 }, 'the later whole value wins; a\'s x is not merged in');
  assert.deepEqual(snap(a), snap(b));
  assert.ok(net);
});

test('a write that breaks the shape is reverted and reported', async () => {
  const { net, client } = setup();
  const a = client('a');
  await net.settle();
  const rejected = [];
  a.on('rejected', event => rejected.push(event));
  a.state.nothing = { x: 1 };
  await net.settle();
  assert.equal(rejected.length, 1);
  assert.equal(a.state.nothing, undefined);
  assert.equal(a.pending, 0);
});

test('the server\'s own patch merges, makes what is missing, and reaches clients', async () => {
  const { store, net, client } = setup();
  const a = client('a');
  await net.settle();
  store.patch({ tasks: { r: { runs: { d1: 'running' } } } });
  store.patch({ tasks: { r: { runs: { d2: 'failed' } } } });
  await net.settle();
  assert.deepEqual(snap(a).tasks.r, { runs: { d1: 'running', d2: 'failed' } });
  store.patch({ tasks: { r: null } });
  await net.settle();
  assert.equal(a.state.tasks.r, undefined);
});

test('validate judges the whole op as one diff, and readOnly names what is the server\'s alone', async () => {
  const seen = [];
  const { net, client } = setup({
    readOnly: ['settings'],
    validate: (diff, { user, replicaId }) => {
      seen.push([Object.keys(diff), replicaId]);
      if (diff.tasks?.bad) throw new Error('No bad tasks');
    }
  });
  const a = client('a');
  await net.settle();
  const rejected = [];
  a.on('rejected', e => rejected.push(e.message));
  a.state.tasks.good = { n: 1 };
  await net.settle();
  a.state.tasks.bad = { n: 2 };
  await net.settle();
  a.state.settings.theme = 'dark';
  await net.settle();
  assert.deepEqual(rejected, ['No bad tasks', "settings is the server's to write"]);
  assert.deepEqual(Object.keys(a.state.tasks), ['good']);
  assert.equal(a.state.settings.theme, 'light');
  assert.deepEqual(seen, [[['tasks'], 'a'], [['tasks'], 'a']]);
});
