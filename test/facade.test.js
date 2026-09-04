// facade.test.js - Plain arrays in the client's state, keyed maps with positions on the wire
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createNetwork, seededRandom, fakeTime } from './helpers.js';

const WIRE_INITIAL = { tasks: {}, settings: { theme: 'light' } };
const VIEW_INITIAL = { tasks: [], settings: { theme: 'light' } };
const LISTS = ['tasks', 'tasks/*/subtasks'];
const snap = state => LazyWatch.snapshot(state);
const titles = array => array.map(t => t.title);
const wireOrder = (map, position = 'pos') => Object.entries(map ?? {}).sort(([ia, p], [ib, q]) => (p[position] < q[position] ? -1 : p[position] > q[position] ? 1 : ia < ib ? -1 : 1)).map(([, r]) => r.title);

function setup() {
  const store = createStore({ initial: WIRE_INITIAL });
  const net = createNetwork(store);
  const app = net.client({ replicaId: 'app', initial: VIEW_INITIAL, lists: LISTS });     // arrays
  const plainClient = net.client({ replicaId: 'plain', initial: WIRE_INITIAL });       // the wire model
  return { store, net, app, plain: plainClient };
}

test('the view is arrays, the wire is maps with positions; push, splice, edits, and reorders all sync', async () => {
  const { store, net, app, plain } = setup();
  await net.settle();
  assert.deepEqual(snap(app.state), VIEW_INITIAL);
  assert.equal(app.wire !== app.state, true);
  assert.equal(plain.wire, plain.state, 'without lists the two are one');

  app.state.tasks.push({ title: 'one' }, { title: 'two' });
  await net.settle();
  assert.equal(typeof app.state.tasks[0].id, 'string', 'an id was minted and written back');
  assert.equal(app.state.tasks[0].pos, undefined, 'positions stay on the wire');
  assert.deepEqual(wireOrder(store.snapshot().tasks), ['one', 'two']);
  assert.deepEqual(plain.list('tasks').all().map(t => t.title), ['one', 'two']);

  app.state.tasks.splice(1, 0, { id: 'mid', title: 'one and a half' });
  app.state.tasks[0].title = 'first';
  app.state.tasks[0].done = true;
  await net.settle();
  assert.deepEqual(wireOrder(store.snapshot().tasks), ['first', 'one and a half', 'two']);
  assert.equal(store.snapshot().tasks.mid.pos > store.snapshot().tasks[app.state.tasks[0].id].pos, true);
  assert.equal(plain.list('tasks').get('mid').title, 'one and a half');
  assert.equal(plain.list('tasks').all()[0].done, true);

  app.state.tasks.reverse();
  await net.settle();
  assert.deepEqual(wireOrder(store.snapshot().tasks), ['two', 'one and a half', 'first']);
  assert.deepEqual(titles(app.state.tasks), ['two', 'one and a half', 'first']);

  const before = app.pending;
  app.state.tasks = snap(app.state.tasks).filter(t => t.id !== 'mid');
  await net.settle();
  assert.deepEqual(wireOrder(store.snapshot().tasks), ['two', 'first']);
  assert.equal(store.snapshot().tasks.mid, undefined);
  assert.equal(app.pending, before);
  assert.deepEqual(snap(app.wire), store.snapshot(), 'the wire state is the server state');
});

test('changes from others arrive as array changes: inserts at their sorted place, moves, deletes, and field edits', async () => {
  const { store, net, app, plain } = setup();
  await net.settle();
  const seen = [];
  app.watch((diff, inverse, meta) => seen.push(meta?.origin));
  const list = plain.list('tasks');
  const a = list.add({ title: 'a' });
  const c = list.add({ title: 'c' });
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['a', 'c']);
  const b = list.add({ title: 'b' }, { before: c });
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['a', 'b', 'c'], 'inserted at its place, not appended');
  list.move(c, { at: 0 });
  plain.state.tasks[a].title = 'a!';
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['c', 'a!', 'b']);
  list.remove(b);
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['c', 'a!']);
  assert.ok(seen.every(origin => origin === 'remote'), `the app saw remote batches: ${JSON.stringify([...new Set(seen)])}`);

  store.patch({ settings: { theme: 'dark' }, tasks: { [a]: { subtasks: { s1: { id: 's1', title: 'sub', pos: 'a0' } } } } });
  await net.settle();
  assert.equal(app.state.settings.theme, 'dark');
  assert.deepEqual(titles(app.state.tasks[1].subtasks), ['sub'], 'a nested list appears as an array');
});

test('nested lists: arrays inside records translate both ways', async () => {
  const { store, net, app, plain } = setup();
  await net.settle();
  app.state.tasks.push({ id: 't', title: 'parent', subtasks: [{ title: 'x' }, { title: 'y' }] });
  await net.settle();
  assert.deepEqual(wireOrder(store.snapshot().tasks.t.subtasks), ['x', 'y']);
  app.state.tasks[0].subtasks.unshift({ title: 'w' });
  app.state.tasks[0].subtasks[2].title = 'y!';
  await net.settle();
  assert.deepEqual(wireOrder(store.snapshot().tasks.t.subtasks), ['w', 'x', 'y!']);
  plain.list('tasks/t/subtasks').move(plain.list('tasks/t/subtasks').ids()[0], { at: 2 });
  await net.settle();
  assert.deepEqual(titles(app.state.tasks[0].subtasks), ['x', 'y!', 'w']);
  app.state.tasks[0].subtasks = [];
  await net.settle();
  assert.deepEqual(store.snapshot().tasks.t.subtasks, {});
});

test('undo and redo run through the wire and show in the view; a rejected batch rolls the view back too', async () => {
  const { store, net, app } = setup();
  await net.settle();
  app.state.tasks.push({ id: 'u', title: 'undo me' });
  await net.settle();
  app.state.tasks[0].title = 'edited';
  await net.settle();
  assert.equal(app.undo(), true);
  await net.settle();
  assert.equal(app.state.tasks[0].title, 'undo me');
  assert.equal(store.snapshot().tasks.u.title, 'undo me');
  assert.equal(app.undo(), true);
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), [], 'the add was undone');
  assert.equal(app.redo(), true);
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['undo me']);
  assert.equal(store.snapshot().tasks.u.title, 'undo me');

  const errors = [];
  app.on('error', err => errors.push(err.message));
  app.state.settings.bad = [{ id: 'x' }];
  await net.settle();
  assert.equal(errors.length, 1);
  assert.equal(app.state.settings.bad, undefined, 'rolled back on the view as well');
});

test('offline edits in the view, a snapshot on reconnect, and a client restored from its cache', async () => {
  const storage = memoryOutbox();
  const store = createStore({ initial: WIRE_INITIAL });
  const net = createNetwork(store);
  const link = net.link();
  const app = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: VIEW_INITIAL, lists: LISTS, storage, replicaId: 'app' });
  app.connect();
  await net.settle();
  app.state.tasks.push({ id: 'online', title: 'online' });
  await net.settle();
  link.goOffline();
  await net.settle();
  app.state.tasks.unshift({ id: 'offline', title: 'offline' });
  store.patch({ tasks: { server: { id: 'server', title: 'from the server', pos: 'a5' } } });
  await net.settle();
  assert.deepEqual(titles(app.state.tasks), ['offline', 'online']);
  app.dispose();

  const restored = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: VIEW_INITIAL, lists: LISTS, storage });
  assert.equal(restored.restored, true);
  assert.deepEqual(titles(restored.state.tasks), ['offline', 'online'], 'the view is rebuilt from the cached wire state');
  assert.equal(restored.pending, 1);
  link.goOnline();
  restored.connect();
  await net.settle();
  assert.equal(restored.pending, 0);
  assert.deepEqual(wireOrder(store.snapshot().tasks), ['offline', 'online', 'from the server']);
  assert.deepEqual(titles(restored.state.tasks), ['offline', 'online', 'from the server']);
  assert.deepEqual(snap(restored.wire), store.snapshot());
});

test('two array clients editing the same list while apart converge with the wire and with each other', async () => {
  const START = 1_000_000;
  const store = createStore({ initial: WIRE_INITIAL, now: fakeTime(START) });
  const net = createNetwork(store);
  const xTime = fakeTime(START);
  const yTime = fakeTime(START);
  const x = net.client({ replicaId: 'x', initial: VIEW_INITIAL, lists: LISTS, now: xTime });
  const y = net.client({ replicaId: 'y', initial: VIEW_INITIAL, lists: LISTS, now: yTime });
  await net.settle();
  x.state.tasks.push({ id: 'a', title: 'a' }, { id: 'b', title: 'b' }, { id: 'c', title: 'c' });
  await net.settle();
  assert.deepEqual(titles(y.state.tasks), ['a', 'b', 'c']);

  x.link.goOffline();
  y.link.goOffline();
  await net.settle();
  xTime.advance(10);
  x.state.tasks.splice(1, 0, { id: 'x1', title: 'x1' });
  x.state.tasks[0].title = 'a by x';
  await net.settle();
  yTime.advance(20);                          // y's deletion is the newer write
  y.state.tasks.splice(1, 0, { id: 'y1', title: 'y1' });
  y.state.tasks.reverse();
  y.state.tasks.pop();                       // drops 'a'
  await net.settle();

  x.link.goOnline(); x.connect();
  y.link.goOnline(); y.connect();
  await net.settle();
  const wire = wireOrder(store.snapshot().tasks);
  assert.deepEqual(titles(x.state.tasks), wire);
  assert.deepEqual(titles(y.state.tasks), wire);
  assert.deepEqual(new Set(wire), new Set(['x1', 'y1', 'b', 'c']), 'both inserts survived, the deletion won');
  assert.deepEqual(snap(x.wire), store.snapshot());
  assert.deepEqual(snap(y.wire), store.snapshot());
});

test('random array edits on three view clients, offline and online, always converge (fuzz)', async () => {
  for (let run = 0; run < 15; run++) {
    const rng = seededRandom(100 + run);
    const store = createStore({ initial: WIRE_INITIAL, rateLimit: false });
    const net = createNetwork(store);
    const clients = [0, 1, 2].map(i => net.client({ replicaId: `v${i}`, initial: VIEW_INITIAL, lists: LISTS }));
    await net.settle();
    const ops = [];
    for (let step = 0; step < 40; step++) {
      const c = rng.pick(clients);
      const tasks = c.state.tasks;
      const roll = rng.next();
      if (roll < 0.25) { tasks.splice(rng.int(tasks.length + 1), 0, { title: `t${step}` }); ops.push('insert'); }
      else if (roll < 0.4 && tasks.length) { tasks.splice(rng.int(tasks.length), 1); ops.push('delete'); }
      else if (roll < 0.55 && tasks.length) { tasks[rng.int(tasks.length)].title = `e${step}`; ops.push('edit'); }
      else if (roll < 0.65 && tasks.length > 1) { c.state.tasks = snap(tasks).sort(() => rng.next() - 0.5); ops.push('shuffle'); }
      else if (roll < 0.75 && tasks.length) { const t = tasks[rng.int(tasks.length)]; if (!t.subtasks) t.subtasks = []; t.subtasks.push({ title: `s${step}` }); ops.push('sub'); }
      else if (roll < 0.85) { if (c.status === 'offline') { c.link.goOnline(); c.connect(); ops.push('online'); } else { c.link.goOffline(); ops.push('offline'); } }
      else if (roll < 0.92 && c.canUndo) { c.undo(); ops.push('undo'); }
      else { tasks.push({ title: `p${step}` }); ops.push('push'); }
      await net.settle();
    }
    for (const c of clients) if (c.status === 'offline') { c.link.goOnline(); c.connect(); }
    await net.settle();
    const expected = wireOrder(store.snapshot().tasks);
    for (const c of clients) {
      assert.equal(c.pending, 0, `run ${run}: ${c.replicaId} still pending after ${ops.join(',')}`);
      assert.deepEqual(snap(c.wire), store.snapshot(), `run ${run}: ${c.replicaId} wire diverged`);
      assert.deepEqual(titles(c.state.tasks), expected, `run ${run}: ${c.replicaId} view order diverged after ${ops.join(',')}`);
      for (const task of c.state.tasks) {
        const rec = store.snapshot().tasks[task.id];
        assert.deepEqual(titles(task.subtasks ?? []), wireOrder(rec.subtasks), `run ${run}: subtasks of ${task.id}`);
      }
    }
    for (const c of clients) c.dispose();
    store.dispose();
  }
});
