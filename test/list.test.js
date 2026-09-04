// list.test.js - db.list: an ordered list of records with a position on each, no register anywhere
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const titles = list => list.all().map(t => t.title);

function setup() {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  return { store, net, a, b };
}

test('add at the end, before, after, and at; move; remove; everyone sorts the same way', async () => {
  const { store, net, a, b } = setup();
  await net.settle();
  const list = a.list('tasks');
  const one = list.add({ title: 'one' });
  const three = list.add({ title: 'three' });
  const two = list.add({ title: 'two' }, { before: three });
  list.add({ title: 'zero' }, { at: 0 });
  list.add({ title: 'two and a half' }, { after: two });
  assert.deepEqual(titles(list), ['zero', 'one', 'two', 'two and a half', 'three']);
  assert.equal(list.get(one).title, 'one');
  assert.equal(list.has('nope'), false);
  assert.equal(typeof list.get(one).pos, 'string', 'the position lives on the record');
  await net.settle();
  assert.deepEqual(titles(b.list('tasks')), ['zero', 'one', 'two', 'two and a half', 'three'], 'the other client sorts the same');
  assert.deepEqual(Object.keys(store.snapshot()), ['tasks'], 'no register on the server');

  assert.equal(list.move(three, { at: 0 }), true);
  assert.equal(list.move(one, { after: two }), true);
  assert.equal(list.move('nope', { at: 0 }), false);
  assert.deepEqual(titles(list), ['three', 'zero', 'two', 'one', 'two and a half']);
  assert.equal(list.remove(two), true);
  assert.equal(list.remove(two), false);
  await net.settle();
  assert.deepEqual(titles(b.list('tasks')), ['three', 'zero', 'one', 'two and a half']);
  assert.deepEqual(a.list('tasks').ids(), b.list('tasks').ids());
  assert.throws(() => list.add({ title: 'x' }, { before: 'nope' }), /No record "nope"/);
});

test('a move that lands where the record already is writes nothing; undo restores the old position', async () => {
  const { net, a } = setup();
  await net.settle();
  const list = a.list('tasks');
  const ids = ['x', 'y', 'z'].map(title => list.add({ title }));
  await net.settle();
  const before = a.pending;
  a.checkpoint();
  const pos = list.get(ids[1]).pos;
  list.move(ids[1], { at: 1 });
  list.move(ids[1], { after: ids[0] });
  list.move(ids[1], { before: ids[2] });
  await net.settle();
  assert.equal(list.get(ids[1]).pos, pos, 'the key is untouched');
  assert.equal(a.pending, before, 'no op was made');

  list.move(ids[2], { at: 0 });
  await net.settle();
  assert.deepEqual(titles(list), ['z', 'x', 'y']);
  a.undo();
  await net.settle();
  assert.deepEqual(titles(list), ['x', 'y', 'z'], 'a move is one field write, undone like any');
});

test('two clients inserting at the same spot while apart both keep their records, in the same order everywhere', async () => {
  const { store, net, a, b } = setup();
  await net.settle();
  const [x, y] = ['x', 'y'].map(title => a.list('tasks').add({ title }));
  await net.settle();
  a.link.goOffline();
  b.link.goOffline();
  await net.settle();
  a.list('tasks').add({ title: 'from a' }, { between: undefined, after: x });
  b.list('tasks').add({ title: 'from b' }, { after: x });
  b.list('tasks').move(y, { at: 0 });
  await net.settle();
  a.link.goOnline(); a.connect();
  b.link.goOnline(); b.connect();
  await net.settle();
  const serverOrder = Object.entries(store.snapshot().tasks).sort(([ia, p], [ib, q]) => (p.pos < q.pos ? -1 : p.pos > q.pos ? 1 : ia < ib ? -1 : 1)).map(([, t]) => t.title);
  assert.deepEqual(new Set(serverOrder), new Set(['x', 'y', 'from a', 'from b']), 'nothing was lost');
  assert.deepEqual(titles(a.list('tasks')), titles(b.list('tasks')), 'both clients agree');
  assert.deepEqual(titles(a.list('tasks')), serverOrder);
  assert.equal(titles(a.list('tasks'))[0], 'y', 'the move landed too');
  assert.deepEqual(titles(a.list('tasks')).slice(1, 2), ['x']);
  assert.equal(a.list('tasks').get(a.list('tasks').ids()[2]).pos, a.list('tasks').get(a.list('tasks').ids()[3]).pos, 'the same spot gave the same key; ids break the tie');
});

test('reconcile takes the order an app shows and writes only the positions that must change', async () => {
  const { net, a, b } = setup();
  await net.settle();
  const list = a.list('tasks');
  const ids = ['a', 'b', 'c', 'd', 'e', 'f'].map(title => list.add({ id: title, title }));
  await net.settle();
  const keys = Object.fromEntries(ids.map(id => [id, list.get(id).pos]));
  assert.equal(list.reconcile(['a', 'b', 'c', 'd', 'e', 'f']), 0, 'already in order');
  assert.equal(list.reconcile(['a', 'b', 'e', 'c', 'd', 'f']), 1, 'one record moved: one write');
  assert.deepEqual(list.ids(), ['a', 'b', 'e', 'c', 'd', 'f']);
  for (const id of ['a', 'b', 'c', 'd', 'f']) assert.equal(list.get(id).pos, keys[id], `${id} kept its key`);
  assert.equal(list.reconcile(['f', 'e', 'd', 'c', 'b', 'a']), 4, 'a reversal keeps the longest run still in order (e before d) and rewrites the rest');
  assert.deepEqual(list.ids(), ['f', 'e', 'd', 'c', 'b', 'a']);
  assert.equal(list.reconcile(['b', 'nope', 'a']), 2, 'unknown ids are ignored; unlisted records follow in their order and keep their keys, so the two named ones move');
  assert.deepEqual(list.ids(), ['b', 'a', 'f', 'e', 'd', 'c']);
  await net.settle();
  assert.deepEqual(b.list('tasks').ids(), ['b', 'a', 'f', 'e', 'd', 'c']);
});

test('records without a position sort last and get one when placed or reconciled; nested lists are just paths', async () => {
  const { store, net, a } = setup();
  store.patch({ tasks: { plain: { id: 'plain', title: 'no position yet' } } });
  await net.settle();
  const list = a.list('tasks');
  list.add({ id: 'p', title: 'positioned' });
  assert.deepEqual(list.ids(), ['p', 'plain'], 'unpositioned last');
  list.add({ id: 'q', title: 'also' }, { before: 'plain' });
  assert.deepEqual(list.ids(), ['p', 'q', 'plain'], 'inserting before an unpositioned record lands after every positioned one');
  assert.equal(list.reconcile(['plain', 'p', 'q']), 1, 'only the unpositioned one needs a key');
  assert.deepEqual(list.ids(), ['plain', 'p', 'q']);

  const subtasks = a.list(['tasks', 'p', 'subtasks']);
  subtasks.add({ title: 'first' });
  subtasks.add({ title: 'zeroth' }, { at: 0 });
  assert.deepEqual(subtasks.all().map(s => s.title), ['zeroth', 'first']);
  assert.deepEqual(a.list('tasks/p/subtasks').all().map(s => s.title), ['zeroth', 'first'], 'a slash path is the same list');
  await net.settle();
  assert.deepEqual(Object.values(store.snapshot().tasks.p.subtasks).map(s => s.title).sort(), ['first', 'zeroth']);
  assert.deepEqual(LazyWatch.snapshot(a.state), store.snapshot());
});
