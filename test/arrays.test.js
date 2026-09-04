// arrays.test.js - Arrays of primitives are whole values anywhere; arrays of records are refused unless declared
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore, memoryStorage, orderToPositions } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { leaves, expandRegisters } from '../src/core/model.js';
import { registerSet } from '../src/core/paths.js';
import { rebuild } from '../src/core/model.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {}, settings: {} };

test('an array of primitives is written whole, undeclared, from either side, and mutating it in place sends the whole value', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err.message));

  a.state.tasks.t1 = { id: 't1', tags: ['urgent', 'home'], point: [1, 2] };
  await net.settle();
  assert.deepEqual(store.snapshot().tasks.t1.tags, ['urgent', 'home']);
  assert.deepEqual([...b.state.tasks.t1.tags], ['urgent', 'home']);

  a.state.tasks.t1.tags.push('later');          // a fragment from lazy-watch, expanded from the live value
  a.state.tasks.t1.tags.splice(0, 1);
  await net.settle();
  assert.deepEqual(store.snapshot().tasks.t1.tags, ['home', 'later']);
  assert.deepEqual([...b.state.tasks.t1.tags], ['home', 'later']);

  store.patch({ settings: { order: ['b', 'a'], empty: [] } });
  await net.settle();
  assert.deepEqual([...a.state.settings.order], ['b', 'a']);
  assert.deepEqual([...a.state.settings.empty], []);
  assert.deepEqual(errors, []);
  assert.deepEqual(LazyWatch.snapshot(a.state), store.snapshot());
});

test('an array holding objects is refused with a pointer to lists, unless the path is a declared register', () => {
  const plain = registerSet([]);
  assert.throws(() => leaves({ tasks: [{ id: 'x' }] }, plain), /The array at "tasks" holds objects.*db\.list/);
  assert.throws(() => leaves({ deep: { list: [1, { a: 1 }] } }, plain), /"deep\/list" holds objects/);
  assert.deepEqual(leaves({ tags: ['a', 1, true, null], empty: [] }, plain), [[['tags'], ['a', 1, true, null]], [['empty'], []]]);
  const declared = registerSet(['tasks']);
  assert.deepEqual(leaves({ tasks: [{ id: 'x' }] }, declared), [[['tasks'], [{ id: 'x' }]]]);
  assert.throws(() => leaves({ tags: { 0: 'a', $length: 1 } }, plain), /must be written as a whole value, not a fragment/);

  const server = createStore({ initial: INITIAL });
  assert.throws(() => server.patch({ tasks: [{ id: 'x' }] }), /holds objects/);
  server.patch({ tasks: { x: { id: 'x', tags: ['ok'] } } });
  assert.deepEqual(server.snapshot().tasks.x.tags, ['ok']);
});

test('expandRegisters reads a touched array from the live state whether or not it is declared', () => {
  const state = new LazyWatch({ tags: ['a', 'b'], tasks: { t: { ids: ['x'] } }, n: 1 });
  const out = expandRegisters({ tags: { 1: 'c', $length: 2 }, tasks: { t: { ids: ['x', 'y'] } }, n: 2 }, registerSet([]), state);
  assert.deepEqual(out, { tags: ['a', 'b'], tasks: { t: { ids: ['x'] } }, n: 2 }, 'the live values, whatever the diff carried');
  assert.equal(Array.isArray(out.tags) && !LazyWatch.isProxy(out.tags), true, 'plain copies');
});

test('rows and the state cache hold undeclared arrays as leaves', async () => {
  const storage = memoryStorage();
  const store = createStore({ initial: INITIAL, storage });
  store.patch({ tasks: { t1: { id: 't1', tags: ['a'] } } });
  assert.deepEqual(storage.load().rows.find(([k]) => k === '["tasks","t1","tags"]')[1].value, ['a']);
  store.dispose();
  const reopened = createStore({ initial: INITIAL, storage });
  assert.deepEqual(reopened.snapshot().tasks.t1.tags, ['a']);
  assert.deepEqual(rebuild({}, [['["x"]', [1, 2]]]), { x: [1, 2] });
});

test('orderToPositions gives every record a position in the register\'s order and deletes the register, wildcards included', () => {
  const store = createStore({ initial: { tasks: {}, order: [] }, registers: ['order', 'tasks/*/subOrder'] });
  store.patch({
    tasks: {
      a: { id: 'a', subs: { s1: { id: 's1' }, s2: { id: 's2' } }, subOrder: ['s2', 's1'] },
      b: { id: 'b', subs: { s3: { id: 's3' } } },
      c: { id: 'c' }
    },
    order: ['c', 'a', 'ghost', 'a']
  });
  assert.deepEqual(orderToPositions(store, { list: 'tasks', order: 'order' }), { lists: 1, records: 3 });
  const tasks = store.snapshot().tasks;
  assert.deepEqual(Object.entries(tasks).sort(([, p], [, q]) => (p.pos < q.pos ? -1 : 1)).map(([id]) => id), ['c', 'a', 'b'], 'listed first, in order; the rest by id');
  assert.equal(store.snapshot().order, undefined, 'the register is gone');

  assert.deepEqual(orderToPositions(store, { list: 'tasks/*/subs', order: 'tasks/*/subOrder', position: 'rank' }), { lists: 2, records: 3 });
  const after = store.snapshot().tasks;
  assert.deepEqual(Object.entries(after.a.subs).sort(([, p], [, q]) => (p.rank < q.rank ? -1 : 1)).map(([id]) => id), ['s2', 's1']);
  assert.equal(after.a.subOrder, undefined);
  assert.equal(typeof after.b.subs.s3.rank, 'string', 'a list without a register still gets positions');
  assert.equal(after.c.subs, undefined, 'a task without the list is left alone');

  const net = createNetwork(store);
  const client = createClient({ transport: net.link().factory, reconnect: false, store: 'main', initial: { tasks: {} }, replicaId: 'r' });
  client.connect();
  return net.settle().then(() => {
    assert.deepEqual(client.list('tasks').ids(), ['c', 'a', 'b'], 'a client without any register declaration reads the migrated order');
    assert.deepEqual(client.list('tasks/a/subs', { position: 'rank' }).ids(), ['s2', 's1']);
  });
});
