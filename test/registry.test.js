import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, isStoreId, memoryStorage } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };

test('store ids are URL- and filename-safe', () => {
  for (const ok of ['a', 'team-1', 'Team_2.v3', 'x~y', 'a'.repeat(128)]) assert.equal(isStoreId(ok), true, ok);
  for (const bad of ['', '-lead', 'has space', 'slash/inside', 'a'.repeat(129), 42, null, '../etc']) assert.equal(isStoreId(bad), false, String(bad));
});

test('a registry creates a store per id on first use and hands back the same instance after', () => {
  const created = [];
  const stores = createStores(id => { created.push(id); return createStore({ initial: INITIAL, storage: memoryStorage() }); });
  const a1 = stores.get('a');
  const a2 = stores.get('a');
  const b = stores.get('b');
  assert.equal(a1, a2);
  assert.notEqual(a1, b);
  assert.deepEqual(created, ['a', 'b']);
  assert.deepEqual(stores.ids(), ['a', 'b']);
  assert.equal(stores.get('not valid!'), null, 'an invalid id is refused before the factory runs');
  assert.equal(created.length, 2);
  stores.dispose();
  assert.deepEqual(stores.ids(), []);
});

test('a factory may refuse an id', () => {
  const stores = createStores(id => (id.startsWith('team-') ? createStore({ initial: INITIAL }) : null));
  assert.ok(stores.get('team-1'));
  assert.equal(stores.get('other'), null);
  assert.equal(stores.has('other'), false);
});

test('stores in one registry are isolated: clients of one never see the other', async () => {
  const stores = createStores(() => createStore({ initial: INITIAL, storage: memoryStorage(), now: fakeTime() }));
  const netA = createNetwork(stores.get('team-a'));
  const netB = createNetwork(stores.get('team-b'));
  const a = netA.client({ replicaId: 'a', initial: INITIAL });
  const b = netB.client({ replicaId: 'b', initial: INITIAL });
  await netA.settle(); await netB.settle();

  a.collection('tasks').add({ id: 'only-a', title: 'A' });
  b.collection('tasks').add({ id: 'only-b', title: 'B' });
  await netA.settle(); await netB.settle();

  assert.deepEqual(stores.get('team-a').snapshot(), { tasks: { 'only-a': { id: 'only-a', title: 'A' } } });
  assert.deepEqual(stores.get('team-b').snapshot(), { tasks: { 'only-b': { id: 'only-b', title: 'B' } } });
  assert.equal(a.collection('tasks').has('only-b'), false);
  assert.equal(b.collection('tasks').has('only-a'), false);

  assert.equal(stores.release('team-a'), true, 'releasing disposes the store and closes its sessions');
  assert.equal(stores.has('team-a'), false);
  a.dispose(); b.dispose();
  stores.dispose();
});
