// collection-watch.test.js - db.collection(name).watch: the records a batch changed, one entry each, with what they were
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { screens: {}, places: {} };

function setup() {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  return { store, net, a, b };
}

test('insert, update and remove, each with the record as it is and as it was', async () => {
  const { net, a } = setup();
  await net.settle();
  const seen = [];
  const stop = a.collection('screens').watch(changes => seen.push(changes));

  a.collection('screens').add({ id: 'kassa-1', name: 'Kassa 1', tags: ['kassa'] });
  await net.settle();
  a.collection('screens').update('kassa-1', { name: 'Kassa 1 norr' });
  a.state.screens['kassa-1'].tags.push('norr');
  await net.settle();
  a.collection('screens').remove('kassa-1');
  await net.settle();

  assert.deepEqual(seen, [
    [{ type: 'insert', id: 'kassa-1', record: { id: 'kassa-1', name: 'Kassa 1', tags: ['kassa'] } }],
    [
      {
        type: 'update',
        id: 'kassa-1',
        record: { id: 'kassa-1', name: 'Kassa 1 norr', tags: ['kassa', 'norr'] },
        previous: { id: 'kassa-1', name: 'Kassa 1', tags: ['kassa'] }
      }
    ],
    [{ type: 'remove', id: 'kassa-1', previous: { id: 'kassa-1', name: 'Kassa 1 norr', tags: ['kassa', 'norr'] } }]
  ]);
  stop();
});

test('a remote change arrives the same way, marked remote, and a copy is a new object each time', async () => {
  const { net, a, b } = setup();
  await net.settle();
  const seen = [];
  b.collection('screens').watch((changes, meta) => seen.push({ changes, origin: meta?.origin }));
  a.collection('screens').add({ id: 'entre', name: 'Entré', status: 'online' });
  await net.settle();
  a.state.screens.entre.status = 'offline';
  await net.settle();

  assert.equal(seen.length, 2);
  assert.equal(seen[0].origin, 'remote');
  assert.equal(seen[0].changes[0].type, 'insert');
  const [{ record, previous }] = seen[1].changes;
  assert.equal(record.status, 'offline');
  assert.equal(previous.status, 'online');
  assert.notEqual(record, b.state.screens.entre, 'a copy, not the live proxy');
  record.status = 'changed by the reader';
  assert.equal(b.state.screens.entre.status, 'offline', 'the copy does not write to the state');
});

test('only its own collection, and every record a batch touched in one call', async () => {
  const { net, a } = setup();
  await net.settle();
  const screens = [];
  a.collection('screens').watch(changes => screens.push(changes.map(c => `${c.type} ${c.id}`)));
  a.collection('places').add({ id: 'goteborg', name: 'Göteborg' });
  a.state.screens.x = { id: 'x', name: 'X' };
  a.state.screens.y = { id: 'y', name: 'Y' };
  await net.settle();
  assert.deepEqual(screens, [['insert x', 'insert y']]);
});

test('the whole collection replaced: what left is removed, what came is inserted, what stayed is updated', async () => {
  const { net, a } = setup();
  await net.settle();
  a.state.screens = { x: { id: 'x', name: 'X' }, y: { id: 'y', name: 'Y' } };
  await net.settle();
  const seen = [];
  a.collection('screens').watch(changes => seen.push(...changes.map(c => `${c.type} ${c.id}`)));
  a.state.screens = { y: { id: 'y', name: 'Y2' }, z: { id: 'z', name: 'Z' } };
  await net.settle();
  assert.deepEqual(seen.sort(), ['insert z', 'remove x', 'update y']);
});

test('the watcher stops when told', async () => {
  const { net, a } = setup();
  await net.settle();
  let calls = 0;
  const stop = a.collection('screens').watch(() => calls++);
  a.collection('screens').add({ id: 'one' });
  await net.settle();
  stop();
  a.collection('screens').add({ id: 'two' });
  await net.settle();
  assert.equal(calls, 1);
});
