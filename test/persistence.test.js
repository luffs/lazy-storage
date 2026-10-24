import { test } from 'node:test';
import assert from 'node:assert/strict';
import 'fake-indexeddb/auto';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { openClient } from '../src/client/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { indexedDBStorage } from '../src/client/indexeddb.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };
const snap = client => LazyWatch.snapshot(client.state);

for (const [kind, make] of [['a document adapter', () => memoryOutbox()], ['IndexedDB', () => indexedDBStorage(`test-${Math.random()}`)]]) {
  test(`a client restarted offline on ${kind} starts from its base and its pending edits, judged as before`, async () => {
    const now = fakeTime();
    const store = createStore({ ...SHAPE, now });
    const net = createNetwork(store);
    const storage = make();
    const open = async () => {
      const link = net.link();
      const db = await openClient({ transport: link.factory, reconnect: false, store: 'main', replicaId: 'a', storage, cacheDelay: 0, ...SHAPE, now });
      db.link = link;
      return db;
    };
    let a = await open();
    a.connect();
    const b = net.client({ replicaId: 'b', ...SHAPE, now });
    await net.settle();
    a.state.tasks.k = { title: 'Milk', meta: { color: 'red' } };
    await net.settle();
    a.link.goOffline();
    await net.settle();
    a.state.tasks.k.meta.color = 'blue';
    a.state.settings.theme = 'dark';
    await net.settle();
    await new Promise(resolve => setTimeout(resolve, 5));
    await storage.settled?.();
    a.dispose();

    // A newer edit of the same field lands meanwhile
    now.advance(10);
    b.state.tasks.k.meta.color = 'green';
    await net.settle();

    a = await open();
    assert.equal(a.restored, true);
    assert.equal(a.pending, 1);
    assert.deepEqual(snap(a), { tasks: { k: { title: 'Milk', meta: { color: 'blue' } } }, settings: { theme: 'dark' } });
    a.connect();
    await net.settle();
    assert.equal(a.pending, 0);
    assert.deepEqual(snap(a), { tasks: { k: { title: 'Milk', meta: { color: 'green' } } }, settings: { theme: 'dark' } }, 'the older offline edit lost, the theme landed');
    assert.deepEqual(snap(a), store.snapshot());
    a.dispose();
  });
}
