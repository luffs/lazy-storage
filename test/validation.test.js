// validation.test.js - What the constructors refuse, said at once: a shape, a
// store document, a store's options, a relay's, a shared connection's; and a
// shared connection on a plain document adapter, which keeps the browser's
// replica across a reload
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createShape } from '../src/core/index.js';
import { createStore, memoryStorage, assertDocument } from '../src/server/index.js';
import { createRelay } from '../src/relay/index.js';
import { createClient, sharedConnection, memoryOutbox } from '../src/index.js';
import { createNetwork } from './helpers.js';

test('a shape refuses what it cannot be', () => {
  assert.throws(() => createShape({ initial: [] }), /plain object/);
  assert.throws(() => createShape({ initial: { a: {} }, singletons: ['b'] }), /Singleton "b" is not in initial/);
  assert.throws(() => createShape({ initial: JSON.parse('{"__proto__":{}}') }), /reserved/);
  assert.throws(() => createShape({ initial: { a: {}, s: 1 }, parents: { s: { x: 'a' } } }), /"s" is not a collection/);
  assert.throws(() => createShape({ initial: { a: {}, b: {} }, parents: { b: { aId: 'nope' } } }), /not a collection/);
  const shape = createShape({ initial: { a: {}, b: {} }, parents: { b: { aId: 'a' } } });
  assert.deepEqual(shape.parentsOf('b'), [{ field: 'aId', parent: 'a' }]);
});

test('a store document is checked before any adapter takes it', () => {
  assert.throws(() => assertDocument(null), /records and a version/);
  assert.throws(() => assertDocument({ records: [], version: 1.5 }), /records and a version/);
  assert.throws(() => assertDocument({ records: [['tasks', 3, {}, {}]], version: 1 }), /A record is/);
  assert.throws(() => assertDocument({ records: [['tasks', 'a']], version: 1 }), /A record is/);
  assert.doesNotThrow(() => assertDocument({ records: [['tasks', 'a', {}, { live: {}, gone: {} }], ['s', null, {}, { live: {}, gone: {} }]], version: 2 }));

  const storage = memoryStorage();
  assert.equal(storage.load(), null, 'never seen');
  storage.replace({ records: [['tasks', 'a', { title: 'x' }, { live: { '[]': 1 }, gone: {} }]], replicas: { r: { seq: 3, seen: 1 } }, version: 4, epoch: 'e', schema: 2, log: [{ v: 4, ts: 1, changes: [] }] });
  const store = createStore({ initial: { tasks: {} }, storage, migrations: [() => {}, () => {}] });
  assert.equal(store.state.tasks.a.title, 'x');
  assert.equal(store.version, 4);
  assert.notEqual(store.epoch, 'e', 'a replaced document starts a new life');
  assert.equal(store.stats().schema, 2);
  store.dispose();
});

test('a store refuses options it cannot work with, and storage migrated by a newer version', () => {
  assert.throws(() => createStore({ rateLimit: { burst: 0, perSecond: 1 } }), /rateLimit/);
  assert.throws(() => createStore({ validate: 'yes' }), /validate/);
  assert.throws(() => createStore({ initial: { tasks: {} }, readOnly: ['nope'] }), /readOnly/);
  assert.throws(() => createStore({ migrations: ['not a function'] }), /migrations/);
  assert.throws(() => createStore({ presence: 'on' }), /presence/);
  const storage = memoryStorage();
  storage.replace({ records: [], replicas: {}, version: 0, epoch: 'e', schema: 3 });
  assert.throws(() => createStore({ storage, migrations: [() => {}] }), err => err.code === 'schema-ahead');
});

test('a relay and a shared connection refuse options they cannot work with', () => {
  assert.throws(() => createRelay({ link: () => ({}) }), /shapeOf/);
  assert.throws(() => createRelay({ shapeOf: () => null }), /link/);
  assert.throws(() => createRelay({ shapeOf: () => null, link: () => ({}), acknowledge: 'never' }), /acknowledge/);
  assert.throws(() => sharedConnection({ transport: () => ({}) }), /name/);
  assert.throws(() => sharedConnection({ name: 'app' }), /transport/);
  assert.throws(() => sharedConnection({ name: 'app', transport: () => ({}), storage: memoryOutbox() }), /storage/);
});

test('a shared connection on document adapters keeps the browser\'s replica and its unsent edits across a reload', async () => {
  const SHAPE = { initial: { tasks: {}, settings: {} }, singletons: ['settings'] };
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const link = net.link({ user: { id: 'ann' } });
  const adapters = new Map();
  const storage = id => { if (!adapters.has(id)) adapters.set(id, memoryOutbox()); return adapters.get(id); };
  const open = () => sharedConnection({ name: 'app', transport: link.factory, storage, channel: null, locks: null, replicaId: 'browser', reconnect: false, keepalive: false, onError: () => {} });
  let connection = open();
  let db = createClient({ connection, store: 'main', ...SHAPE });
  db.connect();
  await net.settle();
  assert.equal(connection.leader, true, 'without locks or a channel, a tab is its own leader');
  db.state.tasks.a = { title: 'synced' };
  await net.settle();
  assert.equal(store.state.tasks.a.title, 'synced');

  link.goOffline();
  await net.settle();
  db.state.tasks.b = { title: 'made offline' };
  await net.settle();
  assert.equal(connection.pending('main'), 1, 'in the browser\'s outbox');
  assert.equal(db.pending, 1);
  connection.dispose();                              // the tab reloads
  db.dispose();

  link.goOnline();
  connection = open();
  db = createClient({ connection, store: 'main', ...SHAPE });
  db.connect();
  await net.settle();
  await net.settle();
  assert.equal(store.state.tasks.b?.title, 'made offline', 'sent from what the adapter kept');
  assert.equal(connection.pending('main'), 0);
  assert.deepEqual(Object.keys(db.state.tasks).sort(), ['a', 'b']);
  assert.equal(store.peers().length === 0 || store.peers().every(p => p.replicaId === 'browser'), true);
  connection.dispose();
});
