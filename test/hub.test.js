import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createHub, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createConnection } from '../src/client/connection.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };

// A hub endpoint looks like a store to the network harness: it hands out a
// session-shaped object per connection. Stores and clients share the real
// clock here; nothing below orders edits by time
function setup(factory = () => createStore({ initial: INITIAL, storage: memoryStorage() })) {
  const stores = createStores(factory);
  const net = createNetwork({ session: ({ send }) => createHub(id => stores.get(id), { send }) });
  const connect = () => {
    const link = net.link();
    const connection = createConnection({ transport: link.factory, reconnect: false });
    connection.link = link;
    return connection;
  };
  const attach = (connection, store, replicaId) => {
    const client = createClient({ connection, store, initial: INITIAL, replicaId });
    client.connect();
    return client;
  };
  return { stores, net, connect, attach };
}

test('two stores travel over one socket, stay isolated, and reach clients on other sockets', async () => {
  const { stores, net, connect, attach } = setup();
  const shared = connect();
  const x = attach(shared, 'team-x', 'x1');
  const y = attach(shared, 'team-y', 'y1');
  const x2 = attach(connect(), 'team-x', 'x2');
  await net.settle();
  assert.equal(shared.attached, 2);
  assert.deepEqual([x.status, y.status, x2.status], ['online', 'online', 'online']);

  const id = x.collection('tasks').add({ title: 'for team x' });
  y.collection('tasks').add({ id: 'y-task', title: 'for team y' });
  await net.settle();

  assert.deepEqual(x2.collection('tasks').get(id), { title: 'for team x', id }, 'same store, other socket');
  assert.equal(y.collection('tasks').has(id), false, 'other store, same socket');
  assert.equal(x.collection('tasks').has('y-task'), false);
  assert.deepEqual(stores.get('team-x').snapshot(), { tasks: { [id]: { title: 'for team x', id } } });
  assert.deepEqual(stores.get('team-y').snapshot(), { tasks: { 'y-task': { id: 'y-task', title: 'for team y' } } });
  assert.equal(stores.get('team-x').sessions, 2, 'one session per socket on team-x');
  assert.equal(stores.get('team-y').sessions, 1);
});

test('a dropped connection takes every attached client offline; reconnecting restores them and flushes both outboxes', async () => {
  const { stores, net, connect, attach } = setup();
  const shared = connect();
  const x = attach(shared, 'team-x', 'x1');
  const y = attach(shared, 'team-y', 'y1');
  await net.settle();

  shared.link.goOffline();
  await net.settle();
  assert.deepEqual([shared.status, x.status, y.status], ['offline', 'offline', 'offline']);
  x.collection('tasks').add({ id: 'xo', title: 'offline x' });
  y.collection('tasks').add({ id: 'yo', title: 'offline y' });
  await net.settle();
  assert.equal(x.pending, 1);
  assert.equal(y.pending, 1);

  shared.link.goOnline();
  shared.connect();
  await net.settle();
  assert.deepEqual([x.status, y.status], ['online', 'online']);
  assert.equal(x.pending + y.pending, 0);
  assert.equal(stores.get('team-x').state.tasks.xo.title, 'offline x');
  assert.equal(stores.get('team-y').state.tasks.yo.title, 'offline y');
});

test('disconnecting one client leaves the socket up for the others, and it can rejoin', async () => {
  const { stores, net, connect, attach } = setup();
  const shared = connect();
  const x = attach(shared, 'team-x', 'x1');
  const y = attach(shared, 'team-y', 'y1');
  await net.settle();

  y.disconnect();
  await net.settle();
  assert.equal(y.status, 'offline');
  assert.equal(x.status, 'online', 'the other store is unaffected');
  assert.equal(shared.status, 'open');
  assert.equal(shared.attached, 1);
  assert.equal(stores.get('team-y').sessions, 0, 'leave closed the hub session for team-y');

  y.collection('tasks').add({ id: 'later', title: 'while detached' });
  await net.settle();
  assert.equal(y.pending, 1);
  assert.equal(stores.get('team-y').snapshot().tasks.later, undefined);

  y.connect();
  await net.settle();
  assert.equal(y.status, 'online');
  assert.equal(y.pending, 0);
  assert.equal(stores.get('team-y').state.tasks.later.title, 'while detached');
});

test('an unknown store is closed for that client only; the other stays online', async () => {
  const { net, connect, attach } = setup(id => (id.startsWith('team-') ? createStore({ initial: INITIAL }) : null));
  const shared = connect();
  const ok = attach(shared, 'team-x', 'x1');
  const nope = attach(shared, 'other', 'o1');
  const closed = [];
  nope.on('closed', info => closed.push(info));
  await net.settle();
  assert.equal(ok.status, 'online');
  assert.equal(nope.status, 'offline');
  assert.deepEqual(closed, [{ code: 'unknown-store', message: 'Unknown store "other"' }]);
  assert.deepEqual(nope.closed, closed[0]);
  assert.equal(shared.attached, 1, 'the refused client detached; the socket stays for the other');
  assert.equal(shared.status, 'open');
});

test('a store factory that throws refuses that store only, without taking the connection down', async () => {
  const { net, connect, attach } = setup(id => {
    if (id === 'broken') throw new Error('migration failed: arrays are not allowed');
    return createStore({ initial: INITIAL });
  });
  const shared = connect();
  const errors = [];
  const originalError = console.error;
  console.error = (...args) => errors.push(args.join(' '));
  try {
    const ok = attach(shared, 'team-x', 'x1');
    const broken = attach(shared, 'broken', 'b1');
    const closed = [];
    broken.on('closed', info => closed.push(info));
    await net.settle();
    assert.equal(ok.status, 'online', 'the healthy store on the same socket is unaffected');
    assert.equal(broken.status, 'offline');
    assert.deepEqual(closed, [{ code: 'unknown-store', message: 'Store "broken" could not be opened: migration failed: arrays are not allowed' }]);
    assert.equal(shared.status, 'open');
    assert.ok(errors.some(e => e.includes('migration failed')), 'the server fault is logged (the default onError)');
  } finally {
    console.error = originalError;
  }
});

test('one client per store per connection', () => {
  const { connect } = setup();
  const shared = connect();
  createClient({ connection: shared, store: 'team-x', initial: INITIAL }).connect();
  assert.throws(() => createClient({ connection: shared, store: 'team-x', initial: INITIAL }).connect(), /already attached/);
  assert.throws(() => createClient({ connection: shared, initial: INITIAL }), /requires a store id/);
  assert.throws(() => createClient({ store: 'team-x', initial: INITIAL }), /connection or a transport/);
});

test('a hub refuses messages without a valid store id and answers pings', () => {
  const sent = [];
  const hub = createHub(() => null, { send: m => sent.push(m) });
  hub.receive({ t: 'ping' });
  hub.receive({ t: 'hello', replicaId: 'r' });
  hub.receive({ t: 'hello', store: 'bad id', replicaId: 'r' });
  assert.deepEqual(sent.map(m => [m.t, m.code]), [['pong', undefined], ['closed', 'invalid-store'], ['closed', 'invalid-store']]);
  assert.deepEqual(hub.stores, []);
});
