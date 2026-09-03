import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createHub } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createConnection } from '../src/client/connection.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// A hub endpoint whose sessions carry the link's user, like an authenticated transport
function hubSetup(authorize) {
  const stores = createStores(() => createStore({ initial: INITIAL, now: fakeTime() }));
  const net = createNetwork({ session: ({ send, user }) => createHub(id => stores.get(id), { send, user, authorize }) });
  const connect = user => {
    const link = net.link({ user });
    const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: false });
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

test('authorize gates each store on a hub, whether it answers synchronously or with a promise', async () => {
  const authorize = (user, storeId) => (storeId === 'team-x'
    ? user.teams.includes('x')
    : Promise.resolve(user.teams.includes(storeId.slice(5))));
  const { stores, net, connect, attach } = hubSetup(authorize);
  const shared = connect({ id: 'u1', teams: ['x', 'z'] });
  const x = attach(shared, 'team-x', 'x1');
  const y = attach(shared, 'team-y', 'y1');
  const z = attach(shared, 'team-z', 'z1');
  const yClosed = [];
  y.on('closed', info => yClosed.push(info));
  await net.settle();

  assert.equal(x.status, 'online', 'allowed synchronously');
  assert.equal(z.status, 'online', 'allowed through a promise');
  assert.equal(y.status, 'offline');
  assert.deepEqual(yClosed, [{ code: 'forbidden', message: 'Not allowed to access store "team-y"' }]);
  assert.equal(stores.get('team-y').sessions, 0, 'no session was ever opened');
  assert.equal(shared.attached, 2, 'the forbidden client detached itself');
  assert.equal(x.presence.length, 1, 'presence sees the hub session\'s user');
});

test('eviction closes the store for that user, reaches the client as final, and connect() rejoins', async () => {
  const { stores, net, connect, attach } = hubSetup();
  const c1 = connect({ id: 'u1' });
  const c2 = connect({ id: 'u2' });
  const a = attach(c1, 'team-x', 'a');
  const b = attach(c2, 'team-x', 'b');
  const bOther = attach(c2, 'team-y', 'b2');
  const closed = [];
  b.on('closed', info => closed.push(info));
  await net.settle();
  assert.equal(stores.get('team-x').sessions, 2);

  assert.equal(stores.get('team-x').closeSessions(s => s.user.id === 'u2', 'You were removed from the team'), 1);
  await net.settle();
  assert.deepEqual(closed, [{ code: 'evicted', message: 'You were removed from the team' }]);
  assert.equal(b.status, 'offline');
  assert.equal(bOther.status, 'online', 'the same socket\'s other store is untouched');
  assert.equal(c2.status, 'open');
  assert.equal(stores.get('team-x').sessions, 1);
  assert.deepEqual(a.presence, [{ id: 'u1' }], 'presence updated for the remaining user');

  b.collection('tasks').add({ id: 'late', title: 'after eviction' });
  await net.settle();
  assert.equal(b.pending, 1, 'edits queue; nothing is sent while closed');
  assert.equal(stores.get('team-x').snapshot().tasks.late, undefined);

  b.connect();  // re-admitted (no authorize configured here)
  await net.settle();
  assert.equal(b.status, 'online');
  assert.equal(b.closed, null);
  assert.equal(b.pending, 0);
  assert.equal(stores.get('team-x').state.tasks.late.title, 'after eviction');
});

test('evicting a direct (per-store) session closes its socket and the client does not reconnect on its own', async () => {
  const store = createStore({ initial: INITIAL, now: fakeTime() });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  assert.deepEqual(store.presence(), [{ id: 'u1' }, { id: 'u2' }]);

  store.closeSessions(s => s.user.id === 'u1', 'bye');
  await net.settle();
  assert.equal(a.status, 'offline');
  assert.deepEqual(a.closed, { code: 'evicted', message: 'bye' });
  assert.equal(a.connection.status, 'offline', 'the private connection was closed by the client, not left retrying');
  assert.deepEqual(b.presence, [{ id: 'u2' }]);
  assert.equal(store.sessions, 1);
});

test('presence lists distinct users, counts a user once across devices, and skips anonymous sessions', async () => {
  const store = createStore({ initial: INITIAL, now: fakeTime() });
  const net = createNetwork(store);
  const seen = [];
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1', name: 'Ann' } });
  a.on('presence', users => seen.push(users.map(u => u.id)));
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const a2 = net.client({ replicaId: 'a2', initial: INITIAL }, { user: { id: 'u1', name: 'Ann' } });
  const anon = net.client({ replicaId: 'anon', initial: INITIAL });
  await net.settle();

  assert.deepEqual(store.presence().map(u => u.id), ['u1', 'u2']);
  assert.deepEqual(a.presence.map(u => u.id), ['u1', 'u2']);
  assert.deepEqual(anon.presence.map(u => u.id), ['u1', 'u2'], 'an anonymous session still sees who is here');

  a2.disconnect();
  await net.settle();
  assert.deepEqual(b.presence.map(u => u.id), ['u1', 'u2'], 'Ann is still here on her other device');
  a.disconnect();
  await net.settle();
  assert.deepEqual(b.presence.map(u => u.id), ['u2']);
  assert.deepEqual(a.presence, [], 'a disconnected client clears its own view');
  assert.deepEqual(seen.at(-1), []);
});

test('the connection pings while open and stops when closed', async () => {
  let pings = 0;
  const net = createNetwork({
    session: ({ send }) => ({
      receive(msg) { if (msg.t === 'ping') { pings++; send({ t: 'pong' }); } },
      close() {}
    })
  });
  const link = net.link();
  const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: 10 });
  connection.connect();
  await net.settle();
  await sleep(75);
  await net.settle();
  assert.ok(pings >= 3, `expected several pings, got ${pings}`);
  connection.close();
  const after = pings;
  await sleep(50);
  await net.settle();
  assert.equal(pings, after, 'no pings after close');
});
