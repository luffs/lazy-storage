// serve.test.js - Runs under Bun only: the WebSocket adapter end to end, over real sockets
import assert from 'node:assert/strict';
import { createStore, createStores, memoryStorage } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';
import { createClient, createConnection, webSocketTransport } from '../../src/index.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = async (pred, label) => {
  for (let i = 0; i < 500; i++) {
    if (pred()) return;
    await sleep(10);
  }
  throw new Error(`timeout: ${label}`);
};
let passed = 0;
const test = async (name, fn) => { await fn(); passed++; console.log('✔', name); };

const users = {
  alice: { id: 'u1', name: 'Alice', teams: ['t1', 't2'] },
  bob: { id: 'u2', name: 'Bob', teams: ['t1'] }
};
const answers = [];   // what stores answered hellos with, in order
const stores = createStores(id => {
  if (!id.startsWith('t')) return null;
  const store = createStore({ initial: INITIAL, presence: true });
  const session = store.session;
  store.session = opts => session({ ...opts, send: m => { if (m.t === 'snapshot' || m.t === 'delta') answers.push(m.t); opts.send(m); } });
  return store;
});
const server = serve({
  port: 0,
  stores,
  perMessageDeflate: true,   // offered; the whole suite runs with it
  authenticate: req => users[new URL(req.url).searchParams.get('token')] ?? null,
  authorize: (user, storeId) => user.teams.includes(storeId),
  fetch: req => (new URL(req.url).pathname === '/health' ? new Response('ok') : null)
});
const http = `http://localhost:${server.port}`;
const ws = `ws://localhost:${server.port}/ws`;
const connect = token => createConnection({ transport: webSocketTransport(`${ws}?token=${token}`), reconnect: { min: 20, max: 50 }, keepalive: false });
const attach = (connection, store, replicaId) => {
  const client = createClient({ connection, store, initial: INITIAL, replicaId });
  client.connect();
  return client;
};
const open = [];

try {
  await test('other requests fall through to the app, and a bad token is turned away: 401 for a plain request, a closed message and code 4401 for a socket', async () => {
    assert.equal(await (await fetch(`${http}/health`)).text(), 'ok');
    assert.equal((await fetch(`${http}/nothing`)).status, 404);
    assert.equal((await fetch(`${http}/ws?token=nope`)).status, 401);
    assert.equal((await fetch(`${http}/ws?token=alice`)).status, 400, 'a plain GET on the socket path is not an upgrade');

    const raw = new WebSocket(`${ws}?token=nope`);
    const heard = await new Promise(resolve => {
      let message = null;
      raw.onmessage = e => { message = JSON.parse(e.data); };
      raw.onclose = e => resolve({ message, code: e.code, reason: e.reason });
    });
    assert.deepEqual(heard, { message: { t: 'closed', code: 'unauthorized', message: 'Unauthorized' }, code: 4401, reason: 'Unauthorized' });

    // A client with a bad token learns it, stops retrying, and gets back in once the URL carries a good one
    let token = 'nope';
    let attempts = 0;
    const factory = webSocketTransport(() => `${ws}?token=${token}`);
    const nobody = createConnection({ transport: () => { attempts++; return factory(); }, reconnect: { min: 20, max: 50 }, keepalive: false });
    open.push(nobody);
    const n1 = attach(nobody, 't1', 'n1');
    await until(() => n1.closed?.code === 'unauthorized', 'the client hears it is not signed in');
    assert.equal(nobody.closed.code, 'unauthorized');
    assert.equal(n1.status, 'offline');
    await new Promise(r => setTimeout(r, 150));
    assert.equal(attempts, 1, 'no retry with the same token');
    token = 'alice';
    n1.connect();
    await until(() => n1.status === 'online', 'online with a fresh token');
    assert.equal(n1.closed, null);
    assert.equal(nobody.closed, null);
    n1.dispose();
  });

  await test('two sockets share a store: snapshot, live patches both ways, presence, and a forbidden store closed for one side only', async () => {
    const alice = connect('alice');
    const bob = connect('bob');
    open.push(alice, bob);
    const a1 = attach(alice, 't1', 'a1');
    const a2 = attach(alice, 't2', 'a2');
    const b1 = attach(bob, 't1', 'b1');
    const b2 = attach(bob, 't2', 'b2');
    await until(() => a1.status === 'online' && a2.status === 'online' && b1.status === 'online', 'three stores online');
    await until(() => b2.closed?.code === 'forbidden', 'bob may not open t2');
    assert.equal(alice.status, 'open');
    assert.equal(bob.status, 'open', 'the refusal did not drop the socket');

    a1.collection('tasks').add({ id: 'x', title: 'from alice' });
    await until(() => b1.state.tasks.x?.title === 'from alice', 'bob sees the task');
    a1.share({ editing: 'x' });
    await until(() => b1.peers.find(p => p.replicaId === 'a1')?.data?.editing === 'x', 'bob sees what alice shares');
    assert.equal(b1.peers.find(p => p.replicaId === 'b1').data, undefined, 'his own entry shares nothing');
    assert.equal(b1.peers.find(p => p.replicaId === 'a1').user.name, 'Alice');
    b1.state.tasks.x.done = true;
    await until(() => a1.state.tasks.x?.done === true, 'alice sees the edit');
    assert.equal(a2.state.tasks.x, undefined, 'the other store on the same socket is untouched');
    assert.deepEqual(stores.get('t1').snapshot(), { tasks: { x: { id: 'x', title: 'from alice', done: true } } });
    await until(() => a1.presence.length === 2 && b1.presence.length === 2, 'presence shows both');
    assert.deepEqual(a1.presence.map(u => u.name).sort(), ['Alice', 'Bob']);
    assert.equal(a1.pending + b1.pending, 0);

    b2.dispose();
    [a1, a2, b1].forEach(c => c.dispose());
  });

  await test('eviction closes one session through the hub; the socket and its other stores stay up', async () => {
    const alice = connect('alice');
    const bob = connect('bob');
    open.push(alice, bob);
    const a1 = attach(alice, 't1', 'a1-b');
    const b1 = attach(bob, 't1', 'b1-b');
    await until(() => a1.status === 'online' && b1.status === 'online', 'both online');
    assert.equal(stores.get('t1').closeSessions(s => s.user?.id === 'u2', 'You were removed'), 1);
    await until(() => b1.closed?.code === 'evicted', 'bob evicted');
    assert.equal(b1.closed.message, 'You were removed');
    assert.equal(b1.status, 'offline');
    assert.equal(bob.status, 'open', 'the socket survives the eviction');
    await until(() => a1.presence.length === 1, 'presence drops bob');
    assert.equal(a1.status, 'online');
    a1.dispose();
    b1.dispose();
  });

  await test('a reconnect after a dropped socket is answered with a delta, and malformed input gets an error, not a crash', async () => {
    const alice = connect('alice');
    open.push(alice);
    const a1 = attach(alice, 't1', 'a1-c');
    await until(() => a1.status === 'online', 'online');
    const before = answers.length;
    alice.close();
    await until(() => a1.status === 'offline', 'offline after close');
    stores.get('t1').patch({ tasks: { y: { id: 'y', title: 'while away' } } });
    alice.connect();
    await until(() => a1.status === 'online', 'back online');
    assert.equal(answers[before], 'delta', 'the reconnect got a delta');
    assert.equal(a1.state.tasks.y.title, 'while away');

    const raw = new WebSocket(`${ws}?token=alice`);
    const received = [];
    raw.onmessage = e => { const m = JSON.parse(e.data); if (m.t === 'error') received.push(m); };
    await until(() => raw.readyState === 1, 'raw socket open');
    raw.send('not json');
    raw.send('[1, 2]');
    raw.send(JSON.stringify({ t: 'hello', store: 't1' }));
    await until(() => received.length >= 3, 'three errors');
    assert.deepEqual(received.map(m => m.message), ['Expected JSON', 'Expected a message object', 'hello requires a replicaId']);
    raw.close();
    a1.state.tasks.y.done = true;
    await until(() => stores.get('t1').snapshot().tasks.y.done === true, 'the server is still serving');
    a1.dispose();
  });

  await test('a message over maxPayload closes that socket; a store factory that throws reaches onError; the server serves on', async () => {
    const faults = [];
    const small = serve({
      port: 0,
      stores: id => { if (id === 'boom') throw new Error('factory boom'); return stores.get(id); },
      maxPayload: 512,
      onError: err => faults.push(err.message)
    });
    try {
      const raw = new WebSocket(`ws://localhost:${small.port}/ws`);
      let closeCode = null;
      raw.onclose = e => { closeCode = e.code; };
      await until(() => raw.readyState === 1, 'raw open');
      raw.send(JSON.stringify({ t: 'hello', store: 't1', replicaId: 'r', ops: [], pad: 'x'.repeat(600) }));
      await until(() => closeCode !== null, 'socket closed by the server');
      assert.ok(closeCode === 1006 || closeCode === 1009, `the server ended the socket (code ${closeCode})`);

      const connection = createConnection({ transport: webSocketTransport(`ws://localhost:${small.port}/ws`), reconnect: false, keepalive: false });
      const boom = createClient({ connection, store: 'boom', initial: INITIAL, replicaId: 'boom-1' });
      const fine = createClient({ connection, store: 't1', initial: INITIAL, replicaId: 'fine-1' });
      boom.connect();
      fine.connect();
      await until(() => boom.closed?.code === 'unknown-store', 'the broken store is refused');
      await until(() => fine.status === 'online', 'the healthy one is served');
      assert.deepEqual(faults, ['factory boom']);
      boom.dispose();
      fine.dispose();
      connection.close();
    } finally {
      small.stop(true);
    }
  });

  await test('shutdown refuses new sockets, closes the open ones so clients reconnect, flushes the stores, and the next process answers with a delta', async () => {
    const storage = memoryStorage();
    const registry = () => createStores(() => createStore({ initial: INITIAL, storage }));
    let stores2 = registry();
    const first = serve({ port: 0, stores: stores2 });
    const port = first.port;
    const url = `ws://localhost:${port}/ws`;
    const connection = createConnection({ transport: webSocketTransport(url), reconnect: { min: 20, max: 50 }, keepalive: false });
    const client = createClient({ connection, store: 'team', initial: INITIAL, replicaId: 'shut-1' });
    client.connect();
    await until(() => client.status === 'online', 'online on the first process');
    client.collection('tasks').add({ id: 'before' });
    await until(() => stores2.get('team').snapshot().tasks.before && client.pending === 0, 'applied and acked');

    const closed = first.shutdown({ reason: 'deploying' });
    await until(() => client.status === 'offline', 'the socket was closed');
    await closed;
    assert.equal((await fetch(`http://localhost:${port}/ws`).catch(() => ({ status: 'refused' }))).status, 'refused', 'the first process is gone');
    assert.deepEqual(stores2.ids(), [], 'the registry was disposed, flushing the store');
    assert.equal(storage.load().rows.length, 1, 'the row is in storage');

    const stores3 = registry();
    stores3.get('team').patch({ tasks: { meanwhile: { id: 'meanwhile' } } });
    const second = serve({ port, stores: stores3 });
    try {
      await until(() => client.status === 'online', 'reconnected to the second process on its own');
      assert.deepEqual(Object.keys(client.state.tasks).sort(), ['before', 'meanwhile']);
      assert.equal(client.version, stores3.get('team').version);
      client.collection('tasks').add({ id: 'after' });
      await until(() => stores3.get('team').snapshot().tasks.after, 'edits flow again');
    } finally {
      client.dispose();
      connection.close();
      second.stop(true);
      stores3.dispose();
    }
  });

  console.log(`\n${passed} passed`);
} finally {
  for (const connection of open) connection.close();
  server.stop(true);
  stores.dispose();
}
