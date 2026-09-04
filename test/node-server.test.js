// node-server.test.js - The Node adapter (ws) end to end, over real sockets
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createStore, createStores, memoryStorage } from '../src/server/index.js';
import { serve, createHandlers } from '../src/server/node.js';
import { createClient, createConnection, webSocketTransport } from '../src/index.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = async (pred, label) => {
  for (let i = 0; i < 500; i++) {
    if (pred()) return;
    await sleep(10);
  }
  throw new Error(`timeout: ${label}`);
};
// A refused handshake: Node 22's WebSocket (undici 6) fires only 'error' for
// it, later versions 'close' too. Resolves to the ready state, never 1 (open)
const refused = ws => new Promise(resolve => {
  ws.onerror = () => resolve(ws.readyState);
  ws.onclose = () => resolve(ws.readyState);
});
/** What a raw socket the server turned away hears: the message, then the close code and reason */
const turnedAway = ws => new Promise(resolve => {
  let message = null;
  ws.onmessage = e => { message = JSON.parse(e.data); };
  ws.onclose = e => resolve({ message, code: e.code, reason: e.reason });
});
const users = {
  alice: { id: 'u1', name: 'Alice', teams: ['t1', 't2'] },
  bob: { id: 'u2', name: 'Bob', teams: ['t1'] }
};

async function listening(server) {
  await once(server, 'listening');
  return server.address().port;
}

function connect(port, token, reconnect = { min: 20, max: 50 }) {
  return createConnection({ transport: webSocketTransport(`ws://localhost:${port}/ws?token=${token}`), reconnect, keepalive: false });
}
function attach(connection, store, replicaId) {
  const client = createClient({ connection, store, initial: INITIAL, replicaId });
  client.connect();
  return client;
}

test('serve: other requests reach the app, a bad token is refused, two sockets sync, a forbidden store is closed, eviction works', async () => {
  const answers = [];
  const stores = createStores(id => {
    if (!id.startsWith('t')) return null;
    const store = createStore({ initial: INITIAL });
    const session = store.session;
    store.session = opts => session({ ...opts, send: m => { if (m.t === 'snapshot' || m.t === 'delta') answers.push(m.t); opts.send(m); } });
    return store;
  });
  const server = serve({
    port: 0,
    stores,
    authenticate: req => users[new URL(req.url).searchParams.get('token')] ?? null,
    authorize: (user, storeId) => user.teams.includes(storeId),
    request: (req, res) => { res.end(req.url === '/health' ? 'ok' : 'other'); }
  });
  const port = await listening(server);
  const open = [];
  try {
    assert.equal(await (await fetch(`http://localhost:${port}/health`)).text(), 'ok');
    assert.equal(await (await fetch(`http://localhost:${port}/ws?token=nope`)).text(), 'other', 'a plain GET is not an upgrade; the app answers');
    assert.deepEqual(await turnedAway(new WebSocket(`ws://localhost:${port}/ws?token=nope`)), {
      message: { t: 'closed', code: 'unauthorized', message: 'Unauthorized' }, code: 4401, reason: 'Unauthorized'
    }, 'a socket with a bad token is told why, then closed');

    // A client with a bad token learns it, stops retrying, and gets back in once the URL carries a good one
    let token = 'nope';
    let attempts = 0;
    const factory = webSocketTransport(() => `ws://localhost:${port}/ws?token=${token}`);
    const nobody = createConnection({ transport: () => { attempts++; return factory(); }, reconnect: { min: 20, max: 50 }, keepalive: false });
    open.push(nobody);
    const n1 = attach(nobody, 't1', 'n1');
    await until(() => n1.closed?.code === 'unauthorized', 'the client hears it is not signed in');
    assert.equal(nobody.closed.code, 'unauthorized');
    assert.equal(n1.status, 'offline');
    await sleep(150);
    assert.equal(attempts, 1, 'no retry with the same token');
    token = 'alice';
    n1.connect();
    await until(() => n1.status === 'online', 'online with a fresh token');
    assert.equal(n1.closed, null);
    assert.equal(nobody.closed, null);
    n1.dispose();

    const alice = connect(port, 'alice');
    const bob = connect(port, 'bob');
    open.push(alice, bob);
    const a1 = attach(alice, 't1', 'a1');
    const a2 = attach(alice, 't2', 'a2');
    const b1 = attach(bob, 't1', 'b1');
    const b2 = attach(bob, 't2', 'b2');
    await until(() => a1.status === 'online' && a2.status === 'online' && b1.status === 'online', 'three stores online');
    await until(() => b2.closed?.code === 'forbidden', 'bob may not open t2');
    assert.equal(bob.status, 'open', 'the refusal did not drop the socket');

    a1.collection('tasks').add({ id: 'x', title: 'from alice' });
    await until(() => b1.state.tasks.x?.title === 'from alice', 'bob sees the task');
    b1.state.tasks.x.done = true;
    await until(() => a1.state.tasks.x?.done === true, 'alice sees the edit');
    assert.equal(a2.state.tasks.x, undefined);
    await until(() => a1.presence.length === 2 && b1.presence.length === 2, 'presence shows both');

    assert.equal(stores.get('t1').closeSessions(s => s.user?.id === 'u2', 'You were removed'), 1);
    await until(() => b1.closed?.code === 'evicted', 'bob evicted');
    assert.equal(bob.status, 'open', 'the socket survives the eviction');
    await until(() => a1.presence.length === 1, 'presence drops bob');

    alice.close();
    await until(() => a1.status === 'offline', 'offline after close');
    stores.get('t1').patch({ tasks: { y: { id: 'y', title: 'while away' } } });
    const before = answers.length;
    alice.connect();
    await until(() => a1.status === 'online', 'back online');
    assert.equal(answers[before], 'delta', 'the reconnect got a delta');
    assert.equal(a1.state.tasks.y.title, 'while away');

    const raw = new WebSocket(`ws://localhost:${port}/ws?token=alice`);
    const received = [];
    raw.onmessage = e => { const m = JSON.parse(e.data); if (m.t === 'error') received.push(m.message); };
    await until(() => raw.readyState === 1, 'raw open');
    raw.send('not json');
    raw.send('[1]');
    await until(() => received.length >= 2, 'errors back');
    assert.deepEqual(received, ['Expected JSON', 'Expected a message object']);
    raw.close();
    [a1, a2, b1, b2].forEach(c => c.dispose());
  } finally {
    for (const c of open) c.close();
    await new Promise(resolve => server.close(resolve));
    stores.dispose();
  }
});

test('createHandlers mounts on an existing http server; maxPayload closes a socket; a throwing factory reaches onError', async () => {
  const { createServer } = await import('node:http');
  const faults = [];
  const store = createStore({ initial: INITIAL });
  const lazy = createHandlers({
    stores: id => { if (id === 'boom') throw new Error('factory boom'); return store; },
    path: '/sync',
    maxPayload: 512,
    onError: err => faults.push(err.message)
  });
  const server = createServer((req, res) => res.end('app'));
  server.on('upgrade', (req, socket, head) => { lazy.upgrade(req, socket, head).then(ours => { if (!ours) socket.destroy(); }); });
  server.listen(0);
  const port = await listening(server);
  try {
    const raw = new WebSocket(`ws://localhost:${port}/sync`);
    const closeCode = await new Promise(resolve => {
      raw.onclose = e => resolve(e.code);
      raw.onopen = () => raw.send(JSON.stringify({ t: 'hello', store: 'main', replicaId: 'r', ops: [], pad: 'x'.repeat(600) }));
    });
    assert.equal(closeCode, 1009, 'message too big');

    const connection = createConnection({ transport: webSocketTransport(`ws://localhost:${port}/sync`), reconnect: false, keepalive: false });
    const boom = createClient({ connection, store: 'boom', initial: INITIAL, replicaId: 'boom-1' });
    const fine = createClient({ connection, store: 'main', initial: INITIAL, replicaId: 'fine-1' });
    boom.connect();
    fine.connect();
    await until(() => boom.closed?.code === 'unknown-store', 'the broken store is refused');
    await until(() => fine.status === 'online', 'the healthy one is served');
    assert.deepEqual(faults, ['factory boom']);

    const elsewhere = new WebSocket(`ws://localhost:${port}/other`);
    assert.notEqual(await refused(elsewhere), 1, 'a socket on another path is not ours');
    boom.dispose();
    fine.dispose();
    connection.close();
  } finally {
    await lazy.close();
    server.closeAllConnections?.();
    await new Promise(resolve => server.close(resolve));
    store.dispose();
  }
});

test('shutdown refuses new sockets, closes the open ones, flushes the stores, and the next process answers with a delta', async () => {
  const storage = memoryStorage();
  const registry = () => createStores(() => createStore({ initial: INITIAL, storage }));
  const stores1 = registry();
  const first = serve({ port: 0, stores: stores1 });
  const port = await listening(first);
  const connection = createConnection({ transport: webSocketTransport(`ws://localhost:${port}/ws`), reconnect: { min: 20, max: 50 }, keepalive: false });
  const client = createClient({ connection, store: 'team', initial: INITIAL, replicaId: 'shut-1' });
  client.connect();
  await until(() => client.status === 'online', 'online on the first process');
  client.collection('tasks').add({ id: 'before' });
  await until(() => stores1.get('team').snapshot().tasks.before && client.pending === 0, 'applied and acked');

  const closed = first.shutdown({ reason: 'deploying' });
  await until(() => client.status === 'offline', 'the socket was closed');
  await closed;
  assert.deepEqual(stores1.ids(), [], 'the registry was disposed');
  assert.equal(storage.load().rows.length, 1);

  const stores2 = registry();
  stores2.get('team').patch({ tasks: { meanwhile: { id: 'meanwhile' } } });
  const second = serve({ port, stores: stores2 });
  await listening(second);
  try {
    await until(() => client.status === 'online', 'reconnected to the second process on its own');
    assert.deepEqual(Object.keys(client.state.tasks).sort(), ['before', 'meanwhile']);
    assert.equal(client.version, stores2.get('team').version);
  } finally {
    client.dispose();
    connection.close();
    await second.shutdown();
  }
});
