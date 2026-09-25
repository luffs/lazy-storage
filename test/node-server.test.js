// node-server.test.js - The Node adapter (ws) end to end, over real sockets
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createStore, createStores, memoryStorage } from '../src/server/index.js';
import { serve, createHandlers } from '../src/server/node.js';
import { createClient, createConnection, webSocketTransport } from '../src/index.js';
import { probeFrames, stalledSocket } from './frames.js';
import { randomBytes } from 'node:crypto';

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
    const store = createStore({ initial: INITIAL, presence: true });
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
    assert.equal(bob.status, 'online', 'the refusal did not drop the socket');

    a1.collection('tasks').add({ id: 'x', title: 'from alice' });
    await until(() => b1.state.tasks.x?.title === 'from alice', 'bob sees the task');
    a1.share({ editing: 'x' });
    await until(() => b1.peers.find(p => p.replicaId === 'a1')?.data?.editing === 'x', 'bob sees what alice shares');
    assert.equal(b1.peers.find(p => p.replicaId === 'b1').data, undefined, 'his own entry shares nothing');
    b1.state.tasks.x.done = true;
    await until(() => a1.state.tasks.x?.done === true, 'alice sees the edit');
    assert.equal(a2.state.tasks.x, undefined);
    await until(() => a1.presence.length === 2 && b1.presence.length === 2, 'presence shows both');

    assert.equal(stores.get('t1').closeSessions(s => s.user?.id === 'u2', 'You were removed'), 1);
    await until(() => b1.closed?.code === 'evicted', 'bob evicted');
    assert.equal(bob.status, 'online', 'the socket survives the eviction');
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
    assert.ok(raw.extensions.includes('permessage-deflate'), `the extension was negotiated (${raw.extensions || 'none'})`);
    raw.send('not json');
    raw.send('[1]');
    await until(() => received.length >= 2, 'errors back');
    assert.deepEqual(received, ['Expected JSON', 'Expected a message object']);
    raw.close();
    [a1, a2, b1, b2].forEach(c => c.dispose());

    // On the wire, by default: a message over the threshold goes compressed, a small one plain
    stores.get('t2').patch({ filler: 'x'.repeat(3000) });
    const probed = await probeFrames(port, '/ws?token=alice', [JSON.stringify({ t: 'hello', store: 't2', replicaId: 'probe', ops: [] }), JSON.stringify({ t: 'ping', store: 't2' })]);
    assert.match(probed.extensions, /permessage-deflate/);
    const largest = [...probed.frames].sort((x, y) => y.bytes - x.bytes)[0];
    assert.equal(largest.compressed, true, `the snapshot frame is compressed (${largest.bytes} B)`);
    assert.ok(largest.bytes < 1500, `and far smaller than the 3 KB it carries (${largest.bytes} B)`);
    const pong = probed.frames.find(f => f.text === '{"t":"pong"}');
    assert.ok(pong && !pong.compressed, 'a pong goes plain');
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
    perMessageDeflate: false,
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

    const plain = await probeFrames(port, '/sync', [JSON.stringify({ t: 'ping', store: 'main' })]);
    assert.equal(plain.extensions, '', 'perMessageDeflate: false accepts no extension');
    assert.ok(plain.frames.some(f => f.text === '{"t":"pong"}'), 'and still answers');
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

test('a large snapshot is fetched over HTTP: the route on the http server serves it with an ETag behind the same gates as the socket, and a client syncs through it', async () => {
  const stores = createStores(id => (id.startsWith('t') ? createStore({ initial: INITIAL, storage: memoryStorage() }) : null));
  stores.get('t1').patch({ filler: 'x'.repeat(3000) });
  const server = serve({
    port: 0,
    stores,
    httpSnapshots: { threshold: 2048 },
    authenticate: req => users[new URL(req.url).searchParams.get('token')] ?? null,
    authorize: (user, storeId) => user.teams.includes(storeId),
    request: (req, res) => { res.end('app'); }
  });
  const port = await listening(server);
  try {
    const url = `http://localhost:${port}/ws/snapshot/t1?token=alice`;
    const res = await fetch(url);
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('content-type'), 'application/json');
    const document = await res.json();
    assert.equal(document.v, stores.get('t1').version);
    assert.equal(document.state.filler.length, 3000);
    const etag = res.headers.get('etag');
    assert.equal((await fetch(url, { headers: { 'if-none-match': etag } })).status, 304);
    assert.equal((await fetch(`http://localhost:${port}/ws/snapshot/t1`)).status, 401, 'no token');
    assert.equal((await fetch(`http://localhost:${port}/ws/snapshot/t2?token=bob`)).status, 403, 'bob is not on t2');
    assert.equal((await fetch(`http://localhost:${port}/ws/snapshot/nope?token=alice`)).status, 404);
    assert.equal(await (await fetch(`http://localhost:${port}/other`)).text(), 'app', 'other requests still reach the app');

    const fetched = [];
    const transport = webSocketTransport(`ws://localhost:${port}/ws?token=alice`, { fetch: (target, init) => { fetched.push(String(target)); return fetch(target, init); } });
    const connection = createConnection({ transport, reconnect: { min: 20, max: 50 }, keepalive: false });
    const client = attach(connection, 't1', 'fetcher');
    await until(() => client.status === 'online', 'synced through the fetch');
    assert.deepEqual(fetched, [url], 'the route resolved against the socket URL, token and all');
    assert.equal(client.state.filler.length, 3000);
    assert.equal(client.version, stores.get('t1').version);
    stores.get('t1').patch({ tasks: { f: { id: 'f' } } });
    await until(() => client.state.tasks.f, 'a patch after the fetch arrives on the socket');
    client.dispose();
    connection.close();
  } finally {
    await server.shutdown();
  }
});

test('idleTimeout closes a socket that has gone quiet; a client that pings stays', async () => {
  const stores = createStores(() => createStore({ initial: INITIAL, storage: memoryStorage() }));
  const server = serve({ stores, port: 0, idleTimeout: 200 });
  const port = await listening(server);
  const quiet = new WebSocket(`ws://localhost:${port}/ws`);
  const closedAt = new Promise(resolve => { quiet.onclose = () => resolve(Date.now()); });
  await new Promise(resolve => { quiet.onopen = resolve; });
  const opened = Date.now();
  const chatty = createConnection({ transport: webSocketTransport(`ws://localhost:${port}/ws`), reconnect: false, keepalive: 50 });
  const a = attach(chatty, 'main', 'a');
  await until(() => a.status === 'online', 'online');
  const elapsed = (await closedAt) - opened;
  assert.ok(elapsed >= 150 && elapsed < 2000, `closed after ${elapsed} ms`);
  await sleep(300);
  assert.equal(a.status, 'online', 'pings count as being heard');
  chatty.close();
  await server.shutdown();
});

test('sockets() shows how far behind each socket is; one that stopped reading is cut off at maxBuffered, and the rest stay', async () => {
  const store = createStore({ initial: { blob: '' }, storage: memoryStorage() });
  const server = serve({ stores: () => store, port: 0, maxBuffered: 1024 * 1024, authenticate: req => ({ id: new URL(req.url).searchParams.get('token') }) });
  const port = await listening(server);
  const healthy = createConnection({ transport: webSocketTransport(`ws://localhost:${port}/ws?token=ann`), reconnect: false, keepalive: false });
  const a = createClient({ connection: healthy, store: 'main', initial: { blob: '' }, replicaId: 'a' });
  a.connect();
  await until(() => a.status === 'online', 'online');
  const stalled = await stalledSocket(port);
  await until(() => server.sockets().length === 2 && server.sockets().every(s => s.stores.includes('main')), 'both on the store');
  const ann = server.sockets().find(s => s.user?.id === 'ann');
  assert.deepEqual(ann.stores, ['main']);
  assert.equal(ann.buffered, 0);
  assert.ok(ann.idleMs >= 0 && ann.openMs >= ann.idleMs, 'times since heard and since opened');
  // Through the hub, a message to one socket is counted with its bytes too:
  // the encoding the socket sends is the one the store counts
  a.state.blob = 'mine';
  await until(() => store.stats().sent.ack?.messages === 1, 'acknowledged');
  const counted = store.stats().sent;
  assert.ok(counted.ack.bytes > 0 && counted.snapshot.bytes > 0, JSON.stringify(counted));

  // A patch at a time, as a live store sends them: a healthy socket drains
  // between them, the stalled one only piles up
  let sent = 0;
  while (server.socketStats().cutOff === 0 && sent < 400) {
    store.patch({ blob: randomBytes(96 * 1024).toString('base64') });   // random: no compression shrinks it
    sent++;
    await sleep(10);
  }
  assert.equal(server.socketStats().cutOff, 1, `cut off after ${sent} patches`);
  await until(() => server.sockets().length === 1, 'the stalled socket gone');
  assert.equal(server.sockets()[0].user.id, 'ann', 'the one that stopped reading was cut, not the healthy one');
  assert.equal(a.status, 'online');
  const last = store.state.blob;
  await until(() => a.state.blob === last, 'the healthy client got every patch');
  const stats = server.socketStats();
  assert.equal(stats.sockets, 1);
  assert.ok(stats.largest < 1024 * 1024, 'nobody left far behind');
  stalled.destroy();
  healthy.close();
  await server.shutdown();
});

test('disconnect() has a user authenticate again: back in while the session holds, turned away once it is gone; revalidate() closes the stores a user may no longer open', async () => {
  const sessions = new Map([['s-alice', { id: 'u1', teams: ['t1', 't2'] }], ['s-bob', { id: 'u2', teams: ['t1'] }]]);
  const stores = createStores(() => createStore({ initial: INITIAL, storage: memoryStorage() }));
  const server = serve({
    port: 0,
    stores,
    authenticate: req => sessions.get(new URL(req.url).searchParams.get('token')) ?? null,
    authorizeId: (user, id) => user.teams.includes(id)
  });
  const port = await listening(server);
  const alice = connect(port, 's-alice');
  const bob = connect(port, 's-bob');
  const a1 = attach(alice, 't1', 'a1');
  const a2 = attach(alice, 't2', 'a2');
  const b1 = attach(bob, 't1', 'b1');
  await until(() => [a1, a2, b1].every(c => c.status === 'online'), 'online');

  // Taken off t2: that store is closed to her, the socket stays for t1
  sessions.get('s-alice').teams = ['t1'];
  assert.equal(await server.revalidate((user, id) => id === 't1'), 0, 'the filter picks the stores judged');
  assert.equal(await server.revalidate(), 1);
  await until(() => a2.closed?.code === 'forbidden', 'alice hears t2 is closed to her');
  assert.equal(a1.status, 'online');
  assert.deepEqual(server.sockets().find(s => s.user.id === 'u1').stores, ['t1']);

  // Disconnected while her session holds: she is back at once, and a write
  // that reached the closed socket lands from her outbox
  let drops = 0;
  alice.on('status', status => { if (status === 'offline') drops++; });
  assert.equal(server.disconnect(user => user.id === 'u1'), 1);
  a1.collection('tasks').add({ id: 'x', title: 'written as the socket closed' });
  await until(() => drops === 1 && a1.status === 'online', 'back online');
  await until(() => b1.state.tasks.x?.title === 'written as the socket closed', 'bob sees it');

  // Signed out: the reconnect is turned away, and only hers
  sessions.delete('s-alice');
  server.disconnect(user => user.id === 'u1');
  await until(() => a1.closed?.code === 'unauthorized', 'alice is signed out');
  assert.equal(b1.status, 'online');
  assert.deepEqual(server.sockets().map(s => s.user.id), ['u2']);
  const { disconnected, revoked } = server.socketStats();
  assert.deepEqual({ disconnected, revoked }, { disconnected: 2, revoked: 1 });
  alice.close();
  bob.close();
  await server.shutdown();
});
