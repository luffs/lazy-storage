// serve.test.js - Runs under Bun only: the WebSocket adapter end to end, over real sockets
import assert from 'node:assert/strict';
import { test, afterAll } from 'bun:test';
import { brotliDecompressSync } from 'node:zlib';
import { createStore, createStores, memoryStorage } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';
import { createClient, createConnection, webSocketTransport } from '../../src/index.js';
import { probeFrames, stalledSocket } from '../frames.js';
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
  httpSnapshots: { threshold: 2048 },   // a snapshot over 2 KB is fetched by a client that can
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

test('sockets() shows how far behind each socket is; one that stopped reading is cut off at maxBuffered, broadcasts included, and the rest stay', async () => {
  const store = createStore({ initial: { blob: '' }, storage: memoryStorage() });
  const lagServer = serve({ port: 0, stores: () => store, maxBuffered: 1024 * 1024, authenticate: req => ({ id: new URL(req.url).searchParams.get('token') }) });
  const healthy = createConnection({ transport: webSocketTransport(`ws://localhost:${lagServer.port}/ws?token=ann`), reconnect: false, keepalive: false });
  const a = createClient({ connection: healthy, store: 'main', initial: { blob: '' }, replicaId: 'a' });
  a.connect();
  await until(() => a.status === 'online', 'online');
  const stalled = await stalledSocket(lagServer.port);
  await until(() => lagServer.sockets().length === 2 && lagServer.sockets().every(s => s.stores.includes('main')), 'both on the store');
  const ann = lagServer.sockets().find(s => s.user?.id === 'ann');
  assert.deepEqual(ann.stores, ['main']);
  assert.ok(ann.idleMs >= 0 && ann.openMs >= ann.idleMs);

  // Patches reach the sockets as one topic publish, past the adapter's own
  // send: the sweep is what finds the stalled socket
  let sent = 0;
  while (lagServer.socketStats().cutOff === 0 && sent < 400) {
    store.patch({ blob: randomBytes(96 * 1024).toString('base64') });
    sent++;
    await sleep(10);
  }
  assert.equal(lagServer.socketStats().cutOff, 1, `cut off after ${sent} patches`);
  await until(() => lagServer.sockets().length === 1, 'the stalled socket gone');
  assert.equal(lagServer.sockets()[0].user.id, 'ann', 'the one that stopped reading was cut, not the healthy one');
  const last = store.state.blob;
  await until(() => a.state.blob === last, 'the healthy client got every patch');
  assert.equal(a.status, 'online');
  stalled.destroy();
  healthy.close();
  await lagServer.shutdown();
});

test('other requests fall through to the app, and a bad token is turned away: 401 for a plain request, a closed message and code 4401 for a socket', async () => {
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

test('two sockets share a store: snapshot, live patches both ways, presence, and a forbidden store closed for one side only', async () => {
  const alice = connect('alice');
  const bob = connect('bob');
  open.push(alice, bob);
  const a1 = attach(alice, 't1', 'a1');
  const a2 = attach(alice, 't2', 'a2');
  const b1 = attach(bob, 't1', 'b1');
  const b2 = attach(bob, 't2', 'b2');
  await until(() => a1.status === 'online' && a2.status === 'online' && b1.status === 'online', 'three stores online');
  await until(() => b2.closed?.code === 'forbidden', 'bob may not open t2');
  assert.equal(alice.status, 'online');
  assert.equal(bob.status, 'online', 'the refusal did not drop the socket');

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

  // On the wire: a message over the threshold goes compressed, a small one plain
  stores.get('t2').patch({ filler: 'x'.repeat(3000) });
  const probed = await probeFrames(server.port, '/ws?token=alice', [JSON.stringify({ t: 'hello', store: 't2', replicaId: 'probe', ops: [] }), JSON.stringify({ t: 'ping', store: 't2' })]);
  assert.match(probed.extensions, /permessage-deflate/);
  const largest = [...probed.frames].sort((x, y) => y.bytes - x.bytes)[0];
  assert.equal(largest.compressed, true, `the snapshot frame is compressed (${largest.bytes} B)`);
  assert.ok(largest.bytes < 1500, `and far smaller than the 3 KB it carries (${largest.bytes} B)`);
  const pong = probed.frames.find(f => f.text === '{"t":"pong"}');
  assert.ok(pong && !pong.compressed, 'a pong goes plain');
});

test('a broadcast goes out as one topic publish: a large patch arrives compressed, a small one plain, and a socket that left the store stops getting them', async () => {
  // A raw socket subscribes to t1 by saying hello, then watches the frames a server-side patch produces
  const big = await probeFrames(server.port, '/ws?token=alice', [
    JSON.stringify({ t: 'hello', store: 't1', replicaId: 'watch', ops: [] })
  ], { settle: 400, after: () => stores.get('t1').patch({ blob: 'y'.repeat(4000) }) });
  assert.equal(big.caused.filter(f => f.compressed).length, 1, 'the 4 KB patch was published compressed');
  assert.ok(!big.caused.some(f => f.text.includes('blob')), 'and not also plain');

  const small = await probeFrames(server.port, '/ws?token=alice', [
    JSON.stringify({ t: 'hello', store: 't1', replicaId: 'watch2', ops: [] })
  ], { settle: 400, after: () => stores.get('t1').patch({ tasks: { z: { id: 'z' } } }) });
  const smallPatch = small.caused.find(f => f.text.includes('"z"'));
  assert.ok(smallPatch && !smallPatch.compressed, 'the small patch went plain');

  // After leave, the socket is unsubscribed and a later patch does not reach it
  const left = await probeFrames(server.port, '/ws?token=alice', [
    JSON.stringify({ t: 'hello', store: 't1', replicaId: 'watch3', ops: [] }),
    JSON.stringify({ t: 'leave', store: 't1' })
  ], { settle: 400, after: () => stores.get('t1').patch({ tasks: { after: { id: 'after' } } }) });
  assert.ok(!left.caused.some(f => f.compressed || f.text.includes('"after"')), 'no patch after leaving the store');
});

test('eviction closes one session through the hub; the socket and its other stores stay up', async () => {
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
  assert.equal(bob.status, 'online', 'the socket survives the eviction');
  await until(() => a1.presence.length === 1, 'presence drops bob');
  assert.equal(a1.status, 'online');
  a1.dispose();
  b1.dispose();
});

test('a reconnect after a dropped socket is answered with a delta, and malformed input gets an error, not a crash', async () => {
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

test('a message over maxPayload closes that socket; a store factory that throws reaches onError; the server serves on', async () => {
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

test('a large snapshot is fetched over HTTP: the route serves it gzipped with an ETag behind the same gates as the socket, and a client syncs through it', async () => {
  // t2 holds the 3 KB filler from above: over the threshold
  const url = `${http}/ws/snapshot/t2?token=alice`;
  const res = await fetch(url, { headers: { 'accept-encoding': 'gzip' }, decompress: false });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('content-encoding'), 'gzip');
  assert.equal(res.headers.get('content-type'), 'application/json');
  const bytes = new Uint8Array(await res.arrayBuffer());
  assert.deepEqual([bytes[0], bytes[1]], [0x1f, 0x8b], 'a gzip body');
  assert.ok(bytes.length < 500, `3 KB of filler gzipped to ${bytes.length} bytes`);
  const document = JSON.parse(Buffer.from(Bun.gunzipSync(bytes)).toString());
  assert.equal(document.v, stores.get('t2').version);
  assert.equal(document.epoch, stores.get('t2').epoch);
  assert.equal(document.state.filler.length, 3000);
  const brotli = await fetch(url, { headers: { 'accept-encoding': 'gzip, br' }, decompress: false });
  assert.equal(brotli.headers.get('content-encoding'), 'br', 'brotli when the client takes it');
  assert.deepEqual(JSON.parse(Buffer.from(brotliDecompressSync(new Uint8Array(await brotli.arrayBuffer()))).toString()), document);
  const etag = res.headers.get('etag');
  assert.equal((await fetch(url, { headers: { 'if-none-match': etag } })).status, 304);
  assert.equal((await fetch(`${http}/ws/snapshot/t2`)).status, 401, 'no token');
  assert.equal((await fetch(`${http}/ws/snapshot/t2?token=bob`)).status, 403, 'bob is not on t2');
  assert.equal((await fetch(`${http}/ws/snapshot/nope?token=alice`)).status, 404);
  assert.equal((await fetch(url, { method: 'POST' })).status, 405);

  // A client on t2 takes the snapshot by the route, through the transport's fetch; the socket carries the rest
  const fetched = [];
  const transport = webSocketTransport(`${ws}?token=alice`, { fetch: (target, init) => { fetched.push(String(target)); return fetch(target, init); } });
  const connection = createConnection({ transport, reconnect: { min: 20, max: 50 }, keepalive: false });
  const client = attach(connection, 't2', 'fetcher');
  await until(() => client.status === 'online', 'synced through the fetch');
  assert.deepEqual(fetched, [url], 'the route resolved against the socket URL, token and all');
  assert.equal(client.state.filler.length, 3000);
  assert.equal(client.version, stores.get('t2').version);
  stores.get('t2').patch({ tasks: { f: { id: 'f' } } });
  await until(() => client.state.tasks.f, 'a patch after the fetch arrives on the socket');
  client.collection('tasks').add({ id: 'g' });
  await until(() => stores.get('t2').state.tasks.g, 'and an edit goes up it');
  client.dispose();
  connection.close();
});

test('shutdown refuses new sockets, closes the open ones so clients reconnect, flushes the stores, and the next process answers with a delta', async () => {
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

test('disconnect() has a user authenticate again: back in while the session holds, turned away once it is gone; revalidate() closes the stores a user may no longer open', async () => {
  const sessions = new Map([['s-alice', { id: 'u1', teams: ['t1', 't2'] }], ['s-bob', { id: 'u2', teams: ['t1'] }]]);
  const own = createStores(() => createStore({ initial: INITIAL, storage: memoryStorage() }));
  const authServer = serve({
    port: 0,
    stores: own,
    authenticate: req => sessions.get(new URL(req.url).searchParams.get('token')) ?? null,
    authorizeId: (user, id) => user.teams.includes(id)
  });
  const at = token => createConnection({ transport: webSocketTransport(`ws://localhost:${authServer.port}/ws?token=${token}`), reconnect: { min: 20, max: 50 }, keepalive: false });
  const alice = at('s-alice');
  const bob = at('s-bob');
  const a1 = attach(alice, 't1', 'a1');
  const a2 = attach(alice, 't2', 'a2');
  const b1 = attach(bob, 't1', 'b1');
  await until(() => [a1, a2, b1].every(c => c.status === 'online'), 'online');

  sessions.get('s-alice').teams = ['t1'];
  assert.equal(await authServer.revalidate((user, id) => id === 't1'), 0, 'the filter picks the stores judged');
  assert.equal(await authServer.revalidate(), 1);
  await until(() => a2.closed?.code === 'forbidden', 'alice hears t2 is closed to her');
  assert.equal(a1.status, 'online');
  assert.deepEqual(authServer.sockets().find(s => s.user.id === 'u1').stores, ['t1']);

  let drops = 0;
  alice.on('status', status => { if (status === 'offline') drops++; });
  assert.equal(authServer.disconnect(user => user.id === 'u1'), 1);
  a1.collection('tasks').add({ id: 'x', title: 'written as the socket closed' });
  await until(() => drops === 1 && a1.status === 'online', 'back online');
  await until(() => b1.state.tasks.x?.title === 'written as the socket closed', 'bob sees it');

  sessions.delete('s-alice');
  authServer.disconnect(user => user.id === 'u1');
  await until(() => a1.closed?.code === 'unauthorized', 'alice is signed out');
  assert.equal(b1.status, 'online');
  assert.deepEqual(authServer.sockets().map(s => s.user.id), ['u2']);
  const { disconnected, revoked } = authServer.socketStats();
  assert.deepEqual({ disconnected, revoked }, { disconnected: 2, revoked: 1 });
  alice.close();
  bob.close();
  await authServer.shutdown();
});

test('expiresAt closes a socket when its session runs out: a client with fresh credentials is back at once, one without is signed out, and a session about to lapse is not let in', async () => {
  const tokens = new Map();   // token -> { user, expires }
  const store = createStore({ initial: INITIAL, storage: memoryStorage() });
  const expiring = serve({
    port: 0,
    stores: () => store,
    minSession: 100,
    authenticate: req => tokens.get(new URL(req.url).searchParams.get('token'))?.user ?? null,
    expiresAt: (user, req) => tokens.get(new URL(req.url).searchParams.get('token')).expires
  });
  const soon = Date.now() + 400;
  tokens.set('first', { user: { id: 'u1' }, expires: soon });
  let token = 'first';
  const connection = createConnection({ transport: webSocketTransport(() => `ws://localhost:${expiring.port}/ws?token=${token}`), reconnect: { min: 20, max: 50 }, keepalive: false });
  const a = attach(connection, 'main', 'a');
  await until(() => a.status === 'online', 'online');
  assert.equal(expiring.sockets()[0].expiresAt, soon);

  tokens.set('fresh', { user: { id: 'u1' }, expires: new Date(Date.now() + 3_600_000) });
  token = 'fresh';
  let drops = 0;
  connection.on('status', status => { if (status === 'offline') drops++; });
  await until(() => drops === 1 && a.status === 'online', 'closed at expiry and back');
  assert.ok(Date.now() >= soon, 'not before its time');
  assert.equal(expiring.sockets()[0].expiresAt, tokens.get('fresh').expires.getTime());
  a.collection('tasks').add({ id: 'x', title: 'after the refresh' });
  await until(() => store.state.tasks.x, 'writes land');

  tokens.set('stale', { user: { id: 'u1' }, expires: Date.now() + 50 });
  tokens.set('past', { user: { id: 'u1' }, expires: Date.now() - 1000 });
  for (const t of ['stale', 'past']) {
    const raw = new WebSocket(`ws://localhost:${expiring.port}/ws?token=${t}`);
    const code = await new Promise(resolve => { raw.onclose = e => resolve(e.code); });
    assert.equal(code, 4401, t);
  }

  tokens.set('last', { user: { id: 'u1' }, expires: Date.now() + 300 });
  token = 'last';
  expiring.disconnect();
  await until(() => a.status === 'online' && expiring.sockets()[0]?.expiresAt === tokens.get('last').expires, 'on the short token');
  tokens.delete('last');
  await until(() => a.closed?.code === 'unauthorized', 'signed out once it ran out');
  const { expired, disconnected } = expiring.socketStats();
  assert.deepEqual({ expired, disconnected }, { expired: 2, disconnected: 1 });
  connection.close();
  await expiring.shutdown();
});

afterAll(async () => {
  for (const connection of open) connection.close();
  server.stop(true);
  stores.dispose();
});
