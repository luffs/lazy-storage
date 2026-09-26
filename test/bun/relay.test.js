// relay.test.js - Runs under Bun only: a relay between displays and a server, over real sockets
import assert from 'node:assert/strict';
import { test, afterAll } from 'bun:test';
import { createStore, createStores, memoryStorage } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';
import { createRelay } from '../../src/relay/index.js';
import { createRelayHandlers, upstreamSocket } from '../../src/relay/bun.js';
import { createClient, createConnection, webSocketTransport } from '../../src/index.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = async (pred, label) => {
  for (let i = 0; i < 1000; i++) {
    if (pred()) return;
    await sleep(10);
  }
  throw new Error(`timeout: ${label}`);
};
const sha256 = text => new Bun.CryptoHasher('sha256').update(text).digest('hex');

// The server: its stores outlive the process serving them (a restart), and
// authenticate records what each request carried
const storages = new Map();
const stores = createStores(id => {
  if (!storages.has(id)) storages.set(id, memoryStorage());
  return createStore({ initial: INITIAL, presence: true, storage: storages.get(id) });
});
const requests = [];
const centralOptions = {
  stores,
  path: '/sync',
  // A snapshot over 64 bytes would be fetched over HTTP by a client that can: the relay asks for it inline
  httpSnapshots: { threshold: 64 },
  authenticate: req => {
    const url = new URL(req.url);
    const token = req.headers.get('authorization')?.replace('Bearer ', '');
    requests.push({ token, deviceCode: url.searchParams.get('deviceCode'), agent: req.headers.get('user-agent'), origin: req.headers.get('origin') });
    return token && token === url.searchParams.get('deviceCode') ? { id: token, agent: req.headers.get('user-agent') } : null;
  }
};
let central = serve({ port: 0, ...centralOptions });
const centralPort = central.port;

const relay = createRelay({ grace: 100, probeEvery: 50, dialTimeout: 1000, keepalive: false, jitter: 0, saveDelay: 20 });
const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  key: req => sha256(req.headers.get('authorization') ?? ''),
  upstream: req => upstreamSocket(`ws://localhost:${centralPort}/sync${new URL(req.url).search}`, {
    headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' }
  })
});
const hub = Bun.serve({
  port: 0,
  fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }),
  websocket: handlers.websocket
});

/** A display: a WebSocket to the relay with its token in a header and its device code in the query, as a kiosk's agent would */
const displays = [];
function display(name) {
  class Authorized extends WebSocket {
    constructor(url) { super(url, { headers: { authorization: `Bearer ${name}`, 'user-agent': `display ${name}` } }); }
  }
  const connection = createConnection({ transport: webSocketTransport(`ws://localhost:${hub.port}/sync?deviceCode=${name}`, { WebSocket: Authorized }), reconnect: { min: 20, max: 100 }, keepalive: false });
  const db = createClient({ connection, store: 'main', initial: INITIAL, replicaId: `r-${name}` });
  db.errors = [];
  db.on('error', err => db.errors.push(err));
  db.connect();
  displays.push(db);
  return db;
}

afterAll(async () => {
  for (const db of displays) db.dispose();
  await handlers.close();
  relay.close();
  hub.stop(true);
  await central.shutdown();
});

test('through a relay: the headers and the query reach the server, a large snapshot comes inline, and the copy follows', async () => {
  const store = stores.get('main');
  store.patch({ tasks: { big: { id: 'big', text: 'x'.repeat(500) } } });   // a snapshot past the HTTP threshold
  const a = display('alpha');
  const b = display('beta');
  await until(() => a.status === 'online' && b.status === 'online', 'online through the relay');
  const alpha = requests.find(r => r.token === 'alpha');
  assert.deepEqual(alpha, { token: 'alpha', deviceCode: 'alpha', agent: 'display alpha', origin: null });
  assert.equal(a.state.tasks.big.text.length, 500);
  assert.deepEqual(a.errors, [], 'no snapshot to fetch from the relay, which serves none: it came inline');
  assert.equal(store.sessions, 2);
  a.state.tasks.x = { id: 'x', title: 'from alpha' };
  await until(() => b.state.tasks.x?.title === 'from alpha' && a.pending === 0, 'crossed');
  assert.deepEqual(relay.copy('main').state, store.snapshot());
  assert.equal(relay.copy('main').v, store.version);
  assert.equal(relay.mode, 'through');
  assert.ok(handlers.sockets().every(s => s.state === 'through' && typeof s.key === 'string'));
});

test('the server gone: the relay answers, the displays stay in step; the server back: each display\'s edits reach it as its own', async () => {
  const store = stores.get('main');
  const [a, b] = displays;
  const modes = [];
  relay.on('mode', mode => modes.push(mode));
  central.stop(true);
  await until(() => relay.mode === 'local' && a.relayed && b.relayed && a.status === 'online' && b.status === 'online', 'answered by the relay');
  a.state.tasks.y = { id: 'y', title: 'alpha, offline' };
  await until(() => b.state.tasks.y?.title === 'alpha, offline' && b.status === 'online', 'b has it');
  b.state.tasks.z = { id: 'z', title: 'beta, offline' };
  await until(() => a.state.tasks.z?.title === 'beta, offline', 'a has it');
  assert.equal(a.pending, 1);
  assert.equal(b.pending, 1);
  assert.equal(store.snapshot().tasks.y, undefined);

  const users = [];
  const stop = store.observe('op', e => users.push(e.user?.id));
  central = serve({ ...centralOptions, port: centralPort });
  await until(() => relay.mode === 'through' && !a.relayed && !b.relayed && a.pending + b.pending === 0 && a.status === 'online' && b.status === 'online', 'through again');
  stop();
  assert.deepEqual(users.sort(), ['alpha', 'beta'], 'each op the server merged was its author\'s');
  assert.deepEqual(modes, ['local', 'through']);
  assert.equal(store.snapshot().tasks.y.title, 'alpha, offline');
  assert.equal(store.snapshot().tasks.z.title, 'beta, offline');
  await until(() => JSON.stringify(relay.copy('main').state) === JSON.stringify(store.snapshot()) && relay.copy('main').live, 'the copy follows again');
  assert.equal(relay.copy('main').diverged, false);
  assert.deepEqual([...a.errors, ...b.errors], []);
});
