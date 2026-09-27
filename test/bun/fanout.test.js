// fanout.test.js - Runs under Bun only: a relay that fans each store out to its displays, over real sockets
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

// The server, with a relay route: the hub is let in by its own token, and
// each display it vouches for is judged as the display's own socket would be
const storages = new Map();
const stores = createStores(id => {
  if (!storages.has(id)) storages.set(id, memoryStorage());
  return createStore({ initial: INITIAL, presence: true, storage: storages.get(id) });
});
const judged = [];
const centralOptions = {
  stores,
  path: '/sync',
  authenticate: (req, context) => {
    const token = req.headers.get('authorization')?.replace('Bearer ', '');
    judged.push({ token, agent: req.headers.get('user-agent'), origin: req.headers.get('origin'), relay: context?.relay ?? null, query: new URL(req.url).search });
    return token && token === new URL(req.url).searchParams.get('deviceCode') ? { id: token } : null;
  },
  relays: {
    authenticate: req => (req.headers.get('authorization') === 'Bearer hub-secret' ? { id: 'hub' } : null)
  }
};
let central = serve({ port: 0, ...centralOptions });
const centralPort = central.port;

const errors = [];
const relay = createRelay({
  grace: 300,
  probeEvery: 50,
  dialTimeout: 1000,
  keepalive: false,
  jitter: 0,
  saveDelay: 20,
  writeIdle: 200,
  onError: err => errors.push(err),
  link: upstreamSocket(`ws://localhost:${centralPort}/sync/relay`, { headers: { authorization: 'Bearer hub-secret' } })
});
const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  key: req => sha256(req.headers.get('authorization') ?? ''),
  upstream: req => upstreamSocket(`ws://localhost:${centralPort}/sync${new URL(req.url).search}`, {
    headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' }
  }),
  credential: req => ({ headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '', origin: 'https://made.up' }, query: new URL(req.url).search })
});
const hub = Bun.serve({
  port: 0,
  fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }),
  websocket: handlers.websocket
});

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

test('fanned out: one session on the store for the relay, each display a peer judged by the server\'s own hooks, and one patch for all', async () => {
  const store = stores.get('main');
  const a = display('alpha');
  const b = display('beta');
  await until(() => a.status === 'online' && b.status === 'online', 'online through the relay');
  const alpha = judged.find(j => j.token === 'alpha');
  assert.deepEqual(alpha, { token: 'alpha', agent: 'display alpha', origin: null, relay: { id: 'hub' }, query: '?deviceCode=alpha' }, 'the credential as the display\'s socket carried it, but the origin the relay made up');
  assert.equal(store.sessions, 3, 'the relay\'s session and the two displays as peers');
  assert.deepEqual(store.presence().map(u => u.id).sort(), ['alpha', 'beta']);
  const listed = central.sockets();
  assert.equal(listed.filter(s => s.relay).length, 1);
  assert.deepEqual(listed.filter(s => s.via).map(s => s.user.id).sort(), ['alpha', 'beta']);
  assert.deepEqual(central.socketStats().relays, 1);
  assert.deepEqual(central.socketStats().clients, 2);
  assert.equal(central.socketStats().sockets, 1, 'the relay\'s link is the only socket');
  assert.equal(relay.stats().link.state, 'open');

  const before = store.stats().sent.patch?.messages ?? 0;
  const users = [];
  const stop = store.observe('op', e => users.push(e.user?.id));
  a.state.tasks.x = { id: 'x', title: 'from alpha' };
  await until(() => b.state.tasks.x?.title === 'from alpha' && a.pending === 0, 'crossed');
  stop();
  assert.deepEqual(users, ['alpha'], 'the op is alpha\'s');
  assert.equal(store.stats().sent.patch.messages - before, 1, 'one patch, sent to the relay alone');
  assert.deepEqual(relay.copy('main').state, store.snapshot());
  await until(() => central.socketStats().sockets === 1, 'alpha\'s socket up closed once idle');
});

test('the server signs a display out, and cuts the relay off: each comes back, judged afresh', async () => {
  const [a, b] = displays;
  const count = judged.length;
  assert.equal(central.disconnect(user => user?.id === 'alpha'), 1);
  await until(() => judged.length > count && a.status === 'online', 'alpha back');
  assert.equal(b.status, 'online');

  assert.equal(central.disconnect(user => user?.id === 'hub'), 1, 'the relay, with its clients');
  await until(() => relay.stats().link.state !== 'open', 'the link closed');
  await until(() => relay.stats().link.state === 'open' && a.status === 'online' && b.status === 'online' && stores.get('main').sessions === 3, 'the link back, the displays vouched for again');
  a.state.tasks.y = { id: 'y' };
  await until(() => b.state.tasks.y && a.pending === 0, 'writes go on');
  assert.deepEqual([...a.errors, ...b.errors], []);
});

test('the server gone: the relay answers, and the displays\' offline edits reach it as their own when it is back', async () => {
  const store = stores.get('main');
  const [a, b] = displays;
  central.stop(true);
  await until(() => relay.mode === 'local' && a.relayed && b.relayed, 'answered by the relay');
  a.state.tasks.z = { id: 'z', title: 'alpha, offline' };
  await until(() => b.state.tasks.z?.title === 'alpha, offline', 'b has it');
  const users = [];
  const stop = store.observe('op', e => users.push(e.user?.id));
  central = serve({ ...centralOptions, port: centralPort });
  await until(() => relay.mode === 'through' && !a.relayed && !b.relayed && a.pending + b.pending === 0 && a.status === 'online' && b.status === 'online' && store.snapshot().tasks.z, 'through again');
  stop();
  assert.deepEqual(users, ['alpha']);
  await until(() => JSON.stringify(relay.copy('main').state) === JSON.stringify(store.snapshot()) && relay.copy('main').live, 'the copy follows again');
  assert.deepEqual([...a.errors, ...b.errors], []);
});
