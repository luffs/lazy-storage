// serve.test.js - Runs under Bun only: the server's adapter, SQLite, and a relay, over real sockets
import assert from 'node:assert/strict';
import { test, afterAll } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, createStores } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';
import { createRelay } from '../../src/relay/index.js';
import { createRelayHandlers, upstreamSocket } from '../../src/relay/bun.js';
import { createClient, webSocketTransport } from '../../src/index.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = async (pred, label) => {
  for (let i = 0; i < 500; i++) {
    if (pred()) return;
    await sleep(10);
  }
  throw new Error(`timeout: ${label}`);
};

const dir = mkdtempSync(join(tmpdir(), 'lazy-bun-'));
const sqlite = sqliteStorage(join(dir, 'app.sqlite'));
const users = { alice: { id: 'u1', teams: ['t1'] }, bob: { id: 'u2', teams: ['t2'] } };
const stores = createStores(id => createStore({ ...SHAPE, storage: sqlite.store(id) }));
const userOf = req => users[new URL(req.url).searchParams.get('token')] ?? null;
const server = serve({
  port: 0,
  path: '/sync',
  stores,
  authenticate: userOf,
  authorizeId: (user, id) => user.teams.includes(id),
  relays: { authenticate: req => (req.headers.get('authorization') === 'Bearer relay-secret' ? { id: 'relay' } : null) }
});
const url = token => `ws://localhost:${server.port}/sync?token=${token}`;
const clients = [];
const client = (token, store, replicaId, base = url(token)) => {
  const db = createClient({ transport: webSocketTransport(base), reconnect: { min: 20, max: 50 }, store, replicaId, ...SHAPE });
  db.connect();
  clients.push(db);
  return db;
};

afterAll(async () => {
  for (const db of clients) db.dispose();
  await server.shutdown();
  sqlite.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* the OS cleans it up */ }
});

test('clients sync through the Bun adapter, and SQLite keeps the store', async () => {
  const a = client('alice', 't1', 'a');
  const b = client('alice', 't1', 'b');
  await until(() => a.status === 'online' && b.status === 'online', 'online');
  a.state.tasks.k = { title: 'Milk', meta: { color: 'red' } };
  await until(() => b.state.tasks.k?.meta?.color === 'red', 'b has it');
  b.state.tasks.k.meta.size = 'L';
  await until(() => a.state.tasks.k.meta.size === 'L' && a.pending === 0 && b.pending === 0, 'a has it');
  stores.get('t1').flush();
  assert.deepEqual(sqlite.ids(), ['t1']);
});

test('a socket that does not authenticate, and a store the user may not open, are turned away', async () => {
  const stranger = client('nobody', 't1', 'x');
  const closed = [];
  stranger.on('closed', e => closed.push(e.code));
  const bob = client('bob', 't1', 'y');
  bob.on('closed', e => closed.push(e.code));
  await until(() => closed.length === 2, 'both closed');
  assert.deepEqual(closed.sort(), ['forbidden', 'unauthorized']);
});

test('a relay served by Bun carries its clients, each judged at the server as itself', async () => {
  const relay = createRelay({
    shapeOf: () => SHAPE,
    link: upstreamSocket(`ws://localhost:${server.port}/sync/relay`, { headers: { authorization: 'Bearer relay-secret' } }),
    reconnect: { min: 20, max: 50 },
    keepalive: false
  });
  const handlers = createRelayHandlers({
    relay,
    path: '/sync',
    credential: req => ({ query: new URL(req.url).search }),
    key: req => new URL(req.url).searchParams.get('token')
  });
  const site = Bun.serve({ port: 0, fetch: async (req, srv) => (await handlers.upgrade(req, srv)) ?? new Response('Not found', { status: 404 }), websocket: handlers.websocket });
  try {
    const siteUrl = token => `ws://localhost:${site.port}/sync?token=${token}`;
    const behind = client('alice', 't1', 'r1', siteUrl('alice'));
    const direct = client('alice', 't1', 'd1');
    await until(() => behind.status === 'online' && direct.status === 'online', 'online');
    assert.equal(behind.state.tasks.k.title, 'Milk', 'the relay answered from its copy of the server\'s');
    behind.state.settings.theme = 'dark';
    await until(() => direct.state.settings.theme === 'dark' && behind.pending === 0, 'through the relay');
    const refused = client('bob', 't1', 'r2', siteUrl('bob'));
    const closed = [];
    refused.on('closed', e => closed.push(e.code));
    await until(() => closed.length === 1, 'refused');
    assert.equal(closed[0], 'forbidden');
  } finally {
    await handlers.close();
    relay.close();
    site.stop(true);
  }
});
