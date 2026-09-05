// check.ts - Exercises the public typings the way an app would; `npm run test:types`
// compiles it (never runs it). Wrong usages carry an expect-error
// directive, so the declarations are tested in both directions.
import {
  createClient, openClient, createConnection, sharedConnection, webSocketTransport, memoryOutbox, localStorageOutbox, indexedDBStorage,
  LazyWatch, compareTs, createClock, pathKey, registerSet, leaves, rebuild, randomId,
  type Client, type ClientError, type Timestamp, type Diff, type ServerMessage, type RowStorage, type Op, type Peer
} from 'lazy-storage';
import { createClient as createClientAgain } from 'lazy-storage/client';
import { sqliteClientStorage } from 'lazy-storage/client/sqlite';
import {
  createStore, createStores, createHub, memoryStorage, jsonFileStorage, isStoreId, toJSON, tagStore,
  type Store, type ServerStorage, type StorageCommit, type OpEvent
} from 'lazy-storage/server';
import { createHandlers, serve } from 'lazy-storage/server/bun';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { mergeOp } from 'lazy-storage/core';
import { useClient as useClientVue } from 'lazy-storage/vue';
import { useClient as useClientReact, trackClient } from 'lazy-storage/react';
import { createNetwork, fakeTime } from 'lazy-storage/testing';

interface Task { id: string; title: string; done: boolean }
interface State { tasks: Record<string, Task>; order: string[] }

// --- Client ---------------------------------------------------------------------

const connection = createConnection({ transport: webSocketTransport(() => 'ws://localhost:3200/ws?token=x'), reconnect: { min: 500, max: 10_000 }, keepalive: false });
const status: 'offline' | 'connecting' | 'open' = connection.status;

const db: Client<State> = createClient<State>({
  connection,
  store: 'team-1',
  initial: { tasks: {}, order: [] },
  registers: ['order'],
  storage: localStorageOutbox('app:team-1')
});
db.connect();
db.state.tasks.a = { id: 'a', title: 'typed', done: false };
db.state.order.push('a');
const tasks = db.collection<Task>('tasks');
const id: string = tasks.add({ title: 'new', done: false });
tasks.update(id, { done: true });
const maybe: Task | undefined = tasks.get(id);
const pending: number = db.pending;
const version: number = db.version;
db.on('status', s => { const _s: 'offline' | 'connecting' | 'online' = s; });
db.on('error', (err: ClientError) => { if (err.code === 'rate-limited') return; });
db.on('closed', c => { const _code: string = c.code; });
db.on('history', ({ canUndo, canRedo }) => { const _both: boolean = canUndo && canRedo; });
db.share({ editing: id });
const peers: Peer[] = db.peers;
const mine: Peer | undefined = peers.find(p => p.replicaId === db.replicaId);
db.on('peers', list => { const _n: number = list.length; });
void mine;
db.watch((changes, inverse, meta) => { if (meta?.origin === 'remote') return; });
db.undo(); db.redo(); db.group(() => 1);
const snapshot: State = LazyWatch.snapshot(db.state);
db.dispose();

// @ts-expect-error a client needs a store id
createClient<State>({ connection });
// @ts-expect-error not both
createClient<State>({ store: 'x', connection, transport: webSocketTransport('ws://x') });
// @ts-expect-error status is read-only
db.status = 'online';

const owned = createClientAgain({ store: 'solo', transport: webSocketTransport('ws://localhost:3200/ws'), storage: memoryOutbox(), cache: false });
owned.disconnect();

// --- Framework entries -------------------------------------------------------

const view = useClientVue(db);
const mirrored: string | undefined = view.state.tasks.a?.title;
const online: boolean = view.status.value === 'online';
const undoable: boolean = view.canUndo.value;
view.stop();
// @ts-expect-error the mirror's status is a ref
const notARef: string = view.status;

const snap = useClientReact(db);
const same: State = snap.state;
const queued: number = snap.pending;
const reason: string | undefined = snap.closed?.code;
const tracker = trackClient(db);
const unsubscribe: () => void = tracker.subscribe(() => {});
unsubscribe();
// @ts-expect-error a snapshot is read-only
snap.pending = 0;
void mirrored; void online; void undoable; void same; void queued; void reason;

// --- Testing entry -----------------------------------------------------------

async function inMemory() {
  const now = fakeTime();
  const testStore = createStore<State>({ initial: { tasks: {}, order: [] }, now });
  const net = createNetwork(testStore);
  const tester = net.client<State>({ replicaId: 'a', initial: { tasks: {}, order: [] }, now }, { user: { id: 'u1' } });
  await net.settle();
  tester.link.goOffline();
  const later: number = now.advance(1000);
  tester.link.goOnline();
  tester.connect();
  const queuedMessages: number = net.pending;
  const link = net.link({ user: { id: 'u2' } });
  const transport = link.factory();
  transport.close();
  void [later, queuedMessages];
  // @ts-expect-error the network takes a store or an endpoint with session()
  createNetwork({ nope: true });
}
void inMemory;

async function browser() {
  const storage = indexedDBStorage('app:team-1', { onError: e => console.warn(e) });
  const opened = await openClient<State>({ store: 'team-1', connection, initial: { tasks: {}, order: [] }, storage });
  await storage.settled();
  await storage.destroy();
  return opened.restored;
}

const rows: RowStorage = sqliteClientStorage('mirror.sqlite');
const opFor: Op = { replicaId: 'r', seq: 1, ts: [1, 0, 'r'], diff: { tasks: { a: null } } };
rows.saveOp(opFor, { replicaId: 'r', seq: 1, version: 3, epoch: 'e' });

// --- Core -------------------------------------------------------------------------

const clock = createClock('r');
const ts: Timestamp = clock.now();
const order: number = compareTs(ts, clock.peek());
const key: string = pathKey(['tasks', 'a']);
const regs = registerSet(['order', 'tasks/*/subtaskOrder']);
const matched: boolean = regs.matches(['order']);
const flat = leaves({ tasks: { a: { title: 'x' } } } as Diff, regs);
const rebuilt: State = rebuild<State>({ tasks: {}, order: [] }, [[key, 1]]);
const merged = mergeOp(new Map(), ts, {}, regs);
const accepted: Diff | null = merged.accepted;
const rid: string = randomId();
const message: ServerMessage = { t: 'patch', diff: {}, ts, v: 1 };
void [status, maybe, pending, version, snapshot, order, matched, flat, rebuilt, accepted, rid, message, browser];

// --- Server ---------------------------------------------------------------------------

const store: Store<State> = createStore<State>({
  initial: { tasks: {}, order: [] },
  registers: ['order'],
  readOnly: ['team'],
  validate(diff, { user, store }) {
    const _s: Store<State> = store;
    if ((user as { role?: string } | undefined)?.role !== 'editor') return false;
    return diff;
  },
  maxSkew: 60_000,
  retention: Infinity,
  rateLimit: { burst: 100, perSecond: 10 },
  onError: err => console.error(err)
});
store.patch({ tasks: { a: { id: 'a', title: 't', done: false } } });
const session = store.session({ send: m => { if (m.t === 'ack') return m.seq; }, user: { id: 'u1' } });
session.receive({ t: 'ping' });
const off = store.observe('op', (e: OpEvent) => e.accepted);
store.observe('session', e => e.event === 'open');
// @ts-expect-error unknown event
store.observe('nope', () => {});
off();
const evicted: number = store.closeSessions(s => (s.user as { id: string }).id === 'u1', 'bye');
const stats = store.stats();
const version2: number = stats.version;
const compacted: { tombstones: number; replicas: number } = store.compact();

const stores = createStores(id => (isStoreId(id) ? createStore<State>({ storage: memoryStorage() }) : null), { idle: 60_000 });
const got: Store<State> | null = stores.get('team-1');
const released: string[] = stores.sweep();

const custom: ServerStorage = {
  load: () => null,
  commit: (change: StorageCommit) => { void change.log?.v; },
  flush() {}
};
void [jsonFileStorage('x.json', { debounce: 100 }), custom];

const hub = createHub(id => stores.get(id), { send: m => toJSON(m), user: { id: 'u1' }, authorize: (user, storeId) => storeId.startsWith('team-') });
hub.receive({ t: 'hello', store: 'team-1', replicaId: 'r', ops: [] });
const tagged = tagStore({ t: 'pong' as const }, 'team-1');
const storeName: string = tagged.store;

const handlers = createHandlers({
  stores,
  authenticate: req => new URL(req.url).searchParams.get('token'),
  authorize: (user, storeId) => typeof user === 'string' && storeId.length > 0,
  maxPayload: 1024 * 1024
});
async function bun() {
  await handlers.close({ reason: 'deploy' });
  const server = serve({ stores, port: 0 });
  const port: number = server.port;
  await server.shutdown();
  return port;
}
const sqlite = sqliteStorage('data/state.sqlite');
const adapter: ServerStorage = sqlite.store('team-1');
void [evicted, version2, compacted, got, released, storeName, bun, adapter];

// --- Node -----------------------------------------------------------------------------
import { serve as serveNode, createHandlers as createNodeHandlers, toRequest } from 'lazy-storage/server/node';
import { sqliteStorage as sqliteStorageNode } from 'lazy-storage/server/sqlite-node';
import { createServer } from 'node:http';

async function node() {
  const nodeServer = serveNode({ stores, port: 0, authenticate: req => new URL(req.url).searchParams.get('token'), request: (req, res) => res.end('app') });
  const port = (nodeServer.address() as { port: number }).port;
  await nodeServer.shutdown({ reason: 'deploy' });
  const lazy = createNodeHandlers({ stores, path: '/sync', maxPayload: 1024 });
  const http = createServer((req, res) => { const r: Request = toRequest(req); res.end(r.url); });
  http.on('upgrade', (req, socket, head) => { lazy.upgrade(req, socket, head).then((ours: boolean) => { if (!ours) socket.destroy(); }); });
  await lazy.close();
  const nodeSqlite: ServerStorage = sqliteStorageNode('data/state.sqlite', { wal: false }).store('team-1');
  return [port, nodeSqlite] as const;
}
void node;

// --- Lists as arrays ------------------------------------------------------------------
interface ViewState { tasks: Array<{ id: string; title: string; done: boolean; subtasks?: Array<{ id: string; title: string }> }> }
const viewDb = createClient<ViewState>({ connection, store: 'team-2', initial: { tasks: [] }, lists: ['tasks', 'tasks/*/subtasks'] });
viewDb.state.tasks.push({ id: 'x', title: 'array-shaped', done: false });
viewDb.state.tasks[0].subtasks = [{ id: 's', title: 'nested' }];
const wireSide: object = viewDb.wire;
const positioned = viewDb.list('tasks').all();
void [wireSide, positioned];

// --- Presence options -----------------------------------------------------------

createStore<State>({
  initial: { tasks: {}, order: [] },
  presence: {
    key: user => String((user as { id: string }).id),
    user: user => ({ id: (user as { id: string }).id }),
    validate: (data, { user, replicaId, store: self }) => { void user; void replicaId; void self; return data; },
    every: 50,
    maxShare: 1024
  }
});
createStore<State>({ initial: { tasks: {}, order: [] }, presence: true });
const quiet = createClient<State>({ store: 'mirror', connection, presence: false });
quiet.dispose();
// @ts-expect-error presence is a boolean or options
createStore<State>({ initial: { tasks: {}, order: [] }, presence: 'yes' });

// --- One socket per browser ---------------------------------------------------

const shared = sharedConnection({
  name: 'app',
  transport: webSocketTransport(() => 'ws://localhost:3200/ws?token=x'),
  storage: storeId => localStorageOutbox(`app:${storeId}`),
  reconnect: { min: 500, max: 10_000 }
});
const leads: boolean = shared.leader;
const tabbed = createClient<State>({ connection: shared, store: 'team-1', initial: { tasks: {}, order: [] } });
tabbed.connect();
shared.dispose();
void leads;
// @ts-expect-error a shared connection needs a name
sharedConnection({ transport: webSocketTransport('ws://x') });

// --- Forgetting a store -------------------------------------------------------
localStorageOutbox('app:gone').clear();
memoryOutbox().clear();
