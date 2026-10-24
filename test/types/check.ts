// check.ts - Exercises the public typings the way an app would; `npm run test:types`
// compiles it (never runs it). Wrong usages carry an expect-error
// directive, so the declarations are tested in both directions.
import {
  createClient, openClient, createConnection, sharedConnection, webSocketTransport, memoryOutbox, localStorageOutbox, indexedDBStorage,
  LazyWatch, createClock, createShape, compile, docFromRecords, inOrder, keyBetween, randomId,
  type Client, type ClientError, type Change, type Lost, type ServerMessage, type RowStorage, type Op, type Peer, type RecordClock
} from 'lazy-storage';
import { createClient as createClientAgain } from 'lazy-storage/client';
import { sqliteClientStorage } from 'lazy-storage/client/sqlite';
import {
  createStore, createStores, createHub, memoryStorage, isStoreId, toJSON, tagStore,
  type Store, type ServerStorage, type StorageCommit, type OpEvent, type StoreDocument
} from 'lazy-storage/server';
import { createHandlers, serve } from 'lazy-storage/server/bun';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { PROTOCOL, diffOf } from 'lazy-storage/core';
import { useClient as useClientVue } from 'lazy-storage/vue';
import { useClient as useClientReact, useClientSelector, trackClient } from 'lazy-storage/react';
import { createNetwork, fakeTime } from 'lazy-storage/testing';

interface Task { id: string; title: string; done: boolean; pos?: string; meta?: { color?: string } }
interface State { tasks: Record<string, Task>; settings: { theme: string; order: string[] } }
const SHAPE = { initial: { tasks: {}, settings: { theme: 'light', order: [] as string[] } }, singletons: ['settings'] };

// --- Client ---------------------------------------------------------------------

const connection = createConnection({ transport: webSocketTransport(() => 'ws://localhost:3200/ws?token=x'), reconnect: { min: 500, max: 10_000 }, keepalive: false });
const status: 'offline' | 'connecting' | 'online' = connection.status;

const db: Client<State> = createClient<State>({
  connection,
  store: 'team-1',
  ...SHAPE,
  registers: ['tasks/*/geo'],
  parents: {},
  storage: localStorageOutbox('app:team-1')
});
db.connect();
db.state.tasks.a = { id: 'a', title: 'typed', done: false };
db.state.tasks.a.meta = { color: 'red' };
db.state.settings.order.push('a');
const list = db.list<Task>('tasks');
const ordered: Task[] = list.all();
list.add({ title: 'new', done: false }, { at: 0 });
list.move('a', { after: 'b' });
const inOrderToo: Task[] = inOrder(db.state.tasks);
const nested = db.list(['settings', 'lists', 'items']);
void [ordered, inOrderToo, nested];
const tasks = db.collection<Task>('tasks');
const id: string = tasks.add({ title: 'new', done: false });
tasks.update(id, { done: true });
tasks.watch((changes, meta) => {
  for (const change of changes) if (change.type === 'update') { const was: Task = change.previous; void was; }
  void meta?.origin;
});
const pending: number = db.pending;
const { oldestPendingMs, ackMs } = db.stats();
const version: number = db.version;
const base: State = db.base;
const clock: RecordClock = db.clockOf('tasks', 'a');
const written: number | undefined = clock.live['["title"]'];
db.on('status', s => { const _s: 'offline' | 'connecting' | 'online' = s; });
db.on('error', (err: ClientError) => { if (err.code === 'shape-mismatch' || err.code === 'rate-limited') return; });
db.on('lost', ({ seq, lost }) => { const first: Lost = lost[0]; const why: 'missing' | 'newer' | 'stale' = first.why; void [seq, why]; });
db.on('rejected', ({ seq, code, changes }) => { const maybe: number | null = seq; const what: Change[] | null = changes; void [maybe, code, what]; });
db.on('reset', ({ epoch, previous }) => { const was: number = previous.version; void [epoch, was, previous.state]; });
db.on('relay', isRelayed => { const _b: boolean = isRelayed; });
db.share({ editing: id });
const peers: Peer[] = db.peers;
db.watch((diff, inverse, meta) => { if (meta?.origin === 'remote') return; void [diff, inverse]; });
db.undo(); db.redo(); db.group(() => 1);
const snapshot: State = LazyWatch.snapshot(db.state);
const saving: boolean = db.isPending('tasks/a/title') || db.isPending(['tasks', 'a']);
void [status, pending, oldestPendingMs, ackMs, version, base, written, peers, snapshot, saving];
db.dispose();

// @ts-expect-error a client needs a store id
createClient<State>({ connection });
// @ts-expect-error not both
createClient<State>({ store: 'x', connection, transport: webSocketTransport('ws://x') });
// @ts-expect-error status is read-only
db.status = 'online';
// @ts-expect-error no conflict event in 1.0: lost says what lost and why
db.on('conflict', () => {});

const owned = createClientAgain({ store: 'solo', transport: webSocketTransport('ws://localhost:3200/ws'), storage: memoryOutbox(), cache: false });
owned.disconnect();

// --- Storage and connections ---------------------------------------------------------

const rows: RowStorage = indexedDBStorage('app:team-1');
void openClient<State>({ connection, store: 'team-1', ...SHAPE, storage: rows });
const bunRows: RowStorage = sqliteClientStorage('mirror.sqlite');
const shared = sharedConnection({ name: 'app', transport: webSocketTransport('ws://x'), storage: store => indexedDBStorage(`app:${store}`), shapeOf: () => SHAPE, linger: 5000 });
const leads: boolean = shared.leader;
void [bunRows, leads];

// --- Framework entries -------------------------------------------------------

const view = useClientVue(db);
const mirrored: string | undefined = view.state.tasks.a?.title;
const online: boolean = view.status.value === 'online';
view.stop();
const editable = useClientVue(db, { writable: true });
editable.state.tasks.a.done = true;
const tracked = trackClient(db);
void [mirrored, online, tracked, useClientReact, useClientSelector];

// --- Core --------------------------------------------------------------------------

const shape = createShape(SHAPE);
const doc = docFromRecords(shape, [['tasks', 'a', { title: 'x' }, { live: { '[]': 1 }, gone: {} }]]);
const { applied, lost } = doc.apply({ c: 'tasks', id: 'a', path: ['title'], set: 'y' }, 2, 'judge');
const changes: Change[] = compile(shape, { tasks: { a: { title: 'z' } } }, { tasks: { a: { title: 'y' } } }, doc.state);
const diff = diffOf(shape, changes);
const t = createClock({ now: () => 1 }).stamp();
const op: Op = { replica: 'r', seq: 1, ts: t, changes: [{ c: 'tasks', id: 'a', del: true }] };
const key: string = keyBetween(null, null);
void [applied, lost, diff, op, key, randomId(), PROTOCOL];
// @ts-expect-error a change is one of put, set or del with what it carries
const badChange: Change = { c: 'tasks', id: 'a', del: false };
void badChange;

// --- Server ------------------------------------------------------------------------

const store: Store<State> = createStore<State>({
  ...SHAPE,
  rules: {
    tasks: (user, before, after, { state, changes: recordChanges }) => (after ?? before)?.title !== 'nope' && Boolean(user) && Boolean(state) && recordChanges.length > 0,
    settings: false,
    '*': true
  },
  readOnly: ['settings'],
  validate: (whole, { user, replicaId }) => { void [whole, user, replicaId]; return true; },
  retention: 30 * 86_400_000,
  maxChanges: 1000,
  presence: { every: 250 },
  storage: memoryStorage()
});
store.patch({ tasks: { a: { title: 'server' } } });
store.del('tasks', 'a');
const result = store.apply({ replica: 'r', seq: 1, ts: Date.now(), changes: [] }, { user: { id: 'u' } });
const dup: boolean = result.duplicate;
const recordClock: RecordClock = store.clock('tasks', 'a');
store.on((changed, meta) => { void [changed.tasks, meta.origin, meta.changes]; });
store.observe('op', (e: OpEvent) => { void [e.replicaId, e.changes]; });
const exported: StoreDocument = store.export();
const records: number = store.stats().records;
void [dup, recordClock, exported, records];
// @ts-expect-error readOnly is true or a list of collections
createStore({ readOnly: 'settings' });

const commits: StorageCommit[] = [];
const mine: ServerStorage = { load: () => null, commit: change => { commits.push(change); }, flush: () => {} };
createStore({ storage: mine });
const registry = createStores(storeId => (isStoreId(storeId) ? createStore({ storage: memoryStorage() }) : null), { idle: 60_000 });
const hub = createHub(storeId => registry.get(storeId), { send: m => void toJSON(m), vouch: credential => credential, vouchRate: { burst: 20, perSecond: 2 } });
void tagStore({ t: 'pong' }, 'x');
hub.revoke(() => true);
const handlers = createHandlers({ stores: registry, relays: { authenticate: () => ({ id: 'relay' }), rate: false } });
void [handlers, serve, sqliteStorage('app.sqlite').fromLeaves];

// --- Testing -------------------------------------------------------------------------

const net = createNetwork(store, { vouch: credential => credential });
const time = fakeTime();
const testClient = net.client({ ...SHAPE, now: time });
testClient.link.goOffline();
const message = null as unknown as ServerMessage;
if (message.t === 'ack' && message.lost) { const why: string = message.lost[0].why; void why; }
