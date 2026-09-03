# lazy-storage

A self-hosted, realtime state store with offline sync, built on
[lazy-watch](https://github.com/luffs/lazy-watch). Think of it as the
"database + sync" half of a Firebase-style backend for a whole application
state tree: clients hold a live mirror they read and write like a plain
object, edits sync as small JSON diffs, work continues offline, and
everything converges when the connection is back.

```bash
npm install lazy-storage
```

```js
import { createClient, webSocketTransport, localStorageOutbox } from 'lazy-storage';

const db = createClient({
  transport: webSocketTransport('wss://example.com/ws?token=...'),
  store: 'todo',
  initial: { tasks: {}, order: [] },
  registers: ['order'],
  storage: localStorageOutbox('todo')
});
db.connect();

const tasks = db.collection('tasks');
const id = tasks.add({ title: 'Ship it', done: false }); // synced, offline or not
tasks.update(id, { done: true });
db.state.order.push(id);

db.watch((diff, inverse, meta) => render(db.state, meta?.origin === 'remote'));
db.undo();
```

```js
import { createStore, createStores } from 'lazy-storage/server';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { serve } from 'lazy-storage/server/bun';

// Any number of stores in one SQLite file, one row per leaf
const sqlite = sqliteStorage('data/app.sqlite');
const stores = createStores(id => createStore({
  initial: { tasks: {}, order: [] },
  registers: ['order'],
  storage: sqlite.store(id)
}));
serve({
  stores,
  port: 3200,
  authenticate: req => userForToken(new URL(req.url).searchParams.get('token')), // null → 401
  authorize: (user, storeId) => user.teams.includes(storeId)                     // false → 403 / closed
});
// clients connect to ws://host:3200/ws?token=... and name their store per client
```

A server with a single store passes `stores: () => store`; there is one
protocol and one route either way.

## The model

State is a tree of plain objects and JSON leaves, and **lists are objects
keyed by id**. That single rule is what makes offline edits mergeable: a
change addresses a record by identity, so two people adding, editing, or
removing different records never collide, and an edit made on a stale
replica still lands on the record it meant.

Arrays are allowed only at declared **registers**: paths whose value is one
unit, written wholesale and resolved as a whole (an ordering of ids, a set
of tags). A `*` segment matches any one segment, so `tasks/*/subtaskOrder`
declares one register per task. Anywhere else an array write is reverted
on the client and reported through the `error` event, and refused by the
server.

## How conflicts resolve

Every op carries a hybrid-logical-clock timestamp. The server keeps the
timestamp of the last accepted write per leaf path and decides per leaf:

- A **write** wins if nothing at that path is newer. Two people editing
  different fields of the same record both land; editing the same field,
  the later timestamp wins everywhere, regardless of who reconnects first.
- A **deletion** wins if nothing at or below the path is newer. It leaves a
  tombstone, so an older edit that arrives later cannot resurrect a
  partial record. A newer edit of a single field of a deleted record is
  also refused (a deleted record does not come back as one field); a
  newer *record* write, an object with an `id`, re-adds it.
- A **register** is one leaf: the newest whole value wins.

A client whose op lost receives a correction with the server's values and
falls back in line. The server is the only merge point, which is what
keeps this small: clients never merge with each other, and tombstones can
be compacted whenever every replica has synced past them.

Clocks are hybrid logical clocks, so a replica with a skewed wall clock is
pulled into line by whatever it receives and does not win (or lose) every
conflict for as long as its clock is off.

## Offline

Local edits go into an **outbox** first, persisted through a storage
adapter (`localStorageOutbox` in browsers, `memoryOutbox` for tests or
throwaway sessions), and are sent when online. The same adapter caches
the **last state** next to the outbox, so a client restarted while offline,
or before its first snapshot has landed, starts from what it last saw with
its pending edits already applied (`db.restored` says so) rather than from
`initial`; the snapshot on reconnect then brings it up to date. Pass
`cache: false` to keep only the outbox. On (re)connect the client
sends the whole outbox in one `hello`; the server merges it and answers
with a snapshot that already contains the result. Edits made while that
was in flight are re-applied on top and sent. Nothing is lost on a
reload while offline: the replica id, sequence numbers, and pending ops
are restored with the outbox, and the server ignores an op it has already
seen.

## Persistence

A store persists as **rows, one per leaf path**: a live row holds the
value and the timestamp that won it, a tombstone holds the timestamp only.
Every accepted op commits exactly the rows it won and the entries it
dropped, inside one transaction, so the write cost of an edit is a few
rows rather than the state. On load the state is `initial` with the rows
applied on top: `initial` is the skeleton an app expects to exist (its
top-level containers), the rows carry the data, and a container added to
`initial` later simply appears.

Adapters:

- `sqliteStorage(file)` (`lazy-storage/server/sqlite`, Bun) — one file for
  any number of stores, keyed by `(store, path)`, WAL mode. `sqlite.store(id)`
  gives a store its adapter; `sqlite.ids()`, `sqlite.remove(id)`, and the
  raw `sqlite.db` are there for administration.
- `jsonFileStorage(file)` — one JSON document per store, written
  atomically and debounced. Fine for small single-store deployments.
- `memoryStorage()` — nothing survives the process; for tests.

Tombstones are the only rows that grow without bound;
`store.compactTombstones(olderThan)` forgets those older than a timestamp
once every replica that could still send an older write has synced.

## Multiple stores

`createStores(id => store)` is a registry: it builds a store on first use
through your factory (typically `createStore` with a per-id storage
adapter) and keeps it live; `stores.release(id)` disposes one, closing
its sessions, and the persisted rows stay. Store ids are restricted to a
URL- and filename-safe alphabet (`isStoreId`), so an id can name a file or
a table key without escaping.

### Any number of stores over one socket

Every message names its store, so one socket carries as many stores as
the clients attached to it. The server's hub keeps one session per store
per socket, and authorizes each store separately. Several clients, one per
store, share a connection:

```js
import { createClient, createConnection, webSocketTransport } from 'lazy-storage';

const connection = createConnection({ transport: webSocketTransport('wss://example.com/ws') });
const teamA = createClient({ connection, store: 'team-a', initial: { tasks: {} } });
const teamB = createClient({ connection, store: 'team-b', initial: { tasks: {} } });
teamA.connect();
teamB.connect();   // one socket, two stores
```

Each client keeps its own outbox, undo history, and status; `connection.status`
is the socket's. `client.disconnect()` on a shared connection leaves only
that store (the socket stays up for the others), and `connection.close()`
drops the socket for all of them. A client created with a `transport`
instead of a `connection` owns a connection of its own, same protocol. While
open, a connection pings every 30 seconds (`keepalive`, or `false`) so idle
sockets survive proxies and server idle timeouts.

## Authentication, presence, and eviction

Two hooks on the Bun adapter decide who gets a session on which store:

- `authenticate(req)` runs at upgrade and returns the user for the request
  (a token in the query string is the usual carrier, since browsers cannot
  set headers on a WebSocket; `webSocketTransport` takes a function URL for
  that). `null` or `undefined` answers 401.
- `authorize(user, storeId, store)` runs per store, before its session
  exists; a refusal arrives as a `closed` message with code `forbidden` and
  affects only that store, while the socket stays up for the others. Both
  hooks may return promises.

The user rides on the session (`store.session({ send, user })` if you drive
sessions yourself), which powers two more features:

- **Presence.** The store broadcasts the distinct users with a live
  session whenever one joins or leaves; `db.presence` holds the list,
  `db.on('presence', users => ...)` follows it, and `store.presence()`
  answers on the server. Users are distinct by `id` (or by value; override
  with `presenceKey`), so a user on two devices counts once.
- **Eviction.** `store.closeSessions(user => ..., message)` ends the
  sessions a predicate selects — say, everyone who was just removed from a
  team. The client receives a `closed` event with `{ code: 'evicted',
  message }`, goes offline for that store, and does not reconnect on its
  own; `db.closed` keeps the reason until `db.connect()` is called again,
  at which point authorization runs afresh. On a shared connection the
  socket stays up for the other stores.

## Embedding in your own server

`createHandlers` returns the two pieces `serve` is built from, so the
sockets can live inside a server that already has routes:

```js
import { createHandlers } from 'lazy-storage/server/bun';

const lazy = createHandlers({ stores, authenticate, authorize });
Bun.serve({
  port: 3200,
  async fetch(req, server) {
    const res = await lazy.upgrade(req, server); // null: not a lazy-storage URL
    if (res !== null) return res;
    return app.fetch(req);                        // your routes
  },
  websocket: lazy.websocket
});
```

## Undo

Each client has a lazy-watch undo manager attached with a `record` filter
that declines remote batches, so undo and redo only ever touch this
replica's own edits, and they sync like any other edit. Because records
are keyed, a teammate's insert does not invalidate your history; only a
change of shape under a path you edited (a register's length, a record
replaced by a leaf) drops the affected steps.

## API

**Client** (`lazy-storage`)

- `createClient({ store, connection | transport, initial, registers, replicaId, storage, cache, undo, undoLimit, reconnect, now })`; `db.restored` — started from the cached state
- `createConnection({ transport, reconnect, keepalive })` → `connect()`, `close()`, `status`, `attached`, `on('status', fn)` — a socket shared by clients
- `db.state` — the mirror (a lazy-watch proxy). Read and write it directly
- `db.store`, `db.connection` — the store id and the connection
- `db.presence` — users with a live session on this store; `db.closed` — `{ code, message }` after the server ended this store for us, else null
- `db.collection(name)` — `add(record) → id`, `update(id, fields)`, `remove(id)`, `get(id)`, `has(id)`, `ids()`, `all()`
- `db.connect()`, `db.disconnect()`, `db.status` (`'offline' | 'connecting' | 'online'`), `db.pending`
- `db.watch(listener)` — state changes; `meta?.origin === 'remote'` marks the server's
- `db.on('status' | 'error' | 'sync' | 'presence' | 'closed', fn)` — lifecycle events; server errors carry a `code`
- `db.undo()`, `db.redo()`, `db.canUndo`, `db.canRedo`, `db.checkpoint()`, `db.group(fn)`, `db.clearHistory()`
- `webSocketTransport(url)`, `memoryOutbox()`, `localStorageOutbox(key)`

**Server** (`lazy-storage/server`)

- `createStore({ initial, registers, storage, presenceKey, now })`
- `store.session({ send, user, onEvict })` → `{ receive(message), close(), user, replicaId }` — one per connection, transport-agnostic
- `store.closeSessions(predicate, message)` — evict sessions; `store.presence()` — distinct users with a live session
- `store.patch(diff)` — a server-side change, timestamped and broadcast
- `store.on(listener)`, `store.snapshot()`, `store.state`, `store.version`
- `store.compactTombstones(olderThan)`, `store.flush()`, `store.dispose()`
- `memoryStorage()`, `jsonFileStorage(path, { debounce })`
- `createStores(factory)` → `get(id)`, `has(id)`, `ids()`, `release(id)`, `dispose()`; `isStoreId(id)`
- `createHub(resolveStore, { send, user, authorize })` → `{ receive(message), close(), stores, user }` — the server side of a multiplexed connection, session-shaped

**SQLite** (`lazy-storage/server/sqlite`, Bun): `sqliteStorage(file, { wal })` →
`store(id)`, `ids()`, `remove(id)`, `db`, `close()`.

**Bun adapter** (`lazy-storage/server/bun`): `serve({ stores, port, path, fetch, authenticate, authorize })` —
`stores` is a registry or `id => store|null` (for one store, `() => store`); the
hub listens at `path`. `createHandlers({ stores, path, authenticate, authorize })` →
`{ upgrade(req, server), websocket }` for mounting inside your own `Bun.serve`.

**Core** (`lazy-storage/core`): the pieces both sides share — `createClock`,
`compareTs`, `mergeOp`, `leaves`, `expandRegisters`, `registerSet`,
`compactTombstones`, `randomId`.

## Wire protocol

Client to server: `{ t: 'hello', replicaId, ops }`, `{ t: 'op', op }`,
`{ t: 'ping' }`, where `op = { replicaId, seq, ts, diff }` and `diff` is a
plain lazy-watch diff.

Server to client: `{ t: 'snapshot', state, ts, seq, registers }` (the
server's register patterns, so a client can detect a declaration that
differs from its own), `{ t: 'patch', diff, ts }`,
`{ t: 'ack', seq, ts, correction }`, `{ t: 'presence', users }`,
`{ t: 'closed', code, message }` (final for the store: `evicted`,
`forbidden`, `unknown-store`, `invalid-store`),
`{ t: 'error', seq?, code?, message }`, `{ t: 'pong' }`.

Every message except `ping`/`pong` carries a `store` field with the store
id, and the client sends `{ t: 'leave', store }` to close one store's
session without dropping the socket.

## Scope

This is last-writer-wins per field with a single merge point. It gives
records identity and makes offline editing safe; it does not merge
concurrent edits to the *same* field (the later one wins) and it is not a
text CRDT. For collaborative documents, embed a purpose-built library for
the document and keep the surrounding state here.

Authentication is a hook, not a system: lazy-storage asks you who a request
is and whether they may open a store, and stores the answer on the session.
Users, tokens, and memberships stay in your application.

## Testing

```bash
npm test          # unit and integration tests plus a fixed-seed convergence run (Node)
npm run test:bun  # the bun:sqlite adapter (Bun)
npm run fuzz      # a longer randomized convergence campaign; a failure prints the seed
```
