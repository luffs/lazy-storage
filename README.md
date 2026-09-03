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
keeps this small: clients never merge with each other, and old tombstones
can be forgotten on a schedule (see [Persistence](#persistence)).

Clocks are hybrid logical clocks, so a replica whose wall clock runs slow
is pulled forward by whatever it receives. A clock that runs *fast* would
win every conflict and drag the server's clock with it, so the server
refuses any op stamped more than `maxSkew` (default five minutes) ahead of
its own time, telling the client the server's time. The client adopts it
as an offset, re-stamps its pending ops, and sends them again; nothing is
lost, and the app hears nothing about it.

## Offline

Local edits go into an **outbox** first, persisted through a storage
adapter (`localStorageOutbox` in browsers, `memoryOutbox` for tests or
throwaway sessions), and are sent when online. The same adapter caches
the **last state** next to the outbox, so a client restarted while offline,
or before its first snapshot has landed, starts from what it last saw with
its pending edits already applied (`db.restored` says so) rather than from
`initial`; the snapshot on reconnect then brings it up to date. Pass
`cache: false` to keep only the outbox.

There are two kinds of storage adapter. A **document** adapter
(`localStorageOutbox`, `memoryOutbox`) keeps the outbox as one small
document written with every op, and the state as another, written
debounced since it costs a serialization of everything; a restore replays
the outbox over the cached state, so a state a few ops behind still comes
up current. Fine for states up to a megabyte or so. A **row** adapter
keeps one row per leaf and one per pending op, so a batch costs the
leaves it touched and only a snapshot touches everything:
`indexedDBStorage(name)` for browsers (a far larger quota than
localStorage and no serialization on the main thread; it loads
asynchronously, so open the client with `await openClient({ ... })`), and
`sqliteClientStorage(file)` from `lazy-storage/client/sqlite` for a client
that runs in Bun, synchronous like the rest. Use one name or file per
store and per replica (a tab is a replica), and `destroy()` to drop an
IndexedDB database a closed tab left behind. Everything else about a
client works in Bun and Node as it does in a browser: the transport needs
a global `WebSocket` (or one passed in), and ids come from `crypto`.

On (re)connect the client sends the whole outbox in one `hello`, together
with the store version it last saw (`db.version`). The server merges the
ops and answers with a **delta**: the accepted diffs since that version,
in order, followed by corrections for whatever the hello's own ops lost.
A reconnect after a network blip therefore costs a few small messages,
and a reconnect that missed nothing costs one empty one. The server keeps
the last `deltaLog` accepted diffs (default 1000) for this, persisted by
the SQLite and memory adapters so a restart or a deploy still answers
with deltas, and falls back to a full snapshot when the log does not
reach back far enough, when the client's cached version belongs to
another life of the storage (storage wiped and re-seeded), or when one
of the hello's ops was refused. A snapshot is encoded straight from the
state and the encoding kept until the next accepted op, so a burst of
first connections pays for one encoding, not one per client. Edits made while the hello was
in flight are re-applied on top and sent. Nothing is lost on a reload
while offline: the replica id, sequence numbers, pending ops, and the
version are restored with the outbox, and the server ignores an op it
has already seen.

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
  any number of stores, keyed by `(store, path)`, WAL mode, the delta log
  kept alongside. `sqlite.store(id)` gives a store its adapter;
  `sqlite.ids()`, `sqlite.remove(id)`, and the raw `sqlite.db` are there
  for administration.
- `sqliteStorage(file)` from `lazy-storage/server/sqlite-node` — the same
  adapter on `node:sqlite` (Node 22.13 and later); the two read each
  other's files.
- `jsonFileStorage(file)` — one JSON document per store, written
  atomically and debounced, without the delta log (a restart answers the
  first reconnects with snapshots). Fine for small single-store deployments.
- `memoryStorage()` — nothing survives the process; for tests.

A custom adapter implements `load()`, `commit(change)`, and `flush()`;
`change` carries the rows an op won and dropped, the replica's progress,
the version and epoch, and optionally the accepted diff as a `log` entry
with the `logFloor` below which the store no longer needs entries. See
the header of `src/server/storage.js` for the exact shapes.

Two things would otherwise grow without bound: tombstones, and the
progress kept per replica (every browser tab that ever connected). A store
keeps both for its **retention window** (`retention`, default 30 days) and
forgets what is older, on load and then once an hour as ops arrive
(`store.compact()` does it on demand and reports what it removed). To make
that safe, an op stamped before the window is refused with code `expired`:
it might be a write to a record whose deletion has since been forgotten,
and would resurrect it. The client drops such an op and resyncs, and the
app can listen for the error to tell the user that a change made more than
a month ago offline could not be kept. `retention: Infinity` keeps
everything and accepts ops of any age.

An adapter records a replica's progress as `{ seq, seen }`, where `seen`
is the store's clock when its last op arrived, and the store's `epoch`, a
random id minted once per life of the storage (it is how a client's cached
version is told apart from one that belongs to storage since wiped). A
database from before 0.3.0 opens as is (SQLite gains the columns on open),
its replicas get a full window before they are pruned, and the store
mints its epoch on the first commit.

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

Opening a store is not the same as writing to it, so the store itself
decides what a client may write:

- `readOnly: ['team', 'tasks/*/createdAt']` names paths clients may not
  touch (same syntax as registers). An op with a leaf at or under one is
  refused whole, with code `forbidden`; the client drops it and resyncs,
  so the local state falls back in line. The server's own `store.patch` is
  the way to write there. Deleting a record whose *field* is read-only is
  allowed: the pattern protects the field, not the record.
- `validate(diff, { user, replicaId, store })` judges every client op that
  passed the read-only check. Return `false` or throw to refuse it (the
  error's message reaches the client), return a diff to accept *that*
  instead (a validator that strips fields the user may not set, say; the
  client is corrected on what was stripped, silently), or return `true` or
  nothing to let it through. It is synchronous and never sees the server's
  own writes.

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

## Limits, memory, and observability

A public server needs a few ceilings, all on by default:

- **Message size.** `maxPayload` on the Bun adapter (default 4 MB) is the
  largest message a socket may send; Bun ends a socket that exceeds it. A
  hello carries at most 1000 ops, the rest following as ordinary ops once
  the answer lands, so a long offline spell stays well under it.
- **Op size.** `maxLeaves` on the store (default 10 000) is the most leaves
  one op may touch; a larger one is refused with code `too-large`, and
  the client drops it and resyncs.
- **Op rate.** `rateLimit` on the store is a token bucket per replica,
  `{ burst: 500, perSecond: 100 }` by default. A live op beyond it is
  refused with code `rate-limited` and a `retryAfter` in milliseconds;
  the client keeps the op and resends its outbox in a hello after that,
  so nothing is lost and a runaway client is throttled rather than
  broken. Ops inside a hello are not counted. `false` turns it off.

**Memory follows the stores in use** when the registry is given an idle
time: `createStores(factory, { idle: 30 * 60_000 })` releases a store
that has had no session for that long (its storage is flushed first) and
loads it again on the next request. Off by default, since a store on
`memoryStorage` would lose its data.

**Watching a store.** `store.observe('op' | 'refused' | 'session', fn)`
reports every merged op (`{ replicaId, seq, user, accepted, rejected,
version }`), every client op turned away (`{ replicaId, seq, user, code,
message }`), and sessions opening and closing, for logs, audits, and
metrics; `store.stats()` counts version, sessions, replicas, rows,
tombstones, and the delta log. Server faults that are nobody's request
(a store factory that throws, an observer that throws, a bug while
handling a message) go to an `onError` option on `createHandlers`,
`createHub`, and `createStore`, which defaults to the console.

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

**Shutting down.** `await lazy.close()` (or `await server.shutdown()` on
what `serve` returns) refuses new sockets with 503, closes the open ones
with WebSocket code 1001 so clients reconnect at once instead of waiting
out a dead connection, and disposes the store registry, which flushes
every store's storage. With the delta log persisted, the reconnect to the
next process is a delta: a deploy costs each client a few small messages.
Wire it to the signals your host sends:

```js
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    await lazy.close();
    sqlite.close();
    process.exit(0);
  });
}
```

### On Node

The server runs on Node too, through the `ws` package (an optional peer
dependency: install it yourself). `lazy-storage/server/node` has the same
`serve` and `createHandlers`, with the same hooks, limits, and graceful
`close`; `authenticate` receives a Web `Request` built from the incoming
Node request, so one function serves both runtimes. Storage comes from
`lazy-storage/server/sqlite-node`, the same adapter on `node:sqlite`.

```js
import { once } from 'node:events';
import { createStore, createStores } from 'lazy-storage/server';
import { serve } from 'lazy-storage/server/node';
import { sqliteStorage } from 'lazy-storage/server/sqlite-node';

const sqlite = sqliteStorage('data/state.sqlite');
const stores = createStores(id => createStore({ initial, registers, storage: sqlite.store(id) }));
const server = serve({ stores, port: 3200, authenticate, authorize });
await once(server, 'listening');
```

To mount inside an http server you already have, handle its `upgrade`
event with `lazy.upgrade(req, socket, head)`, which resolves to false when
the URL is not lazy-storage's, and call `lazy.close()` on shutdown.

## Undo

Each client has a lazy-watch undo manager attached with a `record` filter
that declines remote batches, so undo and redo only ever touch this
replica's own edits, and they sync like any other edit. Because records
are keyed, a teammate's insert does not invalidate your history; only a
change of shape under a path you edited (a register's length, a record
replaced by a leaf) drops the affected steps.

## API

**Client** (`lazy-storage`)

- `createClient({ store, connection | transport, initial, registers, replicaId, storage, cache, undo, undoLimit, reconnect, now })`; `db.restored` — started from the cached state; `db.version` — the store version this client has seen everything up to
- `openClient(options)` → `Promise<db>` — the same, for a storage adapter whose `load()` returns a promise
- `createConnection({ transport, reconnect, keepalive })` → `connect()`, `close()`, `status`, `attached`, `on('status', fn)` — a socket shared by clients
- `db.state` — the mirror (a lazy-watch proxy). Read and write it directly
- `db.store`, `db.connection` — the store id and the connection
- `db.presence` — users with a live session on this store; `db.closed` — `{ code, message }` after the server ended this store for us, else null
- `db.collection(name)` — `add(record) → id`, `update(id, fields)`, `remove(id)`, `get(id)`, `has(id)`, `ids()`, `all()`
- `db.connect()`, `db.disconnect()`, `db.status` (`'offline' | 'connecting' | 'online'`), `db.pending`
- `db.watch(listener)` — state changes; `meta?.origin === 'remote'` marks the server's
- `db.on('status' | 'error' | 'sync' | 'presence' | 'closed', fn)` — lifecycle events; a refused op is an error with a `code` (`forbidden`, `expired`, `invalid`, `too-large` drop the op; `rate-limited` keeps it and retries; `clock-skew` is handled without one)
- `db.undo()`, `db.redo()`, `db.canUndo`, `db.canRedo`, `db.checkpoint()`, `db.group(fn)`, `db.clearHistory()`
- `webSocketTransport(url)`, `memoryOutbox()`, `localStorageOutbox(key)` — document adapters: `{ load(), save(outbox), saveState(cache) }`; without `saveState` the state rides inside `save`
- `indexedDBStorage(name, { onError })` → also `settled()`, `close()`, `destroy()` — a row adapter: `{ load(), commit({ puts, deletes, meta }), replace({ rows, meta }), saveOp(op, meta), dropOps(seq, meta) }`, where a delete removes the path and everything under it and `meta` is `{ replicaId, seq, version, epoch }`

**Bun client** (`lazy-storage/client/sqlite`): `sqliteClientStorage(file, { wal })` → a row adapter on bun:sqlite, plus `db` and `close()`.

**Server** (`lazy-storage/server`)

- `createStore({ initial, registers, readOnly, validate, maxSkew, retention, compactEvery, deltaLog, maxLeaves, rateLimit, storage, presenceKey, onError, now })`; `store.epoch` — this life of the storage
- `store.observe(event, fn)` → unsubscribe — `'op'`, `'refused'`, `'session'`; `store.stats()` → `{ version, epoch, sessions, replicas, rows, tombstones, log }`
- `store.session({ send, user, onEvict })` → `{ receive(message), close(), user, replicaId }` — one per connection, transport-agnostic
- `store.closeSessions(predicate, message)` — evict sessions; `store.presence()` — distinct users with a live session
- `store.patch(diff)` — a server-side change, timestamped and broadcast; `store.apply(op)` — a trusted op, gates skipped
- `store.on(listener)`, `store.snapshot()`, `store.state`, `store.version`, `store.replicas`
- `store.compact()` → `{ tombstones, replicas }` removed; `store.compactTombstones(olderThan)`, `store.flush()`, `store.dispose()`
- `memoryStorage()`, `jsonFileStorage(path, { debounce })`
- `createStores(factory, { idle, sweepEvery, now })` → `get(id)`, `has(id)`, `ids()`, `release(id)`, `sweep()`, `dispose()`; `isStoreId(id)`
- `createHub(resolveStore, { send, user, authorize, onError })` → `{ receive(message), close(), stores, user }` — the server side of a multiplexed connection, session-shaped
- `toJSON(message)` — a message's JSON, encoded once however many sockets it goes to; use it in a transport of your own so a broadcast is not re-encoded per socket (`tagStore(message, id)` is what a hub does)

**SQLite** (`lazy-storage/server/sqlite`, Bun): `sqliteStorage(file, { wal })` →
`store(id)`, `ids()`, `remove(id)`, `db`, `close()`.

**Bun adapter** (`lazy-storage/server/bun`): `serve({ stores, port, path, fetch, authenticate, authorize, maxPayload, onError })` —
`stores` is a registry or `id => store|null` (for one store, `() => store`); the
hub listens at `path`; the returned server gains `shutdown({ reason })`.
`createHandlers({ stores, path, authenticate, authorize, maxPayload, onError })` →
`{ upgrade(req, server), websocket, close({ reason }), closing }` for mounting inside your own `Bun.serve`.

**Node adapter** (`lazy-storage/server/node`, needs `ws`): `serve({ stores, port, host, request, path, authenticate, authorize, maxPayload, onError })` →
an `http.Server` with `shutdown({ reason })`; `createHandlers(options)` → `{ upgrade(req, socket, head), close({ reason }), closing, wss }`;
`toRequest(req)` — the Web `Request` `authenticate` sees. **node:sqlite** (`lazy-storage/server/sqlite-node`): `sqliteStorage(file, { wal })`, as the Bun one.

**Types**: declarations ship with the package for every entry (`types/`);
the Node entry's need `@types/node`.

**Core** (`lazy-storage/core`): the pieces both sides share — `createClock`,
`compareTs`, `mergeOp`, `leaves`, `expandRegisters`, `rebuild`, `registerSet`,
`compactTombstones`, `randomId`.

## Wire protocol

Messages are JSON objects named by a `t` field. Any number of stores
travel over one socket, so every message except `ping` and `pong` also
carries `store`, the store id; `leave` closes one store's session and
keeps the socket for the others. The shapes are declared as
`ClientMessage` and `ServerMessage` in the typings.

An **op** is one client batch:

```
{ replicaId, seq, ts, diff }
```

`seq` counts the replica's ops from 1, and the server ignores one it has
already merged, so a resend is safe. `ts` is a hybrid-logical-clock
timestamp `[ms, count, replicaId]`. `diff` is a plain lazy-watch diff:
nested objects, `null` for a deletion, arrays only at register paths.

### A session, in order

1. The client opens a store with `hello`: its whole outbox, and where its
   knowledge of the store ends.
2. The server merges those ops, then answers with a `snapshot` or a
   `delta` (chosen as described under [Offline](#offline)). Either one
   acknowledges the hello's ops through `seq`.
3. From then on each client batch goes out as an `op`. The server answers
   every op with an `ack` or an `error`, and broadcasts every accepted
   diff as a `patch` to every session on the store, the sender included.
4. `presence` arrives whenever the users with a live session change, and
   a `closed` message ends the store for this client.

### Client to server

| Message | Fields | Meaning |
|---|---|---|
| `hello` | `replicaId`, `ops`, `since?`, `epoch?` | Connect or reconnect. `ops` is the outbox (its first 1000 ops); `since` is the store version the client has seen everything up to and `epoch` the storage life it belongs to, asking for a delta |
| `op` | `op` | One batch, live |
| `leave` | | Close this store's session; the socket stays up |
| `ping` | | Keepalive; answered with `pong` |

### Server to client

| Message | Fields | Meaning |
|---|---|---|
| `snapshot` | `state`, `ts`, `seq`, `registers`, `v`, `epoch` | The whole state, to overwrite with |
| `delta` | `patches`, `ts`, `seq`, `registers`, `v`, `epoch` | The accepted diffs since the client's `since`, in order, followed by corrections for what the hello's own ops lost; applied as patches |
| `patch` | `diff`, `ts`, `v` | An accepted diff from any replica, and the version it made |
| `ack` | `seq`, `ts`, `correction` | The op was merged; `correction` is a diff with the server's values at the leaves it lost, or null |
| `error` | `seq?`, `code?`, `message`, `now?`, `ts?`, `retryAfter?` | With `seq`: that op was refused, for the reason in `code` (below). Without: the message itself was bad (not JSON, an unknown type, a hello without a replica id) |
| `presence` | `users` | The distinct users with a live session |
| `closed` | `code`, `message` | Final for this store on this socket; the client goes offline for it and does not reconnect on its own |
| `pong` | | |

Across these: `ts` is the server's clock on `snapshot`, `delta`, and
`ack`, and the op's own timestamp on `patch`; `seq` on `snapshot` and
`delta` is the last op of this replica the server holds, so the client
can drop acknowledged outbox entries; `registers` are the server's
register patterns, for the client to check against its own; `v` is the
store version the message brings the client up to, and `epoch` the life
of the storage it counts in.

### Refusal codes

An `error` with a `seq` names one of these, and the client acts on it
without help from the app, which only hears an `error` event:

| Code | Why | What the client does |
|---|---|---|
| `invalid` | The op breaks the model: an array outside a register, a malformed op | Drops the op and resyncs from a snapshot |
| `forbidden` | A leaf under a read-only path, or `validate` refused | Same |
| `expired` | Stamped before the retention window | Same |
| `too-large` | More leaves than `maxLeaves` | Same |
| `rate-limited` | Beyond the replica's token bucket; `retryAfter` says how long in ms | Keeps the op and resends its outbox in a hello after `retryAfter` |
| `clock-skew` | Stamped more than `maxSkew` ahead of the server's clock; `now` is the server's time, `ts` the refused stamp | Adopts the server's time, re-stamps the pending ops, sends them again; no error reaches the app |

### Closed codes

`evicted` (`closeSessions` on the server), `forbidden` (`authorize`
refused the store), `unknown-store` (the resolver returned null, or the
store factory threw), and `invalid-store` (an id outside the allowed
alphabet, or a message without one).

## Scope

This is last-writer-wins per field with a single merge point. It gives
records identity and makes offline editing safe; it does not merge
concurrent edits to the *same* field (the later one wins) and it is not a
text CRDT. For collaborative documents, embed a purpose-built library for
the document and keep the surrounding state here.

Authentication is a hook, not a system: lazy-storage asks you who a request
is and whether they may open a store, and stores the answer on the session.
Users, tokens, and memberships stay in your application.

## Examples and benchmark

`examples/` holds small, complete programs that run from a checkout: a
shared list in the browser over a Bun server (`bun examples/basic/server.js`),
the same page served by Node (`node examples/node/server.js`), and a
client that follows a store from a Bun or Node process
(`bun examples/mirror.js`). See `examples/README.md`.

`npm run bench` times the paths that matter: the merge with and without
the gates, a broadcast to a hundred and a thousand sockets, a client's
local op on each kind of storage, and a reconnect answered with a
snapshot versus a delta. It reports the median of several rounds; compare
runs on the same machine.

## Testing

```bash
npm test          # unit and integration tests plus a fixed-seed convergence run (Node)
npm run test:bun  # the bun:sqlite adapter (Bun)
npm run fuzz      # a longer randomized convergence campaign; a failure prints the seed
```
