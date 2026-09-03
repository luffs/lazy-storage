# lazy-storage

A self-hosted, realtime state store with offline sync, built on
[lazy-watch](https://github.com/luffs/lazy-watch). Think of it as the
"database + sync" half of a Firebase-style backend for a whole application
state tree: clients hold a live mirror they read and write like a plain
object, edits sync as small JSON diffs, work continues offline, and
everything converges when the connection is back.

```js
import { createClient, webSocketTransport, localStorageOutbox } from 'lazy-storage';

const db = createClient({
  transport: webSocketTransport('wss://example.com/ws'),
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
serve({ stores, port: 3200 }); // clients connect to ws://host:3200/ws/<storeId>
```

For a single store, `createStore` with `jsonFileStorage('data/todo.json')`
and `serve({ store })` serves it at `/ws`.

## The model

State is a tree of plain objects and JSON leaves, and **lists are objects
keyed by id**. That single rule is what makes offline edits mergeable: a
change addresses a record by identity, so two people adding, editing, or
removing different records never collide, and an edit made on a stale
replica still lands on the record it meant.

Arrays are allowed only at declared **registers**: paths whose value is one
unit, written wholesale and resolved as a whole (an ordering of ids, a set
of tags). Anywhere else an array write is reverted on the client and
reported through the `error` event, and refused by the server.

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
throwaway sessions), and are sent when online. On (re)connect the client
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
URL- and filename-safe alphabet (`isStoreId`), so an id can name a path
segment or a table key without escaping. The Bun adapter routes
`/ws/<storeId>` to the registry; the id lives in the URL rather than in
the protocol so an authenticating wrapper can look at it before a session
exists.

## Undo

Each client has a lazy-watch undo manager attached with a `record` filter
that declines remote batches, so undo and redo only ever touch this
replica's own edits, and they sync like any other edit. Because records
are keyed, a teammate's insert does not invalidate your history; only a
change of shape under a path you edited (a register's length, a record
replaced by a leaf) drops the affected steps.

## API

**Client** (`lazy-storage`)

- `createClient({ transport, initial, registers, replicaId, storage, undo, undoLimit, reconnect, now })`
- `db.state` — the mirror (a lazy-watch proxy). Read and write it directly
- `db.collection(name)` — `add(record) → id`, `update(id, fields)`, `remove(id)`, `get(id)`, `has(id)`, `ids()`, `all()`
- `db.connect()`, `db.disconnect()`, `db.status` (`'offline' | 'connecting' | 'online'`), `db.pending`
- `db.watch(listener)` — state changes; `meta?.origin === 'remote'` marks the server's
- `db.on('status' | 'error' | 'sync', fn)` — lifecycle events
- `db.undo()`, `db.redo()`, `db.canUndo`, `db.canRedo`, `db.checkpoint()`, `db.group(fn)`, `db.clearHistory()`
- `webSocketTransport(url)`, `memoryOutbox()`, `localStorageOutbox(key)`

**Server** (`lazy-storage/server`)

- `createStore({ initial, registers, storage, now })`
- `store.session({ send })` → `{ receive(message), close() }` — one per connection, transport-agnostic
- `store.patch(diff)` — a server-side change, timestamped and broadcast
- `store.on(listener)`, `store.snapshot()`, `store.state`, `store.version`
- `store.compactTombstones(olderThan)`, `store.flush()`, `store.dispose()`
- `memoryStorage()`, `jsonFileStorage(path, { debounce })`
- `createStores(factory)` → `get(id)`, `has(id)`, `ids()`, `release(id)`, `dispose()`; `isStoreId(id)`

**SQLite** (`lazy-storage/server/sqlite`, Bun): `sqliteStorage(file, { wal })` →
`store(id)`, `ids()`, `remove(id)`, `db`, `close()`.

**Bun adapter** (`lazy-storage/server/bun`): `serve({ store, stores, port, path, fetch })` —
a single `store` at `path`, or `stores` (a registry or `id => store|null`) at `path/<id>`.

**Core** (`lazy-storage/core`): the pieces both sides share — `createClock`,
`compareTs`, `mergeOp`, `leaves`, `expandRegisters`, `registerSet`,
`compactTombstones`, `randomId`.

## Wire protocol

Client to server: `{ t: 'hello', replicaId, ops }`, `{ t: 'op', op }`,
`{ t: 'ping' }`, where `op = { replicaId, seq, ts, diff }` and `diff` is a
plain lazy-watch diff.

Server to client: `{ t: 'snapshot', state, ts, version, seq }`,
`{ t: 'patch', diff, ts, version }`, `{ t: 'ack', seq, ts, correction }`,
`{ t: 'error', seq?, message }`, `{ t: 'pong' }`.

## Scope

This is last-writer-wins per field with a single merge point. It gives
records identity and makes offline editing safe; it does not merge
concurrent edits to the *same* field (the later one wins) and it is not a
text CRDT. For collaborative documents, embed a purpose-built library for
the document and keep the surrounding state here.

Authentication and per-user permissions are not part of this version; the
Bun adapter takes a `fetch` handler for anything beside the socket, and
`store.session` is transport-agnostic so an authenticated wrapper can
decide who gets a session on which store.

## Testing

```bash
npm test          # unit and integration tests plus a fixed-seed convergence run (Node)
npm run test:bun  # the bun:sqlite adapter (Bun)
npm run fuzz      # a longer randomized convergence campaign; a failure prints the seed
```
