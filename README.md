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
  initial: { tasks: [] },
  lists: ['tasks'],                       // an ordered list of records, seen as an array
  storage: localStorageOutbox('todo')
});
db.connect();

db.state.tasks.push({ title: 'Ship it', done: false }); // synced, offline or not
db.state.tasks[0].done = true;
db.state.tasks.sort((a, b) => a.title.localeCompare(b.title));

db.watch((diff, inverse, meta) => render(db.state, meta?.origin === 'remote'));
db.undo();
```

```js
import { createStore, createStores } from 'lazy-storage/server';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { serve } from 'lazy-storage/server/bun';

// Any number of stores in one SQLite file, one row per leaf. The server
// knows nothing of arrays: a list is a keyed map with a position per record
const sqlite = sqliteStorage('data/app.sqlite');
const stores = createStores(id => createStore({
  initial: { tasks: {} },
  storage: sqlite.store(id)
}));
serve({
  stores,
  port: 3200,
  authenticate: req => userForToken(new URL(req.url).searchParams.get('token')), // null → closed 'unauthorized'
  authorize: (user, storeId) => user.teams.includes(storeId)                     // false → closed 'forbidden'
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

Arrays are **whole values**. An array of primitives (a set of tags, a list
of ids, a pair of coordinates) may live anywhere; it is written and merged
as one leaf, so the newer array wins as a unit. An array holding objects
is refused, on the client (reverted and reported through the `error`
event) and on the server, because a list of records with unit semantics
would lose one person's add whenever another person reordered. A list of
records is a keyed map with a position on each record, below. The
`registers` option remains for storing an array of anything as one value
on purpose: a `*` segment matches any one segment, so `tasks/*/subtaskOrder`
declares one register per task, and a client whose declaration differs
from the server's is told so.

## Lists

An ordered list of records is a keyed map whose records each carry a
**position key**, and `db.list(path)` is its ordered view:

```js
const tasks = db.list('tasks');                    // state.tasks, an object keyed by id
tasks.all()                                        // records sorted by position, ties by id
const id = tasks.add({ title: 'Milk' });           // at the end; or { before: id }, { after: id }, { at: index }
tasks.move(id, { at: 0 });
tasks.remove(id);
db.list(['tasks', id, 'subtasks']).add({ title: 'Skimmed' });   // nested lists are just paths
```

An add or a move writes one field, `pos`, on one record, and nothing else
in the list changes. So two people inserting at the same spot while apart
both keep their records, in the same order everywhere (ties fall to the
id); a move never loses to an unrelated edit; and a deletion is just the
record's deletion. The server needs no declaration and holds no order
register. Keys are short strings that sort as strings: appending counts
up (`a0`, `a1`, ... `az`, `b00`), and only inserting between two existing
keys lengthens one, by a character every few inserts at the very same
spot.

An app that keeps its own array (a drag-and-drop list, a Vue store) hands
the order back with `tasks.reconcile(ids)`, which writes the fewest
positions that make the sort agree: the longest run of records already in
order keeps its keys. Records without a position sort last until placed.

### Plain arrays

Declare the list paths and the client presents them as arrays:

```js
const db = createClient({ connection, store: 'team-1', initial: { tasks: [] }, lists: ['tasks', 'tasks/*/subtasks'] });
db.state.tasks.push({ title: 'Milk' });             // an id is minted and written back
db.state.tasks.splice(1, 0, { title: 'Eggs' });
db.state.tasks[0].done = true;
db.state.tasks[0].subtasks = [{ title: 'Skimmed' }];
db.state.tasks = db.state.tasks.filter(t => !t.done); // or sort, reverse, reorder
```

`db.state` is then a view: the same tree with every list as a real array
in position order, records carrying their `id` and no position. The synced
state underneath, `db.wire`, keeps the keyed maps with positions, and
everything else (persistence, deltas, undo, `db.list`) works on it as
before. The client keeps the two in step: an edit inside a record maps its
index to the record's id; a splice, a reorder, or a replaced array resyncs
that list by id, deleting what left, adding what appeared, and writing the
fewest positions that make the wire order match. Changes from others
arrive as splices at their sorted place, moves, and field patches, tagged
`origin: 'remote'` for the app's listeners. A record pushed without an id
gets one in the next batch; read it back from the array rather than from
the object you pushed. A list path cannot also be a register.

### With a UI framework

Two entries wrap the client for Vue and React; the pattern behind each
is a few lines for any other framework.

`lazy-storage/vue` keeps a reactive mirror. Vue cannot track `db.state`
(a remote patch lands underneath any proxy Vue wraps around it, and
nothing re-renders), so `useClient(db)` returns a plain copy kept
reactive and patched in place on every batch, local or remote, together
with refs for the client's status, presence, outbox size, closed reason,
and undo state:

```js
import { useClient } from 'lazy-storage/vue';

const { state, status, presence, canUndo } = useClient(db);   // in setup()
db.state.tasks.push({ title: 'Ship it' });                    // writes go to the client; the mirror follows
```

In the Options API the same call fills `data()`
(`data() { return { ...useClient(this.db), title: '' } }`). Either way
the listeners stop with the component.

`lazy-storage/react` needs no mirror: `useClient(db)` subscribes through
`useSyncExternalStore`, so a component reads `db.state` directly and
re-renders on every batch, outbox change, and event, with one
subscription per client however many components use it:

```jsx
import { useClient } from 'lazy-storage/react';

function List({ db }) {
  const { state, status } = useClient(db);
  return <ul>{state.tasks.map(task => <li key={task.id}>{task.title}</li>)}</ul>;
}
```

An app that keeps its own arrays instead, as a Vue store or a
drag-and-drop list does, overwrites the view's arrays with them after a
change (`LazyWatch.overwrite(db.state.tasks, plainTasks)`) and copies them
back on remote, undo, and redo batches; lazy-storage diffs the arrays by
id and writes only what changed. The todo app at
github.com/luffs/kimi-wa-best-todo is that shape in full.

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
- An **array** is one leaf, whether an array of primitives anywhere or a
  declared register: the newest whole value wins. A list of records is
  not an array on the wire (see [Lists](#lists)), so its records merge
  one by one.

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

The outbox holds one op per batch, minus what later batches made moot: a
new op takes over from whatever older pending ops wrote at or under the
paths it writes, since under last-writer-wins those older writes could
never decide a value again. Typing into one field keeps one op pending
rather than one per keystroke, and deleting a record drops its pending
edits. Nothing is re-stamped, so an older write to another field keeps
its own time and the merge decides exactly as if every op had been sent.

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
version is told apart from one that belongs to storage since wiped).

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
  that). `null` or `undefined` turns the request away. A plain request
  gets a 401; a WebSocket handshake is completed only to be told so, since
  a browser cannot read the status of a refused one: the socket receives a
  `closed` message with code `unauthorized` and closes with code 4401.
  The client then stops reconnecting, since retrying an expired token
  would only fail again, and reports it on every store attached:
  `db.closed` is `{ code: 'unauthorized', message }`, `db.on('closed')`
  fires, and `connection.closed` holds the same. Once the app has signed
  in again, `db.connect()` (or `connection.connect()`) is the way back:
  the transport factory runs afresh, so a URL built by a function carries
  the new token.
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

- **Presence.** Off by default: sessions do not learn of each other.
  `createStore({ presence: true })` turns it on, and the store then
  tells every session who else is there; `db.presence` holds the
  distinct users with a live session, `db.on('presence', users => ...)`
  follows it, and `store.presence()` answers on the server. A session
  counts from its hello on. Instead of `true`, an object sets what
  presence does: `key(user)` is what users are distinct by (default
  `id`, else the user's JSON, so a user on two devices counts once);
  `user(user)` is what of a user its peers see, and the default is all
  of it, so a server whose `authenticate` returns roles or tokens should
  pick the public fields here; `validate`, `every`, and `maxShare` are
  below. A client that never reads presence, a mirror say, passes
  `presence: false` to `createClient`: its hello says so and the server
  sends it none of this, though it may still share.
- **Peers.** With presence on, every live session is a peer,
  `{ replicaId, user, key, data }`, where `data` is whatever that client
  chose to share: `db.share({ editing: taskId })` sets it, `null` clears
  it, and the hello carries it, so a reconnect restores it. Shared data
  is never written to the store; it lives as long as the session and
  goes out as it changes (throttle a cursor yourself: a share draws on
  the replica's `rateLimit` bucket like an op, and beyond it is refused
  with `rate-limited`, the client's next hello carrying its latest value
  instead). It must be JSON
  within `presence.maxShare` bytes (default 4096), and
  `presence.validate(data, { user, replicaId, store })` judges it the
  way `validate` judges an op: return `false` or throw to refuse it
  (the client hears an `error` with code `forbidden`), return a value to
  share that instead (strip a field, stamp in the user's id so a peer
  cannot claim to be someone else), return `true` or nothing to let it
  through. `db.peers` holds the list, this client's own entry included
  (its `replicaId` is `db.replicaId`), `db.on('peers', ...)` follows it,
  and `store.peers()` answers on the server. "Ann is editing this task"
  is a filter over it. A change costs every session one small message:
  the whole list goes only to a session that has just said hello, and
  after that only what changed (a peer arriving, leaving, or sharing
  anew), batched per turn of the server's event loop. `presence.every`
  sends at most one such message per that many milliseconds, changes
  within the window going out together and a session's later share
  replacing its earlier one, so a busy room or a deploy's reconnect
  storm costs each socket a message per window rather than one per
  change.
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
- **Compression.** Both adapters offer the permessage-deflate extension
  by default, and a client that takes it (browsers do) receives large
  messages compressed: the JSON of a big store shrinks about tenfold
  (`npm run bench` reports by how much), which is what a client waits on
  at connect and reconnect. Messages under `threshold` bytes (default
  1024, set through the object form of `perMessageDeflate`) go plain,
  since compressing a hundred-byte patch costs ten times its encoding
  and makes it larger. What it costs: about 3 ms of CPU per megabyte of
  snapshot per client, paid per socket, unlike the encoding, which is
  done once, so a reconnect storm of a thousand clients spends a few
  seconds compressing (and sends a tenth of the bytes); and on the Node
  adapter, a socket that has received a compressed message keeps a
  deflate stream of some 75 KB for its lifetime, one that has sent one
  an inflate stream of some 100 KB. `perMessageDeflate: false` turns it
  off.
- **Op size.** `maxLeaves` on the store (default 10 000) is the most leaves
  one op may touch; a larger one is refused with code `too-large`, and
  the client drops it and resyncs.
- **Op rate.** `rateLimit` on the store is a token bucket per replica,
  `{ burst: 500, perSecond: 100 }` by default. A live op beyond it is
  refused with code `rate-limited` and a `retryAfter` in milliseconds;
  the client keeps the op and resends its outbox in a hello after that,
  so nothing is lost and a runaway client is throttled rather than
  broken. Ops inside a hello are not counted. `false` turns it off.
- **Presence rate.** Presence is off unless a store asks for it. On, it
  travels as deltas, one small message to every session per change,
  batched per turn of the event loop; `presence.every` (milliseconds,
  default 0) caps that at one message per window, `presence.maxShare`
  (default 4096 bytes of JSON) bounds what a session may share, and a
  client that never reads presence opts out of receiving it. A room of
  N sessions then costs N small messages per window however many of
  them join, leave, or share.

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
tombstones, and the delta log, and `stores.stats()` on a registry rolls
those up across the live stores, with how many are live and how many
idle, for a health endpoint. Server faults that are nobody's request
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
const stores = createStores(id => createStore({ initial, storage: sqlite.store(id) }));
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
change of shape under a path you edited (an array replaced whole, a record
replaced by a leaf) drops the affected steps. With lists declared, undo
runs on the synced state and shows in the array view like any other change.

## API

**Client** (`lazy-storage`)

- `createClient({ store, connection | transport, initial, registers, lists, position, replicaId, storage, cache, undo, undoLimit, reconnect, presence, now })`; `db.restored` — started from the cached state; `db.version` — the store version this client has seen everything up to; `db.wire` — the synced state under a lists view
- `openClient(options)` → `Promise<db>` — the same, for a storage adapter whose `load()` returns a promise
- `createConnection({ transport, reconnect, keepalive })` → `connect()`, `close()`, `status`, `attached`, `closed` — why the server turned the socket away, or null — `on('status' | 'closed', fn)` — a socket shared by clients
- `db.state` — the mirror (a lazy-watch proxy). Read and write it directly
- `db.store`, `db.connection` — the store id and the connection
- `db.presence` — users with a live session on this store; `db.peers` — every live session as `{ replicaId, user, data }`, this client's own included; `db.share(data)` — what this client shares with them (JSON, `null` clears), read back as `db.shared`; `db.closed` — `{ code, message }` after the server ended this store for us, else null
- `db.collection(name)` — `add(record) → id`, `update(id, fields)`, `remove(id)`, `get(id)`, `has(id)`, `ids()`, `all()`
- `db.list(path, { position })` — an ordered list of records: `all()`, `ids()`, `get(id)`, `has(id)`, `add(record, where) → id`, `move(id, where)`, `remove(id)`, `reconcile(ids) → written`, `keyFor(where)`; `where` is `{ before }`, `{ after }`, `{ at }`, or nothing for the end
- `db.connect()`, `db.disconnect()`, `db.status` (`'offline' | 'connecting' | 'online'`), `db.pending`
- `db.watch(listener)` — state changes; `meta?.origin === 'remote'` marks the server's
- `db.on('status' | 'error' | 'sync' | 'presence' | 'peers' | 'closed' | 'history', fn)` — lifecycle events; a refused op is an error with a `code` (`forbidden`, `expired`, `invalid`, `too-large` drop the op; `rate-limited` keeps it and retries; `clock-skew` is handled without one); `history` carries `{ canUndo, canRedo }` after a local batch, an undo, a redo, or `clearHistory()`
- `db.undo()`, `db.redo()`, `db.canUndo`, `db.canRedo`, `db.checkpoint()`, `db.group(fn)`, `db.clearHistory()`
- `webSocketTransport(url)`, `memoryOutbox()`, `localStorageOutbox(key)` — document adapters: `{ load(), save(outbox), saveState(cache) }`
- `indexedDBStorage(name, { onError })` → also `settled()`, `close()`, `destroy()` — a row adapter: `{ load(), commit({ puts, deletes, meta }), replace({ rows, meta }), saveOp(op, meta), removeOp(seq, meta), dropOps(seq, meta) }`, where a delete removes the path and everything under it, `saveOp` also rewrites an op a newer one pruned, `removeOp` takes out one a newer op emptied, and `meta` is `{ replicaId, seq, version, epoch }`

**Bun client** (`lazy-storage/client/sqlite`): `sqliteClientStorage(file, { wal })` → a row adapter on bun:sqlite, plus `db` and `close()`.

**Vue** (`lazy-storage/vue`): `useClient(db)` → `{ state, status, presence, pending, closed, canUndo, canRedo, restored, stop }` — `state` a reactive mirror patched on every batch, the rest shallow refs; stops with the current effect scope (a component's), else by `stop()`.

**React** (`lazy-storage/react`): `useClient(db)` → `{ state, status, presence, pending, closed, canUndo, canRedo, restored }`, a new object per change, through `useSyncExternalStore`; `trackClient(db)` → the `{ subscribe, getSnapshot }` pair underneath, one per client.

**Testing** (`lazy-storage/testing`): `createNetwork(store | { session })` → `client(options, { user })`, `link({ user })` → `{ factory, goOffline(), goOnline() }`, `settle()`, `pending` — clients and a store linked in memory, see [Testing](#testing); `fakeTime(start)` → a clock with `advance(ms)` and `set(ms)`.

**Server** (`lazy-storage/server`)

- `createStore({ initial, registers, readOnly, validate, maxSkew, retention, compactEvery, deltaLog, maxLeaves, rateLimit, storage, presence, onError, now })` — `presence` is `false` (default), `true`, or `{ key, user, validate, every, maxShare }`; `store.epoch` — this life of the storage
- `store.observe(event, fn)` → unsubscribe — `'op'`, `'refused'`, `'session'`; `store.stats()` → `{ version, epoch, sessions, replicas, rows, tombstones, log }`
- `store.session({ send, user, onEvict })` → `{ receive(message), close(), user, replicaId }` — one per connection, transport-agnostic
- `store.closeSessions(predicate, message)` — evict sessions; `store.presence()` — distinct users with a live session; `store.peers()` — every live session as `{ replicaId, user, data }`
- `store.patch(diff)` — a server-side change, timestamped and broadcast; `store.apply(op)` — a trusted op, gates skipped
- `store.on(listener)`, `store.snapshot()`, `store.state`, `store.version`, `store.replicas`
- `store.compact()` → `{ tombstones, replicas }` removed; `store.flush()`, `store.dispose()`
- `memoryStorage()`, `jsonFileStorage(path, { debounce })`
- `createStores(factory, { idle, sweepEvery, now })` → `get(id)`, `has(id)`, `ids()`, `stats()` — the live stores' stats rolled up: `{ stores, idle, sessions, replicas, rows, tombstones, log }` — `release(id)`, `sweep()`, `dispose()`; `isStoreId(id)`
- `createHub(resolveStore, { send, user, authorize, onError })` → `{ receive(message), close(), stores, user }` — the server side of a multiplexed connection, session-shaped
- `toJSON(message)` — a message's JSON, encoded once however many sockets it goes to; use it in a transport of your own so a broadcast is not re-encoded per socket (`tagStore(message, id)` is what a hub does)

**SQLite** (`lazy-storage/server/sqlite`, Bun): `sqliteStorage(file, { wal })` →
`store(id)`, `ids()`, `remove(id)`, `db`, `close()`.

**Bun adapter** (`lazy-storage/server/bun`): `serve({ stores, port, path, fetch, authenticate, authorize, maxPayload, perMessageDeflate, onError })` —
`stores` is a registry or `id => store|null` (for one store, `() => store`); the
hub listens at `path`; the returned server gains `shutdown({ reason })`.
`createHandlers({ stores, path, authenticate, authorize, maxPayload, perMessageDeflate, onError })` →
`{ upgrade(req, server), websocket, close({ reason }), closing }` for mounting inside your own `Bun.serve`.

**Node adapter** (`lazy-storage/server/node`, needs `ws`): `serve({ stores, port, host, request, path, authenticate, authorize, maxPayload, perMessageDeflate, onError })` →
an `http.Server` with `shutdown({ reason })`; `createHandlers(options)` → `{ upgrade(req, socket, head), close({ reason }), closing, wss }`;
`toRequest(req)` — the Web `Request` `authenticate` sees. **node:sqlite** (`lazy-storage/server/sqlite-node`): `sqliteStorage(file, { wal })`, as the Bun one.

**Types**: declarations ship with the package for every entry (`types/`);
the Node entry's need `@types/node`.

**Core** (`lazy-storage/core`): the pieces both sides share — `createClock`,
`compareTs`, `mergeOp`, `leaves`, `expandRegisters`, `rebuild`, `registerSet`,
`compactTombstones`, `randomId`, and the position keys: `keyBetween(a, b)`,
`keysBetween(a, b, n)`, `comparePositions`, `isPositionKey`.

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
nested objects, `null` for a deletion, and arrays as whole values (of
primitives anywhere, of anything at a register path).

### A session, in order

1. The client opens a store with `hello`: its whole outbox, and where its
   knowledge of the store ends.
2. The server merges those ops, then answers with a `snapshot` or a
   `delta` (chosen as described under [Offline](#offline)). Either one
   acknowledges the hello's ops through `seq`.
3. From then on each client batch goes out as an `op`. The server answers
   every op with an `ack` or an `error`, and broadcasts every accepted
   diff as a `patch` to every session on the store, the sender included.
4. `presence` arrives whenever a session says hello, ends, or shares
   something new, and a `closed` message ends the store for this client. A `closed` without
   a `store` ends the socket itself: the request did not authenticate.

### Client to server

| Message | Fields | Meaning |
|---|---|---|
| `hello` | `replicaId`, `ops`, `since?`, `epoch?`, `share?` | Connect or reconnect. `ops` is the outbox (its first 1000 ops); `since` is the store version the client has seen everything up to and `epoch` the storage life it belongs to, asking for a delta; `share` is what this client shares with its peers, restored on reconnect; `presence: false` asks not to be sent presence |
| `op` | `op` | One batch, live |
| `leave` | | Close this store's session; the socket stays up |
| `share` | `data` | What this session shares with every peer, a JSON value within `presence.maxShare` that `presence.validate` lets through; `null` clears it. Presence goes out again if it changed; refused with `forbidden` while the store's presence is off |
| `ping` | | Keepalive; answered with `pong` |

### Server to client

| Message | Fields | Meaning |
|---|---|---|
| `snapshot` | `state`, `ts`, `seq`, `registers`, `v`, `epoch` | The whole state, to overwrite with |
| `delta` | `patches`, `ts`, `seq`, `registers`, `v`, `epoch` | The accepted diffs since the client's `since`, in order, followed by corrections for what the hello's own ops lost; applied as patches |
| `patch` | `diff`, `ts`, `v` | An accepted diff from any replica, and the version it made |
| `ack` | `seq`, `ts`, `correction` | The op was merged; `correction` is a diff with the server's values at the leaves it lost, or null |
| `error` | `seq?`, `code?`, `message`, `now?`, `ts?`, `retryAfter?` | With `seq`: that op was refused, for the reason in `code` (below). Without: the message itself was bad (not JSON, an unknown type, a hello without a replica id) |
| `presence` | `peers`, or `left?`, `joined?`, `shared?` | With `peers`: every session as `{ replicaId, user, key, data }`, `key` being what presence groups users by and `data` what the session shares; sent right after the hello is answered. Otherwise a delta, applied in the order `left` (replica ids), `joined` (peers), `shared` (`{ replicaId, data }`), batched per `presence.every`. Only with the store's presence on, and never to a session whose hello opted out |
| `closed` | `code`, `message` | Final for this store on this socket; the client goes offline for it and does not reconnect on its own. Without a `store`: final for the socket, which the server then closes with code 4401; the connection stops reconnecting and every client on it reports the reason |
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
| `invalid` | The op breaks the model: an array of objects outside a register, an array fragment, a malformed op | Drops the op and resyncs from a snapshot |
| `forbidden` | A leaf under a read-only path, or `validate` refused | Same |
| `expired` | Stamped before the retention window | Same |
| `too-large` | More leaves than `maxLeaves` | Same |
| `rate-limited` | Beyond the replica's token bucket; `retryAfter` says how long in ms | Keeps the op and resends its outbox in a hello after `retryAfter` |
| `clock-skew` | Stamped more than `maxSkew` ahead of the server's clock; `now` is the server's time, `ts` the refused stamp | Adopts the server's time, re-stamps the pending ops, sends them again; no error reaches the app |

### Closed codes

`evicted` (`closeSessions` on the server), `forbidden` (`authorize`
refused the store), `unknown-store` (the resolver returned null, or the
store factory threw), and `invalid-store` (an id outside the allowed
alphabet, or a message without one) each end one store. `unauthorized`
(`authenticate` returned nothing) ends the socket: it arrives without a
`store`, and the close that follows carries code 4401 in case the message
did not make it.

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

The in-memory network the suite runs on is published as
`lazy-storage/testing`, for an app's own tests: `createNetwork(store)`
(or a hub factory) links clients to a store without sockets, `net.client(
options, { user })` gives a connected client with `client.link.goOffline()`
and `goOnline()`, and `await net.settle()` delivers everything queued
until the network is quiet, so a test reads "edit, settle, assert" and
runs in milliseconds. `fakeTime()` is a wall clock to hand a store and
its clients as `now`, for tests that decide who wrote later:

```js
import { createStore } from 'lazy-storage/server';
import { createNetwork } from 'lazy-storage/testing';

const store = createStore({ initial: { tasks: {} } });
const net = createNetwork(store);
const a = net.client({ replicaId: 'a', initial: { tasks: {} } });
const b = net.client({ replicaId: 'b', initial: { tasks: {} } });
await net.settle();
b.link.goOffline();
a.state.tasks.x = { id: 'x', title: 'while b was away' };
await net.settle();
b.link.goOnline();
b.connect();
await net.settle();
assert.equal(b.state.tasks.x.title, 'while b was away');
```
