# The server

What a store keeps and how it is stored, migrated and backed up; state the
server itself owns; and running lazy-storage inside a server of your own.

## Persistence

A store persists as **rows, one per leaf path**: a live row holds the
value and the timestamp that won it, a tombstone holds the timestamp only.
Every accepted op writes exactly the rows it won and the entries it
dropped, so the write cost of an edit is a few rows rather than the
state, and what one turn of the event loop accepted is committed in one
transaction (below). On load the state is `initial` with the rows
applied on top: `initial` is the skeleton an app expects to exist (its
top-level containers), the rows carry the data, and a container added to
`initial` later simply appears. A client may empty one of those
containers but not delete it (refused with `forbidden`), since its
tombstone would refuse every write under it for the retention window;
the server's own `patch` may.

**Group commit.** A store commits what one turn of the event loop merged
together, at the end of the turn: the ops every socket brought in that
poll, the server's own patches, the replicas hellos claimed. What it
would have sent meanwhile (acks, patches, the answers to hellos,
presence) waits for that commit and then goes out in the order it was
made, so no client hears of a change the storage does not have. SQLite
writes whole 4 KB pages, and a commit touches several however small the
op; a busy store's ops share them, a page written once for the turn, and
the disk stops being what holds the store back. A store with nothing
pending sends at once, so a quiet one answers as it always did.
`store.flush()` commits at once (and throws `unavailable` when the
commit failed), and so do creating a session, an eviction, a snapshot
over HTTP, `export()` and `dispose()`. `groupCommit: false` commits every
change as it is made. A backup holds what is stored: a change from the
turn it runs in is not in it yet, and nothing acknowledged is missing
from it.

What that asks of code on the server:

- **`patch()` and `apply()` return before their change is stored.** A
  process that goes down before the turn ends loses it, and nobody was
  told otherwise, unless the code that called them told someone. So
  before you tell anyone outside the store that a change is saved (an
  HTTP response, a message to another system, a job marked done), call
  `store.flush()`:

  ```js
  store.patch({ orders: { [id]: order } });
  store.flush();   // stored now, or it throws: then say so, and do not answer 'saved'
  res.json({ saved: true });
  ```

- **A commit that fails does not throw from `patch()`.** The store
  reports it to `onError` and unloads itself (its clients say hello
  again, with every edit they had not had acknowledged); `flush()` is
  where a failure reaches the code that wrote.
- **`store.on(listener)` hears a change as it is merged, before it is
  stored.** A listener that passes changes on (a webhook, an audit log, a
  search index) can pass on one whose commit then fails. `store.observe('op',
  fn)` is told of an op once it is stored, as its author is: the one to
  forward from.

Adapters:

- `sqliteStorage(file)` (`lazy-storage/server/sqlite`, Bun) — one file for
  any number of stores, keyed by `(store, path)`, WAL mode, the delta log
  kept alongside. `sqlite.store(id)` gives a store its adapter;
  `sqlite.ids()`, `sqlite.remove(id)`, and the raw `sqlite.db` are there
  for administration. The write-ahead log is copied back into the file
  (checkpointed) on a worker thread with a connection of its own, so the
  thread that commits, the server's event loop, never waits on the disk
  for it; `checkpoints: 'inline'` leaves that to SQLite on the committing
  thread, as it was. A worker that cannot start (a bundle that left
  `sqlite-checkpoint.js` out, say) or fails is reported to `onError`
  (default console) with code `checkpoint-worker`, and SQLite checkpoints
  on the committing thread again.
- `sqliteStorage(file)` from `lazy-storage/server/sqlite-node` — the same
  adapter on `node:sqlite` (Node 22.13 and later); the two read each
  other's files.
- `jsonFileStorage(file)` — one JSON document per store, written
  atomically and debounced, without the delta log (a restart answers the
  first reconnects with snapshots). Fine for small single-store deployments.
- `memoryStorage()` — nothing survives the process; for tests.

One process serves a store at a time. Two processes on one SQLite file,
a deploy whose old and new process overlap or a server started twice,
would each load the same store, commit versions that collide, and keep
their clients in states that never meet again. So the SQLite adapters
take a **lease** on a store when it loads: a row in the file, renewed on
a timer (a commit checks it still holds it, and renews it only when it
runs low), given up when the store is disposed
(a registry's release, a graceful shutdown) or the file is closed. A
process that finds a store leased elsewhere cannot load it (code
`store-locked`); a hub tells the client `unavailable`, and the client
says hello again a moment later, so during a deploy's overlap clients
land on whichever process holds the store. A commit that finds its lease
gone is refused, so a process that stalled cannot write over the one
that took over. A crashed process's lease runs out after `lease.ttl`
(default 30 s), or at once when that process was on the same machine.
Stores are leased one by one: processes may share a file on purpose by
serving different stores. `lease: false` turns it off.

A custom adapter implements `load()`, `commit(change)`, and `flush()`,
and optionally `commitMany(changes)` (a turn's changes in one
transaction, all or none; without it the store commits them one by one),
`close()` (the store let go of it) and `replace(doc)`; `change` carries
the rows an op won and dropped, the replica's progress, the version and
epoch, and optionally the accepted diff as a `log` entry with the
`logFloor` below which the store no longer needs entries. See the header
of `src/server/storage.js` for the exact shapes. A commit that throws
unloads the store (the error goes to `onError`): memory would otherwise
be ahead of disk. Nothing it held was sent: its sessions are told
`unavailable` and say hello again with their unacknowledged ops, and a
registry loads the store afresh from what is on disk. The SQLite
adapters wait up to five seconds for another connection's lock before a
commit fails.

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

### Migrations

`initial` covers a container added to the state; a change to data that is
already stored (a field renamed, a default filled in, records moved) is a
**migration**. A store runs the ones its rows have not been through, in
order, when it loads and before it serves anyone:

```js
createStore({
  initial: { tasks: {} },
  storage: sqlite.store(id),
  migrations: [
    // 0: tasks' title became name
    state => ({ tasks: Object.fromEntries(Object.entries(state.tasks).map(([id, t]) => [id, { name: t.title, title: null }])) }),
    // 1: every task has a priority
    state => ({ tasks: Object.fromEntries(Object.keys(state.tasks).map(id => [id, { priority: 'normal' }])) })
  ]
});
```

A migration is handed a copy of the state and returns a diff (or
nothing), which is applied as the server's own `patch`: persisted, kept
in the delta log, and sent on, so a client that was away catches up with
a delta that includes it. How many migrations have run is stored with the
rows, in the same commit as each migration's, so a crash never leaves one
half done and the next load starts where the last stopped
(`store.stats().schema`). A new store starts with every one done, since
`initial` is already in the latest shape; a store stored before any were
given runs them all. Only append to the list. A migration that throws
stops the store from loading (the error names it). Storage that has run
more migrations than the list holds, because code was rolled back after a
newer version migrated it, is refused with code `schema-ahead` rather
than served and written in the older shape. Stores migrate as they are
first opened; to migrate every one at once, open each (`sqlite.ids()`,
then `stores.get(id)`). Edits a client made offline in the old shape are
applied as written when it returns, so a migration that renames is best
paired with a `validate` that translates or refuses the old name for a
while.

### Backups, restores, and moving a store

`store.export()` gives a store as a JSON document: every row with the
timestamp that won it (tombstones included), each replica's progress and
owner, and the version and schema. Every adapter takes one with
`replace(doc)` as a store's whole storage, as it does another adapter's
`load()`. A store loaded from it refuses what the original would (a stale
write, a write under a tombstone, another user's replica):

```js
// From a JSON file to SQLite
sqlite.store('team-1').replace(jsonFileStorage('data/team-1.json').load());

// A live store, saved for later or to look at
writeFileSync('team-1.json', JSON.stringify(stores.get('team-1').export()));
```

For a whole SQLite file, `sqlite.backup(file)` copies it with `VACUUM
INTO` while the server runs: consistent, compacted, and without the
leases, so a server started on the copy serves its stores at once. The
copy is made synchronously, so the process answers nobody while it runs;
for a large file, back up from a script of its own (opening the file
takes no lease).

To put a copy back while the server runs, hand it to the registry:

```js
const copy = sqliteStorage('backups/2026-09-23.sqlite');
const doc = copy.store('team-1').load();
copy.close();
stores.restore('team-1', doc);
```

The live store ends and its storage takes the document; its sessions are
told `unavailable` and say hello again within a second, and the next
`stores.get` serves the copy. (`store.restore(doc)` does the same for a
store outside a registry, and leaves you to make the next one.) A store
another process serves is refused with `store-locked`: restore it there.
Storage that no longer loads can still take a document through the
adapter, `sqlite.store(id).replace(doc)`, which is refused under a
store this process has loaded (`store-open`) or another serves
(`store-locked`).

A copy is older than what clients have seen by the time it is put back,
so a replaced store, and every store in a backup, starts a new
**epoch**. A client that was connected gets a snapshot when it comes
back, rather than a delta that would come from a history it never had,
and hears `reset` with the state it showed before (see [What happened to
my edit](model.md#what-happened-to-my-edit)), so the app can tell its user that
recent changes may be gone. Edits acknowledged after the copy was made
are lost with it; edits still in a client's outbox are sent again and
applied. One made offline to a record created after the copy brings
that record back with only the fields it wrote: a `validate` that
refuses a record without its `id` keeps such fragments out. Moving a
store with `replace` costs one snapshot per client, and a reset.

## Serving state the server owns

Not every store is a shared document. A panel that mirrors the processes
on a machine, a dashboard fed by a poller, a job streaming its log: the
server is the only writer and browsers follow. lazy-storage serves this
shape well, with a few choices that differ from the collaborative default.

**Isolation is store layout.** A session on a store receives every patch
on it: `authorizeId` decides who may open a store, and there is no per-user
filter inside one (that is what lets a patch go out as a single publish).
So cut the state along the lines people may see, and let `authorizeId`
enforce them: a store per team, per owner, per job. A view whose readers
are a subset of another's is a separate store, not a filtered one.

**Clients only read.** `readOnly: true` refuses every client op (the
client drops it and resyncs, so a stray local edit falls back in line),
and on the client `mirror: true` turns off what a follower never uses:
the undo manager, the state cache, presence.

**The server's patch is the authority.** `store.patch` lifts any tombstone
on its way, so a record the server recreates under a key it had deleted
(a process that came back, a container after a redeploy) lands, `id` or
not; a replica's op is still judged by the rules under [How conflicts
resolve](model.md#how-conflicts-resolve). Retention, rate limits and the outbox
are idle machinery here: harmless, and the defaults are fine.

**Publishing a LazyWatch the server already writes.** When the state
lives in a lazy-watch proxy that other code mutates, forward its batches:

```js
const live = new LazyWatch({ procs: {}, order: [] });
const store = createStore({ initial: { procs: {}, order: [] }, registers: ['order'], readOnly: true });
LazyWatch.on(live, diff => store.patchFrom(diff, live));
live.procs.web = { state: 'online', pid: 41 };   // reaches every session as a patch
```

`patchFrom` replaces the array fragments lazy-watch emits (`{ 2: 'c',
$length: 3 }`, a `$splice`) with the whole arrays read from `live`, since
arrays travel as whole values, then patches as the server. Arrays of
records still need declaring as registers, on both sides.

**Nulls are deletions.** State that uses `null` for "unknown" comes out as
an absent key on the mirror; read it with `?.`.

**A store that lives as long as something runs** (a job's log) is created
when the job starts, written with `patch`, and ended with
`closeSessions(() => true, 'finished')` then `dispose()`: every follower
hears `evicted` and knows to look elsewhere for the final record.

**Registries and `idle`.** A registry's idle sweep counts sessions, and
a server writer is not one, so a store the server writes continuously
looks abandoned whenever no browser is on it. Either resolve it with
`stores.get(id)` before each write, or keep server-written stores in a
map of your own (a resolver function serves the transport just as well
as a registry) and never let them idle.

## Embedding in your own server

`createHandlers` returns the two pieces `serve` is built from, so the
sockets can live inside a server that already has routes:

```js
import { createHandlers } from 'lazy-storage/server/bun';

const lazy = createHandlers({ stores, authenticate, authorizeId });
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

`upgrade` also answers the snapshot route (`<path>/snapshot/<store id>`,
see [Limits](limits.md)) with a Response, so
mounting it this way serves both.

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
`serve` and `createHandlers`, with the same hooks, limits (`maxBuffered`
included, see [Limits](limits.md)), socket listing,
and graceful `close`, plus `idleTimeout` (default 120 s), a ceiling Bun
keeps on its own: it closes a socket that has sent nothing for that long
(a client pings every 30 s); `authenticate` receives a Web `Request` built from the incoming
Node request, so one function serves both runtimes. Storage comes from
`lazy-storage/server/sqlite-node`, the same adapter on `node:sqlite`.

```js
import { once } from 'node:events';
import { createStore, createStores } from 'lazy-storage/server';
import { serve } from 'lazy-storage/server/node';
import { sqliteStorage } from 'lazy-storage/server/sqlite-node';

const sqlite = sqliteStorage('data/state.sqlite');
const stores = createStores(id => createStore({ initial, storage: sqlite.store(id) }));
const server = serve({ stores, port: 3200, authenticate, authorizeId });
await once(server, 'listening');
```

To mount inside an http server you already have, handle its `upgrade`
event with `lazy.upgrade(req, socket, head)` and its `request` event with
`lazy.request(req, res)` (the snapshot route), each of which resolves to
false when the request is not lazy-storage's, and call `lazy.close()` on
shutdown.
