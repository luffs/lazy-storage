# lazy-storage

Realtime shared state for web apps that keeps working offline, on a
server you run yourself. Clients read and write the state like a plain
JavaScript object; every change reaches everyone else as a small JSON
diff, edits made offline are kept and sent later, and everything converges
when the connection is back.

It is the "database + sync" half of a Firebase-style backend as a
library: one dependency ([lazy-watch](https://github.com/luffs/lazy-watch)),
no build step, and on the server one process and a SQLite file.

```bash
npm install lazy-storage
```

## Quickstart

In the browser, a client is a live mirror of a store:

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

On the server (Bun here; [Node works too](docs/server.md#on-node)), a few
lines hold every store and say who may open which:

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
  authorizeId: (user, storeId) => user.teams.includes(storeId)                   // false → closed 'forbidden', nothing loaded
});
// clients connect to ws://host:3200/ws?token=... and name their store per client
```

A server with a single store passes `stores: () => store`; there is one
protocol and one route either way.

To see it run, clone the repository and start the smallest example, a
shared list, then open http://localhost:3200 in two tabs:

```bash
bun install
```

```bash
bun examples/basic/server.js
```

## What you get

- **State you just edit.** No queries, mutators or document types to
  define: assign to `db.state` and it syncs. A list of records is an
  ordinary array to your code (push, splice, sort) and a keyed map on the
  wire, so two people adding or reordering never collide.
  ([The model](docs/model.md))
- **Offline by default.** Edits made without a connection go to an outbox
  that survives reloads, and are sent in order when the server is back.
  ([Offline](docs/offline.md))
- **Conflicts you can reason about.** The server merges field by field:
  edits to different fields of one record both land, and on the same field
  the later one wins everywhere. A client whose edit lost is told what it
  lost. ([How conflicts resolve](docs/model.md#how-conflicts-resolve))
- **Undo and redo** that only ever touch your own edits, not a
  teammate's. ([Undo](docs/model.md#undo))
- **React and Vue bindings**, `lazy-storage/react` and `lazy-storage/vue`,
  and a pattern of a few lines for anything else.
  ([With a UI framework](docs/model.md#with-a-ui-framework))
- **Storage that looks after itself.** SQLite with a row per field, on Bun
  or Node: versioned migrations, and backups and restores while serving.
  ([Persistence](docs/server.md#persistence))
- **Your own users.** Two hooks say who a request is and which stores
  they may open; presence shows who is here and what they share.
  ([Authentication, presence, and eviction](docs/auth.md))
- **One socket for everything.** Any number of stores over one
  connection, and one connection per browser however many tabs it has
  open. ([Multiple stores](docs/stores.md))
- **Relays.** A relay on a shop's or an office's network keeps its
  clients working while the server is away; relays in front of the
  server take the writing to clients off its one core.
  ([Relays](docs/relays.md))
- **Built for the open internet.** Size and rate limits are on by default,
  and the test suite includes a fuzzer that plays a signed-in attacker
  beside honest clients. ([Limits](docs/limits.md))
- **TypeScript declarations** for every entry point, and
  `lazy-storage/testing` to test your app against a store without
  sockets. ([Testing](docs/development.md#testing))

## How fast

Measured with the repository's own benchmarks, on a 4-core Linux server
with the load coming from another machine:

- One store on SQLite took **15 000 writes a second** behind ten relays,
  each write reaching every client at a p99 of some 50 ms.
- **20 000 connected clients** behind three relays left the machine's
  cores nearly idle, and every one of them, dropped at once, was answered
  again with only what it had missed.

[Spreading the load](docs/relays.md#spreading-the-load) says what a
change costs and where, and `examples/podman-caddy` is that layout in a
container behind Caddy. `bench:fanout --url` measures a deployment of
your own ([Examples, benchmarks, and tests](docs/development.md)).

## When to use something else

- **Collaborative text, or merging two edits of the same value.** This is
  last-writer-wins per field, not a text CRDT: on the same field, the
  later edit replaces the earlier. For a shared document, embed a library
  made for it (Yjs, Automerge) and keep the state around it here.
- **Data too large to hand a client whole.** A client that opens a store
  gets all of it: who may read is decided per store (what they may write,
  per path). Large or mixed-audience data has to be split into stores; if
  you need queries over a big dataset synced in part, this is the wrong
  tool.
- **No server to run.** You host it: one process holds a store, and
  backups are yours to schedule. Authentication is a hook, not a system:
  users, tokens and memberships stay in your application.

## Documentation

| | |
|---|---|
| [The model](docs/model.md) | State, lists, plain arrays, UI frameworks, how conflicts resolve, undo, scope |
| [Offline](docs/offline.md) | The outbox, storage adapters, what a reconnect does |
| [Multiple stores](docs/stores.md) | Many stores over one socket, one socket per browser |
| [The server](docs/server.md) | Persistence, migrations, backups, state the server owns, embedding, Node |
| [Authentication, presence, and eviction](docs/auth.md) | Who may open what, who is here, ending sessions |
| [Relays](docs/relays.md) | Working while the server is away, fan-out, spreading the load, compression |
| [Limits, memory, and observability](docs/limits.md) | The ceilings a public server needs, and what to watch |
| [API](docs/api.md) | Every export, in brief |
| [Wire protocol](docs/protocol.md) | The messages, refusal and close codes, a relay's link |
| [Examples, benchmarks, and tests](docs/development.md) | What runs from a checkout |

`examples/` holds complete programs with no build step: the shared list
in plain JavaScript, Vue and React, the same served by Node, a
server-side client, and the podman-caddy deployment. See
[examples/README.md](examples/README.md). Changes are in
[CHANGELOG.md](CHANGELOG.md).

## License

ISC
