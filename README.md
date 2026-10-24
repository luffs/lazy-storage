# lazy-storage

Realtime shared state for web apps that keeps working offline, on a
server you run yourself. The state is collections of records, of any
depth; clients read and write it like a plain JavaScript object, every
change reaches everyone else as the few fields it touched, edits made
offline are kept and sent later, and everything converges when the
connection is back.

It is the "database + sync" half of a Firebase-style backend as a
library: one dependency ([lazy-watch](https://github.com/luffs/lazy-watch)),
no build step, and on the server one process and a SQLite file.

```bash
npm install lazy-storage
```

## Quickstart

In the browser, a client is a live mirror of a store:

```js
import { createClient, webSocketTransport, localStorageOutbox, inOrder } from 'lazy-storage';

const shape = {
  initial: { tasks: {}, settings: { theme: 'light' } },   // a collection of records, and a singleton
  singletons: ['settings']
};
const db = createClient({
  transport: webSocketTransport('wss://example.com/ws?token=...'),
  store: 'todo',
  ...shape,
  storage: localStorageOutbox('todo')
});
db.connect();

const tasks = db.list('tasks');                       // the collection as an ordered list
const id = tasks.add({ title: 'Ship it', done: false }); // synced, offline or not
db.state.tasks[id].done = true;                       // one field, merged on its own
db.state.settings.theme = 'dark';
tasks.move(id, { at: 0 });

db.watch(() => render(inOrder(db.state.tasks)));
db.on('lost', ({ lost }) => console.log('a newer edit won', lost));
db.undo();
```

On the server (Bun here; [Node works too](docs/server.md#on-node)), a few
lines hold every store and say who may open which:

```js
import { createStore, createStores } from 'lazy-storage/server';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { serve } from 'lazy-storage/server/bun';

// Any number of stores in one SQLite file, one row per record. The shape is
// the clients': the server checks theirs against it
const sqlite = sqliteStorage('data/app.sqlite');
const stores = createStores(id => createStore({
  ...shape,
  rules: { settings: user => user.role === 'admin' },   // who may write what; the rest is anyone's
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
  define: declare the collections, assign to `db.state`, and it syncs.
  Records may nest as deep as they like, and each path merges on its
  own; a list is a collection whose records carry a position, so two
  people adding or reordering never collide. ([The model](docs/model.md))
- **Offline by default.** Edits made without a connection go to an outbox
  that survives reloads, and are sent in order when the server is back.
  ([Offline](docs/offline.md))
- **Conflicts you can reason about.** The server decides path by path:
  edits to different fields of one record both land, on the same field
  the later one wins everywhere, a deletion made offline does not take
  away what someone edited after it, and a child never outlives its
  parent. A client whose edit lost is told what and why.
  ([How conflicts resolve](docs/model.md#how-conflicts-resolve))
- **Undo and redo** that only ever touch your own edits, not a
  teammate's. ([Undo](docs/model.md#undo))
- **React and Vue bindings**, `lazy-storage/react` and `lazy-storage/vue`,
  and a pattern of a few lines for anything else.
  ([With a UI framework](docs/model.md#with-a-ui-framework))
- **Storage that looks after itself.** SQLite with a row per record, on
  Bun or Node: versioned migrations, and backups and restores while serving.
  ([Persistence](docs/server.md#persistence))
- **Your own users.** Two hooks say who a request is and which stores
  they may open, rules per collection what they may write; presence
  shows who is here and what they share.
  ([Authentication, presence, and eviction](docs/auth.md))
- **One socket for everything.** Any number of stores over one
  connection, and one connection per browser however many tabs it has
  open. ([Multiple stores](docs/stores.md))
- **Relays.** A relay is a store that follows the server: on a shop's or
  an office's network it keeps its clients working together while the
  server is away, and in front of the server it reads each store once
  for all its clients, taking the writing to them off the server's one
  core.
  ([Relays](docs/relays.md))
- **Built for the open internet.** Size and rate limits are on by default,
  and the test suite includes a fuzzer that plays a signed-in attacker
  beside honest clients. ([Limits](docs/limits.md))
- **TypeScript declarations** for every entry point, and
  `lazy-storage/testing` to test your app against a store without
  sockets. ([Testing](docs/development.md#testing))

## How fast

Measured with the repository's own benchmarks before 1.0, on a 4-core
Linux server with the load coming from another machine (1.0's relays fan
out the same way, and are yet to be measured again):

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
  last-writer-wins per path, not a text CRDT: on the same field, the
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
| [The model](docs/model.md) | Collections, records and singletons, lists, how conflicts resolve, undo, UI frameworks, scope |
| [Offline](docs/offline.md) | The outbox, storage adapters, what a reconnect does |
| [Multiple stores](docs/stores.md) | Many stores over one socket, one socket per browser |
| [The server](docs/server.md) | Persistence, migrations, backups, state the server owns, embedding, Node |
| [Authentication, presence, and eviction](docs/auth.md) | Who may open and write what, who is here, ending sessions |
| [Relays](docs/relays.md) | A store that follows the server: working while it is away, spreading the load, compression |
| [Limits, memory, and observability](docs/limits.md) | The ceilings a public server needs, and what to watch |
| [API](docs/api.md) | Every export, in brief |
| [Wire protocol](docs/protocol.md) | The messages, refusal and close codes, a relay's link |
| [Examples, benchmarks, and tests](docs/development.md) | What runs from a checkout |

`examples/` holds complete programs with no build step: the shared list
in plain JavaScript, Vue and React, the same served by Node, a
server-side client, and the podman-caddy deployment. See
[examples/README.md](examples/README.md). Changes are in
[CHANGELOG.md](CHANGELOG.md), and what 1.0 asks of an app on 0.x under
its heading.

## License

ISC
