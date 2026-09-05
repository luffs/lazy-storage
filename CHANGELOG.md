# Changelog

All notable changes to lazy-storage are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning.

## [0.9.0] - 2026-09-05

### Added

- **One socket per browser.** `sharedConnection({ name, transport,
  storage })` makes a browser one replica however many tabs it has: the
  tabs elect a leader with the Web Locks API, the leader runs the
  browser's replica (a hidden client per store on the real socket,
  persisted through `storage`), and every tab's clients follow it over a
  BroadcastChannel, their edits going into the browser's persisted outbox
  at once, socket or no socket, then upstream under the browser's replica
  id, and every batch coming back to every tab. Presence sees one session
  per browser; each tab keeps its own undo history; a tab's `status` and
  `pending` are the browser's (the socket's status, the replica's unsent
  ops). When the leader tab closes the next tab takes over from the
  persisted outbox, IndexedDB included; a store no tab has open anymore
  is let go after `linger`, tabs that closed without a word found by the
  lock each holds. Without Web Locks or
  BroadcastChannel it is an ordinary connection. `createClient` now hands
  `attach` what it declared (`initial`, `registers`) for this, and reads
  a connection's `upstream` and `pending` when it has them
- **`clear()` on the document adapters.** `localStorageOutbox` and
  `memoryOutbox` can now forget their outbox and cache, for a store that
  is gone for good, as `indexedDBStorage.destroy()` could; nothing is
  deleted on its own, and the README says when an app should
- **`lazy-storage/testing`.** The in-memory network the suite runs on,
  for an app's own tests: `createNetwork(store)` (or a hub factory) links
  clients to a store without sockets, with `client()`, `link()`, links
  taken offline and back, and `settle()` to deliver everything queued;
  `fakeTime()` is a wall clock to hand a store and its clients as `now`
- **Compression, on by default.** Both adapters offer the
  permessage-deflate extension, so a client that takes it receives large
  messages compressed, about tenfold for a big store's snapshot; messages
  under `perMessageDeflate.threshold` bytes (default 1024) go plain,
  where compressing would cost more than it saves. The Node adapter asks
  for no context takeover. `perMessageDeflate: false` turns it off, an
  object sets `threshold` and the runtime's own options, and the
  benchmark reports what it saves
- **`stores.stats()`** on a registry rolls up the live stores' stats for a
  health endpoint: how many are live and how many idle, and the sums of
  sessions, replicas, rows, tombstones, and delta-log entries

### Changed

- A share draws on the replica's `rateLimit` bucket like an op. Beyond
  it, the share is refused with `rate-limited` and the client's next
  hello carries its latest value, so a client cannot flood a room through
  presence

## [0.8.0] - 2026-09-04

### Added

- **The outbox takes over from itself.** A new op removes, from the
  pending ops before it, whatever they wrote at or under the paths it
  writes: under last-writer-wins those older writes could never decide
  a value again. Typing into one field keeps one op pending rather than
  one per keystroke, and deleting a record drops its pending edits.
  Nothing is re-stamped, so the merge decides exactly as if every op
  had been sent; a record's `id` is never pruned, since it is what
  makes a write a record write
- **A socket that did not authenticate is told so.** A browser cannot
  read the status of a refused handshake, so `authenticate` returning
  nothing now completes the handshake only to send a `closed` message
  with code `unauthorized` (without a store: it is the socket that
  ends) and close with code 4401; a plain request still gets a 401. The
  connection stops reconnecting, `connection.closed` and
  `connection.on('closed')` carry the reason, and every client on it
  reports it as its own `closed`. `connect()` is the way back once the
  app has fresh credentials: the transport factory runs afresh
- **Vue and React entries.** `lazy-storage/vue` exports `useClient(db)`:
  a reactive mirror of the client's state, patched in place on every
  batch, local or remote, with refs for its status, presence, outbox
  size, closed reason, and undo state; it stops with the component (or
  the current effect scope), and fills `data()` in the Options API as
  well. `lazy-storage/react` exports `useClient(db)`, which reads the
  client's state and facts through `useSyncExternalStore` with one
  subscription per client, and `trackClient(db)` underneath it. Both
  frameworks are optional peers; the examples use the entries
- A `history` event on the client, carrying `{ canUndo, canRedo }` after
  a local batch, an undo, a redo, or `clearHistory()`. The undo manager
  moves its stacks after the batch it emits, so a listener on that batch
  reads them stale; this is the moment to read them
- **Peers: what a client shares rides on presence.** With presence on,
  `db.share(data)` sets a small JSON value (`null` clears it) that is
  never written to the store and lives as long as the session; the hello
  carries it, so a reconnect restores it. Every live session is a peer,
  `{ replicaId, user, key, data }`, in `db.peers` (this client's own
  entry included), the `peers` event, and `store.peers()`; the Vue and
  React entries expose `peers` too. Presence travels as deltas: the
  whole list goes only to a session that has just said hello, then
  `left`, `joined`, and `shared` for what changed, one small message to
  every session per flush, batched per turn of the event loop or per
  `presence.every`. A share over `presence.maxShare` bytes of JSON
  (default 4096), not JSON, or turned away by `presence.validate` is
  answered with an `error` and changes nothing

### Changed

- A row storage adapter must implement `removeOp(seq, meta)`, which
  takes out one pending op a newer op emptied; `saveOp` is also called
  again for an op a newer one pruned. The IndexedDB and SQLite adapters
  do both
- A transport's `onclose` receives `{ code, reason }` where the socket
  knows them; the connection reads code 4401 in case the `closed`
  message did not make it
- **Presence is off by default and set up as one option.**
  `createStore({ presence })` takes `false` (the default: nothing is
  broadcast, `store.presence()` and `store.peers()` are empty, a share is
  refused with `forbidden`), `true`, or `{ key, user, validate, every,
  maxShare }`. `key` replaces `presenceKey`; `user` chooses what of a
  user its peers see (default: all of it), which a server whose
  `authenticate` returns roles or tokens should narrow;
  `validate(data, { user, replicaId, store })` judges a share the way
  `validate` judges an op; `every` caps presence at one message per that
  many milliseconds. A client that never reads presence passes
  `presence: false` to `createClient`: its hello says so, the server
  sends it none, and it may still share. Presence goes out when a
  session says hello, ends, or shares anew, rather than when it opens,
  since a peer needs the replica id the hello brings; a session that
  never says hello is not announced. The presence message no longer
  carries `users`: peers carry `key`, and the client derives its
  presence list from them

## [0.7.0] - 2026-09-04

### Added

- **Vue and React examples.** `examples/vue` (Composition API),
  `examples/vue-options` (single-file components with the Options API,
  compiled in the browser: an App that owns the client and a list
  component), and `examples/react` are the
  shared list as framework apps, served by either example server at
  `/vue`, `/vue-options`, and `/react` on the same store as the basic
  page, with no build step (the framework comes from a CDN through an
  import map). The Vue ones read a reactive mirror patched on every batch
  and write to the client; the React one reads the client's state and
  re-renders on every batch

### Changed

- The README's opening example, the conflict and undo notes, and the
  examples use lists as arrays instead of an order register; a short
  section covers the two ways to pair the array view with a UI framework

### Removed

- **Compatibility with what versions before 0.3.0 wrote.** The store and
  the memory adapter no longer read the old `seqs` shape, the SQLite
  adapters no longer add the `seen` and `epoch` columns on open, and
  `localStorageOutbox` no longer reads the single document that carried
  the state inside the outbox. A document adapter must implement
  `saveState`; the fallback that put the state inside `save` is gone.
  Storage written by 0.3.0 or later opens as before
- `store.compactTombstones(olderThan)`: `compact()` and the retention
  window replaced it; the core `compactTombstones` stays
- `orderToPositions`: the order-register migration. Lists have carried
  positions since 0.6.0 and every known store has been migrated

## [0.6.0] - 2026-09-04

Lists stop being two things. A list of records is a keyed map with a
position on each record, ordered by `db.list`; an array of primitives is a
whole value anywhere, undeclared. Order registers keep working, and
`orderToPositions` migrates them.

### Added

- **Plain arrays in the client's state.** `createClient({ lists: ['tasks',
  'tasks/*/subtasks'] })` makes `db.state` a view in which every list is a
  real array in position order, records carrying their id and no
  position; `db.wire` is the synced state underneath, keyed maps with
  positions, and persistence, deltas, undo, and `db.list` work on it as
  before. Pushes, splices, index edits, sorts, and whole-array
  replacements are translated into record adds, deletes, field writes,
  and the fewest position changes that make the wire order match; changes
  from others arrive as splices at their sorted place, moves, and field
  patches tagged `origin: 'remote'`. A record pushed without an id gets
  one in the next batch. Tested with a randomized three-client fuzz
- **`db.list(path)`: ordered lists without a register.** Every record
  carries a position key (`pos` by default); the list's order is the keys'
  string order, ties by id, unpositioned records last. `add` (at the end,
  or `{ before }`, `{ after }`, `{ at }`), `move`, `remove`, `all`, `ids`,
  `get`, `has`, and `reconcile(ids)`, which takes the order an app shows
  and writes the fewest positions that make the sort agree. An add or a
  move writes one field on one record, so concurrent inserts at the same
  spot both survive and a move never loses to an unrelated edit. Nested
  lists are just paths. The server needs no declaration
- **Position keys** in core: `keyBetween(a, b)`, `keysBetween(a, b, n)`,
  `comparePositions`, `isPositionKey`. Fractional indexing over base 62
  with a length-prefixed integer part, so appends and prepends count
  compactly and only inserts between two keys grow a fraction
- `orderToPositions(store, { list, order, position })` on the server:
  gives every record of a list a position in its order register's order,
  deletes the register, in one patch; `*` patterns migrate a list per
  record

### Changed

- **Arrays of primitives are whole values anywhere**, no declaration
  needed: written and merged as one leaf, and a client expands lazy-watch's
  fragments from the live value for any array it touches. An array
  holding objects is still refused, now with a message pointing at
  `db.list`, unless its path is a declared register. Fragments arriving at
  the server are refused as before. The `registers` option and the
  mismatch check are unchanged for what is declared

## [0.5.2] - 2026-09-03

### Fixed

- **A client on Node 22 no longer stalls after one refused connection.**
  Node 22's global `WebSocket` (undici 6) fires only `error` for a failed
  handshake, never `close`, so `webSocketTransport` never reported the
  close and the connection's retry loop stopped after its first attempt
  against a server that was down. The transport now reports the close
  itself when the error arrives while the socket is still connecting, and
  deduplicates the `close` that browsers and Node 24+ fire after it. The
  Node server test waited on the same missing event and hung the Node 22
  CI job until it was cancelled

## [0.5.1] - 2026-09-03

### Changed

- **Snapshots cost a fraction of what they did.** A snapshot message is
  now encoded straight from the state's plain target instead of a deep
  copy through the proxy, and the encoding is kept until the next
  accepted op and spliced into every snapshot sent meanwhile; the
  message's `state` is decoded only for a consumer that reads the object.
  On a 10 000-task store a snapshot after an op went from 13 ms to 3 ms,
  and one on a quiet store to a few microseconds, so a burst of first
  connections pays for one encoding. `tagStore` copies a message's
  properties as they are declared, getters included
- The README's wire protocol section is restructured: a session in
  order, one table per direction, and the refusal and closed codes with
  what the client does about each

## [0.5.0] - 2026-09-03

TypeScript declarations, a Node server, examples, a benchmark, and the
two costs the benchmark found. No protocol or storage-format change;
0.4.0 servers and clients interoperate.

### Added

- **TypeScript declarations** for every entry, hand-written under
  `types/` and wired into the package exports: the client (`Client<S>`,
  `ClientOptions`, storage adapter shapes), the server (`Store<S>`,
  `StoreOptions`, the storage interface, registry, hub), core (clocks,
  paths, the merge, the wire protocol as `ClientMessage` and
  `ServerMessage`), and the Bun, Node, and SQLite entries. A type check
  (`npm run test:types`) compiles a file that uses the API as an app
  would, wrong usages included, and runs in CI
- **A Node server.** `lazy-storage/server/node` serves stores over the
  `ws` package (an optional peer dependency) with the same `serve`,
  `createHandlers`, hooks, limits, and graceful `close` as the Bun
  adapter; `authenticate` receives a Web `Request` built from the Node
  request, so one function serves both. `lazy-storage/server/sqlite-node`
  is the SQLite adapter on `node:sqlite` (Node 22.13 and later), reading
  and writing the same files as the Bun one. Both are tested end to end
  in the Node suite
- **Examples.** `examples/basic` is a shared list in the browser over a
  Bun server, `examples/node` the same page served by Node, and
  `examples/mirror.js` a client running in a Bun or Node process; all run
  from a checkout with no build step
- **A benchmark.** `npm run bench` times the merge, the gates, broadcasts
  to many sockets, a client's local op on each kind of storage, and
  reconnects answered with a snapshot versus a delta, as medians over
  several rounds

### Changed

- The two SQLite adapters share one implementation (`sqlite-shared.js`);
  the Bun one behaves as before

### Fixed

- **Every write cost as much as the store was large.** The merge found
  the descendants of a path by scanning every clock key, so a one-leaf
  op into a store of 10 000 tasks took half a millisecond and a
  ten-leaf record add fifteen. The clock table now keeps a children
  index (`ClockMap` in core); the same ops take 29 and 64 microseconds.
  Found by the new benchmark
- **Every local op on a client with registers snapshotted the whole
  state.** `expandRegisters` copied the entire state to read the
  registers a diff touched; it now copies only those. A client op on a
  thousand-task state went from 1.1 ms to 13 µs on the row adapter, and
  the same saving applies to every browser tab of an app that declares a
  register

## [0.4.0] - 2026-09-03

Row persistence on the client, deltas that survive a deploy, and the
ceilings and hooks a public server needs. Also fixes a 0.3.0 bug where a
reconnect answered with a delta could leave a register on a stale value.
Storage from 0.3.0 opens as is; servers and clients should upgrade
together.

### Added

- **The delta log survives a restart.** A commit carries the accepted diff
  as `log: { v, diff }` and a `logFloor`; the SQLite and memory adapters
  keep the entries (a `log` table, pruned to the floor) and hand them back
  on load, so the first reconnects after a restart or a deploy are deltas
  too. A persisted log is used only where it is contiguous and ends at the
  current version. The JSON file adapter does not keep it
- **Graceful shutdown.** `createHandlers` returns `close({ reason })`, and
  `serve()`'s server gains `shutdown({ reason })`: new sockets are refused
  with 503, open ones are closed with WebSocket code 1001 so clients
  reconnect at once, and the store registry is disposed, flushing every
  store. Together with the persisted log, a deploy costs each client a few
  small messages
- **Limits.** `maxPayload` on the Bun adapter (default 4 MB) ends a socket
  that sends a larger message; a hello now carries at most 1000 ops, the
  rest following as ops once the answer lands. `maxLeaves` on the store
  (default 10 000) refuses an op touching more leaves with code
  `too-large`. `rateLimit` on the store (a token bucket per replica,
  default `{ burst: 500, perSecond: 100 }`) refuses a live op beyond it
  with code `rate-limited` and a `retryAfter`; the client keeps the op and
  resends its outbox in a hello after that, so nothing is lost
- **Idle store release.** `createStores(factory, { idle })` releases a
  store that has had no session for `idle` ms, flushing its storage, and
  loads it again on the next request; `sweep()` runs it by hand. Off by
  default
- **Observability.** `store.observe('op' | 'refused' | 'session', fn)`
  reports merged ops, refused client ops, and sessions opening and
  closing; `store.stats()` counts what the store holds. `onError` on
  `createHandlers`, `createHub`, and `createStore` receives server faults
  (a store factory or an observer that throws, a bug while handling a
  message) instead of the console; the client's persistence faults reach
  its `error` event
- The Bun adapter is now tested in the library itself, end to end over
  real sockets (`test/bun/serve.test.js`)
- **Row persistence on the client.** A storage adapter may keep one row
  per leaf and one per pending op instead of a state document: every
  batch the state applies is walked into leaves and committed as the rows
  it touched (a deletion takes the path and everything under it), and a
  snapshot replaces the rows, healing anything a failed write left. So an
  edit costs its leaves, however large the state. Two adapters:
  `indexedDBStorage(name, { onError })` for browsers, with `settled()`,
  `close()`, and `destroy()`, and `sqliteClientStorage(file)`
  (`lazy-storage/client/sqlite`) for a client running in Bun. The row
  adapter interface is `{ load, commit, replace, saveOp, dropOps }`, every
  write carrying the client's `{ replicaId, seq, version, epoch }`
- `openClient(options)`: `createClient` for an adapter whose `load()`
  returns a promise (IndexedDB does); resolves to the client. `createClient`
  refuses such an adapter with a pointer to it
- `rebuild(initial, rows)` in `lazy-storage/core`: `initial` with rows
  applied shallow-first, now shared by the server's load and the client's
  restore

### Fixed

- A delta could leave a reconnecting client on a stale value. The
  correction for a leaf that one of the hello's ops lost was read the
  moment that op was merged; a later op in the same hello could then win
  the same leaf, and since a client applies corrections last, the stale
  value stuck (a register was the usual victim). Corrections are now taken
  once, after every op of the hello. Found by the convergence fuzzer;
  present since 0.3.0

## [0.3.0] - 2026-09-03

Three guards a deployed store needs, and three costs that no longer grow
with the size of the state or the number of sockets. Servers and clients
should upgrade together: an old client still syncs (it gets snapshots), but
is dropped from every conflict once its clock runs ahead, since it cannot
correct itself.

### Added

- **Write authorization.** `createStore` takes `readOnly`, path patterns
  (register syntax, `*` allowed) clients may not write: an op with a leaf
  at or under one is refused whole with code `forbidden`. And
  `validate(diff, { user, replicaId, store })`, asked for every client op
  after that check: return `false` or throw to refuse (the message reaches
  the client), return a diff to accept that instead (the client is
  corrected on the leaves it left out), or `true`/nothing to accept. The
  server's own `patch` and a bare `apply` skip both; `apply(op, session)`
  is the client path
- **A clock guard.** An op stamped more than `maxSkew` (default five
  minutes) ahead of the server's clock is refused with code `clock-skew`
  and the server's time, before it can win every conflict or drag the
  server's clock forward. The client adopts the server's time as an
  offset, rewinds its hybrid clock (never behind what it has received),
  re-stamps the refused op and every pending op after it, and sends them
  again, so nothing is lost and no error reaches the app. The clock gained
  `rewind()` for this
- **A retention window.** Tombstones and per-replica progress are kept for
  `retention` (default 30 days); `store.compact()` forgets what is older
  and reports `{ tombstones, replicas }`, and runs by itself on load and
  every `compactEvery` (default one hour) as ops arrive. An op stamped
  before the window is refused with code `expired`, because the deletion
  it might resurrect may already be forgotten; the client drops it and
  resyncs. `retention: Infinity` keeps everything. `store.replicas` lists
  the replicas still remembered
- **Deltas on reconnect.** A hello carries the store version the client
  last saw (`since`, with the store's `epoch`), and the server answers
  with `{ t: 'delta', patches }`: the accepted diffs since then, in order,
  followed by corrections for what the hello's own ops lost. The store
  keeps the last `deltaLog` accepted diffs (default 1000) in memory for
  it, and sends a snapshot when the log does not reach back far enough,
  when the epoch differs (the client's cache remembers storage since
  wiped), or when a hello op was refused. A reconnect that missed nothing
  costs one empty delta. `db.version` is the client's position; `store.epoch`
  identifies a life of the storage and is minted on the first commit
- `toJSON(message)` and `tagStore(message, id)` (`lazy-storage/server`):
  a broadcast is encoded once for every socket it reaches, and a hub
  splices its store id into the encoded JSON instead of encoding the
  payload again. The Bun adapter uses them; a transport of your own can too

### Changed

- **The client no longer serializes its whole state on every local op.**
  The outbox is written synchronously as before, but the state cache is
  written debounced (50 ms after the last change, flushed on dispose), and
  a restore replays the outbox over the cached state so the few ops it may
  be behind by still show. Client storage adapters gain `saveState(cache)`
  for the state (`localStorageOutbox` keeps it under `key:state`; a
  document from before still loads); an adapter without it gets the state
  inside `save` as before. The cache also records `version` and `epoch`
- `snapshot` and `patch` messages carry the store version (`v`); the
  storage interface carries `epoch` in `load()` and `commit()` (SQLite adds
  the column on open, the JSON document a field)
- Storage adapters record a replica's progress as `{ seq, seen }` (`seen`:
  the store's clock when its last op arrived) under `replicas` in `load()`,
  and `commit` may carry `forgetReplicas`. Documents and databases written
  by 0.2.x load as before: the store still reads `seqs`, SQLite adds the
  `seen` column on open, and a replica without one gets a full window
  before it is pruned
- Every refused op now carries a code (`invalid` for one that breaks the
  model); a client refused during a hello no longer sends a second hello,
  since the snapshot is already on its way

## [0.2.2] - 2026-09-03

A server no longer crashes when a store fails to open. No protocol or
storage-format change.

### Fixed

- A store factory (or a migration inside it) that threw while a hub opened
  a store propagated out of the message handler and, under Bun, took the
  server process down. The hub now refuses that store with a `closed`
  message (code `unknown-store`, carrying the error's message), logs the
  fault, and leaves the connection and its other stores alone; a
  synchronously throwing `authorize` is treated as a refusal. The Bun
  adapter additionally catches anything else thrown while handling a
  message and answers an `error` instead of crashing

## [0.2.1] - 2026-09-03

A client restarted while offline now starts from the state it last saw.
No protocol or storage-format change; 0.2.0 servers and clients interoperate.

### Added

- **Cached state across restarts.** The client's storage adapter now
  keeps the last state next to the outbox (written synchronously with
  every local op, coalesced after remote batches), and a client whose
  storage holds a state for its replica starts from it — pending edits
  already applied — instead of from `initial`. A tab reloaded while
  offline shows its data; the snapshot on reconnect brings it up to date
  as before. `db.restored` reports it; `cache: false` keeps only the outbox

## [0.2.0] - 2026-09-03

The first published version. Since 0.1.0 (never published) the API was
reshaped around one protocol and one route: every message names its
store, a client always names its store and either shares a connection or
owns one, the server keeps a hub per socket. Persistence became
row-oriented with a SQLite adapter on Bun, and the server grew
authentication and authorization hooks, presence, eviction, embedding into
an existing Bun.serve, keepalive, wildcard registers, and a registry for
many stores. The todo app in the sibling repository is the reference
consumer.

### Removed

- **The single-store mode and the per-store URL.** There is one protocol
  (every message names its store) and one route (a hub at `path`).
  `serve({ store })` and `/ws/<storeId>` are gone; a server with one store
  passes `stores: () => store`. `createClient` always takes a `store` id,
  with either a shared `connection` or a `transport` for a connection the
  client owns; `createConnection` lost its `multiplex` flag. The untagged
  protocol had no upside that survived scrutiny (a browser cannot read a
  403 at upgrade anyway, and a `closed` message with a code says more) and
  cost every feature a second code path
- `version` no longer travels on the wire (no client read it); it stays on
  the store for tests and debugging

### Changed

- The snapshot names the server's register patterns, and a client whose
  declaration differs raises an error with code `registers-mismatch` on
  every snapshot, so a silent divergence between the two sides is loud

### Added

- **Authentication and authorization hooks.** `serve` / `createHandlers`
  take `authenticate(req)` (the user for a request; null answers 401 at
  upgrade) and `authorize(user, storeId, store)`, asked per store before
  its session exists; a refusal reaches the client as a `closed` message
  with code `forbidden` for that store alone. Both may return promises; a
  hub queues a store's messages while its authorization is in flight. The
  user rides on the session (`store.session({ send, user, onEvict })`,
  `createHub(..., { user, authorize })`)
- **Eviction.** `store.closeSessions(predicate, message)` ends the
  sessions a predicate selects with a `closed` message (code `evicted`) and
  tells their transport through `onEvict` (the hub drops its entry for that
  store; the socket stays up for the others). The client
  gains a `closed` event and `db.closed`, goes offline for that store
  without reconnecting on its own, and rejoins on `connect()`. Terminal
  hub refusals (`unknown-store`, `invalid-store`, `forbidden`) now arrive
  as `closed` too, instead of `error`; server errors carry a `code`
- **Presence.** Stores broadcast the distinct users with a live session
  when one joins or leaves (and hand the list to a new anonymous session);
  `db.presence` and the `presence` event on the client, `store.presence()`
  on the server, `presenceKey` to change how users are deduplicated
  (default: by `id`)
- **Embedding.** `createHandlers` returns `{ upgrade(req, server), websocket }`
  to mount lazy-storage's sockets inside an existing `Bun.serve`; `serve`
  is now a wrapper over it
- **Keepalive.** A connection pings every 30 s while open (`keepalive`
  option; `false` disables), so idle sockets survive proxies and server
  idle timeouts; the timer never keeps a Node process alive
- **Wildcard registers.** A `*` segment in a register path matches one
  segment (`tasks/*/subtaskOrder`), declaring a register per record;
  `registerSet` is now a matcher and `expandRegisters` walks the diff
- **Any number of stores over one socket.** `createConnection({ transport })`
  is a connection that several clients attach to, one per store
  (`createClient({ connection, store })`); every message names its store,
  and `client.disconnect()` on a shared connection leaves just that store
  (`{ t: 'leave', store }`) while `connection.close()` drops the socket for
  all. Each client keeps its own outbox, undo history, and status. A client
  created with a `transport` instead owns a connection of its own, same
  protocol. On the server, `createHub(resolveStore, { send, user, authorize })`
  is the session-shaped counterpart that keeps one store session per
  socket, and `serve({ stores })` serves it at `path`. Every fuzzer client
  carries two stores on its socket, so an offline toggle drops both
- **SQLite persistence on Bun.** `sqliteStorage(file)` from
  `lazy-storage/server/sqlite` keeps any number of stores in one database
  file, one row per leaf path keyed by `(store, path)`, in WAL mode; each
  op commits as one transaction of the rows it touched. `sqlite.store(id)`
  yields a store's adapter, `ids()` lists stores, `remove(id)` drops one,
  and `db` exposes the connection. Tested with `npm run test:bun`
- **Multiple stores per server.** `createStores(factory)` is a registry
  that builds a store per id on first use and keeps it live (`get`, `has`,
  `ids`, `release`, `dispose`); `isStoreId` restricts ids to a URL- and
  filename-safe alphabet. The hub resolves stores through it (or through a
  plain `id => store|null` function; `() => store` for a single store),
  answering an invalid id with `closed` code `invalid-store` and a refused
  one with `unknown-store`

### Changed

- **Row-oriented persistence.** A store now persists as rows, one per leaf
  path (`{ value, ts, deleted }`), plus per-replica sequence numbers and
  the version, and the storage interface is incremental:
  `load()` / `commit({ upserts, deletes, replica, version })` / `flush()`.
  Every accepted op commits exactly the rows it won and the clock entries
  it dropped (the merge now reports both), so a row-oriented backend
  writes only what changed. On load the state is `initial` with the rows
  applied on top: `initial` is the skeleton an app expects (its top-level
  containers), rows carry the data, and a container added to `initial`
  later appears without a migration. The memory and JSON-file adapters
  implement the new interface; the JSON file's format changed from a
  state document to a row list (0.1.0 files are not read)
- `store.compactTombstones` and the core `compactTombstones` commit the
  removed rows; the core helper returns the removed keys instead of a count

## [0.1.0] - 2026-09-02

Initial version: keyed collections, per-leaf last-writer-wins with
tombstones on a single server merge point, hybrid logical clocks, a
persisted client outbox with snapshot-on-hello resync, registers for
whole-value arrays, an undo manager that declines remote batches, memory
and JSON-file server storage, and a Bun WebSocket adapter.
