# Changelog

All notable changes to lazy-storage are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning.

## [Unreleased]

### Added

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
