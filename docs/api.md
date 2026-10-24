# API

**Client** (`lazy-storage`)

- `createClient({ store, connection | transport, initial, singletons, registers, parents, position, replicaId, storage, mirror, cache, cacheDelay, undo, undoLimit, reconnect, presence, now })` — the shape options as the server declares them (see [The model](model.md)); `mirror: true` is a read-only follower's defaults: `cache`, `undo` and `presence` off, each still settable; `db.restored` — started from the persisted base; `db.version` — the store version this client has everything up to, and `db.epoch` the storage life it counts in (null for a relay's own counting); `db.relayed` — true while a relay answers this store on its own, the server away (see [Relays](relays.md))
- `openClient(options)` → `Promise<db>` — the same, for a storage adapter whose `load()` returns a promise
- `createConnection({ transport, reconnect, keepalive, wake })` → `connect()`, `close()`, `status` (`'offline' | 'connecting' | 'online'` — the socket's; a client says `online` only once its store is synced as well), `closed` — why the server turned the socket away, or null — `fetch(path)` when the transport can — `on('status' | 'closed', fn)` — a socket shared by clients
- `sharedConnection({ name, transport, storage, shapeOf, reconnect, keepalive, channel, locks, tabId, replicaId, sweepEvery, linger, onError })` → the same, plus `leader`, `upstream` — the socket's status — `pending(store)` — the replica's unsent ops — `on('sync', fn)`, `follow(port, stores)` — a page on a MessagePort following the replica for those stores, ended by what it returns — `dispose()` — one socket and one replica per browser, the tabs electing a leader (see [One socket per browser](stores.md#one-socket-per-browser))
- `portConnection(port, { reconnect, keepalive })` → the connection for the other end of `follow`; `messagePortTransport(port)` is the transport underneath
- `db.state` — the base with this client's pending edits on top (a lazy-watch proxy). Read and write it directly; `db.base` — the server's state as last heard, read only; `db.clockOf(c, id)` — a record's clock in the base; `db.shape` — the declaration as the client uses it
- `db.store`, `db.connection`, `db.replicaId`
- `db.list(path, { position })` — an ordered list of records in a collection (`'tasks'`) or a keyed map at a path (`['state', 'lists', 'items']`): `all()`, `ids()`, `get(id)`, `has(id)`, `add(record, where) → id`, `move(id, where)`, `remove(id)`, `reconcile(ids) → written`, `keyFor(where)`; `where` is `{ before }`, `{ after }`, `{ at }`, or nothing for the end. `inOrder(map, { position })` gives any keyed map's records in that order; `listIn(state, path, { position })` is the same list over any state (a framework's writable view, a mirror's copy, a plain object patched to a store), and `listFrom(records)` a whole list as a keyed map in the array's order, for `initial` or a default
- `db.collection(name)` — `add(record) → id`, `update(id, fields)`, `remove(id)`, `get(id)`, `has(id)`, `ids()`, `all()`, `watch((changes, meta) => …)` → unsubscribe — the records a batch changed, `{ type: 'insert' | 'update' | 'remove', id, record, previous }` as plain copies (see [With a UI framework](model.md#with-a-ui-framework))
- `db.isPending(path)` — whether an edit not yet acknowledged writes at, under, or over `path`
- `db.presence` — users with a live session on this store; `db.peers` — every live session as `{ replicaId, user, key, data }`, this client's own included; `db.wantPresence(on)` — hear presence or stop while the client runs (read back as `db.wantsPresence`); `db.share(data)` — what this client shares with them (JSON, `null` clears), read back as `db.shared`; `db.closed` — `{ code, message }` after the server ended this store for us, else null
- `db.connect()`, `db.disconnect()`, `db.dispose()`, `db.status` (`'offline' | 'connecting' | 'online'` — `online` the moment a snapshot or delta is applied, with `db.state` already current), `db.pending`
- `db.stats()` — `{ pending, oldestPendingMs, ackMs }`: unacknowledged ops, how long the oldest has waited, and the last acknowledgement's round trip; `null` where nothing has happened yet
- `db.watch(listener)` — state changes; `meta?.origin === 'remote'` marks the server's
- `db.on(event, fn)` — `status`; `error` (a `code` among `shape-mismatch`, `rate-limited`, `snapshot-fetch`, or one of a refused op's); `sync` (the outbox changed); `presence`, `peers`; `closed` `{ code, message }`, the server's final word on the store (see [Closed codes](protocol.md#closed-codes); on `outdated` the app reloads the page); `history` `{ canUndo, canRedo }`; `lost` `{ seq, lost: [{ c, id, path, why }] }` for an op of which something lost; `rejected` `{ seq, code, message, changes }` for one refused whole and taken back; `reset` `{ epoch, previous: { epoch, version, state } }` when the store's storage started over; `relay` with `db.relayed` when it changes (see [What happened to my edit](model.md#what-happened-to-my-edit))
- `db.undo()`, `db.redo()`, `db.canUndo`, `db.canRedo`, `db.checkpoint()`, `db.group(fn)`, `db.clearHistory()`
- `webSocketTransport(url, { WebSocket, fetch })` — `fetch` fetches a large snapshot from the socket's server (the global fetch by default; your own to add headers; false to keep every snapshot on the socket); `memoryOutbox()`, `localStorageOutbox(key, { onError })` — document adapters: `{ load(), save(outbox), saveState({ state, clocks, version, epoch }) }`, the built-in ones also `clear()`
- `indexedDBStorage(name, { onError })` → also `settled()`, `close()`, `destroy()` — a row adapter: `{ load(), commit({ puts, deletes, meta }), replace({ records, meta }), saveOp(op, meta), removeOp(seq, meta), dropOps(seq, meta) }`, a record being `[c, id, data, clock]` and `meta` `{ replicaId, seq, version, epoch }`

**Bun client** (`lazy-storage/client/sqlite`): `sqliteClientStorage(file, { wal })` → a row adapter on bun:sqlite, plus `db` and `close()`.

**Vue** (`lazy-storage/vue`): `useClient(db, { writable })` → `{ state, status, presence, pending, closed, canUndo, canRedo, restored, stop }` — `state` a read-only reactive mirror patched on every batch (with `writable: true`, a view that reads and writes `db.state`, tracked through the mirror; see [With a UI framework](model.md#with-a-ui-framework)), the rest shallow refs, all one per client and shared by every caller; each call lets go with the current effect scope (a component's), else by `stop()`, and the last to let go stops the mirror.

**React** (`lazy-storage/react`): `useClient(db)` → `{ state, status, presence, pending, closed, canUndo, canRedo, restored }`, a new object per change, through `useSyncExternalStore`; `useClientSelector(db, select(state, db), isEqual?)` → what `select` picks, as plain data, re-rendering only when it changes (deep equality by default); `trackClient(db)` → the `{ subscribe, getSnapshot, version }` underneath, one per client.

**Testing** (`lazy-storage/testing`): `createNetwork(store | relay, hubOptions)` → `client(options, { user })`, `link({ user })` → `{ factory, goOffline(), goOnline(), stall() }`, `settle()`, `pending` — clients and a store (or a relay) linked in memory, see [Testing](development.md#testing); `hubOptions.vouch` lets a relay on the network vouch for its clients; `fakeTime(start)` → a clock with `advance(ms)` and `set(ms)`.

**Server** (`lazy-storage/server`)

- `createStore({ initial, singletons, registers, parents, rules, readOnly, validate, retention, compactEvery, deltaLog, maxChanges, rateLimit, storage, groupCommit, migrations, presence, onError, now })` — `rules` and `validate` see [Who may write what](auth.md#who-may-write-what); `readOnly` is `true` or a list of collections and singletons; `groupCommit` (default true) see [Persistence](server.md#persistence); `migrations` see [Migrations](server.md#migrations); `presence` is `false` (default), `true`, or `{ key, user, validate, every, maxShare }`
- `store.patch(diff)` → the changes that landed — the server's own write, in the tree's shape, stamped now and applied without checks; `store.del(c, id, path)`; `store.apply(op, { user })` → `{ lost, duplicate }` — a replica's op decided as if it came on that user's session (throws a `RefusedError` for one refused whole)
- `store.state`, `store.snapshot()`, `store.clock(c, id)`, `store.shape`, `store.version`, `store.epoch`, `store.sessions`, `store.now()`
- `store.on(listener)` — every batch that lands, as a diff with `{ origin, replicaId, v, changes }`, on the next microtask and before it is stored; `store.observe('op' | 'refused' | 'session', fn)` → unsubscribe — ops once stored (`{ replicaId, seq, user, changes, version }`), refusals, sessions opening and closing
- `store.stats()` → `{ version, epoch, schema, sessions, replicas, records, log, sent }`
- `store.session({ send, user, onEvict, broadcast, httpSnapshot, follower, writes })` → `{ receive(message), close(), replicaId, kind, grant(as, user, replica), ungrant(as), revoke(filter, code, message) }` — one per connection, transport-agnostic; `follower: true` lets it be a relay's link (a hello saying `follower: true` is refused on any other), whose grants are the clients it vouches for (see [A relay's link](protocol.md#a-relays-link)); `writes: false` has it write nothing as itself, only as those clients
- `store.snapshotJSON()`, `store.clocksJSON()` — the state and the records' clocks as JSON, each encoded once per change; `snapshotResponse(store, request, { origins })` — the snapshot route's Response, for a server of your own
- `store.closeSessions(predicate, message)` — evict sessions; `store.presence()` — distinct users with a live session; `store.peers()` — every live session
- `store.compact()` → `{ deletions, replicas }` forgotten; `store.export()` → the store as a JSON document, `store.restore(doc)` puts one in its storage and ends the store (see [Backups](server.md#backups-restores-and-moving-a-store)); `store.flush()` stores what is pending now and sends what waited for it (throws `unavailable` when the commit failed), `store.dispose()`
- `memoryStorage()`, `assertDocument(doc)`
- `createStores(factory, { idle, sweepEvery, now })` → `get(id)`, `has(id)`, `ids()`, `stats()` — the live stores' stats rolled up: `{ stores, idle, sessions, replicas, records, log, sent }` — `release(id)`, `restore(id, doc)`, `sweep()`, `dispose()`; `isStoreId(id)`
- `createHub(resolveStore, { send, user, authorizeId, authorize, vouch, vouchRate, channel, httpSnapshots, onError })` → `{ receive(message), close(), revalidate(filter), revoke(filter, code, message), stores, user }` — the server side of a multiplexed connection; `vouch(credential, storeId)` judges a follower's client and returns its user, or null; `vouchRate` (`{ burst: 100, perSecond: 10 }`, or `false`) how many it may turn away before every vouch waits; `channel` is a transport's topic fan-out, `httpSnapshots` `{ url(id), threshold }` its snapshot route
- `toJSON(message)` — a message's JSON, encoded once however many sockets it goes to (`tagStore(message, id)` is what a hub does); `RefusedError`, `HELLO_OPS`

**SQLite** (`lazy-storage/server/sqlite`, Bun): `sqliteStorage(file, { wal, lease: { ttl } | false, checkpoints: 'worker' | 'inline', onError })` →
`store(id)` (whose load takes the store's lease, `close()` gives it up, and `replace(doc)` takes a document), `ids()`, `remove(id)`, `backup(file)`, `fromLeaves(shapeOf)` (a file from before 1.0, see [Persistence](server.md#persistence)), `db`, `close()` (gives up every lease). `memoryStorage()` takes a document with `replace(doc)` too.

**Bun adapter** (`lazy-storage/server/bun`): `serve({ stores, port, path, fetch, authenticate, authorizeId, authorize, expiresAt, minSession, maxPayload, perMessageDeflate, httpSnapshots, maxBuffered, relays, onError })` —
`stores` is a registry or `id => store|null` (for one store, `() => store`); the
hub listens at `path` and the snapshot route under it; `relays` is `{ authenticate(req), expiresAt(relay, req), authorize(relay, storeId), path, headers, rate }` (see [Relays](relays.md#running-one)); the returned server gains `shutdown({ reason })`, `sockets()`, `socketStats()`, `disconnect(filter)` and `revalidate(filter)`.
`createHandlers(options)` → `{ upgrade(req, server), websocket, close({ reason }), closing, sockets(), socketStats(), disconnect(filter), revalidate(filter) }` for mounting inside your own `Bun.serve`.

**Node adapter** (`lazy-storage/server/node`, needs `ws`): `serve({ stores, port, host, request, path, authenticate, authorizeId, authorize, expiresAt, minSession, maxPayload, perMessageDeflate, httpSnapshots, idleTimeout, maxBuffered, relays, onError })` →
an `http.Server` with `shutdown({ reason })`, `sockets()`, `socketStats()`, `disconnect(filter)` and `revalidate(filter)`; `createHandlers(options)` → `{ upgrade(req, socket, head), request(req, res), close({ reason }), closing, sockets(), socketStats(), disconnect(filter), revalidate(filter), wss }`;
`toRequest(req)` — the Web `Request` `authenticate` sees. **node:sqlite** (`lazy-storage/server/sqlite-node`): `sqliteStorage(file, { wal, lease, checkpoints, onError })`, as the Bun one.

**Relay** (`lazy-storage/relay`, see [Relays](relays.md))

- `createRelay({ shapeOf, link, storage, acknowledge, grace, authorizeOffline, linger, reconnect, keepalive, replicaId, onError, now })` — `shapeOf(storeId)` the shape of a store it carries (with `rules` and `readOnly` to refuse offline what the server would), or null; `link` a transport factory dialling the server's relay route with the relay's own credential; `storage` is `memoryCopies()` (default) or `fileCopies(dir, { saveDelay, onError })`; `acknowledge` is `'upstream'` (default: a site's relay, the server acknowledges) or `'local'` (a browser's tabs); `authorizeOffline(key, storeId)` whether a client the server let in before is answered while it is away; `linger` (30 s) how long a store is kept after its last client left, `false` to keep every store
- `relay.session({ send, credential, key })` → `{ receive(message), close() }` — a client's socket
- `relay.mode` (`'through' | 'local'`), `relay.status` (the link's), `relay.on('mode' | 'status' | 'sync' | 'error', fn)`, `relay.copy(storeId)` — what it shows of a store, `relay.pending(storeId)` — its own unacknowledged ops, `relay.patch(storeId, diff)` — a write of its own, `relay.upstreamUp()`, `relay.upstreamDown()` — the host's word on the server, `relay.stats()` → `{ mode, link, stores: { [id]: { sessions, queue, v, synced, waiting } } }`, `relay.close()`

**Bun relay** (`lazy-storage/relay/bun`): `createRelayHandlers({ relay, credential, key, path, maxPayload, perMessageDeflate, maxBuffered, onError })` → `{ upgrade(req, server), websocket, close({ reason }), sockets() }` for your own `Bun.serve`; `credential(req)` → `{ headers, query }`, what the server judges the client by; `upstreamSocket(url, { headers, WebSocket })` → a transport factory on a WebSocket with headers.

**Types**: declarations ship with the package for every entry (`types/`);
the Node entry's need `@types/node`. A client's state type comes from
`initial`, an empty array there widened from `never[]` to `any[]`; give
the type argument (`createClient<State>(...)`) for records typed
throughout.

**Core** (`lazy-storage/core`, also re-exported by `lazy-storage`): the
pieces both sides share — `createShape`, `createDoc`, `docFromRecords`,
`compile` (a lazy-watch batch to changes), `diffOf` (changes to a diff in
the tree's shape), `changeFor`, `assertChange`, `createClock`, `clamp`,
`isTimestamp`, the path helpers, `randomId`, `PROTOCOL`, and the position
keys: `keyBetween(a, b)`, `keysBetween(a, b, n)`, `comparePositions`,
`isPositionKey`.
