# Changelog

All notable changes to lazy-storage are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning.

## [Unreleased]

### Added

- **`conflict` and `rejected` events: what happened to an edit.** The
  state always showed what won, but an app could not tell its user that
  an edit had lost or been refused: a lost write arrived as an ordinary
  remote patch, a refusal as a bare `error`. `db.on('conflict', ({ seq,
  lost }) => ...)` now reports, per leaf that lost, the path, what this
  client wrote (`mine`) and what won (`theirs`, null where the record
  was deleted), when the op's ack arrives or, for an op made offline,
  when the reconnect is answered. `db.on('rejected', ({ seq, code,
  message, diff }) => ...)` reports an op the server refused, with the
  edit itself, and a batch the model refused locally (`seq` null). The
  `error` event still comes too. Under a `sharedConnection` every tab
  hears the browser replica's. The server says which leaves lost: `lost`
  on an `ack`, and `lost: [{ seq, paths }]` on the answer to a hello
- **`db.isPending(path)`**: whether an edit not yet acknowledged writes
  at, under, or over a path, for a "saving…" mark on a field or record;
  it changes with the outbox, as the `sync` event announces

### Changed

- **A lost write no longer costs the server a copy of the state.**
  `correction()` snapshotted the whole state for every op that lost a
  leaf; it now reads the paths it corrects

## [0.14.1] - 2026-09-23

Four ways to lose data or the server, fixed: IndexedDB storage stopped
saving for good once another tab closed the database; a replica whose
storage failed to open left every tab `connecting`; a follower tab's
edit was acknowledged before the leader had stored it; and a failed
`jsonFileStorage` write crashed the process. Nothing to change on
upgrade; `indexedDBStorage` gains `onReset` and `jsonFileStorage` an
`onError` option.

### Added

- A `LICENSE` file (ISC, as `package.json` has always said), shipped in
  the package

### Fixed

- **IndexedDB storage no longer stops saving after another tab closes
  the database.** The adapter closed its connection when another tab
  deleted or upgraded the database, but kept handing out the closed one:
  every later write failed into `onError`, offline edits included, until
  a reload. It now lets go of a connection closed under it (by
  `versionchange` or the browser's `close`) and opens a new one on the
  next write. When that finds the database gone and made anew, it tells
  the new `onReset` listeners, and the client writes its whole state and
  every pending op again, so a reload does not come back from half the
  rows or without the edits it had not sent
- **A replica whose storage cannot be opened no longer strands every
  tab.** When the leader's `storage(store)` failed to load (IndexedDB
  refused, a quota), the store's entry was dropped with the hellos queued
  on it, and every tab stayed `connecting` until the leader changed. The
  replica now runs from memory instead, and every tab hears an `error`
  with code `storage-unavailable`: edits sync, but do not outlive the
  browser session. The relay also no longer loads a replica's storage
  twice (an IndexedDB read of every row, twice, at each handover)
- **A follower's edit is acknowledged once the replica has stored it.**
  The leader acknowledged a follower tab's op the moment it arrived, and
  the follower dropped it from its outbox; with storage that writes
  asynchronously (IndexedDB), a leader tab killed before the write landed
  took the only copy. The acknowledgement, and the answer to a hello
  carrying ops, now wait for the replica's storage to settle
- **A failed `jsonFileStorage` write no longer crashes the server.** Its
  debounced write ran in a timer, so a full disk or a permission error
  was an uncaught exception that took the process down. It now goes to
  a new `onError` option (default console), the changes stay pending, and
  the write is tried again a second later; `flush()` still throws to its
  caller

## [0.14.0] - 2026-09-23

A replica now belongs to the user who first said hello with it, so a
teammate's replica id seen in presence can no longer be used to lock
them out of a store (found by a new hostile-client fuzzer). The client
got much faster where apps feel it: a remote move no longer freezes a
large list view, a push or move on one no longer resyncs every record,
React components can select what they read (`useClientSelector`), Vue
components share one mirror, and the state cache and offline outbox stop
rewriting everything. What an upgrade may need:

- A browser where someone else signs in with the last user's storage
  gets `replica-taken`: key client storage by user
- The Vue mirror is read-only; write to `db.state` (a write to the mirror
  only ever changed that component's copy)
- `localStorageOutbox` keeps its outbox op by op; one written by an
  earlier version is taken over on load, but a client of an earlier
  version cannot read the new form, so pending edits wait for this
  version after a downgrade
- The state cache of a document adapter is written once changes settle
  (`cacheDelay`, default a second), not 50 ms after every batch
- Requires lazy-watch 6.4.0

### Changed

- **Requires lazy-watch 6.4.0**, whose `splice` and `shift` return what
  they removed. On a list view, the usual move (`const [m] =
  db.state.tasks.splice(i, 1); db.state.tasks.splice(j, 0, m)`) inserted
  a second copy of the record after the one moved and lost the moved
  one, and the loss synced to everyone; it now moves the record. Its
  faster splice also halves a move on a large list view (~20 ms to ~8 ms
  on 5k records)

### Security

- **A replica belongs to its user, connected or away.** 0.13.0 refused a
  replica id only while a live session of another user held it, and the
  first to claim it won: a signed-in user who read a teammate's replica
  id in presence could say hello with it while the teammate was offline,
  and the teammate, back online, was refused its own replica and locked
  out of the store until the other session ended; an op under it could
  also make the teammate's later edits duplicates. The user who first
  says hello with a replica now owns it, recorded with its progress
  (`owner` in the replica row; the SQLite adapters add the column to an
  existing file), and a hello from anyone else is closed with the new
  code `replica-taken`, while the owner carries on. A browser that signs
  in as another user with the last one's storage meets the same code:
  key client storage by user. Sessions without a user own nothing
- **A closed session hears nothing more.** A session the store had closed
  (evicted, refused its replica, its store unloaded) still processed
  messages a transport of your own kept feeding it

### Fixed

- **A remote move no longer freezes a list view.** When another client
  moved a record, the array view shifted every record the move passed,
  each shift a splice of the whole array: a record moved 1000 places in
  a 5k-record list took some 30 s, and 200 places in 2k about 1.2 s. The
  view now keeps the longest run of records already in order and moves
  only the rest, so a move is one splice out and one in (8.6 ms for the
  latter, most of it lazy-watch's splice). A remote field edit finds its
  record without reading the array through the proxy (2.7 ms to 0.15 ms
  on 5k records)
- **A push or a move on a large list view no longer resyncs every
  record.** Any change of length compared every field of every record
  with the wire and checked each id against all before it: a push onto
  5k records took ~77 ms, a move ~90 ms. Only the records the batch put
  in or changed are synced now, ids are checked through a set, and the
  list's positions are read past the proxy; `reconcile` skips its sort
  when given every record (a push ~7 ms, a move ~22 ms, the rest of it
  lazy-watch's splice)
- **Vue components on one client share one mirror.** Every `useClient`
  call deep-copied the whole state and patched its own copy on every
  batch: mounting 200 rows on a 1k-task state made 200 copies (~150 ms),
  and every edit patched 200 mirrors (~1.3 ms). Calls on a client now
  share one mirror and one set of refs, made by the first and stopped
  when the last lets go (mounting ~9 ms, an edit ~0.12 ms). The mirror
  is read-only: a write to it went nowhere but that component's copy,
  and Vue now warns instead; writes go to `db.state`
- **The state cache is no longer rewritten dozens of times a second.** A
  document adapter's state was written at most 50 ms after any batch, so
  steady remote traffic serialized the whole state some 50 times a
  second (38 MB/s for 10k tasks). It is now written once changes have
  settled for `cacheDelay` (a new client option, default 1000 ms), at
  least every ten delays under traffic that never settles, at once when
  the page is hidden or goes away (`visibilitychange`, `pagehide`), and on
  `dispose`. A cache that lags is safe: it carries the version it
  reflects, a restore replays the outbox over it, and the reconnect asks
  for what came since
- **An offline outbox no longer grows quadratically in writes.** The
  outbox was one document rewritten with every op: 1000 ops offline wrote
  58 MB, the last ones ten times slower than the first. A document
  adapter may now take the outbox op by op (`saveOp`, `removeOp`,
  `dropOps`, the row adapters' calls; `save` stays for a whole outbox),
  and `localStorageOutbox` does: `key` holds `{ replicaId, seq, first }`
  and `key:op:<seq>` each op, so the same 1000 ops write 0.2 MB. A
  document written by an earlier version is taken over on load; a client
  of an earlier version cannot read the new form, so pending edits wait
  for this version again after a downgrade
- **A list removed from the wire is removed from the view.** Undoing the
  creation of a nested list (`task.subtasks = []`, then undo) left an
  empty array in the view where the wire had nothing

### Added

- **`useClientSelector(db, select, isEqual)` in `lazy-storage/react`**
  re-renders a component only when what it selects changes. `useClient`
  re-renders every component on it for every batch, share and status
  event, and the state's records keep their identity as they change, so
  `memo` could not help: 500 list rows re-rendered for an edit to one
  (7 ms) and for a peer's cursor (4.9 ms). With the selector one row
  renders for the edit (0.5 ms) and none for the cursor (0.2 ms). The
  selection is copied out as plain data, compared deeply unless `isEqual`
  says otherwise, and kept as the same object until it changes; it rides
  on the client's one subscription. `trackClient` gains `version`
- **`npm run bench:client`**, a benchmark of what an app feels on the
  client: React and Vue components on `useClient` as the store changes
  (counting re-renders and state copies), the array view of a large list
  under local and remote pushes, moves and edits, `localStorageOutbox`
  under remote traffic and a long offline spell (counting bytes
  serialized), and a lazy-watch object write against a field write.
  `--only <text>` runs the cases whose name contains it
- **A hostile-client fuzzer** (`npm run fuzz:hostile`, a fixed-seed pass
  in `npm test`, a longer campaign in CI): an attacker with a socket of
  its own sends forged ops, other replicas' ids and the server's, seqs
  far ahead, poisoned timestamps, reserved names, fragments, skeleton
  deletions, oversized hellos, shares, churn, and garbage beside honest
  clients, and every step checks that `Object.prototype` is untouched,
  the server's own writes land, no honest client hears an error or
  diverges, the skeleton stands, nothing unauthorized is loaded, sessions
  do not leak, and the store on disk equals the store in memory. It found
  the lockout above, and catches each of 0.13.0's security fixes when
  that fix is taken out

## [0.13.0] - 2026-09-23

A hardening release. One signed-in client could pollute
`Object.prototype` on the server, silence the store's own writes by
claiming its replica id, freeze its clock, or have any store loaded
before authorization; all of that is closed, and a dozen ways an edit
could be lost or a replica left behind are fixed. What an upgrade may
need:

- Move a check that needs only the user and the store id from
  `authorize` to the new `authorizeId`, which runs before the store is
  loaded; `authorize` alone still works, and still loads first
- A server of your own must answer `ping` with `pong`: a socket silent
  for two keepalive intervals is now dropped and reopened
- A `localStorageOutbox` key is one tab's at a time; tabs that should
  share a replica use `sharedConnection`
- The rate limit is per user and a hello costs a token; a session keeps
  one replica id; clients may no longer delete a top-level container of
  `initial`
- Requires lazy-watch 6.3.0

### Security

- **A reserved name in an op is refused.** An op whose diff used
  `__proto__` as a key was accepted, and rebuilding the accepted diff on
  the server walked into `Object.prototype`: one client could add a
  property to every object in the server process. `__proto__`,
  `constructor`, `prototype`, `$splice` and `$length` are now refused as
  keys anywhere in an op, register values included, with code `invalid`;
  `setAt` and `rebuild` refuse such a segment too, so a row persisted
  before this release cannot reach it on load
- **A session speaks for one replica.** The store took an op's
  `replicaId` on trust, so a client could send an op as `server` with a
  seq far ahead, after which every `store.patch` was ignored as a
  duplicate (read-only stores included), or as a teammate's replica,
  whose later edits were then acknowledged and dropped. A session is now
  bound to the replica its hello names (or its first op, for a session
  driven without one): an op under another id is refused with
  `forbidden`, as is a hello naming `server`, a second replica on the
  same session, or a replica a live session of another user holds. An
  op's timestamp must carry its own replica id (`invalid` otherwise)
- **A timestamp's counter is bounded.** An op stamped
  `[ms, 2^53 - 1, id]` pushed the server's clock to a counter where
  `count + 1` no longer changes it, so every server stamp came out alike
  and the store's own writes lost to themselves until the wall clock
  caught up. `isTimestamp` now wants a counter below 2^32 (`MAX_COUNT`
  in core) and a non-negative millisecond
- **The rate limit follows the user.** The token bucket was keyed on the
  replica id the client chose, so a client could shed its limit by
  minting ids; it is now keyed on the user (presence's `key`), or the
  replica for a session without a user. A hello costs one token (its ops
  still ride free), so hellos cannot be replayed in a loop, and the
  server merges at most 1000 ops of one hello (`HELLO_OPS`), as the
  client already sent
- **`authorizeId(user, storeId)` judges a store before it is loaded.**
  `authorize(user, storeId, store)` receives the store, so the hub and
  the snapshot route loaded it first: any signed-in user could have
  every store id they asked for read into memory (kept for good without
  a registry `idle`), run the store factory for each, and tell existing
  ids from unknown ones by `forbidden` against `unknown-store`. The new
  hook, on `serve`, `createHandlers` (Bun and Node) and `createHub`,
  runs first; a refusal loads nothing and answers `forbidden` whether
  the store exists or not. `authorize` still runs after it, for a check
  that needs the store. An app whose `authorize` looks only at the user
  and the id should rename it to `authorizeId`; the README's examples
  now do

### Fixed

- **A rate-limited op could be lost.** After a refusal the client kept
  sending new ops live; once the bucket refilled, a later op was
  accepted, and the server then ignored the refused one as a duplicate
  when the retry hello resent it, while the ack of the later op dropped
  it from the outbox. The store now refuses every live op of a session
  after a rate-limit refusal until its next hello, and the client sends
  nothing live until that hello, which resends the outbox in order. A
  long offline spell hit the same path, since the ops beyond the
  hello's 1000 went live: they now follow in another hello, the store
  saying `connecting` until it has them all. A hello refused for the
  rate is retried after `retryAfter` too
- **An empty object no longer empties a record on disk.** An op writing
  `{}` where a record stood (lazy-watch emits one for a container created
  empty: `settings ??= {}` on a replica that had not heard of it yet) was
  merged into the state, which kept the record's fields, but dropped
  their rows, so after a restart the record came back empty. An empty
  object outside a register now ensures the container and keeps what is
  under it, in the rows and on load alike (`rebuild` takes the register
  matcher to tell); in a register it is still the whole new value
- **A client can no longer delete a top-level container of `initial`.**
  `{ tasks: null }` from any client left a tombstone at `tasks`, and every
  later write under it was refused (none being a record write that lifts
  it) until compaction, a month by default. Such a deletion is now
  refused with `forbidden`; emptying the container still works, and the
  server's own `patch` may still delete it
- **A store disposed under open sessions tells them.** `store.dispose()`
  (a registry's `release`, or its idle sweep) closed its sessions without
  a word, and the hub kept routing to them: the client's next op was
  refused as `invalid` and dropped. The store now sends a `closed` with
  the new code `unavailable` and lets the hub forget the session; a
  client takes that code as passing, not final, and says hello again
  after a moment, which loads the store afresh with nothing lost
- **Leaving a store while its authorization was in flight leaked a
  session.** A `leave` and a new hello for the same store before an async
  `authorize` answered let both verdicts open a session; the first was
  never closed, stayed in presence, and kept the store from going idle
  (React StrictMode's double mount does this). A verdict for an attempt
  that was left is now ignored
- **A dead socket is noticed.** The keepalive sent pings and discarded
  the pongs, so a half-open socket (a network switch, a laptop waking)
  stayed `online` while edits queued into nothing until the OS gave up
  on it. A socket not heard from for two keepalive intervals (a minute
  by default) is now dropped and reconnected. A server of your own must
  answer `ping` with `pong`, as the hub and a store session do
- **Reconnects are jittered.** The backoff doubled without randomness,
  so a server that went away (a deploy closes every socket at once)
  brought every client back in the same instant; each retry now waits
  between half its delay and all of it
- **A patch that never arrived is noticed.** The client took each
  patch's version as it came, so a patch a socket dropped (under
  backpressure, say) left the replica wrong for good: later reconnects
  asked for a delta from after the gap. A patch whose version skips one
  now sends a hello for a delta from the last version held, and patches
  wait for its answer. A relay's patches (a `sharedConnection` tab),
  whose versions do not count one per patch, are exempt
- **The Node adapter closes silent and stalled sockets.** A half-open
  connection kept its sessions, and its place in presence, until the OS
  gave up on it, and a client that stopped reading had every patch
  buffered for it without limit. `idleTimeout` (default 120 s, as Bun's
  own) closes a socket that has sent nothing for that long, and
  `maxBuffered` (default 16 MB) closes one whose unsent output passes it
  with code 1013; the client reconnects and catches up with a delta
- **`localStorageOutbox` reports a failed write.** A full quota was
  swallowed, so an app could not tell its user that edits made offline
  no longer survived a reload. `localStorageOutbox(key, { onError })`
  hears it, as `indexedDBStorage` already did
- **A commit that fails no longer leaves memory ahead of disk.** The
  state, version and delta log changed before `commit`; one that threw
  (a full disk, `SQLITE_BUSY` while a backup held the lock) left a change
  in memory that was never saved nor broadcast, and the sender was told
  `invalid` and dropped its op. The store now unloads itself: the error
  goes to `onError`, sessions hear `unavailable` and resend their
  pending ops to a store a registry loads afresh from disk
  (`stores.get` replaces a disposed store rather than handing it out).
  The SQLite adapters set `busy_timeout` to five seconds, and a store's
  `dispose` still tells its sessions when the final flush throws
- **Two tabs on one `localStorageOutbox` key no longer lose each other's
  edits.** Both loaded the same replica id and sequence number, so the
  server acknowledged one tab's ops and dropped the other's as
  duplicates, and the two overwrote each other's outbox; the README's
  first example was this setup whenever a second tab opened. A key is
  now held by the tab that loaded it first, through a lease under
  `key:lease` renewed while the tab is open and given up on `pagehide`
  or `close()`. A second tab starts a replica of its own that keeps
  nothing across a reload, and hears `storage-in-use` through `onError`.
  A `sharedConnection` leader, which a lock already makes the only one,
  takes the key over (`takeOver()`) whatever a crashed tab left behind

### Changed

- **Requires lazy-watch 6.3.0**, which delivers a batch emitted from
  inside a listener after the batch being delivered. The client reverts
  a refused local batch that way, and a listener registered after it (the
  Vue mirror of `lazy-storage/vue`, an app's own `db.watch`) used to get
  the revert first and then the refused edit, and kept the edit

## [0.12.1] - 2026-09-12

### Fixed

- **A register's new value is its whole value.** A key removed from
  inside a register (a field deleted, or the value assigned without it)
  stayed on the server and in every other client, since the value was
  merged in as a patch, which keeps what it does not mention; only the
  persisted row and a fresh snapshot had it right. A register written by
  a client, by the server's own `patch`, or by a follower through a
  shared connection's relay now replaces what was there, and the patch
  sent on says so, all the way down (`replacingRegisters` in core)

## [0.12.0] - 2026-09-12

### Added

- **Port followers.** `connection.follow(port, stores)` on a shared
  connection lets the page on the other end of a MessagePort (an iframe,
  a worker) have clients on the browser's replica, as the tab's own are:
  for the stores named and no other, under the tab's rights, in a session
  that lives as long as the tab's, through a change of leader. The other
  end runs an ordinary client on `portConnection(port)`, with no socket of
  its own, whose status and pending are the browser's, as the host tells
  it; a host that is done calls what `follow` returned.
  `messagePortTransport(port)` is the transport underneath

## [0.11.1] - 2026-09-12

### Fixed

- **`connect()` from inside a `closed` or `status` event no longer breaks
  the other clients.** When the server turned the socket away, the
  connection read its reason again for each client and listener it told;
  a client that called `connect()` from its own `closed` handler (having
  new credentials by then) cleared the reason, and the next client was
  handed `null` and threw, which in a shared connection left the tab
  offline. A listener reconnecting on `offline` cleared it for everyone,
  and on an ordinary drop left a retry on top of its own socket, which
  opened a second one and never closed the first. A `connect()` called
  from inside any event of the drop now takes effect once every client
  and listener has heard: the reason stays what it was and
  `connection.closed` holds it throughout. A handler or listener that
  throws no longer keeps the rest from hearing, and a transport that
  reports its close synchronously drops the socket once
- **In a shared connection, the socket turned away reaches every tab
  once.** It reached each tab twice, as the store's and as the socket's,
  so a tab that signed in again from inside the event was told again once
  it was back and stayed offline, and a tab that reconnected on every
  `closed` looped

## [0.11.0] - 2026-09-07

### Added

- **`store.patchFrom(diff, state)`** publishes a lazy-watch batch from
  state the server keeps elsewhere: the array fragments lazy-watch emits
  (`{ 2: 'c', $length: 3 }`, a `$splice`) are replaced with the whole
  arrays read from `state`, then patched as the server's own change.
  `LazyWatch.on(live, diff => store.patchFrom(diff, live))` serves a
  LazyWatch that other code already writes
- **`mirror: true` on `createClient`**: a follower's defaults — no undo
  manager, no state cache, no presence — each still settable on its own
- README: "Serving state the server owns", for the server-authoritative
  case — isolation as store layout, read-only stores, mirror clients,
  `patchFrom`, and a store that lives as long as a job
- **`readOnly: true`** locks a whole store: every client op is refused
  with `forbidden`, the server still writes
- **A disposed store says so.** `patch`, `apply` and `session` on a store
  that was disposed (a registry's idle sweep does that) throw an error
  naming the cause and the cure — resolve it again with `stores.get(id)`
  before each write — where the proxy underneath used to complain;
  `store.disposed` reads the flag. The registry docs say that idle counts
  sessions and a server writer is not one
- **`httpSnapshots.origins`** says which origins may fetch a snapshot
  cross-origin: `'*'` (the default, as before), an array of origins to
  echo and no other, or `false` for no CORS header at all.
  `snapshotResponse` takes the same as an option

### Changed

- **Policy is judged before age.** The gates run clock guard, read-only
  paths and `validate`, then retention: an op the store would refuse
  anyway is told `forbidden` rather than `expired`
- `db.status` docs say when `online` fires: the moment a snapshot or
  delta is applied, before the batch reaches `watch` (and a snapshot equal
  to what the client had produces none), so "the store is current" is the
  status event

- **A connection's status is `online`, not `open`.** `createConnection`
  and `sharedConnection` report `'offline' | 'connecting' | 'online'`, the
  same words as a client, whose `online` additionally means its store is
  synced. Code comparing `connection.status` (or `upstream`) to `'open'`
  must change

## [0.10.1] - 2026-09-07

### Changed

- **A server patch is never held back by a tombstone.** `store.patch` is
  the authority: whatever it writes at a deleted path re-adds it, `id` or
  not. Until now the merge lifted a tombstone only for a newer object
  carrying an `id` — the right rule for a replica's edit, which may be a
  stale field write, but a server that recreated a record under a key it
  had deleted (a process that came back, an entry rebuilt from another
  source) was refused for good, since its next patches carry only what
  changed. Client ops and `store.apply` are judged as before; `mergeOp`
  takes the rule as an `authority` option

## [0.10.0] - 2026-09-05

### Added

- **Snapshots over HTTP.** Both adapters serve a store's snapshot at
  `<path>/snapshot/<store id>` (`httpSnapshots`, on by default): `{ v,
  epoch, state }`, compressed once per change and kept until the next
  (brotli, which packs this JSON some 15% tighter than gzip in the same
  time, or gzip for a client without it), with an ETag that answers 304
  while the store is unchanged, behind the same
  `authenticate` and `authorize` as the socket. A client that can fetch
  (`webSocketTransport` can, on the route resolved against the socket URL
  with its query; its `fetch` option takes your own or false) says so in
  its hello, and a snapshot of `threshold` bytes or more (default 64 KB)
  is answered with where to fetch it rather than the state. What the
  socket delivers meanwhile waits and lands on top of the fetched state.
  A fetch that fails is reported (`snapshot-fetch`) and the client asks
  again, for the snapshot inline. A 10k-task snapshot (772 KB of JSON)
  cost the socket about 4.6 ms of compression per hello; the route
  answers the fetch in about 20 µs, and a reload of an unchanged store in
  3 µs (`npm run bench` has both).
  Also: `store.snapshotJSON()`, `snapshotResponse(store, request)` for a
  server of your own, `request(req, res)` on the Node handlers,
  `httpSnapshot` on `store.session` and `httpSnapshots` on `createHub`

### Changed

- **The Bun adapter fans a broadcast out through the runtime.** Each socket
  subscribes to a topic per store, and a store's patch goes out as one
  `server.publish` that Bun encodes and compresses once for every
  subscriber, rather than a send per socket. A large write to many
  listeners costs a fraction of what it did: a 13 KB patch to 2000 clients
  fell from about 88 ms of event-loop time to about 5 ms here. Presence
  and eviction stay per socket (targeted or opt-out), and a session opened
  on the store directly is still sent to on its own. The Node adapter is
  unchanged. Behind this, `createHub` takes a `channel` and `store.session`
  a `broadcast`, for a transport that can reach every session at once
- `bun test` runs the Bun suites, which now use bun:test (`bunfig.toml`
  scopes the runner to `test/bun`; the rest of the suite is Node's, under
  `npm test`). `npm run test:bun` is the same command

### Fixed

- A row storage adapter that lacks one of its methods (`removeOp`, added
  in 0.7.0, was easy to miss) is refused with a `TypeError` when the
  client is created, rather than failing inside a listener on every op
- A shared connection closes a replica's storage adapter when it lets the
  store go (or is disposed), so an IndexedDB connection does not stay open
  per abandoned store
- A lock manager that refuses the leader request no longer leaves every
  tab waiting for a leader: the tab leads on its own, as it does without
  Web Locks

## [0.9.1] - 2026-09-05

### Changed

- `connect()` on a shared connection also prods the browser's socket when
  it is down, from any tab, so a tab the user comes back to reconnects at
  once rather than at the next backoff step

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
