# Authentication, presence, and eviction

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
  in again, `db.connect()` (or `connection.connect()`) is the way back,
  from inside the `closed` event itself if it has credentials by then:
  the transport factory runs afresh, so a URL built by a function carries
  the new token. Called with the same token, it is refused again at once,
  with no backoff in between, so reconnect on `closed` only with a new one.
- `authorizeId(user, storeId)` runs per store, before the store is even
  loaded; a refusal arrives as a `closed` message with code `forbidden` and
  affects only that store, while the socket stays up for the others. It is
  the check to use whenever it needs only the user and the id: a store is
  loaded (all its rows, into memory, until a registry's `idle` lets it go)
  only for a user it lets through, and a refused id is answered the same
  whether the store exists or not, so nobody learns which ids do.
  `authorize(user, storeId, store)` runs after it, once the store is
  loaded, for a check that needs the store itself; given alone, every id
  asked for is loaded before it is judged. All three hooks may return
  promises.

The hooks run when a socket opens and when it first asks for a store.
After that an open socket keeps its user and its stores for as long as
it stays up, which may be hours, so when something changes that should
end that, tell the server:

- `server.disconnect(user => ...)` closes the sockets of the users it
  picks (every socket, without a filter) with code 4001. That is not
  final: the client reconnects as after any drop, `authenticate` runs
  again, and every store it asks for is authorized again. So one call
  covers a logout, a deleted account and a changed role: a user whose
  session is gone is turned away with `unauthorized`, and one whose role
  changed comes back under the new one. Nothing reaches the closed
  sockets or is taken from them once the call returns; an edit sent
  meanwhile stays in the client's outbox and lands after the reconnect.
  Returns how many sockets it closed.
- `await server.revalidate((user, storeId) => ...)` runs `authorizeId`
  and `authorize` again on the open store sessions it picks (all of
  them, without a filter), as the user each socket authenticated as, and
  closes those now refused with `forbidden`, as a refusal at open would
  be; the socket stays up for its other stores. That is the one for a
  change to who may open a store, a member taken off a team, say.
  Resolves to how many it closed.

```js
function logout(sessionId) {
  sessions.delete(sessionId);
  server.disconnect(user => user.sessionId === sessionId);   // back to authenticate, which now says no
}

async function removeMember(teamId, userId) {
  await db.removeMember(teamId, userId);
  await server.revalidate((user, storeId) => user.id === userId && storeId === teamId);
}
```

`revalidate` judges with the user object the socket authenticated with,
so it sees a change only where the hooks read it from its source (the
team's members, not a role copied onto the user at upgrade). When what
changed is the user, `disconnect` them instead.

Credentials that run out get the same treatment on their own with a
third hook, `expiresAt(user, req)`: when the session `authenticate` let
in ends (ms since the epoch or a `Date`; nothing for never), read at
upgrade. The server closes the socket then, as `disconnect` does, and
the client reconnects with whatever credentials it has by then: a URL
built by a function carries a refreshed token, a cookie session that
has lapsed is turned away with `unauthorized`. A session with less than
`minSession` left (default 30 s) is turned away at once rather than let
in to be closed again in a moment.

```js
serve({
  stores,
  authenticate: req => sessionFor(req)?.user ?? null,
  expiresAt: (user, req) => sessionFor(req).expiresAt
});
```

How long a session lasts is the app's choice; each end of one costs a
reconnect, whose hello is answered with a delta (usually empty), and
the client's `status` passes through `connecting` for that round trip, so
debounce a "reconnecting" banner rather than show it at once. Where
expiry is what locks out a revoked user, 5 to 15 minutes is usual; with
`disconnect` on logout, an hour or more is fine. `socketStats()` counts
all three, as `disconnected`, `expired` and `revoked`, and `sockets()`
gives each socket's `expiresAt`.

A replica belongs to the user who first says hello with it (by
presence's `key`, or the user's `id`), recorded with its progress and
kept across restarts. Give the user `authenticate` returns an `id` that
is the same in every session: without one (and without presence's
`key`) a user is told apart by its whole value, so a session token or
an expiry in it makes the same person another user at every sign-in,
and the browser's replicas from the last session are refused. The store
says so once, through `onError`. A session speaks for its own replica only, and
another user cannot take one over, connected or away, so a replica id
seen in presence is of no use to anyone else: such a hello is closed with
`replica-taken`. That is also what a browser meets when someone else
signs in with the last user's storage, so key the storage by user
(``localStorageOutbox(`app:${user.id}:${store}`)``); on `replica-taken`
the app decides what becomes of the pending edits and opens the store
with storage of its own. Sessions without a user own nothing.

Opening a store is not the same as writing to it, so the store itself
decides what a client may write:

- `readOnly: true` locks the whole store: clients only read, and every op
  they send is refused with code `forbidden`.
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
  sends it none of this, though it may still share and is still listed
  in the others'. `db.wantPresence(on)` changes it while the client runs
  (only an admin's screen reads who is online, say, and a user signs in
  as one): the client says hello again, answered from where it stands,
  and one that turns presence on is sent the whole list, as a newcomer
  is; off, `db.presence` and `db.peers` empty at once. A browser's shared
  connection hears presence only while one of its tabs wants it, and a
  relay's own session only while a client behind it does.
- **Peers.** With presence on, every live session is a peer,
  `{ replicaId, user, key, data }`, where `data` is whatever that client
  chose to share: `db.share({ editing: taskId })` sets it, `null` clears
  it, and the hello carries it, so a reconnect restores it. Shared data
  is never written to the store; it lives as long as the session and
  goes out as it changes (throttle a cursor yourself: a share draws on
  the user's `rateLimit` bucket like an op, and beyond it is refused
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
