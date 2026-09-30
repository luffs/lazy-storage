# Offline

Local edits go into an **outbox** first, persisted through a storage
adapter (`localStorageOutbox` in browsers, `memoryOutbox` for tests or
throwaway sessions), and are sent when online. The same adapter caches
the **last state** next to the outbox, so a client restarted while offline,
or before its first snapshot has landed, starts from what it last saw with
its pending edits already applied (`db.restored` says so) rather than from
`initial`; the snapshot on reconnect then brings it up to date. Pass
`cache: false` to keep only the outbox.

An outbox is one replica's, and a replica is one tab: two tabs loading
the same outbox would number their ops alike, and the server would drop
one tab's as duplicates of the other's. So a `localStorageOutbox` key is
held by the tab that loaded it first (a lease it renews while open and
gives up on `pagehide`); a second tab on the key starts a replica of its
own that keeps nothing across a reload, and its `onError` hears
`storage-in-use`. For a browser that is one replica however many tabs it
has, use `sharedConnection` (see
[One socket per browser](stores.md#one-socket-per-browser)).

There are two kinds of storage adapter. A **document** adapter
(`localStorageOutbox`, `memoryOutbox`) keeps the outbox, written with
every op (`localStorageOutbox` keeps it op by op, so an op writes itself
and not every op pending), and the state as one document. The state
costs a serialization of everything, so it is written once changes have
settled for `cacheDelay` (default a second), at least every ten of those
under traffic that never settles, and at once when the page is hidden or
goes away. It may lag safely: it is saved with the store version it
reflects, a restore replays the outbox over it, and the reconnect asks
for what came since. Fine for states up to a megabyte or so. A **row** adapter
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

**Nothing an adapter holds is deleted on its own.** A store the server
closed for us (`unknown-store` after it was deleted, `forbidden` after
we left the team) leaves its outbox and cache where they are, and so does
a replica retired by `sharedConnection`; the same codes can come from a
misconfigured server or a lapsed token, and pending edits are worth more
than the kilobytes. Forgetting a store is the app's call: when the user
leaves a team, or on `closed` with a code the app knows is final, call
the adapter's `clear()` (`localStorageOutbox`, `memoryOutbox`) or
`destroy()` (`indexedDBStorage`) for that store, after disposing the
client. Under a `sharedConnection`, the adapter is what `storage(storeId)`
made, so `storage(storeId).clear()` does the same for the browser's
replica.

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
