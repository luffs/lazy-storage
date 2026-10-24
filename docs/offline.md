# Offline

A client keeps two things: its **outbox**, the ops the server has not
acknowledged, and its **base**, the state as the server last told it,
with each record's clock and the version it is at. What the app sees,
`db.state`, is the base with the outbox replayed on top (see [The client:
base and pending](model.md#the-client-base-and-pending)), so a reload
builds it again the same way. Both go through a storage adapter
(`localStorageOutbox` in browsers, `memoryOutbox` for tests or throwaway
sessions), and a client restarted while offline, or before its first
snapshot has landed, starts from what it last saw with its pending edits
already on top (`db.restored` says so) rather than from `initial`; the
answer to its hello then brings it up to date. Pass `cache: false` to keep
only the outbox.

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
and not every op pending), and the base as one document. The base costs a
serialization of everything, so it is written once changes have settled
for `cacheDelay` (default a second), at least every ten of those under
traffic that never settles, and at once when the page is hidden or goes
away. It may lag safely: it is saved with the version it reflects, a
restore replays the outbox over it, and the reconnect asks for what came
since. Fine for states up to a megabyte or so. A **row** adapter keeps
one row per record and one per pending op, so a patch costs the records
it touched and only a snapshot touches everything:
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
we left the team) leaves its outbox and base where they are, and so does
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
paths it writes, since those older writes could never decide a value
again. Typing into one field keeps one op pending rather than one per
keystroke, and deleting a record drops its pending edits. Nothing is
re-stamped, so an older write to another field keeps its own time and
the server decides exactly as if every op had been sent.

On (re)connect the client sends the outbox in one `hello` (up to a
thousand ops, the rest once it is answered), together with the version
and epoch it last saw (`db.version`). The server decides the ops and
answers with a **delta**: the changes accepted since that version, in
order, each with its timestamp, and what the hello's own ops lost. A
reconnect after a network blip therefore costs a few small messages, and
a reconnect that missed nothing costs one empty one. The server keeps the
last `deltaLog` accepted changes (default 1000) for this, persisted by
the SQLite and memory adapters so a restart or a deploy still answers
with deltas, and falls back to a full **snapshot**, the state and every
record's clock, when the log does not reach back far enough or when the
client's version belongs to another life of the storage (storage wiped
and re-seeded, a backup put back). A snapshot is encoded straight from
the state and the encoding kept until the next accepted op, so a burst of
first connections pays for one encoding, not one per client. Edits made
while the hello was in flight stay in the outbox and are sent after it.
Nothing is lost on a reload while offline: the replica id, sequence
numbers, pending ops, and the version are restored with the outbox, and
the server ignores an op it has already seen.

A replica away for longer than the server's **retention window** (30 days
by default) gets its old ops refused: an op stamped before the window is
refused whole as `expired`, since the deletions it might bring back and
the replica's own progress have been forgotten, and nothing can tell
whether it was already applied. The client drops each refused op, takes
its edit back from `db.state`, and the app hears `rejected` with code
`expired` for each (see [What happened to my
edit](model.md#what-happened-to-my-edit)). What the client writes after
it is back is stamped then and lands as any other edit; a replica that
was online within the window is never caught by this.
