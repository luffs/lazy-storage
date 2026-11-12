# History

A store keeps what its records are, not what they were. Made with
`history: true` it keeps both: every write leaves, for each record it
wrote, the record as it was left, the changes to it, when, and who. That
is what a version history panel, an audit of who changed what, a list of
what was deleted with a way to bring it back, and a feed another system
follows are all read from.

```js
const store = createStore({ initial, history: { keep: 90 * DAY, clients: true }, storage: sqlite.store(id) });
```

Four calls read it, the same on the store and on a client:

| Call | What it answers | Store | Client |
|---|---|---|---|
| `historyOf(c, id, options)` | What happened to this record? Who changed this field? | `store.historyOf` | `db.historyOf` |
| `activity(options)` | What changed in the store since yesterday? What did this user do? What was deleted? | `store.activity` | `db.activity` |
| `recordAt(c, id, when)` | What was this record last Tuesday? | `store.recordAt` | `db.recordAt` |
| `revert(c, id, when)` | Put it back as it was. | `store.revert`, a write of the server's | `db.revert`, an edit of the client's ([not the same](#revert-on-the-store-and-on-a-client)) |

The store's answer at once; a client's are promises of the server's
answer, for the clients the store [lets ask](#clients).

```js
const entries = store.historyOf('tasks', 'k');           // this record's writes, newest first
store.historyOf('tasks', 'k', { path: ['title'] });      // who changed the title, and when
store.activity({ since: yesterday });                    // every record's writes
store.activity({ deleted: true, since: lastWeek });      // what was deleted
store.recordAt('tasks', 'k', lastTuesday);               // the record as it was
store.revert('tasks', 'k', { v: entries[3].v });         // and put back so
```

## Which call

- **A version history of one item**: `historyOf`. It reads that record's
  rows alone, and `path` narrows it to one field.
- **A feed of recent changes, an audit of one user, a list of what was
  deleted**: `activity`.
- **Another system that follows the store** (a search index, a webhook
  sender): `activity({ oldestFirst: true, after: version })`, keeping the
  version of the last entry it handled.
- **Undo for something older than the undo stack, or made by someone
  else**: `recordAt` to show it, `revert` to do it.

## Entries

`historyOf` and `activity` answer with the same entries, a write to one
record each, newest first:

```js
{
  c: 'tasks', id: 'k',          // the record it is of; a singleton has no id
  v: 1042, ts: 1762905600000,   // the store's version the write made, and its time on the store's clock
  replicaId: 'r1',              // the replica that wrote; 'server' for the store's own write
  user: 'u1',                   // its user's key, as presence keys users (user.id unless presence.key says otherwise); none for the server
  changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }],   // what the write did to this record
  record: { title: 'Oat milk' } // the record as the write left it; undefined for a deletion
}
```

An op that wrote three records is three entries of one version. An entry
names what to ask of: `recordAt(e.c, e.id, { v: e.v })` is that entry's
`record`, and `{ v: e.v - 1 }` the record before it.

## Options

Both calls take the same options to say how many, from when, whose, and
where to read on from; each has a few of its own.

| Option | | `historyOf` | `activity` |
|---|---|---|---|
| `limit` | How many entries; 50 unless given | yes | yes |
| `since`, `until` | Times on the store's clock: milliseconds or a `Date` | yes | yes |
| `user` | One user's writes, by the user's key | yes | yes |
| `before` | Older than this version | yes | yes |
| `cursor` | The last entry of an answer: read on after it | yes | yes |
| `path` | Only the writes at, above or beneath this path in the record | yes | |
| `after` | Newer than this version | | yes |
| `collection` | One collection's or singleton's records | | yes |
| `deleted` | `true`: the deletions alone | | yes |
| `oldestFirst` | `true`: oldest first | | yes |

**To read on**, pass the last entry you were given as `cursor`, in either
call:

```js
let page = store.activity({ limit: 50 });
while (page.length === 50) page = store.activity({ limit: 50, cursor: page[page.length - 1] });
```

Nothing is told twice and nothing skipped, though a write of several
records may be split between two answers. A singleton has no id, and
takes its options in the id's place: `store.historyOf('settings', { limit: 10 })`.

`path` exists for one record only: across the store it would read every
row there is.

## The record as it was

`recordAt(c, id, when)` is the record at a time on the store's clock
(milliseconds, a `Date`, `{ ts }`) or at a version (`{ v }`);
`undefined` when it was not there. Two writes may bear the same
millisecond, and a time then means the later of them: a version, from an
entry, tells them apart.

## Revert, on the store and on a client

Both put a record back as it was at a time or a version, writing only
what differs: the fields it had are written, the ones it has gained
since are deleted, and what did not change is not touched, so an edit
someone makes to another field at the same moment still lands. Both are
writes like any other: clients hear a patch, the [schemas](model.md#schemas)
as they are now judge the record, and the revert is in the history
itself, so it can be reverted. They differ in who writes:

| | `store.revert` | `db.revert` |
|---|---|---|
| Who writes | The server, as `store.patch` does | The client: it reads the record as it was and writes it to `db.state` |
| [Rules](auth.md#who-may-write-what) | Not judged, as no write of the server's is | Judged, as any edit of that user's |
| A deleted record | Made again, and with it the children its deletion took (and theirs), each as it was just before, in the same write; `{ children: false }` as a fourth argument makes the record alone | Made again, alone |
| A record that was not there then | Deleted, its children with it | Deleted |
| Undo | By another revert | `db.undo()`, like any edit |
| Answer | The changes that landed | A promise of the record as it was |

An app that wants a client to bring a record back with its children
calls `store.revert` behind an endpoint of its own. `store.revert`
throws when nothing is known of the record then (before the store kept
history), and what a patch throws.

A list of what was deleted, each with a way back:

```js
for (const e of store.activity({ deleted: true, since: lastWeek })) {
  // e.c, e.id: what is gone; e.user, e.ts: who deleted it and when
  // store.revert(e.c, e.id, { v: e.v - 1 }) makes it again
}
```

## Clients

Clients read history when the store lets them, which it does not by
default: what a record was may be exactly what was taken out of it.

```js
createStore({ ..., history: { clients: true } });
createStore({ ..., history: { clients: (user, { c, id, store }) => c === 'tasks' || user.role === 'admin' } });

const entries = await db.historyOf('tasks', 'k', { limit: 20 });
const was = await db.recordAt('tasks', 'k', { v: entries[3].v });
await db.revert('tasks', 'k', { v: entries[3].v });
const feed = await db.activity({ since: yesterday });
```

- `clients: true` lets every client that has the store open read every
  record's history. A function decides per record, with the user the
  socket authenticated as; in `db.activity`, the records it refuses are
  left out of the answer.
- The calls ask the server and resolve to its answer. One request is
  answered with at most 100 entries, and the server reads at most 1000
  rows for it (looking for the writes that touch a `path`, or the
  records a client may read); a call that wants more asks again by
  itself, and each request draws on the [rate limit](limits.md) as an op
  does.
- They reject with an error whose `code` says why: `forbidden`;
  `offline` while the client is not connected (history is the server's,
  and none of it is kept on the client); `unavailable` behind a
  [relay](relays.md) whose server is away; `rate-limited`, with
  `retryAfter`; `invalid` for an option that is not what it says;
  `timeout` when the server has not answered within the client's
  `requestTimeout` (30 s by default).
- A relay, and a browser's tabs sharing [one socket](stores.md#one-socket-per-browser),
  pass the request to the server and the answer back.

On a client `db.clearHistory()` and the `history` event are the undo
manager's, which is why a record's history is `historyOf` and not
`history`, on the client and the store alike.

## What it costs, and how long it is kept

A row per record per write, holding the record whole: a record of 2 KB
written a thousand times is 2 MB of history. On SQLite a one-field write
to a small record took 13 µs with history and 9 µs without, and 20 000
such writes added 3.5 MB. So it is off unless asked for.

- **`keep`** (milliseconds) forgets what is older, at the hourly
  compaction, but for each record's last form before it: `recordAt`
  still answers at the edge, and a record deleted within `keep` can be
  made again however long ago it was last written. A record deleted
  longer ago than `keep` leaves nothing. Without `keep` everything is
  kept.
- **History begins when it is turned on.** A store that already has
  records gets a row for each as it stands (an entry with no `changes`).
  Before that row nothing is known: `recordAt` is `undefined` and
  `revert` throws.
- **A [restore](server.md#backups-restores-and-moving-a-store) starts it
  over**, and `store.export()` does not carry it. A SQLite file's backup
  does.

## Storage

The memory and SQLite adapters keep history. In SQLite the rows are the
table `history` (`store`, `collection`, `id`, `v`, `ts`, `user`,
`replica`, `data`, `changes`), there to be [read with SQL](server.md#reading-the-file-with-sql)
like the rest:

```sql
SELECT user, count(*) FROM history WHERE store = 'team-1' AND ts > ? GROUP BY user;
```

It is keyed by record and version and indexed by version, so a record's
history and the store's activity by version or time are read off an
index; `activity` narrowed to one `user` reads rows until it has enough.

An adapter of your own keeps history by storing the `history` rows a
commit carries and answering `history(query)`, `historyOfVersion(v)`,
`activity(query)`, `historyCount()` and `pruneHistory(before)`; the
header of [storage.js](../src/server/storage.js) says what each takes. A
store made with `history` on an adapter without them throws when it is
made.
