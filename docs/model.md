# The model

State is a tree of plain objects and JSON leaves, and **lists are objects
keyed by id**. That single rule is what makes offline edits mergeable: a
change addresses a record by identity, so two people adding, editing, or
removing different records never collide, and an edit made on a stale
replica still lands on the record it meant.

Arrays are **whole values**. An array of primitives (a set of tags, a list
of ids, a pair of coordinates) may live anywhere; it is written and merged
as one leaf, so the newer array wins as a unit. An array holding objects
is refused, on the client (reverted and reported through the `error`
event) and on the server, because a list of records with unit semantics
would lose one person's add whenever another person reordered. A list of
records is a keyed map with a position on each record, below. The
`registers` option remains for storing an array of anything as one value
on purpose: a `*` segment matches any one segment, so `tasks/*/subtaskOrder`
declares one register per task, and a client whose declaration differs
from the server's is told so.

## Lists

An ordered list of records is a keyed map whose records each carry a
**position key**, and `db.list(path)` is its ordered view:

```js
const tasks = db.list('tasks');                    // state.tasks, an object keyed by id
tasks.all()                                        // records sorted by position, ties by id
const id = tasks.add({ title: 'Milk' });           // at the end; or { before: id }, { after: id }, { at: index }
tasks.move(id, { at: 0 });
tasks.remove(id);
db.list(['tasks', id, 'subtasks']).add({ title: 'Skimmed' });   // nested lists are just paths
```

An add or a move writes one field, `pos`, on one record, and nothing else
in the list changes. So two people inserting at the same spot while apart
both keep their records, in the same order everywhere (ties fall to the
id); a move never loses to an unrelated edit; and a deletion is just the
record's deletion. The server needs no declaration and holds no order
register. Keys are short strings that sort as strings: appending counts
up (`a0`, `a1`, ... `az`, `b00`), and only inserting between two existing
keys lengthens one, by a character every few inserts at the very same
spot.

An app that keeps its own array (a drag-and-drop list, a Vue store) hands
the order back with `tasks.reconcile(ids)`, which writes the fewest
positions that make the sort agree: the longest run of records already in
order keeps its keys. Records without a position sort last until placed.

### Plain arrays

Declare the list paths and the client presents them as arrays:

```js
const db = createClient({ connection, store: 'team-1', initial: { tasks: [] }, lists: ['tasks', 'tasks/*/subtasks'] });
db.state.tasks.push({ title: 'Milk' });             // an id is minted and written back
db.state.tasks.splice(1, 0, { title: 'Eggs' });
db.state.tasks[0].done = true;
db.state.tasks[0].subtasks = [{ title: 'Skimmed' }];
db.state.tasks = db.state.tasks.filter(t => !t.done); // or sort, reverse, reorder
```

`db.state` is then a view: the same tree with every list as a real array
in position order, records carrying their `id` and no position. The synced
state underneath, `db.wire`, keeps the keyed maps with positions, and
everything else (persistence, deltas, undo, `db.list`) works on it as
before. The client keeps the two in step: an edit inside a record maps its
index to the record's id; a splice, a reorder, or a replaced array resyncs
that list by id, deleting what left, adding what appeared, and writing the
fewest positions that make the wire order match. Changes from others
arrive as splices at their sorted place, moves, and field patches, tagged
`origin: 'remote'` for the app's listeners. A record pushed without an id
gets one in the next batch; read it back from the array rather than from
the object you pushed. A record read from the array (`const task =
db.state.tasks[i]`) stays that record wherever a sort, a splice, or
another client's move takes it, and so does a listener on it
(`LazyWatch.on(task, …)`); `splice` hands back the records it removed,
so the usual move (`tasks.splice(j, 0, ...tasks.splice(i, 1))`) keeps
them. A list path cannot also be a register.

### With a UI framework

Two entries wrap the client for Vue and React; the pattern behind each
is a few lines for any other framework.

`lazy-storage/vue` keeps a reactive mirror. Vue cannot track `db.state`
(a remote patch lands underneath any proxy Vue wraps around it, and
nothing re-renders), so `useClient(db)` returns a plain copy kept
reactive and patched in place on every batch, local or remote, together
with refs for the client's status, presence, outbox size, closed reason,
and undo state:

```js
import { useClient } from 'lazy-storage/vue';

const { state, status, presence, canUndo } = useClient(db);   // in setup()
db.state.tasks.push({ title: 'Ship it' });                    // writes go to the client; the mirror follows
```

In the Options API the same call fills `data()`
(`data() { return { ...useClient(this.db), title: '' } }`). There is one
mirror per client, however many components call `useClient`: the first
makes it, the rest share it (a list of two hundred rows holds one copy
of the state and patches it once per batch), and it stops following the
client when the last of them unmounts. The mirror is read-only; writes
go to `db.state`.

`lazy-storage/react` needs no mirror: `useClient(db)` subscribes through
`useSyncExternalStore`, so a component reads `db.state` directly and
re-renders on every batch, outbox change, and event, with one
subscription per client however many components use it:

```jsx
import { useClient } from 'lazy-storage/react';

function List({ db }) {
  const { state, status } = useClient(db);
  return <ul>{state.tasks.map(task => <li key={task.id}>{task.title}</li>)}</ul>;
}
```

A component that reads a little of the state (a row of a long list, a
badge) picks it with `useClientSelector(db, select, isEqual)` and
re-renders only when that changes, not on every batch, share, or status
event. The selection comes out as plain data (the state's own records
keep their identity as they change, so they could never compare
unequal), compared deeply unless `isEqual` says otherwise, and stays the
same object until it changes, so it is safe in a dependency list:

```jsx
import { useClientSelector } from 'lazy-storage/react';

function Row({ db, id }) {
  const task = useClientSelector(db, state => state.tasks.find(t => t.id === id));
  return <li>{task?.title}</li>;
}
const status = useClientSelector(db, (state, db) => db.status);
```

Five hundred rows on `useClient` all re-render for an edit to one of
them, or a peer moving a cursor; on `useClientSelector` one row does,
and none for the cursor (`npm run bench:client`).

A view that keeps rows of its own (a data table, a search index, a
cache keyed by id) follows a collection record by record instead:
`db.collection('screens').watch(changes => ...)` hears, once per batch,
every record under `state.screens` the batch changed, as `{ type:
'insert', id, record }`, `{ type: 'update', id, record, previous }` or
`{ type: 'remove', id, previous }`. `record` and `previous` are plain
copies, new objects each time, so a view that compares rows by identity
sees the changed one, and `previous` is the record as it was before the
batch, for a filtered list that wants to know whether a row just started
or stopped matching. The batch's `meta` comes second, `origin: 'remote'`
for the server's:

```js
const stop = db.collection('screens').watch((changes, meta) => {
  for (const change of changes) table.apply(change);   // insert, update, remove by id
});
```

An app that keeps its own arrays instead, as a Vue store or a
drag-and-drop list does, overwrites the view's arrays with them after a
change (`LazyWatch.overwrite(db.state.tasks, plainTasks)`) and copies them
back on remote, undo, and redo batches; lazy-storage diffs the arrays by
id and writes only what changed. The todo app at
github.com/luffs/kimi-wa-best-todo is that shape in full.

## How conflicts resolve

Every op carries a hybrid-logical-clock timestamp. The server keeps the
timestamp of the last accepted write per leaf path and decides per leaf:

- A **write** wins if nothing at that path is newer. Two people editing
  different fields of the same record both land; editing the same field,
  the later timestamp wins everywhere, regardless of who reconnects first.
- A **deletion** wins if nothing at or below the path is newer. It leaves a
  tombstone, so an older edit that arrives later cannot resurrect a
  partial record. A newer edit of a single field of a deleted record is
  also refused (a deleted record does not come back as one field); a
  newer *record* write, an object with an `id`, re-adds it. The server's
  own `store.patch` is never held back this way: it is the authority, not
  a replica that may be stale, and whatever it writes at a deleted path
  re-adds it, `id` or not, so a record the server recreates under a key it
  had deleted (a process that came back, an entry rebuilt from another
  source) needs no ceremony. `store.apply`, a replica's op taken on trust,
  is judged like any other.
- An **array** is one leaf, whether an array of primitives anywhere or a
  declared register: the newest whole value wins. A list of records is
  not an array on the wire (see [Lists](#lists)), so its records merge
  one by one.

A client whose op lost receives a correction with the server's values and
falls back in line. The server is the only merge point, which is what
keeps this small: clients never merge with each other, and old tombstones
can be forgotten on a schedule (see [Persistence](server.md#persistence)).

### What happened to my edit

The state always shows what won, so an app that only renders needs
nothing more. One that wants to tell its user hears it:

```js
db.on('conflict', ({ seq, lost }) => {
  // lost: [{ path: ['tasks', 'x', 'title'], mine: 'Buy milk', theirs: 'Buy oat milk' }]
  // theirs is null where the record was deleted under the edit
});
db.on('rejected', ({ seq, code, message, diff }) => {
  // the server refused the op (forbidden, expired, invalid, too-large), or
  // the model refused the batch locally (seq null): dropped, the state back in line
});
db.on('reset', ({ previous }) => {
  // the server's storage started over (a backup put back): the state is
  // now the server's, and previous.state is what this client showed
});
db.isPending('tasks/x/title');   // an edit not yet acknowledged writes at, under, or over the path
```

A conflict is reported when the op's acknowledgement arrives, or, for an
op made offline, when the reconnect is answered. Paths are the synced
state's: under a list declared as an array, a record is addressed by its
id (`['tasks', id, 'title']`). `isPending` changes when the outbox does,
which the `sync` event announces (and `useClientSelector` follows). Under
a `sharedConnection`, a tab's own edits are acknowledged once the
browser's replica holds them, and every tab hears the replica's
conflicts, refusals, and resets.

Clocks are hybrid logical clocks, so a replica whose wall clock runs slow
is pulled forward by whatever it receives. A clock that runs *fast* would
win every conflict and drag the server's clock with it, so the server
refuses any op stamped more than `maxSkew` (default five minutes) ahead of
its own time, telling the client the server's time. The client adopts it
as an offset, re-stamps its pending ops, and sends them again; nothing is
lost, and the app hears nothing about it.

## Undo

Each client has a lazy-watch undo manager attached with a `record` filter
that declines remote batches, so undo and redo only ever touch this
replica's own edits, and they sync like any other edit. Because records
are keyed, a teammate's insert does not invalidate your history; only a
change of shape under a path you edited (an array replaced whole, a record
replaced by a leaf) drops the affected steps. With lists declared, undo
runs on the synced state and shows in the array view like any other change.

## Scope

This is last-writer-wins per field with a single merge point. It gives
records identity and makes offline editing safe; it does not merge
concurrent edits to the *same* field (the later one wins) and it is not a
text CRDT. For collaborative documents, embed a purpose-built library for
the document and keep the surrounding state here.

Authentication is a hook, not a system: lazy-storage asks you who a request
is and whether they may open a store, and stores the answer on the session.
Users, tokens, and memberships stay in your application.
