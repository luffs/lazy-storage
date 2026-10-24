# The model

A store is **collections of records**, and **singletons**. A collection
is records keyed by id; a singleton is one record without an id; and a
record is a JSON object of any depth. The server and every client
declare this top level alike, in `initial`:

```js
const shape = {
  initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } },
  singletons: ['settings'],                 // one record, not a collection
  parents: { subtasks: { taskId: 'tasks' } } // a subtask names its task
};
const store = createStore({ ...shape, storage });       // the server
const db = createClient({ ...shape, store: 'team-1', transport });   // every client
```

A key of `initial` is a collection when it is `{}` (or holds records),
and a singleton when `singletons` names it or its value is not an
object (`build: ''`). A client cannot delete a collection or a
singleton, only what is in it. Records a collection holds in `initial`
are its seeds: a new store writes them when it starts, and from then on
they are records like any, so one deleted stays deleted. A client whose
declaration differs from the server's hears an `error` with code
`shape-mismatch`, and passes over what the server sends of a collection
or singleton it does not declare.

## Records, paths and what merges

Every path inside a record merges on its own. Two people who change
`tasks.k.meta.color` and `tasks.k.meta.size` at once both keep their
change; two who change the same path keep the later one. So nest what
belongs to the record, at any depth, and it merges as finely as it is
written.

Three things refine that:

- **An array is a leaf.** It is written and merged whole (`tags`, a pair
  of coordinates, a list of ids) and may hold anything. A list that
  several people add to at once belongs in a keyed map with positions
  (below), where each add is a record of its own.
- **A register is a leaf.** A path declared in `registers`, with `*` for
  any one segment, is written and merged whole even when its value is an
  object: `tasks/*/geometry` keeps a window's x and y together, and
  `events/*` makes each record of `events` a value of its own. Assigning
  a whole object outside a register replaces it too (the keys it lacks
  are deleted), but each of its paths then merges with what others
  write meanwhile.
- **What has a life of its own is a collection.** Records of a
  collection are added, moved and deleted on their own. A child names
  its parent in a field, and `parents` declares it: `subtasks` whose
  `taskId` names a task are that task's children. The server never lets
  a child in while its parent is missing, and deletes the children with
  the parent.

## Lists

An ordered list of records is a keyed map whose records each carry a
**position key**, `pos`; its order is the keys' string order, ties by
id. `db.list(path)` reads and writes one, a collection or a map at a
path inside a record, and `inOrder(map)` gives a map's records in order,
for rendering:

```js
const tasks = db.list('tasks');                       // a collection
tasks.all();                                          // the records in order
const id = tasks.add({ title: 'Milk' });              // at the end; or { before: id }, { after: id }, { at: index }
tasks.move(id, { at: 0 });
tasks.remove(id);
db.list(['state', 'lists', 'items']).add({ text: 'Eggs' });   // a list inside a singleton
inOrder(db.state.tasks);                              // the same order, from any map: a client's, a framework's mirror
```

`db.list(path)` is `listIn(db.state, path)`, and `listIn` takes any
state: a framework's writable view of the client, a mirror's copy, or a
plain object whose changes go to the server with `store.patch`. A list
that starts from an array, in `initial` or as a default, is
`listFrom(records)`: each record keyed by its `id` (minted where it has
none) with positions in the array's order:

```js
import { listIn, listFrom } from 'lazy-storage';

createStore({ initial: { steps: listFrom([{ id: 'plan', title: 'Plan' }, { id: 'do', title: 'Do' }]) } });
listIn(view, ['state', 'lists', 'items']).add({ text: 'Milk' });
```

An add or a move writes one field, `pos`, on one record, and nothing
else in the list changes. So two people inserting at the same spot while
apart both keep their records, in the same order everywhere; a move
never loses to an unrelated edit; and a deletion is the record's
deletion. Keys are short strings: appending counts up (`a0`, `a1`, ...
`az`, `b00`), and only inserting between two keys lengthens one, by a
character every few inserts at the very same spot. An app that keeps an
array of its own (a drag-and-drop list) hands the order back with
`tasks.reconcile(ids)`, which writes the fewest positions that make the
sort agree.

## How conflicts resolve

Every op carries a timestamp: milliseconds, stamped by its client so
that an edit made after seeing something is always newer than it, and
clamped by the server to its own time, so a clock that runs fast wins
nothing (see [Time](#time)). Each record has a **clock**: when the
record was made, with what it held (its **birth**, `at`, the time of
every path inside it that has none of its own); for a path written or
made since, when; and, for a path deleted inside a living record, when
it was deleted. So a record written whole costs one timestamp, and one
more for each path edited after. The server decides each change of an op
on its own, against that clock:

- A **write** lands when its record and its parent are there, and
  nothing at its path is newer. One that replaces a container also needs
  nothing beneath it to be newer.
- A **deletion** lands when nothing at or beneath its path is newer; for
  a record, its children in other collections count as beneath it. So a
  deletion made offline at nine does not take away what someone edited
  at ten.
- A container that was not there before (a new record, a new nested
  object) is **made**, with what it holds, unless something deleted at
  its path is newer.
- Every change also needs to be no older than the birth of the
  containers above it: an edit made before a record was made again is an
  edit of the record that was deleted, and is not taken.

A deleted record leaves nothing behind: its row and its clock go. A
write to it finds no parent, and a record made again under the same id
has a new birth that older edits lose to. Only a deleted path inside a
living record keeps its time, in the record's clock. Every time in a
clock is kept until it is older than the retention window: an op older
than that is refused unjudged, so nothing can lose to it any more, and
it goes at the next compaction. A record nobody has touched for a
window carries no times at all.

What lost is reported, with why: `missing` (the record, the parent, or a
child's parent record was not there), `newer` (a later write), or
`stale` (made before the container was made again). One gap remains,
and it is narrow: an edit made offline, after a record was deleted and
made again under the same id, by someone who never saw either, lands on
the new record. Time cannot tell what the writer had seen. With random
ids it cannot happen, and for an undone deletion it is what one wants.

### What happened to my edit

`db.state` always shows what won, so an app that only renders needs
nothing more. One that wants to tell its user hears it:

```js
db.on('lost', ({ seq, lost }) => {
  // lost: [{ c: 'tasks', id: 'x', path: ['title'], why: 'newer' }]
});
db.on('rejected', ({ seq, code, message, changes }) => {
  // the server refused the op whole (forbidden, expired, invalid, too-large), or
  // the shape refused the batch here (seq null): taken back
});
db.on('reset', ({ epoch, previous }) => {
  // the server's storage started over (a backup put back): previous.state is what this client showed
});
db.isPending('tasks/x/title');   // an edit not yet acknowledged writes at, beneath, or above the path
```

### The client: base and pending

A client keeps the server's state apart from its own edits. Its
**base** is the state as the server last told it, with each record's
clock; its **outbox** holds the ops the server has not acknowledged.
`db.state` is the base with the outbox replayed on top, judged as the
server will judge it, so an edit of one's own that loses to a newer one
already falls away when the newer one arrives, and an edit to a record
someone deleted goes with the record. When a patch arrives, the records
it touched are worked out again and written into `db.state`, so the app
sees only what changed; when an ack arrives, its op leaves the outbox
and its records are worked out again. `db.base` and `db.clockOf(c, id)`
show what the client is working from.

### Time

A timestamp is a whole number of milliseconds. A client stamps with
`max(wall clock + offset, highest seen + 1)`: `offset` comes from the
server's `now` in every snapshot, delta and ack, so a clock that runs
slow is corrected, and `highest seen` is the largest timestamp it has
received, so its edits sort after everything it has seen. The server
clamps a timestamp ahead of its own time to its own time and stamps its
own writes with its own time. Nothing is ever re-stamped.

## Undo

Each client has a lazy-watch undo manager with a `record` filter that
declines remote batches, so undo and redo only touch this replica's own
edits, and they sync like any other edit. An undone deletion makes the
record again. An undone write writes the old value with a new time, and
so overwrites a teammate's later write of the same path.

## With a UI framework

Two entries wrap the client for Vue and React; the pattern behind each
is a few lines for any other framework.

`lazy-storage/vue` keeps a reactive mirror. Vue cannot track `db.state`
(a remote patch lands underneath any proxy Vue wraps around it), so
`useClient(db)` returns a plain copy kept reactive and patched in place
on every batch, together with refs for the client's status, presence,
outbox size, closed reason, and undo state:

```js
import { useClient } from 'lazy-storage/vue';
import { inOrder } from 'lazy-storage';

const { state, status, presence, canUndo } = useClient(db);   // in setup()
// <li v-for="task in inOrder(state.tasks)" :key="task.id">
```

There is one mirror per client, however many components call
`useClient`, and it stops following the client when the last of them
unmounts. A component that edits what it shows asks for a writable state
instead (`useClient(db, { writable: true })`): reads come from
`db.state`, so a write is read back at once, and each read touches the
same path in the shared mirror, which is what Vue tracks.

`lazy-storage/react` needs no mirror: `useClient(db)` subscribes through
`useSyncExternalStore`, so a component reads `db.state` directly and
re-renders on every batch, outbox change and event.
`useClientSelector(db, select, isEqual)` re-renders a component only
when what it selects changes:

```jsx
function Row({ db, id }) {
  const task = useClientSelector(db, state => state.tasks[id]);
  return <li>{task?.title}</li>;
}
```

A view that keeps rows of its own follows a collection record by
record: `db.collection('tasks').watch(changes => ...)` hears, once per
batch, every record of `tasks` the batch changed, as `{ type: 'insert',
id, record }`, `{ type: 'update', id, record, previous }` or `{ type:
'remove', id, previous }`, plain copies each time.

## Scope

This is a merge per path with a single merge point. It gives records
identity and makes offline editing safe; it does not merge two edits of
the same value (the later one wins), and it is not a text CRDT. For a
collaborative document, embed a library made for it and keep the state
around it here. Authentication is a hook, not a system: lazy-storage
asks who a request is and whether they may open a store, and what they
may write, record by record (see [Who may write what](auth.md#who-may-write-what)).
