# The inspector

What a store holds is JSON in a process, and what it did is rows in its
storage: to see either took a script. The inspector is a page the server
serves of itself, for whoever runs it:

- **Records**: the stores that are live, a store's collections, its
  records (searched by id or by anything in them), and a record with
  the clock the merge decides it by. A record is a tree, coloured by
  kind, its objects and lists folded and unfolded, copied as JSON; and
  it is the record as it is now, shown again when it is written. Where the page is, is in its
  address: a record can be linked to, and the browser's back goes back.
- **History**: every write to a record, newest first, with who and
  when (`2026-11-12 10:16:12`, in the reader's own zone, and how long
  ago), each change as what was there and what is there now, and the
  record as the write left it; and a button that puts the record back so. A deleted
  record is restored, with the children its deletion took.
- **Activity**: the store's writes as they land, narrowed to a user, a
  collection or the deletions.
- **Sessions**: who is connected to the store and what they share (with
  `presence` on); the server's sockets, each with its user, its stores,
  how long it has been open and quiet and how much is unsent to it, and
  a button that closes a user's sockets, for its clients to authenticate
  afresh; and what the store has sent, by message.

```js
import { serve } from 'lazy-storage/server/node';   // or 'lazy-storage/server/bun'

serve({
  stores,
  inspector: { authorize: (request, { write }) => isAdmin(request) }
});
```

It is then at `<path>/inspect`: `http://localhost:3200/ws/inspect` for a
server on its defaults. `node examples/inspector/server.js` runs one with
something to look at.

A revert and a disconnect are asked about in the page before they are
done, a revert with what it will write laid out, field by field. A part
of the page that cannot be filled says why in its own place, with a way
to try again; and when the server stops answering, the page says so,
keeps what it shows, and asks again after a while, a longer while each
time up to thirty seconds.

## Who may look

The inspector reads every record of every store, as the server does, by
no rule of the stores' own. So nothing is open by default: there is no
inspector unless the server is given `authorize`, and a request it does
not let in is answered 401.

`authorize(request, { write })` is given the Web `Request` and says
whether it may look; a falsy answer, or a throw (which is reported to
`onError`), turns it away, and it may answer through a promise. `write`
is true for a request that changes something, a revert or a disconnect:
answer by it to let someone look who may not change.

What it answers is also who is looking. Answer with the user (an object
with an `id`, or a string) and a revert is told in the store's history
as theirs, made through the server; answer `true` and it is told as the
server's own, with nobody to it.

```js
inspector: {
  authorize: async (request, { write }) => {
    const session = await sessionOf(request.headers.get('cookie'));
    const may = session?.role === 'admin' || (!write && session?.role === 'support');
    return may && { id: session.userId };
  }
}
```

The page asks its own server for what it shows, so whatever the browser
sends with a request to it is what `authorize` has to go by: a session
cookie, a header a proxy in front adds, HTTP basic credentials. A token
in the page's address is not sent with what the page asks next.

What else it does to stay an administrator's page:

- The page is one document, its script and style inline, served under a
  Content-Security-Policy that lets nothing else load or run and no other
  page frame it.
- What it shows it writes as text, never as markup: a record is whatever
  a client wrote.
- A change is taken as JSON with a header of the page's own, which a
  form on another site cannot send, and is asked of `authorize` with
  `write: true`.
- Nothing is cached.

## What it needs of a store

It reads through the store's own calls, so what a store keeps is what it
shows. Records, clocks and sessions are always there. History and
activity are a store's [history](history.md): a store made without
`history` says so on the page, and has no revert. Who is connected is a
store's [presence](auth.md#presence-peers-and-eviction): without it the
page counts the sessions and names nobody.

The stores it lists are the ones live in a registry (`createStores`); a
server given a resolver function has no list, and the page takes a store
by its id. Looking at a store loads it, as a client opening it would.

A request costs the server what it looks at, so each is bounded: a page
of records is a hundred, read from the collection's ids sorted once per
version of the store, and a search looks into five thousand records and
says there are more to look in (the page's "Search further"). Of a
server's sockets, and of who is connected to a store, the page is told a
hundred and how many there are in all: the sockets furthest behind
first, those with the store open unless every store's are asked for,
and narrowed to a user by what is typed. What the page asks every two
seconds, to keep its top line, carries none of them.

It is a view for a person, not an interface to build on: for a program,
the same things are `store.state`, `store.historyOf`, `store.activity`
and `store.revert`, or SQL over a [SQLite file](server.md#reading-the-file-with-sql).

## In a server of your own

`createInspector` is what the adapters mount, for a server that is
neither:

```js
import { createInspector } from 'lazy-storage/server';

const inspector = createInspector({ stores, authorize, sockets, disconnect, onError });
const response = await inspector.handle(request, '/admin/stores');   // a Response, or null for a path that is not under it
```

`handle` takes a Web `Request` and the path the inspector is served
under, and answers the page at that path and its requests beneath it.
`sockets()` and `disconnect(filter)` are the server's, as the adapters'
are, for the page's list of sockets and its button; without them the
page says the server tells it of none.
