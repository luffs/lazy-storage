# Multiple stores

`createStores(id => store)` is a registry: it builds a store on first use
through your factory (typically `createStore` with a per-id storage
adapter) and keeps it live; `stores.release(id)` disposes one, closing
its sessions, and the persisted rows stay. Store ids are restricted to a
URL- and filename-safe alphabet (`isStoreId`), so an id can name a file or
a table key without escaping.

## Any number of stores over one socket

Every message names its store, so one socket carries as many stores as
the clients attached to it. The server's hub keeps one session per store
per socket, and authorizes each store separately. Several clients, one per
store, share a connection:

```js
import { createClient, createConnection, webSocketTransport } from 'lazy-storage';

const connection = createConnection({ transport: webSocketTransport('wss://example.com/ws') });
const teamA = createClient({ connection, store: 'team-a', initial: { tasks: {} } });
const teamB = createClient({ connection, store: 'team-b', initial: { tasks: {} } });
teamA.connect();
teamB.connect();   // one socket, two stores
```

Each client keeps its own outbox, undo history, and status; `connection.status`
is the socket's. `client.disconnect()` on a shared connection leaves only
that store (the socket stays up for the others), and `connection.close()`
drops the socket for all of them. A client created with a `transport`
instead of a `connection` owns a connection of its own, same protocol. While
open, a connection pings every 30 seconds (`keepalive`, or `false`) so idle
sockets survive proxies and server idle timeouts, and drops and reopens a
socket it has not heard from for two of those intervals (a half-open one
that would otherwise stay `online` with nothing arriving). Reconnects back
off with jitter, so a server restart does not bring every client back in
the same instant; in a browser, a socket that is down tries again at once
when the network comes back (`online`) or the page is looked at again
(`visibilitychange`), rather than at the next backoff step (`wake`, or
`false`).

## One socket per browser

Every tab is a client, and left alone every tab is also its own replica:
its own socket, its own outbox, its own entry in presence. `sharedConnection`
makes a browser one replica however many tabs it has:

```js
import { createClient, sharedConnection, webSocketTransport, localStorageOutbox } from 'lazy-storage';

const connection = sharedConnection({
  name: 'app',                                            // one per app: names the channel and the lock
  transport: webSocketTransport(() => `${url}?token=${token()}`),
  storage: store => localStorageOutbox(`app:${store}`)   // the browser's replica, per store
});
const db = createClient({ connection, store: 'team-1', initial, lists });   // in every tab, as ever
```

The tabs elect a leader with the Web Locks API, and the leader runs the
browser's replica: a hidden client per store on the real socket, persisted
through `storage`, which may be any adapter, `indexedDBStorage` included
(the tabs' messages wait while it opens). The app's own clients, in every
tab including the leader's, follow it over a BroadcastChannel (in the
leader's tab directly): a follower's edits are applied to the replica at
once, socket or no socket, so they sit in the browser's persisted outbox
and go upstream under the browser's replica id, every batch the replica
sees comes back to every tab as a patch, and presence and peers are
passed along (the replica hears presence only while one of its followers
on the store wants it), so the server sees one session per browser. An edit made
offline in one tab is thus in the others a moment later and survives
that tab closing, which a tab with a replica of its own could not offer.
Each tab keeps its own undo history. A tab's `db.status` and `db.pending`
are the browser's: the socket's status, and the replica's unsent ops
(`connection.upstream` and `connection.pending(store)` say the same).
`online` means what it does for a plain client, that the state is
current: with the socket up, the replica answers a tab's hello once it
has the store from the server itself, so a store the browser has not
opened before stays `connecting` until the server's answer is in; with
the socket down, it answers at once from what it has, and the tab says
`offline`.
When the leader tab closes, the next tab acquires the lock, loads the
replica's outbox and state from `storage`, reconnects, and the other tabs
follow it, resending whatever the old leader had not acknowledged. A
store no tab has open anymore is let go after `linger` (default five
seconds, so a reload comes back to it): its client is disposed and its
session on the server ends, while its persisted outbox and cache stay for
the next tab to open it. A tab that closes without a word is found by the
lock it held, which the leader checks every `sweepEvery` (default ten
seconds). Leave the clients' own `storage` at its default: the replica is
what persists.

A page the tab holds in a frame, or a worker, can follow the replica too,
over a MessagePort: `connection.follow(port, [{ store, initial, registers }])`
lets the other end have clients on the stores named and no other, under
the tab's rights, in a session that lives as long as the tab's and that
follows a change of leader; the other end runs an ordinary client on
`portConnection(port)`, with no socket, no token and no storage of its
own, whose `db.status` and `db.pending` are the browser's, as a tab's are
(the host tells it the socket's status and the replica's unsent ops).
`follow` returns what ends it. So a sandboxed iframe gets a live
`db.state` on exactly the store its host means it to have:

```js
// The host tab
const { port1, port2 } = new MessageChannel();
const stop = connection.follow(port1, [{ store: 'team-1', initial, registers }]);
frame.contentWindow.postMessage({ hello: true }, '*', [port2]);

// The page in the frame
const port = (await new Promise(resolve => addEventListener('message', e => resolve(e.ports[0]), { once: true })));
const db = createClient({ connection: portConnection(port), store: 'team-1', initial, registers });
db.connect();
```
`connection.leader` says whether this tab leads, `dispose()` hands
leadership on early, `connect()` also prods the browser's socket when it
is down (for a tab the user just came back to), and what a tab shares (`db.share`) is the browser's
share, the last tab to set it winning; a tab's own entry among `db.peers`
carries the browser's replica id, not the tab's. Where Web Locks or
BroadcastChannel are missing, a tab is its own leader and this is an
ordinary connection.
