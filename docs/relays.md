# Relays

`lazy-storage/relay` is a process between clients and the server: the
clients connect to it as they would to the server, with the same
credentials, and it keeps a copy of every store they open. It is for two
things, each alone or both at once:

- **Working on when the server is away.** Clients that share a place (the
  screens in a shop, the tablets in a kitchen) keep working together when
  the internet goes, with a relay on their network: it answers them from
  its copies meanwhile, and their edits reach the server as their own when
  it is back (below).
- **Taking the reads off the server.** Given a `link` of its own to the
  server, a relay reads each store once for all its clients and passes it
  on to each of them (see [One read for many clients](#one-read-for-many-clients-fan-out)),
  so the server holds a socket a relay rather than a socket a client, and
  writes each change once a relay. Relays in front of the server spread
  the load of many clients over machines, in a data centre as well as on
  the clients' own networks (see [Spreading the load](#spreading-the-load)).

The server stays the judge either way: it sees each client as itself (its
session, its replica, its presence), judges every op as that client's, and
alone acknowledges anything. A relay never writes as anyone.

- **While the server answers, the relay passes the clients' traffic on.**
  Without a `link`, it is a pipe: each client's socket is dialled through
  to the server with that client's own credentials, and every frame goes
  up and comes down as it came; neither the server nor the clients change.
  With a `link` it fans each store out instead (see [One read for many
  clients](#one-read-for-many-clients-fan-out)), and a client's edits still
  go up a socket of the client's own; a client without a credential, or a
  link the server turns away, is passed through as without one. On the way
  past, the relay builds its copy of every store from the snapshots,
  deltas and patches it passes on (fanned out: from its own session on the
  store). So that it sees them, it asks for a large snapshot inline rather
  than over HTTP, and asks for a snapshot rather than a delta where its
  copy could not use the delta.
- **When the server has not answered for `grace`** (ten seconds by
  default: a failed dial, a socket that dropped or went silent; a
  server's deploy is shorter, and is waited out), or the host says so
  with `relay.upstreamDown()`, the relay answers on its own. A failure
  is one socket's word, so the relay asks every other socket it passes
  through (a ping), and a server that answers any of them is there; a
  client the server never answered counts for nothing, so one whose own
  way to the server is broken does not send the rest local. A client's
  hello is answered from the copy, its ops are merged into the copy
  last-writer-wins as the server would, and each applied op goes to every
  client on the store as a patch, so the clients on the network stay in
  step. Nothing is acknowledged: every answer's `seq` is 0, there is no
  `ack`, no refusal and no `lost`, so each op stays pending in its
  author's outbox, persisted there, until the server has it; the relay
  keeps no queue of anyone's ops. Presence is the clients on the relay,
  shown as the server showed their users. `db.status` is `online`,
  `db.relayed` is true (the `relay` event says when it changes), and
  `db.pending` grows with the edits the server has not had.
- **When the server answers again** (the relay dials it every
  `probeEvery`, five seconds, with a client's credentials, or the host
  says so with `relay.upstreamUp()`), the relay waits a random moment
  (`jitter`, up to a second, so that many relays do not bring their
  clients back in one instant) and closes its clients' sockets, and they
  reconnect through it to the server, each outbox in its own hello, under
  its own identity: what
  the server refuses (an access rule, a read-only path) comes back to
  that client as `rejected` and is reverted, what lost to a newer write
  raises `conflict`, and every other client is sent a snapshot that
  reverts whatever it saw of either.

Versions and epochs make that last step exact. While nobody has edited a
store offline, the relay's answers carry the server's own epoch and
version, so the way back is a delta, not every client fetching every
store at once. The first offline edit makes the copy the relay's own: its
clients are told to say hello again (`closed` 'unavailable', which a
client does by itself), and from then on the answers carry epoch `null`,
which asks the server for a snapshot when it is back and raises no
`reset` either way. A client that saw the server further along than the
relay did (the relay restarted from copies it had written a moment
before) is not rolled back: it keeps its state and is sent the offline
edits alone. A client under an epoch the copy never held (a restore the
relay missed) may be the newer of the two, so it is not answered and
waits for the server; one under an epoch the copy moved on from missed a
restore the relay saw, and hears the `reset` as the server would say it.

What a relay cannot do:

- **Judge an op as the server would.** An edit the server will refuse
  shows on the network until the server is back and refuses it. The
  merge's clocks are known from the patches seen since the last snapshot,
  so an offline write may win where the server would keep a newer one;
  the server decides again. `validate` lets the host refuse offline what
  it can tell; a deletion of a top-level container is never applied
  offline, since the relay does not know the server's skeleton.
- **Acknowledge.** Offline edits live in their authors' storage until the
  server has them: a client wiped during an outage loses the edits the
  others already saw.
- **Answer what the server never answered.** A store the relay has no
  copy of, a credential the server never let in through it on that store
  (`authorizeOffline` decides otherwise), or a replica id the server
  never answered with that credential: the client waits, `connecting` on
  its own cache with its edits pending, as it would with no relay. A
  credential may have several replicas on a store (a device's windows,
  each a client of its own): the 32 answered last are kept. A socket
  whose hellos on a store named two replicas has neither recorded, since
  the server answers one and refuses the other without saying which.
- **Take a clock's word.** An offline op stamped more than `maxSkew`
  ahead of the relay's reference time (the server's clock as last heard,
  carried on by the time elapsed since, or the relay's own if that is
  later) is not applied, and stays pending: one fast clock would
  otherwise pull every client's along, and the server would refuse them
  all. The relay stamps nothing of its own. It writes the server's clock
  every minute, and a relay restarted in an outage carries it on from
  there, not counting the time it was off: one off for longer than
  `maxSkew`, on a machine whose clock is wrong at boot (no clock
  battery, no network), holds its clients' offline edits back until the
  server is back.
- **Be one of two.** Two relays for one store's clients split them into
  two islands that meet again only at the server.

On Bun, `lazy-storage/relay/bun` mounts a relay as the server's adapter
mounts a hub. `key(req)` is a fingerprint of the request's credential
(the relay never reads credentials; they stay in your closure) and
`upstream(req)` dials the server with that credential and the same path
and query; `upstreamSocket(url, { headers })` does that on Bun's
WebSocket, keeping frames as they came:

```js
import { createRelay, fileCopies } from 'lazy-storage/relay';
import { createRelayHandlers, upstreamSocket } from 'lazy-storage/relay/bun';

const CENTRAL = 'wss://central.example.com';
const sha256 = text => new Bun.CryptoHasher('sha256').update(text).digest('hex');

const relay = createRelay({ storage: fileCopies('/data/copies') });   // a relay restarted in an outage answers from these
const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  maxPayload: 16 * 1024 * 1024,
  key: req => sha256(req.headers.get('authorization') ?? ''),
  upstream: req => upstreamSocket(CENTRAL + '/sync' + new URL(req.url).search, {
    headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' }
  })
});
Bun.serve({
  port: 36610,
  fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }),
  websocket: handlers.websocket
});
relay.on('mode', mode => console.log(`relay: ${mode}`));   // 'through' | 'local'
// The hub's own link to the server, if it has one, can say what it knows:
// link.on('down', () => relay.upstreamDown()); link.on('up', () => relay.upstreamUp());
```

The clients connect to the relay as they would to the server, with the
same credentials (a process on the same machine as well, over 127.0.0.1,
which the relay answers offline too once the server has let it in). A
browser's shared connection works through a relay unchanged, and its tabs
see `db.relayed` as its replica does. The core is transport-agnostic:
`relay.accept({ send, close, key, upstream })` takes any socket, which is
also how `createNetwork` runs one in a test (see [Testing](development.md#testing)).
`relay.copy(id)` shows what the relay holds of a store, `stats()` and
`sockets()` how it is doing, and `fileCopies(dir)` keeps one JSON file
per store and one for the credentials, each written whole through a
rename, at most once a second (`saveDelay`).

## One read for many clients (fan-out)

Passed through, every client is a socket of the server's, and every
change goes to each of them. Many clients behind one relay (a chain's
thousands of screens, each shop's behind its own relay) are better read
once: give the relay a `link` of its own to the server, and each store is
read ONCE, on the relay's own session, and passed on to every client
behind it. The server stays the judge of who reads what and who wrote
what:

- **Reads, vouched for.** For each store a client opens, the relay asks
  the server whether that client may read it (`vouch`, with the client's
  credential), and the server judges it by the same `authenticate`,
  `expiresAt`, `authorizeId` and `authorize` as it would the client's own
  socket (handed `{ relay }` as their last argument, so an app can tell).
  A client let in is listed in the store's presence as itself, its replica
  claimed as its own; the relay's session on a store is let in only while
  some client of it is. The relay answers the client from its copy under
  the server's epoch and version (a delta out of the copy's log of the
  server's last 1000 patches where that reaches back, else a snapshot),
  and passes every patch on to each client in version order, encoded once.
- **Writes, as their own.** A client's edits never go through the relay's
  session: its ops (and a hello that carries some) go up a socket of the
  client's own, dialled with its credential as a pass-through socket is,
  as a write-only session (see below). The server judges and acknowledges
  each op as its author's, as ever; the relay could not write as anyone if
  it tried. An ack is held until the client has heard every patch up to
  the ack's version, so it lands where it would on a direct socket (one
  heard after later patches has its correction read from the copy, which
  is where the server stood by then). The socket up closes once it has
  had nothing to wait for for `writeIdle`.
- **Access taken away reaches the clients.** The server's `disconnect(filter)`
  and a client's expiry end its sessions through the relay as they would
  its own socket (4001, and it comes back judged afresh), `revalidate`
  closes a store now refused ('forbidden'), and a store's own eviction
  closes it ('evicted'). A verdict like that is kept offline too: the
  relay no longer answers that credential from the copy. A relay the
  filter picks (`disconnect` is handed the relay as the user) is cut off
  with its clients, `relays.expiresAt` ends its link as `expiresAt` does
  a socket, and `revalidate` judges the relay itself on every store it
  carries (`relays.authorize`).
- **A store the relay may not carry is passed through.** Where
  `relays.authorize` turns the relay's own session down, that says
  nothing of its clients: each client's session on that store goes up the
  client's own socket (the one its edits take) as a whole session, as a
  socket passed through does, and the server judges it as the client's
  own, so one that may read the store reads it and one that may not hears
  `forbidden` from the server. The relay keeps nothing of the store and
  answers it from no copy offline; after ten minutes it asks to carry it
  again, when a client next opens it. `relay.stats().link.passed` counts
  those sessions.
- **Offline is as before.** The link failing is the server failing: after
  `grace` the relay answers on its own, and on the way back every client
  that sent an op meanwhile, or whose copy took offline edits, says hello
  again, so its outbox reaches the server in order. A link lost for less
  than `grace` is not noticed by the clients at all: they are vouched for
  again and brought along from where they were. A server that answers the
  clients' own dials but not the link (one without relays, a proxy that
  refuses the route), or turns the link away (4401, tried again five
  minutes later or at `upstreamUp()`), has the relay pass every socket
  through as before until the link answers.

On the server, `relays` turns the route on:

```js
const handlers = createHandlers({
  stores, path: '/sync', authenticate, authorizeId,
  relays: {
    authenticate: req => relayTokens.get(req.headers.get('authorization')) ?? null,   // the relay, as itself
    authorize: (relay, storeId) => storeId.startsWith(`${relay.shop}-`)                 // what it may carry at all
  }
});
```

and the relay dials it (`<path>/relay`), saying for each client what the
server should judge it by: the headers and query its own socket would
have carried, of which the server takes only `authorization`, `cookie`
and `user-agent` (and `relays.headers`), never what a proxy or a browser
vouches for (an origin, a forwarded address), which a relay could make up:

```js
const relay = createRelay({
  storage: fileCopies('/data/copies'),
  link: upstreamSocket(CENTRAL + '/sync/relay', { headers: { authorization: `Bearer ${RELAY_TOKEN}` } })
});
const handlers = createRelayHandlers({
  relay, path: '/sync', key, upstream,
  credential: req => ({ headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' }, query: new URL(req.url).search })
});
```

A client without a credential is passed through as before, beside the
others. The server's `sockets()` lists a relay's socket with `relay` and
`grants`, and each client it carries after it with `via`; `socketStats()`
counts `relays` and `clients`; `relay.stats().link` says how the link is
(`open`, `down`, `broken`, `refused`), how many stores and clients are
on it, and how many clients' sessions it passes through on stores it may
not carry.

What a relay with many clients spends its time on is writing to their
sockets: a socket costs a write (a system call, and a TLS record) per send
however little it carries, and a patch goes to every client. So the Bun
handlers write what goes to a socket in one turn of the event loop as one
write (`ws.cork`): the patches and presence the link brought together
reach each client together, at the price of that turn. A topic would not
help here, since Bun's `publish` makes the same writes to the same sockets.
What helps besides is sending fewer messages: when a store's clients all
come back at once (the relay's link back, a server restarted), every one
of them joining is a presence change to every other, and `presence.every`
on the store (say 250 ms) makes that a few messages to each rather than
one a turn, and so does an app that writes something of each client as
it comes (a last-seen time) gathering those into one patch. Measured with
a thousand clients of one store connecting through a relay over TLS, the
app stamping each one's arrival: the relay's CPU for it went from some
14 s to 6 s with the writes joined, and to about 1 s with the stamps
gathered and presence every 250 ms.

## Spreading the load

A server's work on a change is of two kinds. Its own: the gates, the
merge and the commit, some 70 µs a write on a busy 4-core Linux server
(see [Persistence](server.md#persistence) for how commits are grouped). And a write
to every socket that hears the change, some 13 µs each on the same server:
`server.publish` fans a patch out without a send per socket from
JavaScript, but the runtime still writes, and compresses past the
threshold, once for each subscriber. The second grows with the clients,
and all of it lands on the one core a store has: with 4000 clients on
sockets of their own, one change costs that core some 50 ms. Relays with a
link take it off the server: the server writes each change once a relay,
and each relay once for each of its clients, on cores or machines of its
own. A server or a relay that falls behind loses nothing: what was
published meanwhile goes out together, a write a socket, a turn later.

Measured with `npm run bench:fanout`, the server alone on that 4-core
machine and the relays and clients on another on the same network: behind
ten relays, one store on SQLite took 15 000 writes a second, each reaching
every client at a p99 of some 50 ms, and delivered 18 000 with the
server's core full. Each relay adds a write, and the change's bytes, to
every change, so relays by the hundred (one at every site of a chain) meet
the network before the core: with 200 relays and patches of some 300
bytes, a gigabit link was full at some 2000 changes a second. On a single
machine, relays on its other cores do the writing to clients while the
server merges and stores: `examples/podman-caddy` is that layout, behind
Caddy.

A relay also takes the reconnect storm: a client's hello is answered from
the relay's copy, a delta out of its log where that reaches back, and the
server sees a vouch (below) rather than a snapshot to send.

What stays with the server:

- **Writes.** A client's ops go up a socket of the client's own, judged and
  acknowledged by the server as ever; a relay does not spread them.
- **Who is let in.** Every client is vouched for on every store it opens,
  by the server's own `authenticate` and `authorize`: the cost of a
  connect is the server's, the snapshot is not.
- **Presence.** Every client a relay carries is listed at the server as
  itself, and a client that hears presence is sent every change of it.
  A relay's own session hears the store's presence only while a client
  behind it wants it, so clients that say `presence: false` spare the
  server and the relays that traffic.

Where the relays go is the host's affair: one on each network of clients
(a shop's), or a pool in front of the server that clients are spread over
(DNS, a load balancer). A client needs no particular relay, since each
socket is vouched for on its own; it keeps its socket's relay for the
socket's life. A relay reads what its clients read, so it runs where the
server's operator trusts it, as itself (`relays.authenticate`), carrying
only the stores `relays.authorize` lets it. One in a data centre that
should not answer on its own while the server is away says so with
`authorizeOffline: () => false`: its clients then wait for the server, as
without a relay. Relays spread over a pool are also each other's islands
while the server is away (see what a relay cannot do, above): a store's
clients on two relays see each other's offline edits only once the server
is back.

**Compression, hop by hop.** Compression is paid for once per socket and
message, by whichever process writes to the socket, and the Bun adapters
negotiate it without context takeover: each message is compressed on its
own, so a patch of a few hundred bytes hardly shrinks, and a snapshot
shrinks several-fold. So set it per hop, by what that hop's bandwidth is
worth:

| Hop | Relay on the clients' network | Relays next to the server, spreading its load |
|---|---|---|
| Server → relay | Compress: the site's uplink is the slow part, and there are few links (the server's default, 1 KB, suits it) | Don't: loopback or the data centre. `perMessageDeflate: false` on a server that only relays connect to spares the store's core |
| Relay → clients | Don't: the LAN has the bandwidth (the relay's default compresses only what passes 64 KB) | Compress what is big, for clients on slow links: `perMessageDeflate: { threshold: 4096 }` on the relays, paid for on their cores, not the store's |

What a client on a slow link waits on most is a snapshot: one coming back
within the delta log is sent the patches it missed instead (a relay's copy
keeps a log too), and small patches and `presence.every` keep the stream
itself light. `examples/podman-caddy` is the second column.

Two things the store does for this, which any transport can use. A
relay's own session (`store.session({ relay: true })`) hears the store and
its presence, is nobody's peer, and writes nothing. And
`store.peer({ user, replicaId, via })` is a client a relay serves: listed,
claimed, judged, sent nothing, and closed by `closeSessions` and `dispose`
like any session. A hello with `follow: false` is a write-only session,
answered but hearing no patches or presence after, and unlisted; it is
heeded only for a replica such a peer holds (anyone else's is closed
'unavailable'), so nobody reads a store unlisted.
