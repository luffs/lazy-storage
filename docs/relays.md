# Relays

`lazy-storage/relay` is a process between clients and the server: the
clients connect to it as they would to the server, with the same
credentials, and it keeps a copy of every store they open, read once on a
session of its own. It is for two things, and does both at once:

- **Working on when the server is away.** Clients that share a place (the
  screens in a shop, the tablets in a kitchen) keep working together when
  the internet goes, with a relay on their network: it answers them from
  its copies meanwhile, and their edits reach the server as their own when
  it is back.
- **Taking the reads off the server.** A relay reads each store once for
  all its clients and passes every change on to each of them, so the
  server holds a socket a relay rather than a socket a client, and writes
  each change once a relay. Relays in front of the server spread the load
  of many clients over machines, in a data centre as well as on the
  clients' own networks (see [Spreading the load](#spreading-the-load)).

The server stays the judge: it sees each client as itself (its user, its
replica, its presence), judges every op as that client's, and alone
acknowledges anything. A relay never writes as anyone.

## How a relay works

A relay is a **follower store**. For each store its clients open, it keeps
a copy: the server's records with their clocks (its **base**, read on the
relay's own session) and a **queue** of the clients' ops the server has not
acknowledged. What its clients see is the base with the queue replayed on
top, decided by the same rule the server decides by (see [How conflicts
resolve](model.md#how-conflicts-resolve)), so an op is shown to every
client on the relay the moment the relay has it, and when the server
answers, what it decided falls into place record by record. Because the
relay and the server decide alike, the relay's clients see while the
server is away what they will see once it is back, except for what the
server alone can tell (an edit from elsewhere that is newer, a rule only
the server knows).

- **Reads, vouched for.** For each store a client opens, the relay asks
  the server whether that client may read it (`vouch`, with the client's
  credential), and the server judges it by the same `authenticate`,
  `expiresAt`, `authorizeId` and `authorize` as it would the client's own
  socket (handed `{ relay }` as their last argument, so an app can tell).
  A client let in is listed in the store's presence as itself, its replica
  claimed as its own. The relay itself reads nothing of a store until a
  client of it is let in there: the server holds its answer, and every
  change, until then, so a store no client behind it may open never
  reaches the relay or its disk. The relay answers the client from its copy (a delta
  out of the copy's log of its last 1000 changes where that reaches back,
  else a snapshot), and passes every change on to each client.
- **Writes, as their own.** A client's op is applied to the copy at once,
  queued, and sent up the relay's link under the client's name, where the
  server judges and acknowledges it as its author's. The client's ack is
  the server's, passed on, so its outbox holds the op until the server has
  it, and a refusal or a lost change reaches it as it would on a socket of
  its own.
- **Access taken away reaches the clients.** The server's `disconnect(filter)`
  and a client's expiry end its sessions through the relay as they would
  its own socket (4001, and it comes back judged afresh), `revalidate`
  closes a store now refused (`forbidden`), and a store's own eviction
  closes it (`evicted`). A verdict like that is kept offline too: the
  relay no longer answers that client from the copy. A relay the filter
  picks (`disconnect` is handed the relay as the user) is cut off with its
  clients, `relays.expiresAt` ends its link as `expiresAt` does a socket,
  and `relays.authorize(relay, storeId)` says which stores it may carry
  at all. Without it, a relay may open any store its clients are let into
  and writes nothing as itself; with it, it may write as itself
  (`relay.patch`) to the stores it lets the relay carry, judged by the
  rules with the relay as the user.
- **A store the relay may not carry** is answered with the server's word,
  a `closed` with the server's code, for ten minutes, and asked about again
  when a client next opens it after that.
- **A store no client has open is let go of.** After `linger` (default
  30 s) without a client, the relay saves its copy, leaves its session at
  the server, and holds nothing of the store but what storage keeps, so
  the server may unload it. A client that opens it again is answered from
  that copy and the server's delta. A store with ops of the relay's own the
  server has not acknowledged waits for them; `linger: false` keeps every
  store.
- **Vouches turned away are counted.** Every vouch costs the server its
  `authenticate` and `authorize` hooks, so a relay's socket may have only
  so many turned away, for a credential it does not take or a store the
  client may not open (`relays.rate`, default a burst of 100, then 10 a
  second), before every vouch it sends waits: the server answers
  `rate-limited` with `retryAfter`, and the relay asks again then, its
  client waiting meanwhile. Vouches the server lets in cost nothing, so a
  relay whose clients may open what they ask for is never held back.

## Working while the server is away

When the link has not answered for `grace` (ten seconds by default: a
failed dial, a socket that dropped or went silent; a server's deploy is
shorter, and is waited out), or the host says so with
`relay.upstreamDown()`, the relay answers on its own. A client's hello is
answered from the copy, its ops are decided against the copy and applied,
and each change goes to every client on the store, so the clients on the
network stay in step. Nothing is acknowledged meanwhile: each op stays in
its author's outbox, persisted there, and in the relay's queue, until the
server has it. Presence is the clients on the relay. `db.status` is
`online`, `db.relayed` is true (the `relay` event says when it changes),
and `db.pending` grows with the edits the server has not had.

When the link is back (or `relay.upstreamUp()` says so), the relay takes a
snapshot or a delta of each store on its own session, vouches for its
clients again, and sends the queue up in order, each op as its author's.
What the server refuses (a rule, a read-only collection) comes back to
that client as `rejected` and is taken back; what lost comes back as
`lost`; and every other client sees the records it touched fall into
place. A link lost for less than `grace` is not noticed by the clients at
all.

The relay's answers count in its own versions and epoch and say `relay:
true`, so a client that later talks to the server directly asks it for a
snapshot and hears no `reset`. A relay restarted in an outage answers from
its storage (`fileCopies(dir)` keeps one JSON file per store, each written
whole through a rename): the copy, and which clients the server let in.

What a relay cannot do:

- **Know what only the server knows.** An edit from a client elsewhere,
  newer than one made on the site, wins when the server is back, and a
  rule the relay was not given refuses only then. Give the relay the
  store's `rules` and `readOnly` in its shape (`shapeOf`) and it refuses
  offline what the server would by them; a store's `validate` runs on the
  server alone.
- **Acknowledge.** Offline edits live in their authors' storage until the
  server has them: a client wiped during an outage loses the edits the
  others already saw.
- **Answer what the server never answered.** A store the relay has no
  copy of, or a client the server never let in through it on that store
  (by `key`, a fingerprint of its credential, and its replica;
  `authorizeOffline(key, storeId)` may refuse more): the client waits,
  `connecting` on its own cache with its edits pending, as it would with
  no relay.
- **Be one of two.** Two relays for one store's clients split them into
  two islands while the server is away, which meet again only at the
  server.

## Running one

On Bun, `lazy-storage/relay/bun` mounts a relay as the server's adapter
mounts a hub. `credential(req)` says what the server should judge a
client by: the headers and query its own socket would have carried, of
which the server takes only `authorization`, `cookie` and `user-agent`
(and `relays.headers`), never what a proxy or a browser vouches for (an
origin, a forwarded address), which a relay could make up. `key(req)` is a
fingerprint of it, by which the relay knows the client while the server
is away (the relay never reads credentials; they stay in your closure);
without one the relay takes the credential's SHA-256, so what it keeps on
disk and shows in `sockets()` is never the credential itself.
`upstreamSocket(url, { headers })` dials the server's relay route under
the relay's own credential:

```js
import { createRelay, fileCopies } from 'lazy-storage/relay';
import { createRelayHandlers, upstreamSocket } from 'lazy-storage/relay/bun';

const sha256 = text => new Bun.CryptoHasher('sha256').update(text).digest('hex');

const relay = createRelay({
  shapeOf: storeId => (storeId.startsWith('shop-') ? shopShape : null),   // the stores it carries, and their shape
  storage: fileCopies('/data/copies'),                                     // a relay restarted in an outage answers from these
  link: upstreamSocket('wss://central.example.com/sync/relay', { headers: { authorization: `Bearer ${RELAY_TOKEN}` } })
});
const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  maxPayload: 16 * 1024 * 1024,
  key: req => sha256(req.headers.get('authorization') ?? ''),
  credential: req => ({
    headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' },
    query: new URL(req.url).search
  })
});
Bun.serve({
  port: 36610,
  fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }),
  websocket: handlers.websocket
});
relay.on('mode', mode => console.log(`relay: ${mode}`));   // 'through' | 'local'
```

On the server, `relays` turns the route on (`<path>/relay`):

```js
const handlers = createHandlers({
  stores, path: '/sync', authenticate, authorizeId,
  relays: {
    authenticate: req => relayTokens.get(req.headers.get('authorization')) ?? null,   // the relay, as itself
    authorize: (relay, storeId) => storeId.startsWith(`${relay.shop}-`)                 // what it may carry at all
  }
});
```

The clients connect to the relay as they would to the server, with the
same credentials (a process on the same machine as well, over 127.0.0.1).
A browser's shared connection works through a relay unchanged, and its
tabs see `db.relayed` as its replica does. The core is
transport-agnostic: `relay.session({ send, credential, key })` takes any
socket, which is also how `createNetwork` runs one in a test (see
[Testing](development.md#testing)). `relay.copy(id)` shows what the relay
holds of a store, `relay.stats()` how it is doing (per store: its
sessions, its queue and what waits in it, whether it is synced), and
`relay.patch(storeId, diff)` writes as the relay itself, stamped with the
server's time and sent up as the relay's own op, which the server takes
only where its `relays.authorize` lets the relay carry the store (it is
refused with `forbidden` on a server without one).

A browser's tabs use the same follower in the other mode,
`acknowledge: 'local'`: the tabs are one replica, the relay's, and a tab's
op is acknowledged once it is in the relay's persisted queue (see [One
socket per browser](stores.md#one-socket-per-browser)).

## Spreading the load

A server's work on a change is of two kinds. Its own: the gates, the
merge and the commit (see [Persistence](server.md#persistence) for how
commits are grouped). And a write to every socket that hears the change:
`server.publish` fans a patch out without a send per socket from
JavaScript, but the runtime still writes, and compresses past the
threshold, once for each subscriber. The second grows with the clients,
and all of it lands on the one core a store has. Relays take it off the
server: the server writes each change once a relay, and each relay once
for each of its clients, on cores or machines of its own. A server or a
relay that falls behind loses nothing: what was published meanwhile goes
out together, a write a socket, a turn later.

`npm run bench:fanout` measures it (see [Examples, benchmarks, and
tests](development.md)): direct, behind one relay, and behind many. The
figures in the README were measured before 1.0, with relays that fanned
out the same way. Each relay adds a write, and the change's bytes, to
every change, so relays by the hundred (one at every site of a chain) meet
the network before the core. On a single machine, relays on its other
cores do the writing to clients while the server merges and stores:
`examples/podman-caddy` is that layout, behind Caddy.

A relay also takes the reconnect storm: a client's hello is answered from
the relay's copy, a delta out of its log where that reaches back, and the
server sees a vouch rather than a snapshot to send.

What stays with the server:

- **Writes.** Every op is judged and acknowledged by the server as its
  author's; a relay passes them up, it does not decide them.
- **Who is let in.** Every client is vouched for on every store it opens,
  by the server's own `authenticate` and `authorize`: the cost of a
  connect is the server's, the snapshot is not.
- **Presence.** Every client a relay carries is listed at the server as
  itself, and a client that hears presence is sent every change of it.

What a relay with many clients spends its time on is writing to their
sockets: a socket costs a write (a system call, and a TLS record) per send
however little it carries, and a change goes to every client. So the Bun
handlers write what goes to a socket in one turn of the event loop as one
write (`ws.cork`). Measured, that is as far as it goes: a relay with 4000
displays spends the same time on a change whether a turn's patches go as
one message or several, and holding them for 50 ms to write them
together saves it a tenth, for that much more latency. The cost per
socket and change is the runtime's, so a relay with thousands of
displays saturates at a couple of hundred changes a second, and more
displays want more relays. What helps besides is sending fewer messages:
when a store's clients all come back at once, every one of them joining
is a presence change to every other, and `presence.every` on the store
(say 250 ms) makes that a few messages to each rather than one a turn.

Where the relays go is the host's affair: one on each network of clients
(a shop's), or a pool in front of the server that clients are spread over
(DNS, a load balancer). A client needs no particular relay, since each is
vouched for on its own. A relay reads what its clients read, so it runs
where the server's operator trusts it, as itself (`relays.authenticate`),
carrying only the stores `relays.authorize` lets it. One in a data centre
that should not answer on its own while the server is away says so with
`authorizeOffline: () => false`: its clients then wait for the server, as
without a relay.

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

On the server, what a relay's link uses is a session that says
`follower: true` in its hello, which only a session made with `follower:
true` may (a hub made with `vouch` makes them so; a client's hello that
asks is refused). It is answered once a client of it is let in, is sent
the records' clocks with its snapshots, and for each client the transport has vouched for, it holds a
grant (`session.grant(as, user, replica)`), so an op, a share or a leave
that carries `as` is judged as that client's. Any transport can drive one;
`createHub(resolve, { vouch })` does it for a socket of your own.
