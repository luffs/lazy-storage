# Limits, memory, and observability

A public server needs a few ceilings, all on by default:

- **Message size.** `maxPayload` on the Bun adapter (default 4 MB) is the
  largest message a socket may send; Bun ends a socket that exceeds it. A
  hello carries at most 1000 ops (the server merges no more), the rest
  following in the next hello once the answer lands, so a long offline
  spell stays well under it.
- **Compression.** Both adapters offer the permessage-deflate extension
  by default, and a client that takes it (browsers do) receives large
  messages compressed: the JSON of a big store shrinks about tenfold
  (`npm run bench` reports by how much), which is what a client waits on
  at connect and reconnect. Messages under `threshold` bytes (default
  1024, set through the object form of `perMessageDeflate`) go plain,
  since compressing a hundred-byte patch costs ten times its encoding
  and makes it larger. What it costs: about 3 ms of CPU per megabyte of
  snapshot per client, paid per socket, unlike the encoding, which is
  done once, so a reconnect storm of a thousand clients spends a few
  seconds compressing (and sends a tenth of the bytes); and on the Node
  adapter, a socket that has received a compressed message keeps a
  deflate stream of some 75 KB for its lifetime, one that has sent one
  an inflate stream of some 100 KB. `perMessageDeflate: false` turns it
  off.
- **Fan-out.** A store's patch reaches every session on it, so a write to
  a store with many listeners is the cost that grows first. On the Bun
  adapter each socket subscribes to a topic per store and a patch goes out
  as one `server.publish`, encoded once and fanned out by the runtime
  rather than sent per socket from JavaScript; a session opened on the
  store directly, without the adapter, is still sent to on its own. The
  runtime still writes to each socket, on the event loop once the publish
  has returned, and compresses for each one what passes the threshold: on
  one Windows machine some 12–20 µs a socket for a small patch, and some
  30 µs more to deflate 4 KB, so a patch to 4000 sockets is some 50–80 ms
  of the loop. Patches published in one turn go out together, a write a
  socket (see `npm run bench:fanout`), and a relay in front of each group
  of clients takes the writes off the server (see [One read for many
  clients](relays.md#one-read-for-many-clients-fan-out)). The Node adapter sends per socket. Either way, egress still
  grows with the listeners, so a big write to a big audience is bandwidth
  the uplink has to carry; splitting a large value into records, so a
  patch carries only what changed, is what keeps it small.
- **Slow sockets.** A patch to a store is queued for every socket on it,
  and a client that stops reading (a phone gone underground, a stalled
  tab) holds everything queued for it. `maxBuffered` on either adapter
  (default 16 MB) closes a socket whose unsent output passes it, with
  code 1013; the client reconnects and its hello catches up with a delta,
  so nothing is lost. A socket that never answers the close is dropped a
  second later. On Bun the check runs on every send and, since a store's
  patch goes out as one topic publish Bun fans out itself, once a second
  over every socket; `false` turns it off, leaving Bun to drop what it
  cannot buffer (the client then notices the gap and catches up the same
  way, on its next patch). `sockets()` and `socketStats()` show how far
  behind the sockets are (see below).
- **Snapshots over HTTP.** A hello answered with a large snapshot costs
  the wire a compression of the whole state per socket (about 5 ms per
  megabyte) and a copy of it into that socket's send buffer. Both adapters
  serve snapshots at `<path>/snapshot/<store id>` instead, compressed once
  per change (brotli, or gzip for a client without it; brotli packs this
  JSON some 15% tighter in the same time) and kept until the next: a
  client that can fetch (one on
  `webSocketTransport`, by default) says so in its hello, and a snapshot
  of `httpSnapshots.threshold` bytes or more (default 64 KB) is answered
  with where to fetch it rather than the state. The route sits behind the
  same `authenticate`, `authorizeId` and `authorize` as the socket, and the response
  carries an ETag, so a reload that finds the store unchanged costs a 304.
  Patches keep flowing on the socket meanwhile; the client lays the ones
  newer than the snapshot it fetched on top of it. A fetch that fails (a
  proxy that does not pass the route, a cookie session across origins) is
  reported as an error with code `snapshot-fetch`, and the client asks
  again, for the snapshot inline. `httpSnapshots.origins` says which
  origins may fetch it cross-origin: `'*'` by default (credentials travel
  in the URL as they do for the socket, and a browser withholds cookies
  from a cross-origin request under `'*'`, so a cookie session stays
  same-origin), an array of origins to echo and no other, or `false` for
  no CORS header at all. `httpSnapshots: false` on the adapter,
  or `fetch: false` on the transport, keeps every snapshot on the socket;
  `snapshotResponse(store, request)` serves the route from a server of
  your own.
- **Op size.** `maxLeaves` on the store (default 10 000) is the most leaves
  one op may touch; a larger one is refused with code `too-large`, and
  the client drops it and resyncs.
- **Op rate.** `rateLimit` on the store is a token bucket per user (by
  presence's `key`, so minting replica ids does not refill it; per
  replica for a session without a user), `{ burst: 500, perSecond: 100 }`
  by default. A live op beyond it is refused with code `rate-limited` and
  a `retryAfter` in milliseconds, and so is every later live op of that
  session until its next hello; the client stops sending, keeps the ops,
  and resends its outbox in a hello after `retryAfter`, in order, so
  nothing is lost and a runaway client is throttled rather than broken.
  A hello costs one token and the ops inside it are not counted. `false`
  turns it off.
- **Presence rate.** Presence is off unless a store asks for it. On, it
  travels as deltas, one small message to every session per change,
  batched per turn of the event loop; `presence.every` (milliseconds,
  default 0) caps that at one message per window, `presence.maxShare`
  (default 4096 bytes of JSON) bounds what a session may share, and a
  client that never reads presence opts out of receiving it. A room of
  N sessions then costs N small messages per window however many of
  them join, leave, or share.

**Memory follows the stores in use** when the registry is given an idle
time: `createStores(factory, { idle: 30 * 60_000 })` releases a store
that has had no session for that long (its storage is flushed first) and
loads it again on the next request. Off by default, since a store on
`memoryStorage` would lose its data. Idle counts sessions, and a server
that writes to a store is not one: a writer holding a reference across
the sweep is left with a disposed store, whose next `patch` throws and
says so, while the next `get` builds a fresh instance the writer never
reaches. With `idle` set, resolve the store with `stores.get(id)` before
each write (which also resets its idle clock), or keep the stores a
server writes out of the registry.

**Watching a store.** `store.observe('op' | 'refused' | 'session', fn)`
reports every merged op once it is stored (`{ replicaId, seq, user,
accepted, rejected, version, diff, ms }`, `diff` what the op changed as
merged and broadcast, read-only, `ms` the time the store spent on it:
the gates and the merge; its commit, grouped with the turn's, and its
broadcast come after),
every client op turned away (`{ replicaId, seq, user, code,
message }`), and sessions opening and closing, for logs, audits, and
metrics; `store.stats()` counts version, sessions, replicas, rows,
tombstones, and the delta log, plus `sent`, what the store has sent by
message type (`patch`, `ack`, `snapshot`, `delta`, `presence`,
`http-snapshot`, …) as `{ messages, bytes }`: deliveries, a broadcast
once per session it reached, and their size in UTF-8 bytes of JSON,
before compression (an HTTP snapshot's as served, compressed), so what
goes over the wire is less. Bytes are read from the encoding the socket
sends, never encoded for the count, so a transport that hands on objects
(the in-memory network of `lazy-storage/testing`) counts messages, and
bytes only for what the store encodes itself (broadcasts, snapshots).
The counts run from when the store was loaded: a store a registry
released for being idle starts over when it is loaded again, which a
counter-based metrics system reads as a reset. `stores.stats()` on a
registry rolls all of it up across the live stores, `sent` included,
with how many are live and how many idle, for a health endpoint. Both adapters list their open sockets:
`server.sockets()` gives each one's `user`, the `stores` it has open,
`buffered` (bytes queued for it and not yet sent: how far behind it is),
`idleMs` (since it last sent anything; a client pings every 30 s),
`openMs` and `expiresAt` (when its session runs out, or null), and
`server.socketStats()` rolls them up (`sockets`, `buffered`, `largest`,
`cutOff`, the sockets closed for passing `maxBuffered`, and
`disconnected`, `expired` and `revoked`, what `disconnect()`, session
expiry and `revalidate()` closed; see
[Authentication](auth.md)):

```js
app.get('/status', (req, res) => res.json({
  stores: stores.stats(),
  sockets: server.socketStats(),
  lagging: server.sockets().filter(s => s.buffered > 64 * 1024)
}));
```

The numbers are plain values, so a dashboard is a few lines in whatever
the server already runs; with `prom-client`, say:

```js
import client from 'prom-client';
const opMs = new client.Histogram({ name: 'lazystorage_op_ms', help: 'op merge time (ms)', buckets: [1, 5, 20, 100, 500] });
store.observe('op', ({ ms }) => opMs.observe(ms));
new client.Gauge({ name: 'lazystorage_patch_bytes', help: 'patch bytes sent', collect() { this.set(store.stats().sent.patch?.bytes ?? 0); } });
new client.Gauge({ name: 'lazystorage_buffered_bytes', help: 'largest unsent socket buffer', collect() { this.set(server.socketStats().largest); } });
```

Server faults that are nobody's request
(a store factory that throws, an observer that throws, a bug while
handling a message) go to an `onError` option on `createHandlers`,
`createHub`, and `createStore`, which defaults to the console.
