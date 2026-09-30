# Wire protocol

Messages are JSON objects named by a `t` field. Any number of stores
travel over one socket, so every message except `ping` and `pong` also
carries `store`, the store id; `leave` closes one store's session and
keeps the socket for the others. The shapes are declared as
`ClientMessage` and `ServerMessage` in the typings.

An **op** is one client batch:

```
{ replicaId, seq, ts, diff }
```

`seq` counts the replica's ops from 1, and the server ignores one it has
already merged, so a resend is safe. `ts` is a hybrid-logical-clock
timestamp `[ms, count, replicaId]`. `diff` is a plain lazy-watch diff:
nested objects, `null` for a deletion, and arrays as whole values (of
primitives anywhere, of anything at a register path).

## A session, in order

1. The client opens a store with `hello`: its whole outbox, and where its
   knowledge of the store ends.
2. The server merges those ops, then answers with a `snapshot` or a
   `delta` (chosen as described under [Offline](offline.md)). Either one
   acknowledges the hello's ops through `seq`.
3. From then on each client batch goes out as an `op`. The server answers
   every op with an `ack` or an `error`, and broadcasts every accepted
   diff as a `patch` to every session on the store, the sender included.
4. `presence` arrives whenever a session says hello, ends, or shares
   something new, and a `closed` message ends the store for this client. A `closed` without
   a `store` ends the socket itself: the request did not authenticate.

## Client to server

| Message | Fields | Meaning |
|---|---|---|
| `hello` | `replicaId`, `ops`, `since?`, `epoch?`, `share?`, `presence?`, `fetch?`, `follow?` | Connect or reconnect. `ops` is the outbox (its first 1000 ops); `since` is the store version the client has seen everything up to and `epoch` the storage life it belongs to, asking for a delta; `share` is what this client shares with its peers, restored on reconnect; `presence: false` asks not to be sent presence, and every hello says it again either way (a later hello without it turns presence back on, and brings the whole list); `fetch: true` says the client can fetch a large snapshot over HTTP; `follow: false` (a relay's, for a client it fans out to) makes a write-only session |
| `op` | `op` | One batch, live |
| `leave` | | Close this store's session; the socket stays up |
| `share` | `data` | What this session shares with every peer, a JSON value within `presence.maxShare` that `presence.validate` lets through; `null` clears it. Presence goes out again if it changed; refused with `forbidden` while the store's presence is off |
| `ping` | | Keepalive; answered with `pong` |

## Server to client

| Message | Fields | Meaning |
|---|---|---|
| `snapshot` | `state` or `fetch`, `ts`, `seq`, `registers`, `v`, `epoch`, `lost?` | The whole state, to overwrite with. For a client that can fetch, when the state is `httpSnapshots.threshold` bytes or more: `fetch` names the route to fetch `{ v, epoch, state }` from instead, and `v` is only where the store stood at the hello; the fetched document says where it is |
| `delta` | `patches`, `ts`, `seq`, `registers`, `v`, `epoch`, `lost?` | The accepted diffs since the client's `since`, in order, followed by corrections for what the hello's own ops lost; applied as patches. On either answer, `lost` is `[{ seq, paths }]`: which leaves of which of the hello's ops lost |
| `patch` | `diff`, `ts`, `v` | An accepted diff from any replica, and the version it made |
| `ack` | `seq`, `ts`, `correction`, `lost?`, `v` | The op was merged; `correction` is a diff with the server's values at the leaves it lost, or null, and `lost` the paths of those leaves; `v` the store's version with the op in it |
| `error` | `seq?`, `code?`, `message`, `now?`, `ts?`, `retryAfter?` | With `seq`: that op was refused, for the reason in `code` (below). Without: the message itself was bad (not JSON, an unknown type, a hello without a replica id) |
| `presence` | `peers`, or `left?`, `joined?`, `shared?` | With `peers`: every session as `{ replicaId, user, key, data }`, `key` being what presence groups users by and `data` what the session shares; sent right after the hello is answered, the first one or a later one that turns presence on. Otherwise a delta, applied in the order `left` (replica ids), `joined` (peers), `shared` (`{ replicaId, data }`), batched per `presence.every`. Only with the store's presence on, and never to a session whose last hello opted out |
| `closed` | `code`, `message` | Final for this store on this socket; the client goes offline for it and does not reconnect on its own. Without a `store`: final for the socket, which the server then closes with code 4401; the connection stops reconnecting and every client on it reports the reason |
| `relay` | `status` | From a relay, never a server: `'local'` after each answer it makes from its own copy while the server is away, `'through'` (from a browser's shared connection) once its replica is answered by the server again. The client keeps it as `db.relayed`; a client that predates it ignores it, as it does any message it does not know |
| `pong` | | |

Across these: `ts` is the server's clock on `snapshot`, `delta`, and
`ack`, and the op's own timestamp on `patch`; `seq` on `snapshot` and
`delta` is the last op of this replica the server holds, so the client
can drop acknowledged outbox entries; `registers` are the server's
register patterns, for the client to check against its own; `v` is the
store version the message brings the client up to, and `epoch` the life
of the storage it counts in. An answer a relay makes from its own copy
(a browser's shared connection, or a relay (`lazy-storage/relay`) once its copy holds
offline edits) carries `epoch: null` and versions of the relay's own, and
`seq` 0: it acknowledges nothing, and the next hello the server itself
answers is answered with a snapshot.

## Refusal codes

An `error` with a `seq` names one of these, and the client acts on it
without help from the app, which only hears an `error` event:

| Code | Why | What the client does |
|---|---|---|
| `invalid` | The op breaks the model: an array of objects outside a register, an array fragment, a malformed op | Drops the op and resyncs from a snapshot |
| `forbidden` | A leaf under a read-only path, `validate` refused, or a deletion of a top-level container of `initial` | Same |
| `expired` | Stamped before the retention window | Same |
| `too-large` | More leaves than `maxLeaves` | Same |
| `rate-limited` | Beyond the user's token bucket, or after such a refusal and before the next hello; `retryAfter` says how long in ms | Keeps the op, sends nothing more live, and resends its outbox in a hello after `retryAfter` |
| `clock-skew` | Stamped more than `maxSkew` ahead of the server's clock; `now` is the server's time, `ts` the refused stamp | Adopts the server's time, re-stamps the pending ops, sends them again; no error reaches the app |

## Closed codes

`evicted` (`closeSessions` on the server), `forbidden` (`authorizeId`
or `authorize` refused the store, when it was opened or on `revalidate()`), `unknown-store` (the resolver returned null, or the
store factory threw), and `invalid-store` (an id outside the allowed
alphabet, or a message without one) each end one store. `unauthorized`
(`authenticate` returned nothing) ends the socket: it arrives without a
`store`, and the close that follows carries code 4401 in case the message
did not make it. A socket closed with code 4001 was disconnected by the
app (`disconnect()`) or its session ran out (`expiresAt`), which is not final: the client reconnects, and
`authenticate` decides again. `replica-taken` ends one store: the hello named a
replica another user owns (see [Authentication](auth.md)).
`unavailable` is the one that is not final: the store was
disposed under an open session (a registry released it), or another
process serves it for now (see the lease under
[Persistence](server.md#persistence)), and the client says hello again after a
moment, which loads it afresh; nothing pending is lost and the app hears
no `closed`.

## A relay's link

A relay that fans out (see [One read for many clients](relays.md#one-read-for-many-clients-fan-out))
speaks the store-tagged messages above on its link for its own session on
each store (`hello`, `leave`, and what the store sends it), and these:

| Message | Direction | Fields | Meaning |
|---|---|---|---|
| `vouch` | relay → server | `grant`, `socket?`, `store`, `replicaId`, `credential` | May this client read this store? `grant` is the relay's name for the client on the store, `socket` for its socket; `credential` is `{ headers?, query? }` |
| `unvouch` | relay → server | `grant` | The client left |
| `share` | relay → server | `grant`, `data` | What the client shares, judged as its own |
| `admitted` | server → relay | `grant`, `store`, `peer`, `presence`, `until` | Yes: how presence shows it, whether the store has presence at all (the relay's own session hears it only while a client behind it wants it, and its hellos say so either way), and when its session runs out (or null) |
| `refused` | server → relay | `grant`, `store`, `code`, `message`, `retryAfter?` | No: the code its own socket would have heard (`unauthorized`, `forbidden`, `unknown-store`, `replica-taken`), or `rate-limited` / `unavailable`, which are not final |
| `revoked` | server → relay | `grants`, `code`, `message` | Access taken away: `reauthenticate` (the client's socket is closed with 4001), `forbidden`, `evicted`, `unavailable` (the store unloaded: say hello again) |
| `error` | server → relay | `grant`, `store`, `code`, `message` | A client's share refused |

The relay's own session on a store is closed `unused` when no client
behind it is let in to the store (not final: it says hello again once one
is), and `forbidden` when the relay may not carry the store (its clients
there are then passed through on their own sockets, see above).
