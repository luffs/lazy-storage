# Wire protocol

Messages are JSON objects named by a `t` field. Any number of stores
travel over one socket, so every message except `ping` and `pong` also
carries `store`, the store id; `leave` closes one store's session and
keeps the socket for the others. The shapes are declared as
`ClientMessage` and `ServerMessage` in the typings. This is protocol 3
(`PROTOCOL` in `lazy-storage/core`), the one 1.0 speaks.

An **op** is one client batch:

```
{ replica, seq, ts, changes }
```

`seq` counts the replica's ops from 1, and the server ignores one it has
already seen, so a resend is safe. `ts` is a timestamp in milliseconds
(see [Time](model.md#time)). `changes` are the batch's changes, each one
record's:

```
{ c, id?, path?, put | set | del }
```

`c` names a collection or singleton, `id` the record (absent for a
singleton), and `path` the place inside it (absent or `[]` for the record
itself). `put: object` makes or merges an object there, path by path;
`set: value` writes a leaf (an array, a register, a scalar); `del: true`
deletes. Each change is decided on its own (see [How conflicts
resolve](model.md#how-conflicts-resolve)).

## A session, in order

1. The client opens a store with `hello`: its outbox (the first 1000
   ops), and where its knowledge of the store ends.
2. The server decides those ops, then answers with a `snapshot` or a
   `delta` (chosen as described under [Offline](offline.md)). Either one
   acknowledges the hello's ops through `seq`, and says what of them lost.
3. From then on each client batch goes out as an `op`. The server answers
   every op with an `ack` or an `error`, and broadcasts what landed as a
   `patch` to every session on the store, the sender included.
4. `presence` arrives whenever a session says hello, ends, or shares
   something new, and a `closed` message ends the store for this client. A `closed` without
   a `store` ends the socket itself: the request did not authenticate.

## Client to server

| Message | Fields | Meaning |
|---|---|---|
| `hello` | `proto`, `replica`, `ops`, `since?`, `epoch?`, `clocks?`, `share?`, `presence?`, `fetch?`, `follower?` | Connect or reconnect. `proto` is the protocol the client speaks; a server that speaks another answers `closed` with code `outdated`. `since` is the store version the client has everything up to and `epoch` the storage life it belongs to, asking for a delta; `clocks: true` asks for the records' clocks with a snapshot (a client judging its own pending edits does); `share` is what this client shares with its peers, restored on reconnect; `presence: false` asks not to be sent presence, and every hello says it again either way; `fetch: true` says the client can fetch a large snapshot over HTTP; `follower: true` is a relay's own session (below) |
| `op` | `op`, `as?` | One batch, live |
| `leave` | `as?` | Close this store's session; the socket stays up |
| `share` | `data`, `as?` | What this session shares with every peer, a JSON value within `presence.maxShare` that `presence.validate` lets through; `null` clears it. Refused with `forbidden` while the store's presence is off |
| `vouch` | `as`, `credential`, `replica` | A follower's: may this client open the store? (below) |
| `ping` | | Keepalive; answered with `pong` |

## Server to client

| Message | Fields | Meaning |
|---|---|---|
| `snapshot` | `state` or `fetch`, `clocks?`, `seq`, `v`, `epoch`, `now`, `shape`, `lost?`, `relay?` | The whole state, to start from. `clocks` lists `[c, id, clock]` per record when the hello asked. For a client that can fetch, when the state is `httpSnapshots.threshold` bytes or more, `fetch` names the route to fetch `{ v, epoch, state, clocks }` from instead |
| `delta` | `ops`, `seq`, `v`, `epoch`, `now`, `shape`, `lost?`, `relay?` | The changes accepted since the client's `since`, as `[{ v, ts, changes }]`, in order |
| `patch` | `changes`, `ts`, `v` | What landed of an op from any replica, or a server write, with its timestamp and the version it made |
| `ack` | `seq`, `v`, `now`, `lost?`, `as?`, `replica?` | The op was decided; `lost` lists what of it lost as `[{ c, id, path, why }]`, `why` being `missing`, `newer` or `stale`; `v` the store's version with the op in it |
| `error` | `seq?`, `code?`, `message`, `retryAfter?`, `as?`, `replica?` | With `seq`: that op was refused whole, for the reason in `code` (below). Without: the message itself was bad (not JSON, an unknown type, a hello without a replica id) |
| `presence` | `peers`, or `left?`, `joined?`, `shared?` | With `peers`: every session as `{ replicaId, user, key, data }`, sent right after the hello is answered. Otherwise a delta, applied in the order `left` (replica ids), `joined` (peers), `shared` (`{ replicaId, data }`), batched per `presence.every`. Only with the store's presence on, and never to a session whose last hello opted out |
| `closed` | `code`, `message` | Final for this store on this socket; the client goes offline for it and does not reconnect on its own. Without a `store`: final for the socket, which the server then closes with code 4401 |
| `relay` | `status` | From a relay, never a server: `'local'` while it answers from its own copy with the server away, `'through'` once the server answers again. The client keeps it as `db.relayed` |
| `lost`, `rejected`, `reset` | as the client events | From a browser's shared connection: what the server said of an op the browser's replica sent for this tab |
| `pong` | | |

Across these: `now` is the server's time, which the client adopts as the
offset of its clock; `seq` on `snapshot` and `delta` is the last op of
this replica the server holds, so the client can drop acknowledged ops;
`shape` is a hash of the server's declaration (`initial`, `singletons`,
`registers`, `parents`), and a client whose own differs reports an
`error` with code `shape-mismatch`; `v` is the store version the message
brings the client up to, and `epoch` the life of the storage it counts
in. An answer a relay makes from its own copy says `relay: true` and
counts in the relay's own versions and epoch, so the next hello the
server itself answers is answered with a snapshot, and no `reset` is
heard.

## Refusal codes

An `error` with a `seq` names one of these, and the client acts on it
without help from the app: it drops the op, takes its edit back from
`db.state`, and emits `rejected` with the code.

| Code | Why |
|---|---|
| `invalid` | The op breaks the shape: a collection or singleton the store does not have, a change that is not one of put, set or del, a record of the wrong kind, a forbidden key |
| `forbidden` | A rule, `readOnly` or `validate` refused it |
| `expired` | Stamped before the retention window (see [Persistence](server.md#persistence)) |
| `too-large` | More changes than `maxChanges` |
| `rate-limited` | Beyond the user's token bucket, or after such a refusal and before the next hello; `retryAfter` says how long in ms. The client keeps the op, sends nothing more live, and resends its outbox in a hello after `retryAfter`; no `rejected` |

## Closed codes

`evicted` (`closeSessions` on the server), `forbidden` (`authorizeId`
or `authorize` refused the store, when it was opened or on
`revalidate()`), `unknown-store` (the resolver returned null, or the
store factory threw), and `invalid-store` (an id outside the allowed
alphabet, or a message without one) each end one store. `unauthorized`
(`authenticate` returned nothing) ends the socket: it arrives without a
`store`, and the close that follows carries code 4401 in case the message
did not make it. A socket closed with code 4001 was disconnected by the
app (`disconnect()`) or its session ran out (`expiresAt`), which is not
final: the client reconnects, and `authenticate` decides again.
`replica-taken` ends one store: the hello named a replica another user
owns (see [Authentication](auth.md)). `outdated` ends one store: the
hello's `proto` is not the protocol the server speaks (a page left open
across a deploy, or a client from before 1.0), and the app's answer is to
reload. `unavailable` is the one that is not final: the store was
disposed under an open session (a registry released it), or another
process serves it for now (see the lease under
[Persistence](server.md#persistence)), and the client says hello again
after a moment, which loads it afresh; nothing pending is lost and the app
hears no `closed`.

## A relay's link

A relay (see [Relays](relays.md)) dials the server's relay route as
itself and opens each store it carries with a hello that says `follower:
true`; on any other route that hello is refused with `forbidden`. The
server acknowledges what the hello brought but holds its answer, and
every patch and presence, until the first of the relay's clients is let
in there, so the relay reads nothing nobody behind it may. On that session it speaks for clients of its own, each known by a
name the relay gives it on the store, `as`:

| Message | Direction | Fields | Meaning |
|---|---|---|---|
| `vouch` | relay → server | `as`, `credential`, `replica` | May this client open the store? `credential` is `{ headers?, query? }`, what its own socket would have carried |
| `admitted` | server → relay | `as` | Yes: the session holds a grant for the client, and lists it in presence |
| `refused` | server → relay | `as`, `code`, `message`, `retryAfter?` | No: the code its own socket would have heard (`unauthorized`, `forbidden`, `replica-taken`); or `rate-limited`, not a verdict: the relay has had too many credentials turned away, and asks again after `retryAfter` ms |
| `revoked` | server → relay | `as`, `code`, `message` | Access taken away (`evicted`, `forbidden`, a disconnect) |
| `op`, `share`, `leave` | relay → server | `as` | As the client's own, judged as its user's |
| `ack`, `error` | server → relay | `as`, `replica` | For the client's op, routed back to it by `as`, or by `replica` once it has gone |

A `leave` without `as` ends the relay's own session on the store, its grants with it: what a relay sends when it lets go of a store no client has open. What a follower's own session is sent besides is the store's patches and
presence, as any session is, and the records' clocks with every snapshot.
