# Examples, benchmarks, and tests

What runs from a checkout of the repository.

## Examples and benchmark

`examples/` holds small, complete programs that run from a checkout: a
shared list in the browser over a Bun server (`bun examples/basic/server.js`),
the same page served by Node (`node examples/node/server.js`), and a
client that follows a store from a Bun or Node process
(`bun examples/mirror.js`). See `examples/README.md`.

`npm run bench` times the paths that matter: deciding an op with and
without the gates, a broadcast to a hundred and a thousand sockets, a client's
local op on each kind of storage, a reconnect answered with a snapshot
versus a delta, and a snapshot compressed per socket versus served by the
HTTP route. It reports the median of several rounds; compare
runs on the same machine.

`npm run bench:fanout` (Bun) is how a store that every client follows
behaves when the server publishes to it: thousands of thin clients over
real sockets, all but a few admins' without presence, straight to the
server, behind one relay, or spread over many (`--topologies
direct,hub,hubs`, `hubs:off` for a store without presence), each part a
process of its own. It steps the rate up until a patch is late by more
than two seconds, and reports what each patch took from when it was due
and from when it went out, and each part's CPU; then 4 KB patches and a
burst in one turn. Then its clients write to it: `--writers` of them
write the store between them, rate by rate, as live ops on their own
sockets (through a relay, up its link as their own), every
client hearing each op as it hears the server's patches and its author
timing its ack; the server's CPU for each op is all a write costs it, the
fan-out included (`--n 200 --writers 200 --phases write` for the write
path with few readers). Every phase says how many of the machine's
logical CPUs the processes kept busy between them, and flags one past
half of them MACHINE BUSY: the relays and the clients share the machine
with the server, and a phase that fills it measures the machine
(`--topologies hubs --n 400 --hubs 200` shows the server behind many
relays with room to spare). To give the server a machine of its own, run
`bun bench/fanout/agent.js` there and `--remote <its host>:36700` here:
the agent starts a fresh server for each topology, the relays and the
clients stay here, and the server's clock is measured against this one so
their latencies hold across the two. `bun bench/fanout/run.js --n 4000`
for the defaults' size; see the header of `bench/fanout/run.js` for the
rest.

To see how many clients a deployment takes, rather than the server alone,
`--url wss://<host>/<path>` runs the clients by themselves against it,
through whatever stands in front (Caddy, relays), `--clients
1000,5000,10000` at a time, over `--stores` stores, each client named in
the query by `--auth-param` as the deployment's `authenticate` reads it
(`--insecure` takes a local certificate). Each step says how long its new
clients took to be answered, writes at `--write-rates` as every client of
a store hears them, and runs a reconnect storm: every client dropped at
once and back as a client comes back after a drop, with how long until
all were answered again, and whether with deltas. What the deployment
spends meanwhile is its own machine's to watch. One machine runs out of
ports to one address at some 16 000 sockets on Windows and 28 000 on
Linux; past that, more machines.

`npm run bench:client` times what an app feels in the browser, in a DOM
from happy-dom: React and Vue components on `useClient` as the store
changes (with how many of them re-render), a 5k-record list under
moves, adds and remote edits, read in order with `inOrder`, the
`localStorageOutbox` under a long offline spell (with how much it
serializes), and lazy-watch's cost for an object write against a field
write.

## Testing

```bash
npm test              # unit and integration tests plus fixed-seed runs of the fuzzers (Node)
npm run test:bun      # the Bun adapters (the server's and the relay's) and the bun:sqlite adapters (Bun; `bun test` runs the same)
npm run fuzz          # a longer randomized convergence campaign; a failure prints the seed
npm run fuzz:hostile  # the same with an attacker beside honest clients
node test/fuzz/run.js --mode relay   # clients behind a relay whose link drops, beside clients on the store
npm run test:coverage # the Node suite under c8, failing below 96% of lines and statements, 88% of branches, 92% of functions
npm run bench:check   # both benchmarks, failing when a case is over its ceiling or a count over its bound
```

CI runs all of these. The benchmark ceilings are about ten times a
laptop's medians, so only a regression of the kind worth catching trips
them; the client cases' counts (a selector's renders, a mount's state
copies, what the outbox writes) are held exactly, since no machine
changes them.

The hostile fuzzer gives a signed-in attacker a socket of its own and
lets it send anything: ops under other replicas' ids and the server's,
seqs far ahead, timestamps far ahead and far behind, reserved names as
collections, ids and path segments, changes of every wrong shape,
deletions of collections and singletons, oversized ops and hellos, a
follower's hello and ops under clients nobody vouched for, shares,
churn, and garbage. After every step it checks that `Object.prototype` is
untouched, the server's own writes land, no honest client hears an error
or diverges, the collections and singletons stand, nothing was loaded
that nobody may open, sessions do not leak, and at the end that the store
on disk equals the store in memory.

The convergence fuzzer has clients edit records at every depth (made,
written deep, written whole, a field or the record deleted), make
children under parents that may be gone, write singletons and registers,
undo, and go offline and back, while the server patches too and time
moves or stands still for ties. In relay mode half of them are behind a
relay whose link to the server drops and comes back. At the end every
client, and the relay's copy, holds the store's state, no child names a
missing parent, no outbox holds anything, and no op was applied twice.

The in-memory network the suite runs on is published as
`lazy-storage/testing`, for an app's own tests: `createNetwork(store)`
(or a relay) links clients to a store
without sockets, `net.client(options, { user })` gives a connected client
with `client.link.goOffline()` and `goOnline()`, and `await net.settle()`
delivers everything queued until the network is quiet, so a test reads
"edit, settle, assert" and runs in milliseconds. `fakeTime()` is a wall
clock to hand a store and its clients as `now`, for tests that decide
who wrote later:

```js
import { createStore } from 'lazy-storage/server';
import { createNetwork } from 'lazy-storage/testing';

const store = createStore({ initial: { tasks: {} } });
const net = createNetwork(store);
const a = net.client({ replicaId: 'a', initial: { tasks: {} } });
const b = net.client({ replicaId: 'b', initial: { tasks: {} } });
await net.settle();
b.link.goOffline();
a.state.tasks.x = { title: 'while b was away' };
await net.settle();
b.link.goOnline();
b.connect();
await net.settle();
assert.equal(b.state.tasks.x.title, 'while b was away');
```
