# Changelog

All notable changes to lazy-storage are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning.

## [Unreleased]

## [0.24.0] - 2026-10-07

A collection can now be followed record by record: `db.collection(name)
.watch()` hears each record a batch inserted, changed or removed, with
plain copies of what it is and what it was, for a view that keeps rows
of its own. On the server, `store.observe('op')` carries the diff the op
made, so a log, an index or a store kept from others forwards what is
stored rather than catching changes before they are. Both are
additions: on upgrade, an observer that copied the whole event (to a log
or a metric) now copies the diff with it; leave it out if that is too
much.

### Added

- **`db.collection(name).watch(listener)` follows a collection record by record.** Once per batch, the listener hears every record under `state[name]` the batch changed, as `{ type: 'insert', id, record }`, `{ type: 'update', id, record, previous }` or `{ type: 'remove', id, previous }`, with the batch's `meta` second. A view that keeps rows of its own (a data table, a search index, a cache keyed by id) had to read record ids out of every diff and rebuild what a record was from the inverse; `record` and `previous` are plain copies, new objects each time, so a view that compares rows by identity sees the changed one, and `previous` tells a filtered list whether a row just started or stopped matching
- **`store.observe('op')` says what the op changed.** The event carries `diff`, the op's change as merged and broadcast (a register whole, `null` where it deleted), or null when nothing was accepted. It was the hook to forward stored changes from (an audit log, a search index, a store the server keeps from others), but said only that an op was accepted; the diff it needs had to be caught in `store.on` before the change was stored. It is the object the delta log keeps: read it, do not change it

## [0.23.3] - 2026-10-01

A deploy that finds its stores leased by the container before it now
says why it waits: from another host it cannot be told whether that
process still runs, and one killed without closing the file holds its
stores until their leases run out. The docs say what a deploy needs so
that it does not wait. Only a message changes: there is nothing to
check on upgrade.

### Changed

- **A store leased on another host says why it may wait.** A new container finding a store leased by the one before it cannot tell whether that process still runs, so it waits out the lease (`lease.ttl`, 30 s) whenever the old one was killed, or stopped without closing the file; the `store-locked` message only said it would be served "N s after it stops renewing". When the lease's host is not this one, it now says too that from here it cannot be told whether that process still runs, and that one killed without closing the file holds the store until its lease runs out. docs/server.md says what a deploy needs for the wait not to happen: the storage closed on SIGTERM, the signal reaching the process, and its stop ending before the platform kills it

## [0.23.2] - 2026-09-30

The README is now a pitch and a quickstart, and the manual it was is a
page per topic in `docs/`, which ships in the package: what npm and
GitHub show first is what lazy-storage is, a client and a server, what
you get and when to use something else, rather than 1900 lines of
reference. The code is that of 0.23.1; there is nothing to check on
upgrade.

### Changed

- **The README is a pitch and a quickstart, and the reference moved to `docs/`.** At 1900 lines the README was the whole manual, and someone meeting the library had to find the quickstart in it. It now says what lazy-storage is, shows a client and a server, lists what you get, how fast it is and when to use something else, and points to the reference: a page per topic in `docs/` (the model, offline, multiple stores, the server, authentication and presence, relays, limits, the API, the wire protocol, and examples, benchmarks and tests), with the text as it was and the links between sections now links between pages. `docs/` ships in the npm package. Earlier entries below that name a section of the README mean the page that now holds it

## [0.23.1] - 2026-09-29

A reconnect storm through fan-out relays no longer costs the server the
square of its clients: 20 000 clients behind three relays, dropped at
once, now cost the server's core little more than their vouches, where
most of it went on finding whether a store still had a client. Beside
the fix, `examples/podman-caddy` shows a server for as many clients as
one machine takes (relays on the other cores, Caddy in front, compression
set hop by hop), and `bench:fanout --url` load-tests such a deployment
from another machine: connects, writes, and a reconnect storm. Nothing
changes for code on the server or the clients, on the wire or on disk;
upgrade the server and its relays in any order.

### Added

- **`examples/podman-caddy`: a server for as many clients as one machine takes.** A store lives in one process and uses one core, and each change costs a write to every socket that hears it, so the clients connect to relays rather than to the server: `main.js` starts the server alone on 127.0.0.1 and `RELAYS` relays that share the public port (`reusePort`: on Linux the kernel spreads the connections over them), each reading every store once on a link of its own and writing to its clients from another core; Caddy ends TLS in front. The server compresses nothing for its relays on loopback, and the relays compress what passes 4 KB for their clients on the internet. It serves the basic example's page, runs without containers too, and its README has the podman pod and Caddy commands and what to change for an app of one's own (authentication, how many relays, compression, open files, the data volume). Caddy opens a socket to a relay for every client, all to one address and port, which Linux's default range of source ports caps at some 28,000, so the pod widens the range and the README says how to add addresses; Caddy's container gets twice the app's limit on open files, holding two sockets for every client. `PROFILE=<dir>` has the server and each relay write a CPU profile when they stop, for a load test's run
- **`bench:fanout` measures a deployment of one's own.** `--url` runs its clients alone against a server already up, through whatever stands in front of it (Caddy, relays), `--clients` at a time in steps and over `--stores` stores, each client named in the query as the deployment's `authenticate` reads it (`--auth-param`, `--query`; `--insecure` for a local certificate). Each step says how long its new clients took to be answered, times writes as every client of a store hears them, and runs a reconnect storm: every client dropped at once and back as a client comes back after a drop, answered with the delta since its version (the `storm` phase, which the bench's own topologies can run too). No server of the bench's says which versions a phase is, so the writers put the phase's number in their ops, and the replicas' ids carry the run's, so a store kept from an earlier run takes the new ops as new. It is the way to find how many clients a machine takes behind Caddy, which the bench's own server cannot show. A step says how many of its sockets failed to open and were dialled again, since a load machine out of ports (the ones a storm's dropped sockets hold for a minute or two after) looks from the numbers alone like a slow server
- **The README says how to set compression, hop by hop.** It is paid for once per socket and message by whoever writes to the socket, and Bun compresses each message on its own, so a small patch hardly shrinks and a snapshot shrinks several-fold: a relay on the clients' network wants its link to the server compressed and its LAN not, relays next to the server spreading its load want the reverse (see "Spreading the load")

### Fixed

- **A reconnect storm through fan-out relays no longer costs the server the square of its clients.** Every client leaving a relay's link asked whether its store still had a client by copying and filtering every grant the link held, so a storm, every client leaving at once, walked the link's grants once per client: with 20 000 clients behind three relays that was most of the server's core for seconds, and all of them were back only after 7 s. The link now keeps its grants by store as well, and the question costs nothing

## [0.23.0] - 2026-09-29

A busy store no longer waits on its disk. What one turn of the event loop
merged is committed together (`groupCommit`, on by default), each SQLite
row written once for the turn, the write-ahead log is copied back on a
worker thread, and the Bun adapter writes what one task sends a socket
in one go: one store on SQLite, on a 4-core Linux server behind ten
relays, went from some 2500 writes a second to 15000, at a p99 of some
50 ms. `npm run bench:fanout` now measures writes, on one machine or on
two. The wire protocol and the SQLite files are unchanged, so clients
and relays of 0.22 work with it as they are. On upgrade, check the server
code that writes: `patch()` and `apply()` return before their change is
stored and no longer throw a commit that failed, so call `store.flush()`
before telling anyone a change is saved; `store.on` listeners hear a
change before it is stored (pass changes on from `store.observe('op')`);
and a test that reads storage straight after a `patch` needs a `flush()`
first. `groupCommit: false` keeps the old timing.

### Added

- **`npm run bench:fanout` has the clients write, not only the server publish.** Its write phases have `--writers` of the thin clients (400) write the store between them at each of `--write-rates`, as live ops on their own sockets, or through their relay up a write-only socket of their own; every client hears each op as it hears the server's patches, each op's author times its ack, and the server's CPU for each op is reported with the sockets it holds. What a write costs the server was measured nowhere: it is the one load relays cannot spread. `--phases` picks the phases to run, and `--profile <dir>` has the server and the relay processes each write a CPU profile there (Bun's `--cpu-prof-md`)
- **`npm run bench:fanout` says how busy the machine was.** Every phase reports how many of the machine's logical CPUs its processes kept busy between them, and flags one past `--busy` of them (half) MACHINE BUSY: the relays and the clients share the machine with the server, and a phase that fills it measures the machine, not the server
- **`npm run bench:fanout` can put the server on a machine of its own.** `bun bench/fanout/agent.js` on the server's machine starts a fresh server for each topology when `run.js --remote <host>:36700` on another asks, on fixed ports (the agent's, and the next two) a firewall can let in; the relays and the clients stay with `run.js`. The server's clock is measured against `run.js`'s (the quickest of 20 round trips) and it stamps its patches in that time, so latencies hold across the two machines to within half that round trip. On one machine the relays and the clients share the server's cores, and each socket's kernel work at both ends of the loopback is on the same machine too
- **The SQLite adapters take an `onError`, and report a checkpoint worker that failed.** A worker that could not start (a bundle that left `sqlite-checkpoint.js` out, say), failed or ended handed the checkpoints back to SQLite without a word, and showed only as slow answers: it is now reported, with code `checkpoint-worker` (default console)

### Changed

- **A store commits what one turn of the event loop merged in one go (`groupCommit`, on by default).** The ops every socket brought in one poll, the server's own patches and the replicas hellos claimed go to storage as one commit at the end of the turn (through an adapter's new, optional `commitMany`: one SQLite transaction), and what the store would have sent meanwhile, acks, patches, answers and presence, waits for that commit and then goes out in the order it was made, so no client hears of a change the storage does not have. SQLite writes whole pages, and a turn's ops now share them: behind ten relays on one machine, a store on SQLite kept up with 8000 writes a second (p99 71 ms) where it had fallen seconds behind, and spent up to a fifth less CPU on each. A store with nothing pending sends at once. For code on the server: `patch()` and `apply()` return before their change is stored, and a commit that fails unloads the store and goes to `onError` rather than throwing from them; `store.flush()` stores at once, and throws `unavailable` when it failed; storage read straight after a `patch` (in a test, a backup taken in the same turn) needs a `flush()` first, and so does telling anyone outside the store that a change is saved. `store.on` listeners hear a change as it is merged, before it is stored: `store.observe('op')`, told of an op once it is stored, is the one to pass changes on from (the README's Persistence says what group commit asks of server code). Creating a session, an eviction, a snapshot over HTTP, `export()` and `dispose()` commit what is pending first, and the hub and the relay route subscribe a socket to a store's broadcasts once its session exists, so a socket never hears a patch from before its hello. A turn that merges more than 256 changes is stored that many at a time. `groupCommit: false` commits every change as it is made, as before
- **Tagging a message with its store id is three times cheaper.** A hub and a relay link tag every message a store sends with the store's id, and the tag copied each property through its descriptor, so that a snapshot's `state`, decoded only when read, stayed a getter: some 8% of a busy server's time, at 18000 writes a second. A message with a lazy property is now marked as such (a snapshot is), and every other one (each patch and ack) is spread
- **The SQLite adapters checkpoint on a worker thread.** In WAL mode SQLite copied the log back into the database file, and waited for the disk, at the end of the commit that took the log past a thousand pages: on the thread that committed, a server's event loop, which answered nobody meanwhile, and a busy store's slowest answers were mostly those waits. A worker thread with a connection of its own now checkpoints every 100 ms (PASSIVE: it copies what no reader still needs, and never makes a writer wait), and the committing connection checkpoints only a log past some 40 MB, for a worker fallen far behind or gone. The slowest turns of a busy store took half as long on a laptop's disk. A worker that cannot start hands the checkpoints back to SQLite; `close()` has the worker let the file go before closing the main connection. `checkpoints: 'inline'` (both `sqliteStorage`s) keeps them on the committing thread
- **The Bun adapter writes what one task sends a socket in one go.** A store sends a turn's acks and answers together, after the turn's commit and outside the callback in which Bun corks a socket's writes itself, so each message cost a system call of its own. What a socket is sent is now queued and written together (`ws.cork`) once the task is done, in the order it was sent. A topic publish still goes out at once, and the sockets that hear the store get what was queued for them first; a socket the server closes gets what was queued before the close. `maxBuffered` is checked once a write rather than once a message
- **A grouped SQLite commit writes each row once.** A turn's changes went to SQLite in one transaction, but each change still ran every statement of its own: the store's version once an op where only the last counts, the lease read once an op, a replica's progress and a leaf that several changes wrote once a change. A commit now writes the last version, reads the lease once, gives each leaf and each replica the row the last change left it, writes every log entry, and prunes the log once, to the last floor the changes name: a third less CPU for each op of a grouped commit (some 17 µs in place of 27). `commit(change)` is a batch of one
- **The SQLite adapters write a fifth less for each op.** SQLite writes whole 4 KB pages, and the commit of a small op touched six of them, some 26 KB of WAL for a few hundred bytes of change; on a server's disk that, not its CPU, held a busy store back (a 4-core Linux server took 8000 writes a second behind ten relays with memory storage, and some 2500 with SQLite). A commit now reads its lease rather than renewing it (the timer keeps it fresh, and a commit renews one with less than half its `ttl` left), and prunes the delta log once its floor has moved a hundred entries rather than with every commit, so the `log` table holds up to a hundred entries past the store's floor, which the store leaves out on load as it does anything past its `deltaLog`. That is 5.1 pages an op instead of 6.4, in about a quarter less time
- **`npm run bench:fanout` spreads its clients over 50 relays by default, not 200.** Every relay parses, applies and logs each patch itself, and here they all do it on one machine; fifty cost the relays a little less. With 4000 clients either count keeps more than half of a 24-thread laptop busy from 50 patches a second (the fan-out itself costs the most), so the header of `bench/fanout/run.js` points to `--topologies hubs --n 400 --hubs 200` for the server behind many relays: there it kept up with 500 patches and 1000 writes a second

## [0.22.0] - 2026-09-28

A store the server will not let a fanning-out relay carry is passed
through to the relay's clients rather than closed to them: each client's
session there goes up its own socket and is judged by the server as its
own, so a client that may read the store keeps reading it behind the
relay, and one that may not hears so from the server. Only the relay
changes; servers and clients of 0.21 work with it as they are.

### Changed

- **A store the relay may not carry is passed through, not closed to its clients.** Where the server turns a fanning-out relay's own session on a store down (`relays.authorize`, closed 'forbidden'), each client's session there used to hear 'forbidden' as if the client could not read the store, and a client's 'forbidden' is final: it stopped syncing the store without a word, while its own socket would have been let in. Now each client's session on that store goes up the client's own socket (the one its edits take) as a whole session, judged by the server as the client's own: one that may read the store reads and writes it, one that may not hears 'forbidden' from the server. The relay vouches for nobody there, keeps nothing of the store (a copy it held goes), answers it from no copy offline, and asks to carry it again ten minutes later, when a client next opens it. `relay.stats().link.passed` counts those sessions, and `relay.sockets()` lists their stores. The fuzzer's fan-out runs have the relay refused the notes store every other run

## [0.21.0] - 2026-09-27

A relay can read each store once for all its clients. Given a `link` to
the server's new relay route, it vouches for each client with the
client's own credential, the server judges it by its own hooks and lists
it as itself, and each client's edits still go up a socket of its own, so
the server keeps judging and acknowledging every write as its author's;
it writes each change once a relay rather than once a client. Presence is
heard only where it is read: `db.wantPresence(on)` changes a client's
wish while it runs, and a browser's shared connection and a relay's own
session ask for it only while someone behind them wants it.
`npm run bench:fanout` measures what that and the relays save. Opt-in:
a server without `relays` and a relay without a `link` work as before.
On upgrading, update the server, the relays and the clients together (a
hello now says presence either way, and a later one may turn it back
on); a relay's Bun handlers compress from 64 KB unless given a
`threshold`; and `socketStats().sockets` no longer counts the clients a
relay carries (`clients` does).

### Added

- **Fan-out: a relay reads each store once for all its clients, and the server still judges each one.** Given a `link` to the server's relay route under its own credential, a relay no longer dials a socket through per client: for each store a client opens, it asks the server whether that client may read it (`vouch`, with the client's credential, judged by the same `authenticate`, `expiresAt`, `authorizeId` and `authorize` as the client's own socket, handed `{ relay }`), reads the store once on a session of its own (let in only while some client of it is, closed `linger` after the last), answers each client from its copy under the server's epoch and version (a delta from a log of the server's last 1000 patches where it reaches, else a snapshot), and passes every patch on to each client in version order, encoded once. A client's edits go up a socket of its own, dialled with its credential, as a write-only session: the server judges and acknowledges each op as its author's, and the relay writes nothing as anyone. Acks are held until the client has heard the patches up to their version (one heard after later patches is corrected from the copy). The server's `disconnect`, expiry, `revalidate` and evictions reach the relay's clients as they would their own sockets, and a final verdict is kept offline too; the relay itself is cut off by `disconnect`, judged by `relays.authorize` on `revalidate`, and ended by `relays.expiresAt`. The link failing is the server failing (`grace`, then local mode as before; on the way back every client that sent an op meanwhile says hello again); a link lost for less than `grace` goes unnoticed by the clients; a server that answers the clients but not the link, or turns the link away, has the relay pass sockets through as before. On the server, the adapters' `relays: { authenticate, expiresAt, authorize, path, linger, headers, rate }` (a relay connects at `<path>/relay`), `createRelayLink` for a transport of one's own; on the relay, `createRelay({ link, linger, writeIdle })`, `accept({ credential })` and `createRelayHandlers({ credential })`. `socketStats()` counts `relays` and `clients`, `sockets()` lists a relay with its clients (`via`), and `relay.stats().link` says how the link is. See the README's "One read for many clients"
- **`store.peer({ user, replicaId, via })`, a client a relay serves**: listed in presence, its replica claimed and its share judged as a hello's would be, sent nothing, closed by `closeSessions` and `dispose` (its `onEvict` told why)
- **A relay's own session, `store.session({ relay: true })`**: it hears the store and its presence, is nobody's peer, and its ops and shares are refused
- **`follow: false` in a hello makes a write-only session**, answered as ever but hearing no patches or presence after and not listed; only for a replica a relay serves (a peer holds it), anyone else's is closed 'unavailable', so nobody reads a store unlisted. The Bun adapter takes such a socket off the store's topic
- **`ack` carries `v`**, the store's version with the op in it
- **`db.wantPresence(on)`: hear presence, or stop, while the client runs** (read back as `db.wantsPresence`). A screen that only an admin reads who is online on, say, turns it on when an admin signs in and off when they sign out, and every other screen is spared every session's coming and going. The client says hello again, answered as a reconnect's is from where it stands, and the server sends the whole list to a hello that turns presence on, as it does a newcomer's; the server (and a relay on the way) must be of this version. Off, `db.presence` and `db.peers` empty at once, and presence still on its way is dropped
- **A browser's shared connection hears presence only while one of its tabs (or a web app's port) wants it on that store**: its replica's first hello already says so, and it asks again, either way, as tabs come, change their mind and go
- **`admitted` says whether the store has presence** (`presence`), so a relay knows it without hearing any
- **`npm run bench:fanout` (Bun): a store every client follows, published to by the server**, with thousands of thin clients over real sockets straight to the server, behind one relay or spread over many, each part a process of its own: what each patch took from when it was due and from when it went out, and each part's CPU, rate by rate, then 4 KB patches and a burst in one turn. The README's "Relays" says what it showed
- **The fuzzer runs displays behind a fanning-out relay** (`node test/fuzz/run.js --mode fanout`): link drops, sign-outs and a display passed through beside the others, with the same checks

### Changed

- **The 'session' observers hear `kind`** ('client', 'relay' or 'peer', a peer with `via`)
- **Every hello says whether its session hears presence, either way**: a later hello without `presence: false` turns it back on (where before only the first one counted, and presence could only go off), and is sent the whole list. A client from before always says the same, so nothing changes for it
- **A fan-out relay's own session hears presence only while a client behind it wants it.** Every client joining or leaving the store went to every relay, a store's thousands of displays behind hundreds of hubs costing each hub every one of them; now a relay whose clients all say `presence: false` hears none, and one whose first presence-wanting client arrives says hello again for it (the whole list comes). It stops hearing it with that session, not in place, so a client that comes and goes costs no hello each time. A client let in again after the link was lost and found (it waited its turn to be vouched for) is sent the relay's list anew, which the link's absence may have changed
- **A relay keeps the users of its own clients only**, for their presence while the server is away: those it may answer offline and those on the store through it now, learned from `admitted` too. It kept every peer the store's presence ever showed, and wrote them all with the copy about once a second under traffic (400 relays of 8000 clients spent most of their CPU there in `bench:fanout`); a copy saved by an earlier version is trimmed at its next write
- **A relay writes the server's clock with its known credentials once a minute**, as meant, and when the clock jumped further than that (the first stamp after a restart), not at every save: the clock moves with every read, so the file was rewritten about once a second under traffic
- **The Bun relay's `perMessageDeflate` threshold defaults to 64 KB**, not the server's 1 KB. The runtime deflates per socket, a topic publish included (measured: some 30 µs a 4 KB message a socket), so a relay compressed every patch of 1 KB or more once per client, which one relay with 4000 clients could not keep up with at 20 patches a second; a LAN does not need it, while a snapshot a display on Wi-Fi waits for still goes compressed. The README no longer says a topic publish compresses once
- **`closeSessions` leaves a relay's own session alone**: it is no client's; its clients are peers, which it evicts as any session, and the relay's session goes when they have
- **`socketStats().sockets` counts the server's own sockets**, a relay's link among them (also as `relays`), and not the clients a relay carries (`clients`)
- **The README's relay section is "Relays"**: working on while the server is away and taking the reads off it, with what a relay spreads of a server's load and what stays with the server
- **The Bun relay writes what goes to a socket in one turn of the event loop as one write** (`ws.cork`), so the patches and presence the link brought together reach each client together, a turn later. A socket costs a write per send, however little it carries, and that is what a relay fanning a store out to many clients spent its time on: a thousand clients connecting through it took a third of the CPU per message they heard. What waits for a socket still goes before the relay closes it, and `maxBuffered` is checked at each write

### Fixed

- **A page's port let go of a store it never opened no longer opens it in the browser's shared connection.** `follow`'s `stop()` says leave for every store the port was allowed, and a leave for a store the replica held no entry for made one, with a session on the server that never went (nobody was on it to leave) and heard presence nobody read

## [0.20.0] - 2026-09-26

Clients on one network can keep working together when the internet
goes. `lazy-storage/relay` is a process on their LAN that their
sockets go through: a pipe while the server answers, each client
dialled through under its own credentials, and the answers from its
own copies while the server is away, acknowledging nothing, so every
edit reaches the server as its author's when it is back and is judged
there. Opt-in: servers and clients that use no relay need no change,
and a client that predates `db.relayed` ignores the relay's word. The
wire protocol and stored data are unchanged. Fixed besides: a client
with more than a hello's worth of pending ops no longer says hello for
ever to an answer that acknowledges none of them.

### Added

- **A relay on the LAN, `lazy-storage/relay`, for clients that should keep working together when the internet goes.** A process on their network that their sockets go through. While the server answers, it is a pipe: each client's socket is dialled through with that client's own credentials (the host's `upstream` holds them; the relay never reads them) and every frame passes as it came, so the server sees, judges and acknowledges each client as itself, and neither side changes; on the way past, the relay builds a copy of every store from the snapshots, deltas and patches (asking for large snapshots inline, and for a snapshot where its copy could not use a delta). When the server has not answered for `grace` (ten seconds, so a deploy is waited out; one client's failure is checked against the other sockets passed through, and a client the server never answered counts for nothing), or the host says so (`upstreamDown()`), it answers from its copies: hellos answered, ops merged last-writer-wins and sent to the store's clients as patches, presence among the clients on the relay. It acknowledges nothing (every answer's `seq` 0, no `ack`, no refusal, no `lost`), so every op stays pending in its author's persisted outbox, and when the server is back (a probe dialled every five seconds with a client's credentials, or `upstreamUp()`) the clients reconnect through it and each outbox reaches the server under its own identity, to be judged there: refused edits come back as `rejected`, lost ones as conflicts. While nobody has edited a store offline the relay answers under the server's epoch and version, so the way back is a delta; the first offline edit has its clients say hello again, to answers under epoch null, which ask the server for a snapshot when it is back and raise no `reset`. Offline ops are gated by `maxSkew` against the server's clock as last heard (carried on by elapsed time), by `authorizeOffline(key, storeId)` (default: the server answered that credential on that store) and the replica ids the server saw with it, and by an optional `validate`. `memoryCopies()` and `fileCopies(dir)` (a JSON file per store, the state, its clocks and the replicas' progress written together through a rename) keep the copies over a restart. `lazy-storage/relay/bun` mounts it in a `Bun.serve` (`createRelayHandlers`) and dials the server with headers (`upstreamSocket`). See the README's "A relay on the LAN"
- **`db.relayed` and the `relay` event: whether a relay answers the store on its own.** A relay says so after every answer it makes from its copy (`{ t: 'relay', status: 'local' }`); an answer from the server itself, or a socket gone, sets it back to false. A browser's shared connection passes it on to its tabs and the pages following it. A client that predates it ignores the message, as it does any type it does not know
- **The in-memory network closes with a code, and a link can hang.** A `session` endpoint's `onEvict(code, reason)` closes the link's connection with that close code, so a test can see what a client does on 4401 or 4001, and `link.stall()` makes the link's next connections neither open nor close, for timeouts. A relay's `accept` is such an endpoint

### Fixed

- **A client with more than a hello's worth of pending ops no longer says hello for ever to an answer that does not acknowledge them.** The client sends its outbox 1000 ops to a hello and, while more remain, another hello once the first is answered; a relay's answer (epoch null) acknowledges nothing, so the next hello carried the same ops and was answered alike, for ever, and the client never came online. The rest now goes out live after an answer under epoch null, as edits made during a hello do; a server's answer is unchanged
- **A transport of the in-memory network closed before it opened no longer opens afterwards.** It opened a session at the store that nothing ever closed, as a WebSocket closed while connecting does not; it now reports its close instead

## [0.19.0] - 2026-09-25

A socket no longer outlives what let it in. `disconnect(filter)` ends
the sockets of the users it picks, after a logout, a deleted account or
a changed role; `expiresAt(user, req)` ends each one when its session
runs out; either way the client reconnects and `authenticate` decides
again, so a refreshed token carries on and a lapsed session is signed
out. `revalidate(filter)` judges open stores again after a change to
who may open them. All three are opt-in and clients need no change: a
client of any version reconnects on the new close code, 4001. To check
on upgrade: `socketStats()` gains `disconnected`, `expired` and
`revoked`, and each `sockets()` entry `expiresAt`, which a test
comparing them whole will notice. The wire protocol is otherwise
unchanged, and so is stored data.

### Added

- **`disconnect(filter)` on both adapters, for a logout or a changed user.** An open socket kept the user it authenticated as, and its stores, for as long as it stayed up, so a signed-out or deleted user, or one whose role changed, went on reading and writing until the socket happened to drop. `server.disconnect(user => ...)` closes the sockets of the users it picks with code 4001: their sessions end at once, and the client reconnects as after any drop, so `authenticate` decides again (`unauthorized` once the session is gone, the new role otherwise) and every store is authorized again. An edit sent as the socket closed stays in the outbox and lands after the reconnect. No client change: a client of any version reconnects on 4001.
- **`revalidate(filter)` on both adapters and on a hub, for a change to who may open a store.** `await server.revalidate((user, storeId) => ...)` runs `authorizeId` and `authorize` again on the open store sessions it picks and closes those now refused with `forbidden`, leaving the socket up for its other stores. A store still waiting for its first verdict is asked about again, so a check that began before the change cannot let it in. `socketStats()` counts both, as `disconnected` and `revoked`.
- **`expiresAt(user, req)` on both adapters, for credentials that run out.** A socket outlived the token or cookie session it was opened with. The hook, read at upgrade, says when the session ends (ms since the epoch or a `Date`, nothing for never); the server closes the socket then with code 4001, as `disconnect` does, and the client reconnects with whatever credentials it has by then, so a refreshed token carries on and a lapsed session is signed out. A session with less than `minSession` left (default 30 s) is turned away as unauthorized rather than let in to be closed again in a moment. Waits longer than a timer can hold (some 24.8 days) are taken in steps. `sockets()` gives each socket's `expiresAt`, `socketStats()` counts `expired`.

## [0.18.0] - 2026-09-25

The server can say how it is doing: how far behind each socket is, what
each store sends by message type and how many bytes, and how long each
op takes, with `db.stats()` for the same on the client; plain numbers
for a status endpoint or whatever metrics system the server already
runs. A socket that stops reading is now cut off on Bun as on Node. One
change to check on upgrade: the Bun adapter closes a socket past
`maxBuffered` (16 MB) with code 1013 where it used to keep it and drop
messages; clients reconnect and catch up, and `maxBuffered: false`
keeps the old behaviour. `stats()` results and the `op` event gain
fields, which a test comparing them whole will notice. The wire protocol
and stored data are unchanged.

### Added

- **How far behind each socket is.** Both adapters' servers (and
  handlers) gain `sockets()`, each open socket's `user`, the `stores` it
  has open, `buffered` (bytes queued for it and not yet sent), `idleMs`
  and `openMs`, and `socketStats()`, the same rolled up: `sockets`,
  `buffered`, `largest`, and `cutOff`, how many sockets were closed for
  passing `maxBuffered`. Enough for a status endpoint that shows which
  clients are falling behind
- **What a store sends, and how long an op takes.** `store.stats()`
  gains `sent`, by message type (`patch`, `ack`, `snapshot`, `delta`,
  `presence`, `http-snapshot`, …): `{ messages, bytes }`, deliveries (a
  broadcast once per session it reached) and their UTF-8 bytes of JSON
  before compression, an HTTP snapshot's as served; counted since the
  store was loaded, and summed across a registry's live stores by
  `stores.stats()`. Bytes are read from the encoding the socket sends,
  measured once per message and never encoded for the count; a
  transport that hands on objects (the in-memory network) counts
  messages only. `observe('op')` gains `ms`, the time from the op
  reaching the store to its patch handed to the sessions (an adapter's
  later asynchronous write excluded). The README shows a status endpoint
  and a `prom-client` setup built on these; lazy-storage depends on
  neither
- **`db.stats()` on the client**: `{ pending, oldestPendingMs, ackMs,
  remoteAgeMs }`, the ops waiting and for how long, the last
  acknowledgement's round trip (latency), and how old the last patch
  from another replica was when it arrived (staleness, including any
  time its writer spent offline). For a "still saving…" hint or a status
  line

### Fixed

- **A Bun socket that stopped reading stayed open, and lost patches.**
  Past its buffer limit Bun drops what it cannot queue, and keeps the
  socket; the client noticed the gap only on a later patch. The Bun
  adapter now takes `maxBuffered` (default 16 MB) as the Node one does and
  closes such a socket with code 1013, checked on every send and, for a
  store's broadcasts, which Bun fans out itself, once a second; the client
  reconnects and catches up with a delta
- **A Node socket cut off for falling behind lingered for 30 s.** It was
  closed with code 1013, but a socket that stopped reading never answers
  a close, and `ws` waits 30 s for it with the store session (and its
  presence entry) still attached; it is now dropped after a second

## [0.17.1] - 2026-09-25

Small fixes, and CI that holds the line: lazy-watch 7.0.2 stores
`undefined` inside a written value as JSON carries it, `presence.every`
runs on the store's clock, and CI now fails on a drop in test coverage
or a benchmark over its ceiling. Nothing to change on upgrade; the wire
protocol and stored data are unchanged.

### Added

- **A coverage gate and a benchmark guard in CI.** `npm run
  test:coverage` runs the Node suite under c8 and fails below 96% of
  lines and statements, 88% of branches or 92% of functions (it covers
  98%, 90% and 94% now); CI and the publish workflow run it. `npm run
  bench:check` runs both benchmarks with `--check`, which holds each
  case to a ceiling about ten times a laptop's median and the client
  cases' counts exactly: one row render per batch on
  `useClientSelector`, none for a peer's cursor, at most one state copy
  for a Vue mount, a handful of outbox writes under remote traffic. CI
  runs it on every push

### Changed

- **Requires lazy-watch 7.0.2**, which stores `undefined` inside a
  written value as JSON carries it. A record written with a key set to
  `undefined` (`db.state.tasks[id] = { title, note: undefined }`) kept
  that key in the client's state until the server acknowledged the write;
  it is now left out at once

### Fixed

- **`presence.every` reads the store's clock.** The presence batching
  window was timed with `Date.now` while everything else in a store runs
  on the `now` it is given, so a test on `fakeTime()` that moved the
  clock past the window still waited the window out in real time before
  presence went out

## [0.17.0] - 2026-09-25

A record read from a list view stays that record wherever it moves, by
the app's own sort or splice or by another client's move, and so does a
listener registered on it (lazy-watch 7.0.1). One change to check on
upgrade: `splice`, `shift` and `pop` on a list return the records'
own handles, and a write to one before it is put back throws, where 0.16
wrote to a copy; put it back first, or edit `LazyWatch.snapshot(task)`.
An app with lazy-watch in its own `package.json` moves it to `^7.0.1`
too, since a 6.x copy does not recognize 7.x state (its `LazyWatch.on`
and `snapshot` throw); `LazyWatch` imported from lazy-storage is always
the matching one. The wire protocol and stored data are unchanged.

### Fixed

- **A record read from a list view stays that record.** A handle kept on
  a record (`const task = db.state.tasks[0]`, a row's click handler
  holding one) addressed an index: after the app's own sort, `splice`,
  or `unshift` it edited whichever record had come to sit there, and
  the edit synced to everyone; after another client moved the record, it
  was detached and writes through it threw. It now follows its record
  through every move, local or remote, and so does a listener registered
  on it

### Changed

- **Requires lazy-watch 7.0.1**, whose handles follow their objects. On
  a list view, `splice`, `shift` and `pop` return the removed
  records' handles rather than copies: the usual move
  (`tasks.splice(j, 0, ...tasks.splice(i, 1))`) keeps the record
  itself, but a write to a removed record before it is put back throws;
  put it back first, or edit a copy (`LazyWatch.snapshot(task)`). A
  listener on a record hears `null` once when the record is deleted and
  does not follow another record that comes to its index. A move on a
  5k-record list view takes ~5 ms instead of ~6. 7.0.1 over 7.0.0: a
  record taken out and pushed back is a move too, its listeners stay
  exact however it comes back, and undo stays exact across the batches
  of one step

## [0.16.0] - 2026-09-23

Running the server gets its operations: one process per store on a
SQLite file (leases), versioned migrations that run as a store loads,
and backups and restores, down to putting a copy back while the server
runs, with every client told when that happened (`reset`). Nothing to
change on upgrade. Leases are on by default and protect deploys from the
next one on: a 0.15 process takes none, so the deploy that replaces it
is not yet covered. A SQLite file gains a `leases` table and a `schema`
column, which 0.15 leaves alone, so a rollback still opens it. The wire
protocol is unchanged; a tab on an earlier version ignores the `reset` a
newer leader tab relays.

### Added

- **One process serves a store: SQLite leases.** Two processes on one
  SQLite file (a deploy whose old and new process overlap, a server
  started twice) each loaded the same store, committed versions that
  collided, overwrote each other's delta log, and kept their clients in
  states that never met again. The SQLite adapters now take a lease on a
  store when it loads, renewed with every commit and on a timer, given
  up when the store is disposed or the file closed. A process that finds
  a store leased elsewhere is refused with code `store-locked`, which a
  hub tells its client as `unavailable` (the client says hello again a
  moment later) and the snapshot route as a 503; a commit that finds its
  lease gone is refused (`lease-lost`), so the store unloads rather than
  write over the process that took it. A lease left by a process that
  died runs out after `lease.ttl` (default 30 s), or at once when that
  process was on this machine. Stores are leased one by one, so
  processes may still share a file by serving different stores.
  `lease: false` on `sqliteStorage` turns it off. A store now calls its
  storage's optional `close()` when disposed
- **Migrations.** `initial` covers a container added to the state, but a
  change to data already stored (a field renamed, a default filled in)
  had nowhere to go but the app's own startup code. `createStore({
  migrations: [state => diff, ...] })` runs the ones a store's rows have
  not been through, in order, when it loads and before anyone is served;
  each diff is applied as the server's own `patch` (persisted, logged,
  sent on). How many have run (`schema`) is stored with the rows in the
  same commit as each migration's, so a crash never leaves one half done:
  in the SQLite adapters a column on `stores` (added to an existing
  file), in the others a field of the document. A new store starts with
  every one done; one stored before any were given runs them all; storage
  that has run more than the list holds (a rollback after a newer version
  migrated it) is refused with code `schema-ahead`; a migration that
  throws stops the load, naming it. `store.stats()` gains `schema`
- **Backups, restores, and moving a store.** `store.export()` gives a
  store as a JSON document (rows with their timestamps, tombstones,
  replicas' progress and owners, version and schema), and every adapter
  takes one, or another adapter's `load()`, with `replace(doc)`: from a
  JSON file to SQLite, from one server to another, or back from a copy.
  `sqlite.backup(file)` copies a whole SQLite file with `VACUUM INTO`
  while the server runs, without its leases. A replaced store, and every
  store in a backup, starts a new epoch, so a client that saw more than
  the copy holds is sent a snapshot rather than a delta from a history it
  never had. The SQLite adapter refuses to replace a store this process
  has loaded (`store-open`) or another serves (`store-locked`), and
  writes the document in one transaction
- **Restore while the server runs.** `stores.restore(id, doc)` (or
  `store.restore(doc)` outside a registry) ends the live store, puts the
  document in its storage, and lets the next `get` serve it; its
  sessions say hello again within a second
- **The client hears when the store started over.** `db.on('reset', ({
  epoch, previous }) => ...)` fires when the server answers under a new
  epoch while the client held a version of the old one (a backup put
  back, a store moved or wiped), with the state the client showed just
  before, so an app can tell its user that recent changes may be gone.
  Under a `sharedConnection` every tab hears it

## [0.15.0] - 2026-09-23

An app can now tell its user what happened to an edit: `conflict` says
which of its writes lost and what won, `rejected` which the server (or
the model) refused, and `db.isPending(path)` whether a field or record
still has an edit on its way. A dropped socket comes back as soon as the
browser is online or the tab is looked at again, and the quickstart
type-checks. Nothing to change on upgrade; the server's `ack` and its
answer to a hello gain an optional `lost`, which earlier clients ignore.

### Added

- **`conflict` and `rejected` events: what happened to an edit.** The
  state always showed what won, but an app could not tell its user that
  an edit had lost or been refused: a lost write arrived as an ordinary
  remote patch, a refusal as a bare `error`. `db.on('conflict', ({ seq,
  lost }) => ...)` now reports, per leaf that lost, the path, what this
  client wrote (`mine`) and what won (`theirs`, null where the record
  was deleted), when the op's ack arrives or, for an op made offline,
  when the reconnect is answered. `db.on('rejected', ({ seq, code,
  message, diff }) => ...)` reports an op the server refused, with the
  edit itself, and a batch the model refused locally (`seq` null). The
  `error` event still comes too. Under a `sharedConnection` every tab
  hears the browser replica's. The server says which leaves lost: `lost`
  on an `ack`, and `lost: [{ seq, paths }]` on the answer to a hello
- **`db.isPending(path)`**: whether an edit not yet acknowledged writes
  at, under, or over a path, for a "saving…" mark on a field or record;
  it changes with the outbox, as the `sync` event announces
- **A socket that is down comes back when the browser does.** After a
  drop the connection waited out its backoff (up to ten seconds) even
  once the network was back or the user returned to the tab. In a
  browser it now tries again at once on `online`, and on
  `visibilitychange` to visible, with the backoff starting over; a
  connection closed on purpose or turned away by the server stays down.
  `wake: false` on `createConnection` (or `sharedConnection`) turns it off

### Changed

- **The quickstart type-checks.** `createClient({ initial: { tasks: [] } })`
  inferred the state as `{ tasks: never[] }`, so `db.state.tasks.push(...)`
  was a type error. A state type taken from `initial` now widens an empty
  array to `any[]` (`FromInitial`); a type argument, or `any`, passes as
  it is

- **A lost write no longer costs the server a copy of the state.**
  `correction()` snapshotted the whole state for every op that lost a
  leaf; it now reads the paths it corrects

## [0.14.1] - 2026-09-23

Four ways to lose data or the server, fixed: IndexedDB storage stopped
saving for good once another tab closed the database; a replica whose
storage failed to open left every tab `connecting`; a follower tab's
edit was acknowledged before the leader had stored it; and a failed
`jsonFileStorage` write crashed the process. Nothing to change on
upgrade; `indexedDBStorage` gains `onReset` and `jsonFileStorage` an
`onError` option.

### Added

- A `LICENSE` file (ISC, as `package.json` has always said), shipped in
  the package

### Fixed

- **IndexedDB storage no longer stops saving after another tab closes
  the database.** The adapter closed its connection when another tab
  deleted or upgraded the database, but kept handing out the closed one:
  every later write failed into `onError`, offline edits included, until
  a reload. It now lets go of a connection closed under it (by
  `versionchange` or the browser's `close`) and opens a new one on the
  next write. When that finds the database gone and made anew, it tells
  the new `onReset` listeners, and the client writes its whole state and
  every pending op again, so a reload does not come back from half the
  rows or without the edits it had not sent
- **A replica whose storage cannot be opened no longer strands every
  tab.** When the leader's `storage(store)` failed to load (IndexedDB
  refused, a quota), the store's entry was dropped with the hellos queued
  on it, and every tab stayed `connecting` until the leader changed. The
  replica now runs from memory instead, and every tab hears an `error`
  with code `storage-unavailable`: edits sync, but do not outlive the
  browser session. The relay also no longer loads a replica's storage
  twice (an IndexedDB read of every row, twice, at each handover)
- **A follower's edit is acknowledged once the replica has stored it.**
  The leader acknowledged a follower tab's op the moment it arrived, and
  the follower dropped it from its outbox; with storage that writes
  asynchronously (IndexedDB), a leader tab killed before the write landed
  took the only copy. The acknowledgement, and the answer to a hello
  carrying ops, now wait for the replica's storage to settle
- **A failed `jsonFileStorage` write no longer crashes the server.** Its
  debounced write ran in a timer, so a full disk or a permission error
  was an uncaught exception that took the process down. It now goes to
  a new `onError` option (default console), the changes stay pending, and
  the write is tried again a second later; `flush()` still throws to its
  caller

## [0.14.0] - 2026-09-23

A replica now belongs to the user who first said hello with it, so a
teammate's replica id seen in presence can no longer be used to lock
them out of a store (found by a new hostile-client fuzzer). The client
got much faster where apps feel it: a remote move no longer freezes a
large list view, a push or move on one no longer resyncs every record,
React components can select what they read (`useClientSelector`), Vue
components share one mirror, and the state cache and offline outbox stop
rewriting everything. What an upgrade may need:

- A browser where someone else signs in with the last user's storage
  gets `replica-taken`: key client storage by user
- The Vue mirror is read-only; write to `db.state` (a write to the mirror
  only ever changed that component's copy)
- `localStorageOutbox` keeps its outbox op by op; one written by an
  earlier version is taken over on load, but a client of an earlier
  version cannot read the new form, so pending edits wait for this
  version after a downgrade
- The state cache of a document adapter is written once changes settle
  (`cacheDelay`, default a second), not 50 ms after every batch
- Requires lazy-watch 6.4.0

### Changed

- **Requires lazy-watch 6.4.0**, whose `splice` and `shift` return what
  they removed. On a list view, the usual move (`const [m] =
  db.state.tasks.splice(i, 1); db.state.tasks.splice(j, 0, m)`) inserted
  a second copy of the record after the one moved and lost the moved
  one, and the loss synced to everyone; it now moves the record. Its
  faster splice also halves a move on a large list view (~20 ms to ~8 ms
  on 5k records)

### Security

- **A replica belongs to its user, connected or away.** 0.13.0 refused a
  replica id only while a live session of another user held it, and the
  first to claim it won: a signed-in user who read a teammate's replica
  id in presence could say hello with it while the teammate was offline,
  and the teammate, back online, was refused its own replica and locked
  out of the store until the other session ended; an op under it could
  also make the teammate's later edits duplicates. The user who first
  says hello with a replica now owns it, recorded with its progress
  (`owner` in the replica row; the SQLite adapters add the column to an
  existing file), and a hello from anyone else is closed with the new
  code `replica-taken`, while the owner carries on. A browser that signs
  in as another user with the last one's storage meets the same code:
  key client storage by user. Sessions without a user own nothing
- **A closed session hears nothing more.** A session the store had closed
  (evicted, refused its replica, its store unloaded) still processed
  messages a transport of your own kept feeding it

### Fixed

- **A remote move no longer freezes a list view.** When another client
  moved a record, the array view shifted every record the move passed,
  each shift a splice of the whole array: a record moved 1000 places in
  a 5k-record list took some 30 s, and 200 places in 2k about 1.2 s. The
  view now keeps the longest run of records already in order and moves
  only the rest, so a move is one splice out and one in (8.6 ms for the
  latter, most of it lazy-watch's splice). A remote field edit finds its
  record without reading the array through the proxy (2.7 ms to 0.15 ms
  on 5k records)
- **A push or a move on a large list view no longer resyncs every
  record.** Any change of length compared every field of every record
  with the wire and checked each id against all before it: a push onto
  5k records took ~77 ms, a move ~90 ms. Only the records the batch put
  in or changed are synced now, ids are checked through a set, and the
  list's positions are read past the proxy; `reconcile` skips its sort
  when given every record (a push ~7 ms, a move ~22 ms, the rest of it
  lazy-watch's splice)
- **Vue components on one client share one mirror.** Every `useClient`
  call deep-copied the whole state and patched its own copy on every
  batch: mounting 200 rows on a 1k-task state made 200 copies (~150 ms),
  and every edit patched 200 mirrors (~1.3 ms). Calls on a client now
  share one mirror and one set of refs, made by the first and stopped
  when the last lets go (mounting ~9 ms, an edit ~0.12 ms). The mirror
  is read-only: a write to it went nowhere but that component's copy,
  and Vue now warns instead; writes go to `db.state`
- **The state cache is no longer rewritten dozens of times a second.** A
  document adapter's state was written at most 50 ms after any batch, so
  steady remote traffic serialized the whole state some 50 times a
  second (38 MB/s for 10k tasks). It is now written once changes have
  settled for `cacheDelay` (a new client option, default 1000 ms), at
  least every ten delays under traffic that never settles, at once when
  the page is hidden or goes away (`visibilitychange`, `pagehide`), and on
  `dispose`. A cache that lags is safe: it carries the version it
  reflects, a restore replays the outbox over it, and the reconnect asks
  for what came since
- **An offline outbox no longer grows quadratically in writes.** The
  outbox was one document rewritten with every op: 1000 ops offline wrote
  58 MB, the last ones ten times slower than the first. A document
  adapter may now take the outbox op by op (`saveOp`, `removeOp`,
  `dropOps`, the row adapters' calls; `save` stays for a whole outbox),
  and `localStorageOutbox` does: `key` holds `{ replicaId, seq, first }`
  and `key:op:<seq>` each op, so the same 1000 ops write 0.2 MB. A
  document written by an earlier version is taken over on load; a client
  of an earlier version cannot read the new form, so pending edits wait
  for this version again after a downgrade
- **A list removed from the wire is removed from the view.** Undoing the
  creation of a nested list (`task.subtasks = []`, then undo) left an
  empty array in the view where the wire had nothing

### Added

- **`useClientSelector(db, select, isEqual)` in `lazy-storage/react`**
  re-renders a component only when what it selects changes. `useClient`
  re-renders every component on it for every batch, share and status
  event, and the state's records keep their identity as they change, so
  `memo` could not help: 500 list rows re-rendered for an edit to one
  (7 ms) and for a peer's cursor (4.9 ms). With the selector one row
  renders for the edit (0.5 ms) and none for the cursor (0.2 ms). The
  selection is copied out as plain data, compared deeply unless `isEqual`
  says otherwise, and kept as the same object until it changes; it rides
  on the client's one subscription. `trackClient` gains `version`
- **`npm run bench:client`**, a benchmark of what an app feels on the
  client: React and Vue components on `useClient` as the store changes
  (counting re-renders and state copies), the array view of a large list
  under local and remote pushes, moves and edits, `localStorageOutbox`
  under remote traffic and a long offline spell (counting bytes
  serialized), and a lazy-watch object write against a field write.
  `--only <text>` runs the cases whose name contains it
- **A hostile-client fuzzer** (`npm run fuzz:hostile`, a fixed-seed pass
  in `npm test`, a longer campaign in CI): an attacker with a socket of
  its own sends forged ops, other replicas' ids and the server's, seqs
  far ahead, poisoned timestamps, reserved names, fragments, skeleton
  deletions, oversized hellos, shares, churn, and garbage beside honest
  clients, and every step checks that `Object.prototype` is untouched,
  the server's own writes land, no honest client hears an error or
  diverges, the skeleton stands, nothing unauthorized is loaded, sessions
  do not leak, and the store on disk equals the store in memory. It found
  the lockout above, and catches each of 0.13.0's security fixes when
  that fix is taken out

## [0.13.0] - 2026-09-23

A hardening release. One signed-in client could pollute
`Object.prototype` on the server, silence the store's own writes by
claiming its replica id, freeze its clock, or have any store loaded
before authorization; all of that is closed, and a dozen ways an edit
could be lost or a replica left behind are fixed. What an upgrade may
need:

- Move a check that needs only the user and the store id from
  `authorize` to the new `authorizeId`, which runs before the store is
  loaded; `authorize` alone still works, and still loads first
- A server of your own must answer `ping` with `pong`: a socket silent
  for two keepalive intervals is now dropped and reopened
- A `localStorageOutbox` key is one tab's at a time; tabs that should
  share a replica use `sharedConnection`
- The rate limit is per user and a hello costs a token; a session keeps
  one replica id; clients may no longer delete a top-level container of
  `initial`
- Requires lazy-watch 6.3.0

### Security

- **A reserved name in an op is refused.** An op whose diff used
  `__proto__` as a key was accepted, and rebuilding the accepted diff on
  the server walked into `Object.prototype`: one client could add a
  property to every object in the server process. `__proto__`,
  `constructor`, `prototype`, `$splice` and `$length` are now refused as
  keys anywhere in an op, register values included, with code `invalid`;
  `setAt` and `rebuild` refuse such a segment too, so a row persisted
  before this release cannot reach it on load
- **A session speaks for one replica.** The store took an op's
  `replicaId` on trust, so a client could send an op as `server` with a
  seq far ahead, after which every `store.patch` was ignored as a
  duplicate (read-only stores included), or as a teammate's replica,
  whose later edits were then acknowledged and dropped. A session is now
  bound to the replica its hello names (or its first op, for a session
  driven without one): an op under another id is refused with
  `forbidden`, as is a hello naming `server`, a second replica on the
  same session, or a replica a live session of another user holds. An
  op's timestamp must carry its own replica id (`invalid` otherwise)
- **A timestamp's counter is bounded.** An op stamped
  `[ms, 2^53 - 1, id]` pushed the server's clock to a counter where
  `count + 1` no longer changes it, so every server stamp came out alike
  and the store's own writes lost to themselves until the wall clock
  caught up. `isTimestamp` now wants a counter below 2^32 (`MAX_COUNT`
  in core) and a non-negative millisecond
- **The rate limit follows the user.** The token bucket was keyed on the
  replica id the client chose, so a client could shed its limit by
  minting ids; it is now keyed on the user (presence's `key`), or the
  replica for a session without a user. A hello costs one token (its ops
  still ride free), so hellos cannot be replayed in a loop, and the
  server merges at most 1000 ops of one hello (`HELLO_OPS`), as the
  client already sent
- **`authorizeId(user, storeId)` judges a store before it is loaded.**
  `authorize(user, storeId, store)` receives the store, so the hub and
  the snapshot route loaded it first: any signed-in user could have
  every store id they asked for read into memory (kept for good without
  a registry `idle`), run the store factory for each, and tell existing
  ids from unknown ones by `forbidden` against `unknown-store`. The new
  hook, on `serve`, `createHandlers` (Bun and Node) and `createHub`,
  runs first; a refusal loads nothing and answers `forbidden` whether
  the store exists or not. `authorize` still runs after it, for a check
  that needs the store. An app whose `authorize` looks only at the user
  and the id should rename it to `authorizeId`; the README's examples
  now do

### Fixed

- **A rate-limited op could be lost.** After a refusal the client kept
  sending new ops live; once the bucket refilled, a later op was
  accepted, and the server then ignored the refused one as a duplicate
  when the retry hello resent it, while the ack of the later op dropped
  it from the outbox. The store now refuses every live op of a session
  after a rate-limit refusal until its next hello, and the client sends
  nothing live until that hello, which resends the outbox in order. A
  long offline spell hit the same path, since the ops beyond the
  hello's 1000 went live: they now follow in another hello, the store
  saying `connecting` until it has them all. A hello refused for the
  rate is retried after `retryAfter` too
- **An empty object no longer empties a record on disk.** An op writing
  `{}` where a record stood (lazy-watch emits one for a container created
  empty: `settings ??= {}` on a replica that had not heard of it yet) was
  merged into the state, which kept the record's fields, but dropped
  their rows, so after a restart the record came back empty. An empty
  object outside a register now ensures the container and keeps what is
  under it, in the rows and on load alike (`rebuild` takes the register
  matcher to tell); in a register it is still the whole new value
- **A client can no longer delete a top-level container of `initial`.**
  `{ tasks: null }` from any client left a tombstone at `tasks`, and every
  later write under it was refused (none being a record write that lifts
  it) until compaction, a month by default. Such a deletion is now
  refused with `forbidden`; emptying the container still works, and the
  server's own `patch` may still delete it
- **A store disposed under open sessions tells them.** `store.dispose()`
  (a registry's `release`, or its idle sweep) closed its sessions without
  a word, and the hub kept routing to them: the client's next op was
  refused as `invalid` and dropped. The store now sends a `closed` with
  the new code `unavailable` and lets the hub forget the session; a
  client takes that code as passing, not final, and says hello again
  after a moment, which loads the store afresh with nothing lost
- **Leaving a store while its authorization was in flight leaked a
  session.** A `leave` and a new hello for the same store before an async
  `authorize` answered let both verdicts open a session; the first was
  never closed, stayed in presence, and kept the store from going idle
  (React StrictMode's double mount does this). A verdict for an attempt
  that was left is now ignored
- **A dead socket is noticed.** The keepalive sent pings and discarded
  the pongs, so a half-open socket (a network switch, a laptop waking)
  stayed `online` while edits queued into nothing until the OS gave up
  on it. A socket not heard from for two keepalive intervals (a minute
  by default) is now dropped and reconnected. A server of your own must
  answer `ping` with `pong`, as the hub and a store session do
- **Reconnects are jittered.** The backoff doubled without randomness,
  so a server that went away (a deploy closes every socket at once)
  brought every client back in the same instant; each retry now waits
  between half its delay and all of it
- **A patch that never arrived is noticed.** The client took each
  patch's version as it came, so a patch a socket dropped (under
  backpressure, say) left the replica wrong for good: later reconnects
  asked for a delta from after the gap. A patch whose version skips one
  now sends a hello for a delta from the last version held, and patches
  wait for its answer. A relay's patches (a `sharedConnection` tab),
  whose versions do not count one per patch, are exempt
- **The Node adapter closes silent and stalled sockets.** A half-open
  connection kept its sessions, and its place in presence, until the OS
  gave up on it, and a client that stopped reading had every patch
  buffered for it without limit. `idleTimeout` (default 120 s, as Bun's
  own) closes a socket that has sent nothing for that long, and
  `maxBuffered` (default 16 MB) closes one whose unsent output passes it
  with code 1013; the client reconnects and catches up with a delta
- **`localStorageOutbox` reports a failed write.** A full quota was
  swallowed, so an app could not tell its user that edits made offline
  no longer survived a reload. `localStorageOutbox(key, { onError })`
  hears it, as `indexedDBStorage` already did
- **A commit that fails no longer leaves memory ahead of disk.** The
  state, version and delta log changed before `commit`; one that threw
  (a full disk, `SQLITE_BUSY` while a backup held the lock) left a change
  in memory that was never saved nor broadcast, and the sender was told
  `invalid` and dropped its op. The store now unloads itself: the error
  goes to `onError`, sessions hear `unavailable` and resend their
  pending ops to a store a registry loads afresh from disk
  (`stores.get` replaces a disposed store rather than handing it out).
  The SQLite adapters set `busy_timeout` to five seconds, and a store's
  `dispose` still tells its sessions when the final flush throws
- **Two tabs on one `localStorageOutbox` key no longer lose each other's
  edits.** Both loaded the same replica id and sequence number, so the
  server acknowledged one tab's ops and dropped the other's as
  duplicates, and the two overwrote each other's outbox; the README's
  first example was this setup whenever a second tab opened. A key is
  now held by the tab that loaded it first, through a lease under
  `key:lease` renewed while the tab is open and given up on `pagehide`
  or `close()`. A second tab starts a replica of its own that keeps
  nothing across a reload, and hears `storage-in-use` through `onError`.
  A `sharedConnection` leader, which a lock already makes the only one,
  takes the key over (`takeOver()`) whatever a crashed tab left behind

### Changed

- **Requires lazy-watch 6.3.0**, which delivers a batch emitted from
  inside a listener after the batch being delivered. The client reverts
  a refused local batch that way, and a listener registered after it (the
  Vue mirror of `lazy-storage/vue`, an app's own `db.watch`) used to get
  the revert first and then the refused edit, and kept the edit

## [0.12.1] - 2026-09-12

### Fixed

- **A register's new value is its whole value.** A key removed from
  inside a register (a field deleted, or the value assigned without it)
  stayed on the server and in every other client, since the value was
  merged in as a patch, which keeps what it does not mention; only the
  persisted row and a fresh snapshot had it right. A register written by
  a client, by the server's own `patch`, or by a follower through a
  shared connection's relay now replaces what was there, and the patch
  sent on says so, all the way down (`replacingRegisters` in core)

## [0.12.0] - 2026-09-12

### Added

- **Port followers.** `connection.follow(port, stores)` on a shared
  connection lets the page on the other end of a MessagePort (an iframe,
  a worker) have clients on the browser's replica, as the tab's own are:
  for the stores named and no other, under the tab's rights, in a session
  that lives as long as the tab's, through a change of leader. The other
  end runs an ordinary client on `portConnection(port)`, with no socket of
  its own, whose status and pending are the browser's, as the host tells
  it; a host that is done calls what `follow` returned.
  `messagePortTransport(port)` is the transport underneath

## [0.11.1] - 2026-09-12

### Fixed

- **`connect()` from inside a `closed` or `status` event no longer breaks
  the other clients.** When the server turned the socket away, the
  connection read its reason again for each client and listener it told;
  a client that called `connect()` from its own `closed` handler (having
  new credentials by then) cleared the reason, and the next client was
  handed `null` and threw, which in a shared connection left the tab
  offline. A listener reconnecting on `offline` cleared it for everyone,
  and on an ordinary drop left a retry on top of its own socket, which
  opened a second one and never closed the first. A `connect()` called
  from inside any event of the drop now takes effect once every client
  and listener has heard: the reason stays what it was and
  `connection.closed` holds it throughout. A handler or listener that
  throws no longer keeps the rest from hearing, and a transport that
  reports its close synchronously drops the socket once
- **In a shared connection, the socket turned away reaches every tab
  once.** It reached each tab twice, as the store's and as the socket's,
  so a tab that signed in again from inside the event was told again once
  it was back and stayed offline, and a tab that reconnected on every
  `closed` looped

## [0.11.0] - 2026-09-07

### Added

- **`store.patchFrom(diff, state)`** publishes a lazy-watch batch from
  state the server keeps elsewhere: the array fragments lazy-watch emits
  (`{ 2: 'c', $length: 3 }`, a `$splice`) are replaced with the whole
  arrays read from `state`, then patched as the server's own change.
  `LazyWatch.on(live, diff => store.patchFrom(diff, live))` serves a
  LazyWatch that other code already writes
- **`mirror: true` on `createClient`**: a follower's defaults — no undo
  manager, no state cache, no presence — each still settable on its own
- README: "Serving state the server owns", for the server-authoritative
  case — isolation as store layout, read-only stores, mirror clients,
  `patchFrom`, and a store that lives as long as a job
- **`readOnly: true`** locks a whole store: every client op is refused
  with `forbidden`, the server still writes
- **A disposed store says so.** `patch`, `apply` and `session` on a store
  that was disposed (a registry's idle sweep does that) throw an error
  naming the cause and the cure — resolve it again with `stores.get(id)`
  before each write — where the proxy underneath used to complain;
  `store.disposed` reads the flag. The registry docs say that idle counts
  sessions and a server writer is not one
- **`httpSnapshots.origins`** says which origins may fetch a snapshot
  cross-origin: `'*'` (the default, as before), an array of origins to
  echo and no other, or `false` for no CORS header at all.
  `snapshotResponse` takes the same as an option

### Changed

- **Policy is judged before age.** The gates run clock guard, read-only
  paths and `validate`, then retention: an op the store would refuse
  anyway is told `forbidden` rather than `expired`
- `db.status` docs say when `online` fires: the moment a snapshot or
  delta is applied, before the batch reaches `watch` (and a snapshot equal
  to what the client had produces none), so "the store is current" is the
  status event

- **A connection's status is `online`, not `open`.** `createConnection`
  and `sharedConnection` report `'offline' | 'connecting' | 'online'`, the
  same words as a client, whose `online` additionally means its store is
  synced. Code comparing `connection.status` (or `upstream`) to `'open'`
  must change

## [0.10.1] - 2026-09-07

### Changed

- **A server patch is never held back by a tombstone.** `store.patch` is
  the authority: whatever it writes at a deleted path re-adds it, `id` or
  not. Until now the merge lifted a tombstone only for a newer object
  carrying an `id` — the right rule for a replica's edit, which may be a
  stale field write, but a server that recreated a record under a key it
  had deleted (a process that came back, an entry rebuilt from another
  source) was refused for good, since its next patches carry only what
  changed. Client ops and `store.apply` are judged as before; `mergeOp`
  takes the rule as an `authority` option

## [0.10.0] - 2026-09-05

### Added

- **Snapshots over HTTP.** Both adapters serve a store's snapshot at
  `<path>/snapshot/<store id>` (`httpSnapshots`, on by default): `{ v,
  epoch, state }`, compressed once per change and kept until the next
  (brotli, which packs this JSON some 15% tighter than gzip in the same
  time, or gzip for a client without it), with an ETag that answers 304
  while the store is unchanged, behind the same
  `authenticate` and `authorize` as the socket. A client that can fetch
  (`webSocketTransport` can, on the route resolved against the socket URL
  with its query; its `fetch` option takes your own or false) says so in
  its hello, and a snapshot of `threshold` bytes or more (default 64 KB)
  is answered with where to fetch it rather than the state. What the
  socket delivers meanwhile waits and lands on top of the fetched state.
  A fetch that fails is reported (`snapshot-fetch`) and the client asks
  again, for the snapshot inline. A 10k-task snapshot (772 KB of JSON)
  cost the socket about 4.6 ms of compression per hello; the route
  answers the fetch in about 20 µs, and a reload of an unchanged store in
  3 µs (`npm run bench` has both).
  Also: `store.snapshotJSON()`, `snapshotResponse(store, request)` for a
  server of your own, `request(req, res)` on the Node handlers,
  `httpSnapshot` on `store.session` and `httpSnapshots` on `createHub`

### Changed

- **The Bun adapter fans a broadcast out through the runtime.** Each socket
  subscribes to a topic per store, and a store's patch goes out as one
  `server.publish` that Bun encodes and compresses once for every
  subscriber, rather than a send per socket. A large write to many
  listeners costs a fraction of what it did: a 13 KB patch to 2000 clients
  fell from about 88 ms of event-loop time to about 5 ms here. Presence
  and eviction stay per socket (targeted or opt-out), and a session opened
  on the store directly is still sent to on its own. The Node adapter is
  unchanged. Behind this, `createHub` takes a `channel` and `store.session`
  a `broadcast`, for a transport that can reach every session at once
- `bun test` runs the Bun suites, which now use bun:test (`bunfig.toml`
  scopes the runner to `test/bun`; the rest of the suite is Node's, under
  `npm test`). `npm run test:bun` is the same command

### Fixed

- A row storage adapter that lacks one of its methods (`removeOp`, added
  in 0.7.0, was easy to miss) is refused with a `TypeError` when the
  client is created, rather than failing inside a listener on every op
- A shared connection closes a replica's storage adapter when it lets the
  store go (or is disposed), so an IndexedDB connection does not stay open
  per abandoned store
- A lock manager that refuses the leader request no longer leaves every
  tab waiting for a leader: the tab leads on its own, as it does without
  Web Locks

## [0.9.1] - 2026-09-05

### Changed

- `connect()` on a shared connection also prods the browser's socket when
  it is down, from any tab, so a tab the user comes back to reconnects at
  once rather than at the next backoff step

## [0.9.0] - 2026-09-05

### Added

- **One socket per browser.** `sharedConnection({ name, transport,
  storage })` makes a browser one replica however many tabs it has: the
  tabs elect a leader with the Web Locks API, the leader runs the
  browser's replica (a hidden client per store on the real socket,
  persisted through `storage`), and every tab's clients follow it over a
  BroadcastChannel, their edits going into the browser's persisted outbox
  at once, socket or no socket, then upstream under the browser's replica
  id, and every batch coming back to every tab. Presence sees one session
  per browser; each tab keeps its own undo history; a tab's `status` and
  `pending` are the browser's (the socket's status, the replica's unsent
  ops). When the leader tab closes the next tab takes over from the
  persisted outbox, IndexedDB included; a store no tab has open anymore
  is let go after `linger`, tabs that closed without a word found by the
  lock each holds. Without Web Locks or
  BroadcastChannel it is an ordinary connection. `createClient` now hands
  `attach` what it declared (`initial`, `registers`) for this, and reads
  a connection's `upstream` and `pending` when it has them
- **`clear()` on the document adapters.** `localStorageOutbox` and
  `memoryOutbox` can now forget their outbox and cache, for a store that
  is gone for good, as `indexedDBStorage.destroy()` could; nothing is
  deleted on its own, and the README says when an app should
- **`lazy-storage/testing`.** The in-memory network the suite runs on,
  for an app's own tests: `createNetwork(store)` (or a hub factory) links
  clients to a store without sockets, with `client()`, `link()`, links
  taken offline and back, and `settle()` to deliver everything queued;
  `fakeTime()` is a wall clock to hand a store and its clients as `now`
- **Compression, on by default.** Both adapters offer the
  permessage-deflate extension, so a client that takes it receives large
  messages compressed, about tenfold for a big store's snapshot; messages
  under `perMessageDeflate.threshold` bytes (default 1024) go plain,
  where compressing would cost more than it saves. The Node adapter asks
  for no context takeover. `perMessageDeflate: false` turns it off, an
  object sets `threshold` and the runtime's own options, and the
  benchmark reports what it saves
- **`stores.stats()`** on a registry rolls up the live stores' stats for a
  health endpoint: how many are live and how many idle, and the sums of
  sessions, replicas, rows, tombstones, and delta-log entries

### Changed

- A share draws on the replica's `rateLimit` bucket like an op. Beyond
  it, the share is refused with `rate-limited` and the client's next
  hello carries its latest value, so a client cannot flood a room through
  presence

## [0.8.0] - 2026-09-04

### Added

- **The outbox takes over from itself.** A new op removes, from the
  pending ops before it, whatever they wrote at or under the paths it
  writes: under last-writer-wins those older writes could never decide
  a value again. Typing into one field keeps one op pending rather than
  one per keystroke, and deleting a record drops its pending edits.
  Nothing is re-stamped, so the merge decides exactly as if every op
  had been sent; a record's `id` is never pruned, since it is what
  makes a write a record write
- **A socket that did not authenticate is told so.** A browser cannot
  read the status of a refused handshake, so `authenticate` returning
  nothing now completes the handshake only to send a `closed` message
  with code `unauthorized` (without a store: it is the socket that
  ends) and close with code 4401; a plain request still gets a 401. The
  connection stops reconnecting, `connection.closed` and
  `connection.on('closed')` carry the reason, and every client on it
  reports it as its own `closed`. `connect()` is the way back once the
  app has fresh credentials: the transport factory runs afresh

- **Vue and React entries.** `lazy-storage/vue` exports `useClient(db)`:
  a reactive mirror of the client's state, patched in place on every
  batch, local or remote, with refs for its status, presence, outbox
  size, closed reason, and undo state; it stops with the component (or
  the current effect scope), and fills `data()` in the Options API as
  well. `lazy-storage/react` exports `useClient(db)`, which reads the
  client's state and facts through `useSyncExternalStore` with one
  subscription per client, and `trackClient(db)` underneath it. Both
  frameworks are optional peers; the examples use the entries
- A `history` event on the client, carrying `{ canUndo, canRedo }` after
  a local batch, an undo, a redo, or `clearHistory()`. The undo manager
  moves its stacks after the batch it emits, so a listener on that batch
  reads them stale; this is the moment to read them
- **Peers: what a client shares rides on presence.** With presence on,
  `db.share(data)` sets a small JSON value (`null` clears it) that is
  never written to the store and lives as long as the session; the hello
  carries it, so a reconnect restores it. Every live session is a peer,
  `{ replicaId, user, key, data }`, in `db.peers` (this client's own
  entry included), the `peers` event, and `store.peers()`; the Vue and
  React entries expose `peers` too. Presence travels as deltas: the
  whole list goes only to a session that has just said hello, then
  `left`, `joined`, and `shared` for what changed, one small message to
  every session per flush, batched per turn of the event loop or per
  `presence.every`. A share over `presence.maxShare` bytes of JSON
  (default 4096), not JSON, or turned away by `presence.validate` is
  answered with an `error` and changes nothing

### Changed

- A row storage adapter must implement `removeOp(seq, meta)`, which
  takes out one pending op a newer op emptied; `saveOp` is also called
  again for an op a newer one pruned. The IndexedDB and SQLite adapters
  do both
- A transport's `onclose` receives `{ code, reason }` where the socket
  knows them; the connection reads code 4401 in case the `closed`
  message did not make it

- **Presence is off by default and set up as one option.**
  `createStore({ presence })` takes `false` (the default: nothing is
  broadcast, `store.presence()` and `store.peers()` are empty, a share is
  refused with `forbidden`), `true`, or `{ key, user, validate, every,
  maxShare }`. `key` replaces `presenceKey`; `user` chooses what of a
  user its peers see (default: all of it), which a server whose
  `authenticate` returns roles or tokens should narrow;
  `validate(data, { user, replicaId, store })` judges a share the way
  `validate` judges an op; `every` caps presence at one message per that
  many milliseconds. A client that never reads presence passes
  `presence: false` to `createClient`: its hello says so, the server
  sends it none, and it may still share. Presence goes out when a
  session says hello, ends, or shares anew, rather than when it opens,
  since a peer needs the replica id the hello brings; a session that
  never says hello is not announced. The presence message no longer
  carries `users`: peers carry `key`, and the client derives its
  presence list from them

## [0.7.0] - 2026-09-04

### Added

- **Vue and React examples.** `examples/vue` (Composition API),
  `examples/vue-options` (single-file components with the Options API,
  compiled in the browser: an App that owns the client and a list
  component), and `examples/react` are the
  shared list as framework apps, served by either example server at
  `/vue`, `/vue-options`, and `/react` on the same store as the basic
  page, with no build step (the framework comes from a CDN through an
  import map). The Vue ones read a reactive mirror patched on every batch
  and write to the client; the React one reads the client's state and
  re-renders on every batch

### Changed

- The README's opening example, the conflict and undo notes, and the
  examples use lists as arrays instead of an order register; a short
  section covers the two ways to pair the array view with a UI framework

### Removed

- **Compatibility with what versions before 0.3.0 wrote.** The store and
  the memory adapter no longer read the old `seqs` shape, the SQLite
  adapters no longer add the `seen` and `epoch` columns on open, and
  `localStorageOutbox` no longer reads the single document that carried
  the state inside the outbox. A document adapter must implement
  `saveState`; the fallback that put the state inside `save` is gone.
  Storage written by 0.3.0 or later opens as before
- `store.compactTombstones(olderThan)`: `compact()` and the retention
  window replaced it; the core `compactTombstones` stays
- `orderToPositions`: the order-register migration. Lists have carried
  positions since 0.6.0 and every known store has been migrated

## [0.6.0] - 2026-09-04

Lists stop being two things. A list of records is a keyed map with a
position on each record, ordered by `db.list`; an array of primitives is a
whole value anywhere, undeclared. Order registers keep working, and
`orderToPositions` migrates them.

### Added

- **Plain arrays in the client's state.** `createClient({ lists: ['tasks',
  'tasks/*/subtasks'] })` makes `db.state` a view in which every list is a
  real array in position order, records carrying their id and no
  position; `db.wire` is the synced state underneath, keyed maps with
  positions, and persistence, deltas, undo, and `db.list` work on it as
  before. Pushes, splices, index edits, sorts, and whole-array
  replacements are translated into record adds, deletes, field writes,
  and the fewest position changes that make the wire order match; changes
  from others arrive as splices at their sorted place, moves, and field
  patches tagged `origin: 'remote'`. A record pushed without an id gets
  one in the next batch. Tested with a randomized three-client fuzz
- **`db.list(path)`: ordered lists without a register.** Every record
  carries a position key (`pos` by default); the list's order is the keys'
  string order, ties by id, unpositioned records last. `add` (at the end,
  or `{ before }`, `{ after }`, `{ at }`), `move`, `remove`, `all`, `ids`,
  `get`, `has`, and `reconcile(ids)`, which takes the order an app shows
  and writes the fewest positions that make the sort agree. An add or a
  move writes one field on one record, so concurrent inserts at the same
  spot both survive and a move never loses to an unrelated edit. Nested
  lists are just paths. The server needs no declaration
- **Position keys** in core: `keyBetween(a, b)`, `keysBetween(a, b, n)`,
  `comparePositions`, `isPositionKey`. Fractional indexing over base 62
  with a length-prefixed integer part, so appends and prepends count
  compactly and only inserts between two keys grow a fraction
- `orderToPositions(store, { list, order, position })` on the server:
  gives every record of a list a position in its order register's order,
  deletes the register, in one patch; `*` patterns migrate a list per
  record

### Changed

- **Arrays of primitives are whole values anywhere**, no declaration
  needed: written and merged as one leaf, and a client expands lazy-watch's
  fragments from the live value for any array it touches. An array
  holding objects is still refused, now with a message pointing at
  `db.list`, unless its path is a declared register. Fragments arriving at
  the server are refused as before. The `registers` option and the
  mismatch check are unchanged for what is declared

## [0.5.2] - 2026-09-03

### Fixed

- **A client on Node 22 no longer stalls after one refused connection.**
  Node 22's global `WebSocket` (undici 6) fires only `error` for a failed
  handshake, never `close`, so `webSocketTransport` never reported the
  close and the connection's retry loop stopped after its first attempt
  against a server that was down. The transport now reports the close
  itself when the error arrives while the socket is still connecting, and
  deduplicates the `close` that browsers and Node 24+ fire after it. The
  Node server test waited on the same missing event and hung the Node 22
  CI job until it was cancelled

## [0.5.1] - 2026-09-03

### Changed

- **Snapshots cost a fraction of what they did.** A snapshot message is
  now encoded straight from the state's plain target instead of a deep
  copy through the proxy, and the encoding is kept until the next
  accepted op and spliced into every snapshot sent meanwhile; the
  message's `state` is decoded only for a consumer that reads the object.
  On a 10 000-task store a snapshot after an op went from 13 ms to 3 ms,
  and one on a quiet store to a few microseconds, so a burst of first
  connections pays for one encoding. `tagStore` copies a message's
  properties as they are declared, getters included
- The README's wire protocol section is restructured: a session in
  order, one table per direction, and the refusal and closed codes with
  what the client does about each

## [0.5.0] - 2026-09-03

TypeScript declarations, a Node server, examples, a benchmark, and the
two costs the benchmark found. No protocol or storage-format change;
0.4.0 servers and clients interoperate.

### Added

- **TypeScript declarations** for every entry, hand-written under
  `types/` and wired into the package exports: the client (`Client<S>`,
  `ClientOptions`, storage adapter shapes), the server (`Store<S>`,
  `StoreOptions`, the storage interface, registry, hub), core (clocks,
  paths, the merge, the wire protocol as `ClientMessage` and
  `ServerMessage`), and the Bun, Node, and SQLite entries. A type check
  (`npm run test:types`) compiles a file that uses the API as an app
  would, wrong usages included, and runs in CI
- **A Node server.** `lazy-storage/server/node` serves stores over the
  `ws` package (an optional peer dependency) with the same `serve`,
  `createHandlers`, hooks, limits, and graceful `close` as the Bun
  adapter; `authenticate` receives a Web `Request` built from the Node
  request, so one function serves both. `lazy-storage/server/sqlite-node`
  is the SQLite adapter on `node:sqlite` (Node 22.13 and later), reading
  and writing the same files as the Bun one. Both are tested end to end
  in the Node suite
- **Examples.** `examples/basic` is a shared list in the browser over a
  Bun server, `examples/node` the same page served by Node, and
  `examples/mirror.js` a client running in a Bun or Node process; all run
  from a checkout with no build step
- **A benchmark.** `npm run bench` times the merge, the gates, broadcasts
  to many sockets, a client's local op on each kind of storage, and
  reconnects answered with a snapshot versus a delta, as medians over
  several rounds

### Changed

- The two SQLite adapters share one implementation (`sqlite-shared.js`);
  the Bun one behaves as before

### Fixed

- **Every write cost as much as the store was large.** The merge found
  the descendants of a path by scanning every clock key, so a one-leaf
  op into a store of 10 000 tasks took half a millisecond and a
  ten-leaf record add fifteen. The clock table now keeps a children
  index (`ClockMap` in core); the same ops take 29 and 64 microseconds.
  Found by the new benchmark
- **Every local op on a client with registers snapshotted the whole
  state.** `expandRegisters` copied the entire state to read the
  registers a diff touched; it now copies only those. A client op on a
  thousand-task state went from 1.1 ms to 13 µs on the row adapter, and
  the same saving applies to every browser tab of an app that declares a
  register

## [0.4.0] - 2026-09-03

Row persistence on the client, deltas that survive a deploy, and the
ceilings and hooks a public server needs. Also fixes a 0.3.0 bug where a
reconnect answered with a delta could leave a register on a stale value.
Storage from 0.3.0 opens as is; servers and clients should upgrade
together.

### Added

- **The delta log survives a restart.** A commit carries the accepted diff
  as `log: { v, diff }` and a `logFloor`; the SQLite and memory adapters
  keep the entries (a `log` table, pruned to the floor) and hand them back
  on load, so the first reconnects after a restart or a deploy are deltas
  too. A persisted log is used only where it is contiguous and ends at the
  current version. The JSON file adapter does not keep it
- **Graceful shutdown.** `createHandlers` returns `close({ reason })`, and
  `serve()`'s server gains `shutdown({ reason })`: new sockets are refused
  with 503, open ones are closed with WebSocket code 1001 so clients
  reconnect at once, and the store registry is disposed, flushing every
  store. Together with the persisted log, a deploy costs each client a few
  small messages
- **Limits.** `maxPayload` on the Bun adapter (default 4 MB) ends a socket
  that sends a larger message; a hello now carries at most 1000 ops, the
  rest following as ops once the answer lands. `maxLeaves` on the store
  (default 10 000) refuses an op touching more leaves with code
  `too-large`. `rateLimit` on the store (a token bucket per replica,
  default `{ burst: 500, perSecond: 100 }`) refuses a live op beyond it
  with code `rate-limited` and a `retryAfter`; the client keeps the op and
  resends its outbox in a hello after that, so nothing is lost
- **Idle store release.** `createStores(factory, { idle })` releases a
  store that has had no session for `idle` ms, flushing its storage, and
  loads it again on the next request; `sweep()` runs it by hand. Off by
  default
- **Observability.** `store.observe('op' | 'refused' | 'session', fn)`
  reports merged ops, refused client ops, and sessions opening and
  closing; `store.stats()` counts what the store holds. `onError` on
  `createHandlers`, `createHub`, and `createStore` receives server faults
  (a store factory or an observer that throws, a bug while handling a
  message) instead of the console; the client's persistence faults reach
  its `error` event
- The Bun adapter is now tested in the library itself, end to end over
  real sockets (`test/bun/serve.test.js`)
- **Row persistence on the client.** A storage adapter may keep one row
  per leaf and one per pending op instead of a state document: every
  batch the state applies is walked into leaves and committed as the rows
  it touched (a deletion takes the path and everything under it), and a
  snapshot replaces the rows, healing anything a failed write left. So an
  edit costs its leaves, however large the state. Two adapters:
  `indexedDBStorage(name, { onError })` for browsers, with `settled()`,
  `close()`, and `destroy()`, and `sqliteClientStorage(file)`
  (`lazy-storage/client/sqlite`) for a client running in Bun. The row
  adapter interface is `{ load, commit, replace, saveOp, dropOps }`, every
  write carrying the client's `{ replicaId, seq, version, epoch }`
- `openClient(options)`: `createClient` for an adapter whose `load()`
  returns a promise (IndexedDB does); resolves to the client. `createClient`
  refuses such an adapter with a pointer to it
- `rebuild(initial, rows)` in `lazy-storage/core`: `initial` with rows
  applied shallow-first, now shared by the server's load and the client's
  restore

### Fixed

- A delta could leave a reconnecting client on a stale value. The
  correction for a leaf that one of the hello's ops lost was read the
  moment that op was merged; a later op in the same hello could then win
  the same leaf, and since a client applies corrections last, the stale
  value stuck (a register was the usual victim). Corrections are now taken
  once, after every op of the hello. Found by the convergence fuzzer;
  present since 0.3.0

## [0.3.0] - 2026-09-03

Three guards a deployed store needs, and three costs that no longer grow
with the size of the state or the number of sockets. Servers and clients
should upgrade together: an old client still syncs (it gets snapshots), but
is dropped from every conflict once its clock runs ahead, since it cannot
correct itself.

### Added

- **Write authorization.** `createStore` takes `readOnly`, path patterns
  (register syntax, `*` allowed) clients may not write: an op with a leaf
  at or under one is refused whole with code `forbidden`. And
  `validate(diff, { user, replicaId, store })`, asked for every client op
  after that check: return `false` or throw to refuse (the message reaches
  the client), return a diff to accept that instead (the client is
  corrected on the leaves it left out), or `true`/nothing to accept. The
  server's own `patch` and a bare `apply` skip both; `apply(op, session)`
  is the client path
- **A clock guard.** An op stamped more than `maxSkew` (default five
  minutes) ahead of the server's clock is refused with code `clock-skew`
  and the server's time, before it can win every conflict or drag the
  server's clock forward. The client adopts the server's time as an
  offset, rewinds its hybrid clock (never behind what it has received),
  re-stamps the refused op and every pending op after it, and sends them
  again, so nothing is lost and no error reaches the app. The clock gained
  `rewind()` for this
- **A retention window.** Tombstones and per-replica progress are kept for
  `retention` (default 30 days); `store.compact()` forgets what is older
  and reports `{ tombstones, replicas }`, and runs by itself on load and
  every `compactEvery` (default one hour) as ops arrive. An op stamped
  before the window is refused with code `expired`, because the deletion
  it might resurrect may already be forgotten; the client drops it and
  resyncs. `retention: Infinity` keeps everything. `store.replicas` lists
  the replicas still remembered
- **Deltas on reconnect.** A hello carries the store version the client
  last saw (`since`, with the store's `epoch`), and the server answers
  with `{ t: 'delta', patches }`: the accepted diffs since then, in order,
  followed by corrections for what the hello's own ops lost. The store
  keeps the last `deltaLog` accepted diffs (default 1000) in memory for
  it, and sends a snapshot when the log does not reach back far enough,
  when the epoch differs (the client's cache remembers storage since
  wiped), or when a hello op was refused. A reconnect that missed nothing
  costs one empty delta. `db.version` is the client's position; `store.epoch`
  identifies a life of the storage and is minted on the first commit
- `toJSON(message)` and `tagStore(message, id)` (`lazy-storage/server`):
  a broadcast is encoded once for every socket it reaches, and a hub
  splices its store id into the encoded JSON instead of encoding the
  payload again. The Bun adapter uses them; a transport of your own can too

### Changed

- **The client no longer serializes its whole state on every local op.**
  The outbox is written synchronously as before, but the state cache is
  written debounced (50 ms after the last change, flushed on dispose), and
  a restore replays the outbox over the cached state so the few ops it may
  be behind by still show. Client storage adapters gain `saveState(cache)`
  for the state (`localStorageOutbox` keeps it under `key:state`; a
  document from before still loads); an adapter without it gets the state
  inside `save` as before. The cache also records `version` and `epoch`
- `snapshot` and `patch` messages carry the store version (`v`); the
  storage interface carries `epoch` in `load()` and `commit()` (SQLite adds
  the column on open, the JSON document a field)
- Storage adapters record a replica's progress as `{ seq, seen }` (`seen`:
  the store's clock when its last op arrived) under `replicas` in `load()`,
  and `commit` may carry `forgetReplicas`. Documents and databases written
  by 0.2.x load as before: the store still reads `seqs`, SQLite adds the
  `seen` column on open, and a replica without one gets a full window
  before it is pruned
- Every refused op now carries a code (`invalid` for one that breaks the
  model); a client refused during a hello no longer sends a second hello,
  since the snapshot is already on its way

## [0.2.2] - 2026-09-03

A server no longer crashes when a store fails to open. No protocol or
storage-format change.

### Fixed

- A store factory (or a migration inside it) that threw while a hub opened
  a store propagated out of the message handler and, under Bun, took the
  server process down. The hub now refuses that store with a `closed`
  message (code `unknown-store`, carrying the error's message), logs the
  fault, and leaves the connection and its other stores alone; a
  synchronously throwing `authorize` is treated as a refusal. The Bun
  adapter additionally catches anything else thrown while handling a
  message and answers an `error` instead of crashing

## [0.2.1] - 2026-09-03

A client restarted while offline now starts from the state it last saw.
No protocol or storage-format change; 0.2.0 servers and clients interoperate.

### Added

- **Cached state across restarts.** The client's storage adapter now
  keeps the last state next to the outbox (written synchronously with
  every local op, coalesced after remote batches), and a client whose
  storage holds a state for its replica starts from it — pending edits
  already applied — instead of from `initial`. A tab reloaded while
  offline shows its data; the snapshot on reconnect brings it up to date
  as before. `db.restored` reports it; `cache: false` keeps only the outbox

## [0.2.0] - 2026-09-03

The first published version. Since 0.1.0 (never published) the API was
reshaped around one protocol and one route: every message names its
store, a client always names its store and either shares a connection or
owns one, the server keeps a hub per socket. Persistence became
row-oriented with a SQLite adapter on Bun, and the server grew
authentication and authorization hooks, presence, eviction, embedding into
an existing Bun.serve, keepalive, wildcard registers, and a registry for
many stores. The todo app in the sibling repository is the reference
consumer.

### Removed

- **The single-store mode and the per-store URL.** There is one protocol
  (every message names its store) and one route (a hub at `path`).
  `serve({ store })` and `/ws/<storeId>` are gone; a server with one store
  passes `stores: () => store`. `createClient` always takes a `store` id,
  with either a shared `connection` or a `transport` for a connection the
  client owns; `createConnection` lost its `multiplex` flag. The untagged
  protocol had no upside that survived scrutiny (a browser cannot read a
  403 at upgrade anyway, and a `closed` message with a code says more) and
  cost every feature a second code path
- `version` no longer travels on the wire (no client read it); it stays on
  the store for tests and debugging

### Changed

- The snapshot names the server's register patterns, and a client whose
  declaration differs raises an error with code `registers-mismatch` on
  every snapshot, so a silent divergence between the two sides is loud

### Added

- **Authentication and authorization hooks.** `serve` / `createHandlers`
  take `authenticate(req)` (the user for a request; null answers 401 at
  upgrade) and `authorize(user, storeId, store)`, asked per store before
  its session exists; a refusal reaches the client as a `closed` message
  with code `forbidden` for that store alone. Both may return promises; a
  hub queues a store's messages while its authorization is in flight. The
  user rides on the session (`store.session({ send, user, onEvict })`,
  `createHub(..., { user, authorize })`)
- **Eviction.** `store.closeSessions(predicate, message)` ends the
  sessions a predicate selects with a `closed` message (code `evicted`) and
  tells their transport through `onEvict` (the hub drops its entry for that
  store; the socket stays up for the others). The client
  gains a `closed` event and `db.closed`, goes offline for that store
  without reconnecting on its own, and rejoins on `connect()`. Terminal
  hub refusals (`unknown-store`, `invalid-store`, `forbidden`) now arrive
  as `closed` too, instead of `error`; server errors carry a `code`
- **Presence.** Stores broadcast the distinct users with a live session
  when one joins or leaves (and hand the list to a new anonymous session);
  `db.presence` and the `presence` event on the client, `store.presence()`
  on the server, `presenceKey` to change how users are deduplicated
  (default: by `id`)
- **Embedding.** `createHandlers` returns `{ upgrade(req, server), websocket }`
  to mount lazy-storage's sockets inside an existing `Bun.serve`; `serve`
  is now a wrapper over it
- **Keepalive.** A connection pings every 30 s while open (`keepalive`
  option; `false` disables), so idle sockets survive proxies and server
  idle timeouts; the timer never keeps a Node process alive
- **Wildcard registers.** A `*` segment in a register path matches one
  segment (`tasks/*/subtaskOrder`), declaring a register per record;
  `registerSet` is now a matcher and `expandRegisters` walks the diff
- **Any number of stores over one socket.** `createConnection({ transport })`
  is a connection that several clients attach to, one per store
  (`createClient({ connection, store })`); every message names its store,
  and `client.disconnect()` on a shared connection leaves just that store
  (`{ t: 'leave', store }`) while `connection.close()` drops the socket for
  all. Each client keeps its own outbox, undo history, and status. A client
  created with a `transport` instead owns a connection of its own, same
  protocol. On the server, `createHub(resolveStore, { send, user, authorize })`
  is the session-shaped counterpart that keeps one store session per
  socket, and `serve({ stores })` serves it at `path`. Every fuzzer client
  carries two stores on its socket, so an offline toggle drops both
- **SQLite persistence on Bun.** `sqliteStorage(file)` from
  `lazy-storage/server/sqlite` keeps any number of stores in one database
  file, one row per leaf path keyed by `(store, path)`, in WAL mode; each
  op commits as one transaction of the rows it touched. `sqlite.store(id)`
  yields a store's adapter, `ids()` lists stores, `remove(id)` drops one,
  and `db` exposes the connection. Tested with `npm run test:bun`
- **Multiple stores per server.** `createStores(factory)` is a registry
  that builds a store per id on first use and keeps it live (`get`, `has`,
  `ids`, `release`, `dispose`); `isStoreId` restricts ids to a URL- and
  filename-safe alphabet. The hub resolves stores through it (or through a
  plain `id => store|null` function; `() => store` for a single store),
  answering an invalid id with `closed` code `invalid-store` and a refused
  one with `unknown-store`

### Changed

- **Row-oriented persistence.** A store now persists as rows, one per leaf
  path (`{ value, ts, deleted }`), plus per-replica sequence numbers and
  the version, and the storage interface is incremental:
  `load()` / `commit({ upserts, deletes, replica, version })` / `flush()`.
  Every accepted op commits exactly the rows it won and the clock entries
  it dropped (the merge now reports both), so a row-oriented backend
  writes only what changed. On load the state is `initial` with the rows
  applied on top: `initial` is the skeleton an app expects (its top-level
  containers), rows carry the data, and a container added to `initial`
  later appears without a migration. The memory and JSON-file adapters
  implement the new interface; the JSON file's format changed from a
  state document to a row list (0.1.0 files are not read)
- `store.compactTombstones` and the core `compactTombstones` commit the
  removed rows; the core helper returns the removed keys instead of a count

## [0.1.0] - 2026-09-02

Initial version: keyed collections, per-leaf last-writer-wins with
tombstones on a single server merge point, hybrid logical clocks, a
persisted client outbox with snapshot-on-hello resync, registers for
whole-value arrays, an undo manager that declines remote batches, memory
and JSON-file server storage, and a Bun WebSocket adapter.
