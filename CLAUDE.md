# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

lazy-storage is a self-hosted realtime state store with offline sync, built on [lazy-watch](https://github.com/luffs/lazy-watch). A server holds each store's state tree and merges ops into it last-writer-wins per leaf path; clients keep a synced, offline-capable mirror in a LazyWatch proxy, turn every local batch into an op, and keep an outbox that survives reconnects and reloads. Plain ES modules, no build step, one runtime dependency (lazy-watch). The README is the user-facing reference: the model, lists, conflicts, offline, persistence, multiple stores, auth and presence, the API, and the wire protocol.

## Development Commands

```bash
npm test              # node --test "test/*.test.js": unit and integration tests, plus fixed-seed runs of the three fuzzers
npm run test:bun      # the Bun-only tests in test/bun (bun:sqlite adapters, the Bun server)
npm run test:types    # tsc over test/types against the declarations in types/
npm run test:coverage # the Node suite under c8; fails below 96% lines/statements, 88% branches, 92% functions
npm run fuzz          # a longer convergence campaign: node test/fuzz/run.js [--mode convergence|hostile] [--seed N] [--runs N] [--steps N] [--clients N]
npm run fuzz:hostile  # the same with an attacker beside honest clients
node test/fuzz/run.js --mode relay   # displays behind a relay (src/relay) through outages, relay restarts and restores
npm run bench         # server benchmark (bench/run.js); medians of 5 rounds
npm run bench:client  # client benchmark (bench/client.js) in happy-dom: React/Vue renders, list views, persistence
npm run bench:check   # both benchmarks with --check: every case under its ceiling, the client counts within their bounds
```

Run one test file with `node --test test/facade.test.js`, one test with `--test-name-pattern="..."`. A fuzz failure prints its seed and a reproduction command. CI (`.github/workflows/test.yml`) runs the tests on Node 22/24/26, the Bun tests, the coverage gate, the benchmark guard, and a random-seed campaign of both fuzzers; the publish workflow runs tests, types, the coverage gate and the Bun tests before `npm publish`.

## Layout

- `src/core/` — shared by server and clients: `hlc.js` (hybrid logical clock, stamps are `[ms, count, replicaId]`), `merge.js` (per-path LWW with tombstones), `clocks.js` (the per-path clock table), `model.js` (the data model on top of lazy-watch: leaves, registers, `expandRegisters`, `rebuild`), `paths.js`, `positions.js` (fractional-index position keys for lists), `ids.js`
- `src/client/` — `index.js` (`createClient`: the mirror, ops, outbox and `supersede`, undo, events), `connection.js` (one socket for many clients: reconnect, backoff), `facade.js` (list paths as plain arrays over keyed maps with positions), `list.js` (`db.list(path)`), `persistence.js` + `storage.js` (document adapters: `localStorageOutbox` etc.), `indexeddb.js` and `sqlite-bun.js` (row adapters), `shared.js` + `port.js` (one socket per browser: a leader tab), `transport.js`
- `src/server/` — `store.js` (`createStore`: the authority; merges ops, holds state, serves sessions, presence, retention, rate limits), `registry.js` (`createStores`: many stores, idle sweep), `hub.js` (many stores over one socket), `bun.js` / `node.js` (WebSocket servers), `snapshot.js` (the HTTP snapshot route), `storage.js` (memory and JSON-file adapters), `sqlite-shared.js` + `sqlite-bun.js` / `sqlite-node.js` (row-per-leaf SQLite, leases, migrations, backups), `wire.js` (encode once, tag per store)
- `src/react/`, `src/vue/` — the framework bindings (`useClient`, `useClientSelector`; a shared read-only Vue mirror)
- `src/relay/` — a relay on the LAN (`lazy-storage/relay`): `index.js` (`createRelay`: clients' sockets passed through to the server under their own credentials while it answers, a copy of each store built from what passes, and answers from the copy while the server is away, acknowledging nothing), `storage.js` (`memoryCopies`, `fileCopies`), `bun.js` (`createRelayHandlers`, `upstreamSocket`: `lazy-storage/relay/bun`)
- `src/testing/` — `createNetwork` (in-memory network: clients linked to a store without sockets) and `fakeTime`, published as `lazy-storage/testing`
- `src/index.js` — the client entry; it re-exports `LazyWatch` so apps drive `db.state` with the same copy lazy-storage uses (two copies do not recognize each other's proxies)
- `types/` — hand-written declarations, one file per export path; `test/types/` checks them
- `examples/`, `bench/`, `test/` (`helpers.js` re-exports `createNetwork`/`fakeTime` and adds `seededRandom`; `test/fuzz/`; `test/bun/`)

## Architecture

- **State and ops.** State is a tree of plain objects and JSON leaves. A local batch on the client's LazyWatch proxy becomes one op `{ replicaId, seq, ts, diff }`; the server merges it leaf by leaf (a newer stamp wins per path, deletions leave tombstones) and broadcasts the accepted part as a patch. `null` means delete, on the wire and in state.
- **Arrays travel whole.** An array is a value, not something merged index by index: `expandRegisters` replaces every array a batch touched with its whole current value. Paths declared as `registers` are arrays of ids written whole. Lists of records are keyed maps whose records carry a position key; with `lists` declared, the client shows them as plain arrays (`facade.js`) and translates between the two in both directions.
- **The client.** `db.state` is the mirror (with lists, the view; `db.wire` the synced keyed maps). The state has `inverse: true`, used both to revert a batch the model rejects and by the undo manager (on unless `mirror: true`). The outbox holds unacknowledged ops; a new op supersedes older pending writes to the paths it writes (`supersede`), and the outbox is persisted by the storage adapter. A reconnect sends a hello with the outbox and the version the client knows; the server answers with a delta or a snapshot (inline, or over HTTP when large).
- **The server.** A store is one LazyWatch state, the clock table, and a storage adapter it commits to on every accepted op. `store.state` is read-only by contract; server-side writes go through `store.patch(diff)`, `store.apply(op)` or `store.patchFrom(diff, live)`. SQLite adapters lease a store to one process, run versioned migrations on load, and can back up and restore while serving; a restore changes the store's epoch and clients hear `reset`.

## Conventions

- Match the surrounding code: small modules, named functions, comments that say why and describe the design in prose (file headers explain the module's model). Keep the comment density of the file you edit.
- Tests use `node:test` and `node:assert/strict`, and drive clients and stores through `createNetwork` rather than sockets: "edit, `await net.settle()`, assert". A bug fix comes with a test that fails without it; a fuzz-found bug gets a pinned test.
- lazy-watch is a dependency, not a peer: its version lives in `package.json` and `bun.lock` (the tracked lockfile). `npm install` creates a `package-lock.json` the repo does not track; delete it. Bump with `bun install` after editing the range, and check the lockfile's integrity matches `npm view lazy-watch@<version> dist.integrity`.
- `CHANGELOG.md` follows Keep a Changelog. Work goes under `## [Unreleased]` (Added / Changed / Fixed / Security), each bullet a bold lead sentence and the what and why after it; a release turns the heading into `## [x.y.z] - YYYY-MM-DD` with a short summary paragraph under it that ends with what to check on upgrade.
- Commits: `feat:`, `fix:`, `chore:`, `release:` plus a subject in the imperative, and a body that explains the change. A release commit only moves the changelog heading and the version in `package.json`.
- Releasing is the maintainer's: they push master and create the GitHub release, whose `published` event runs the publish workflow (npm trusted publishing). Never push or tag. Push master before creating the release, or the tag lands on the previous commit.
