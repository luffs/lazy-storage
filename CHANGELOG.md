# Changelog

All notable changes to lazy-storage are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning.

## [Unreleased]

### Added

- **Many stores over one socket.** `createConnection({ transport })` is a
  shared, multiplexed connection that several clients attach to, one per
  store (`createClient({ connection, store })`); messages are tagged with
  the store id, and `client.disconnect()` on a shared connection leaves
  just that store (`{ t: 'leave', store }`) while `connection.close()`
  drops the socket for all. Each client keeps its own outbox, undo history,
  and status. On the server, `createHub(resolveStore, { send })` is the
  session-shaped counterpart that keeps one store session per socket, and
  `serve({ stores })` serves it at `path` while `path/<id>` keeps the
  plain per-store protocol. A client with its own `transport` gets a
  private, untagged connection, so single-store servers and URLs are
  unchanged. The fuzzer gained a `hub` mode in which clients share two
  connections and an offline toggle drops everyone on that socket

- **SQLite persistence on Bun.** `sqliteStorage(file)` from
  `lazy-storage/server/sqlite` keeps any number of stores in one database
  file, one row per leaf path keyed by `(store, path)`, in WAL mode; each
  op commits as one transaction of the rows it touched. `sqlite.store(id)`
  yields a store's adapter, `ids()` lists stores, `remove(id)` drops one,
  and `db` exposes the connection. Tested with `npm run test:bun`
- **Multiple stores per server.** `createStores(factory)` is a registry
  that builds a store per id on first use and keeps it live (`get`, `has`,
  `ids`, `release`, `dispose`); `isStoreId` restricts ids to a URL- and
  filename-safe alphabet. `serve({ stores })` routes `/ws/<storeId>` to
  it, answering 400 for an invalid id and 404 for one the factory refuses;
  `serve({ store })` still serves a single store at `/ws`

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
