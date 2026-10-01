// sqlite-node.test.js - The node:sqlite adapter (Node 22.13+); skipped where node:sqlite is missing
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore } from '../src/server/index.js';

const INITIAL = { tasks: {}, order: [], settings: { theme: 'light' } };
const T = (ms, id = 'x') => [ms, 0, id];

let sqliteStorage = null;
try {
  ({ sqliteStorage } = await import('../src/server/sqlite-node.js'));
} catch (err) {
  console.log(`# node:sqlite unavailable here (${err.message}); skipping`);
}

test('a store round-trips through node:sqlite: rows, replicas, epoch, and the delta log survive a reopen', { skip: !sqliteStorage }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  const file = join(dir, 'stores.sqlite');
  try {
    let sqlite = sqliteStorage(file);
    assert.equal(sqlite.store('a').load(), null);
    const one = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1'), deltaLog: 3 });
    one.patch({ tasks: { a: { id: 'a', title: 'Kept', done: false }, b: { id: 'b', title: 'Gone' } }, order: ['b', 'a'] });
    one.patch({ tasks: { a: { done: true } }, settings: { theme: 'dark' } });
    one.patch({ tasks: { b: null }, order: ['a'] });
    one.patch({ tasks: { c: { id: 'c' } } });
    const epoch = one.epoch;
    one.dispose();
    assert.deepEqual(sqlite.ids(), ['team-1']);
    sqlite.close();

    sqlite = sqliteStorage(file);
    const loaded = sqlite.store('team-1').load();
    assert.equal(loaded.epoch, epoch);
    assert.deepEqual(loaded.log.map(e => e.v), [2, 3, 4], 'the turn\'s four patches, one commit, pruned to the floor they left');
    const two = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1'), deltaLog: 3 });
    assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'Kept', done: true }, c: { id: 'c' } }, order: ['a'], settings: { theme: 'dark' } });
    assert.equal(two.version, 4);
    assert.equal(two.stats().log, 3, 'the store keeps its own last three');
    const late = two.apply({ replicaId: 'late', seq: 1, ts: T(1, 'late'), diff: { tasks: { b: { title: 'ghost' } } } });
    assert.equal(late.accepted, null, 'the tombstone survived the reopen');
    assert.equal(two.apply({ replicaId: 'server', seq: 1, ts: T(1), diff: { settings: { theme: 'x' } } }).duplicate, true, 'replica progress survived');
    two.dispose();
    sqlite.remove('team-1');
    assert.deepEqual(sqlite.ids(), []);
    assert.equal(sqlite.store('team-1').load(), null);
    sqlite.close();
  } finally {
    for (let attempt = 0; attempt < 5; attempt++) {
      try { rmSync(dir, { recursive: true, force: true }); break; } catch { /* Windows holds the WAL files briefly */ }
    }
  }
});


test('a file from before replicas had owners gains the column, and an owner survives a reopen', { skip: !sqliteStorage }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  const file = join(dir, 'old.sqlite');
  try {
    const { DatabaseSync } = await import('node:sqlite');
    const old = new DatabaseSync(file);
    old.exec(`CREATE TABLE replicas (store TEXT NOT NULL, replica TEXT NOT NULL, seq INTEGER NOT NULL, seen INTEGER, PRIMARY KEY (store, replica)) WITHOUT ROWID;
      CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0, epoch TEXT) WITHOUT ROWID;
      INSERT INTO stores VALUES ('main', 3, 'e1');
      INSERT INTO replicas VALUES ('main', 'legacy', 7, 1000);`);
    old.close();

    const sqlite = sqliteStorage(file);
    const storage = sqlite.store('main');
    assert.deepEqual(storage.load().replicas, { legacy: { seq: 7, seen: 1000 } }, 'an old row reads as before');
    storage.commit({ upserts: [], deletes: [], replica: { id: 'r1', seq: 0, seen: 2000, owner: 'ann' }, version: 3, epoch: 'e1' });
    storage.commit({ upserts: [], deletes: [], replica: { id: 'r1', seq: 4, seen: 3000 }, version: 4, epoch: 'e1' });
    sqlite.close();

    const again = sqliteStorage(file);
    assert.deepEqual(again.store('main').load().replicas.r1, { seq: 4, seen: 3000, owner: 'ann' }, 'progress moves on, the owner stays');
    again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('one process serves a store: a second is refused until the first lets it go, and one that lost its lease cannot write', { skip: !sqliteStorage }, async () => {
  const { createHub } = await import('../src/server/index.js');
  const { hostname } = await import('node:os');
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  const file = join(dir, 'leased.sqlite');
  try {
    const first = sqliteStorage(file);     // two handles on one file: two processes, as far as the file can tell
    const second = sqliteStorage(file);
    const one = createStore({ initial: INITIAL, storage: first.store('team-1') });
    one.patch({ tasks: { a: { id: 'a' } } });
    assert.throws(() => createStore({ initial: INITIAL, storage: second.store('team-1') }), err => err.code === 'store-locked');
    const faults = [];
    const other = createStore({ initial: INITIAL, storage: second.store('team-2'), onError: err => faults.push(err.code) });
    other.patch({ tasks: { b: { id: 'b' } } });   // another store in the same file is served there
    other.flush();                                 // and stored now, not at the end of the turn (see groupCommit)

    // Through a hub, a client of the second process hears 'unavailable', not a final refusal
    const sent = [];
    const hub = createHub(id => createStore({ initial: INITIAL, storage: second.store(id) }), { send: m => sent.push(m), onError: err => assert.fail(err.message) });
    hub.receive({ t: 'hello', store: 'team-1', replicaId: 'r', ops: [] });
    assert.deepEqual([sent[0].t, sent[0].code], ['closed', 'unavailable']);

    one.dispose();                          // the first lets it go (a registry's release, a shutdown)
    const taken = createStore({ initial: INITIAL, storage: second.store('team-1') });
    assert.deepEqual(taken.state.tasks, { a: { id: 'a' } }, 'and the second serves it, from the rows the first wrote');

    // A lease left by a process on this machine that is gone is taken at once
    first.db.prepare('UPDATE leases SET holder = ?, host = ?, pid = ?, until = ? WHERE store = ?').run('dead', hostname(), 2 ** 22 + 12345, Date.now() + 60_000, 'team-2');
    const revived = createStore({ initial: INITIAL, storage: first.store('team-2') });
    assert.deepEqual(revived.state.tasks, { b: { id: 'b' } });
    // ...and the process it was taken from can no longer write: its store unloads instead, when
    // the change is stored (at the end of the turn, see groupCommit, or at flush(), which throws)
    other.patch({ tasks: { late: { id: 'late' } } });
    assert.throws(() => other.flush(), err => err.code === 'unavailable');
    assert.equal(other.disposed, true);
    assert.deepEqual(faults, ['lease-lost'], 'reported as the fault it is');
    assert.equal(revived.state.tasks.late, undefined);

    taken.dispose();
    revived.dispose();
    hub.close();
    first.close();
    second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a store leased on another host says it cannot tell whether that process runs; one on this host does not', { skip: !sqliteStorage }, async () => {
  const { hostname } = await import('node:os');
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  const first = sqliteStorage(join(dir, 'hosts.sqlite'));
  const second = sqliteStorage(join(dir, 'hosts.sqlite'));
  const one = createStore({ initial: INITIAL, storage: first.store('team-1') });
  const refusal = () => {
    try {
      createStore({ initial: INITIAL, storage: second.store('team-1') });
    } catch (err) {
      return err;
    }
    assert.fail('the store was served twice');
  };
  try {
    // A process on this host, which runs: it is waited for, and nothing is said of telling
    const here = refusal();
    assert.equal(here.code, 'store-locked');
    assert.ok(here.message.includes(`is open in another process (${hostname()}, pid ${process.pid})`), here.message);
    assert.ok(!here.message.includes('cannot be told'), here.message);
    // On another host (the container before this one): whether it runs cannot be told, and a killed one holds the store until its lease runs out
    first.db.prepare('UPDATE leases SET host = ? WHERE store = ?').run('f812a3c6b6b5', 'team-1');
    const elsewhere = refusal();
    assert.equal(elsewhere.code, 'store-locked');
    assert.ok(elsewhere.message.startsWith('Store "team-1" is open in another process (f812a3c6b6b5, pid '), elsewhere.message);
    assert.ok(elsewhere.message.endsWith('s after it stops renewing; from here it cannot be told whether that process still runs: one killed without closing the file holds the store until its lease runs out'), elsewhere.message);
  } finally {
    one.dispose();
    first.close();
    second.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a batch of changes leaves each row as the last of them left it, and is stored whole or not at all', { skip: !sqliteStorage }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  try {
    const sqlite = sqliteStorage(join(dir, 'batch.sqlite'));
    const storage = sqlite.store('main');
    storage.load();   // takes the lease, as a store does
    const change = (version, rest) => ({ upserts: [], deletes: [], version, epoch: 'e', schema: 0, logFloor: 1, ...rest });
    // One turn's changes: each row is written once, as the last of them leaves it
    storage.commitMany([
      change(1, { upserts: [['["a"]', { value: 1, ts: T(1) }], ['["b"]', { value: 1, ts: T(1) }]], replica: { id: 'r1', seq: 1, seen: 10 }, log: { v: 1, diff: { a: 1, b: 1 } } }),
      change(2, { deletes: ['["a"]'], replica: { id: 'r2', seq: 1, seen: 20 }, log: { v: 2, diff: { a: null } } }),
      change(3, { upserts: [['["a"]', { value: 3, ts: T(3) }], ['["b"]', { ts: T(3), deleted: true }]], replica: { id: 'r1', seq: 2, seen: 30 }, forgetReplicas: ['r2'], log: { v: 3, diff: { a: 3, b: null } } })
    ]);
    const loaded = storage.load();
    assert.deepEqual(loaded.rows, [['["a"]', { value: 3, ts: T(3) }], ['["b"]', { ts: T(3), deleted: true }]], 'written, deleted, written again; and a tombstone');
    assert.deepEqual(loaded.replicas, { r1: { seq: 2, seen: 30 } }, 'the last progress, and a replica forgotten after it was recorded');
    assert.equal(loaded.version, 3, 'the last version');
    assert.deepEqual(loaded.log.map(e => e.v), [1, 2, 3], 'and every log entry');

    // Another process took the store: the batch that finds out is stored not at all
    sqlite.db.prepare('UPDATE leases SET holder = ?, until = ? WHERE store = ?').run('other', Date.now() + 60_000, 'main');
    assert.throws(() => storage.commitMany([
      change(4, { upserts: [['["c"]', { value: 4, ts: T(4) }]], log: { v: 4, diff: { c: 4 } } }),
      change(5, { upserts: [['["a"]', { value: 5, ts: T(5) }]], log: { v: 5, diff: { a: 5 } } })
    ]), err => err.code === 'lease-lost');
    assert.equal(sqlite.db.prepare('SELECT version FROM stores WHERE store = ?').get('main').version, 3);
    assert.equal(sqlite.db.prepare('SELECT COUNT(*) AS n FROM leaves WHERE store = ? AND path = ?').get('main', '["c"]').n, 0);
    sqlite.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the WAL is copied back into the file on a worker thread, not by the thread that commits', { skip: !sqliteStorage }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  try {
    const file = join(dir, 'checkpoints.sqlite');
    const faults = [];
    const sqlite = sqliteStorage(file, { onError: err => faults.push(err) });
    const autocheckpoint = sqlite.db.prepare('PRAGMA wal_autocheckpoint').get().wal_autocheckpoint;
    assert.ok(autocheckpoint > 1000, 'the committing connection checkpoints only a log far longer than SQLite would');
    const store = createStore({ initial: INITIAL, storage: sqlite.store('main') });
    // Some 3 MB: short of what would have the committing connection checkpoint either way
    for (let i = 0; i < 30; i++) {
      store.patch({ tasks: { [`t${i}`]: { id: `t${i}`, body: 'x'.repeat(100_000) } } });
      store.flush();
    }
    const { statSync } = await import('node:fs');
    for (let waited = 0; statSync(file).size < 2_000_000 && waited < 5000; waited += 50) await new Promise(r => setTimeout(r, 50));
    assert.ok(statSync(file).size >= 2_000_000, `the database file holds what the log did (${statSync(file).size} bytes)`);
    store.dispose();
    sqlite.close();
    assert.deepEqual(faults, [], 'a worker that ran, and was let go of, is no fault');

    const inline = sqliteStorage(file, { checkpoints: 'inline' });
    assert.equal(inline.db.prepare('PRAGMA wal_autocheckpoint').get().wal_autocheckpoint, 1000, "'inline' leaves them to SQLite, as they were");
    inline.close();
    assert.throws(() => sqliteStorage(file, { checkpoints: 'sometimes' }), /checkpoints/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a checkpoint worker that fails is reported, and SQLite checkpoints on the thread that commits again', { skip: !sqliteStorage }, async () => {
  const { sqliteStorageOn } = await import('../src/server/sqlite-shared.js');
  const { DatabaseSync } = await import('node:sqlite');
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  try {
    const file = join(dir, 'failing.sqlite');
    const db = new DatabaseSync(file);
    const errors = [];
    let reported;
    const told = new Promise(resolve => { reported = resolve; });
    // A worker told to open the file with bun:sqlite, under Node: it cannot, as a bundle without sqlite-checkpoint.js could not start it
    const sqlite = sqliteStorageOn({
      db,
      driver: 'bun',
      exec: sql => db.exec(sql),
      prepare: sql => db.prepare(sql),
      transaction: fn => (...args) => {
        db.exec('BEGIN');
        try {
          const result = fn(...args);
          db.exec('COMMIT');
          return result;
        } catch (err) {
          db.exec('ROLLBACK');
          throw err;
        }
      },
      close: () => db.close()
    }, { file, wal: true, onError: err => { errors.push(err); reported(); } });
    await Promise.race([told, new Promise(resolve => setTimeout(resolve, 5000))]);
    // Silent, it showed only as slow answers
    assert.deepEqual(errors.map(e => e.code), ['checkpoint-worker'], 'reported once');
    assert.match(errors[0].message, /back on the thread that commits/);
    assert.equal(db.prepare('PRAGMA wal_autocheckpoint').get().wal_autocheckpoint, 1000, 'and SQLite checkpoints as it did');
    sqlite.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a commit reads its lease, and writes it only when it runs low', { skip: !sqliteStorage }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-'));
  try {
    const ttl = 3_600_000;   // the timer renews every twenty minutes: never within this test
    const sqlite = sqliteStorage(join(dir, 'lease.sqlite'), { lease: { ttl } });
    const store = createStore({ initial: INITIAL, storage: sqlite.store('team-1') });
    const until = () => sqlite.db.prepare('SELECT until FROM leases WHERE store = ?').get('team-1').until;
    const setUntil = ms => sqlite.db.prepare('UPDATE leases SET until = ? WHERE store = ?').run(ms, 'team-1');

    // Renewing it in every commit wrote the lease's page every commit, while the timer keeps it fresh anyway
    const fresh = Date.now() + ttl - 60_000;
    setUntil(fresh);
    store.patch({ tasks: { a: { id: 'a' } } });
    store.flush();
    store.patch({ tasks: { b: { id: 'b' } } });
    store.flush();
    assert.equal(until(), fresh, 'a lease with more than half its time left is only read');

    setUntil(Date.now() + ttl / 4);   // the timer fell behind
    store.patch({ tasks: { c: { id: 'c' } } });
    store.flush();
    assert.ok(until() > Date.now() + ttl - 60_000, 'one running low is renewed with the commit');
    store.dispose();
    sqlite.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
