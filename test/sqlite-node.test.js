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
    assert.deepEqual(loaded.log.map(e => e.v), [2, 3, 4], 'the log, pruned to three');
    const two = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1'), deltaLog: 3 });
    assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'Kept', done: true }, c: { id: 'c' } }, order: ['a'], settings: { theme: 'dark' } });
    assert.equal(two.version, 4);
    assert.equal(two.stats().log, 3);
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
    // ...and the process it was taken from can no longer write: its store unloads instead
    assert.throws(() => other.patch({ tasks: { late: { id: 'late' } } }), err => err.code === 'unavailable');
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
