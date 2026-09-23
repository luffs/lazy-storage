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
