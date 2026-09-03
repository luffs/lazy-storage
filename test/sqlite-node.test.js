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

test('a database from before 0.3.0 gains the new columns and tables on open', { skip: !sqliteStorage }, async () => {
  const { DatabaseSync } = await import('node:sqlite');
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-node-sqlite-old-'));
  const old = join(dir, 'old.sqlite');
  try {
    const legacy = new DatabaseSync(old);
    legacy.exec(`
      CREATE TABLE leaves (store TEXT NOT NULL, path TEXT NOT NULL, value TEXT, ts_ms INTEGER NOT NULL, ts_count INTEGER NOT NULL,
        ts_replica TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (store, path)) WITHOUT ROWID;
      CREATE TABLE stores (store TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;
      CREATE TABLE replicas (store TEXT NOT NULL, replica TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (store, replica)) WITHOUT ROWID;
      INSERT INTO leaves VALUES ('t', '["tasks","a","id"]', '"a"', 10, 0, 'r1', 0);
      INSERT INTO stores VALUES ('t', 1);
      INSERT INTO replicas VALUES ('t', 'r1', 7);
    `);
    legacy.close();
    const sqlite = sqliteStorage(old);
    const loaded = sqlite.store('t').load();
    assert.deepEqual(loaded.replicas, { r1: { seq: 7, seen: null } });
    assert.equal(loaded.epoch, null);
    assert.deepEqual(loaded.log, []);
    const store = createStore({ initial: INITIAL, storage: sqlite.store('t'), now: () => 5_000_000 });
    assert.deepEqual(store.snapshot().tasks, { a: { id: 'a' } });
    store.patch({ marker: 1 });
    assert.equal(sqlite.store('t').load().epoch, store.epoch);
    store.dispose();
    sqlite.close();
  } finally {
    for (let attempt = 0; attempt < 5; attempt++) {
      try { rmSync(dir, { recursive: true, force: true }); break; } catch { /* retry */ }
    }
  }
});
