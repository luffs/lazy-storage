// sqlite.test.js - Runs under Bun only (bun:sqlite): `bun test/bun/sqlite.test.js`
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore } from '../../src/server/index.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';

const T = (ms, id = 'x') => [ms, 0, id];
const INITIAL = { tasks: {}, order: [], settings: { theme: 'light' } };
let passed = 0;
const test = (name, fn) => { fn(); passed++; console.log('✔', name); };

const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-sqlite-'));
const file = join(dir, 'stores.sqlite');
try {
  test('adapters load null until their first commit, and stores share one file without mixing', () => {
    const sqlite = sqliteStorage(file);
    const a = sqlite.store('a');
    const b = sqlite.store('b');
    assert.equal(a.load(), null);
    a.commit({
      upserts: [['["tasks","x","title"]', { value: 'X', ts: T(10) }], ['["tasks","y"]', { ts: T(11), deleted: true }], ['["order"]', { value: ['x'], ts: T(12) }]],
      deletes: [],
      replica: { id: 'r1', seq: 2 },
      version: 1
    });
    assert.equal(b.load(), null, 'store b is untouched');
    assert.deepEqual(a.load(), {
      rows: [
        ['["order"]', { value: ['x'], ts: T(12) }],
        ['["tasks","x","title"]', { value: 'X', ts: T(10) }],
        ['["tasks","y"]', { ts: T(11), deleted: true }]
      ],
      seqs: { r1: 2 },
      version: 1
    });
    a.commit({ upserts: [], deletes: ['["tasks","y"]'], replica: { id: 'r2', seq: 1 }, version: 2 });
    const loaded = a.load();
    assert.equal(loaded.rows.length, 2);
    assert.deepEqual(loaded.seqs, { r1: 2, r2: 1 });
    assert.equal(loaded.version, 2);
    assert.deepEqual(sqlite.ids(), ['a']);
    sqlite.close();
  });

  test('a store round-trips through SQLite: rows on disk, state rebuilt on reopen, tombstones kept', () => {
    let sqlite = sqliteStorage(file);
    const one = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1') });
    one.patch({ tasks: { a: { id: 'a', title: 'Kept', done: false }, b: { id: 'b', title: 'Gone' } }, order: ['b', 'a'] });
    one.patch({ tasks: { a: { done: true } }, settings: { theme: 'dark' } });
    one.patch({ tasks: { b: null }, order: ['a'] });
    one.dispose();
    const rowCount = sqlite.db.query('SELECT COUNT(*) AS n FROM leaves WHERE store = ?').get('team-1').n;
    assert.equal(rowCount, 6, 'id, title, done, settings.theme, order, and one tombstone');
    sqlite.close();

    sqlite = sqliteStorage(file);
    const two = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1') });
    assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'Kept', done: true } }, order: ['a'], settings: { theme: 'dark' } });
    assert.equal(two.version, 3);
    const late = two.apply({ replicaId: 'late', seq: 1, ts: T(1, 'late'), diff: { tasks: { b: { title: 'ghost' } } } });
    assert.equal(late.accepted, null, 'the tombstone survived the reopen');
    assert.deepEqual(late.correction, { tasks: { b: null } });
    const dup = two.apply({ replicaId: 'server', seq: 1, ts: T(1), diff: { settings: { theme: 'x' } } });
    assert.equal(dup.duplicate, true, 'replica sequence numbers survived too');
    two.dispose();
    assert.deepEqual(sqlite.ids(), ['a', 'team-1']);
    sqlite.remove('a');
    assert.deepEqual(sqlite.ids(), ['team-1']);
    assert.equal(sqlite.store('a').load(), null);
    sqlite.close();
  });

  test('an in-memory database works for tests and throwaway servers', () => {
    const sqlite = sqliteStorage(':memory:');
    const store = createStore({ initial: INITIAL, storage: sqlite.store('tmp') });
    store.patch({ tasks: { a: { id: 'a' } } });
    assert.deepEqual(sqlite.store('tmp').load().rows.map(([k]) => k), ['["tasks","a","id"]']);
    store.dispose();
    sqlite.close();
  });

  console.log(`\n${passed} passed`);
} finally {
  // Windows can hold SQLite's WAL side files briefly after close; the temp
  // dir is disposable either way
  for (let attempt = 0; attempt < 5; attempt++) {
    try { rmSync(dir, { recursive: true, force: true }); break; } catch { await new Promise(r => setTimeout(r, 100)); }
  }
}
