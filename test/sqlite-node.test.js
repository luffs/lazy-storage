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

