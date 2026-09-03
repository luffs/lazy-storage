import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, memoryStorage, jsonFileStorage } from '../src/server/index.js';

const T = (ms, id = 'x') => [ms, 0, id];
const INITIAL = { tasks: {}, order: [], settings: { theme: 'light' } };

test('a fresh adapter loads null and a committed one loads its rows, seqs, and version', () => {
  const storage = memoryStorage();
  assert.equal(storage.load(), null);
  storage.commit({
    upserts: [['["tasks","a","title"]', { value: 'A', ts: T(10) }], ['["tasks","b"]', { ts: T(11), deleted: true }]],
    deletes: [],
    replica: { id: 'r1', seq: 3 },
    version: 1
  });
  storage.commit({ upserts: [], deletes: ['["tasks","b"]'], replica: { id: 'r2', seq: 1 }, version: 2 });
  assert.deepEqual(storage.load(), {
    rows: [['["tasks","a","title"]', { value: 'A', ts: T(10) }]],
    seqs: { r1: 3, r2: 1 },
    version: 2
  });
});

test('the store persists every accepted op as rows and rebuilds state as initial plus rows', () => {
  const storage = memoryStorage();
  const one = createStore({ initial: INITIAL, registers: ['order'], storage });
  one.patch({ tasks: { a: { id: 'a', title: 'Kept', done: false } }, order: ['a'] });
  one.patch({ tasks: { a: { done: true } }, settings: { theme: 'dark' } });
  one.patch({ tasks: { a: { done: null } } });   // a field deletion leaves a tombstone row
  one.dispose();

  const saved = storage.load();
  assert.deepEqual(Object.fromEntries(saved.rows.map(([k, r]) => [k, r.deleted ? 'tombstone' : r.value])), {
    '["tasks","a","id"]': 'a',
    '["tasks","a","title"]': 'Kept',
    '["tasks","a","done"]': 'tombstone',
    '["order"]': ['a'],
    '["settings","theme"]': 'dark'
  });
  assert.equal(saved.seqs.server, 3);
  assert.equal(saved.version, 3);

  const two = createStore({ initial: INITIAL, registers: ['order'], storage });
  assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'Kept' } }, order: ['a'], settings: { theme: 'dark' } });
  // The tombstone still guards: an older write to the deleted field loses
  const r = two.apply({ replicaId: 'late', seq: 1, ts: T(1, 'late'), diff: { tasks: { a: { done: true } } } });
  assert.equal(r.accepted, null);
  assert.equal(two.version, 3, 'nothing accepted, version unchanged');
  assert.equal(storage.load().seqs.late, 1, 'the replica\'s progress is still recorded');
});

test('a skeleton key added to initial appears on load even though no row mentions it', () => {
  const storage = memoryStorage();
  const one = createStore({ initial: { tasks: {} }, storage });
  one.patch({ tasks: { a: { id: 'a' } } });
  one.dispose();
  const two = createStore({ initial: { tasks: {}, labels: {} }, storage });
  assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a' } }, labels: {} });
});

test('compacting tombstones commits the removed rows', () => {
  const storage = memoryStorage();
  const store = createStore({ initial: INITIAL, storage, now: () => 1000 });
  store.patch({ tasks: { a: { id: 'a' } } });
  store.patch({ tasks: { a: null } });
  assert.equal(store.compactTombstones([2000, 0, 'z']), 1);
  assert.deepEqual(storage.load().rows, []);
});

test('the JSON file adapter round-trips through disk, atomically and debounced', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-'));
  try {
    const file = join(dir, 'nested', 'store.json');
    const one = createStore({ initial: INITIAL, registers: ['order'], storage: jsonFileStorage(file, { debounce: 5 }) });
    one.patch({ tasks: { a: { id: 'a', title: 'On disk' } }, order: ['a'] });
    one.patch({ tasks: { b: { id: 'b' } } });
    one.dispose();   // flushes
    const onDisk = JSON.parse(readFileSync(file, 'utf8'));
    assert.equal(onDisk.rows.length, 4);
    assert.equal(onDisk.version, 2);

    const two = createStore({ initial: INITIAL, registers: ['order'], storage: jsonFileStorage(file) });
    assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'On disk' }, b: { id: 'b' } }, order: ['a'], settings: { theme: 'light' } });
    two.dispose();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
