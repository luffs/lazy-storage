import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, memoryStorage, jsonFileStorage } from '../src/server/index.js';

const T = (ms, id = 'x') => [ms, 0, id];
const INITIAL = { tasks: {}, order: [], settings: { theme: 'light' } };

test('a fresh adapter loads null and a committed one loads its rows, replicas, and version', () => {
  const storage = memoryStorage();
  assert.equal(storage.load(), null);
  storage.commit({
    upserts: [['["tasks","a","title"]', { value: 'A', ts: T(10) }], ['["tasks","b"]', { ts: T(11), deleted: true }]],
    deletes: [],
    replica: { id: 'r1', seq: 3, seen: 500 },
    version: 1
  });
  storage.commit({ upserts: [], deletes: ['["tasks","b"]'], replica: { id: 'r2', seq: 1, seen: 600 }, version: 2 });
  assert.deepEqual(storage.load(), {
    rows: [['["tasks","a","title"]', { value: 'A', ts: T(10) }]],
    replicas: { r1: { seq: 3, seen: 500 }, r2: { seq: 1, seen: 600 } },
    version: 2,
    epoch: null,
    log: []
  });
  storage.commit({ upserts: [], deletes: [], forgetReplicas: ['r1'], version: 3, epoch: 'e1' });
  assert.deepEqual(storage.load().replicas, { r2: { seq: 1, seen: 600 } });
  assert.equal(storage.load().epoch, 'e1');
});

test('the memory adapter keeps the delta log, pruned to the floor the store names; the JSON file adapter does not', async () => {
  const storage = memoryStorage();
  storage.commit({ upserts: [], deletes: [], version: 1, epoch: 'e', log: { v: 1, diff: { a: 1 } }, logFloor: 1 });
  storage.commit({ upserts: [], deletes: [], version: 2, epoch: 'e', log: { v: 2, diff: { a: 2 } }, logFloor: 1 });
  storage.commit({ upserts: [], deletes: [], version: 3, epoch: 'e', log: { v: 3, diff: { a: 3 } }, logFloor: 2 });
  assert.deepEqual(storage.load().log, [{ v: 2, diff: { a: 2 } }, { v: 3, diff: { a: 3 } }]);

  const store = createStore({ initial: INITIAL, storage: memoryStorage(), deltaLog: 2 });
  for (let i = 0; i < 4; i++) store.patch({ tasks: { [`t${i}`]: { id: `t${i}` } } });
  assert.deepEqual(store.stats().log, 2);

  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-'));
  try {
    const file = join(dir, 'store.json');
    const one = createStore({ initial: INITIAL, storage: jsonFileStorage(file, { debounce: 5 }) });
    one.patch({ tasks: { a: { id: 'a' } } });
    one.dispose();
    assert.equal('log' in JSON.parse(readFileSync(file, 'utf8')), false);
    assert.equal(createStore({ initial: INITIAL, storage: jsonFileStorage(file) }).stats().log, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a store mints an epoch once and keeps it across reopens; a wiped storage gets a new one', () => {
  const storage = memoryStorage();
  const one = createStore({ initial: INITIAL, storage });
  assert.match(one.epoch, /^[0-9a-z]+$/i);
  one.patch({ tasks: { a: { id: 'a' } } });
  one.dispose();
  assert.equal(storage.load().epoch, one.epoch);
  const two = createStore({ initial: INITIAL, storage });
  assert.equal(two.epoch, one.epoch);
  const fresh = createStore({ initial: INITIAL, storage: memoryStorage() });
  assert.notEqual(fresh.epoch, one.epoch);
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
  assert.equal(saved.replicas.server.seq, 3);
  assert.equal(saved.version, 3);

  const two = createStore({ initial: INITIAL, registers: ['order'], storage });
  assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a', title: 'Kept' } }, order: ['a'], settings: { theme: 'dark' } });
  // The tombstone still guards: an older write to the deleted field loses
  const r = two.apply({ replicaId: 'late', seq: 1, ts: T(1, 'late'), diff: { tasks: { a: { done: true } } } });
  assert.equal(r.accepted, null);
  assert.equal(two.version, 3, 'nothing accepted, version unchanged');
  assert.equal(storage.load().replicas.late.seq, 1, 'the replica\'s progress is still recorded');
});

test('a skeleton key added to initial appears on load even though no row mentions it', () => {
  const storage = memoryStorage();
  const one = createStore({ initial: { tasks: {} }, storage });
  one.patch({ tasks: { a: { id: 'a' } } });
  one.dispose();
  const two = createStore({ initial: { tasks: {}, labels: {} }, storage });
  assert.deepEqual(two.snapshot(), { tasks: { a: { id: 'a' } }, labels: {} });
});

test('a container written empty keeps its row beside its children, so a rebuild matches the live state after they are removed', () => {
  const storage = memoryStorage();
  const store = createStore({ initial: { tasks: {} }, storage });
  store.patch({ tasks: { a: { id: 'a', assignees: {} } } });     // an empty container is a leaf row
  store.patch({ tasks: { a: { assignees: { u1: true } } } });    // gains a child; the {} row stays
  store.patch({ tasks: { a: { assignees: { u1: null } } } });    // loses it again (tombstone)
  store.patch({ tasks: { a: { score: 5 } } });                   // a leaf...
  store.patch({ tasks: { a: { score: { high: 9 } } } });         // ...that becomes a container: both rows exist, shallow-first rebuild wins
  const live = store.snapshot();
  assert.deepEqual(live, { tasks: { a: { id: 'a', assignees: {}, score: { high: 9 } } } });
  const rows = storage.load().rows.map(([k, r]) => `${k}${r.deleted ? ' (tombstone)' : ''}`);
  assert.ok(rows.includes('["tasks","a","assignees"]'), 'the empty-container row is kept');
  assert.ok(rows.includes('["tasks","a","assignees","u1"] (tombstone)'));
  store.dispose();

  const reopened = createStore({ initial: { tasks: {} }, storage });
  assert.deepEqual(reopened.snapshot(), live, 'the rebuilt state equals what the server held before the restart');
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
