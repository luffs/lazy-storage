// export.test.js - A store as a document: exported, taken into any adapter, backed up and served at once
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, memoryStorage, jsonFileStorage } from '../src/server/index.js';
import { sqliteStorage } from '../src/server/sqlite-node.js';
import { fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] };

function filled(storage, now) {
  const store = createStore({ ...SHAPE, now, storage });
  store.apply({ replica: 'r1', seq: 1, ts: now(), changes: [{ c: 'tasks', id: 'k', put: { title: 'Milk', note: 'x' } }] }, { user: { id: 'u1' } });
  now.advance(10);
  store.apply({ replica: 'r1', seq: 2, ts: now(), changes: [{ c: 'tasks', id: 'k', path: ['note'], del: true }, { c: 'settings', path: ['theme'], set: 'dark' }] }, { user: { id: 'u1' } });
  return store;
}

test('an exported store is a JSON document; any adapter that takes it serves the same store, refusing what it would', () => {
  const now = fakeTime();
  const source = filled(memoryStorage(), now);
  const doc = JSON.parse(JSON.stringify(source.export()));
  assert.equal(doc.version, 2);
  assert.deepEqual(doc.replicas.r1.seq, 2);
  const dir = mkdtempSync(join(tmpdir(), 'lazy-export-'));
  try {
    for (const storage of [memoryStorage(), jsonFileStorage(join(dir, 'store.json'))]) {
      storage.replace(doc);
      const copy = createStore({ ...SHAPE, now, storage });
      assert.deepEqual(copy.snapshot(), source.snapshot());
      assert.deepEqual(copy.clock('tasks', 'k'), source.clock('tasks', 'k'));
      assert.notEqual(copy.epoch, source.epoch, 'a new epoch: clients get a snapshot');
      // The deletion is still remembered: a write from before it loses
      copy.apply({ replica: 'r2', seq: 1, ts: now() - 5, changes: [{ c: 'tasks', id: 'k', path: ['note'], set: 'old' }] });
      assert.equal(copy.state.tasks.k.note, undefined);
      // And the replica is still its owner's
      assert.throws(() => copy.apply({ replica: 'r1', seq: 3, ts: now(), changes: [] }, { user: { id: 'u2' } }), err => err.code === 'replica-taken');
      copy.dispose();
    }
  } finally {
    try { rmSync(dir, { recursive: true, force: true }); } catch { /* the OS cleans it up */ }
  }
});

test('SQLite takes a document in one transaction, never under a live store, and backs up a file that serves at once', () => {
  const now = fakeTime();
  const dir = mkdtempSync(join(tmpdir(), 'lazy-export-'));
  try {
    const sqlite = sqliteStorage(join(dir, 'stores.sqlite'), { checkpoints: 'inline' });
    const live = filled(sqlite.store('a'), now);
    assert.throws(() => sqlite.store('a').replace(live.export()), err => err.code === 'store-open');
    sqlite.store('b').replace(live.export());
    const b = createStore({ ...SHAPE, now, storage: sqlite.store('b') });
    assert.deepEqual(b.snapshot(), live.snapshot());
    live.flush();
    sqlite.backup(join(dir, 'backup.sqlite'));
    live.dispose();
    b.dispose();
    sqlite.close();
    const backup = sqliteStorage(join(dir, 'backup.sqlite'), { checkpoints: 'inline' });
    const restored = createStore({ ...SHAPE, now, storage: backup.store('a') });
    assert.deepEqual(restored.snapshot(), { tasks: { k: { title: 'Milk' } }, settings: { theme: 'dark' } });
    restored.dispose();
    backup.close();
  } finally {
    try { rmSync(dir, { recursive: true, force: true }); } catch { /* the OS cleans it up */ }
  }
});

test('a JSON file from before 1.0 is refused rather than served empty, and nothing is written over it until replace', () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-json-old-'));
  try {
    const file = join(dir, 'store.json');
    const old = JSON.stringify({ rows: [['["tasks","k","title"]', { v: 'Milk', ts: 5 }]], replicas: {}, version: 7, epoch: 'e0' });
    writeFileSync(file, old);
    const storage = jsonFileStorage(file);
    assert.throws(() => createStore({ ...SHAPE, storage }), err => err.code === 'storage-format');
    assert.throws(() => storage.commit({ upserts: [], deletes: [], version: 8 }), /before 1\.0/);
    storage.flush();
    assert.equal(readFileSync(file, 'utf8'), old, 'the file as it was');
    const now = fakeTime();
    storage.replace(JSON.parse(JSON.stringify(filled(memoryStorage(), now).export())));
    assert.equal(createStore({ ...SHAPE, storage }).state.tasks.k.title, 'Milk', 'replace moves it');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
