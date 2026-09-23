// export.test.js - A store as a document: export it, take it into any adapter, and back up a SQLite file whole
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, createHub, memoryStorage, jsonFileStorage } from '../src/server/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {}, order: [] };
const T = (ms, id) => [ms, 0, id];

let sqliteStorage = null;
try {
  ({ sqliteStorage } = await import('../src/server/sqlite-node.js'));
} catch { /* node:sqlite missing: those tests skip */ }

/** A store with every kind of thing a document must carry */
function busy() {
  const store = createStore({ initial: INITIAL, registers: ['order'], migrations: [() => {}] });
  store.patch({ tasks: { a: { id: 'a', title: 'Milk', tags: ['x'] }, b: { id: 'b', title: 'Gone' }, e: {} }, order: ['a', 'b'] });
  store.patch({ tasks: { b: null } });                                       // a tombstone
  const session = store.session({ send() {}, user: { id: 'ann' } });
  session.receive({ t: 'hello', replicaId: 'r1', ops: [{ replicaId: 'r1', seq: 1, ts: T(Date.now(), 'r1'), diff: { tasks: { a: { done: true } } } }] });
  return store;
}

/** What a store taken from a document must do as the original did */
function assertSame(copy, original, label) {
  assert.deepEqual(copy.snapshot(), original.snapshot(), `${label}: the state`);
  assert.notEqual(copy.epoch, original.epoch, `${label}: a new epoch`);
  assert.equal(copy.version, original.version, `${label}: the version`);
  assert.equal(copy.stats().schema, 1, `${label}: the schema`);
  assert.equal(copy.apply({ replicaId: 'late', seq: 1, ts: T(1, 'late'), diff: { tasks: { b: { title: 'ghost' } } } }).accepted, null, `${label}: the tombstone`);
  assert.equal(copy.apply({ replicaId: 'r1', seq: 1, ts: T(Date.now(), 'r1'), diff: { tasks: { a: { done: false } } } }).duplicate, true, `${label}: replica progress`);
  const thief = [];
  copy.session({ send: m => thief.push(m), user: { id: 'mallory' } }).receive({ t: 'hello', replicaId: 'r1', ops: [] });
  assert.equal(thief[0].code, 'replica-taken', `${label}: the replica's owner`);
}

test('an exported store is a JSON document; any adapter that takes it serves the same store', () => {
  const original = busy();
  const doc = JSON.parse(JSON.stringify(original.export()));
  assert.equal(doc.format, 'lazy-storage/store');
  assert.deepEqual(doc.rows.find(([key]) => key === '["tasks","e"]')[1].value, {}, 'an empty-object leaf stays one');

  const memory = memoryStorage();
  memory.replace(doc);
  const fromMemory = createStore({ initial: INITIAL, registers: ['order'], storage: memory, migrations: [() => {}] });
  assertSame(fromMemory, original, 'memory');
  assert.equal(fromMemory.stats().log, 0, 'no delta log: no client can ask for one under the new epoch');

  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-export-'));
  try {
    const file = jsonFileStorage(join(dir, 'store.json'));
    file.replace(doc);
    const fromFile = createStore({ initial: INITIAL, registers: ['order'], storage: jsonFileStorage(join(dir, 'store.json')), migrations: [() => {}] });
    assertSame(fromFile, original, 'json file');
    fromFile.dispose();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  fromMemory.dispose();
  original.dispose();
});

test('SQLite takes a document in one transaction, never under a live store, and backs up a file that serves at once', { skip: !sqliteStorage }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-export-'));
  try {
    const original = busy();
    const doc = original.export();
    const sqlite = sqliteStorage(join(dir, 'stores.sqlite'));
    sqlite.store('team-1').replace(doc);
    const loaded = createStore({ initial: INITIAL, registers: ['order'], storage: sqlite.store('team-1'), migrations: [() => {}] });
    assertSame(loaded, original, 'sqlite');

    assert.throws(() => sqlite.store('team-1').replace(doc), err => err.code === 'store-open', 'not under a store this process serves');
    const elsewhere = sqliteStorage(join(dir, 'stores.sqlite'));
    assert.throws(() => elsewhere.store('team-1').replace(doc), err => err.code === 'store-locked', 'nor one another process serves');

    loaded.patch({ tasks: { c: { id: 'c', title: 'after the copy' } } });
    const backupFile = join(dir, 'backup.sqlite');
    sqlite.backup(backupFile);                 // while the store is live
    const restored = sqliteStorage(backupFile);
    const fromBackup = createStore({ initial: INITIAL, registers: ['order'], storage: restored.store('team-1'), migrations: [() => {}] });
    assert.deepEqual(fromBackup.snapshot(), loaded.snapshot(), 'the backup holds everything, served at once: no lease came with it');
    assert.notEqual(fromBackup.epoch, loaded.epoch, 'under a new epoch');
    assert.throws(() => sqlite.backup(backupFile), 'a backup does not overwrite a file');

    fromBackup.dispose();
    restored.close();
    loaded.dispose();
    elsewhere.close();
    sqlite.close();
    original.dispose();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a client that saw more than a restored copy holds gets a snapshot, not a delta from a history it never had', async () => {
  let current = createStore({ initial: INITIAL, storage: memoryStorage() });
  const net = createNetwork({ session: ({ send, user }) => createHub(() => current, { send, user }) });
  const client = net.client({ replicaId: 'c', initial: INITIAL });
  current.patch({ tasks: { a: { id: 'a' } } });
  await net.settle();
  const backup = JSON.parse(JSON.stringify(current.export()));

  current.patch({ tasks: { b: { id: 'b' } } });            // after the backup: lost with the restore
  current.patch({ tasks: { c: { id: 'c' } } });
  await net.settle();
  assert.equal(client.state.tasks.c.id, 'c');
  client.link.goOffline();
  await net.settle();
  current.dispose();

  const restored = memoryStorage();
  restored.replace(backup);
  current = createStore({ initial: INITIAL, storage: restored });
  for (const id of ['x', 'y', 'z']) current.patch({ tasks: { [id]: { id } } });  // past the version the client holds
  assert.ok(current.version > 3);
  const answers = [];
  const session = current.session;
  current.session = options => session({ ...options, send: m => { answers.push(m.t); options.send(m); } });
  client.link.goOnline();
  client.connect();
  await net.settle();
  assert.ok(answers.includes('snapshot') && !answers.includes('delta'));
  assert.deepEqual(JSON.parse(JSON.stringify(client.state)), current.snapshot(), 'the client holds what the server holds');
  current.dispose();
});
