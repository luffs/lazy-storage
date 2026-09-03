// sqlite-client.test.js - Runs under Bun only (bun:sqlite): `bun test/bun/sqlite-client.test.js`
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../../src/server/index.js';
import { createClient } from '../../src/client/index.js';
import { sqliteClientStorage } from '../../src/client/sqlite-bun.js';
import { rebuild } from '../../src/core/model.js';
import { createNetwork } from '../helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const snap = client => LazyWatch.snapshot(client.state);
let passed = 0;
const test = async (name, fn) => { await fn(); passed++; console.log('✔', name); };

const dir = mkdtempSync(join(tmpdir(), 'lazy-storage-client-sqlite-'));
const file = join(dir, 'mirror.sqlite');
try {
  await test('a Bun client persists rows and ops in SQLite synchronously and comes back from the file', async () => {
    const store = createStore({ initial: INITIAL, registers: REGISTERS });
    const net = createNetwork(store);
    const link = net.link();
    let storage = sqliteClientStorage(file);
    assert.equal(storage.load(), null);
    const a = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'a' });
    a.connect();
    await net.settle();
    a.collection('tasks').add({ id: 't1', title: 'one', sub: { s1: { id: 's1' } } });
    a.state.order.push('t1');
    await net.settle();
    let saved = storage.load();
    assert.deepEqual(rebuild(INITIAL, saved.rows), snap(a));
    assert.deepEqual(saved.ops, []);
    assert.equal(saved.version, store.version);
    assert.equal(saved.epoch, store.epoch);

    link.goOffline();
    await net.settle();
    delete a.state.tasks.t1;
    a.state.order = [];
    await net.settle();
    a.collection('tasks').add({ id: 't2', title: 'offline' });
    await net.settle();
    saved = storage.load();
    assert.deepEqual(saved.ops.map(op => op.seq), [2, 3]);
    assert.equal(storage.db.query('SELECT COUNT(*) AS n FROM leaves WHERE path >= ? AND path < ?').get('["tasks","t1",', '["tasks","t1",￿').n, 0, 'descendant rows went with the record');
    const before = snap(a);
    assert.deepEqual(rebuild(INITIAL, saved.rows), before);
    a.dispose();
    storage.close();

    storage = sqliteClientStorage(file);
    const b = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, registers: REGISTERS, storage });
    assert.equal(b.restored, true);
    assert.equal(b.replicaId, 'a');
    assert.equal(b.pending, 2);
    assert.deepEqual(snap(b), before);
    link.goOnline();
    b.connect();
    await net.settle();
    assert.equal(b.pending, 0);
    assert.deepEqual(store.snapshot(), snap(b));
    assert.deepEqual(rebuild(INITIAL, storage.load().rows), store.snapshot());
    assert.deepEqual(storage.load().ops, []);
    b.dispose();
    storage.close();
  });

  console.log(`\n${passed} passed`);
} finally {
  for (let attempt = 0; attempt < 5; attempt++) {
    try { rmSync(dir, { recursive: true, force: true }); break; } catch { await new Promise(r => setTimeout(r, 100)); }
  }
}
