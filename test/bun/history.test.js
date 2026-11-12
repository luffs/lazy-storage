// history.test.js - Runs under Bun only: a store's history kept on bun:sqlite, as on node:sqlite (test/history.test.js)
import assert from 'node:assert/strict';
import { test } from 'bun:test';
import { createStore } from '../../src/server/index.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';

test('a store on bun:sqlite keeps its history: read back, asked for as it was, put back, and pruned', () => {
  let t = 100 * 86_400_000;
  const sqlite = sqliteStorage();
  const store = createStore({ initial: { tasks: {} }, now: () => t, history: { keep: 10 * 86_400_000 }, storage: sqlite.store('main') });
  store.patch({ tasks: { k: { title: 'Milk' } } });
  t += 1000;
  store.patch({ tasks: { k: { title: 'Oat milk', extra: 1 } } });
  assert.deepEqual(store.history('tasks', 'k').map(e => e.record), [{ title: 'Oat milk', extra: 1 }, { title: 'Milk' }]);
  assert.deepEqual(store.recordAt('tasks', 'k', t - 500), { title: 'Milk' });
  store.revert('tasks', 'k', t - 500);
  assert.deepEqual(store.state.tasks.k, { title: 'Milk' });
  t += 20 * 86_400_000;
  store.patch({ tasks: { k: { title: 'Later' } } });
  assert.deepEqual(store.history('tasks', 'k').map(e => e.record.title), ['Later', 'Milk'], 'the last form before what is kept, and what came after');
  assert.equal(sqlite.db.prepare('SELECT count(*) AS n FROM history').get().n, 2);
  store.dispose();
  sqlite.close();
});
