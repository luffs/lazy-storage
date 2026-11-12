// views.test.js - Runs under Bun only: the records laid out as SQL views on bun:sqlite, as on node:sqlite (test/views.test.js)
import assert from 'node:assert/strict';
import { test } from 'bun:test';
import { createStore, optional, listItem } from '../../src/server/index.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';

test('views lay the records of a bun:sqlite file out by their schemas, and are brought up to date by a call again', () => {
  const schemas = { tasks: listItem({ title: 'string', done: 'boolean?', meta: optional({ color: 'string' }) }), settings: { theme: 'string', '*': 'any' } };
  const sqlite = sqliteStorage();
  const store = createStore({ initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], schemas, storage: sqlite.store('team-1') });
  assert.deepEqual(sqlite.views(schemas, { singletons: ['settings'] }), ['tasks', 'settings']);
  store.patch({ tasks: { a: { title: 'Milk', done: true, meta: { color: 'red' } } }, settings: { theme: 'dark', extra: 1 } });
  store.flush();
  assert.deepEqual(sqlite.db.prepare('SELECT * FROM tasks').all(), [{ store: 'team-1', id: 'a', pos: null, title: 'Milk', done: 1, meta_color: 'red' }]);
  assert.deepEqual(sqlite.db.prepare('SELECT * FROM settings').all(), [{ store: 'team-1', theme: 'dark', data: '{"theme":"dark","extra":1}' }]);
  assert.deepEqual(sqlite.views({ tasks: { title: 'string' } }, { store: 'team-1' }), ['tasks']);
  assert.deepEqual(sqlite.db.prepare('SELECT * FROM tasks').all(), [{ id: 'a', title: 'Milk' }]);
  assert.deepEqual(sqlite.db.prepare("SELECT name FROM sqlite_master WHERE type = 'view'").all(), [{ name: 'tasks' }]);
  assert.throws(() => sqlite.views({ records: {} }), /Give views a prefix/);
  store.dispose();
  sqlite.close();
});
