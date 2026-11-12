// views.test.js - The records of a SQLite file laid out by their schemas, as SQL views, for what else reads it
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, optional, oneOf, listItem } from '../src/server/index.js';
import { fieldsOf } from '../src/core/index.js';
import { sqliteStorage } from '../src/server/sqlite-node.js';

const INITIAL = { tasks: {}, notes: {}, settings: { theme: 'light' } };
const SCHEMAS = {
  tasks: listItem({
    title: 'string',
    done: 'boolean?',
    estimate: 'number?',
    tags: optional(['string']),
    meta: optional({ color: oneOf('red', 'green'), size: 'integer?', inner: optional({ deep: 'string?' }) }),
    votes: optional({ '*': 'number' })
  }),
  settings: { theme: oneOf('light', 'dark'), '*': 'any' }
};
const rows = (sqlite, sql, ...args) => sqlite.db.prepare(sql).all(...args).map(row => ({ ...row }));

function served(id, sqlite, options = {}) {
  return createStore({ initial: INITIAL, singletons: ['settings'], schemas: SCHEMAS, storage: sqlite.store(id), ...options });
}

test('a schema of fields names its fields by path, an object of named fields followed into its own', () => {
  assert.deepEqual(fieldsOf(SCHEMAS.tasks), {
    fields: [['id'], ['pos'], ['title'], ['done'], ['estimate'], ['tags'], ['meta', 'color'], ['meta', 'size'], ['meta', 'inner', 'deep'], ['votes']],
    open: false
  });
  assert.deepEqual(fieldsOf(SCHEMAS.settings), { fields: [['theme']], open: true });
  assert.deepEqual(fieldsOf(optional({ a: 'string', empty: {} })), { fields: [['a'], ['empty']], open: false });
  for (const schema of ['string', () => true, ['string'], { parse: v => v }, { safeParse: v => v }, { '~standard': { validate: v => v } }]) assert.equal(fieldsOf(schema), null);
});

test('views lay a collection out as a row a record and a column a field, and a singleton as one row a store', () => {
  const sqlite = sqliteStorage();
  assert.deepEqual(sqlite.views(SCHEMAS, { singletons: ['settings'] }), ['tasks', 'settings']);
  const one = served('team-1', sqlite);
  const two = served('team-2', sqlite);
  one.patch({
    tasks: { a: { title: 'Milk', done: true, estimate: 1.5, tags: ['x', 'y'], meta: { color: 'red', size: 2, inner: { deep: 'd' } }, votes: { u1: 1 } }, b: { title: 'Eggs', done: false } },
    notes: { n: { free: 'no schema, no view' } },
    settings: { theme: 'dark', extra: [1] }
  });
  two.patch({ tasks: { a: { title: "it's \"quoted\"" } } });
  one.flush();
  two.flush();

  assert.deepEqual(rows(sqlite, 'SELECT * FROM tasks ORDER BY store, id'), [
    { store: 'team-1', id: 'a', pos: null, title: 'Milk', done: 1, estimate: 1.5, tags: '["x","y"]', meta_color: 'red', meta_size: 2, meta_inner_deep: 'd', votes: '{"u1":1}' },
    { store: 'team-1', id: 'b', pos: null, title: 'Eggs', done: 0, estimate: null, tags: null, meta_color: null, meta_size: null, meta_inner_deep: null, votes: null },
    { store: 'team-2', id: 'a', pos: null, title: "it's \"quoted\"", done: null, estimate: null, tags: null, meta_color: null, meta_size: null, meta_inner_deep: null, votes: null }
  ]);
  // A schema that lets in fields it does not name keeps the record whole beside them. A singleton nobody has
  // written yet is its initial value, which lives in the code: it has no row, and so none in its view
  assert.deepEqual(rows(sqlite, 'SELECT * FROM settings ORDER BY store'), [
    { store: 'team-1', theme: 'dark', data: '{"theme":"dark","extra":[1]}' }
  ]);
  // What SQL is for
  assert.deepEqual(rows(sqlite, 'SELECT store, count(*) AS open FROM tasks WHERE done IS NOT 1 GROUP BY store ORDER BY store'), [{ store: 'team-1', open: 1 }, { store: 'team-2', open: 1 }]);

  // A view stores nothing: it is the rows as they are now
  one.patch({ tasks: { b: { done: true } } });
  one.del('tasks', 'a');
  one.flush();
  assert.deepEqual(rows(sqlite, "SELECT id, done FROM tasks WHERE store = 'team-1'"), [{ id: 'b', done: 1 }]);
  one.dispose();
  two.dispose();
  sqlite.close();
});

test('views of one store have no store column; a prefix names them apart, and a call again brings them up to date', () => {
  const sqlite = sqliteStorage();
  const store = served('team-1', sqlite);
  const elsewhere = served('team-2', sqlite);
  elsewhere.patch({ tasks: { other: { title: 'Elsewhere' } } });
  elsewhere.flush();
  store.patch({ tasks: { a: { title: 'Milk' } }, settings: { theme: 'dark' } });
  store.flush();
  assert.deepEqual(sqlite.views(SCHEMAS, { singletons: ['settings'], store: 'team-1', prefix: 'one_' }), ['one_tasks', 'one_settings']);
  assert.deepEqual(rows(sqlite, 'SELECT id, title FROM one_tasks'), [{ id: 'a', title: 'Milk' }]);
  assert.deepEqual(rows(sqlite, 'SELECT * FROM one_settings'), [{ theme: 'dark', data: '{"theme":"dark"}' }]);

  // Other views beside them, under a prefix of their own
  sqlite.views({ tasks: SCHEMAS.tasks }, { prefix: 'all_' });
  const viewNames = () => rows(sqlite, "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name").map(r => r.name);
  assert.deepEqual(viewNames(), ['all_tasks', 'one_settings', 'one_tasks']);

  // The schemas changed: a field gone, a field new, a collection without a schema now
  assert.deepEqual(sqlite.views({ tasks: { title: 'string', owner: 'string?' } }, { store: 'team-1', prefix: 'one_' }), ['one_tasks']);
  assert.deepEqual(viewNames(), ['all_tasks', 'one_tasks'], 'the view of what has no schema any more is dropped, and no other prefix is touched');
  assert.deepEqual(rows(sqlite, 'SELECT * FROM one_tasks'), [{ id: 'a', title: 'Milk', owner: null }]);
  store.dispose();
  elsewhere.dispose();
  sqlite.close();
});

test('a schema that names no fields gives the record whole; names that would clash are told apart', () => {
  const sqlite = sqliteStorage();
  const store = createStore({ initial: { events: {}, odd: {}, flags: { on: true } }, singletons: ['flags'], storage: sqlite.store('s') });
  store.patch({ flags: { on: false }, events: { e: { at: 5 } }, odd: { o: { store: 'mine', id: 'inner', data: 'd', a: { b: 1 }, a_b: 2, 'say "hi"': 3, "it's": 4 } } });
  store.flush();
  sqlite.views({
    events: record => typeof record.at === 'number',
    odd: { store: 'string', id: 'string', data: 'string', a: { b: 'number' }, a_b: 'number', 'say "hi"': 'number', "it's": 'number', '*': 'any' },
    flags: { safeParse: value => ({ success: true, data: value }) }
  }, { singletons: ['flags'], prefix: 'v_' });
  assert.deepEqual(rows(sqlite, 'SELECT * FROM v_events'), [{ store: 's', id: 'e', data: '{"at":5}' }]);
  assert.deepEqual(rows(sqlite, 'SELECT * FROM v_flags'), [{ store: 's', data: '{"on":false}' }]);
  const [odd] = rows(sqlite, 'SELECT * FROM v_odd');
  assert.deepEqual(Object.keys(odd), ['store', 'id', 'store_', 'data', 'a_b', 'a_b_', "it's", 'data_'], "the record's own id is the row's, and a key with a quote in it has no column");
  assert.deepEqual([odd.store, odd.id, odd.store_, odd.data, odd.a_b, odd.a_b_, odd["it's"]], ['s', 'o', 'mine', 'd', 1, 2, 4]);
  assert.equal(JSON.parse(odd.data_)['say "hi"'], 3, 'it is in the record whole');
  store.dispose();
  sqlite.close();
});

test('views refuse a name the file has already, and what is not schemas, a prefix or a store id', () => {
  const sqlite = sqliteStorage();
  sqlite.db.exec('CREATE TABLE tasks (mine TEXT)');
  assert.throws(() => sqlite.views({ tasks: SCHEMAS.tasks }), /"tasks" cannot be made: the file has a table of that name already\. Give views a prefix/);
  assert.throws(() => sqlite.views({ records: { a: 'string' } }), /"records" cannot be made/);
  assert.throws(() => sqlite.views({ notes: {}, tasks: SCHEMAS.tasks }), /"tasks" cannot be made/);
  assert.deepEqual(rows(sqlite, "SELECT name FROM sqlite_master WHERE type = 'view'"), [], 'all of them or none: the view before the one refused is not left behind');
  assert.deepEqual(sqlite.views({ tasks: SCHEMAS.tasks }, { prefix: 'v_' }), ['v_tasks']);
  assert.throws(() => sqlite.views(null), /views takes the schemas/);
  assert.throws(() => sqlite.views([]), /views takes the schemas/);
  assert.throws(() => sqlite.views({}, { prefix: 'a b' }), /prefix is letters, digits and underscores/);
  assert.throws(() => sqlite.views({}, { store: '' }), /store id/);
  sqlite.close();
});
