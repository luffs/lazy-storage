// schema.test.js - What a record may hold: the schemas a store, and a relay, judge every write by
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, optional, oneOf, listItem, defineSchemas } from '../src/server/index.js';
import { createShape, createSchemas, SchemaError } from '../src/core/index.js';
import { createRelay } from '../src/relay/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {}, notes: {}, settings: { theme: 'light' } };
const SHAPE = { initial: INITIAL, singletons: ['settings'] };
const SCHEMAS = {
  tasks: {
    title: 'string',
    done: 'boolean?',
    estimate: 'integer?',
    tags: optional(['string']),
    meta: optional({ color: oneOf('red', 'green'), votes: optional({ '*': 'number' }) }),
    due: optional(value => (typeof value === 'string' && !Number.isNaN(Date.parse(value))) || 'expected a date')
  },
  settings: { theme: oneOf('light', 'dark'), size: 'number?' }
};

test('a schema of names, fields, lists and checks says what is wrong with a record, and where', () => {
  const schemas = createSchemas(createShape(SHAPE), SCHEMAS);
  const wrong = record => schemas.check('tasks', 'k', record)?.message ?? null;
  assert.equal(wrong({ title: 'Milk' }), null);
  assert.equal(wrong({ title: 'Milk', done: true, estimate: 3, tags: ['a', 'b'], meta: { color: 'red', votes: { u1: 1, u2: 2 } }, due: '2026-11-10' }), null);
  assert.equal(wrong({}), 'tasks/k/title: missing');
  assert.equal(wrong({ title: 5 }), 'tasks/k/title: expected a string');
  assert.equal(wrong({ title: 'Milk', done: 'yes' }), 'tasks/k/done: expected true or false');
  assert.equal(wrong({ title: 'Milk', estimate: 1.5 }), 'tasks/k/estimate: expected a whole number');
  assert.equal(wrong({ title: 'Milk', extra: 1 }), 'tasks/k/extra: not a field of its schema');
  assert.equal(wrong({ title: 'Milk', tags: 'a' }), 'tasks/k/tags: expected a list');
  assert.equal(wrong({ title: 'Milk', tags: ['a', 2] }), 'tasks/k/tags/1: expected a string');
  assert.equal(wrong({ title: 'Milk', meta: {} }), 'tasks/k/meta/color: missing');
  assert.equal(wrong({ title: 'Milk', meta: { color: 'blue' } }), 'tasks/k/meta/color: expected one of "red", "green"');
  assert.equal(wrong({ title: 'Milk', meta: { color: 'red', votes: { u1: 'x' } } }), 'tasks/k/meta/votes/u1: expected a number');
  assert.equal(wrong({ title: 'Milk', due: 'soon' }), 'tasks/k/due: expected a date');
  assert.equal(wrong('Milk'), 'tasks/k: expected an object');
  assert.equal(schemas.check('settings', undefined, { theme: 'blue' }).message, 'settings/theme: expected one of "light", "dark"');
  assert.equal(schemas.check('notes', 'n', { anything: [1] }), null, 'a collection without a schema holds anything');
  const broken = schemas.check('tasks', 'k', { title: 'Milk', tags: [1] });
  assert.ok(broken instanceof SchemaError);
  assert.equal(broken.code, 'invalid');
  assert.deepEqual(broken.path, ['tasks', 'k', 'tags', '0']);
});

test('a check that returns false or throws refuses, with its own message when it has one', () => {
  const schemas = createSchemas(createShape(SHAPE), {
    tasks: (record, { c, id }) => { if (record.title === id) throw new Error(`a ${c} record is not named after its id`); return record.title !== ''; }
  });
  assert.equal(schemas.check('tasks', 'k', { title: 'Milk' }), null);
  assert.equal(schemas.check('tasks', 'k', { title: '' }).message, 'tasks/k: not allowed');
  assert.equal(schemas.check('tasks', 'k', { title: 'k' }).message, 'tasks/k: a tasks record is not named after its id');
});

test("another library's validator is asked whether a value is valid: Standard Schema, safeParse or parse", () => {
  const titled = value => typeof value?.title === 'string';
  const standard = { '~standard': { version: 1, vendor: 'test', validate: value => (titled(value) ? { value } : { issues: [{ message: 'Expected string', path: ['title'] }] }) } };
  const keyed = { '~standard': { version: 1, vendor: 'test', validate: () => ({ issues: [{ message: 'Too short', path: [{ key: 'tags' }, 0] }] }) } };
  const later = { '~standard': { version: 1, vendor: 'test', validate: async value => ({ value }) } };
  // A library's schemas are instances of its classes, as Zod's are
  const library = methods => Object.assign(Object.create({ library: true }), methods);
  const safe = library({ safeParse: value => (titled(value) ? { success: true, data: value } : { success: false, error: { issues: [{ message: 'Required', path: ['title'] }] } }) });
  const throwing = library({ parse: value => { if (!titled(value)) throw Object.assign(new Error('bad'), { issues: [{ message: 'Required', path: ['title'] }] }); return { ...value, added: true }; } });
  const plainThrow = library({ parse: () => { throw new Error('no good'); } });
  const safeThrow = library({ safeParse: () => { throw new Error('refinement broke'); } });
  const check = (schema, record) => createSchemas(createShape(SHAPE), { tasks: schema }).check('tasks', 'k', record)?.message ?? null;
  for (const schema of [standard, safe, throwing]) assert.equal(check(schema, { title: 'Milk' }), null);
  assert.equal(check(standard, {}), 'tasks/k/title: Expected string');
  assert.equal(check(keyed, {}), 'tasks/k/tags/0: Too short');
  assert.match(check(later, { title: 'Milk' }), /asynchronously/);
  assert.equal(check(safe, {}), 'tasks/k/title: Required');
  assert.equal(check(throwing, {}), 'tasks/k/title: Required');
  assert.equal(check(plainThrow, {}), 'tasks/k: no good');
  assert.equal(check(safeThrow, {}), 'tasks/k: refinement broke', 'a safeParse that throws refuses, as a parse that throws does');
  // Inside a schema of fields too
  assert.equal(check({ title: 'string', inner: safe }, { title: 'a', inner: {} }), 'tasks/k/inner/title: Required');
});

test('a plain object is an object of fields, so a record may have fields named parse or safeParse', () => {
  const schemas = createSchemas(createShape(SHAPE), {
    tasks: { parse: value => typeof value === 'string' || 'expected a string', safeParse: 'boolean?', title: 'string' }
  });
  const wrong = record => schemas.check('tasks', 'k', record)?.message ?? null;
  assert.equal(wrong({ parse: 'x', title: 'Milk' }), null);
  assert.equal(wrong({ parse: 'x' }), 'tasks/k/title: missing');
  assert.equal(wrong({ parse: 1, title: 'Milk' }), 'tasks/k/parse: expected a string');
  assert.equal(wrong({ parse: 'x', title: 'Milk', extra: 1 }), 'tasks/k/extra: not a field of its schema');
});

test('what is not a schema is refused when the store is made, as is an initial state that does not fit', () => {
  assert.throws(() => createStore({ ...SHAPE, schemas: { nope: {} } }), /"nope" is not a collection or singleton/);
  assert.throws(() => createStore({ ...SHAPE, schemas: { tasks: { title: 'text' } } }), /"text" at tasks\/title is not a type/);
  assert.throws(() => createStore({ ...SHAPE, schemas: { tasks: { tags: ['string', 'number'] } } }), /a list at tasks\/tags/);
  assert.throws(() => createStore({ ...SHAPE, schemas: { tasks: { title: 5 } } }), /tasks\/title is not a schema/);
  assert.throws(() => createStore({ ...SHAPE, schemas: { tasks: new Date() } }), /tasks is not a schema/);
  assert.throws(() => createStore({ ...SHAPE, schemas: [] }), /schemas must be an object/);
  assert.throws(() => createStore({ ...SHAPE, schemas: { settings: { theme: oneOf('dark') } } }), /initial does not fit schemas: settings\/theme/);
  assert.throws(() => createStore({ initial: { tasks: { seed: { name: 'x' } } }, schemas: { tasks: { title: 'string' } } }), /initial does not fit schemas: tasks\/seed\/name/);
});

test("a client's op that leaves a record outside its schema is refused whole with invalid, and the client takes it back", async () => {
  const store = createStore({ ...SHAPE, schemas: SCHEMAS });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  const b = net.client({ replicaId: 'b', ...SHAPE });
  await net.settle();
  const rejected = [];
  a.on('rejected', e => rejected.push([e.code, e.message]));

  a.state.tasks.k = { title: 'Milk' };
  a.state.notes.n = { anything: [1, { goes: true }] };
  await net.settle();
  assert.deepEqual(store.state.tasks, { k: { title: 'Milk' } });
  assert.deepEqual(rejected, []);

  // One field of a record that is there: the record is judged as the write leaves it
  a.state.tasks.k.done = 'yes';
  await net.settle();
  assert.deepEqual(rejected, [['invalid', 'tasks/k/done: expected true or false']]);
  assert.deepEqual(store.state.tasks.k, { title: 'Milk' });
  assert.equal(a.state.tasks.k.done, undefined, 'taken back');
  assert.equal(a.pending, 0);

  // Refused whole: the valid record made in the same batch does not land either
  rejected.length = 0;
  a.state.tasks.good = { title: 'Eggs' };
  a.state.tasks.bad = { name: 'no title' };
  await net.settle();
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0][0], 'invalid');
  assert.deepEqual(Object.keys(store.state.tasks), ['k']);

  // A field the schema needs cannot be deleted; the record can
  rejected.length = 0;
  delete a.state.tasks.k.title;
  await net.settle();
  assert.deepEqual(rejected, [['invalid', 'tasks/k/title: missing']]);
  delete a.state.tasks.k;
  await net.settle();
  assert.deepEqual(store.state.tasks, {});

  // A singleton
  a.state.settings.theme = 'blue';
  await net.settle();
  assert.equal(store.state.settings.theme, 'light');
  a.state.settings.theme = 'dark';
  await net.settle();
  assert.equal(b.state.settings.theme, 'dark');
});

test('a list writes an id and a position on its records: listItem is the schema that has them, for a collection or a list inside a record', async () => {
  const initial = { tasks: {}, boards: {} };
  const strict = createStore({ initial, schemas: { tasks: { title: 'string' } } });
  const strictNet = createNetwork(strict);
  const refused = strictNet.client({ replicaId: 'a', initial });
  await strictNet.settle();
  const rejected = [];
  refused.on('rejected', e => rejected.push(e.message));
  const lost = refused.list('tasks').add({ title: 'Milk' });
  await strictNet.settle();
  assert.deepEqual(rejected, [`tasks/${lost}/id: not a field of its schema`], 'a schema of the fields alone refuses what a list adds');

  const schemas = defineSchemas({
    tasks: listItem({ title: 'string' }),
    boards: { name: 'string', cards: optional({ '*': listItem({ text: 'string' }, { position: 'at' }) }) }
  });
  const store = createStore({ initial, schemas });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial });
  await net.settle();
  a.on('rejected', e => rejected.push(e.message));
  rejected.length = 0;
  const tasks = a.list('tasks');
  const milk = tasks.add({ title: 'Milk' });
  const eggs = tasks.add({ title: 'Eggs' });
  tasks.move(eggs, { at: 0 });
  a.state.tasks.plain = { title: 'written without the list' };
  a.state.boards.b = { name: 'Board', cards: {} };
  await net.settle();
  const card = a.list(['boards', 'b', 'cards'], { position: 'at' }).add({ text: 'A card' });
  await net.settle();
  assert.deepEqual(rejected, []);
  assert.deepEqual(Object.keys(store.state.tasks).sort(), [eggs, milk, 'plain'].sort());
  assert.equal(typeof store.state.tasks[milk].pos, 'string');
  assert.equal(store.state.boards.b.cards[card].text, 'A card');
  a.state.tasks[milk].pos = 'not a key!';
  await net.settle();
  assert.deepEqual(rejected, [`tasks/${milk}/pos: expected a position key`]);
  assert.throws(() => listItem('title'), /listItem takes the fields/);
});

test('two edits that each fit, made apart, are judged by what they make together', async () => {
  const store = createStore({ ...SHAPE, schemas: { tasks: record => !(record.done && record.blocked) || 'a blocked task is not done' } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  const b = net.client({ replicaId: 'b', ...SHAPE });
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  const rejected = [];
  b.on('rejected', e => rejected.push(e.message));
  b.link.goOffline();
  await net.settle();
  a.state.tasks.k.done = true;
  b.state.tasks.k.blocked = true;
  await net.settle();
  b.link.goOnline(); b.connect();
  await net.settle();
  assert.deepEqual(rejected, ['tasks/k: a blocked task is not done']);
  assert.deepEqual(store.state.tasks.k, { title: 'Milk', done: true });
  assert.deepEqual({ ...b.state.tasks.k }, { title: 'Milk', done: true });
});

test('a schema is judged before the rules, which may count on the record it let through', () => {
  const seen = [];
  const store = createStore({
    ...SHAPE,
    schemas: { tasks: { title: 'string', owner: 'string' } },
    rules: { tasks: (user, before, after) => { seen.push(after); return after === null || after.owner.toUpperCase() === user.id; } }
  });
  const op = (seq, put) => ({ replica: 'r', seq, ts: Date.now(), changes: [{ c: 'tasks', id: `t${seq}`, put }] });
  assert.throws(() => store.apply(op(1, { title: 'x' }), { user: { id: 'U1' } }), err => err.code === 'invalid' && /owner: missing/.test(err.message));
  assert.deepEqual(seen, [], 'the rule never saw the record without an owner');
  assert.throws(() => store.apply(op(2, { title: 'x', owner: 'u2' }), { user: { id: 'U1' } }), err => err.code === 'forbidden');
  store.apply(op(3, { title: 'x', owner: 'u1' }), { user: { id: 'U1' } });
  assert.deepEqual(Object.keys(store.state.tasks), ['t3']);
});

test("the server's own write that does not fit throws and leaves nothing behind; a migration's too", () => {
  const store = createStore({ ...SHAPE, schemas: SCHEMAS });
  const heard = [];
  store.on(diff => heard.push(diff));
  store.patch({ tasks: { k: { title: 'Milk' } } });
  const version = store.version;
  assert.throws(() => store.patch({ tasks: { k: { done: 1 }, other: { title: 'Eggs' } } }), err => err instanceof TypeError && err.message === 'tasks/k/done: expected true or false');
  assert.throws(() => store.patch({ tasks: { made: { done: true } } }), /tasks\/made\/title: missing/);
  assert.throws(() => store.del('tasks', 'k', ['title']), /tasks\/k\/title: missing/);
  assert.throws(() => store.patch({ settings: { theme: 'blue' } }), /settings\/theme/);
  assert.deepEqual(store.state.tasks, { k: { title: 'Milk' } }, 'the record that fit, written in the same patch, is put back too');
  assert.equal(store.state.settings.theme, 'light');
  assert.equal(store.version, version);
  store.patch({ tasks: { k: { done: true } }, notes: { n: { free: 1 } } });
  store.del('tasks', 'k');
  assert.deepEqual(store.state.tasks, {});
  assert.throws(
    () => createStore({ ...SHAPE, schemas: SCHEMAS, migrations: [() => ({ tasks: { m: { name: 'old form' } } })], storage: { load: () => ({ records: [], replicas: {}, version: 0, epoch: 'e', schema: 0, log: [] }), commit() {} } }),
    /Migration 0 failed: tasks\/m\/name: not a field of its schema/
  );
});

test('a relay given the schemas refuses while the server is away what the server would', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, schemas: SCHEMAS, now });
  const central = createNetwork(store, { vouch: credential => credential });
  const uplink = central.link({ user: { id: 'relay' } });
  const relay = createRelay({ shapeOf: () => ({ ...SHAPE, schemas: SCHEMAS }), link: uplink.factory, grace: 0, reconnect: false, keepalive: false, now });
  const site = createNetwork(relay);
  const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); if (central.pending === 0 && site.pending === 0) return; } };
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  await settle();
  uplink.goOffline();
  await settle();
  const rejected = [];
  a.on('rejected', e => rejected.push([e.code, e.message]));
  a.state.tasks.k = { title: 'Milk' };
  await settle();
  a.state.tasks.k.estimate = 'long';
  await settle();
  assert.deepEqual(rejected, [['invalid', 'tasks/k/estimate: expected a whole number']]);
  assert.deepEqual(relay.copy('main').tasks, { k: { title: 'Milk' } });
  uplink.goOnline(); relay.upstreamUp();
  await settle();
  assert.deepEqual(store.state.tasks, { k: { title: 'Milk' } });
  relay.close();
});
