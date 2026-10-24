// core.test.js - The pieces the server, relays and clients share, on their own:
// the rule in its modes, the shape's checks, the compiler, rules, the clock
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import {
  createShape, createDoc, docFromRecords, compile, createClock, parsePathKey, pathKey, deleteAt, setAt, valueAt
} from '../src/core/index.js';
import { createRules, RuleError } from '../src/core/rules.js';

const shape = createShape({
  initial: { tasks: {}, subtasks: {}, settings: {} },
  singletons: ['settings'],
  registers: ['tasks/*/geo'],
  parents: { subtasks: { taskId: 'tasks' } }
});

test('the server\'s own write makes what is missing on the way; a judged one loses as missing', () => {
  const doc = createDoc(shape);
  const judged = doc.apply({ c: 'tasks', id: 'a', path: ['meta', 'color'], set: 'red' }, 10, 'judge');
  assert.deepEqual(judged.applied, []);
  assert.equal(judged.lost[0].why, 'missing');
  const made = doc.apply({ c: 'tasks', id: 'a', path: ['meta', 'color'], set: 'red' }, 20, 'authority');
  assert.deepEqual(made.applied.at(-1), { c: 'tasks', id: 'a', path: ['meta', 'color'], set: 'red' }, 'what was made, then the write');
  assert.deepEqual(doc.get('tasks', 'a'), { meta: { color: 'red' } });
  const clock = doc.clock('tasks', 'a');
  assert.equal(clock.live['[]'], 20, 'the record made at the write\'s time');
  assert.equal(clock.live['["meta"]'], 20, 'and the object on the way');
  assert.equal(doc.newest('tasks', 'a'), 20);
  doc.apply({ c: 'settings', path: ['a', 'b'], set: 1 }, 30, 'authority');
  assert.deepEqual(doc.get('settings'), { a: { b: 1 } }, 'a singleton too');
});

test('a put on an object merges into it path by path, and one on a leaf replaces it', () => {
  const doc = createDoc(shape);
  doc.apply({ c: 'tasks', id: 'a', put: { meta: { color: 'red' }, title: 'x' } }, 10, 'judge');
  doc.apply({ c: 'tasks', id: 'a', path: ['meta'], put: { size: 2 } }, 20, 'judge');
  assert.deepEqual(doc.get('tasks', 'a').meta, { color: 'red', size: 2 });
  doc.apply({ c: 'tasks', id: 'a', path: ['title'], put: { nested: true } }, 30, 'judge');
  assert.deepEqual(doc.get('tasks', 'a').title, { nested: true });
});

test('records come back from storage by kind, and a stranger row is skipped', () => {
  const doc = docFromRecords(shape, [
    ['tasks', 'a', { title: 'x' }, { live: { '[]': 1, '["title"]': 1 }, gone: {} }],
    ['settings', null, { theme: 'dark' }, { live: { '[]': 1 }, gone: {} }],
    ['nope', 'z', { what: 1 }, { live: {}, gone: {} }],
    ['tasks', null, { orphan: true }, { live: {}, gone: {} }]
  ]);
  assert.deepEqual(doc.state.tasks, { a: { title: 'x' } });
  assert.deepEqual(doc.state.settings, { theme: 'dark' });
  assert.equal(doc.state.nope, undefined);
});

test('a collection whose initial holds something that is not a record says to make it a singleton', () => {
  assert.throws(() => createShape({ initial: { prefs: { theme: 'light' } }, singletons: [] }), /list "prefs" in singletons/);
  assert.doesNotThrow(() => createShape({ initial: { prefs: { theme: 'light' } }, singletons: ['prefs'] }));
  assert.doesNotThrow(() => createShape({ initial: { events: { a: 'x' } }, registers: ['events/*'] }), 'a register may hold any value');
});

test('a field written and deleted within one batch compiles to a deletion only where it was there before', async () => {
  const state = new LazyWatch({ tasks: { a: { title: 'x' } }, subtasks: {}, settings: {} }, { inverse: true });
  const batches = [];
  LazyWatch.on(state, (diff, inverse) => batches.push(compile(shape, diff, inverse, LazyWatch.snapshot(state))));
  state.tasks.a.temp = 1;
  delete state.tasks.a.temp;
  state.tasks.a.title = 'y';
  await new Promise(resolve => setImmediate(resolve));
  const changes = batches.flat();
  assert.ok(!changes.some(ch => ch.path?.[0] === 'temp' && !ch.del), JSON.stringify(changes));
  assert.ok(changes.some(ch => ch.path?.[0] === 'title' && ch.set === 'y'));
});

test('a rule that throws refuses with its message, and one that says false with the record\'s name', () => {
  const doc = createDoc(shape);
  doc.apply({ c: 'tasks', id: 'a', put: { title: 'x' } }, 1, 'authority');
  const judge = createRules(shape, {
    tasks: (user, before, after) => { if (after?.title === 'secret') throw new Error('no secrets'); return user === 'ann'; },
    settings: { write: false }
  });
  const run = (list, user, ts = 5) => judge(doc, list, user, 'r', {}, () => list.map(ch => doc.apply(ch, ts, 'judge')));
  assert.throws(() => run([{ c: 'tasks', id: 'a', path: ['title'], set: 'secret' }], 'ann'), err => err instanceof RuleError && err.message === 'no secrets');
  assert.deepEqual(doc.get('tasks', 'a'), { title: 'x' }, 'a refusal puts back what the trial wrote');
  assert.equal(doc.clock('tasks', 'a').live['["title"]'], 1, 'and its clock');
  assert.throws(() => run([{ c: 'tasks', id: 'a', path: ['title'], set: 'y' }], 'bob'), /Not allowed to write tasks\/a/);
  assert.throws(() => run([{ c: 'settings', path: ['x'], set: 1 }], 'ann'), /settings/);
  assert.doesNotThrow(() => run([{ c: 'tasks', id: 'a', path: ['title'], set: 'y' }], 'ann'));
  assert.equal(doc.get('tasks', 'a').title, 'y');
  assert.throws(() => createRules(shape, { tasks: 'yes' }), TypeError);
  assert.throws(() => createRules(shape, { nope: true }), TypeError);
});

test('the clock adopts only a server time that is one, and stamps after what it has seen', () => {
  let wall = 1000;
  const clock = createClock({ now: () => wall });
  clock.adopt('soon');
  clock.adopt(undefined);
  assert.equal(clock.offset, 0);
  clock.adopt(5000);
  assert.equal(clock.offset, 4000);
  assert.equal(clock.stamp(), 5000);
  clock.observe(9000);
  assert.equal(clock.stamp(), 9001, 'after the highest it has seen');
  wall = 2000;
  assert.equal(clock.time, 6000);
});

test('path helpers: keys parse back, a deletion leaves the ancestors, and nothing walks into the prototype', () => {
  const path = ['a', 'b/c', '0'];
  assert.deepEqual(parsePathKey(pathKey(path)), path);
  const target = { a: { b: { c: 1, d: 2 } } };
  deleteAt(target, ['a', 'b', 'c']);
  assert.deepEqual(target, { a: { b: { d: 2 } } });
  deleteAt(target, ['a', 'nope', 'c']);
  deleteAt({ list: [1, 2] }, ['list', '0']);
  assert.deepEqual(target, { a: { b: { d: 2 } } });
  assert.equal(valueAt(target, ['a', 'b', 'd']), 2);
  assert.equal(valueAt(target, ['__proto__']), undefined);
  assert.throws(() => setAt({}, ['constructor', 'x'], 1), TypeError);
});

test('an object written on a singleton that holds a plain value replaces it, as the server and as a client', () => {
  const plainShape = createShape({ initial: { build: '', tasks: {} } });
  const doc = createDoc(plainShape);
  doc.apply({ c: 'build', set: 'b-1' }, 10, 'authority');
  doc.apply({ c: 'build', put: { id: 'b-2', at: 1 } }, 20, 'authority');
  assert.deepEqual(doc.get('build'), { id: 'b-2', at: 1 });
  doc.apply({ c: 'build', set: 'b-3' }, 30, 'judge');
  const judged = doc.apply({ c: 'build', put: { id: 'b-4' } }, 40, 'judge');
  assert.deepEqual(judged.lost, []);
  assert.deepEqual(doc.get('build'), { id: 'b-4' });
  const older = doc.apply({ c: 'build', put: { id: 'late' } }, 35, 'judge');
  assert.equal(older.lost[0].why, 'stale', 'an older one loses: the object was made after it');
  assert.deepEqual(doc.get('build'), { id: 'b-4' });
});

test('a rule judges what the merge makes of an op, not what it asked for', () => {
  // Done only by the assignee: an op whose time lets the status land and the assignee lose would leave a task no rule allowed
  const doc = createDoc(shape);
  doc.apply({ c: 'tasks', id: 't', put: { status: 'open' } }, 1000, 'authority');
  doc.apply({ c: 'tasks', id: 't', path: ['assignee'], set: 'bob' }, 2000, 'authority');
  const judge = createRules(shape, { tasks: (user, before, after) => after?.status !== 'done' || after.assignee === user });
  const list = [{ c: 'tasks', id: 't', path: ['assignee'], set: 'mallory' }, { c: 'tasks', id: 't', path: ['status'], set: 'done' }];
  assert.throws(() => judge(doc, list, 'mallory', 'm', {}, () => list.map(ch => doc.apply(ch, 1500, 'judge'))), /Not allowed to write tasks\/t/);
  assert.deepEqual(doc.get('tasks', 't'), { status: 'open', assignee: 'bob' }, 'nothing of it stays');
  assert.equal(doc.clock('tasks', 't').live['["status"]'], 1000);
});

test('a trial that throws puts back what it did, a deleted record and its children included', () => {
  const doc = createDoc(shape);
  doc.apply({ c: 'tasks', id: 'p', put: { title: 'x' } }, 1, 'authority');
  doc.apply({ c: 'subtasks', id: 's', put: { taskId: 'p' } }, 1, 'authority');
  assert.throws(() => doc.trial(() => {
    doc.apply({ c: 'tasks', id: 'p', del: true }, 5, 'judge');
    throw new Error('boom');
  }), /boom/);
  assert.deepEqual(doc.get('tasks', 'p'), { title: 'x' });
  assert.deepEqual(doc.get('subtasks', 's'), { taskId: 'p' });
  assert.deepEqual(doc.childrenOf('tasks', 'p'), [['subtasks', 's']], 'and the index of children');
});
