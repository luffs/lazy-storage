// history.test.js - A store that keeps its history: every record as each write left it, when and by whom,
// read back, asked for as it was, and put back
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { sqliteStorage } from '../src/server/sqlite-node.js';
import { createNetwork, fakeTime } from './helpers.js';

const DAY = 86_400_000;
const SHAPE = {
  initial: { tasks: {}, subtasks: {}, notes: {}, settings: { theme: 'light' } },
  singletons: ['settings'],
  registers: ['tasks/*/geo'],
  parents: { subtasks: { taskId: 'tasks' }, notes: { subId: 'subtasks' } }
};

// The same on each storage that keeps history
const storages = { memory: () => memoryStorage(), sqlite: () => sqliteStorage().store('main') };

for (const [kind, makeStorage] of Object.entries(storages)) {
  test(`${kind}: a record's history is every write to it, newest first, with when, who and the record as it was left`, async () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: true, storage: makeStorage() });
    const net = createNetwork(store);
    const a = net.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
    const b = net.client({ replicaId: 'b', ...SHAPE, now }, { user: { id: 'bob' } });
    await net.settle();
    a.state.tasks.k = { title: 'Milk', meta: { color: 'red' } };
    await net.settle();
    now.advance(1000);
    b.state.tasks.k.title = 'Oat milk';
    b.state.tasks.other = { title: 'Eggs' };
    await net.settle();
    now.advance(1000);
    store.patch({ tasks: { k: { meta: { color: 'green' } } } });
    now.advance(1000);
    delete a.state.tasks.k;
    await net.settle();

    const history = store.history('tasks', 'k');
    assert.deepEqual(history.map(e => [e.replicaId, e.user, e.record]), [
      ['a', 'alice', undefined],
      ['server', undefined, { title: 'Oat milk', meta: { color: 'green' } }],
      ['b', 'bob', { title: 'Oat milk', meta: { color: 'red' } }],
      ['a', 'alice', { title: 'Milk', meta: { color: 'red' } }]
    ]);
    assert.deepEqual(history.map(e => Math.floor(e.ts / 1000)), [1003, 1002, 1001, 1000], 'each at the time of its write, on the store\'s clock');
    assert.ok(history.every((e, i) => i === 0 || e.v < history[i - 1].v), 'newest first');
    assert.deepEqual(history[2].changes, [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }], 'the changes to this record alone, not the other the op wrote');
    assert.deepEqual(history[0].changes, [{ c: 'tasks', id: 'k', del: true }]);

    // A page at a time, and the writes to one path
    assert.deepEqual(store.history('tasks', 'k', { limit: 2 }).map(e => e.v), history.slice(0, 2).map(e => e.v));
    assert.deepEqual(store.history('tasks', 'k', { limit: 2, before: history[1].v }).map(e => e.v), history.slice(2).map(e => e.v));
    assert.deepEqual(store.history('tasks', 'k', { path: ['meta', 'color'] }).map(e => e.replicaId), ['a', 'server', 'a'], 'the record made and deleted touch every path in it');
    assert.deepEqual(store.history('tasks', 'k', { path: ['title'] }).map(e => e.replicaId), ['a', 'b', 'a']);
    assert.deepEqual(store.history('tasks', 'never'), []);

    // As it was: by time, by version, and now
    assert.deepEqual(store.recordAt('tasks', 'k', 1_000_500), { title: 'Milk', meta: { color: 'red' } });
    assert.deepEqual(store.recordAt('tasks', 'k', new Date(1_001_500)), { title: 'Oat milk', meta: { color: 'red' } });
    assert.deepEqual(store.recordAt('tasks', 'k', { ts: 1_002_500 }), { title: 'Oat milk', meta: { color: 'green' } });
    assert.deepEqual(store.recordAt('tasks', 'k', { v: history[3].v }), { title: 'Milk', meta: { color: 'red' } });
    assert.equal(store.recordAt('tasks', 'k'), undefined, 'deleted now');
    assert.equal(store.recordAt('tasks', 'k', 999_000), undefined, 'not there yet');
    store.dispose();
  });

  test(`${kind}: revert puts a record back as it was: fields written, fields gained since deleted, a deleted record made again`, () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: true, storage: makeStorage() });
    store.patch({ tasks: { k: { title: 'Milk', meta: { color: 'red', size: 1 }, geo: { x: 1, y: 2 }, tags: ['a'] } }, subtasks: { s: { taskId: 'k', n: 1 } } });
    now.advance(1000);
    store.patch({ tasks: { k: { title: 'Oat milk', meta: { size: null, extra: true }, geo: { x: 5 }, tags: ['a', 'b'], added: 1 } }, settings: { theme: 'dark', size: 2 } });
    assert.deepEqual(store.state.tasks.k, { title: 'Oat milk', meta: { color: 'red', extra: true }, geo: { x: 5 }, tags: ['a', 'b'], added: 1 });

    const landed = store.revert('tasks', 'k', 1_000_500);
    assert.deepEqual(store.state.tasks.k, { title: 'Milk', meta: { color: 'red', size: 1 }, geo: { x: 1, y: 2 }, tags: ['a'] });
    assert.ok(landed.some(change => change.del === true && change.path?.join('/') === 'added'), 'what it has gained since is deleted');
    assert.ok(!landed.some(change => change.path?.join('/') === 'meta/color'), 'what did not change is not written');
    assert.deepEqual(store.state.subtasks, { s: { taskId: 'k', n: 1 } }, 'its children are left alone');
    assert.deepEqual(store.revert('tasks', 'k', 1_000_500), [], 'already as it was: nothing to write');
    assert.equal(store.history('tasks', 'k')[0].replicaId, 'server', 'a revert is a write, and in the history as one');

    // A singleton, by time in the id's place
    store.revert('settings', 1_000_500);
    assert.deepEqual(store.state.settings, { theme: 'light' });
    assert.deepEqual(store.recordAt('settings', 1_001_000), { theme: 'dark', size: 2 });
    assert.deepEqual(store.history('settings', { limit: 1 })[0].record, { theme: 'light' });

    // A record deleted since is made again; one that was not there then is deleted
    now.advance(1000);
    store.del('tasks', 'k');
    assert.deepEqual(store.state.subtasks, {}, 'the deletion took its children');
    store.revert('tasks', 'k', 1_001_500);
    assert.deepEqual(store.state.tasks.k, { title: 'Milk', meta: { color: 'red', size: 1 }, geo: { x: 1, y: 2 }, tags: ['a'] });
    store.patch({ tasks: { late: { title: 'Late' } } });
    assert.throws(() => store.revert('tasks', 'late', 1_000_500), /Nothing is known of tasks\/late then/);
    store.revert('tasks', 'k', { v: store.history('tasks', 'k').find(e => e.record === undefined).v });
    assert.equal(store.state.tasks.k, undefined);
    store.dispose();
  });

  test(`${kind}: a record made again brings back what its deletion took with it: its children and theirs, as they were, and nothing else`, () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: true, storage: makeStorage() });
    store.patch({ tasks: { k: { title: 'Milk' }, other: { title: 'Eggs' } } });
    store.patch({ subtasks: { s1: { taskId: 'k', n: 1 }, s2: { taskId: 'k', n: 2 }, elsewhere: { taskId: 'other', n: 3 } }, notes: { deep: { subId: 's1', text: 'a note' } } });
    now.advance(1000);
    store.patch({ subtasks: { s1: { n: 10 } } });   // after the time the task is taken back to
    now.advance(1000);
    // One write deletes the task, which takes its children and theirs, and a record that is no child of it
    store.patch({ tasks: { k: null, other: null } });
    assert.deepEqual([store.state.tasks, store.state.subtasks, store.state.notes], [{}, {}, {}]);
    const version = store.version;

    const landed = store.revert('tasks', 'k', 1_000_500);
    assert.deepEqual(store.state.tasks, { k: { title: 'Milk' } }, 'the other record that write deleted stays deleted');
    assert.deepEqual(store.state.subtasks, { s1: { taskId: 'k', n: 10 }, s2: { taskId: 'k', n: 2 } }, 'its children as they were just before, not as they were then');
    assert.deepEqual(store.state.notes, { deep: { subId: 's1', text: 'a note' } }, 'and a child of a child');
    assert.equal(store.version, version + 1, 'one write for them all');
    assert.equal(landed.length, 4);

    // Asked not to, or with a child made again since, it brings back only what is still gone
    store.del('tasks', 'k');
    store.revert('tasks', 'k', 1_000_500, { children: false });
    assert.deepEqual([Object.keys(store.state.tasks), store.state.subtasks], [['k'], {}]);
    store.del('tasks', 'k');
    store.revert('tasks', 'k', 1_000_500);
    assert.deepEqual(store.state.subtasks, {}, 'the deletion that took them was an earlier one: this one took none');
    store.dispose();
  });

  test(`${kind}: history turned on over records already there starts from them as they stand, once`, () => {
    const now = fakeTime(1_000_000);
    const storage = makeStorage();
    const before = createStore({ ...SHAPE, now, storage });
    before.patch({ tasks: { old: { title: 'From before' } } });
    before.flush();
    before.dispose();
    now.advance(DAY);
    let store = createStore({ ...SHAPE, now, history: true, storage });
    assert.deepEqual(store.history('tasks', 'old').map(e => [e.changes, e.record]), [[[], { title: 'From before' }]], 'a row as it stands, with no changes');
    assert.deepEqual(store.recordAt('tasks', 'old'), { title: 'From before' });
    assert.equal(store.recordAt('tasks', 'old', 1_000_500), undefined, 'before the store kept history, nothing is known');
    assert.deepEqual(store.recordAt('settings'), { theme: 'light' }, 'a singleton too, though nobody has written it');
    store.patch({ tasks: { old: { done: true } } });
    store.dispose();
    store = createStore({ ...SHAPE, now, history: true, storage });
    assert.equal(store.history('tasks', 'old').length, 2, 'loaded again, nothing is added');
    store.dispose();
  });

  test(`${kind}: a record deleted longer ago than history.keep leaves no row, and one deleted since can still be made again`, () => {
    const now = fakeTime(100 * DAY);
    const storage = makeStorage();
    const store = createStore({ ...SHAPE, now, history: { keep: 10 * DAY }, storage });
    const settled = storage.historyCount();   // the singleton's row
    for (let i = 0; i < 20; i++) {
      store.patch({ tasks: { [`gone${i}`]: { n: i } } });
      store.del('tasks', `gone${i}`);
    }
    store.patch({ tasks: { old: { title: 'Written long ago' } } });
    now.advance(15 * DAY);
    store.del('tasks', 'old');   // within what is kept; and an hour and more since the last compaction
    store.flush();
    assert.equal(storage.historyCount(), settled + 2, 'of the twenty made and deleted, nothing; of the one deleted since, its last form and its deletion');
    assert.equal(store.recordAt('tasks', 'gone3', 100 * DAY + 1), undefined);
    assert.deepEqual(store.history('tasks', 'old').map(e => e.record), [undefined, { title: 'Written long ago' }]);
    store.revert('tasks', 'old', now() - DAY);
    assert.deepEqual(store.state.tasks.old, { title: 'Written long ago' });
    store.dispose();
  });

  test(`${kind}: history.keep forgets what is older, but for the last form of each record before it`, () => {
    const now = fakeTime(100 * DAY);
    const store = createStore({ ...SHAPE, now, history: { keep: 10 * DAY }, storage: makeStorage() });
    store.patch({ tasks: { k: { n: 1 }, quiet: { n: 1 } } });
    now.advance(DAY);
    store.patch({ tasks: { k: { n: 2 } } });
    now.advance(DAY);
    store.patch({ tasks: { k: { n: 3 } } });
    now.advance(20 * DAY);
    store.patch({ tasks: { k: { n: 4 } } });   // an hour and more since the last compaction: forgotten as it arrives
    assert.deepEqual(store.history('tasks', 'k').map(e => e.record.n), [4, 3]);
    assert.deepEqual(store.recordAt('tasks', 'k', 112 * DAY), { n: 3 }, 'what it was at the edge of what is kept');
    assert.deepEqual(store.recordAt('tasks', 'quiet'), { n: 1 }, 'a record nobody has written since keeps its one row');
    store.dispose();
  });
}

test('who: a write is told with the key of its user, as presence keys users, and a write in one turn keeps its own form of the record', async () => {
  const store = createStore({ ...SHAPE, history: true, presence: { key: user => user.email } });
  const one = store.apply({ replica: 'r1', seq: 1, ts: Date.now(), changes: [{ c: 'tasks', id: 'k', put: { n: 1 } }] }, { user: { email: 'a@x' } });
  // The same turn of the event loop: one commit for both, and each row its own record
  const two = store.apply({ replica: 'r2', seq: 1, ts: Date.now(), changes: [{ c: 'tasks', id: 'k', path: ['n'], set: 2 }] }, {});
  assert.ok(one && two);
  assert.deepEqual(store.history('tasks', 'k').map(e => [e.replicaId, e.user, e.record]), [['r2', undefined, { n: 2 }], ['r1', 'a@x', { n: 1 }]]);
  store.dispose();
});

test('history is off unless asked for, needs a storage that keeps it, and says what it does not take', () => {
  const plain = createStore(SHAPE);
  assert.throws(() => plain.history('tasks', 'k'), /History is off on this store/);
  assert.throws(() => plain.recordAt('tasks', 'k'), /History is off/);
  assert.throws(() => plain.revert('tasks', 'k', 1), /History is off/);
  assert.throws(() => createStore({ ...SHAPE, history: true, storage: { load: () => null, commit() {}, flush() {} } }), /storage keeps no history/);
  assert.throws(() => createStore({ ...SHAPE, history: 'yes' }), /history must be true, false, or \{ keep \}/);
  assert.throws(() => createStore({ ...SHAPE, history: { keep: 0 } }), /history.keep/);
  const store = createStore({ ...SHAPE, history: true });
  assert.throws(() => store.history('nope', 'k'), /not a collection or singleton/);
  assert.throws(() => store.history('tasks'), /named by its id/);
  assert.throws(() => store.history('tasks', 'k', { limit: 0 }), /limit/);
  assert.throws(() => store.history('tasks', 'k', { path: 'title' }), /path is an array/);
  assert.throws(() => store.recordAt('tasks', 'k', 'yesterday'), /When is a time/);
});

test('a document that replaces the storage starts its history over; a store removed from a SQLite file takes its history with it', () => {
  const sqlite = sqliteStorage();
  let store = createStore({ ...SHAPE, history: true, storage: sqlite.store('main') });
  store.patch({ tasks: { k: { n: 1 } } });
  store.patch({ tasks: { k: { n: 2 } } });
  const doc = store.export();
  store.dispose();
  sqlite.store('main').replace(doc);
  store = createStore({ ...SHAPE, history: true, storage: sqlite.store('main') });
  assert.deepEqual(store.history('tasks', 'k').map(e => e.changes), [[]], 'one row, the record as the document has it');
  store.dispose();
  sqlite.remove('main');
  assert.equal(sqlite.db.prepare('SELECT count(*) AS n FROM history').get().n, 0);
  sqlite.close();
});
