// history.test.js - A store that keeps its history: every record as each write left it, when and by whom,
// read back, asked for as it was, and put back
import { test } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { sqliteStorage } from '../src/server/sqlite-node.js';
import { createRelay } from '../src/relay/index.js';
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

    const history = store.historyOf('tasks', 'k');
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
    assert.deepEqual(store.historyOf('tasks', 'k', { limit: 2 }).map(e => e.v), history.slice(0, 2).map(e => e.v));
    assert.deepEqual(store.historyOf('tasks', 'k', { limit: 2, before: history[1].v }).map(e => e.v), history.slice(2).map(e => e.v));
    assert.deepEqual(store.historyOf('tasks', 'k', { path: ['meta', 'color'] }).map(e => e.replicaId), ['a', 'server', 'a'], 'the record made and deleted touch every path in it');
    assert.deepEqual(store.historyOf('tasks', 'k', { path: ['title'] }).map(e => e.replicaId), ['a', 'b', 'a']);
    assert.deepEqual(store.historyOf('tasks', 'never'), []);

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
    assert.equal(store.historyOf('tasks', 'k')[0].replicaId, 'server', 'a revert is a write, and in the history as one');

    // A singleton, by time in the id's place
    store.revert('settings', 1_000_500);
    assert.deepEqual(store.state.settings, { theme: 'light' });
    assert.deepEqual(store.recordAt('settings', 1_001_000), { theme: 'dark', size: 2 });
    assert.deepEqual(store.historyOf('settings', { limit: 1 })[0].record, { theme: 'light' });

    // A record deleted since is made again; one that was not there then is deleted
    now.advance(1000);
    store.del('tasks', 'k');
    assert.deepEqual(store.state.subtasks, {}, 'the deletion took its children');
    store.revert('tasks', 'k', 1_001_500);
    assert.deepEqual(store.state.tasks.k, { title: 'Milk', meta: { color: 'red', size: 1 }, geo: { x: 1, y: 2 }, tags: ['a'] });
    store.patch({ tasks: { late: { title: 'Late' } } });
    store.revert('tasks', 'late', 1_000_500);
    assert.equal(store.state.tasks.late, undefined, 'one that was not there then is deleted');
    store.revert('tasks', 'k', { v: store.historyOf('tasks', 'k').find(e => e.record === undefined).v });
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
    assert.deepEqual(store.historyOf('tasks', 'old').map(e => [e.changes, e.record]), [[[], { title: 'From before' }]], 'a row as it stands, with no changes');
    assert.deepEqual(store.recordAt('tasks', 'old'), { title: 'From before' });
    assert.equal(store.recordAt('tasks', 'old', 1_000_500), undefined, 'before the store kept history, nothing is known');
    assert.deepEqual(store.recordAt('settings'), { theme: 'light' }, 'a singleton too, though nobody has written it');
    store.patch({ tasks: { old: { done: true } } });
    store.dispose();
    store = createStore({ ...SHAPE, now, history: true, storage });
    assert.equal(store.historyOf('tasks', 'old').length, 2, 'loaded again, nothing is added');
    store.dispose();
  });

  test(`${kind}: a record deleted longer ago than history.keep leaves no row, and one deleted since can still be made again`, () => {
    const now = fakeTime(100 * DAY);
    const storage = makeStorage();
    const store = createStore({ ...SHAPE, now, history: { keep: 10 * DAY }, storage });
    const rows = () => store.activity({ limit: 1000 }).length;
    const settled = rows();   // the singleton's row
    for (let i = 0; i < 20; i++) {
      store.patch({ tasks: { [`gone${i}`]: { n: i } } });
      store.del('tasks', `gone${i}`);
    }
    store.patch({ tasks: { old: { title: 'Written long ago' } } });
    now.advance(15 * DAY);
    store.del('tasks', 'old');   // within what is kept; and an hour and more since the last compaction
    assert.equal(rows(), settled + 2, 'of the twenty made and deleted, nothing; of the one deleted since, its last form and its deletion');
    assert.equal(store.recordAt('tasks', 'gone3', 100 * DAY + 1), undefined);
    assert.deepEqual(store.historyOf('tasks', 'old').map(e => e.record), [undefined, { title: 'Written long ago' }]);
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
    assert.deepEqual(store.historyOf('tasks', 'k').map(e => e.record.n), [4, 3]);
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
  assert.deepEqual(store.historyOf('tasks', 'k').map(e => [e.replicaId, e.user, e.record]), [['r2', undefined, { n: 2 }], ['r1', 'a@x', { n: 1 }]]);
  store.dispose();
});

test('history is off unless asked for, needs a storage that keeps it, and says what it does not take', () => {
  const plain = createStore(SHAPE);
  assert.throws(() => plain.historyOf('tasks', 'k'), /History is off on this store/);
  assert.throws(() => plain.recordAt('tasks', 'k'), /History is off/);
  assert.throws(() => plain.revert('tasks', 'k', 1), /History is off/);
  assert.throws(() => createStore({ ...SHAPE, history: true, storage: { load: () => null, commit() {}, flush() {} } }), /storage keeps no history/);
  assert.throws(() => createStore({ ...SHAPE, history: 'yes' }), /history must be true, false, or \{ keep \}/);
  assert.throws(() => createStore({ ...SHAPE, history: { keep: 0 } }), /history.keep/);
  const store = createStore({ ...SHAPE, history: true });
  assert.throws(() => store.historyOf('nope', 'k'), /not a collection or singleton/);
  assert.throws(() => store.historyOf('tasks'), /named by its id/);
  assert.throws(() => store.historyOf('tasks', 'k', { limit: 0 }), /limit/);
  assert.throws(() => store.historyOf('tasks', 'k', { path: 'title' }), /path is an array/);
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
  assert.deepEqual(store.historyOf('tasks', 'k').map(e => e.changes), [[]], 'one row, the record as the document has it');
  store.dispose();
  sqlite.remove('main');
  assert.equal(sqlite.db.prepare('SELECT count(*) AS n FROM history').get().n, 0);
  sqlite.close();
});

// --- Clients ------------------------------------------------------------------------------

/** What a promise that should reject rejects with */
const refusal = promise => promise.then(() => assert.fail('it resolved'), err => err);

test('a client reads a record\'s history and the record as it was, when the store lets its clients; not by default', async () => {
  const now = fakeTime(1_000_000);
  const closed = createStore({ ...SHAPE, now, history: true });
  const closedNet = createNetwork(closed);
  const shut = closedNet.client({ replicaId: 'a', ...SHAPE, now });
  const none = createNetwork(createStore({ ...SHAPE, now })).client({ replicaId: 'a', ...SHAPE, now });
  assert.equal((await refusal(shut.historyOf('tasks', 'k'))).code, 'offline', 'not connected yet');
  await closedNet.settle();
  const asked = [refusal(shut.historyOf('tasks', 'k')), refusal(shut.recordAt('tasks', 'k', 5))];
  await closedNet.settle();
  for (const err of await Promise.all(asked)) assert.deepEqual([err.code, err.message], ['forbidden', 'This store does not let its clients read its history']);
  none.connect();
  const errors = [];
  shut.on('error', err => errors.push(err));
  assert.deepEqual(errors, [], 'a refusal is its asker\'s to hear, not an error event');

  const store = createStore({ ...SHAPE, now, history: { clients: true } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
  const b = net.client({ replicaId: 'b', ...SHAPE, now }, { user: { id: 'bob' } });
  await net.settle();
  a.state.tasks.k = { title: 'Milk' };
  await net.settle();
  now.advance(1000);
  b.state.tasks.k.title = 'Oat milk';
  b.state.settings.theme = 'dark';
  await net.settle();
  now.advance(1000);
  delete a.state.tasks.k;
  await net.settle();

  const answers = [
    b.historyOf('tasks', 'k'),
    b.historyOf('tasks', 'k', { limit: 1 }),
    b.historyOf('tasks', 'k', { path: ['title'], limit: 500 }),
    b.recordAt('tasks', 'k', 1_000_500),
    b.recordAt('tasks', 'k', new Date(1_001_500)),
    b.recordAt('tasks', 'k', { ts: new Date(1_002_500) }),
    b.historyOf('settings', { limit: 5 }),
    b.recordAt('settings', 1_000_500),
    refusal(b.historyOf('tasks', 'k', { path: 'title' })),
    refusal(b.recordAt('tasks', 'k', 'yesterday'))
  ];
  await net.settle();
  const [all, one, titles, first, second, gone, settings, theme, badPath, badWhen] = await Promise.all(answers);
  assert.deepEqual(all.map(e => [e.replicaId, e.user, e.record]), [['a', 'alice', undefined], ['b', 'bob', { title: 'Oat milk' }], ['a', 'alice', { title: 'Milk' }]]);
  assert.deepEqual(all[1].changes, [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }]);
  assert.equal(one.length, 1);
  assert.equal(titles.length, 3);
  assert.deepEqual([first, second, gone], [{ title: 'Milk' }, { title: 'Oat milk' }, undefined]);
  assert.deepEqual(settings.map(e => e.record.theme), ['dark', 'light']);
  assert.deepEqual(theme, { theme: 'light' });
  assert.deepEqual([badPath.code, badWhen.code], ['invalid', 'invalid']);
  await assert.rejects(b.historyOf('nope', 'k'), /not a collection/);
  await assert.rejects(b.recordAt('tasks', 'k'), /takes a time or a version/);

  // What is asked when the connection goes is not left waiting
  const hanging = refusal(b.historyOf('tasks', 'k'));
  b.link.goOffline();
  await net.settle();
  assert.equal((await hanging).code, 'offline');
});

test('history.clients as a function says who may read which record; a request draws on the rate limit', async () => {
  const seen = [];
  const store = createStore({
    ...SHAPE,
    rateLimit: { burst: 4, perSecond: 1 },
    history: { clients: (user, { c, id }) => { seen.push([user.id, c, id]); return c === 'tasks' && user.id === 'alice'; } }
  });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE }, { user: { id: 'alice' } });
  const b = net.client({ replicaId: 'b', ...SHAPE }, { user: { id: 'bob' } });
  await net.settle();
  store.patch({ tasks: { k: { title: 'Milk' } } });
  const answers = [a.historyOf('tasks', 'k'), refusal(b.historyOf('tasks', 'k')), refusal(a.historyOf('settings')), a.historyOf('tasks', 'k'), a.historyOf('tasks', 'k'), refusal(a.historyOf('tasks', 'k'))];
  await net.settle();
  const [mine, theirs, settings, again, third, limited] = await Promise.all(answers);
  assert.equal(mine.length, 1);
  assert.deepEqual([theirs.code, settings.code], ['forbidden', 'forbidden']);
  assert.deepEqual(seen.slice(0, 3), [['alice', 'tasks', 'k'], ['bob', 'tasks', 'k'], ['alice', 'settings', undefined]]);
  assert.equal(again.length + third.length, 2);
  assert.equal(limited.code, 'rate-limited', 'the hello and three requests spent the bucket');
  assert.ok(limited.retryAfter > 0);
  assert.throws(() => createStore({ ...SHAPE, history: { clients: 'all' } }), /history.clients/);
});

test('a client reverts a record as an edit of its own: judged by the rules, one op, undone with undo', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({
    ...SHAPE,
    now,
    history: { clients: true },
    rules: { tasks: (user, before, after) => (before ?? after).owner === user.id }
  });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
  const b = net.client({ replicaId: 'b', ...SHAPE, now }, { user: { id: 'bob' } });
  await net.settle();
  a.state.tasks.k = { owner: 'alice', title: 'Milk', meta: { color: 'red' }, geo: { x: 1, y: 2 } };
  await net.settle();
  now.advance(1000);
  a.state.tasks.k.title = 'Oat milk';
  a.state.tasks.k.meta = { size: 2 };
  a.state.tasks.k.geo = { x: 9 };
  a.state.tasks.k.added = true;
  a.state.settings.theme = 'dark';
  await net.settle();
  const version = store.version;

  const settle = async promise => { await net.settle(); const value = await promise; await net.settle(); return value; };
  assert.deepEqual(await settle(a.revert('tasks', 'k', 1_000_500)), { owner: 'alice', title: 'Milk', meta: { color: 'red' }, geo: { x: 1, y: 2 } });
  assert.deepEqual(store.state.tasks.k, { owner: 'alice', title: 'Milk', meta: { color: 'red' }, geo: { x: 1, y: 2 } });
  assert.equal(store.version, version + 1, 'one op');
  assert.equal(store.historyOf('tasks', 'k')[0].replicaId, 'a', 'the client\'s write, not the server\'s');
  a.undo();
  await net.settle();
  assert.equal(store.state.tasks.k.title, 'Oat milk', 'undone like any edit');

  // Another user's revert is refused as its edit would be, and taken back
  const rejected = [];
  b.on('rejected', e => rejected.push(e.code));
  await settle(b.revert('tasks', 'k', 1_000_500));
  assert.deepEqual(rejected, ['forbidden']);
  assert.equal(b.state.tasks.k.title, 'Oat milk');

  // A singleton, and a record that was not there then
  await settle(a.revert('settings', 1_000_500));
  assert.equal(store.state.settings.theme, 'light');
  a.state.tasks.late = { owner: 'alice', title: 'Late' };
  await net.settle();
  assert.equal(await settle(a.revert('tasks', 'late', 1_000_500)), undefined);
  assert.equal(store.state.tasks.late, undefined);
  await assert.rejects(a.revert('tasks', 'k'), /takes a time or a version/);
});

for (const acknowledge of ['upstream', 'local']) {
  test(`behind a relay (${acknowledge}) a client's history request is asked of the server and answered to it; with the server away it is refused`, async () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: { clients: (user, { c }) => c === 'tasks' } });
    const central = createNetwork(store, { vouch: credential => credential });
    const uplink = central.link({ user: { id: 'relay' } });
    const errors = [];
    const relay = createRelay({ shapeOf: () => SHAPE, link: uplink.factory, acknowledge, grace: 0, reconnect: false, keepalive: false, now, onError: err => errors.push(err) });
    const site = createNetwork(relay);
    const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); if (central.pending === 0 && site.pending === 0) return; } };
    const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
    const other = site.client({ replicaId: 'o', ...SHAPE, now }, { user: { id: 'olga' } });
    await settle();
    a.state.tasks.k = { title: 'Milk' };
    await settle();
    now.advance(1000);
    a.state.tasks.k.title = 'Oat milk';
    await settle();

    const answers = [a.historyOf('tasks', 'k'), other.recordAt('tasks', 'k', 1_000_500), refusal(a.historyOf('settings'))];
    await settle();
    const [entries, was, refused] = await Promise.all(answers);
    assert.deepEqual(entries.map(e => e.record), [{ title: 'Oat milk' }, { title: 'Milk' }]);
    assert.equal(entries[0].user, acknowledge === 'upstream' ? 'alice' : 'relay', 'who wrote: the client, or the browser its tabs are');
    assert.deepEqual(was, { title: 'Milk' });
    assert.equal(refused.code, 'forbidden');
    assert.deepEqual(errors, [], 'a refusal is the client\'s to hear, and no fault of the relay\'s');

    uplink.goOffline();
    await settle();
    const away = refusal(a.historyOf('tasks', 'k'));
    await settle();
    assert.equal((await away).code, 'unavailable');
    relay.close();
  });
}

test('a request the server never answers is given up on, and one with fields that are not what they say is refused as invalid, no fault of the server\'s', async () => {
  const faults = [];
  const sqlite = sqliteStorage();
  const store = createStore({ ...SHAPE, history: { clients: true }, storage: sqlite.store('main'), onError: err => faults.push(err) });
  store.patch({ tasks: { k: { title: 'Milk' } } });
  const session = store.session;
  let deaf = false;
  store.session = options => {
    const made = session(options);
    const receive = made.receive;
    made.receive = message => { if (!(deaf && message.t === 'history')) receive(message); };
    return made;
  };
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, requestTimeout: 30 });
  await net.settle();

  const asked = [
    refusal(a.historyOf('tasks', 'k', { before: {} })),
    refusal(a.historyOf('tasks', 'k', { before: 'x' })),
    refusal(a.historyOf('tasks', 'k', { path: [1] })),
    refusal(a.recordAt('tasks', 'k', { ts: {} })),
    refusal(a.recordAt('tasks', 'k', { v: 1.5 })),
    refusal(a.historyOf('tasks', 'k', { limit: 'many' })),
    refusal(a.activity({ limit: 0 }))
  ];
  await net.settle();
  const answers = await Promise.all(asked);
  assert.deepEqual(answers.slice(0, 5).map(err => err.code), ['invalid', 'invalid', 'invalid', 'invalid', 'invalid']);
  assert.ok(answers[5] instanceof TypeError && answers[6] instanceof TypeError, 'a limit that is none is refused before it is asked, as the store refuses it');
  assert.deepEqual(faults, []);
  assert.throws(() => store.historyOf('tasks', 'k', { before: {} }), /before is a version/);
  assert.throws(() => store.historyOf('tasks', 'k', { limit: 1.5 }), /limit/);
  assert.throws(() => store.recordAt('tasks', 'k', NaN), /When is a time/);

  deaf = true;
  const unanswered = refusal(a.historyOf('tasks', 'k'));
  await net.settle();
  const err = await unanswered;
  assert.deepEqual([err.code, err.message], ['timeout', 'The server did not answer within 30 ms']);
  store.dispose();
  sqlite.close();
});

test('a path few writes touch is read a thousand rows a request: the client asks on by itself, and each request is one the rate limit counts', async () => {
  const store = createStore({ ...SHAPE, history: { clients: true }, rateLimit: { burst: 4, perSecond: 1 } });
  store.patch({ tasks: { k: { title: 'Milk', n: 0 } } });
  store.patch({ tasks: { k: { title: 'Oat milk' } } });
  for (let i = 1; i <= 2500; i++) store.patch({ tasks: { k: { n: i } } });
  let requests = 0;
  const session = store.session;
  store.session = options => {
    const made = session(options);
    const receive = made.receive;
    made.receive = message => { if (message.t === 'history') requests++; receive(message); };
    return made;
  };
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  const settle = async promise => { for (let i = 0; i < 10; i++) await net.settle(); return promise; };

  const titles = await settle(a.historyOf('tasks', 'k', { path: ['title'] }));
  assert.deepEqual(titles.map(e => e.record.title), ['Oat milk', 'Milk'], 'the two writes that touched it, 2500 rows down');
  assert.equal(requests, 3, 'a thousand rows a request');
  assert.equal(store.historyOf('tasks', 'k', { path: ['title'] }).length, 2, 'the server reads its own through');

  // The hello and three requests spent the bucket: asking on is not free
  const limited = await settle(refusal(a.historyOf('tasks', 'k', { path: ['title'] })));
  assert.equal(limited.code, 'rate-limited');
  store.dispose();
});

// --- The store's activity ---------------------------------------------------------------------

for (const [kind, makeStorage] of Object.entries(storages)) {
  test(`${kind}: activity is every record's writes, newest first: narrowed by time, user, collection and deletions, and read on by a cursor`, () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: true, storage: makeStorage() });
    const op = (replica, seq, changes, user) => store.apply({ replica, seq, ts: now(), changes }, { user: { id: user } });
    op('a', 1, [{ c: 'tasks', id: 'k', put: { title: 'Milk' } }, { c: 'tasks', id: 'j', put: { title: 'Eggs' } }], 'alice');
    now.advance(1000);
    op('b', 1, [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }, { c: 'settings', path: ['theme'], set: 'dark' }], 'bob');
    now.advance(1000);
    store.patch({ subtasks: { s: { taskId: 'k', n: 1 } } });
    now.advance(1000);
    op('a', 2, [{ c: 'tasks', id: 'k', del: true }], 'alice');

    const told = entries => entries.map(e => `${e.c}/${e.id ?? ''}@${e.replicaId}${e.record === undefined ? ' gone' : ''}`);
    const all = store.activity();
    assert.deepEqual(told(all), [
      'tasks/k@a gone', 'subtasks/s@a gone',       // one write: the task, and the child it took, in the order it wrote them
      'subtasks/s@server',
      'tasks/k@b', 'settings/@b',
      'tasks/k@a', 'tasks/j@a',
      'settings/@server'                            // as it stood when history began
    ]);
    assert.deepEqual([all[1].user, all[2].user, all[3].user], ['alice', undefined, 'bob']);
    assert.deepEqual(all[3].changes, [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }]);
    assert.deepEqual(all[3].record, { title: 'Oat milk' });
    assert.ok(!('id' in all[4]), 'a singleton has no id');
    assert.deepEqual([all[0].n, all[1].n, all[0].v === all[1].v], [0, 1, true], 'a write\'s rows are numbered in it');

    assert.deepEqual(told(store.activity({ user: 'alice' })), ['tasks/k@a gone', 'subtasks/s@a gone', 'tasks/k@a', 'tasks/j@a']);
    assert.deepEqual(told(store.activity({ collection: 'tasks' })), ['tasks/k@a gone', 'tasks/k@b', 'tasks/k@a', 'tasks/j@a']);
    assert.deepEqual(told(store.activity({ deleted: true })), ['tasks/k@a gone', 'subtasks/s@a gone'], 'what was deleted, with no id to ask by');
    assert.deepEqual(told(store.activity({ since: 1_000_500, until: new Date(1_002_500) })), ['subtasks/s@server', 'tasks/k@b', 'settings/@b']);
    assert.deepEqual(told(store.activity({ before: all[2].v, after: all[5].v })), ['tasks/k@b', 'settings/@b']);
    assert.deepEqual(told(store.activity({ oldestFirst: true, after: all[5].v })), ['tasks/k@b', 'settings/@b', 'subtasks/s@server', 'tasks/k@a gone', 'subtasks/s@a gone'], 'the store followed from a version on');

    // A page at a time, the last entry the cursor: a write of several records is split and nothing is told twice
    const paged = [];
    for (let cursor; ;) {
      const page = store.activity({ limit: 3, cursor });
      paged.push(...page);
      if (page.length < 3) break;
      cursor = page[page.length - 1];
    }
    assert.deepEqual(told(paged), told(all));

    // What was deleted is made again from what the feed says
    const gone = store.activity({ deleted: true, collection: 'tasks' })[0];
    store.revert(gone.c, gone.id, { v: gone.v - 1 });
    assert.deepEqual([store.state.tasks.k, store.state.subtasks.s], [{ title: 'Oat milk' }, { taskId: 'k', n: 1 }]);

    assert.throws(() => store.activity({ limit: 0 }), /limit/);
    assert.throws(() => store.activity({ before: 'x' }), /before is a version/);
    assert.throws(() => store.activity({ since: 'yesterday' }), /since is a time/);
    assert.throws(() => store.activity({ user: 5 }), /user is a user's key/);
    assert.throws(() => store.activity({ collection: 'nope' }), /not a collection/);
    assert.throws(() => store.activity({ deleted: 'yes' }), /true or false/);
    assert.throws(() => store.activity({ cursor: { v: 'x' } }), /cursor is the entry/);
    assert.throws(() => createStore(SHAPE).activity(), /History is off/);
    store.dispose();
  });
}

test('a client reads the store\'s activity: the records it may not read the history of are left out, a large answer comes a request at a time', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, history: { clients: (user, { c }) => user.id === 'admin' || c === 'tasks' } });
  for (let i = 0; i < 130; i++) store.patch({ tasks: { [`t${i}`]: { n: i } }, subtasks: { [`s${i}`]: { taskId: `t${i}` } } });
  let requests = 0;
  const session = store.session;
  store.session = options => {
    const made = session(options);
    const receive = made.receive;
    made.receive = message => { if (message.t === 'history') requests++; receive(message); };
    return made;
  };
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
  const admin = net.client({ replicaId: 'r', ...SHAPE, now }, { user: { id: 'admin' } });
  await net.settle();
  const settle = async promise => { for (let i = 0; i < 12; i++) await net.settle(); return promise; };

  const mine = await settle(a.activity({ limit: 120 }));
  assert.equal(mine.length, 120);
  assert.ok(mine.every(e => e.c === 'tasks'), 'what alice may not read the history of is not told');
  assert.deepEqual(mine.slice(0, 2).map(e => e.id), ['t129', 't128']);
  assert.equal(requests, 2, 'a hundred an answer');
  const everything = await settle(admin.activity({ limit: 4 }));
  assert.deepEqual(everything.map(e => `${e.c}/${e.id}`), ['tasks/t129', 'subtasks/s129', 'tasks/t128', 'subtasks/s128']);
  assert.equal((await settle(a.activity())).length, 50, 'fifty unless asked for more');
  const first = await settle(a.activity({ oldestFirst: true, limit: 2, since: new Date(0) }));
  assert.deepEqual(first.map(e => e.id), ['t0', 't1']);
  const next = await settle(a.activity({ oldestFirst: true, limit: 2, cursor: first[1] }));
  assert.deepEqual(next.map(e => e.id), ['t2', 't3']);
  assert.deepEqual((await settle(a.activity({ deleted: true }))), []);
  assert.equal((await settle(refusal(a.activity({ user: 5 })))).code, 'invalid');
  await assert.rejects(a.activity({ collection: 'nope' }), /not a collection/);

  const shut = createNetwork(createStore({ ...SHAPE, history: true }));
  const b = shut.client({ replicaId: 'b', ...SHAPE });
  await shut.settle();
  const refused = refusal(b.activity());
  await shut.settle();
  assert.equal((await refused).code, 'forbidden');
});

test('behind a relay a client\'s request for the store\'s activity is the server\'s to answer', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, history: { clients: true } });
  const central = createNetwork(store, { vouch: credential => credential });
  const uplink = central.link({ user: { id: 'relay' } });
  const relay = createRelay({ shapeOf: () => SHAPE, link: uplink.factory, grace: 0, reconnect: false, keepalive: false, now });
  const site = createNetwork(relay);
  const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); if (central.pending === 0 && site.pending === 0) return; } };
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'alice' } });
  await settle();
  a.state.tasks.k = { title: 'Milk' };
  await settle();
  const asked = a.activity({ user: 'alice' });
  await settle();
  assert.deepEqual((await asked).map(e => [e.c, e.id, e.user]), [['tasks', 'k', 'alice']]);
  relay.close();
});

// --- One shape, one way to read on ------------------------------------------------------------

for (const [kind, makeStorage] of Object.entries(storages)) {
  test(`${kind}: a record's history and the store's activity tell the same entries, take the same narrowing, and are read on the same way`, async () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: { clients: true }, storage: makeStorage() });
    const write = (n, user) => store.apply({ replica: `r-${user}`, seq: n, ts: now(), changes: [{ c: 'tasks', id: 'k', path: ['n'], set: n }] }, { user: { id: user } });
    store.patch({ tasks: { k: { n: 0 } } });
    for (let n = 1; n <= 120; n++) {
      now.advance(1000);
      write(n, n % 2 ? 'alice' : 'bob');
    }
    const of = store.historyOf('tasks', 'k');
    const all = store.activity({ collection: 'tasks' });
    assert.equal(of.length, 50, 'fifty unless asked for more');
    assert.deepEqual(of, all, 'the same entries, each with the record it is of');
    assert.deepEqual([of[0].c, of[0].id, of[0].record], ['tasks', 'k', { n: 120 }]);
    assert.ok(!('id' in store.historyOf('settings')[0]) && store.historyOf('settings')[0].c === 'settings');

    // The last entry of one answer reads on after it, in either
    for (const read of [cursor => store.historyOf('tasks', 'k', { limit: 30, cursor }), cursor => store.activity({ collection: 'tasks', limit: 30, cursor })]) {
      const pages = [];
      for (let cursor; ;) {
        const page = read(cursor);
        pages.push(...page);
        if (page.length < 30) break;
        cursor = page[page.length - 1];
      }
      assert.deepEqual(pages.map(e => e.record.n), Array.from({ length: 121 }, (_, i) => 120 - i));
    }
    assert.deepEqual(store.historyOf('tasks', 'k', { before: of[3].v, limit: 2 }).map(e => e.record.n), [116, 115], 'before bounds it by version');

    // The same narrowing
    const narrowing = { since: 1_100_000, until: new Date(1_110_000), user: 'bob' };
    const narrowed = store.historyOf('tasks', 'k', narrowing);
    assert.deepEqual(narrowed.map(e => e.record.n), [110, 108, 106, 104, 102, 100]);
    assert.deepEqual(store.activity(narrowing), narrowed);
    assert.throws(() => store.historyOf('tasks', 'k', { since: 'x' }), /since is a time/);
    assert.throws(() => store.historyOf('tasks', 'k', { user: 5 }), /user is a user's key/);
    assert.throws(() => store.historyOf('tasks', 'k', { cursor: 5 }), /cursor is the entry/);

    // And so for a client
    const net = createNetwork(store);
    const a = net.client({ replicaId: 'a', ...SHAPE, now });
    await net.settle();
    const settle = async promise => { for (let i = 0; i < 12; i++) await net.settle(); return promise; };
    assert.deepEqual(await settle(a.historyOf('tasks', 'k')), of);
    assert.deepEqual(await settle(a.historyOf('tasks', 'k', narrowing)), narrowed);
    const whole = await settle(a.historyOf('tasks', 'k', { limit: 121 }));
    assert.equal(whole.length, 121, 'more than an answer carries, asked on by itself');
    const next = await settle(a.historyOf('tasks', 'k', { limit: 2, cursor: of[of.length - 1] }));
    assert.deepEqual(next.map(e => e.record.n), [70, 69]);
    assert.deepEqual(await settle(a.recordAt(next[0].c, next[0].id, { v: next[0].v })), { n: 70 }, 'an entry says what to ask of');
    store.dispose();
  });
}

// --- Found in review ---------------------------------------------------------------------------

test('a client that reverts to a time before the store kept history is refused, and the record is not deleted', async () => {
  const now = fakeTime(1_000_000);
  const storage = memoryStorage();
  const before = createStore({ ...SHAPE, now, storage });
  before.patch({ tasks: { k: { title: 'From before' } } });
  before.dispose();
  now.advance(DAY);
  const store = createStore({ ...SHAPE, now, storage, history: { clients: true } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE, now });
  const behind = createNetwork(createRelay({ shapeOf: () => SHAPE, link: net.link({ user: { id: 'relay' } }).factory, acknowledge: 'local', grace: 0, reconnect: false, keepalive: false, now }));
  const tab = behind.client({ replicaId: 'tab', ...SHAPE, now });
  const settle = async promise => { for (let i = 0; i < 12; i++) { await net.settle(); await behind.settle(); } return promise; };
  await settle();
  assert.throws(() => store.revert('tasks', 'k', 1_000_500), /Nothing is known/);
  for (const db of [a, tab]) {
    const err = await settle(refusal(db.revert('tasks', 'k', 1_000_500)));
    assert.deepEqual([err.code, err.message], ['unknown', 'Nothing is known of tasks/k then: it is before the store kept its history']);
    assert.equal(await settle(db.recordAt('tasks', 'k', 1_000_500)), undefined);
  }
  await settle();
  assert.deepEqual(store.state.tasks.k, { title: 'From before' }, 'still there');
  // Not there then, where the history does know: that one is deleted
  now.advance(1000);
  a.state.tasks.late = { title: 'Late' };
  await settle();
  assert.equal(await settle(a.revert('tasks', 'late', now() - 500)), undefined);
  assert.equal(store.state.tasks.late, undefined);
});

test('a request that narrows the store\'s activity reads no more rows than one that does not: the narrowing is done on what is read', async () => {
  const sqlite = sqliteStorage();
  const storage = sqlite.store('main');
  const store = createStore({ ...SHAPE, history: { clients: true }, storage });
  for (let i = 0; i < 2600; i++) store.patch({ tasks: { [`t${i % 7}`]: { n: i } } });
  store.flush();
  // What the storage is asked, and how much of it: a narrowing it was given would have it read rows it never returns
  const asked = [];
  const activity = storage.activity;
  storage.activity = query => { const rows = activity(query); asked.push([Object.keys(query).filter(key => query[key] !== undefined && query[key] !== false).sort().join(), rows.length]); return rows; };
  const history = storage.history;
  storage.history = query => { const rows = history(query); asked.push([Object.keys(query).filter(key => query[key] !== undefined).sort().join(), rows.length]); return rows; };
  const sent = [];
  const session = store.session({ send: message => sent.push(message) });
  session.receive({ t: 'hello', proto: 3, replica: 'x', ops: [] });
  store.flush();
  const ask = query => { asked.length = 0; sent.length = 0; session.receive({ t: 'history', req: 1, ...query }); store.flush(); return sent.find(m => m.t === 'history'); };

  for (const narrowing of [{ user: 'nobody' }, { deleted: true }, { since: 9e15 }, { collection: 'subtasks' }]) {
    const answer = ask({ all: true, ...narrowing });
    assert.deepEqual(answer.entries, [], JSON.stringify(narrowing));
    assert.equal(asked.reduce((sum, [, rows]) => sum + rows, 0), 1000, 'a thousand rows read, and no more');
    assert.ok(asked.every(([keys]) => !/user|deleted|fromAt|toAt|\bc\b/.test(keys)), `the storage is not asked to narrow: ${asked[0][0]}`);
    assert.ok(Number.isSafeInteger(answer.more.v), 'and it says where it stopped');
  }
  const one = ask({ c: 'tasks', id: 't0', user: 'nobody' });
  assert.deepEqual(one.entries, []);
  assert.ok(asked.every(([keys]) => !/user|fromAt|upToAt/.test(keys)), 'nor for one record');
  // The store's own reading is its own to spend: the storage narrows it
  asked.length = 0;
  assert.deepEqual(store.activity({ user: 'nobody' }), []);
  assert.ok(/user/.test(asked[0][0]));
  store.dispose();
  sqlite.close();
});

test('where a request for the activity stops says nothing of a record the client may not read', async () => {
  const store = createStore({ ...SHAPE, history: { clients: (user, { c }) => c === 'tasks' } });
  store.patch({ tasks: { mine: { n: 1 } } });
  for (let i = 0; i < 1100; i++) store.patch({ subtasks: { [`hidden-${i}`]: { n: i } } });
  const sent = [];
  const session = store.session({ send: message => sent.push(message) });
  session.receive({ t: 'hello', proto: 3, replica: 'x', ops: [] });
  session.receive({ t: 'history', req: 1, all: true });
  store.flush();
  const answer = sent.find(m => m.t === 'history');
  assert.deepEqual(answer.entries, []);
  assert.deepEqual(Object.keys(answer.more).sort(), ['n', 'v'], 'a place in the history, and no record');
  assert.ok(!JSON.stringify(answer).includes('hidden'));
  // And it is a place to read on from: the next request reaches what the client may read
  sent.length = 0;
  session.receive({ t: 'history', req: 2, all: true, cursor: answer.more });
  store.flush();
  assert.deepEqual(sent.find(m => m.t === 'history').entries.map(e => `${e.c}/${e.id}`), ['tasks/mine']);
});

for (const [kind, makeStorage] of Object.entries(storages)) {
  test(`${kind}: the record as it was at a time is what the store held then, though an edit stamped earlier landed later`, () => {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now, history: true, storage: makeStorage() });
    store.patch({ tasks: { k: { a: 1, b: 1 } } });
    now.advance(1000);
    const early = now();   // an edit made now, offline
    now.advance(1000);
    store.patch({ tasks: { k: { a: 2 } } });
    now.advance(1000);
    // It lands now, with the stamp it was made with
    store.apply({ replica: 'r', seq: 1, ts: early, changes: [{ c: 'tasks', id: 'k', path: ['b'], set: 2 }] }, { user: { id: 'u' } });
    assert.deepEqual(store.state.tasks.k, { a: 2, b: 2 });

    const [landed, patched] = store.historyOf('tasks', 'k');
    assert.deepEqual([landed.ts, landed.at, patched.ts, patched.at], [early, 1_003_000, 1_002_000, 1_002_000], 'an entry says when the write was made and when it landed');
    assert.deepEqual(store.recordAt('tasks', 'k', 1_001_500), { a: 1, b: 1 }, 'before either write had landed');
    assert.deepEqual(store.recordAt('tasks', 'k', 1_002_500), { a: 2, b: 1 }, 'the offline edit had not landed yet, whatever its stamp');
    assert.deepEqual(store.recordAt('tasks', 'k', 1_003_500), { a: 2, b: 2 });
    assert.deepEqual(store.historyOf('tasks', 'k', { since: 1_002_500 }).map(e => e.record), [{ a: 2, b: 2 }], 'since and until are of when it landed too');
    assert.deepEqual(store.activity({ until: 1_002_500, collection: 'tasks' }).map(e => e.record), [{ a: 2, b: 1 }, { a: 1, b: 1 }]);
    store.revert('tasks', 'k', 1_002_500);
    assert.deepEqual(store.state.tasks.k, { a: 2, b: 1 });
    store.dispose();
  });

  test(`${kind}: history turned off for a while and on again is brought up to the records: what was written and deleted meanwhile gets a row`, () => {
    const now = fakeTime(1_000_000);
    const storage = makeStorage();
    let store = createStore({ ...SHAPE, now, history: true, storage });
    store.patch({ tasks: { k: { title: 'one' }, same: { title: 'untouched' }, doomed: { title: 'to go' } } });
    store.dispose();
    now.advance(1000);
    store = createStore({ ...SHAPE, now, storage });   // without history
    store.patch({ tasks: { k: { title: 'two' }, fresh: { title: 'new' }, doomed: null } });
    store.dispose();
    now.advance(1000);
    store = createStore({ ...SHAPE, now, history: true, storage });
    assert.deepEqual(store.recordAt('tasks', 'k'), { title: 'two' }, 'as it is, not as the history last knew it');
    assert.deepEqual(store.recordAt('tasks', 'fresh'), { title: 'new' });
    assert.equal(store.recordAt('tasks', 'doomed'), undefined);
    assert.deepEqual(store.historyOf('tasks', 'k').map(e => [e.record.title, e.changes.length]), [['two', 0], ['one', 1]], 'a row as it stands, with no changes, over what it knew');
    assert.deepEqual(store.historyOf('tasks', 'doomed').map(e => e.record), [undefined, { title: 'to go' }]);
    assert.equal(store.historyOf('tasks', 'same').length, 1, 'what did not change gets none');
    assert.deepEqual(store.recordAt('tasks', 'k', 1_000_500), { title: 'one' }, 'and what it knew from before still stands');
    assert.deepEqual(store.activity({ deleted: true }).map(e => e.id), ['doomed']);
    const rows = storage.historyCount();
    store.dispose();
    store = createStore({ ...SHAPE, now, history: true, storage });
    assert.equal(storage.historyCount(), rows, 'loaded again with nothing unrecorded, nothing is added');
    store.dispose();
  });
}

test('before what history.keep has kept, a record with no row is not known to have been absent: its revert is refused', async () => {
  const now = fakeTime(100 * DAY);
  const store = createStore({ ...SHAPE, now, history: { keep: 10 * DAY, clients: true } });
  store.patch({ tasks: { k: { n: 1 } } });
  now.advance(DAY);
  store.patch({ tasks: { k: { n: 2 } } });
  now.advance(30 * DAY);
  store.patch({ tasks: { k: { n: 3 }, later: { n: 1 } } });   // compacts: of k, the last form before what is kept stays
  assert.deepEqual(store.recordAt('tasks', 'k', 105 * DAY), { n: 2 }, 'a record that has a row by then is told');
  assert.throws(() => store.revert('tasks', 'later', 105 * DAY), /Nothing is known/, 'one that has none may have had rows that are forgotten');
  assert.throws(() => store.revert('tasks', 'later', { v: 1 }), /Nothing is known/);
  assert.deepEqual(store.state.tasks.later, { n: 1 });
  now.advance(DAY);
  store.patch({ tasks: { newest: { n: 1 } } });
  store.revert('tasks', 'newest', now() - 1000);
  assert.equal(store.state.tasks.newest, undefined, 'within what is kept, no row is not there');
});

test('the app\'s history.clients is asked of records the store could hold, never of what a client made up; one that throws lets nobody in', () => {
  const faults = [];
  const seen = [];
  const store = createStore({
    ...SHAPE,
    onError: err => faults.push(err.message),
    history: { clients: (user, { c, id }) => { seen.push([c, id]); return id === undefined || id.startsWith('t'); } }
  });
  store.patch({ tasks: { t1: { n: 1 } } });
  const sent = [];
  const session = store.session({ send: message => sent.push(message) });
  session.receive({ t: 'hello', proto: 3, replica: 'x', ops: [] });
  store.flush();
  const ask = query => { sent.length = 0; session.receive({ t: 'history', req: 1, ...query }); store.flush(); return sent[0]; };
  for (const id of [5, {}, null, undefined, '', ['t1']]) assert.equal(ask({ c: 'tasks', id }).code, 'invalid', JSON.stringify(id));
  for (const c of [5, 'nope', '__proto__', undefined]) assert.equal(ask({ c, id: 't1' }).code, 'invalid', String(c));
  assert.deepEqual(seen, [], 'the function was not asked');
  assert.deepEqual(faults, []);
  assert.equal(ask({ c: 'tasks', id: 't1' }).entries.length, 1);
  assert.equal(ask({ c: 'settings', id: 'ignored for a singleton' }).entries.length, 1);

  const thrower = createStore({ ...SHAPE, onError: err => faults.push(err.message), history: { clients: () => { throw new Error('the app\'s own bug'); } } });
  const other = thrower.session({ send: message => sent.push(message) });
  other.receive({ t: 'hello', proto: 3, replica: 'x', ops: [] });
  thrower.flush();
  sent.length = 0;
  other.receive({ t: 'history', req: 2, c: 'tasks', id: 't1' });
  thrower.flush();
  assert.equal(sent[0].code, 'forbidden');
  assert.deepEqual(faults, ['the app\'s own bug'], 'reported, as the fault of the app\'s it is');
});

test('a server from before history refuses the request with no req: the client, and a relay\'s client, hear unavailable at once and no error event', async () => {
  const store = createStore(SHAPE);
  // As a server from before the request answers it: an unknown message type, and no req
  const session = store.session;
  store.session = options => {
    const made = session(options);
    const receive = made.receive;
    made.receive = message => (message.t === 'history' ? options.send({ t: 'error', message: 'Unknown message type: history' }) : receive(message));
    return made;
  };
  const net = createNetwork(store, { vouch: credential => credential });
  const faults = [];
  const relay = createRelay({ shapeOf: () => SHAPE, link: net.link({ user: { id: 'relay' } }).factory, grace: 0, reconnect: false, keepalive: false, onError: err => faults.push(err) });
  const site = createNetwork(relay);
  const direct = net.client({ replicaId: 'a', ...SHAPE });
  const behind = site.client({ replicaId: 'b', ...SHAPE }, { user: { id: 'bob' } });
  const settle = async promise => { for (let i = 0; i < 12; i++) { await net.settle(); await site.settle(); } return promise; };
  await settle();
  for (const db of [direct, behind]) {
    const errors = [];
    db.on('error', err => errors.push(err));
    const err = await settle(refusal(db.historyOf('tasks', 'k')));
    assert.equal(err.code, 'unavailable');
    assert.deepEqual(errors, []);
  }
  assert.deepEqual(faults, [], 'no fault of the relay\'s');
  assert.equal(relay.stats().stores.main.waiting.length, 0);
  relay.close();
});

test('a store that has lost its lease does not prune the history of the store another process serves now', () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-history-'));
  const file = join(dir, 'app.sqlite');
  const faults = [];
  let t = 100 * DAY;
  const mine = sqliteStorage(file, { checkpoints: 'inline', lease: { ttl: 1000 } });
  const store = createStore({ ...SHAPE, now: () => t, history: { keep: 10 * DAY }, storage: mine.store('main'), onError: err => faults.push(err.code ?? err.message) });
  store.patch({ tasks: { k: { n: 1 } } });
  t += DAY;
  store.patch({ tasks: { k: { n: 2 } } });
  store.flush();
  // Another process takes the store: this one stalled past its lease
  const theirs = sqliteStorage(file, { checkpoints: 'inline', lease: { ttl: 1000 } });
  theirs.db.prepare("UPDATE leases SET holder = 'someone-else', until = ? WHERE store = 'main'").run(Date.now() + 60_000);
  const rows = () => theirs.db.prepare("SELECT count(*) AS n FROM history WHERE store = 'main' AND collection = 'tasks'").get().n;
  assert.equal(rows(), 2);
  t += 30 * DAY;
  store.compact();   // by this store's clock both rows are older than it keeps
  assert.equal(rows(), 2, 'nothing of the other process\'s store is forgotten');
  assert.deepEqual(faults, ['lease-lost']);
  theirs.close();
  try { mine.close(); } catch { /* its lease is gone */ }
  rmSync(dir, { recursive: true, force: true });
});
