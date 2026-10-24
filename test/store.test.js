// store.test.js - A store's edges, driven by hand: what patch, del and observe
// refuse, a whole op judged by validate, presence that refuses a share, a
// follower's grants, a commit that fails, a migration that throws, and
// replicas the retention window forgets
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { PROTOCOL } from '../src/core/index.js';
import { fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: {} }, singletons: ['settings'] };

function raw(store, replica = 'r1', user = { id: 'u' }, hello = {}) {
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user, follower: hello.follower === true });
  session.receive({ t: 'hello', proto: PROTOCOL, replica, ops: [], ...hello });
  const heard = () => { store.flush(); return sent; };
  const op = (seq, changes) => session.receive({ t: 'op', op: { replica, seq, ts: store.now(), changes } });
  return { session, heard, op, last: () => heard().at(-1) };
}

test('patch and del write as the server, and refuse what is not the store\'s', () => {
  const store = createStore({ ...SHAPE, parents: {} });
  store.patch({ tasks: { a: { title: 'x', meta: { color: 'red' } } }, settings: { theme: 'dark' } });
  store.del('tasks', 'a', ['meta', 'color']);
  assert.deepEqual(store.state.tasks.a, { title: 'x', meta: {} });
  store.del('settings', ['theme']);
  assert.deepEqual(store.state.settings, {});
  store.del('tasks', 'a');
  assert.equal(store.state.tasks.a, undefined);
  assert.throws(() => store.patch('x'), TypeError);
  assert.throws(() => store.patch({ nope: {} }), TypeError);
  assert.throws(() => store.patch({ tasks: 'not records' }), TypeError);
  assert.throws(() => store.patch({ settings: null }), TypeError);
  assert.throws(() => store.observe('nope', () => {}), TypeError);
  assert.throws(() => store.observe('op', 'not a function'), TypeError);
  assert.throws(() => store.session({}), TypeError);
  store.dispose();
  store.dispose();
  assert.throws(() => store.patch({ tasks: {} }), /dispos|unloaded/i);
});

test('validate judges the whole op: false or a throw refuses it, with the throw\'s message', () => {
  const refused = [];
  const store = createStore({
    ...SHAPE,
    validate: (diff, { changes }) => {
      if (diff.tasks?.bad) throw new Error('no bad tasks');
      return changes.length <= 2;
    }
  });
  store.observe('refused', e => refused.push(e.code));
  const { op, last } = raw(store);
  op(1, [{ c: 'tasks', id: 'bad', put: {} }]);
  assert.deepEqual([last().code, last().message], ['forbidden', 'no bad tasks']);
  op(2, [{ c: 'tasks', id: 'a', put: {} }, { c: 'tasks', id: 'b', put: {} }, { c: 'tasks', id: 'c', put: {} }]);
  assert.equal(last().code, 'forbidden');
  op(3, [{ c: 'tasks', id: 'a', put: {} }]);
  assert.equal(last().t, 'ack');
  assert.deepEqual(refused, ['forbidden', 'forbidden']);
});

test('a session hears why a message is no message, and pings are answered', () => {
  const store = createStore(SHAPE);
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user: { id: 'u' } });
  session.receive('not an object');
  session.receive({ t: 'ping' });
  session.receive({ t: 'hello', proto: PROTOCOL, replica: 'r', ops: [] });
  session.receive({ t: 'op', op: { replica: '', seq: 1, ts: store.now(), changes: [] } });
  session.receive({ t: 'what' });
  session.receive({ t: 'op', as: 'nobody', op: {} });
  session.receive({ t: 'leave', as: 'nobody' });
  store.flush();
  assert.equal(sent[0].message, 'Expected a message object');
  assert.equal(sent[1].t, 'pong');
  const errors = sent.filter(m => m.t === 'error');
  assert.ok(errors.some(m => m.code === 'invalid'));
  assert.ok(errors.some(m => /Unknown message type/.test(m.message)));
  assert.ok(errors.some(m => m.as === 'nobody' && m.code === 'forbidden'));
});

test('presence.validate refuses a share by false, by a throw, or by a value that is not JSON', () => {
  const store = createStore({
    ...SHAPE,
    presence: { validate: data => (data === 'throw' ? (() => { throw new Error('not that'); })() : data === 'fn' ? () => {} : data !== 'no') }
  });
  const { session, heard } = raw(store);
  for (const data of ['no', 'throw', 'fn']) session.receive({ t: 'share', data });
  const errors = heard().filter(m => m.t === 'error');
  assert.deepEqual(errors.map(m => m.code), ['forbidden', 'forbidden', 'forbidden']);
  assert.equal(errors[1].message, 'not that');
});

test('a follower\'s grants: only a follower holds them, each names its replica, and revoke tells why', () => {
  const store = createStore({ ...SHAPE, presence: true });
  const plain = raw(store, 'p');
  assert.equal(plain.session.grant('x', { id: 'a' }, 'ra').code, 'forbidden');
  assert.equal(plain.session.revoke(() => true, 'forbidden', 'no'), 0);
  const follower = raw(store, 'relay', { id: 'relay' }, { follower: true });
  assert.equal(follower.session.grant('x', { id: 'a' }, '').code, 'invalid');
  assert.equal(follower.session.grant('x', { id: 'a' }, 'p').code, 'replica-taken', 'another user\'s replica');
  assert.equal(follower.session.grant('x', { id: 'a' }, 'ra'), null);
  assert.equal(follower.session.grant('x', { id: 'a' }, 'ra'), null, 'again, the same');
  assert.deepEqual(store.peers().map(p => p.replicaId).sort(), ['p', 'ra']);
  follower.session.receive({ t: 'op', as: 'x', op: { replica: 'ra', seq: 1, ts: store.now(), changes: [{ c: 'tasks', id: 'k', put: { by: 'a' } }] } });
  assert.equal(follower.heard().at(-1).t, 'ack');
  assert.equal(follower.heard().at(-1).as, 'x');
  assert.equal(store.state.tasks.k.by, 'a');
  assert.equal(follower.session.revoke(user => user.id === 'a', 'forbidden', 'left'), 1);
  assert.deepEqual(follower.heard().at(-1), { t: 'revoked', as: 'x', code: 'forbidden', message: 'left' });
  follower.session.ungrant('x');
  assert.deepEqual(store.peers().map(p => p.replicaId), ['p']);
});

test('a commit that fails unloads the store: flush says so, and the sessions are told to say hello again', () => {
  const errors = [];
  const storage = memoryStorage();
  let failing = false;
  const commit = storage.commit;
  storage.commit = change => { if (failing) throw new Error('disk full'); commit(change); };
  delete storage.commitMany;
  const store = createStore({ ...SHAPE, storage, onError: err => errors.push(err.message) });
  const { heard } = raw(store);
  failing = true;
  store.patch({ tasks: { a: { title: 'lost' } } });
  assert.throws(() => store.flush(), err => err.code === 'unavailable');
  assert.ok(errors.includes('disk full'));
  assert.equal(heard().at(-1).code, 'unavailable');
  assert.equal(store.disposed, true);
});

test('a migration that throws stops the store from loading, and says which', () => {
  const storage = memoryStorage();
  storage.replace({ records: [], replicas: {}, version: 0, epoch: 'e', schema: 0 });
  assert.throws(() => createStore({ ...SHAPE, storage, migrations: [() => ({ settings: { a: 1 } }), () => { throw new Error('bad data'); }] }), /Migration 1 failed: bad data/);
  const store = createStore({ ...SHAPE, storage, migrations: [() => ({ settings: { a: 1 } }), () => {}] });
  assert.equal(store.stats().schema, 2, 'the first stayed done, the second ran');
  assert.deepEqual(store.state.settings, { a: 1 });
});

test('replicas not heard from within the retention window are forgotten, and their old ops expire', () => {
  const now = fakeTime(1_000_000_000);
  const store = createStore({ ...SHAPE, now, retention: 10 * 86_400_000 });
  const old = raw(store, 'old');
  old.op(1, [{ c: 'tasks', id: 'a', put: { x: 1 } }, { c: 'tasks', id: 'a', path: ['x'], del: true }]);
  assert.equal(store.stats().replicas >= 1, true);
  old.session.close();
  now.advance(11 * 86_400_000);
  const forgot = store.compact();
  assert.equal(forgot.replicas >= 1, true);
  assert.equal(forgot.deletions, 1, 'and the deletion inside a record');
  const again = raw(store, 'old');
  again.session.receive({ t: 'op', op: { replica: 'old', seq: 1, ts: now() - 11 * 86_400_000, changes: [] } });
  assert.equal(again.last().code, 'expired', 'an op it may have sent before is refused, not applied twice');
});

test('a follower\'s client held back by the rate limit is let go once its time is up, though it says no hello of its own', () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, rateLimit: { burst: 1, perSecond: 1 } });
  const follower = raw(store, 'relay', { id: 'relay' }, { follower: true });
  assert.equal(follower.session.grant('x', { id: 'a' }, 'ra'), null);
  const op = seq => follower.session.receive({ t: 'op', as: 'x', op: { replica: 'ra', seq, ts: now(), changes: [{ c: 'tasks', id: `k${seq}`, put: { n: seq } }] } });
  op(1);
  assert.equal(follower.last().t, 'ack');
  op(2);
  assert.equal(follower.last().code, 'rate-limited');
  const { retryAfter } = follower.last();
  now.advance(retryAfter);
  op(2);
  assert.equal(follower.last().t, 'ack', 'its wait is over');
  assert.equal(store.state.tasks.k2.n, 2);
});
