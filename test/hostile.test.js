// hostile.test.js - What a client that does not play by the protocol can
// and cannot do to a store: every session is authenticated, but nothing it
// sends is taken on trust
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { leaves, rebuild, ModelError } from '../src/core/model.js';
import { registerSet, setAt } from '../src/core/paths.js';

/** A raw session on the store, speaking the protocol by hand */
function rawSession(store, replicaId = 'r1', user = { id: 'u' }) {
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user });
  session.receive({ t: 'hello', replicaId, ops: [] });
  return { session, sent, last: () => sent.at(-1) };
}

test('a reserved name in an op is refused and never reaches Object.prototype', () => {
  const store = createStore({ initial: { tasks: {} }, registers: ['meta'] });
  const { session, last } = rawSession(store);
  const hostile = [
    '{"__proto__":{"polluted":"yes"}}',
    '{"tasks":{"__proto__":{"polluted":"yes"}}}',
    '{"tasks":{"x":{"constructor":{"prototype":{"polluted":"yes"}}}}}',
    '{"meta":{"__proto__":{"polluted":"yes"}}}',
    '{"tasks":{"x":{"$length":3}}}'
  ];
  hostile.forEach((json, i) => {
    session.receive({ t: 'op', op: { replicaId: 'r1', seq: i + 1, ts: [Date.now(), 0, 'r1'], diff: JSON.parse(json) } });
    assert.equal(last().t, 'error', json);
    assert.equal(last().code, 'invalid', json);
  });
  assert.equal(({}).polluted, undefined);
  assert.deepEqual(store.state, { tasks: {} });
  store.dispose();
});

test('leaves, setAt and rebuild refuse the prototype machinery as a path segment', () => {
  const NONE = registerSet([]);
  assert.throws(() => leaves(JSON.parse('{"a":{"__proto__":{"b":1}}}'), NONE), ModelError);
  assert.throws(() => leaves({ a: { prototype: 1 } }, NONE), ModelError);
  assert.throws(() => leaves({ tags: JSON.parse('[{"__proto__":{"b":1}}]') }, registerSet(['tags'])), ModelError);
  assert.throws(() => setAt({}, ['__proto__', 'polluted'], 'yes'), TypeError);
  assert.throws(() => rebuild({}, [['["__proto__","polluted"]', 'yes']]), TypeError);
  assert.equal(({}).polluted, undefined);
});

test('a session speaks for one replica: another id, the server\'s, or a live user\'s is refused', () => {
  const store = createStore({ initial: { tasks: {} }, readOnly: ['locked'] });
  const { session, last } = rawSession(store, 'mine');
  const ts = id => [Date.now(), 0, id];

  session.receive({ t: 'op', op: { replicaId: 'server', seq: 1e12, ts: ts('server'), diff: {} } });
  assert.equal(last().code, 'forbidden', 'the server\'s id is not the session\'s');
  assert.equal(store.patch({ tasks: { a: { id: 'a' } } }).duplicate, false, 'the server still writes');
  assert.deepEqual(store.state, { tasks: { a: { id: 'a' } } });

  session.receive({ t: 'op', op: { replicaId: 'other', seq: 1, ts: ts('other'), diff: { tasks: { b: { id: 'b' } } } } });
  assert.equal(last().code, 'forbidden');
  session.receive({ t: 'op', op: { replicaId: 'mine', seq: 1, ts: ts('someone-else'), diff: { tasks: { b: { id: 'b' } } } } });
  assert.equal(last().code, 'invalid', 'the stamp names the op\'s own replica');
  session.receive({ t: 'hello', replicaId: 'other', ops: [] });
  assert.equal(last().code, 'forbidden', 'a session keeps the replica it first spoke for');

  // A victim's replica id, as presence shows it, belongs to the victim: it cannot be borrowed
  const victim = rawSession(store, 'victim', { id: 'ann' });
  const thief = rawSession(store, 'victim', { id: 'mallory' });
  assert.deepEqual([thief.last().t, thief.last().code], ['closed', 'replica-taken']);
  const heard = thief.sent.length;
  thief.session.receive({ t: 'op', op: { replicaId: 'victim', seq: 1e12, ts: ts('victim'), diff: {} } });
  assert.equal(thief.sent.length, heard, 'the refused session hears nothing more');
  const other = rawSession(store, 'm2', { id: 'mallory' });
  other.session.receive({ t: 'op', op: { replicaId: 'victim', seq: 1e12, ts: ts('victim'), diff: {} } });
  assert.equal(other.last().code, 'forbidden', 'nor through another session of its own');
  victim.session.receive({ t: 'op', op: { replicaId: 'victim', seq: 1, ts: ts('victim'), diff: { tasks: { v: { id: 'v' } } } } });
  assert.equal(victim.last().t, 'ack');
  assert.equal(store.state.tasks.v.id, 'v');

  // The same user on a second device, or a hello with no session yet, is fine
  const again = rawSession(store, 'victim', { id: 'ann' });
  assert.equal(again.last().t, 'snapshot');
  assert.equal(rawSession(store, 'server').last().code, 'forbidden', 'nobody says hello as the server');
  store.dispose();
});

test('a timestamp counter near 2^53 is refused and cannot freeze the server clock', () => {
  const store = createStore({ initial: { tasks: {} } });
  const { session, last } = rawSession(store, 'r1');
  session.receive({ t: 'op', op: { replicaId: 'r1', seq: 1, ts: [Date.now() + 1000, Number.MAX_SAFE_INTEGER, 'r1'], diff: { n: 1 } } });
  assert.equal(last().code, 'invalid');
  store.patch({ n: 1 });
  assert.equal(store.patch({ n: 2 }).accepted?.n, 2, 'the server\'s own writes keep landing');
  store.dispose();
});

test('a client cannot delete a top-level container of initial; the server can, and clearing it is fine', () => {
  const store = createStore({ initial: { tasks: {}, order: [] } });
  store.patch({ tasks: { a: { id: 'a' } } });
  const { session, last } = rawSession(store, 'r1');
  const op = (seq, diff) => session.receive({ t: 'op', op: { replicaId: 'r1', seq, ts: [Date.now(), seq, 'r1'], diff } });
  op(1, { tasks: null });
  assert.equal(last().code, 'forbidden');
  op(2, { tasks: { b: { id: 'b' } } });
  assert.equal(last().t, 'ack', 'writes under it still land');
  op(3, { tasks: { a: null, b: null } });
  assert.deepEqual(store.state.tasks, {}, 'emptied, not deleted');
  op(4, { order: null });
  assert.equal(last().t, 'ack', 'an array of initial is a leaf, and may go');
  store.dispose();
});

test('a replica belongs to the user who first spoke for it, while it is away and after a restart too', () => {
  const storage = memoryStorage();
  const store = createStore({ initial: { tasks: {} }, storage });
  const victim = rawSession(store, 'victim', { id: 'ann' });
  victim.session.close();                    // offline: no live session holds the id
  const thief = rawSession(store, 'victim', { id: 'mallory' });
  assert.equal(thief.last().code, 'replica-taken', 'refused while the victim is away');
  store.dispose();

  const again = createStore({ initial: { tasks: {} }, storage });
  assert.equal(rawSession(again, 'victim', { id: 'mallory' }).last().code, 'replica-taken', 'and after a restart');
  const back = rawSession(again, 'victim', { id: 'ann' });
  back.session.receive({ t: 'op', op: { replicaId: 'victim', seq: 1, ts: [Date.now(), 0, 'victim'], diff: { tasks: { a: { id: 'a' } } } } });
  assert.equal(back.last().t, 'ack', 'the owner carries on');
  assert.equal(rawSession(again, 'anon', undefined).last().t, 'snapshot', 'a session without a user owns nothing and is not held back');
  again.dispose();
});
