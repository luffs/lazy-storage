// hostile.test.js - What a client that does not play by the protocol can
// and cannot do to a store: every session is authenticated, but nothing it
// sends is taken on trust
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, memoryStorage } from '../src/server/index.js';
import { PROTOCOL, assertChange, createShape, compile, ModelError } from '../src/core/index.js';
import { setAt } from '../src/core/paths.js';
import { createNetwork, fakeTime } from './helpers.js';

/**
 * A raw session on the store, speaking the protocol by hand. What it heard
 * is read as the turn ends (see groupCommit): the store is flushed first
 */
function rawSession(store, replica = 'r1', user = { id: 'u' }) {
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user });
  session.receive({ t: 'hello', proto: PROTOCOL, replica, ops: [] });
  const heard = () => {
    store.flush();
    return sent;
  };
  const op = (seq, changes, ts = store.now(), as = replica) => session.receive({ t: 'op', op: { replica: as, seq, ts, changes } });
  return { session, heard, op, last: () => heard().at(-1) };
}

const own = (key, value, base = {}) => Object.defineProperty(base, key, { value, enumerable: true, writable: true, configurable: true });

test('a reserved name as a collection, an id, a path segment or a key in a value is refused, and never reaches Object.prototype', () => {
  const store = createStore({ initial: { tasks: {}, settings: {} }, singletons: ['settings'], registers: ['tasks/*/meta'] });
  const { op, last } = rawSession(store);
  const hostile = [
    [{ c: '__proto__', id: 'x', put: { polluted: 'yes' } }],
    [{ c: 'tasks', id: '__proto__', put: { polluted: 'yes' } }],
    [{ c: 'tasks', id: 'x', path: ['constructor', 'prototype'], set: 'yes' }],
    [{ c: 'tasks', id: 'x', put: own('__proto__', { polluted: 'yes' }) }],
    [{ c: 'tasks', id: 'x', path: ['meta'], set: own('__proto__', { polluted: 'yes' }) }],
    [{ c: 'settings', path: ['tags'], set: [own('__proto__', { polluted: 'yes' })] }],
    [{ c: 'tasks', id: 'x', put: { $length: 3 } }]
  ];
  hostile.forEach((changes, i) => {
    op(i + 1, JSON.parse(JSON.stringify(changes)));   // as the wire hands it over
    assert.equal(last().t, 'error', JSON.stringify(changes));
    assert.equal(last().code, 'invalid', JSON.stringify(changes));
  });
  assert.equal(({}).polluted, undefined);
  assert.deepEqual(store.state, { tasks: {}, settings: {} });
  store.dispose();
});

test('the shape and the path helpers refuse the prototype machinery, and the compiler never passes it on', () => {
  const shape = createShape({ initial: { tasks: {} } });
  assert.throws(() => assertChange(shape, { c: 'tasks', id: 'x', path: ['__proto__'], set: 1 }), ModelError);
  assert.throws(() => assertChange(shape, { c: 'tasks', id: 'x', put: JSON.parse('{"a":{"__proto__":{"b":1}}}') }), ModelError);
  // A batch is compiled from the state it left, where no such key can be: it never reaches a change
  const compiled = compile(shape, JSON.parse('{"tasks":{"x":{"a":{"__proto__":{"b":1}}}}}'), {}, { tasks: { x: {} } });
  assert.ok(!JSON.stringify(compiled).includes('__proto__'), JSON.stringify(compiled));
  assert.throws(() => setAt({}, ['__proto__', 'polluted'], 'yes'), TypeError);
  assert.equal(({}).polluted, undefined);
});

test('a change of the wrong shape is refused whole, and nothing of its op lands', () => {
  const store = createStore({ initial: { tasks: {}, settings: {} }, singletons: ['settings'] });
  const { op, last } = rawSession(store);
  const wrong = [
    { c: 'nope', id: 'x', put: {} },
    { c: 'tasks', put: { title: 'no id' } },
    { c: 'settings', id: 'x', put: {} },
    { c: 'tasks', id: 'x', put: { a: 1 }, set: 2 },
    { c: 'tasks', id: 'x', set: 'a record that is not an object' },
    { c: 'tasks', id: 'x', path: 'title', set: 'a path that is not an array' }
  ];
  wrong.forEach((change, i) => {
    op(i + 1, [{ c: 'tasks', id: 'fine', put: { title: 'would land' } }, change]);
    assert.equal(last().code, 'invalid', JSON.stringify(change));
  });
  assert.deepEqual(store.state.tasks, {});
  store.dispose();
});

test('a session speaks for one replica: another id, the server\'s, or a live user\'s is refused', () => {
  const store = createStore({ initial: { tasks: {} } });
  const { session, op, last } = rawSession(store, 'mine');

  op(1e12, [], store.now(), 'server');
  assert.equal(last().code, 'forbidden', 'the server\'s id is not the session\'s');
  store.patch({ tasks: { a: { title: 'server' } } });
  assert.equal(store.state.tasks.a.title, 'server', 'the server still writes');

  op(1, [{ c: 'tasks', id: 'b', put: {} }], store.now(), 'other');
  assert.equal(last().code, 'forbidden');
  session.receive({ t: 'hello', proto: PROTOCOL, replica: 'other', ops: [] });
  assert.equal(last().code, 'forbidden', 'a session keeps the replica it first spoke for');

  // A victim's replica id, as presence shows it, belongs to the victim: it cannot be borrowed
  const victim = rawSession(store, 'victim', { id: 'ann' });
  const thief = rawSession(store, 'victim', { id: 'mallory' });
  assert.deepEqual([thief.last().t, thief.last().code], ['closed', 'replica-taken']);
  const heard = thief.heard().length;
  thief.op(1e12, []);
  assert.equal(thief.heard().length, heard, 'the refused session hears nothing more');
  const other = rawSession(store, 'm2', { id: 'mallory' });
  other.op(1e12, [], store.now(), 'victim');
  assert.equal(other.last().code, 'forbidden', 'nor through another session of its own');
  victim.op(1, [{ c: 'tasks', id: 'v', put: { title: 'mine' } }]);
  assert.equal(victim.last().t, 'ack');
  assert.equal(store.state.tasks.v.title, 'mine');

  // The same user on a second device is fine; nobody says hello as the server
  assert.equal(rawSession(store, 'victim', { id: 'ann' }).last().t, 'snapshot');
  assert.equal(rawSession(store, 'server').last().code, 'forbidden');
  store.dispose();
});

test('a timestamp far ahead is clamped to the server\'s time: it wins nothing over an edit made after it', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ initial: { notes: {} }, now });
  const { op, last } = rawSession(store, 'mallory', { id: 'mallory' });
  op(1, [{ c: 'notes', id: 'n', put: { text: 'from the future' } }], now() + 365 * 86_400_000);
  assert.equal(last().t, 'ack', 'taken, at the server\'s time');
  assert.equal(store.clock('notes', 'n').live['["text"]'], now(), 'stamped no later than it arrived');
  op(2, [{ c: 'notes', id: 'n', put: { text: 'later still' } }], Number.MAX_SAFE_INTEGER);
  assert.equal(last().t, 'ack');

  const net = createNetwork(store);
  const honest = net.client({ replicaId: 'ann', initial: { notes: {} }, now }, { user: { id: 'ann' } });
  await net.settle();
  now.advance(1);
  honest.state.notes.n.text = 'an honest edit, a moment later';
  await net.settle();
  assert.equal(store.state.notes.n.text, 'an honest edit, a moment later');
  assert.ok(store.now() < 2_000_000, 'and the server\'s clock was not carried off');
  store.dispose();
});

test('an op that is not a timestamp, a seq or a list of changes is refused, and one too old is expired', () => {
  const now = fakeTime(100 * 86_400_000);
  const store = createStore({ initial: { notes: {} }, now });
  const { session, op, last } = rawSession(store);
  for (const ts of [-5, 1.5, 'yesterday', null, [now(), 0, 'r1']]) {
    op(1, [], ts);
    assert.equal(last().code, 'invalid', JSON.stringify(ts));
  }
  for (const seq of [0, -1, 1.5, '3']) {
    op(seq, []);
    assert.equal(last().code, 'invalid', JSON.stringify(seq));
  }
  session.receive({ t: 'op', op: { replica: 'r1', seq: 1, ts: now(), changes: 'not changes' } });
  assert.equal(last().code, 'invalid');
  op(1, [{ c: 'notes', id: 'n', put: { a: 1 } }], now() - 60 * 86_400_000);
  assert.equal(last().code, 'expired');
  assert.deepEqual(store.state.notes, {});
  store.dispose();
});

test('a client cannot delete a collection or a singleton, only what is in them', () => {
  const store = createStore({ initial: { tasks: {}, settings: { theme: 'light' } }, singletons: ['settings'] });
  store.patch({ tasks: { a: { title: 'a' } } });
  const { op, last } = rawSession(store, 'r1');
  op(1, [{ c: 'tasks', del: true }]);
  assert.equal(last().code, 'invalid');
  op(2, [{ c: 'settings', del: true }]);
  assert.equal(last().code, 'invalid');
  op(3, [{ c: 'tasks', id: 'b', put: { title: 'b' } }]);
  assert.equal(last().t, 'ack', 'writes in it still land');
  op(4, [{ c: 'tasks', id: 'a', del: true }, { c: 'tasks', id: 'b', del: true }, { c: 'settings', path: ['theme'], del: true }]);
  assert.deepEqual(store.state, { tasks: {}, settings: {} }, 'emptied, not deleted');
  assert.throws(() => store.patch({ settings: null }), TypeError, 'nor does the server delete a singleton');
  store.dispose();
});

test('a follower\'s hello earns nothing by itself: an op or a share as a client nobody vouched for is refused', () => {
  const store = createStore({ initial: { tasks: {} } });
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user: { id: 'mallory' } });
  session.receive({ t: 'hello', proto: PROTOCOL, replica: 'm', ops: [], follower: true });
  session.receive({ t: 'op', as: 'ann', op: { replica: 'ann', seq: 1, ts: store.now(), changes: [{ c: 'tasks', id: 'x', put: { title: 'as ann' } }] } });
  session.receive({ t: 'share', as: 'ann', data: { cursor: 1 } });
  session.receive({ t: 'share', data: { cursor: 1 } });
  store.flush();
  const errors = sent.filter(m => m.t === 'error');
  assert.deepEqual(errors.map(m => m.code), ['forbidden', 'forbidden', 'forbidden']);
  assert.deepEqual(store.state.tasks, {});
  assert.deepEqual(store.peers(), [], 'and it is nobody\'s peer');
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
  back.op(1, [{ c: 'tasks', id: 'a', put: { title: 'a' } }]);
  assert.equal(back.last().t, 'ack', 'the owner carries on');
  assert.equal(rawSession(again, 'anon', undefined).last().t, 'snapshot', 'a session without a user owns nothing and is not held back');
  again.dispose();
});

test('a hello of another protocol version is answered closed, code outdated, and the session hears nothing more', () => {
  const store = createStore({ initial: { tasks: {} } });
  for (const [label, hello] of [['none', { t: 'hello', replica: 'old', ops: [] }], ['older', { t: 'hello', proto: 2, replica: 'old', ops: [] }], ['newer', { t: 'hello', proto: PROTOCOL + 1, replica: 'old', ops: [] }]]) {
    const sent = [];
    const session = store.session({ send: m => sent.push(m), user: { id: 'u' } });
    session.receive(hello);
    session.receive({ t: 'op', op: { replica: 'old', seq: 1, ts: store.now(), changes: [{ c: 'tasks', id: 'x', put: {} }] } });
    store.flush();
    assert.equal(sent.length, 1, `${label}: one answer`);
    assert.equal(sent[0].t, 'closed', label);
    assert.equal(sent[0].code, 'outdated', label);
    assert.match(sent[0].message, new RegExp(`protocol ${PROTOCOL}`), label);
  }
  assert.equal(store.snapshot().tasks.x, undefined, 'nothing of theirs landed');
  assert.equal(store.stats().sessions, 0, 'none of them stayed');
});

test('a client told outdated is closed for good: it reports the code, stays offline, and keeps its outbox for the page that reloads', async () => {
  const store = createStore({ initial: { tasks: {} } });
  // A server of another version, as the client meets one: every hello is read as protocol 2
  const net = createNetwork({ session: ({ send, user }) => {
    const session = store.session({ send, user });
    return { ...session, receive: m => session.receive(m.t === 'hello' ? { ...m, proto: 2 } : m) };
  } });
  const client = net.client({ replicaId: 'c', initial: { tasks: {} } });
  const closed = [];
  client.on('closed', info => closed.push(info));
  client.state.tasks.x = { title: 'x' };
  await net.settle();
  assert.equal(closed.length, 1);
  assert.equal(closed[0].code, 'outdated');
  assert.equal(client.closed.code, 'outdated');
  assert.equal(client.status, 'offline');
  assert.equal(client.pending, 1, "the edit waits for a client that speaks the server's protocol");
  assert.equal(store.snapshot().tasks.x, undefined);
});
