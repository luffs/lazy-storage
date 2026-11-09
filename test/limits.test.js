// limits.test.js - What a public server refuses: too many changes, too fast, too many ops in one hello
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { PROTOCOL } from '../src/core/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {} } };

test('an op of more changes than maxChanges is refused with too-large; the client takes it back', async () => {
  const store = createStore({ ...SHAPE, maxChanges: 3 });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  const rejected = [];
  a.on('rejected', e => rejected.push(e.code));
  for (let i = 0; i < 5; i++) a.state.tasks[`t${i}`] = { n: i };
  await net.settle();
  assert.deepEqual(rejected, ['too-large']);
  assert.deepEqual(a.state.tasks, {});
  assert.deepEqual(store.state.tasks, {});
});

test('a hello carries at most 1000 ops; the rest go once it is answered, and every one lands', async () => {
  const store = createStore({ ...SHAPE, rateLimit: false });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  a.link.goOffline();
  await net.settle();
  for (let i = 0; i < 1005; i++) {
    a.state.tasks[`t${i}`] = { n: i };
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(a.pending, 1005);
  let largest = 0;
  const receive = store.session;
  store.session = options => {
    const session = receive(options);
    const inner = session.receive;
    session.receive = message => {
      if (message.t === 'hello') largest = Math.max(largest, message.ops.length);
      inner(message);
    };
    return session;
  };
  a.link.goOnline(); a.connect();
  await net.settle();
  assert.equal(largest, 1000);
  assert.equal(a.pending, 0);
  assert.equal(Object.keys(store.state.tasks).length, 1005);
});

test('the rate limit follows the user, and every live op after a refusal is refused until the next hello', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now, rateLimit: { burst: 3, perSecond: 1 } });
  const sent = [];
  const s = store.session({ send: m => sent.push(m), user: { id: 'u1' } });
  s.receive({ t: 'hello', proto: PROTOCOL, replica: 'r1', ops: [] });   // a hello costs a token
  const op = seq => ({ t: 'op', op: { replica: 'r1', seq, ts: now(), changes: [{ c: 'tasks', id: `t${seq}`, put: { n: seq } }] } });
  s.receive(op(1));
  s.receive(op(2));
  s.receive(op(3));
  now.advance(5000);
  s.receive(op(4));
  store.flush();
  const answers = sent.filter(m => m.t === 'ack' || m.t === 'error').map(m => (m.t === 'ack' ? 'ack' : m.code));
  assert.deepEqual(answers, ['ack', 'ack', 'rate-limited', 'rate-limited'], 'refused until a hello, whatever the bucket holds by then');
  // Another session of the same user draws on the same bucket
  const other = [];
  const s2 = store.session({ send: m => other.push(m), user: { id: 'u1' } });
  s2.receive({ t: 'hello', proto: PROTOCOL, replica: 'r2', ops: [] });
  s2.receive({ t: 'op', op: { replica: 'r2', seq: 1, ts: now(), changes: [{ c: 'tasks', id: 'x', put: {} }] } });
  store.flush();
  assert.ok(other.some(m => m.t === 'ack' || m.code === 'rate-limited'));
});

test('the ops in a hello draw on the bucket too: the first beyond it and those after wait, in order, and are asked back when the bucket holds enough for them', () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now, rateLimit: { burst: 5, perSecond: 1 } });
  const sent = [];
  const s = store.session({ send: m => sent.push(m), user: { id: 'u1' } });
  const op = seq => ({ replica: 'r1', seq, ts: now(), changes: [{ c: 'tasks', id: `t${seq}`, put: { n: seq } }] });
  const hello = seqs => s.receive({ t: 'hello', proto: PROTOCOL, replica: 'r1', ops: seqs.map(op) });
  const range = (from, to) => Array.from({ length: to - from + 1 }, (_, i) => from + i);
  hello(range(1, 12));
  store.flush();
  // The hello's token paid for its first op, four more tokens for the next four
  assert.deepEqual(Object.keys(store.state.tasks), ['t1', 't2', 't3', 't4', 't5']);
  const refused = sent.filter(m => m.t === 'error');
  assert.deepEqual(refused.map(m => [m.seq, m.code]), [[6, 'rate-limited']], 'one refusal, at the first op beyond the bucket: none after it is judged');
  assert.equal(refused[0].retryAfter, 5000, 'until the bucket is full: seven ops are left, and it holds five');
  assert.equal(sent.find(m => m.t === 'snapshot' || m.t === 'delta').seq, 5, 'the answer acknowledges what landed');

  // Asked back at once, the hello itself is refused, with the same wait
  sent.length = 0;
  hello(range(6, 12));
  assert.deepEqual(sent.map(m => [m.t, m.code, m.retryAfter]), [['error', 'rate-limited', 5000]]);

  // After the wait: ops the store has merged already cost nothing again, and the next five land
  now.advance(5000);
  sent.length = 0;
  hello(range(1, 12));
  store.flush();
  assert.equal(Object.keys(store.state.tasks).length, 10);
  assert.deepEqual(sent.filter(m => m.t === 'error').map(m => [m.seq, m.retryAfter]), [[11, 2000]], 'two ops are left');
  now.advance(2000);
  hello(range(11, 12));
  store.flush();
  assert.equal(Object.keys(store.state.tasks).length, 12);
});

test('a bucket of one lands one op a hello: the hello\'s token pays for its first op', () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now, rateLimit: { burst: 1, perSecond: 1 } });
  const sent = [];
  const s = store.session({ send: m => sent.push(m), user: { id: 'u1' } });
  const op = seq => ({ replica: 'r1', seq, ts: now(), changes: [{ c: 'tasks', id: `t${seq}`, put: { n: seq } }] });
  for (let landed = 1; landed <= 3; landed++) {
    sent.length = 0;
    s.receive({ t: 'hello', proto: PROTOCOL, replica: 'r1', ops: [1, 2, 3].map(op) });
    store.flush();
    assert.equal(Object.keys(store.state.tasks).length, landed);
    const refusal = sent.find(m => m.t === 'error');
    if (landed < 3) {
      assert.deepEqual([refusal.seq, refusal.retryAfter], [landed + 1, 1000]);
      now.advance(refusal.retryAfter);
    } else assert.equal(refusal, undefined);
  }
});

test('a client back with more ops than the bucket holds lands them all, a hello at a time, and none is rejected', async () => {
  const store = createStore({ ...SHAPE, rateLimit: { burst: 5, perSecond: 500 } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  await net.settle();
  const rejected = [];
  a.on('rejected', e => rejected.push(e));
  a.on('error', () => {});   // rate-limited, each time the bucket runs out
  a.link.goOffline();
  await net.settle();
  for (let i = 0; i < 30; i++) {
    a.state.tasks[`t${i}`] = { n: i };
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(a.pending, 30);
  a.link.goOnline(); a.connect();
  // The network moves when settled; the client's next hello waits on a timer, `retryAfter` from the refusal
  for (let i = 0; i < 300 && a.pending > 0; i++) {
    await net.settle();
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.equal(a.pending, 0);
  assert.equal(Object.keys(store.state.tasks).length, 30);
  assert.deepEqual(rejected, []);
});
