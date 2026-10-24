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
