// limits.test.js - Ops that are too big or too many, and a hello that stays small
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const START = 1_000_000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const tick = () => new Promise(resolve => setImmediate(resolve));

/** A store whose sessions record what they receive and answer */
function setup(options = {}) {
  const store = createStore({ initial: INITIAL, ...options });
  const received = [];
  const session = store.session;
  store.session = opts => {
    const s = session(opts);
    const receive = s.receive;
    s.receive = message => { received.push(message); return receive(message); };
    return s;
  };
  return { store, net: createNetwork(store), received };
}

test('an op touching more leaves than maxLeaves is refused with too-large; the client drops it and resyncs', async () => {
  const { store, net } = setup({ maxLeaves: 5 });
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push([err.code, err.message]));

  a.collection('tasks').add({ id: 'big', a: 1, b: 2, c: 3, d: 4, e: 5 });   // six leaves with the id
  await net.settle();
  assert.deepEqual(errors, [['too-large', 'The op touches 6 leaves; the store accepts at most 5 in one op']]);
  assert.equal(store.snapshot().tasks.big, undefined);
  assert.equal(a.state.tasks.big, undefined, 'rolled back by the snapshot');
  assert.equal(a.pending, 0);

  a.collection('tasks').add({ id: 'small', a: 1 });
  await net.settle();
  assert.equal(store.snapshot().tasks.small.a, 1);
});

test('a replica past its rate limit is refused without losing anything: the client resends its outbox after retryAfter', async () => {
  const time = fakeTime(START);
  const { store, net } = setup({ now: time, rateLimit: { burst: 3, perSecond: 10 } });
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: fakeTime(START) });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now: fakeTime(START) });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push([err.code, err.message]));

  for (const id of ['t1', 't2', 't3', 't4', 't5']) {
    a.collection('tasks').add({ id });
    await net.settle();
  }
  assert.deepEqual(Object.keys(store.snapshot().tasks), ['t1', 't2', 't3'], 'the bucket held three');
  assert.deepEqual(errors, [['rate-limited', 'Too many ops; try again in 100 ms'], ['rate-limited', 'Too many ops; try again in 100 ms']]);
  assert.equal(a.pending, 2, 'the refused ops stay in the outbox');
  assert.deepEqual(Object.keys(a.state.tasks), ['t1', 't2', 't3', 't4', 't5'], 'and in the local state');
  assert.equal(a.status, 'online');

  b.collection('tasks').add({ id: 'from-b' });
  await net.settle();
  time.advance(1000);
  await sleep(150);          // the client's retry timer runs on real time
  await net.settle();
  assert.equal(a.pending, 0);
  assert.deepEqual(Object.keys(store.snapshot().tasks).sort(), ['from-b', 't1', 't2', 't3', 't4', 't5']);
  assert.deepEqual(Object.keys(a.state.tasks).sort(), ['from-b', 't1', 't2', 't3', 't4', 't5']);
  assert.equal(errors.length, 2, 'the hello that resent them was not throttled');
});

test('rateLimit: false turns the limit off; a bad rateLimit is refused up front', async () => {
  const { store, net } = setup({ rateLimit: false });
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  await net.settle();
  for (let i = 0; i < 20; i++) {
    a.collection('tasks').add({ id: `t${i}` });
    await tick();
  }
  await net.settle();
  assert.equal(Object.keys(store.snapshot().tasks).length, 20);
  assert.throws(() => createStore({ initial: INITIAL, rateLimit: { burst: 0, perSecond: 1 } }), /positive burst/);
});

test('a hello carries at most 1000 ops; the rest follow as ops once the answer lands, and every one is accepted', async () => {
  const { store, net, received } = setup({ rateLimit: false });
  const link = net.link();
  const a = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'a' });
  for (let i = 0; i < 1005; i++) {
    a.collection('tasks').add({ id: `t${i}` });
    await tick();
  }
  assert.equal(a.pending, 1005);
  a.connect();
  await net.settle();
  const hello = received.find(m => m.t === 'hello');
  assert.equal(hello.ops.length, 1000);
  assert.equal(received.filter(m => m.t === 'op').length, 5, 'the remaining five went as ops');
  assert.equal(a.pending, 0);
  assert.equal(Object.keys(store.snapshot().tasks).length, 1005);
  assert.equal(a.status, 'online');
});
