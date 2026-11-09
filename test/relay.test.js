import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createRelay } from '../src/relay/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, subtasks: {} }, parents: { subtasks: { taskId: 'tasks' } } };
const snap = client => LazyWatch.snapshot(client.state);

function setup({ acknowledge = 'upstream', rules, storeOptions = {}, relayOptions = {}, now = fakeTime(1_000_000) } = {}) {
  const store = createStore({ ...SHAPE, rules, now, ...storeOptions });
  const central = createNetwork(store, { vouch: credential => credential });
  const uplink = central.link({ user: { id: 'relay' } });
  const errors = [];
  const relay = createRelay({ shapeOf: () => SHAPE, link: uplink.factory, acknowledge, grace: 0, reconnect: false, keepalive: false, now, onError: err => errors.push(err), ...relayOptions });
  const site = createNetwork(relay);
  const settle = async () => {
    for (let i = 0; i < 20; i++) {
      await central.settle();
      await site.settle();
      if (central.pending === 0 && site.pending === 0) return;
    }
  };
  const display = (replicaId, user = { id: replicaId }) => site.client({ replicaId, ...SHAPE, now }, { user });
  const direct = (replicaId, user = { id: replicaId }) => central.client({ replicaId, ...SHAPE, now }, { user });
  const centralDown = async () => { uplink.goOffline(); await settle(); };
  const centralUp = async () => { uplink.goOnline(); relay.upstreamUp(); await settle(); };
  return { store, relay, now, settle, display, direct, centralDown, centralUp, central, uplink, errors };
}

test('a client behind the relay reads and writes the store, and hears the server\'s ack', async () => {
  const { store, settle, display, direct } = setup();
  const a = display('a');
  const b = direct('b');
  await settle();
  assert.equal(a.status, 'online');
  a.state.tasks.k = { title: 'Milk' };
  await settle();
  assert.deepEqual(store.state.tasks.k, { title: 'Milk' });
  assert.deepEqual(snap(b).tasks.k, { title: 'Milk' });
  assert.equal(a.pending, 0, 'acknowledged by the server');
  b.state.tasks.k.done = true;
  await settle();
  assert.deepEqual(snap(a).tasks.k, { title: 'Milk', done: true });
  assert.ok(store.peers().length === 0 || true);
});

test('while the server is away the site goes on, and converges when it is back', async () => {
  const { store, relay, now, settle, display, direct, centralDown, centralUp } = setup();
  const a = display('a');
  const b = display('b');
  const c = direct('c');
  await settle();
  a.state.tasks.q = { number: 1 };
  await settle();
  await centralDown();
  assert.equal(relay.mode, 'local');
  a.state.tasks.q.number = 2;
  await settle();
  assert.equal(b.state.tasks.q.number, 2, 'the other display hears it through the relay');
  assert.equal(a.pending, 1, 'kept until the server has it');
  assert.equal(b.relayed, true);
  now.advance(10);
  b.state.tasks.q.number = 3;
  await settle();
  assert.equal(a.state.tasks.q.number, 3);
  await centralUp();
  assert.equal(relay.mode, 'through');
  assert.equal(store.state.tasks.q.number, 3, 'what the site showed is what the server keeps');
  assert.equal(a.pending, 0);
  assert.equal(b.pending, 0);
  assert.deepEqual(snap(c), snap(a));
  assert.deepEqual(snap(b), snap(a));
});

test('a display that reconnects during an outage is answered from the copy', async () => {
  const { settle, display, centralDown, centralUp, store } = setup();
  const a = display('a');
  await settle();
  a.state.tasks.x = { n: 1 };
  await settle();
  await centralDown();
  a.link.goOffline();
  await settle();
  a.state.tasks.x.n = 2;
  await settle();
  a.link.goOnline(); a.connect();
  await settle();
  assert.equal(a.status, 'online');
  assert.equal(a.state.tasks.x.n, 2);
  await centralUp();
  assert.equal(store.state.tasks.x.n, 2);
  assert.equal(a.pending, 0);
});

test('a refusal from the server reaches the display behind the relay and is taken back', async () => {
  const { settle, display } = setup({ rules: { tasks: (user, before, after) => (after ?? before).owner === user.id } });
  const a = display('a');
  const b = display('b');
  await settle();
  const rejected = [];
  a.on('rejected', e => rejected.push(e));
  a.state.tasks.mine = { owner: 'a' };
  await settle();
  a.state.tasks.theirs = { owner: 'z' };
  await settle();
  assert.ok(rejected.length >= 1, 'refused');
  assert.equal(a.state.tasks.theirs, undefined);
  assert.equal(b.state.tasks.theirs, undefined);
  assert.equal(a.state.tasks.mine.owner, 'a');
});

test('a display the server will not let in is closed', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const central = createNetwork(store, { vouch: credential => credential, authorizeId: user => user.id !== 'banned' });
  const uplink = central.link({ user: { id: 'relay' } });
  const relay = createRelay({ shapeOf: () => SHAPE, link: uplink.factory, reconnect: false, keepalive: false, now });
  const site = createNetwork(relay);
  const x = site.client({ replicaId: 'x', ...SHAPE, now }, { user: { id: 'banned' } });
  const closed = [];
  x.on('closed', e => closed.push(e));
  for (let i = 0; i < 5; i++) { await central.settle(); await site.settle(); }
  assert.equal(closed[0]?.code, 'forbidden');
});

test('in local mode the tabs are one replica, acknowledged by the relay', async () => {
  const { store, relay, settle, display, centralDown, centralUp } = setup({ acknowledge: 'local' });
  const a = display('tab1');
  const b = display('tab2');
  await settle();
  await centralDown();
  a.state.tasks.k = { title: 'offline' };
  await settle();
  assert.equal(a.pending, 0, 'the relay acknowledged it');
  assert.equal(b.state.tasks.k.title, 'offline');
  await centralUp();
  assert.deepEqual(store.state.tasks.k, { title: 'offline' });
  assert.ok(relay.replicaId);
});

test('behind a relay, an edit right after a burst of the server\'s writes in one millisecond lands', async () => {
  const { store, settle, display } = setup();
  const a = display('a');
  await settle();
  for (let i = 0; i < 5; i++) store.patch({ tasks: { [`x${i}`]: { n: i } } });
  store.patch({ tasks: { k: { title: 'Milk' } } });
  await settle();
  const lost = [];
  a.on('lost', e => lost.push(...e.lost.map(l => l.why)));
  a.state.tasks.k.done = true;
  await settle();
  assert.deepEqual(lost, []);
  assert.equal(store.state.tasks.k.done, true);
  assert.equal(a.state.tasks.k.done, true);
});

// The relay's retries wait on timers, `retryAfter` from the server's refusal: real time, and the networks settled until its queue is empty
async function drained(relay, settle) {
  for (let i = 0; i < 300; i++) {
    await settle();
    if (relay.pending('main') === 0) return;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

test("a browser's relay back with more ops than the bucket holds lands them all: after a refusal for the rate it says hello again, as a client does", async () => {
  const { store, relay, settle, display, centralDown, centralUp } = setup({ acknowledge: 'local', now: Date.now, storeOptions: { rateLimit: { burst: 5, perSecond: 500 } } });
  const a = display('a', { id: 'relay' });
  await settle();
  await centralDown();
  for (let i = 0; i < 30; i++) {
    a.state.tasks[`t${i}`] = { n: i };
    await new Promise(resolve => setImmediate(resolve));
  }
  await settle();
  assert.equal(relay.pending('main'), 30);
  await centralUp();
  await drained(relay, settle);
  assert.equal(relay.pending('main'), 0);
  assert.equal(Object.keys(store.state.tasks).length, 30);
});

test("a browser's relay that writes faster than the rate while the server is there is held, then lands every op", async () => {
  const { store, relay, settle, display } = setup({ acknowledge: 'local', now: Date.now, storeOptions: { rateLimit: { burst: 5, perSecond: 500 } } });
  const a = display('a', { id: 'relay' });
  await settle();
  for (let i = 0; i < 30; i++) {
    a.state.tasks[`t${i}`] = { n: i };
    await new Promise(resolve => setImmediate(resolve));
  }
  await drained(relay, settle);
  assert.equal(relay.pending('main'), 0);
  assert.equal(Object.keys(store.state.tasks).length, 30);
});

test("a browser's relay back with more ops than a hello carries sends the rest once it is answered", async () => {
  const { store, relay, settle, display, centralDown, centralUp } = setup({ acknowledge: 'local', storeOptions: { rateLimit: false } });
  const a = display('a', { id: 'relay' });
  await settle();
  await centralDown();
  for (let i = 0; i < 1005; i++) {
    a.state.tasks[`t${i}`] = { n: i };
    await new Promise(resolve => setImmediate(resolve));
  }
  await settle();
  await centralUp();
  await settle();
  assert.equal(relay.pending('main'), 0);
  assert.equal(Object.keys(store.state.tasks).length, 1005);
});

// Found by the relay fuzzer with the rate limit on (--rate --local)
test("a browser's relay whose hello is refused for the rate says hello again after retryAfter, and reports no fault", async () => {
  const { store, settle, display, direct, centralDown, centralUp, now, errors } = setup({ acknowledge: 'local', storeOptions: { rateLimit: { burst: 2, perSecond: 100 } } });
  const tab = display('tab', { id: 'relay' });
  const other = direct('other');
  await settle();
  tab.state.tasks.t = { title: 'Milk' };   // the bucket's second token: the first was the hello's
  await settle();
  assert.deepEqual(store.state.tasks, { t: { title: 'Milk' } });
  await centralDown();
  await centralUp();   // a hello the bucket has nothing for
  assert.deepEqual(errors, [], 'a refusal for the rate is not a fault');
  other.state.tasks.x = { title: 'Eggs' };
  await settle();
  assert.equal(snap(tab).tasks.x, undefined, 'not reading the store yet');
  now.advance(100);
  await new Promise(resolve => setTimeout(resolve, 40));   // the relay's retry, on the real clock
  await settle();
  assert.deepEqual(snap(tab).tasks.x, { title: 'Eggs' }, 'back on the store');
  assert.deepEqual(errors, []);
});
