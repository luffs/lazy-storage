// peers.test.js - What a client shares rides on presence: every live session as a peer with its data
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const byReplica = peers => Object.fromEntries(peers.map(p => [p.replicaId, { user: p.user, data: p.data }]));

test('a share made before connecting rides in the hello, one made online is broadcast, null clears it; peers carry replica ids and users', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const link = net.link({ user: { id: 'u1', name: 'Ann' } });
  const a = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'a' });
  a.share({ editing: 'x' });
  assert.deepEqual(a.shared, { editing: 'x' });
  assert.deepEqual(a.peers, []);
  a.connect();
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  await net.settle();
  const expected = { a: { user: { id: 'u1', name: 'Ann' }, data: { editing: 'x' } }, b: { user: { id: 'u2', name: 'Bo' }, data: undefined } };
  assert.deepEqual(byReplica(a.peers), expected);
  assert.deepEqual(byReplica(b.peers), expected);
  assert.deepEqual(byReplica(store.peers()), expected);
  assert.ok(a.peers.some(p => p.replicaId === a.replicaId), 'a client sees its own entry');
  assert.deepEqual(a.presence.map(u => u.name).sort(), ['Ann', 'Bo']);

  const seen = [];
  b.on('peers', peers => seen.push(byReplica(peers)));
  a.share({ editing: 'y' });
  await net.settle();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].a.data, { editing: 'y' });
  a.share({ editing: 'y' });
  await net.settle();
  assert.equal(seen.length, 1, 'the same value again is not broadcast');
  a.share(null);
  await net.settle();
  assert.equal(seen.length, 2);
  assert.equal(seen[1].a.data, undefined);
  assert.equal(a.shared, undefined);
  assert.throws(() => a.share(() => {}), /JSON/);
});

test('a peer goes when its session ends and comes back with its share on reconnect; offline, a client has no peers', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  a.share({ cursor: 3 });
  await net.settle();
  assert.deepEqual(byReplica(b.peers).a.data, { cursor: 3 });

  a.link.goOffline();
  await net.settle();
  assert.deepEqual(a.peers, []);
  assert.deepEqual(Object.keys(byReplica(b.peers)), ['b']);
  assert.deepEqual(a.shared, { cursor: 3 }, 'still what this client shares');
  a.link.goOnline();
  a.connect();
  await net.settle();
  assert.deepEqual(byReplica(b.peers).a.data, { cursor: 3 }, 'restored by the hello');
  assert.deepEqual(byReplica(a.peers).a.data, { cursor: 3 });

  assert.equal(store.closeSessions(s => s.user?.id === 'u1', 'bye'), 1);
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(b.peers)), ['b']);
  assert.deepEqual(a.peers, []);
});

test('shared data must be JSON within maxShare; a refusal is an error on the sharer and changes nothing', async () => {
  const store = createStore({ initial: INITIAL, maxShare: 64 });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err.code));
  a.share({ note: 'x'.repeat(100) });
  await net.settle();
  assert.deepEqual(errors, ['too-large']);
  assert.equal(byReplica(b.peers).a.data, undefined);
  a.share({ ok: true });
  await net.settle();
  assert.deepEqual(byReplica(b.peers).a.data, { ok: true });

  // Straight at a session: what a client would never send
  const received = [];
  const raw = store.session({ send: m => received.push(m), user: { id: 'u3' } });
  raw.receive({ t: 'hello', replicaId: 'raw', ops: [] });
  raw.receive({ t: 'share', data: () => {} });
  assert.deepEqual(received.filter(m => m.t === 'error').map(m => m.code), ['invalid']);
  raw.close();
  assert.throws(() => createStore({ initial: INITIAL, maxShare: 0 }), /maxShare/);
});

test('a session counts, as a peer and in presence, from its hello; one that never says hello is not announced', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  const received = [];
  const silent = store.session({ send: m => received.push(m), user: { id: 'u9' } });
  await net.settle();
  assert.deepEqual(store.presence().map(u => u.id), ['u1']);
  assert.deepEqual(a.presence.map(u => u.id), ['u1']);
  assert.deepEqual(received, [], 'nothing until it says hello');

  silent.receive({ t: 'hello', replicaId: 's', ops: [], share: { here: true } });
  await net.settle();
  assert.deepEqual(received.map(m => m.t), ['snapshot', 'presence'], 'its own presence follows its snapshot');
  assert.deepEqual(a.presence.map(u => u.id).sort(), ['u1', 'u9']);
  assert.deepEqual(byReplica(a.peers).s, { user: { id: 'u9' }, data: { here: true } });
  const before = received.length;
  silent.receive({ t: 'hello', replicaId: 's', ops: [] });
  assert.deepEqual(received.slice(before).map(m => m.t), ['snapshot'], 'a re-hello on the same session is not a new peer: no presence follows');

  silent.close();
  await net.settle();
  assert.deepEqual(a.presence.map(u => u.id), ['u1']);
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a']);
});
