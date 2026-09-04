// peers.test.js - What a client shares rides on presence: every live session as a peer with its data
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const byReplica = peers => Object.fromEntries(peers.map(p => [p.replicaId, { user: p.user, data: p.data }]));

test('a share made before connecting rides in the hello, one made online is broadcast, null clears it; peers carry replica ids and users', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
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
  const store = createStore({ initial: INITIAL, presence: true });
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
  const store = createStore({ initial: INITIAL, presence: { maxShare: 64 } });
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
  assert.throws(() => createStore({ initial: INITIAL, presence: { maxShare: 0 } }), /maxShare/);
});

test('a session counts, as a peer and in presence, from its hello; one that never says hello is not announced', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
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
  assert.deepEqual(received.map(m => m.t), ['snapshot', 'presence', 'presence'], 'the whole list follows its snapshot, then the delta everyone gets');
  assert.deepEqual(received[1].peers.map(p => p.replicaId), ['a', 's']);
  assert.deepEqual(received[2], { t: 'presence', joined: [{ replicaId: 's', user: { id: 'u9' }, key: 'u9', data: { here: true } }] });
  assert.deepEqual(a.presence.map(u => u.id).sort(), ['u1', 'u9']);
  assert.deepEqual(byReplica(a.peers).s, { user: { id: 'u9' }, data: { here: true } });
  const before = received.length;
  silent.receive({ t: 'hello', replicaId: 's', ops: [] });
  await net.settle();
  assert.deepEqual(received.slice(before).map(m => m.t), ['snapshot'], 'a re-hello on the same session is not a new peer: no presence follows');

  silent.close();
  await net.settle();
  assert.deepEqual(a.presence.map(u => u.id), ['u1']);
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a']);
});

/**
 * A raw session that only listens, recording the presence deltas it gets.
 * Its own whole list comes at once and the delta announcing it with the
 * next flush; `reset()` after a settle discards those.
 */
function observer(store, replicaId = 'obs') {
  const messages = [];
  const session = store.session({ send: m => { if (m.t === 'presence') messages.push(m); }, user: { id: replicaId } });
  session.receive({ t: 'hello', replicaId, ops: [] });
  assert.ok(Array.isArray(messages[0]?.peers), 'the whole list comes first');
  let start = 1;
  return {
    session,
    deltas: () => messages.slice(start),
    reset() {
      assert.deepEqual(messages.slice(1).map(m => m.joined?.map(p => p.replicaId)), [[replicaId]], 'its own join was the only delta so far');
      start = messages.length;
    }
  };
}

test('presence travels as deltas: a newcomer gets the whole list, everyone else what changed, in small messages', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1', name: 'Ann' } });
  await net.settle();
  const obs = observer(store);
  await net.settle();
  obs.reset();

  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  await net.settle();
  assert.deepEqual(obs.deltas(), [{ t: 'presence', joined: [{ replicaId: 'b', user: { id: 'u2', name: 'Bo' }, key: 'u2' }] }]);
  assert.deepEqual(byReplica(a.peers).b, { user: { id: 'u2', name: 'Bo' }, data: undefined });

  b.share({ editing: 'x' });
  await net.settle();
  assert.deepEqual(obs.deltas().at(-1), { t: 'presence', shared: [{ replicaId: 'b', data: { editing: 'x' } }] });
  assert.deepEqual(byReplica(a.peers).b.data, { editing: 'x' });
  b.share(null);
  await net.settle();
  assert.deepEqual(obs.deltas().at(-1), { t: 'presence', shared: [{ replicaId: 'b' }] }, 'cleared: no data');
  assert.equal(byReplica(a.peers).b.data, undefined);

  b.link.goOffline();
  await net.settle();
  assert.deepEqual(obs.deltas().at(-1), { t: 'presence', left: ['b'] });
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'obs']);

  // The users list follows the peers: one user on two sessions is one entry until the last session goes
  const a2 = net.client({ replicaId: 'a2', initial: INITIAL }, { user: { id: 'u1', name: 'Ann' } });
  await net.settle();
  assert.deepEqual(a.presence.map(u => u.id), ['u1', 'obs']);
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'obs', 'a2']);
  const presenceEvents = [];
  a.on('presence', users => presenceEvents.push(users.map(u => u.id)));
  a2.disconnect();
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'obs']);
  assert.deepEqual(presenceEvents, [], 'the same users: no presence event');
  obs.session.close();
  await net.settle();
  assert.deepEqual(presenceEvents, [['u1']]);
});

test('a join and a leave within one flush is just the leave; a share before the join goes out rides on the join', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  const obs = observer(store);
  await net.settle();
  obs.reset();
  const events = [];
  a.on('peers', peers => events.push(peers.map(p => p.replicaId)));

  const brief = store.session({ send: () => {}, user: { id: 'u5' } });
  brief.receive({ t: 'hello', replicaId: 'brief', ops: [] });
  brief.close();
  await net.settle();
  assert.deepEqual(obs.deltas(), [{ t: 'presence', left: ['brief'] }], 'never announced as joined');
  assert.deepEqual(events, [], 'a leave of an unknown peer changes nothing for a client');

  const late = store.session({ send: () => {}, user: { id: 'u6' } });
  late.receive({ t: 'hello', replicaId: 'late', ops: [] });
  late.receive({ t: 'share', data: { cursor: 1 } });
  late.receive({ t: 'share', data: { cursor: 2 } });
  await net.settle();
  assert.deepEqual(obs.deltas().at(-1), { t: 'presence', joined: [{ replicaId: 'late', user: { id: 'u6' }, key: 'u6', data: { cursor: 2 } }] });
  assert.deepEqual(events, [['a', 'obs', 'late']]);
  late.close();
});

test('presence.every batches: changes within the window go out together, a later share replacing an earlier one', async () => {
  const store = createStore({ initial: INITIAL, presence: { every: 60 } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  const obs = observer(store);
  await new Promise(resolve => setTimeout(resolve, 150));   // its own join lands with the window, then a quiet spell
  await net.settle();
  obs.reset();

  a.share({ cursor: 1 });
  await net.settle();
  assert.deepEqual(obs.deltas(), [{ t: 'presence', shared: [{ replicaId: 'a', data: { cursor: 1 } }] }], 'the first change after a quiet spell goes out at once');
  a.share({ cursor: 2 });
  await net.settle();
  a.share({ cursor: 3 });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  assert.equal(obs.deltas().length, 1, 'within the window: nothing yet');
  assert.equal(b.peers.length, 3, 'the newcomer still got the whole list at once');
  await new Promise(resolve => setTimeout(resolve, 90));
  await net.settle();
  assert.deepEqual(obs.deltas().slice(1), [{ t: 'presence', joined: [{ replicaId: 'b', user: { id: 'u2' }, key: 'u2' }], shared: [{ replicaId: 'a', data: { cursor: 3 } }] }]);
  assert.deepEqual(byReplica(b.peers).a.data, { cursor: 3 });
  assert.throws(() => createStore({ initial: INITIAL, presence: { every: -1 } }), /presence\.every/);
  store.dispose();
});

test('presence is off by default: nothing is sent, the lists stay empty, and a share is refused with forbidden', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const received = [];
  const raw = store.session({ send: m => received.push(m), user: { id: 'u0' } });
  raw.receive({ t: 'hello', replicaId: 'raw', ops: [], share: { here: true } });
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const errors = [];
  a.on('error', err => errors.push(err.code));
  await net.settle();
  assert.deepEqual(received.filter(m => m.t === 'presence'), [], 'no whole list, no delta');
  assert.deepEqual(received.filter(m => m.t === 'error').map(m => m.code), ['forbidden'], 'the share in the hello was refused');
  assert.deepEqual(a.presence, []);
  assert.deepEqual(a.peers, []);
  assert.deepEqual(store.presence(), []);
  assert.deepEqual(store.peers(), []);
  a.share({ editing: 'x' });
  await net.settle();
  assert.deepEqual(errors, ['forbidden']);
  assert.deepEqual(a.peers, []);
  raw.close();
  assert.throws(() => createStore({ initial: INITIAL, presence: 'yes' }), /presence must be/);
});

test('presence.user chooses what of a user its peers see, and presence.key what users are distinct by', async () => {
  const store = createStore({ initial: INITIAL, presence: { user: u => ({ name: u.name }), key: u => u.email } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { email: 'ann@x', name: 'Ann', role: 'admin' } });
  const a2 = net.client({ replicaId: 'a2', initial: INITIAL }, { user: { email: 'ann@x', name: 'Ann', role: 'admin' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { email: 'bo@x', name: 'Bo', role: 'member' } });
  await net.settle();
  assert.deepEqual(b.peers.map(p => [p.replicaId, p.user, p.key]), [['a', { name: 'Ann' }, 'ann@x'], ['a2', { name: 'Ann' }, 'ann@x'], ['b', { name: 'Bo' }, 'bo@x']], 'the role never left the server');
  assert.deepEqual(b.presence, [{ name: 'Ann' }, { name: 'Bo' }]);
  assert.deepEqual(store.presence(), [{ name: 'Ann' }, { name: 'Bo' }], 'the server shows the same');
  assert.throws(() => createStore({ initial: INITIAL, presence: { user: 'name' } }), /presence\.key and presence\.user/);
  a.dispose();
  a2.dispose();
});

test('presence.validate judges a share: false or a throw refuses it with forbidden, a value replaces it', async () => {
  const validated = [];
  const store = createStore({
    initial: INITIAL,
    presence: {
      validate(data, { user, replicaId, store: self }) {
        validated.push([replicaId, data]);
        assert.equal(self, store);
        if (data.editing === 'secret') return false;
        if (data.editing === 'boom') throw new Error('Not yours to edit');
        return { ...data, by: user.id };   // a peer cannot claim to be someone else
      }
    }
  });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push([err.code, err.message]));

  a.share({ editing: 'x', by: 'u2' });
  await net.settle();
  assert.deepEqual(byReplica(b.peers).a.data, { editing: 'x', by: 'u1' }, 'replaced by what the hook returned');
  a.share({ editing: 'secret' });
  a.share({ editing: 'boom' });
  await net.settle();
  assert.deepEqual(errors, [['forbidden', 'Share refused'], ['forbidden', 'Not yours to edit']]);
  assert.deepEqual(byReplica(b.peers).a.data, { editing: 'x', by: 'u1' }, 'unchanged');
  a.share(null);
  await net.settle();
  assert.equal(byReplica(b.peers).a.data, undefined);
  assert.deepEqual(validated.map(([id]) => id), ['a', 'a', 'a'], 'clearing is not judged');
  assert.throws(() => createStore({ initial: INITIAL, presence: { validate: true } }), /presence\.validate/);
});

test('a client with presence: false is sent no presence, has no peers, but is a peer to the others and may still share', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const mirror = net.client({ replicaId: 'm', initial: INITIAL, presence: false }, { user: { id: 'bot' } });
  await net.settle();
  const events = [];
  mirror.on('peers', () => events.push('peers'));
  mirror.on('presence', () => events.push('presence'));
  assert.deepEqual(mirror.peers, []);
  assert.deepEqual(mirror.presence, []);
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'm'], 'the others see it');
  mirror.share({ following: true });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  assert.deepEqual(byReplica(a.peers).m.data, { following: true });
  assert.deepEqual(Object.keys(byReplica(b.peers)), ['a', 'm', 'b']);
  assert.deepEqual(events, [], 'still nothing for the mirror');
  assert.deepEqual(mirror.peers, []);
  mirror.disconnect();
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'b']);
});
