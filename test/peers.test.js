// peers.test.js - What a client shares rides on presence: every live session as a peer with its data
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork, fakeTime } from './helpers.js';

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
  store.flush();   // what it heard, as the turn ends (see groupCommit)
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
  store.flush();   // the answer and the list follow its replica's claim once stored: at the end of the turn (see groupCommit), or now
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

test("presence.every reads the store's clock: once `now` has moved past the window, a change goes out at once", async () => {
  const now = fakeTime();
  const store = createStore({ initial: INITIAL, presence: { every: 60_000 }, now });
  const net = createNetwork(store);
  const obs = observer(store);
  await new Promise(resolve => setImmediate(resolve));   // its own join goes out, opening a window
  obs.reset();

  now.advance(60_000);   // a minute on the store's clock, no time at all on the real one
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(obs.deltas(), [{ t: 'presence', joined: [{ replicaId: 'a', user: { id: 'u1' }, key: 'u1' }] }], "the window closed on the store's clock");
  a.dispose();
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

test('a share draws on the replica\'s rate limit like an op: beyond it, refused with rate-limited, and the next hello carries the latest', async () => {
  // Three tokens: the hello takes one, two shares the rest
  const store = createStore({ initial: INITIAL, presence: true, rateLimit: { burst: 3, perSecond: 50 } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err.code));
  a.share({ cursor: 1 });
  a.share({ cursor: 2 });
  a.share({ cursor: 3 });
  await net.settle();
  assert.deepEqual(errors, ['rate-limited']);
  assert.deepEqual(byReplica(b.peers).a.data, { cursor: 2 }, 'the third was refused');
  await new Promise(resolve => setTimeout(resolve, 60));   // the client's retry hello, after retryAfter
  await net.settle();
  assert.deepEqual(byReplica(b.peers).a.data, { cursor: 3 }, 'the hello carried the latest');
  assert.equal(a.pending, 0);
});

test('a hello says whether its session hears presence: off, it hears none and is still listed; a later hello turns it on with the whole list at once, and off again', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  const received = [];
  const heard = () => received.filter(m => m.t === 'presence');
  const quiet = store.session({ send: m => received.push(m), user: { id: 'u9' } });
  quiet.receive({ t: 'hello', replicaId: 'q', ops: [], presence: false, share: { here: true } });
  await net.settle();
  assert.deepEqual(received.map(m => m.t), ['snapshot'], 'answered, with no whole list');
  assert.deepEqual(store.presence().map(u => u.id), ['u1', 'u9'], 'still listed');
  assert.deepEqual(Object.keys(byReplica(store.peers())), ['a', 'q']);
  assert.deepEqual(byReplica(a.peers).q, { user: { id: 'u9' }, data: { here: true } }, 'the others hear of it, and of what it shares');

  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  b.share({ cursor: 1 });
  await net.settle();
  b.disconnect();
  await net.settle();
  assert.deepEqual(heard(), [], 'no delta of a join, a share or a leave');

  // On: the whole list right away, as a newcomer's, then the deltas
  let before = received.length;
  quiet.receive({ t: 'hello', replicaId: 'q', ops: [] });
  assert.deepEqual(received.slice(before).map(m => m.t), ['snapshot', 'presence'], 'the list follows the answer at once');
  assert.deepEqual(byReplica(received.at(-1).peers), { a: { user: { id: 'u1' }, data: undefined }, q: { user: { id: 'u9' }, data: { here: true } } }, 'its own share kept');
  const c = net.client({ replicaId: 'c', initial: INITIAL }, { user: { id: 'u3' } });
  await net.settle();
  assert.deepEqual(heard().at(-1), { t: 'presence', joined: [{ replicaId: 'c', user: { id: 'u3' }, key: 'u3' }] });
  c.share({ cursor: 2 });
  await net.settle();
  assert.deepEqual(heard().at(-1), { t: 'presence', shared: [{ replicaId: 'c', data: { cursor: 2 } }] });
  before = received.length;
  quiet.receive({ t: 'hello', replicaId: 'q', ops: [] });
  assert.deepEqual(received.slice(before).map(m => m.t), ['snapshot'], 'already on: no list again');

  // Off again: nothing more, and still listed
  quiet.receive({ t: 'hello', replicaId: 'q', ops: [], presence: false });
  before = received.length;
  a.share({ editing: 'x' });
  c.disconnect();
  const d = net.client({ replicaId: 'd', initial: INITIAL }, { user: { id: 'u4' } });
  await net.settle();
  assert.deepEqual(received.slice(before).filter(m => m.t === 'presence'), [], 'off: no deltas');
  assert.deepEqual(Object.keys(byReplica(d.peers)), ['a', 'q', 'd'], 'a newcomer sees it listed');
  assert.deepEqual(store.presence().map(u => u.id), ['u1', 'u9', 'u4']);
  quiet.close();
  store.dispose();
});

test('a write-only session hears no presence whatever its hellos say; a peer says whether the store has presence', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  const peer = store.peer({ user: { id: 'u5' }, replicaId: 'w', via: 'hub' });
  assert.equal(peer.presence, true);
  const received = [];
  const writer = store.session({ send: m => received.push(m), user: { id: 'u5' } });
  writer.receive({ t: 'hello', replicaId: 'w', ops: [], follow: false });
  writer.receive({ t: 'hello', replicaId: 'w', ops: [] });   // presence not refused: still none for it
  writer.receive({ t: 'hello', replicaId: 'w', ops: [], presence: true });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  a.share({ cursor: 1 });
  await net.settle();
  assert.deepEqual(received.map(m => m.t), ['snapshot', 'snapshot', 'snapshot'], 'answered each time, and nothing else');
  assert.deepEqual(Object.keys(byReplica(b.peers)), ['a', 'w', 'b'], 'listed once, as the peer');
  writer.close();
  peer.close();

  const off = createStore({ initial: INITIAL });
  const offPeer = off.peer({ user: { id: 'u5' }, replicaId: 'w' });
  assert.equal(offPeer.presence, false);
  assert.deepEqual(offPeer.peer, { replicaId: 'w' });
  offPeer.close();
  store.dispose();
  off.dispose();
});

test('db.wantPresence turns presence on and off while the client runs: on fills its peers, off empties them at once and keeps them empty', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const m = net.client({ replicaId: 'm', initial: INITIAL, presence: false }, { user: { id: 'bot' } });
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  a.share({ editing: 'x' });
  await net.settle();
  assert.equal(m.wantsPresence, false);
  assert.deepEqual(m.peers, []);
  const events = [];
  m.on('peers', peers => events.push(['peers', peers.map(p => p.replicaId)]));
  m.on('presence', users => events.push(['presence', users.map(u => u.id)]));

  m.wantPresence(true);
  assert.equal(m.wantsPresence, true);
  await net.settle();
  assert.deepEqual(byReplica(m.peers), { m: { user: { id: 'bot' }, data: undefined }, a: { user: { id: 'u1' }, data: { editing: 'x' } } });
  assert.deepEqual(m.presence.map(u => u.id), ['bot', 'u1']);
  assert.deepEqual(events, [['peers', ['m', 'a']], ['presence', ['bot', 'u1']]]);
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(m.peers)), ['m', 'a', 'b'], 'then the deltas');
  const count = events.length;
  m.wantPresence(true);
  await net.settle();
  assert.equal(events.length, count, 'the same wish again changes nothing');

  events.length = 0;
  m.wantPresence(false);
  assert.equal(m.wantsPresence, false);
  assert.deepEqual(m.peers, [], 'emptied at once');
  assert.deepEqual(m.presence, []);
  assert.deepEqual(events, [['peers', []], ['presence', []]]);
  b.disconnect();
  a.share({ editing: 'y' });
  const c = net.client({ replicaId: 'c', initial: INITIAL }, { user: { id: 'u3' } });
  await net.settle();
  assert.deepEqual(m.peers, []);
  assert.equal(events.length, 2, 'no later join, leave or share reaches it');
  assert.deepEqual(Object.keys(byReplica(c.peers)), ['m', 'a', 'c'], 'still a peer to the others');
  m.state.tasks.x = { id: 'x' };
  await net.settle();
  assert.deepEqual(c.state.tasks.x, { id: 'x' }, 'and syncs as before');
  assert.equal(m.pending, 0);
});

/** A link whose hellos are recorded as the client sends them */
function recordingLink(net, user) {
  const link = net.link({ user });
  const hellos = [];
  const factory = () => {
    const t = link.factory();
    const send = t.send;
    t.send = message => {
      if (message.t === 'hello') hellos.push(message);
      send(message);
    };
    return t;
  };
  return { link, hellos, factory };
}

test('db.wantPresence offline says nothing; the first hello once connected carries the wish', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();

  // Never connected: off when made, on by the first hello
  const quiet = recordingLink(net, { id: 'u2' });
  const m = createClient({ transport: quiet.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'm', presence: false });
  m.wantPresence(true);
  await net.settle();
  assert.deepEqual(quiet.hellos, [], 'nothing sent while not connected');
  m.connect();
  await net.settle();
  assert.equal(quiet.hellos.length, 1);
  assert.equal('presence' in quiet.hellos[0], false, 'the hello does not refuse presence');
  assert.deepEqual(Object.keys(byReplica(m.peers)), ['a', 'm']);

  // Gone offline: on while it was online, off by the hello that reconnects
  const loud = recordingLink(net, { id: 'u3' });
  const n = createClient({ transport: loud.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'n' });
  n.connect();
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(n.peers)), ['a', 'm', 'n']);
  loud.link.goOffline();
  await net.settle();
  n.wantPresence(false);
  await net.settle();
  assert.equal(loud.hellos.length, 1, 'nothing sent offline');
  loud.link.goOnline();
  n.connect();
  await net.settle();
  assert.equal(loud.hellos.length, 2);
  assert.equal(loud.hellos[1].presence, false, 'the reconnect says it');
  assert.deepEqual(n.peers, []);
  assert.deepEqual(Object.keys(byReplica(a.peers)), ['a', 'm', 'n'], 'listed again for the others');
  m.dispose();
  n.dispose();
});

test('the hello wantPresence says again loses no pending op and merges none twice', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const merged = [];
  store.observe('op', ({ replicaId, seq }) => merged.push(`${replicaId}:${seq}`));
  const net = createNetwork(store);
  const recorded = recordingLink(net, { id: 'u1' });
  const m = createClient({ transport: recorded.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'm', presence: false });
  m.connect();
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();

  // An op sent live and not yet acknowledged rides in the hello as well
  m.state.tasks.x = { id: 'x', title: 'one' };
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(m.pending, 1);
  assert.ok(net.pending > 0, 'sent, not yet delivered');
  m.wantPresence(true);
  assert.equal(recorded.hellos.length, 2);
  assert.deepEqual(recorded.hellos[1].ops.map(op => op.seq), [1], 'the hello carries the op still pending');
  // And one made after the hello went out, before its answer
  m.state.tasks.y = { id: 'y', title: 'two' };
  await net.settle();
  assert.deepEqual(merged, ['m:1', 'm:2'], 'each merged once');
  assert.equal(m.pending, 0);
  const tasks = { x: { id: 'x', title: 'one' }, y: { id: 'y', title: 'two' } };
  assert.deepEqual(store.snapshot().tasks, tasks);
  assert.deepEqual(JSON.parse(JSON.stringify(m.state.tasks)), tasks);
  assert.deepEqual(JSON.parse(JSON.stringify(b.state.tasks)), tasks);
  assert.deepEqual(Object.keys(byReplica(m.peers)), ['m', 'b']);

  // The same off, with the op written in the same turn as the wish, before it is even made
  m.state.tasks.x.title = 'three';
  m.wantPresence(false);
  await net.settle();
  assert.deepEqual(merged, ['m:1', 'm:2', 'm:3']);
  assert.equal(m.pending, 0);
  assert.equal(store.snapshot().tasks.x.title, 'three');
  assert.equal(b.state.tasks.x.title, 'three');
  assert.equal(m.state.tasks.x.title, 'three');
  assert.deepEqual(m.peers, []);
  m.dispose();
});

test('presence already on its way when a client stops wanting it is not kept, nor a whole list asked for and let go of at once', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const m = net.client({ replicaId: 'm', initial: INITIAL }, { user: { id: 'u1' } });
  await net.settle();
  assert.deepEqual(Object.keys(byReplica(m.peers)), ['m']);

  // A join the server sends before it hears the hello that says off
  const raw = store.session({ send: () => {}, user: { id: 'u9' } });
  raw.receive({ t: 'hello', replicaId: 'raw', ops: [] });
  await new Promise(resolve => setImmediate(resolve));   // the flush: the delta is on the wire to m
  assert.ok(net.pending > 0, 'the delta waits in the network');
  const events = [];
  m.on('peers', peers => events.push(peers.map(p => p.replicaId)));
  m.wantPresence(false);
  await net.settle();
  assert.deepEqual(m.peers, [], 'the late delta is ignored');
  assert.deepEqual(m.presence, []);
  assert.deepEqual(events, [[]], 'only the clearing');

  // On and off again before the server answers: the list that comes is not kept
  m.wantPresence(true);
  m.wantPresence(false);
  await net.settle();
  assert.deepEqual(m.peers, []);
  assert.deepEqual(events, [[]]);
  assert.equal(m.wantsPresence, false);
  raw.close();
});
