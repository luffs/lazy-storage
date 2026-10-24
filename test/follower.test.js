// follower.test.js - The relay as a follower store: its own writes, the host's
// word on the server, copies kept on disk, stores it may not carry, presence
// and evictions passed on, and what it refuses on its own
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub } from '../src/server/index.js';
import { createRelay, fileCopies, memoryCopies } from '../src/relay/index.js';
import { PROTOCOL } from '../src/core/index.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {}, settings: {} }, singletons: ['settings'] };
const snap = client => LazyWatch.snapshot(client.state);

/**
 * A store, a relay on a link to it, and the relay's own network of
 * displays. The store's hubs are kept, to take clients back by hand
 */
function setup({ storeOptions = {}, relayOptions = {}, hubOptions = {}, shape = SHAPE } = {}) {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now, ...storeOptions });
  const hubs = [];
  const central = createNetwork({
    session: ({ send, user }) => {
      const hub = createHub(() => store, { send, user, vouch: credential => credential, ...hubOptions });
      hubs.push(hub);
      return hub;
    }
  });
  const uplink = central.link({ user: { id: 'relay' } });
  const errors = [];
  const relay = createRelay({ shapeOf: () => shape, link: uplink.factory, grace: 0, reconnect: false, keepalive: false, now, onError: err => errors.push(err), ...relayOptions });
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
  return { store, relay, now, settle, display, direct, uplink, hubs, errors, central, site };
}

test('the relay writes as itself: shown at once, sent up as its own op, acknowledged by the server', async () => {
  const { store, relay, settle, display } = setup();
  const a = display('a');
  await settle();
  relay.patch('main', { tasks: { k: { title: 'from the hub' } }, settings: { theme: 'dark' } });
  assert.equal(relay.copy('main').tasks.k.title, 'from the hub', 'in the copy at once');
  assert.equal(relay.pending('main'), 1);
  const waiting = relay.stats().stores.main.waiting;
  assert.equal(waiting.length, 1);
  assert.equal(waiting[0].own, true);
  await settle();
  assert.equal(store.state.tasks.k.title, 'from the hub');
  assert.equal(store.state.settings.theme, 'dark');
  assert.equal(snap(a).tasks.k.title, 'from the hub');
  assert.equal(relay.pending('main'), 0, 'acknowledged');
  assert.deepEqual(relay.stats().stores.main, { sessions: 1, queue: 0, v: store.version, synced: true, waiting: [] });
  assert.equal(relay.stats().link, 'online');

  assert.throws(() => relay.patch('main', 'not a diff'), TypeError);
  assert.throws(() => relay.patch('main', { nope: {} }), TypeError);
  relay.patch('main', { tasks: { k: null } });
  await settle();
  assert.equal(store.state.tasks.k, undefined, 'and deletes');
});

test('a store the relay does not carry is closed unknown-store, and a shape that throws is reported', async () => {
  const errors = [];
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const central = createNetwork(store, { vouch: credential => credential });
  const relay = createRelay({
    shapeOf: id => { if (id === 'broken') throw new Error('no shape for you'); return id === 'main' ? SHAPE : null; },
    link: central.link({ user: { id: 'relay' } }).factory,
    grace: 0, reconnect: false, keepalive: false, now, onError: err => errors.push(err)
  });
  assert.throws(() => relay.patch('elsewhere', {}), TypeError);
  const site = createNetwork(relay);
  const closed = [];
  for (const store of ['elsewhere', 'broken']) {
    const c = site.client({ store, replicaId: `c-${store}`, ...SHAPE, now }, { user: { id: 'u' } });
    c.on('closed', info => closed.push(info.code));
  }
  await site.settle();
  assert.deepEqual(closed, ['unknown-store', 'unknown-store']);
  assert.match(errors[0].message, /no shape for you/);
});

test('upstreamDown answers from the copy while the link is up, and upstreamUp hands back to the server', async () => {
  const { relay, settle, display } = setup();
  const a = display('a');
  const b = display('b');
  await settle();
  const modes = [];
  relay.on('mode', m => modes.push(m));
  relay.upstreamDown();
  await settle();
  assert.equal(relay.mode, 'local');
  assert.equal(a.relayed, true, 'the displays hear it');
  a.state.tasks.x = { title: 'while the host says the server is away' };
  await settle();
  assert.equal(b.state.tasks.x.title, 'while the host says the server is away');
  assert.equal(a.pending, 1, 'and nothing goes up meanwhile');
  relay.upstreamUp();
  await settle();
  assert.equal(relay.mode, 'through');
  assert.equal(a.pending, 0);
  assert.deepEqual(modes, ['local', 'through']);
});

test('a relay restarted from its files during an outage answers the displays the server let in, and catches up after', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-relay-'));
  try {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now });
    const central = createNetwork(store, { vouch: credential => credential });
    const uplink = central.link({ user: { id: 'relay' } });
    const options = { shapeOf: () => SHAPE, link: uplink.factory, grace: 0, reconnect: false, keepalive: false, now };
    const first = createRelay({ ...options, storage: fileCopies(dir, { saveDelay: 10_000 }) });
    let site = createNetwork(first);
    const settle = async () => {
      for (let i = 0; i < 20; i++) {
        await central.settle();
        await site.settle();
        if (central.pending === 0 && site.pending === 0) return;
      }
    };
    const outbox = memoryOutbox();   // the display's own storage, kept across its reload
    const a = site.client({ replicaId: 'a', ...SHAPE, now, storage: outbox }, { user: { id: 'a' } });
    await settle();
    a.state.tasks.k = { title: 'before the outage' };
    await settle();
    const replicaId = first.replicaId;
    first.close();                                  // flushes what waited for saveDelay
    assert.ok(existsSync(join(dir, 'main.json')));
    assert.ok(existsSync(join(dir, 'relay.json')));
    a.dispose();

    uplink.goOffline();                              // the server is away when the relay comes back
    const second = createRelay({ ...options, storage: fileCopies(dir, { saveDelay: 0 }) });
    assert.equal(second.replicaId, replicaId, 'the same replica, from relay.json');
    site = createNetwork(second);
    const again = site.client({ replicaId: 'a', ...SHAPE, now, storage: outbox }, { user: { id: 'a' } });
    const stranger = site.client({ replicaId: 's', ...SHAPE, now }, { user: { id: 's' } });
    await settle();
    assert.equal(again.status, 'online', 'answered from the copy');
    assert.equal(again.relayed, true);
    assert.equal(again.state.tasks.k.title, 'before the outage');
    assert.equal(stranger.status, 'connecting', 'one the server never let in waits');
    again.state.tasks.k.done = true;
    await settle();
    assert.equal(again.pending, 1);

    uplink.goOnline();
    second.upstreamUp();
    await settle();
    assert.equal(store.state.tasks.k.done, true, 'the outage edit reached the server');
    assert.equal(again.pending, 0);
    assert.equal(stranger.status, 'online', 'and the stranger is let in by the server');
    second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a store the server will not let the relay carry is closed with the server\'s word, and asked about again later', async () => {
  let allowed = false;
  const { relay, now, settle, display } = setup({ hubOptions: { authorizeId: user => user.id !== 'relay' || allowed } });
  const closed = [];
  const a = display('a');
  a.on('closed', info => closed.push(info.code));
  await settle();
  assert.deepEqual(closed, ['forbidden']);
  const b = display('b');
  b.on('closed', info => closed.push(info.code));
  await settle();
  assert.deepEqual(closed, ['forbidden', 'forbidden'], 'answered at once from what the server said');

  allowed = true;
  now.advance(11 * 60_000);
  const c = display('c');
  await settle();
  await settle();
  assert.equal(c.status, 'online', 'after ten minutes the relay asks again');
  assert.equal(relay.stats().stores.main.synced, true);
});

test('presence passes through the relay: the displays hear who is on the store, and what they share', async () => {
  const { settle, display, direct } = setup({ storeOptions: { presence: true } });
  const a = display('a');
  const b = direct('b');
  await settle();
  const ids = client => client.peers.map(p => p.replicaId).sort();
  assert.deepEqual(ids(a), ['a', 'b']);
  b.share({ editing: 'k' });
  await settle();
  assert.deepEqual(a.peers.find(p => p.replicaId === 'b').data, { editing: 'k' });
  a.share({ editing: 'j' });
  await settle();
  assert.deepEqual(b.peers.find(p => p.replicaId === 'a').data, { editing: 'j' }, 'and a display shares as itself');
  b.share(null);
  await settle();
  assert.equal(a.peers.find(p => p.replicaId === 'b').data, undefined);
  b.disconnect();
  await settle();
  assert.deepEqual(ids(a), ['a']);
});

test('a display the server takes back is closed, and the others stay', async () => {
  const { store, settle, display, hubs } = setup();
  const a = display('a');
  const b = display('b');
  await settle();
  const closed = [];
  a.on('closed', info => closed.push(info.code));
  store.closeSessions(session => session.user?.id === 'a', 'gone');
  await settle();
  assert.deepEqual(closed, ['evicted']);
  assert.equal(b.status, 'online');

  const c = display('c');
  await settle();
  c.on('closed', info => closed.push(info.code));
  assert.equal(hubs[0].revoke(user => user?.id === 'c', 'forbidden', 'left the team'), 1);
  await settle();
  assert.deepEqual(closed, ['evicted', 'forbidden']);
  b.state.tasks.x = { title: 'b carries on' };
  await settle();
  assert.equal(store.state.tasks.x.title, 'b carries on');
});

test('the relay refuses what is not a client\'s to send, by hand', async () => {
  const shape = { ...SHAPE, rules: { settings: false } };
  const { settle, relay } = setup({ shape, storeOptions: { rules: { settings: false } } });
  const heard = [];
  const socket = relay.session({ send: m => heard.push(m), credential: { id: 'u' }, key: 'u' });
  const last = () => heard.at(-1);
  socket.receive({ t: 'ping' });
  assert.equal(last().t, 'pong');
  socket.receive({ t: 'hello', store: '../etc' });
  assert.equal(last().code, 'invalid-store');
  socket.receive({ t: 'op', store: 'main', op: { replica: 'u', seq: 1, ts: 1, changes: [] } });
  assert.equal(last().code, 'rate-limited', 'say hello first');
  socket.receive({ t: 'hello', store: 'main', proto: PROTOCOL - 1, replica: 'u', ops: [] });
  assert.equal(last().code, 'outdated');
  socket.receive({ t: 'hello', store: 'main', proto: PROTOCOL, ops: [] });
  assert.equal(last().t, 'error');
  socket.receive({ t: 'hello', store: 'main', proto: PROTOCOL, replica: 'u', ops: [] });
  await settle();
  assert.equal(heard.find(m => m.t === 'snapshot')?.store, 'main');
  socket.receive({ t: 'hello', store: 'main', proto: PROTOCOL, replica: 'other', ops: [] });
  assert.equal(last().code, 'forbidden');
  socket.receive({ t: 'nonsense', store: 'main' });
  assert.match(last().message, /Unknown message type/);
  const op = (changes, extra = {}) => socket.receive({ t: 'op', store: 'main', op: { replica: 'u', seq: 1, ts: 1_000_000, changes, ...extra } });
  op([], { seq: 0 });
  assert.equal(last().code, 'invalid');
  op([], { replica: 'other' });
  assert.equal(last().code, 'forbidden');
  op([{ c: 'nope', id: 'x', put: {} }]);
  assert.equal(last().code, 'invalid');
  op([{ c: 'settings', path: ['theme'], set: 'dark' }]);
  assert.equal(last().code, 'forbidden', 'the rules the relay was given');
  socket.receive({ t: 'leave', store: 'main' });
  socket.close();
  socket.receive({ t: 'ping' });
  assert.equal(last().code, 'forbidden', 'and nothing after it is closed');
});

test('a relay\'s copies in memory keep its replica, and a stats line per store', async () => {
  const storage = memoryCopies();
  const { relay, settle, display } = setup({ relayOptions: { storage } });
  display('a');
  await settle();
  assert.equal(storage.loadMeta().replicaId, relay.replicaId);
  assert.equal(storage.load('main').v, relay.stats().stores.main.v);
  assert.throws(() => relay.on('nope', () => {}), TypeError);
  relay.close();
  relay.close();
});

test('a client the server asks to authenticate again has its socket on the relay closed, as its own socket would be', async () => {
  const { settle, display, hubs, site } = setup();
  const a = display('a');
  const b = display('b');
  await settle();
  void site;
  assert.equal(hubs[0].revoke(user => user?.id === 'a', 'reauthenticate', 'sign in again'), 1);
  await settle();
  assert.equal(a.link.current, null, 'its socket closed: it comes back judged afresh');
  assert.equal(a.closed, null, 'not a final word');
  assert.equal(b.status, 'online');
});

test('in local mode a tab shares as the browser, and the server lists one peer for it', async () => {
  const { settle, display, direct } = setup({ relayOptions: { acknowledge: 'local' }, storeOptions: { presence: true } });
  const tab = display('tab');
  const other = direct('other');
  await settle();
  tab.share({ editing: 'k' });
  await settle();
  const browser = other.peers.find(p => p.replicaId !== 'other');
  assert.ok(browser, 'the browser is a peer');
  assert.deepEqual(browser.data, { editing: 'k' });
  assert.equal(other.peers.length, 2);
});

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

test('a store no client has open is let go of after linger: its session at the server left, and opened again from the copy', async () => {
  const { store, relay, settle, display } = setup({ relayOptions: { linger: 20 } });
  const a = display('a');
  await settle();
  a.state.tasks.k = { title: 'kept' };
  await settle();
  assert.equal(store.stats().sessions, 1, 'the relay\'s session on the store');
  a.disconnect();
  await settle();
  assert.ok(relay.stats().stores.main, 'kept while it lingers');
  await wait(40);
  await settle();
  assert.equal(relay.stats().stores.main, undefined, 'let go of');
  assert.equal(store.stats().sessions, 0, 'and the server holds no session for it');
  const b = display('b');
  await settle();
  assert.equal(b.status, 'online');
  assert.equal(b.state.tasks.k.title, 'kept');
  assert.equal(store.stats().sessions, 1);
});

test('a client that comes back while the store lingers keeps it, and the relay\'s own unacknowledged ops hold it', async () => {
  const { store, relay, settle, display, uplink } = setup({ relayOptions: { linger: 20 } });
  const a = display('a');
  await settle();
  a.disconnect();
  await settle();
  const b = display('b');
  await settle();
  await wait(40);
  await settle();
  assert.ok(relay.stats().stores.main, 'a client is on it again');
  b.disconnect();
  uplink.goOffline();
  await settle();
  relay.patch('main', { tasks: { r: { by: 'the relay' } } });
  await wait(40);
  await settle();
  assert.ok(relay.stats().stores.main, 'its own op waits for the server, and the store with it');
  uplink.goOnline();
  relay.upstreamUp();
  await settle();
  assert.equal(store.state.tasks.r.by, 'the relay');
  await wait(60);
  await settle();
  assert.equal(relay.stats().stores.main, undefined, 'let go of once the server has it');
});

test('linger false keeps every store the relay has opened', async () => {
  const { relay, settle, display } = setup({ relayOptions: { linger: false } });
  const a = display('a');
  await settle();
  a.disconnect();
  await settle();
  await wait(30);
  assert.ok(relay.stats().stores.main);
  assert.throws(() => createRelay({ shapeOf: () => SHAPE, link: () => ({}), linger: -1 }), /linger/);
});

test('the server holds back a relay that sends credentials it turns away, and lets those it accepts through', async () => {
  let now = 1_000_000;
  const store = createStore(SHAPE);
  const sent = [];
  const hub = createHub(() => store, {
    send: m => sent.push(m),
    user: { id: 'relay' },
    vouch: credential => (credential.ok ? { id: credential.id } : null),
    vouchRate: { burst: 2, perSecond: 1 },
    now: () => now
  });
  hub.receive({ t: 'hello', store: 'main', proto: PROTOCOL, replica: 'relay', ops: [], follower: true });
  const ask = (as, credential) => hub.receive({ t: 'vouch', store: 'main', as, credential, replica: `r-${as}` });
  const answer = async as => {
    for (let i = 0; i < 20 && !sent.some(m => m.as === as && (m.t === 'admitted' || m.t === 'refused')); i++) await new Promise(resolve => setImmediate(resolve));
    store.flush();
    return sent.find(m => m.as === as && (m.t === 'admitted' || m.t === 'refused'));
  };
  ask('good1', { ok: true, id: 'ann' });
  assert.equal((await answer('good1')).t, 'admitted');
  ask('bad1', {});
  ask('bad2', {});
  assert.equal((await answer('bad1')).code, 'unauthorized');
  assert.equal((await answer('bad2')).code, 'unauthorized');
  ask('good2', { ok: true, id: 'bo' });
  const limited = await answer('good2');
  assert.equal(limited.code, 'rate-limited', 'two turned away spend the bucket: every vouch waits');
  assert.equal(limited.retryAfter, 1000);
  now += 1000;
  ask('good3', { ok: true, id: 'bo' });
  assert.equal((await answer('good3')).t, 'admitted', 'a token back, and one let in keeps it');
  ask('good4', { ok: true, id: 'cy' });
  assert.equal((await answer('good4')).t, 'admitted');
  assert.throws(() => createHub(() => store, { send() {}, vouchRate: { burst: 0, perSecond: 1 } }), /vouchRate/);
  hub.close();
});

test('a relay told to wait asks again, and its client is let in then', async () => {
  const { settle, display } = setup({ hubOptions: { vouch: credential => (credential.bad ? null : credential), vouchRate: { burst: 1, perSecond: 50 } } });
  const closed = [];
  const bad = display('bad', { id: 'bad', bad: true });
  bad.on('closed', info => closed.push(info.code));
  await settle();
  assert.deepEqual(closed, ['unauthorized']);
  const good = display('good');
  await settle();
  for (let i = 0; i < 20 && good.status !== 'online'; i++) {
    await new Promise(resolve => setTimeout(resolve, 10));
    await settle();
  }
  assert.equal(good.status, 'online', 'admitted once the bucket had a token again');
  assert.deepEqual(closed, ['unauthorized']);
});

test('a copy opened again from storage passes on what the server changed meanwhile to a display it answered while away', async () => {
  // Found by the relay fuzzer with linger 0: a copy made from storage shared its records with its view, so a
  // change from the server landed in both at once, and the relay saw nothing to send its displays
  const { store, relay, settle, display, uplink } = setup({ relayOptions: { linger: 0 } });
  const a = display('a', { id: 'a' });
  a.state.tasks.k = { title: 'x' };
  await settle();
  a.disconnect();
  await settle();
  await wait(10);
  await settle();
  assert.equal(relay.stats().stores.main, undefined, 'let go of');
  uplink.goOffline();
  await settle();
  store.patch({ tasks: { k: { done: true } } });
  const again = display('a', { id: 'a' });
  await settle();
  assert.equal(again.status, 'online', 'answered from the copy, the server away');
  assert.equal(again.state.tasks.k.done, undefined);
  uplink.goOnline();
  relay.upstreamUp();
  await settle();
  assert.equal(again.state.tasks.k.done, true, 'the server\'s change reaches the display');
  assert.deepEqual(LazyWatch.snapshot(again.state), store.snapshot());
});

test('a store a display may not read reaches neither the display nor the relay', async () => {
  const { store, relay, settle, display } = setup({
    relayOptions: { linger: false },
    hubOptions: { authorizeId: (user, id) => user?.id !== 'stranger' }
  });
  store.patch({ tasks: { k: { title: 'not for strangers' } } });
  const closed = [];
  const stranger = display('stranger');
  stranger.on('closed', info => closed.push(info.code));
  await settle();
  assert.deepEqual(closed, ['forbidden']);
  assert.equal(relay.copy('main')?.tasks?.k, undefined, 'the relay was told nothing of the store');
  const friend = display('friend');
  await settle();
  assert.equal(friend.state.tasks.k.title, 'not for strangers', 'and is once a client of it is let in');
});

test('every vouch the server turns away counts, a store the client may not open as well as a credential it does not take', async () => {
  let now = 1_000_000;
  const store = createStore(SHAPE);
  const sent = [];
  const hub = createHub(() => store, {
    send: m => sent.push(m),
    user: { id: 'relay' },
    vouch: credential => ({ id: credential.id }),
    authorizeId: (user, id) => user.id === 'relay' || user.id === 'ann',
    vouchRate: { burst: 2, perSecond: 1 },
    now: () => now
  });
  hub.receive({ t: 'hello', store: 'main', proto: PROTOCOL, replica: 'relay', ops: [], follower: true });
  const answer = async as => {
    for (let i = 0; i < 20 && !sent.some(m => m.as === as && (m.t === 'admitted' || m.t === 'refused')); i++) await new Promise(resolve => setImmediate(resolve));
    return sent.find(m => m.as === as && (m.t === 'admitted' || m.t === 'refused'));
  };
  const ask = (as, id) => hub.receive({ t: 'vouch', store: 'main', as, credential: { id }, replica: `r-${as}` });
  ask('b1', 'bo');
  assert.equal((await answer('b1')).code, 'forbidden');
  ask('b2', 'bo');
  assert.equal((await answer('b2')).code, 'forbidden');
  ask('a1', 'ann');
  assert.equal((await answer('a1')).code, 'rate-limited', 'two refused spend the bucket, whatever the reason');
  now += 1000;
  ask('a2', 'ann');
  assert.equal((await answer('a2')).t, 'admitted');
  hub.close();
});

test('the relay hands storage its copy without building its records, which are built once, when storage reads them', async () => {
  let saves = 0;
  let last = null;
  const storage = { loadMeta: () => null, saveMeta () {}, load: () => null, save (id, copy) { saves++; last = copy; }, flush () {} };
  const { store, settle, display } = setup({ relayOptions: { storage } });
  const a = display('a');
  await settle();
  for (let i = 0; i < 20; i++) {
    a.state.tasks['k' + i] = { n: i };
    await settle();
  }
  assert.ok(saves > 20, 'saved with every change');
  const lazy = Object.getOwnPropertyDescriptor(last, 'records');
  assert.equal(typeof lazy.get, 'function', 'its records a getter, not built');
  const records = last.records;
  assert.equal(last.records, records, 'built once');
  assert.equal(records.filter(([c]) => c === 'tasks').length, 20);
  assert.deepEqual(Object.fromEntries(records.filter(([c]) => c === 'tasks').map(([, id, data]) => [id, data])), store.snapshot().tasks);
});
