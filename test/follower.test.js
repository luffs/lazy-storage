// follower.test.js - The relay as a follower store: its own writes, the host's
// word on the server, copies kept on disk, stores it may not carry, presence
// and evictions passed on, and what it refuses on its own
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub, memoryStorage } from '../src/server/index.js';
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
    assert.ok(existsSync(join(dir, '_relay.json')));
    a.dispose();

    uplink.goOffline();                              // the server is away when the relay comes back
    const second = createRelay({ ...options, storage: fileCopies(dir, { saveDelay: 0 }) });
    assert.equal(second.replicaId, replicaId, 'the same replica, from _relay.json');
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
  assert.equal(typeof Object.getOwnPropertyDescriptor(last, 'admitted').get, 'function', 'and who was admitted, an entry per client, built when read too');
  assert.ok(Object.keys(last.admitted).length >= 1);
  assert.ok(JSON.parse(JSON.stringify(last)).admitted, 'written out as plain data');
  const records = last.records;
  assert.equal(last.records, records, 'built once');
  assert.equal(records.filter(([c]) => c === 'tasks').length, 20);
  assert.deepEqual(Object.fromEntries(records.filter(([c]) => c === 'tasks').map(([, id, data]) => [id, data])), store.snapshot().tasks);
});

test('on the adapters\' relay route the relay writes as itself only where relays.authorize lets it carry the store', async () => {
  const { followerHub } = await import('../src/server/follow.js');
  for (const authorizeRelay of [undefined, () => true]) {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now });
    const central = createNetwork({
      session: ({ send, user }) => followerHub(() => store, { send, relay: user, admit: async credential => ({ user: credential }), authorizeRelay })
    });
    const relay = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: 'relay' }).factory, grace: 0, reconnect: false, keepalive: false, now, onError: () => {} });
    const site = createNetwork(relay);
    const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); } };
    site.client({ replicaId: 'a', ...SHAPE, now }, { user: 'a' });
    await settle();
    relay.patch('main', { tasks: { k: { by: 'the relay' } } });
    await settle();
    if (authorizeRelay) assert.equal(store.state.tasks.k?.by, 'the relay', 'let carry it, it writes');
    else assert.equal(store.state.tasks.k, undefined, 'without authorize it writes nothing as itself');
  }
});

test('the relay route tells the relay from its clients by how it asks, not by an equal user', async () => {
  // Users that are plain strings: a client whose id is the relay's must still face the app's authorize
  const { followerHub } = await import('../src/server/follow.js');
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const asked = [];
  const central = createNetwork({
    session: ({ send, user }) => followerHub(() => store, {
      send, relay: user, admit: async credential => ({ user: credential.id }),
      authorize: (user, id, s, context) => { asked.push([user, context?.relay]); return false; }
    })
  });
  const relay = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: 'same' }).factory, grace: 0, reconnect: false, keepalive: false, now, onError: () => {} });
  const site = createNetwork(relay);
  const db = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'same' } });
  for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); }
  assert.deepEqual(asked, [['same', 'same']], 'the client is judged by authorize, the relay is not');
  assert.notEqual(db.status, 'online');
});

test('a relay\'s files keep a fingerprint of a credential, never the credential, and a store named relay is a store like any', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-relay-files-'));
  try {
    const now = fakeTime(1_000_000);
    const store = createStore({ ...SHAPE, now });
    const central = createNetwork({ session: ({ send, user }) => createHub(() => store, { send, user, vouch: credential => ({ id: credential.token.slice(0, 1) }) }) });
    const options = { shapeOf: () => SHAPE, link: central.link({ user: { id: 'relay' } }).factory, grace: 0, reconnect: false, keepalive: false, now };
    const relay = createRelay({ ...options, storage: fileCopies(dir, { saveDelay: 0 }) });
    const site = createNetwork(relay);
    const db = site.client({ replicaId: 'a', ...SHAPE, now, store: 'relay' }, { user: { token: 'a-secret-bearer-token' } });
    for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); }
    db.state.tasks.k = { title: 'x' };
    for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); }
    // Written by the time the relay is closed: saveDelay 0 is still a timer, which a fast machine reads the directory before
    relay.close();
    const files = readdirSync(dir).sort();
    assert.deepEqual(files, ['_relay.json', 'relay.json']);
    for (const file of files) assert.ok(!readFileSync(join(dir, file), 'utf8').includes('a-secret-bearer-token'), `${file} holds no credential`);
    const copy = JSON.parse(readFileSync(join(dir, 'relay.json'), 'utf8'));
    assert.match(Object.keys(copy.admitted)[0], /^sha256:[0-9a-f]{64}$/);
    assert.equal(JSON.parse(readFileSync(join(dir, '_relay.json'), 'utf8')).replicaId, relay.replicaId);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a store the server could not open at first is opened again with every client vouched for afresh', async () => {
  // A deploy's overlap: the store is locked elsewhere, so the hello is answered unavailable and the vouch sent after it is dropped
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  let locked = true;
  const resolve = () => {
    if (!locked) return store;
    locked = false;
    throw Object.assign(new Error('Store "main" is held by another process'), { code: 'store-locked' });
  };
  const central = createNetwork({ session: ({ send, user }) => createHub(resolve, { send, user, vouch: credential => credential, authorizeId: async () => true, onError: () => {} }) });
  const relay = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: { id: 'relay' } }).factory, grace: 60_000, reconnect: false, keepalive: false, now, onError: () => {} });
  const site = createNetwork(relay);
  const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); } };
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  await settle();
  await new Promise(done => setTimeout(done, 1100));
  await settle();
  assert.equal(a.status, 'online', 'answered once the store opens');
  a.state.tasks.k = { title: 'after' };
  await settle();
  assert.equal(store.state.tasks.k?.title, 'after');
});

test('a vouch the server dropped with its session is asked again on the next', async () => {
  // The server's session closed unavailable, taking the vouch in flight: the grant is not left waiting for an answer that never comes
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  let swallowed = false;
  const central = createNetwork({
    session: ({ send, user }) => {
      const hub = createHub(() => store, { send, user, vouch: credential => credential });
      return {
        receive(msg) {
          if (msg.t === 'vouch' && !swallowed) {
            swallowed = true;
            return send({ t: 'closed', store: msg.store, code: 'unavailable', message: 'The store was unloaded; say hello again' });
          }
          hub.receive(msg);
        },
        close: () => hub.close()
      };
    }
  });
  const relay = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: { id: 'relay' } }).factory, grace: 60_000, reconnect: false, keepalive: false, now, onError: () => {} });
  const site = createNetwork(relay);
  const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); } };
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  await settle();
  await new Promise(done => setTimeout(done, 1100));
  await settle();
  assert.ok(swallowed);
  assert.equal(a.status, 'online');
});

test('a copy whose ops are of another shape (a browser\'s storage from before 1.0) is let go of, and the store is served', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const central = createNetwork({ session: ({ send, user }) => createHub(() => store, { send, user, vouch: credential => credential }) });
  const old = { v: 3, epoch: 'old', own: { seq: 1, ops: [{ replicaId: 'x', seq: 1, ts: 5, diff: { tasks: { k: { title: 'old' } } } }] }, admitted: {} };
  const faults = [];
  const storage = { loadMeta: () => null, saveMeta: () => {}, load: () => Promise.resolve(old), save: () => {}, flush: () => {} };
  const relay = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: { id: 'relay' } }).factory, storage, grace: 60_000, reconnect: false, keepalive: false, now, onError: err => faults.push(err) });
  const site = createNetwork(relay);
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); }
  assert.equal(a.status, 'online');
  a.state.tasks.n = { title: 'new' };
  for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); }
  assert.equal(store.state.tasks.n?.title, 'new');
  assert.equal(store.state.tasks.k, undefined);
  assert.deepEqual(faults, []);
});

test('an op a client sent up before it left goes up again when it comes back, though the link dropped meanwhile', async () => {
  // The server took the op but said nothing; the client left; the link dropped. Nothing must hold on to the op under the old session
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  let hold = true;
  const central = createNetwork({
    session: ({ send, user }) => {
      const hub = createHub(() => store, { send, user, vouch: credential => credential });
      return { receive: msg => (hold && msg.t === 'op' ? undefined : hub.receive(msg)), close: () => hub.close() };
    }
  });
  const uplink = central.link({ user: { id: 'relay' } });
  const relay = createRelay({ shapeOf: () => SHAPE, link: uplink.factory, grace: 60_000, keepalive: false, now, onError: () => {} });
  const site = createNetwork(relay);
  const settle = async () => { for (let i = 0; i < 20; i++) { await central.settle(); await site.settle(); } };
  const outbox = memoryOutbox();
  const a = site.client({ replicaId: 'a', ...SHAPE, now, storage: outbox }, { user: { id: 'a' } });
  await settle();
  a.state.tasks.k = { title: 'kept' };
  await settle();
  assert.equal(relay.stats().stores.main.waiting.length, 1, 'sent up, not acknowledged');
  a.dispose();
  await settle();
  uplink.goOffline();
  await settle();
  hold = false;
  uplink.goOnline();
  const b = site.client({ replicaId: 'a', ...SHAPE, now, storage: outbox }, { user: { id: 'a' } });
  for (let i = 0; i < 30 && store.state.tasks.k === undefined; i++) {
    await new Promise(done => setTimeout(done, 100));
    await settle();
  }
  assert.equal(store.state.tasks.k?.title, 'kept');
  assert.equal(b.isPending(['tasks', 'k']), false);
  relay.close();
});

test('a patch to many displays is encoded once, tagged per display as the hub tags', async () => {
  const { store, relay, settle } = setup();
  const sent = [];
  const sessions = [];
  for (let i = 0; i < 3; i++) {
    const session = relay.session({ send: m => sent.push(m), credential: { id: 'd' + i } });
    session.receive({ t: 'hello', store: 'main', proto: PROTOCOL, replica: 'd' + i, ops: [] });
    sessions.push(session);
  }
  await settle();
  sent.length = 0;
  const stringify = JSON.stringify;
  let encoded = 0;
  JSON.stringify = function (value, ...rest) {
    if (value && typeof value === 'object' && value.t === 'patch') encoded++;
    return stringify.call(this, value, ...rest);
  };
  try {
    store.patch({ tasks: { k: { title: 'to everyone' } } });
    await settle();
    const patches = sent.filter(m => m.t === 'patch');
    assert.equal(patches.length, 3);
    assert.ok(patches.every(m => m.store === 'main'));
    const { toJSON } = await import('../src/server/wire.js');
    for (const m of patches) toJSON(m);
    // Once the server's, to the relay; once the relay's, whose three copies are spliced from it (four and more with a stringify per display)
    assert.equal(encoded, 2);
  } finally {
    JSON.stringify = stringify;
    for (const s of sessions) s.close();
  }
});

// A relay restarted from its copies takes its time from the newest stamp
// they hold, as the server does from its records: while the server is away
// it decides its clients' edits on its own, and a clock that started at the
// wall's time would clamp an edit made right after a burst of the server's
// writes back below them, and show it lost when the server will keep it
test('a relay restarted from its copies during an outage keeps its time after the stamps they hold', async () => {
  const now = fakeTime(1_000_000);
  const store = createStore({ ...SHAPE, now });
  const central = createNetwork(store, { vouch: credential => credential });
  const uplink = central.link({ user: { id: 'relay' } });
  const storage = memoryCopies();
  const options = { shapeOf: () => SHAPE, link: uplink.factory, storage, grace: 0, reconnect: false, keepalive: false, now };
  const first = createRelay(options);
  let site = createNetwork(first);
  const settle = async () => {
    for (let i = 0; i < 20; i++) {
      await central.settle();
      await site.settle();
      if (central.pending === 0 && site.pending === 0) return;
    }
  };
  const a = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  await settle();
  for (let i = 0; i < 5; i++) store.patch({ tasks: { [`x${i}`]: { n: i } } });
  store.patch({ tasks: { k: { title: 'Milk' } } });
  await settle();
  assert.equal(first.copy('main').tasks.k.title, 'Milk');
  first.close();
  a.dispose();

  uplink.goOffline();
  const second = createRelay(options);
  site = createNetwork(second);
  const again = site.client({ replicaId: 'a', ...SHAPE, now }, { user: { id: 'a' } });
  await settle();
  assert.equal(again.relayed, true, 'answered from the copy');
  again.state.tasks.k.done = true;    // under a record the burst made
  again.state.tasks.x4.n = 40;        // on a path the burst wrote
  await settle();
  assert.equal(second.copy('main').tasks.k.done, true, 'the relay decides it as the server will');
  assert.equal(second.copy('main').tasks.x4.n, 40);

  uplink.goOnline();
  second.upstreamUp();
  await settle();
  assert.equal(store.state.tasks.k.done, true);
  assert.equal(store.state.tasks.x4.n, 40);
  second.close();
});
