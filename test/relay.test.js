// relay.test.js - A relay on the LAN: clients' sockets passed through to the server, and answered from a copy while it is away
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStore, createStores, createHub, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createConnection } from '../src/client/connection.js';
import { sharedConnection } from '../src/client/shared.js';
import { createRelay, memoryCopies, fileCopies } from '../src/relay/index.js';
import { createNetwork, fakeTime } from './helpers.js';
import { LazyWatch } from 'lazy-watch';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const plain = value => JSON.parse(JSON.stringify(value));
/** Each edit a batch of its own, and so an op of its own */
const ops = (db, n, edit) => { for (let i = 0; i < n; i++) { edit(i); LazyWatch.flush(db.wire); } };

/**
 * The server ("central") on a network of its own, a relay, and displays on
 * the LAN. Central's stores come from a registry, with presence on; every
 * link to central is one the relay dialled for a display, as that display's
 * user, and `goDown()` takes them all offline (and every new one), `goUp()`
 * brings them back. `central.heard` is every message central's hubs
 * received, `central.sessions` its sockets (to evict with a code), and
 * `central.filter(message, user)` may drop what central sends. Displays
 * reconnect on their own, quickly; `heard` is everything a display received
 */
function lan({ relay: relayOptions = {}, store: storeOptions = {}, hub: hubOptions = {}, storage = memoryCopies(), realTime = false } = {}) {
  // A shared fake clock, unless a client that cannot be handed one (a browser's replica) is in the test
  const time = realTime ? Object.assign(() => Date.now(), { advance() {} }) : fakeTime(1_000_000);
  const storages = new Map();
  const stores = createStores(id => {
    if (id.startsWith('unknown')) return null;
    if (!storages.has(id)) storages.set(id, memoryStorage());
    return createStore({ initial: INITIAL, presence: true, now: time, storage: storages.get(id), ...storeOptions });
  });
  const central = {
    stores,
    heard: [],
    links: new Set(),
    sessions: new Set(),
    down: false,
    filter: null,
    refuse: null,         // (user) => true: that user's dials fail, as for a client whose way to the server is broken
    store: (id = 'main') => stores.get(id),
    hellos: (user, store = 'main') => central.heard.filter(h => h.user === user && h.message.t === 'hello' && h.message.store === store).map(h => h.message),
    goDown() {
      central.down = true;
      for (const link of central.links) link.goOffline();
    },
    goUp() {
      central.down = false;
      for (const link of central.links) link.goOnline();
    }
  };
  central.net = createNetwork({
    session: ({ send, user, onEvict }) => {
      const filtered = message => { if (!central.filter || central.filter(message, user)) send(message); };
      const hub = createHub(id => stores.get(id), { send: filtered, user, ...hubOptions });
      const entry = { user, send: filtered, onEvict };
      central.sessions.add(entry);
      return {
        receive(message) {
          central.heard.push({ user: user?.id, message });
          hub.receive(message);
        },
        close() {
          central.sessions.delete(entry);
          hub.close();
        }
      };
    }
  });
  const env = { central, storage, time, displays: [] };
  const upstreamFor = user => {
    const link = central.net.link({ user });
    central.links.add(link);
    if (central.down) link.goOffline();
    // A client refused is refused at each dial, and let through again once it is not
    return () => {
      if (central.refuse?.(user)) link.goOffline();
      else if (!central.down && !link.online) link.goOnline();
      return link.factory();
    };
  };
  env.makeRelay = (options = {}) => createRelay({
    storage, grace: 0, probeEvery: 5, dialTimeout: 200, keepalive: false, jitter: 0, saveDelay: 5, now: time, onError: err => { throw err; },
    ...relayOptions, ...options
  });
  env.relay = env.makeRelay();
  env.net = createNetwork({
    session: ({ send, user, onEvict }) => env.relay.accept({ send, close: (code, reason) => onEvict(code, reason), key: user.key, upstream: upstreamFor(user) })
  });
  env.until = async (pred, label) => {
    for (let i = 0; i < 800; i++) {
      await central.net.settle();
      await env.net.settle();
      await central.net.settle();
      if (pred()) return;
      await sleep(2);
    }
    throw new Error(`timeout: ${label}`);
  };
  /** A socket on the LAN as `name` (credential `key-<name>`), and what it heard */
  env.socket = (name, key = `key-${name}`) => {
    const user = { id: name, key };
    const link = env.net.link({ user });
    const heard = [];
    const factory = () => {
      const t = link.factory();
      const tap = { onopen: null, onmessage: null, onclose: null, send: m => t.send(m), close: () => t.close() };
      t.onopen = () => tap.onopen?.();
      t.onmessage = m => { heard.push(m); tap.onmessage?.(m); };
      t.onclose = info => tap.onclose?.(info);
      return tap;
    };
    const connection = createConnection({ transport: factory, reconnect: { min: 2, max: 10 }, keepalive: false, wake: false });
    return { connection, link, heard, user };
  };
  /** A display: a client on its own socket, store 'main' and replica `r-<name>` unless given */
  env.display = (name, { key, store = 'main', replicaId = `r-${name}`, socket, ...options } = {}) => {
    socket ??= env.socket(name, key);
    const db = createClient({ connection: socket.connection, store, initial: INITIAL, replicaId, now: env.time, ...options });
    Object.assign(db, { name, link: socket.link, heard: socket.heard, user: socket.user, socket });
    db.connect();
    env.displays.push(db);
    return db;
  };
  /** What a display heard for its store, from index `from` on */
  env.answers = (db, from = 0) => db.heard.slice(from).filter(m => m.store === db.store && (m.t === 'snapshot' || m.t === 'delta'));
  env.close = () => {
    for (const db of env.displays) db.dispose();
    env.relay.close();
    stores.dispose();
  };
  return env;
}

const online = (...dbs) => () => dbs.every(db => db.status === 'online');

test('through: a socket is passed through as it is: the server sees each display as itself, and it alone acknowledges', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'both online');
  const store = env.central.store();
  const users = [];
  store.observe('op', e => users.push(e.user?.id));
  assert.equal(env.relay.mode, 'through');
  assert.equal(store.sessions, 2, 'a session per display, as without a relay');
  assert.deepEqual(store.replicas.sort(), ['r-a', 'r-b']);
  assert.equal(store.export().replicas['r-a'].owner, 'a', 'each replica is its display\'s user\'s');
  assert.deepEqual(a.presence.map(u => u.id).sort(), ['a', 'b']);
  assert.deepEqual(a.peers.map(p => p.replicaId).sort(), ['r-a', 'r-b']);

  a.state.tasks.x = { id: 'x', title: 'from a' };
  b.state.tasks.y = { id: 'y', title: 'from b' };
  await env.until(() => a.state.tasks.y && b.state.tasks.x && a.pending + b.pending === 0, 'the edits crossed');
  assert.deepEqual(users.sort(), ['a', 'b'], 'each op is its author\'s');
  const acks = [...a.heard, ...b.heard].filter(m => m.t === 'ack').length;
  assert.equal(acks, store.stats().sent.ack.messages, 'every ack a display heard is one the server sent');
  assert.ok(env.relay.sockets().every(s => s.state === 'through'));
  assert.equal(a.relayed, false);
});

test('the copy follows: the server\'s state, version and epoch, each patch once, and a delta sliced from where the copy stands, corrections included', async t => {
  const env = lan();
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  let changes = 0;
  env.relay.on('copy', () => changes++);
  a.state.tasks.x = { id: 'x', title: 'one' };
  await env.until(() => b.state.tasks.x?.title === 'one', 'reached b');
  store.patch({ tasks: { y: { id: 'y', title: 'the server\'s' } } });
  await env.until(() => env.relay.copy('main').v === store.version, 'the copy caught up');
  let copy = env.relay.copy('main');
  assert.deepEqual(copy.state, store.snapshot());
  assert.equal(copy.epoch, store.epoch);
  assert.equal(copy.live, true);
  assert.equal(changes, 2, 'two versions, each once, though both sockets carried both');

  // b away; the server moves on while a follows; then a is away too and
  // the server moves on with nobody following. b comes back with an edit
  // that loses to the server's newer one: the delta it gets is the two
  // versions and a correction, of which the copy takes what it lacks
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b offline');
  b.state.tasks.x.title = 'b, offline, early';
  env.time.advance(1000);
  store.patch({ tasks: { x: { title: 'the server, later' } } });
  await env.until(() => env.relay.copy('main').v === store.version && a.state.tasks.x.title === 'the server, later', 'a and the copy have it');
  a.link.goOffline();
  await env.until(() => a.status === 'offline' && env.relay.copy('main').live === false, 'nobody follows');
  store.patch({ tasks: { z: { id: 'z', title: 'while nobody followed' } } });
  const conflicts = [];
  b.on('conflict', c => conflicts.push(c));
  const from = b.heard.length;
  b.link.goOnline();
  b.connection.connect();
  await env.until(() => b.status === 'online' && b.pending === 0, 'b back');
  const [answer] = env.answers(b, from);
  assert.equal(answer.t, 'delta', 'the copy could use a delta, so the hello was left as it was');
  assert.equal(answer.patches.length, 3, 'two versions and a correction');
  assert.equal(conflicts.length, 1, 'b hears its edit lost');
  copy = env.relay.copy('main');
  assert.equal(copy.v, store.version);
  assert.equal(copy.live, true);
  assert.deepEqual(copy.state, store.snapshot());
  assert.equal(copy.state.tasks.x.title, 'the server, later');
});

test('the copy resyncs: after a patch it missed, after a restart that lost its last writes, and after the last display on it left', async t => {
  const env = lan({ relay: { saveDelay: 60_000 } });
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');

  // A patch nobody got: the copy stops following, the displays catch up, and the copy with them
  const lost = store.version + 1;
  env.central.filter = message => message.t !== 'patch' || message.v !== lost;
  a.state.tasks.x = { id: 'x', title: 'lost on the way' };
  await env.until(() => store.version === lost && a.pending === 0, 'the server has it');
  assert.equal(env.relay.copy('main').v, lost - 1, 'the copy never saw it');
  a.state.tasks.y = { id: 'y', title: 'after it' };
  await env.until(() => b.state.tasks.x && b.state.tasks.y && env.relay.copy('main').v === store.version, 'everyone caught up');
  assert.equal(env.relay.copy('main').live, true);
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());
  env.central.filter = null;

  // A restart that lost the copy's last writes: the displays are ahead of
  // it, so their hellos go up without `since` and the copy takes the snapshot
  env.relay.flush();
  const stale = env.storage.load();
  a.state.tasks.z = { id: 'z', title: 'not written' };
  await env.until(() => env.relay.copy('main').v === store.version && b.state.tasks.z, 'the copy has it in memory');
  env.relay.close();
  env.relay = env.makeRelay({ storage: { load: () => stale, write() {}, writeKnown() {}, remove() {} } });
  assert.equal(env.relay.copy('main').live, false, 'a copy loaded is not live');
  assert.ok(env.relay.copy('main').v < store.version);
  const mark = env.central.heard.length;
  await env.until(() => online(a, b)() && env.relay.copy('main').live, 'back through the new relay');
  const hellos = env.central.heard.slice(mark).map(h => h.message).filter(m => m.t === 'hello' && m.store === 'main');
  assert.equal(hellos[0].since, undefined, 'the first hello went up stripped, for a snapshot the copy could take');
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());

  // The last display on the store leaves it (the socket stays for another
  // store): the copy stops following, and catches up when one says hello again
  const side = createClient({ connection: a.socket.connection, store: 'side', initial: INITIAL, replicaId: 'r-a-side', now: env.time });
  side.connect();
  t.after(() => side.dispose());
  b.disconnect();
  await env.until(() => side.status === 'online', 'side open');
  a.disconnect();
  await env.until(() => env.relay.copy('main').live === false, 'nobody follows main');
  assert.equal(a.socket.connection.status, 'online', 'the socket stays for the other store');
  store.patch({ tasks: { w: { id: 'w', title: 'meanwhile' } } });
  a.connect();
  await env.until(() => a.status === 'online' && env.relay.copy('main').live, 'following again');
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());
});

test('local: answers carry seq 0 and never acknowledge; the first edit makes every display say hello again, to epoch null; edits cross between displays and stay pending', async t => {
  const env = lan();
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  a.state.tasks.x = { id: 'x', title: 'before' };
  await env.until(() => b.state.tasks.x && a.pending === 0, 'synced');
  const from = { a: a.heard.length, b: b.heard.length };
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && online(a, b)() && a.relayed && b.relayed, 'answered locally');
  assert.deepEqual(modes, ['local']);
  for (const [db, i] of [[a, from.a], [b, from.b]]) {
    const [answer] = env.answers(db, i);
    assert.equal(answer.t, 'delta', 'nobody edited it: an empty delta');
    assert.equal(answer.epoch, store.epoch, 'under the server\'s epoch, which it still is');
    assert.equal(answer.v, store.version);
    assert.equal(answer.seq, 0);
  }

  const after = { a: a.heard.length, b: b.heard.length };
  a.state.tasks.x.title = 'offline, from a';
  await env.until(() => b.state.tasks.x.title === 'offline, from a' && online(a, b)(), 'b has it');
  for (const [db, i] of [[a, after.a], [b, after.b]]) {
    const heard = db.heard.slice(i);
    assert.ok(heard.some(m => m.t === 'closed' && m.code === 'unavailable'), `${db.name} was told to say hello again`);
    const answers = env.answers(db, i);
    const last = answers.at(-1);
    assert.equal(last.epoch, null, 'answers of the relay\'s own, now');
    assert.ok(Number.isInteger(last.v));
    assert.ok(answers.every(m => m.seq === 0));
  }
  b.state.tasks.y = { id: 'y', title: 'offline, from b' };
  await env.until(() => a.state.tasks.y?.title === 'offline, from b', 'a has b\'s');
  assert.equal(a.pending, 1, 'a\'s edit is pending');
  assert.equal(b.pending, 1, 'b\'s too');
  for (const db of [a, b]) {
    const heard = db.heard.slice(from[db.name]);
    assert.ok(!heard.some(m => m.t === 'ack' || (m.t === 'error' && m.seq !== undefined) || m.lost), `${db.name} was acknowledged nothing`);
    assert.equal(db.status, 'online');
    assert.equal(db.relayed, true);
  }
  const copy = env.relay.copy('main');
  assert.equal(copy.diverged, true);
  assert.equal(copy.local, 2);
  assert.equal(copy.state.tasks.x.title, 'offline, from a');
  assert.equal(store.snapshot().tasks.x.title, 'before', 'the server has none of it yet');
});

test('local: last writer wins by stamp, whichever edit arrives first', async t => {
  for (const order of ['earlier first', 'later first']) {
    const env = lan();
    // b's clock runs half a second ahead of a's
    const a = env.display('a', { now: fakeTime(1_000_000) });
    const b = env.display('b', { now: fakeTime(1_000_500) });
    const c = env.display('c');
    await env.until(online(a, b, c), 'online');
    a.state.tasks.x = { id: 'x', title: 'first' };
    await env.until(() => b.state.tasks.x && c.state.tasks.x && a.pending === 0, 'synced');
    env.central.goDown();
    await env.until(() => a.relayed && b.relayed && c.relayed, 'local');
    c.state.tasks.w = { id: 'w' };   // the copy diverges first, so the two below meet an answered session each
    await env.until(() => env.relay.copy('main').local === 1 && online(a, b, c)() && a.state.tasks.w && b.state.tasks.w, 'diverged');
    // Both in one turn, so neither has seen the other's before stamping its own
    const [first, second] = order === 'earlier first' ? [a, b] : [b, a];
    first.state.tasks.x.title = `from ${first.name}`;
    LazyWatch.flush(first.wire);
    second.state.tasks.x.title = `from ${second.name}`;
    await env.until(() => c.state.tasks.x.title === 'from b' && env.relay.copy('main').state.tasks.x.title === 'from b' && online(a, b, c)(), `${order}: the later stamp holds`);
    assert.equal(b.state.tasks.x.title, 'from b');
    assert.equal(env.relay.copy('main').local, order === 'earlier first' ? 3 : 2, 'the earlier stamp, arriving second, changed nothing');
    env.close();
  }
});

test('back online: each display\'s edits reach the server in its own hello, as its own: refused ones are reverted everywhere, lost ones raise a conflict', async t => {
  const env = lan({ store: { validate: (diff, { user }) => user?.id !== 'viewer' } });
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  const viewer = env.display('viewer');
  await env.until(online(a, b, viewer), 'online');
  a.state.tasks.x = { id: 'x', title: 'start' };
  await env.until(() => b.state.tasks.x && viewer.state.tasks.x && a.pending === 0, 'synced');
  const users = [];
  store.observe('op', e => { if (e.user) users.push(e.user.id); });
  const refusedTo = [];
  store.observe('refused', e => refusedTo.push(e.user?.id));

  env.central.goDown();
  await env.until(() => a.relayed && b.relayed && viewer.relayed, 'local');
  viewer.state.tasks.v = { id: 'v', title: 'the viewer may not' };
  await env.until(() => a.state.tasks.v && b.state.tasks.v && online(a, b, viewer)(), 'offline, it shows everywhere');
  a.state.tasks.x.title = 'a, offline';
  b.state.tasks.y = { id: 'y', title: 'b, offline' };
  await env.until(() => b.state.tasks.x.title === 'a, offline' && a.state.tasks.y && online(a, b, viewer)(), 'crossed');
  env.time.advance(1000);
  store.patch({ tasks: { x: { title: 'the server, later' } } });   // while the relay cannot see it
  const conflicts = [];
  a.on('conflict', c => conflicts.push(c));
  const rejected = [];
  viewer.on('rejected', r => rejected.push(r));
  const resets = [];
  for (const db of [a, b, viewer]) db.on('reset', r => resets.push(r));

  env.central.goUp();
  await env.until(() => env.relay.mode === 'through' && online(a, b, viewer)() && a.pending + b.pending + viewer.pending === 0 && !a.relayed, 'back through, nothing pending');
  assert.deepEqual([...new Set(users)].sort(), ['a', 'b'], 'each display\'s op reached the server as its own');
  assert.deepEqual(refusedTo, ['viewer'], 'and the viewer\'s was judged as the viewer\'s');
  assert.equal(store.export().replicas['r-a'].owner, 'a');
  assert.equal(rejected.length, 1, 'the viewer\'s edit was refused, and it heard so');
  assert.equal(conflicts.length, 1, 'a\'s edit lost to the server\'s later one');
  assert.equal(resets.length, 0, 'no reset: the relay\'s answers carried no epoch of their own');
  await env.until(() => [a, b, viewer].every(db => db.state.tasks.v === undefined && db.state.tasks.x.title === 'the server, later'), 'refused and lost edits reverted everywhere');
  for (const db of [a, b, viewer]) assert.deepEqual(plain(db.state), store.snapshot());
  assert.equal(store.snapshot().tasks.y.title, 'b, offline');
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());
  assert.equal(env.relay.copy('main').diverged, false);
});

test('no duplicates: an offline op resent after every answer is applied once, and reaches the server under the display\'s own seq', async t => {
  const env = lan();
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  ops(a, 5, i => { a.state.tasks[`t${i}`] = { id: `t${i}`, n: i }; });
  await env.until(() => Object.keys(b.state.tasks).length === 5 && online(a, b)(), 'b has them');
  // Three more answers for a, each followed by its whole outbox again
  for (let i = 0; i < 3; i++) {
    a.link.goOffline();
    await env.until(() => a.status === 'offline', 'a away');
    a.link.goOnline();
    a.socket.connection.connect();
    await env.until(() => a.status === 'online', 'a back');
  }
  assert.equal(env.relay.copy('main').local, 5, 'five ops, five applied');
  assert.equal(a.pending, 5);
  env.central.goUp();
  await env.until(() => a.pending === 0 && online(a, b)() && !a.relayed, 'through');
  assert.equal(store.export().replicas['r-a'].seq, 5, 'the server holds the display\'s own seqs');
  assert.deepEqual(plain(b.state), store.snapshot());
});

test('epochs: an outage raises no reset, and a restore at the server reaches the displays as one while the copy takes the new epoch', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  a.state.tasks.x = { id: 'x', title: 'kept' };
  await env.until(() => a.pending === 0, 'synced');
  const doc = env.central.store().export();
  const resets = [];
  a.on('reset', r => resets.push(r));
  env.central.goDown();
  await env.until(() => a.relayed, 'local');
  a.state.tasks.y = { id: 'y', title: 'offline' };
  await env.until(() => env.relay.copy('main').local === 1 && a.status === 'online', 'applied');
  env.central.goUp();
  await env.until(() => !a.relayed && a.pending === 0 && a.status === 'online', 'through');
  assert.equal(resets.length, 0);

  const before = env.central.store().epoch;
  a.state.tasks.z = { id: 'z', title: 'lost to the restore' };
  await env.until(() => a.pending === 0, 'synced');
  env.central.stores.restore('main', doc);
  await env.until(() => resets.length === 1 && a.status === 'online' && env.relay.copy('main').epoch === env.central.store().epoch, 'restored');
  assert.notEqual(env.central.store().epoch, before);
  assert.equal(env.relay.copy('main').live, true);
  assert.deepEqual(env.relay.copy('main').state, env.central.store().snapshot());
  assert.deepEqual(plain(a.state), env.central.store().snapshot());
});

test('local presence: the displays on the relay, as the server showed their users, and what they share', async t => {
  const env = lan();
  t.after(() => env.close());
  const byId = list => plain(list).sort((x, y) => x.id.localeCompare(y.id));
  const a = env.display('a');
  const b = env.display('b');
  const quiet = env.display('quiet', { presence: false });   // hears no presence, but is in everyone else's
  await env.until(() => online(a, b, quiet)() && a.peers.length === 3, 'online');
  const users = byId(a.presence);
  assert.equal(users.length, 3);
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed && quiet.relayed && a.peers.length === 3 && b.peers.length === 3, 'local, all there');
  assert.deepEqual(byId(a.presence), users, 'the users as the server showed them');
  assert.deepEqual(quiet.peers, [], 'and none for the one that asked for none');
  b.share({ cursor: 3 });
  await env.until(() => a.peers.find(p => p.replicaId === 'r-b')?.data?.cursor === 3, 'b\'s share reached a');
  assert.equal(b.peers.find(p => p.replicaId === 'r-b').data.cursor, 3, 'and b itself');
  b.disconnect();
  await env.until(() => a.peers.length === 2, 'b left');
  assert.deepEqual(byId(a.presence).map(u => u.id), ['a', 'quiet']);
});

test('local: a credential the server never let in, a store it was refused, and another\'s replica id get no answer', async t => {
  const env = lan({ hub: { authorizeId: (user, id) => id !== 'private' || user.id === 'a' } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  const aPrivate = env.display('a', { store: 'private', replicaId: 'r-a-private', socket: a.socket });
  const bPrivate = env.display('b', { store: 'private', replicaId: 'r-b-private', socket: b.socket });
  await env.until(() => online(a, b, aPrivate)() && bPrivate.closed?.code === 'forbidden', 'b is refused the private store');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed && aPrivate.relayed, 'the known ones are answered');
  bPrivate.connect();
  const stranger = env.display('stranger');
  const forger = env.display('forger', { key: 'key-b', replicaId: 'r-a' });   // b's credential, a's replica
  await sleep(30);
  await env.until(() => true, 'settled');
  assert.equal(bPrivate.status, 'connecting', 'a store refused before is not answered');
  assert.equal(stranger.status, 'connecting', 'an unknown credential is not answered');
  assert.equal(forger.status, 'connecting', 'another credential\'s replica is not answered');
  assert.equal(env.relay.copy('private').v >= 0, true);
});

test('local: a store the relay never saw waits for the server', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  env.central.goDown();
  await env.until(() => a.relayed, 'local');
  const other = env.display('a', { store: 'other', replicaId: 'r-a-other', socket: a.socket });
  other.state.tasks.x = { id: 'x', title: 'waits' };
  await sleep(20);
  await env.until(() => true, 'settled');
  assert.equal(other.status, 'connecting');
  assert.equal(env.relay.copy('other'), null);
  env.central.goUp();
  await env.until(() => other.status === 'online' && other.pending === 0, 'through, and answered by the server');
  assert.equal(env.central.store('other').snapshot().tasks.x.title, 'waits');
});

test('a display with more than a hello\'s worth of pending ops is answered once and sends the rest live, whether the relay applies them or not', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  a.link.goOffline();
  await env.until(() => a.status === 'offline', 'a away');
  ops(a, 1500, i => { a.state.tasks[`t${i}`] = { id: `t${i}`, n: i }; });
  const hellos = a.heard.length;
  a.link.goOnline();
  a.socket.connection.connect();
  await env.until(() => Object.keys(b.state.tasks).length === 1500 && a.status === 'online', 'b has every op');
  const answers = env.answers(a, hellos);
  assert.equal(answers.length, 1, 'answered once, not over and over');
  assert.equal(a.pending, 1500);
  assert.equal(env.relay.copy('main').local, 1500);

  // A clock far ahead: nothing is applied, and a hello carrying a full load is still answered once, as a relay's
  const env2 = lan();
  t.after(() => env2.close());
  const c = env2.display('c');
  await env2.until(online(c), 'online');
  env2.central.goDown();
  await env2.until(() => c.relayed, 'local');
  c.dispose();
  // c's replica and credential again, offline, with a clock an hour ahead
  const socket = env2.socket('c');
  socket.link.goOffline();
  const d = env2.display('c', { socket, now: fakeTime(1_000_000 + 60 * 60_000) });
  ops(d, 1100, i => { d.state.tasks[`t${i}`] = { id: `t${i}` }; });
  await env2.until(() => d.pending === 1100, 'd\'s outbox');
  const from = d.heard.length;
  socket.link.goOnline();
  await env2.until(() => d.status === 'online', 'd answered');
  await sleep(20);
  await env2.until(() => true, 'settled');
  const answered = env2.answers(d, from);
  assert.equal(answered.length, 1);
  assert.equal(answered[0].epoch, null, 'a full hello is answered as a relay\'s, though the copy took none of it');
  assert.equal(env2.relay.copy('main').local, 0);
  assert.equal(env2.relay.copy('main').diverged, false);
});

test('a display ahead of the copy is not rolled back: it keeps its state, and gets the offline edits alone', async t => {
  const env = lan({ relay: { saveDelay: 60_000 } });
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  a.state.tasks.x = { id: 'x', title: 'old' };
  await env.until(() => b.state.tasks.x && a.pending === 0, 'synced');
  env.relay.flush();
  const stale = env.storage.load();
  store.patch({ tasks: { x: { title: 'newer than the copy on disk' } } });
  await env.until(() => a.state.tasks.x.title === 'newer than the copy on disk' && b.state.tasks.x.title === 'newer than the copy on disk', 'displays have it');

  // The relay restarts with what it had written, and the server is away
  env.central.goDown();
  env.relay.close();
  env.relay = env.makeRelay({ storage: { load: () => stale, write() {}, writeKnown() {}, remove() {} } });
  env.relay.upstreamDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  assert.equal(a.state.tasks.x.title, 'newer than the copy on disk', 'not rolled back');
  assert.equal(env.relay.copy('main').state.tasks.x.title, 'old');
  const from = a.heard.length;
  b.state.tasks.y = { id: 'y', title: 'offline' };
  await env.until(() => a.state.tasks.y && a.status === 'online', 'a has b\'s edit');
  assert.equal(a.state.tasks.x.title, 'newer than the copy on disk', 'and still its newer state');
  const [answer] = env.answers(a, from);
  assert.equal(answer.t, 'delta');
  assert.equal(answer.epoch, null);
  assert.deepEqual(answer.patches, [{ tasks: { y: { id: 'y', title: 'offline' } } }]);
});

test('close codes: 4401 and a closed without a store end the display\'s socket and forget the credential; 4001 and 1013 reconnect, and are not an outage', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  const evict = (id, code) => { for (const s of [...env.central.sessions]) if (s.user.id === id) s.onEvict(code, 'test'); };

  for (const code of [4001, 1013]) {
    const from = a.heard.length;
    evict('a', code);
    await env.until(() => a.status === 'online' && env.answers(a, from).length === 1, `reconnected after ${code}`);
  }
  assert.equal(env.relay.stats().downSince, null);
  assert.deepEqual(modes, []);

  for (const s of [...env.central.sessions]) {
    if (s.user.id !== 'b') continue;
    s.send({ t: 'closed', code: 'unauthorized', message: 'Unauthorized' });
    s.onEvict(4401, 'Unauthorized');
  }
  await env.until(() => b.closed?.code === 'unauthorized', 'b turned away');
  assert.equal(b.socket.connection.closed.code, 'unauthorized', 'the socket stays down');
  assert.equal(env.relay.stats().credentials, 1, 'b\'s credential is forgotten');
  env.central.goDown();
  b.socket.connection.connect();
  await env.until(() => a.relayed, 'local');
  await sleep(20);
  await env.until(() => true, 'settled');
  assert.equal(b.status, 'connecting', 'and not answered offline');
});

test('an outage shorter than grace is waited out: the displays come back through, with deltas', async t => {
  const env = lan({ relay: { grace: 2000 } });
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  await env.until(online(a), 'online');
  a.state.tasks.x = { id: 'x', title: 'before' };
  await env.until(() => a.pending === 0, 'synced');
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  const from = a.heard.length;
  const snapshots = store.stats().sent.snapshot.messages;
  env.central.goDown();
  await env.until(() => a.status !== 'online' && env.relay.stats().downSince !== null, 'the server is away');
  store.patch({ tasks: { y: { id: 'y', title: 'meanwhile' } } });
  await sleep(50);
  env.central.goUp();
  await env.until(() => a.status === 'online' && a.state.tasks.y, 'back through');
  assert.deepEqual(modes, [], 'never local');
  assert.equal(env.answers(a, from).at(-1).t, 'delta');
  assert.equal(store.stats().sent.snapshot.messages, snapshots, 'no snapshot');
  assert.equal(env.relay.stats().downSince, null);
});

test('a store nobody edited offline comes back with deltas, not snapshots', async t => {
  const env = lan();
  t.after(() => env.close());
  const store = env.central.store();
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  a.state.tasks.x = { id: 'x', title: 'before' };
  await env.until(() => b.state.tasks.x && a.pending === 0, 'synced');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  const snapshots = store.stats().sent.snapshot.messages;
  const from = { a: a.heard.length, b: b.heard.length };
  env.central.goUp();
  await env.until(() => env.relay.mode === 'through' && online(a, b)() && !a.relayed && !b.relayed, 'through');
  assert.equal(store.stats().sent.snapshot.messages, snapshots, 'no snapshot');
  assert.equal(env.answers(a, from.a).at(-1).t, 'delta');
  assert.equal(env.answers(b, from.b).at(-1).t, 'delta');
  assert.equal(env.relay.copy('main').live, true);
});

test('a relay restarted in an outage answers from what it wrote, and applies no op twice', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  ops(a, 2, i => { a.state.tasks[i ? 'y' : 'x'] = { id: i ? 'y' : 'x', title: i ? 'offline too' : 'offline' }; });
  await env.until(() => b.state.tasks.y && online(a, b)(), 'b has them');
  env.relay.close();
  env.relay = env.makeRelay();
  env.relay.upstreamDown();
  assert.equal(env.relay.copy('main').local, 2);
  await env.until(() => a.relayed && b.relayed && online(a, b)(), 'answered by the new relay');
  assert.equal(env.relay.copy('main').local, 2, 'a\'s ops, resent in its hello, were not applied again');
  assert.equal(b.state.tasks.x.title, 'offline');
  assert.equal(a.pending, 2);
  env.central.goUp();
  await env.until(() => a.pending === 0 && online(a, b)() && !a.relayed, 'through');
  assert.deepEqual(plain(b.state), env.central.store().snapshot());
});

test('a hostile display cannot change the copy offline, forge a replica, or touch Object.prototype', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const m = env.display('mallory');
  await env.until(online(a, m), 'online');
  a.state.tasks.x = { id: 'x', title: 'kept' };
  await env.until(() => a.pending === 0, 'synced');
  env.central.goDown();
  await env.until(() => a.relayed && m.relayed, 'local');
  const before = env.relay.copy('main');
  const heard = [];
  const own = (key, value, base = {}) => Object.defineProperty(base, key, { value, enumerable: true, writable: true, configurable: true });
  const s = env.relay.accept({ send: msg => heard.push(msg), close() {}, key: 'key-mallory', upstream: () => { throw new Error('never dialled'); } });
  const ts = [1_000_100, 0, 'r-mallory'];
  s.receive({ t: 'hello', store: 'main', replicaId: 'r-mallory', ops: [], since: 0, epoch: null });
  const garbage = [
    null, 'text', 42, [], { t: 'hello' }, { t: 'op', store: 'main' }, { t: 'op', store: 'main', op: 'x' },
    { t: 'hello', store: '../etc', replicaId: 'r-mallory', ops: [] },
    { t: 'hello', store: 'main', replicaId: 'r-a', ops: [] },
    { t: 'op', store: 'main', op: { replicaId: 'r-a', seq: 99, ts: [1_000_100, 0, 'r-a'], diff: { tasks: { x: { title: 'forged' } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 1, ts: [1_000_100, 0, 'r-a'], diff: { tasks: { x: { title: 'stolen stamp' } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 2, ts, diff: own('__proto__', { polluted: 'yes' }) } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 3, ts, diff: { tasks: own('__proto__', { polluted: 'yes' }) } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 4, ts, diff: { tasks: { x: { constructor: { prototype: { polluted: 'yes' } } } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 5, ts, diff: { tasks: { x: { $splice: [[0, 0, ['y']]] } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 6, ts, diff: { tasks: null } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 7, ts, diff: { tasks: { x: [{ nested: 1 }] } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 8, ts: [9e15, 0, 'r-mallory'], diff: { tasks: { x: { title: 'from the future' } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 9, ts: 'yesterday', diff: { tasks: { x: { title: 'no stamp' } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 1.5, ts, diff: { tasks: { x: { title: 'half' } } } } },
    { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 10, ts, diff: { tasks: Object.fromEntries(Array.from({ length: 10_001 }, (_, i) => [`k${i}`, i])) } } },
    { t: 'share', store: 'main', data: 'x'.repeat(10_000) },
    own('__proto__', { t: 'hello' }, { store: 'main' })
  ];
  for (const message of garbage) s.receive(message);
  s.receive(own('__proto__', { polluted: 'yes' }, { t: 'op', store: 'main', op: { replicaId: 'r-mallory', seq: 11, ts, diff: { tasks: { z: 1 } } } }));
  assert.equal({}.polluted, undefined);
  assert.equal(Object.prototype.polluted, undefined);
  const after = env.relay.copy('main');
  assert.deepEqual(after.state.tasks.x, before.state.tasks.x, 'nothing forged, stolen, or from the future landed');
  assert.equal(after.state.tasks.k0, undefined);
  assert.ok(after.state.tasks, 'the skeleton stands');
  assert.ok(!heard.some(msg => msg.t === 'ack' || msg.t === 'error'));
  s.close();
});

test('a client whose hello a relay answers with epoch null sends the rest of a long outbox live, rather than saying hello for ever', async () => {
  // A stand-in relay: every hello answered with a snapshot of nothing, epoch
  // null and seq 0, and no op acknowledged, as lazy-storage/relay answers
  const hellos = [];
  const sent = [];
  const net = createNetwork({
    session: ({ send }) => ({
      receive(msg) {
        if (msg.t === 'hello') {
          hellos.push(msg.ops.length);
          if (hellos.length > 3) return;   // a client in a loop would say hello for ever
          send({ t: 'snapshot', store: msg.store, state: { tasks: {} }, ts: null, seq: 0, registers: [], v: 0, epoch: null });
        } else if (msg.t === 'op') {
          sent.push(msg.op.seq);
        }
      },
      close() {}
    })
  });
  const link = net.link();
  link.goOffline();
  const db = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL });
  ops(db, 1500, i => { db.state.tasks[`t${i}`] = { id: `t${i}` }; });
  await net.settle();
  link.goOnline();
  db.connect();
  await net.settle();
  assert.deepEqual(hellos, [1000], 'one hello');
  assert.equal(sent.length, 1500, 'every op then went live');
  assert.equal(db.pending, 1500);
  assert.equal(db.status, 'online');
  db.dispose();
});

test('db.relayed: true while a relay answers on its own, false once the server does; a browser\'s tabs hear it through the shared connection', async t => {
  const env = lan({ realTime: true });
  t.after(() => env.close());
  const a = env.display('a');
  const link = env.net.link({ user: { id: 'b', key: 'key-b' } });
  const shared = sharedConnection({ name: 'relay-test', transport: link.factory, locks: null, channel: null, reconnect: { min: 2, max: 10 }, keepalive: false, wake: false });
  const b = createClient({ connection: shared, store: 'main', initial: INITIAL, replicaId: 'tab-b', now: env.time });
  b.connect();
  t.after(() => { b.dispose(); shared.dispose(); });
  await env.until(() => online(a, b)(), 'online');
  const events = [];
  a.on('relay', relayed => events.push(relayed));
  assert.equal(a.relayed, false);
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'relayed, the tab too');
  b.state.tasks.x = { id: 'x', title: 'from the tab' };
  await env.until(() => a.state.tasks.x && b.relayed && a.relayed && a.status === 'online', 'crossed, still relayed');
  env.central.goUp();
  await env.until(() => !a.relayed && !b.relayed && online(a, b)() && b.pending === 0, 'the server answers again');
  assert.equal(events[0], true);
  assert.equal(events.at(-1), false);
  assert.equal(env.central.store().snapshot().tasks.x.title, 'from the tab');
});

test('the default probe finds the server back, and upstreamUp and upstreamDown are the host\'s word', async t => {
  const env = lan({ relay: { grace: 60_000, probeEvery: 150 } });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  env.relay.upstreamDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed, 'local at the host\'s word, with the server there');
  await env.until(() => env.relay.mode === 'through' && online(a)() && !a.relayed, 'and the probe found the server');

  const probed = createRelay({ probe: () => false, probeEvery: 5, grace: 0, keepalive: false, jitter: 0 });
  t.after(() => probed.close());
  probed.upstreamDown();
  assert.equal(probed.mode, 'local');
  await sleep(30);
  assert.equal(probed.mode, 'local', 'a probe that says no keeps it local');
  probed.upstreamUp();
  assert.equal(probed.mode, 'through');
});

test('a dial that never opens times out and counts as the server being away', async t => {
  const env = lan({ relay: { dialTimeout: 20 } });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  // Every dial from now on hangs
  const factory = env.central.net.link;
  env.central.net.link = options => { const link = factory(options); link.stall(); return link; };
  a.link.goOffline();
  await env.until(() => a.status === 'offline', 'a away');
  a.link.goOnline();
  a.socket.connection.connect();
  await env.until(() => env.relay.mode === 'local' && a.relayed, 'the relay answers');
  env.central.net.link = factory;
});

test('copies in files: a copy per store and the credentials, written through a rename, read by the next relay', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-relay-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const env = lan({ storage: fileCopies(dir) });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  a.state.tasks.x = { id: 'x', title: 'on disk' };
  await env.until(() => a.pending === 0 && env.relay.copy('main').state.tasks.x, 'the copy has it');
  env.relay.flush();
  assert.deepEqual(readdirSync(dir).sort(), ['_known.json', 'main.json']);
  const next = createRelay({ storage: fileCopies(dir), keepalive: false });
  t.after(() => next.close());
  assert.equal(next.copy('main').state.tasks.x.title, 'on disk');
  assert.equal(next.copy('main').live, false);
  assert.equal(next.stats().credentials, 1);
});

test('a copy and a credential nobody used for forgetAfter are let go; one in use is kept however long', async t => {
  const env = lan({ relay: { forgetAfter: 1000 } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b', { store: 'other', replicaId: 'r-b-other' });
  await env.until(online(a, b), 'online');
  a.socket.connection.close();
  await env.until(() => env.relay.copy('main').live === false, 'a gone');
  env.relay.sweep();
  assert.ok(env.relay.copy('main'), 'not yet');
  env.time.advance(2000);
  env.relay.sweep();
  assert.equal(env.relay.copy('main'), null);
  assert.ok(env.relay.copy('other'), 'b has its store open');
  assert.equal(env.relay.stats().credentials, 1, 'and its credential is kept while its socket is open');
  env.time.advance(5000);
  env.relay.sweep();
  assert.equal(env.relay.stats().credentials, 1, 'however long the server has not answered a hello of it');
});

/** Transports the test drives by hand: every one the relay opens, with what was sent on it */
function manual() {
  const opened = [];
  const factory = () => {
    const t = { onopen: null, onmessage: null, onclose: null, sent: [], closed: false, send(m) { t.sent.push(m); }, close() { t.closed = true; } };
    opened.push(t);
    return t;
  };
  factory.opened = opened;
  return factory;
}

test('a patch counts only on a socket whose session is under the copy\'s epoch, however the sockets\' messages interleave', () => {
  const relay = createRelay({ keepalive: false, grace: 0, jitter: 0, onError: err => { throw err; } });
  const x = manual();
  const y = manual();
  const sx = relay.accept({ send() {}, close() {}, key: 'kx', upstream: x });
  const sy = relay.accept({ send() {}, close() {}, key: 'ky', upstream: y });
  x.opened[0].onopen();
  y.opened[0].onopen();
  sx.receive({ t: 'hello', store: 'main', replicaId: 'rx', ops: [] });
  sy.receive({ t: 'hello', store: 'main', replicaId: 'ry', ops: [] });
  const answer = (t, epoch, v, state) => t.onmessage({ t: 'snapshot', store: 'main', state, ts: [1, 0, 'server'], seq: 0, registers: [], v, epoch });
  answer(x.opened[0], 'E1', 2, { tasks: {} });
  answer(y.opened[0], 'E1', 2, { tasks: {} });
  assert.equal(relay.copy('main').live, true);
  // The store is restored at the server: x's session is answered anew under
  // E2, while y's, still under E1, delivers a patch the old store sent
  answer(x.opened[0], 'E2', 2, { tasks: { restored: true } });
  y.opened[0].onmessage({ t: 'patch', store: 'main', diff: { tasks: { stale: true } }, ts: [2, 0, 'ry'], v: 3 });
  assert.equal(relay.copy('main').v, 2, 'the old epoch\'s patch is not the new one\'s third version');
  assert.deepEqual(relay.copy('main').state, { tasks: { restored: true } });
  x.opened[0].onmessage({ t: 'patch', store: 'main', diff: { tasks: { fresh: true } }, ts: [3, 0, 'rx'], v: 3 });
  assert.deepEqual(relay.copy('main').state, { tasks: { restored: true, fresh: true } });
  assert.equal(relay.copy('main').epoch, 'E2');
  relay.close();
});

test('the relay\'s pings share the way up with the client\'s: its own pongs stay with it, and a server silent for two of them is taken for gone', async () => {
  const relay = createRelay({ storage: knownCopies({ k: { main: ['r'] } }), keepalive: 15, grace: 60_000, jitter: 0, onError: err => { throw err; } });
  const up = manual();
  const heard = [];
  const closes = [];
  const s = relay.accept({ send: m => heard.push(m), close: code => closes.push(code), key: 'k', upstream: up });
  const t = up.opened[0];
  t.onopen();
  s.receive({ t: 'ping' });
  await sleep(20);
  const pings = t.sent.filter(m => m.t === 'ping').length;
  assert.ok(pings >= 2, 'the client\'s ping and the relay\'s went up');
  for (let i = 0; i < pings; i++) t.onmessage({ t: 'pong' });
  assert.equal(heard.filter(m => m.t === 'pong').length, 1, 'only the client\'s pong came down');
  await sleep(80);
  assert.deepEqual(closes, [1012], 'unanswered, the socket was closed for the client to reconnect');
  assert.equal(t.closed, true);
  assert.notEqual(relay.stats().downSince, null, 'and the server counts as failing');
  relay.close();
});

test('grace: a server that fails for longer is taken for away, and a hello that waited meanwhile is answered from the copy', async t => {
  const env = lan({ relay: { grace: 60 } });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  env.central.goDown();
  await env.until(() => env.relay.stats().downSince !== null && env.relay.stats().sockets.dialing === 1, 'a dials, and fails');
  assert.equal(env.relay.mode, 'through', 'not yet');
  assert.equal(a.status, 'connecting', 'its hello waits');
  await env.until(() => a.relayed && a.status === 'online', 'answered from the copy once grace is out');
  assert.deepEqual(modes, ['local']);
});

test('options and arguments are checked, and faults go to onError and the error event without stopping anything', async t => {
  assert.throws(() => createRelay({ storage: {} }), /storage needs load, write and writeKnown/);
  assert.throws(() => createRelay({ authorizeOffline: 'yes' }), /authorizeOffline must be a function/);
  assert.throws(() => createRelay({ validate: 1 }), /validate must be a function/);
  assert.throws(() => createRelay({ probe: 'sometimes' }), /probe must be a function, or false/);
  const quiet = createRelay({ keepalive: false });
  t.after(() => quiet.close());
  assert.throws(() => quiet.accept({ send() {} }), /send and close/);
  assert.throws(() => quiet.accept({ send() {}, close() {} }), /upstream transport factory/);
  assert.throws(() => quiet.on('nothing', () => {}), /Unknown relay event/);

  // Storage that fails: the load is reported and the relay starts empty; a write is reported and tried again
  const errors = [];
  let failing = true;
  const writes = [];
  const flaky = {
    load() { throw new Error('unreadable'); },
    write(id) { if (failing) throw new Error('disk full'); writes.push(id); },
    writeKnown() { if (failing) throw new Error('disk full'); }
  };
  const env = lan({ storage: flaky, relay: { onError: err => errors.push(err.message) } });
  t.after(() => env.close());
  const heard = [];
  env.relay.on('error', err => heard.push(err.message));
  assert.deepEqual(errors, ['unreadable']);
  const a = env.display('a');
  await env.until(() => a.status === 'online' && errors.includes('disk full'), 'a write failed');
  assert.ok(heard.includes('disk full'), 'the error event heard it too');
  failing = false;
  await env.until(() => writes.includes('main'), 'written once the disk had room');

  // A throwing listener is reported, and the rest still hear
  let after = 0;
  env.relay.on('copy', () => { throw new Error('listener'); });
  env.relay.on('copy', () => after++);
  a.state.tasks.x = { id: 'x' };
  await env.until(() => after > 0 && errors.includes('listener'), 'reported, and the next listener heard');
});

test('authorizeOffline and validate: the host decides who is answered from which copy, and which offline edits it takes', async t => {
  const decided = [];
  const env = lan({
    relay: {
      authorizeOffline: (key, storeId) => {
        decided.push([key, storeId]);
        if (key === 'key-thrower') throw new Error('no idea');
        return key !== 'key-b';
      },
      validate: (diff, { key, replicaId, storeId, state }) => {
        assert.equal(storeId, 'main');
        assert.ok(state.tasks);
        if (diff.tasks?.bad) return false;
        if (diff.tasks?.worse) throw new Error('refused');
        return key === `key-${replicaId.slice(2)}`;
      },
      onError: () => {}
    }
  });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  const thrower = env.display('thrower');
  await env.until(online(a, b, thrower), 'online');
  env.central.goDown();
  await env.until(() => a.relayed, 'a is answered');
  await sleep(20);
  await env.until(() => true, 'settled');
  assert.equal(b.status, 'connecting', 'the host said no to b');
  assert.equal(thrower.status, 'connecting', 'and a check that throws is a no');
  assert.ok(decided.some(([key]) => key === 'key-b'));
  ops(a, 3, i => { a.state.tasks[['bad', 'worse', 'good'][i]] = { id: String(i) }; });
  await env.until(() => env.relay.copy('main').local === 1 && a.status === 'online', 'one of three taken');
  assert.deepEqual(Object.keys(env.relay.copy('main').state.tasks), ['good']);
  assert.equal(a.pending, 3, 'the other two stay pending, for the server to judge');
});

/** memoryCopies holding credentials the server answered: `{ key: { store: [replica ids] } }` */
function knownCopies(keys, copies = []) {
  const storage = memoryCopies();
  for (const doc of copies) storage.write(doc.store, doc);
  storage.writeKnown({
    format: 'lazy-storage/relay-known',
    centralTs: null,
    keys: Object.entries(keys).map(([key, stores]) => [key, { at: Date.now(), stores: Object.fromEntries(Object.entries(stores).map(([id, replicaIds]) => [id, { replicaIds }])) }])
  });
  return storage;
}

test('an upstream that throws or gives no transport, and a door closed with 4401 while dialling, are handled', async t => {
  const relay = createRelay({ storage: knownCopies({ k: { main: ['r'] } }), keepalive: false, grace: 0, jitter: 0, probe: false, onError: () => {} });
  t.after(() => relay.close());
  const closes = [];
  relay.accept({ send() {}, close: code => closes.push(code), key: 'stranger', upstream: () => { throw new Error('no route'); } });
  assert.equal(relay.mode, 'through', 'a credential the server never answered says nothing of the server');
  relay.accept({ send() {}, close: code => closes.push(code), key: 'k', upstream: () => { throw new Error('no route'); } });
  assert.equal(relay.mode, 'local', 'a dial that cannot even start is a failure');
  relay.upstreamUp();
  relay.accept({ send() {}, close: code => closes.push(code), key: 'k', upstream: () => null });
  assert.equal(relay.mode, 'local');
  relay.upstreamUp();
  const door = manual();
  const s = relay.accept({ send() {}, close: code => closes.push(code), key: 'k', upstream: door });
  door.opened[0].onclose({ code: 4401, reason: 'Unauthorized' });
  assert.equal(s.state, 'closed');
  assert.equal(closes.at(-1), 4401, 'turned away at the door, as the server would');
  assert.equal(relay.mode, 'through', 'which is no outage');
  const odd = manual();
  const stranger = relay.accept({ send() {}, close: code => closes.push(code), key: 'stranger', upstream: odd });
  odd.opened[0].onopen();
  odd.opened[0].onclose({ code: 1009, reason: 'Message too big' });
  assert.equal(stranger.state, 'closed');
  assert.equal(closes.at(-1), 1009);
  assert.equal(relay.mode, 'through', 'nor is a socket the server closed on a credential it never answered');
});

test('a probe that throws is reported and asked again; upstreamDown during the jitter back keeps the relay local', async t => {
  let calls = 0;
  const errors = [];
  const relay = createRelay({ keepalive: false, probeEvery: 5, jitter: 60_000, probe: () => { calls++; throw new Error('probe broke'); }, onError: err => errors.push(err.message) });
  t.after(() => relay.close());
  relay.upstreamDown();
  await sleep(40);
  assert.ok(calls >= 2, 'asked again');
  assert.ok(errors.includes('probe broke'));
  relay.upstreamUp();
  assert.equal(relay.mode, 'local', 'waiting out its jitter');
  const before = calls;
  relay.upstreamDown();
  assert.equal(relay.mode, 'local');
  await sleep(40);
  assert.ok(calls > before, 'probing again');
});

test('fileCopies: a file that cannot be read is reported and skipped; a store let go is removed from the disk', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'lazy-relay-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const errors = [];
  const storage = fileCopies(join(dir, 'copies'), { onError: err => errors.push(err.message) });
  assert.equal(storage.load(), null, 'nothing yet');
  storage.write('main', { format: 'lazy-storage/relay-copy', store: 'main', state: { tasks: {} }, v: 1, epoch: 'e' });
  storage.writeKnown({ format: 'lazy-storage/relay-known', centralTs: 5, keys: [] });
  writeFileSync(join(dir, 'copies', 'broken.json'), '{ not json');
  writeFileSync(join(dir, 'copies', 'main.json.123.tmp'), 'left by a crash');
  const loaded = storage.load();
  assert.deepEqual(loaded.copies.map(c => c.store), ['main']);
  assert.equal(loaded.known.centralTs, 5);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /broken\.json/);
  storage.remove('main');
  assert.deepEqual(storage.load().copies, []);
});

/** Let the network deliver for `ms` of real time */
async function run(env, ms) {
  for (const start = Date.now(); Date.now() - start < ms;) {
    await env.until(() => true, 'settled');
    await sleep(3);
  }
}

test('one client\'s dials failing is no outage while the server answers another\'s socket, at the defaults\' proportions: the relay asks the sockets it passes through', async t => {
  // grace 10 s and keepalive 15 s, scaled: a's socket is quiet for longer than grace
  const env = lan({ relay: { grace: 100, keepalive: 150 } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  // b, which the server answered, can no longer get through (its requests turned away on the way), while a's socket is fine
  let refused = 0;
  env.central.refuse = user => user.id === 'b' && ++refused > 0;
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b\'s socket dropped');
  b.link.goOnline();
  // Three dials, each failing, over seven graces
  await env.until(() => refused >= 3, 'b dials, and fails, again and again');
  assert.deepEqual(modes, [], 'the server answered a\'s socket each time');
  assert.equal(a.status, 'online');
  assert.equal(a.relayed, false);
  assert.equal(b.status, 'connecting');
  env.central.refuse = null;
  env.relay.upstreamUp();   // and the host may have the waiting sockets try again at once
  await env.until(() => b.status === 'online' && !b.relayed, 'b through');
});

test('a client the server never answered is no outage however its dials fail, even with no grace at all', async t => {
  const env = lan();   // grace 0
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  const modes = [];
  env.relay.on('mode', mode => modes.push(mode));
  env.central.refuse = user => user.id === 'mallory';
  const m = env.display('mallory');
  await env.until(() => env.relay.stats().sockets.dialing === 1, 'mallory dials');
  await run(env, 100);
  assert.deepEqual(modes, []);
  assert.equal(env.relay.stats().downSince, null);
  assert.equal(m.status, 'connecting');
  assert.equal(a.relayed, false);
});

test('the default probe dials with credentials the server answered, taking turns: a client whose dials fail does not keep the relay local', async t => {
  const env = lan({ relay: { grace: 60_000, probeEvery: 10 } });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  env.relay.upstreamDown();
  await env.until(() => a.relayed, 'local');
  // The newest socket, of a client whose every dial fails
  let dials = 0;
  const s = env.relay.accept({
    send() {}, close() {}, key: 'key-mallory',
    upstream: () => { dials++; const link = env.central.net.link({ user: { id: 'mallory' } }); link.goOffline(); return link.factory(); }
  });
  t.after(() => s.close());
  await env.until(() => env.relay.mode === 'through' && online(a)() && !a.relayed, 'the probe found the server with a\'s');
  assert.equal(dials, 0);
});

test('a socket whose hellos on a store named two replicas has neither recorded: a replica the server refused it never becomes its credential\'s', async t => {
  const env = lan();
  t.after(() => env.close());
  const b = env.display('b');
  await env.until(online(b), 'online');
  // Mallory names its own replica and, at once, b's: the server answers the first and refuses the second
  const heard = [];
  const first = env.net.link({ user: { id: 'mallory', key: 'key-mallory' } }).factory();
  first.onmessage = m => heard.push(m);
  first.onopen = () => {
    first.send({ t: 'hello', store: 'main', replicaId: 'r-mallory', ops: [] });
    first.send({ t: 'hello', store: 'main', replicaId: 'r-b', ops: [] });
  };
  await env.until(() => heard.some(m => m.t === 'snapshot') && heard.some(m => m.t === 'error' && m.code === 'forbidden'), 'answered, and refused');
  first.close();
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && b.relayed, 'local');
  // Offline, Mallory speaks as b, with a seq far ahead of b's
  const later = [];
  const second = env.net.link({ user: { id: 'mallory', key: 'key-mallory' } }).factory();
  second.onmessage = m => later.push(m);
  second.onopen = () => second.send({ t: 'hello', store: 'main', replicaId: 'r-b', ops: [
    { replicaId: 'r-b', seq: 1_000_000, ts: [env.time(), 0, 'r-b'], diff: { tasks: { forged: { id: 'forged', title: 'as b' } } } }
  ] });
  await run(env, 30);
  assert.ok(!later.some(m => m.t === 'snapshot' || m.t === 'delta'), 'not answered as b');
  assert.equal(env.relay.copy('main').state.tasks.forged, undefined);
  b.state.tasks.real = { id: 'real', title: 'b\'s own' };
  await env.until(() => env.relay.copy('main').state.tasks.real, 'b\'s own offline edit is taken');
  second.close();
});

test('one credential\'s replicas on a store are each answered offline, after a restart too: a device\'s windows, each a client of its own', async t => {
  const env = lan();
  t.after(() => env.close());
  const w1 = env.display('w1', { key: 'key-device' });
  const w2 = env.display('w2', { key: 'key-device' });
  await env.until(online(w1, w2), 'online');
  env.central.goDown();
  await env.until(() => w1.relayed && w2.relayed && online(w1, w2)(), 'both answered by the relay');
  w1.state.tasks.x = { id: 'x', title: 'from one window' };
  await env.until(() => w2.state.tasks.x, 'the other has it');
  env.relay.close();
  env.relay = env.makeRelay();
  env.relay.upstreamDown();
  await env.until(() => w1.relayed && w2.relayed && online(w1, w2)(), 'both answered by the relay restarted');
});

test('a credential keeps the 32 replicas on a store answered last', () => {
  const relay = createRelay({ keepalive: false, grace: 0, jitter: 0, probe: false, onError: err => { throw err; } });
  const snapshot = { t: 'snapshot', store: 'main', state: { tasks: {} }, ts: [1, 0, 'server'], seq: 0, registers: [], v: 1, epoch: 'E' };
  const answer = replicaId => {
    const up = manual();
    const s = relay.accept({ send() {}, close() {}, key: 'k', upstream: up });
    up.opened[0].onopen();
    s.receive({ t: 'hello', store: 'main', replicaId, ops: [] });
    up.opened[0].onmessage({ ...snapshot });
  };
  for (let i = 0; i < 33; i++) answer(`r${i}`);
  answer('r1');   // answered again: the most recent now
  answer('r33');
  relay.upstreamDown();
  const served = replicaId => {
    const heard = [];
    relay.accept({ send: m => heard.push(m), close() {}, key: 'k', upstream: manual() }).receive({ t: 'hello', store: 'main', replicaId, ops: [] });
    return heard.some(m => m.t === 'snapshot');
  };
  assert.equal(served('r0'), false, 'the first was let go for the 33rd');
  assert.equal(served('r1'), true, 'the second, answered again since, was kept');
  assert.equal(served('r2'), false, 'and the third let go in its place');
  assert.equal(served('r33'), true);
  relay.close();
});

test('an offline edit the server\'s snapshot does not hold, its author not yet back, is taken again in the next outage', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  env.central.goDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  b.state.tasks.x = { id: 'x', title: 'offline' };
  await env.until(() => a.state.tasks.x, 'a has b\'s edit');
  // b is switched off, its edit pending; the server comes back, and a with it
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b off');
  env.central.goUp();
  await env.until(() => env.relay.mode === 'through' && online(a)() && !a.relayed && !env.relay.copy('main').diverged, 'a back through');
  assert.equal(a.state.tasks.x, undefined, 'the server never had it');
  // The next outage, and b is back
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed, 'local again');
  b.link.goOnline();
  await env.until(() => a.state.tasks.x?.title === 'offline' && b.relayed, 'b\'s edit reaches a again');
  assert.equal(b.pending, 1);
  env.central.goUp();
  await env.until(() => b.pending === 0 && online(a, b)() && !a.relayed && !b.relayed, 'and the server');
  assert.deepEqual(plain(a.state), env.central.store().snapshot());
  assert.equal(env.central.store().snapshot().tasks.x.title, 'offline');
});

test('a display under an epoch the copy never held waits for the server rather than be rolled back; one under an epoch the copy moved on from hears the reset', async t => {
  const env = lan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  a.state.tasks.x = { id: 'x', title: 'before the restore' };
  await env.until(() => a.pending === 0 && b.state.tasks.x, 'synced');
  const doc = env.central.store().export();
  a.state.tasks.y = { id: 'y', title: 'lost to the restore' };
  await env.until(() => a.pending === 0 && b.state.tasks.y, 'synced');
  env.relay.flush();
  const stale = env.storage.load();   // the copy under the first epoch, with y
  const resets = { a: [], b: [] };
  a.on('reset', r => resets.a.push(r));
  b.on('reset', r => resets.b.push(r));
  // b is away while the server is restored; a and the relay see it
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b away');
  env.central.stores.restore('main', doc);
  const restored = env.central.store().epoch;
  await env.until(() => resets.a.length === 1 && a.status === 'online' && env.relay.copy('main').epoch === restored, 'restored');

  // The server goes away: b, under the epoch the copy moved on from, is answered and hears the reset
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed, 'local');
  b.link.goOnline();
  await env.until(() => b.relayed && b.status === 'online', 'b answered');
  assert.equal(resets.b.length, 1);
  assert.equal(b.state.tasks.y, undefined);

  // A relay restarted from a copy written before the restore: a, under the newer epoch, is not rolled back to it
  env.relay.close();
  env.relay = env.makeRelay({ storage: { load: () => stale, write() {}, writeKnown() {}, remove() {} } });
  env.relay.upstreamDown();
  await run(env, 40);
  assert.equal(a.status, 'connecting', 'a waits for the server');
  assert.equal(a.relayed, false);
  assert.equal(a.state.tasks.y, undefined, 'with its own state');
  assert.equal(resets.a.length, 1, 'and no reset');
  env.central.goUp();
  env.relay.upstreamUp();
  await env.until(() => online(a)() && !a.relayed, 'through');
  assert.equal(resets.a.length, 1);
  assert.deepEqual(plain(a.state), env.central.store().snapshot());
});

test('the server\'s clock is written every minute with nothing else to write', async t => {
  t.mock.timers.enable({ apis: ['setInterval', 'setTimeout'] });
  const writes = [];
  const storage = knownCopies({});
  storage.writeKnown({ format: 'lazy-storage/relay-known', centralTs: 5_000, keys: [] });
  const writeKnown = storage.writeKnown;
  storage.writeKnown = doc => { writes.push(doc.centralTs); writeKnown(doc); };
  const relay = createRelay({ storage, keepalive: false, onError: err => { throw err; } });
  for (const until = performance.now() + 2; performance.now() < until;);   // the clock carried on a little
  t.mock.timers.tick(60_000);
  assert.equal(writes.length, 0, 'once the minute is out, within saveDelay');
  t.mock.timers.tick(1_000);
  assert.equal(writes.length, 1);
  assert.ok(writes[0] > 5_000, 'carried on from where it was loaded');
  relay.close();
});

test('a socket waiting to be dialled through holds no more than 16 MB of what its client said', () => {
  const relay = createRelay({ keepalive: false, grace: 60_000, jitter: 0, onError: err => { throw err; } });
  const closes = [];
  const s = relay.accept({ send() {}, close: code => closes.push(code), key: 'k', upstream: manual() });
  const data = 'x'.repeat(3 * 1024 * 1024);
  for (let i = 0; i < 5; i++) s.receive({ t: 'share', store: 'main', data });
  assert.equal(s.state, 'dialing', 'fifteen of them wait');
  s.receive({ t: 'share', store: 'main', data });
  assert.equal(s.state, 'closed');
  assert.deepEqual(closes, [1008]);
  relay.close();
});
