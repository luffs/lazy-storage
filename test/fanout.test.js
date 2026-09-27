// fanout.test.js - A relay that reads each store once for all its clients, each judged by the server, each writing as itself
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createHub, memoryStorage } from '../src/server/index.js';
import { createRelayLink, credentialRequest, relaySockets } from '../src/server/relays.js';
import { createClient } from '../src/client/index.js';
import { createConnection } from '../src/client/connection.js';
import { createRelay, memoryCopies } from '../src/relay/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * The server ("central") on a network of its own, with a relay route; a
 * relay with a link to it; displays on the LAN. `central.sockets` are the
 * clients' own sockets at central (a display's socket up, for its edits, or
 * a display passed through), `central.endpoints` the relay links. A display
 * is known to central by its credential, `key-<name>`: `central.users` maps
 * it to the user, and `central.deny` holds 'user:store' pairs refused.
 * `central.filter(message, user, via)` returning false withholds what central
 * sends (`central.withheld`, to release). `goDown()` takes central away, the
 * relay's link with it
 */
function fanLan({ relay: relayOptions = {}, store: storeOptions = {}, link: linkOptions = {}, storage = memoryCopies() } = {}) {
  const time = fakeTime(1_000_000);
  const storages = new Map();
  const stores = createStores(id => {
    if (id.startsWith('unknown')) return null;
    if (!storages.has(id)) storages.set(id, memoryStorage());
    return createStore({ initial: INITIAL, presence: true, now: time, storage: storages.get(id), ...storeOptions });
  });
  const central = {
    stores,
    heard: [],
    users: new Map(),
    deny: new Set(),
    sockets: new Set(),
    endpoints: new Set(),
    links: new Set(),
    withheld: [],
    filter: null,
    down: false,
    store: (id = 'main') => stores.get(id),
    /** Deliver what was withheld and `which(entry)` picks ({ message, via, user }), in order */
    release(which = () => true) {
      const now = central.withheld.filter(which);
      central.withheld = central.withheld.filter(entry => !now.includes(entry));
      for (const entry of now) entry.deliver();
    },
    goDown() {
      central.down = true;
      for (const link of central.links) link.goOffline();
      relayLink.goOffline();
    },
    goUp() {
      central.down = false;
      for (const link of central.links) link.goOnline();
      relayLink.goOnline();
    }
  };
  const authorizeId = (user, id) => !central.deny.has(`${user?.id}:${id}`);
  const withholding = (send, user, via) => message => {
    if (central.filter && central.filter(message, user, via) === false) central.withheld.push({ message, via, user, deliver: () => send(message) });
    else send(message);
  };
  central.net = createNetwork({
    session: ({ send, user, onEvict }) => {
      const hub = createHub(id => stores.get(id), { send: withholding(send, user, 'client'), user, authorizeId });
      const entry = { user, onEvict, hub };
      central.sockets.add(entry);
      return {
        receive(message) {
          central.heard.push({ user: user?.id, via: 'client', message });
          hub.receive(message);
        },
        close() {
          central.sockets.delete(entry);
          hub.close();
        }
      };
    }
  });
  central.linkNet = createNetwork({
    session: ({ send, user, onEvict }) => {
      const endpoint = createRelayLink(id => stores.get(id), {
        send: withholding(send, user, 'link'),
        relay: user,
        admit: credential => {
          const who = central.users.get(credential?.headers?.authorization);
          return who ? { user: who, expires: who.expires ?? null } : null;
        },
        authorizeId,
        linger: 50,
        rate: false,
        ...linkOptions
      });
      const entry = { endpoint, onEvict };
      central.endpoints.add(endpoint);
      return {
        receive(message) {
          central.heard.push({ user: 'relay', via: 'link', message });
          endpoint.receive(message);
        },
        close() {
          central.endpoints.delete(endpoint);
          endpoint.close();
        },
        entry
      };
    }
  });
  const relayLink = central.linkNet.link({ user: { id: 'hub-1' } });
  central.relayLink = relayLink;
  const env = { central, storage, time, displays: [], errors: [] };
  const upstreamFor = user => {
    const link = central.net.link({ user });
    central.links.add(link);
    if (central.down) link.goOffline();
    return () => {
      if (!central.down && !link.online) link.goOnline();
      return link.factory();
    };
  };
  env.makeRelay = (options = {}) => createRelay({
    storage, grace: 0, probeEvery: 5, dialTimeout: 200, keepalive: false, jitter: 0, saveDelay: 5, now: time,
    onError: err => env.errors.push(err), link: relayLink.factory, linger: 50, writeIdle: 50,
    ...relayOptions, ...options
  });
  env.relay = env.makeRelay();
  env.net = createNetwork({
    session: ({ send, user, onEvict }) => env.relay.accept({
      send,
      close: (code, reason) => onEvict(code, reason),
      key: user.key,
      upstream: upstreamFor(user),
      credential: user.through ? undefined : { headers: { authorization: user.key } }
    })
  });
  env.until = async (pred, label) => {
    for (let i = 0; i < 800; i++) {
      await central.net.settle();
      await central.linkNet.settle();
      await env.net.settle();
      await central.linkNet.settle();
      await central.net.settle();
      if (pred()) return;
      await sleep(2);
    }
    throw new Error(`timeout: ${label} ${JSON.stringify(env.relay.stats())} central sockets ${central.sockets.size}`);
  };
  /** A display: a client on its own socket (store 'main', replica `r-<name>`), known to central unless `stranger` */
  env.display = (name, { key = `key-${name}`, store = 'main', replicaId = `r-${name}`, through = false, stranger = false, socket, ...options } = {}) => {
    if (!stranger && !central.users.has(key)) central.users.set(key, { id: name });
    if (!socket) {
      const user = { id: name, key, through };
      const link = env.net.link({ user });
      const heard = [];
      const factory = () => {
        const t = link.factory();
        const tap = { onopen: null, onmessage: null, onclose: null, send: m => t.send(m), close: () => t.close() };
        t.onopen = () => tap.onopen?.();
        t.onmessage = m => { heard.push(m); tap.onmessage?.(m); };
        t.onclose = info => { heard.push({ t: '#close', code: info?.code }); tap.onclose?.(info); };
        return tap;
      };
      const connection = createConnection({ transport: factory, reconnect: { min: 2, max: 10 }, keepalive: false, wake: false });
      socket = { connection, link, heard, user };
    }
    const db = createClient({ connection: socket.connection, store, initial: INITIAL, replicaId, now: env.time, ...options });
    Object.assign(db, { name, link: socket.link, heard: socket.heard, socket });
    db.connect();
    env.displays.push(db);
    return db;
  };
  env.answers = (db, from = 0) => db.heard.slice(from).filter(m => m.store === db.store && (m.t === 'snapshot' || m.t === 'delta'));
  env.close = () => {
    for (const db of env.displays) db.dispose();
    env.relay.close();
    stores.dispose();
  };
  return env;
}

const online = (...dbs) => () => dbs.every(db => db.status === 'online');
const clientHellos = (env, user) => env.central.heard.filter(h => h.via === 'client' && h.user === user && h.message.t === 'hello').map(h => h.message);
const vouches = (env, store = 'main') => env.central.heard.filter(h => h.via === 'link' && h.message.t === 'vouch' && h.message.store === store).map(h => h.message);

test('fan-out: the relay reads each store once for all its displays, and the server lists each display as itself', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const [a, b, c] = ['a', 'b', 'c'].map(name => env.display(name));
  await env.until(online(a, b, c), 'online');
  const store = env.central.store();
  assert.deepEqual(store.presence().map(u => u.id).sort(), ['a', 'b', 'c'], 'each display in presence, as its own user');
  assert.deepEqual(store.peers().map(p => p.replicaId).sort(), ['r-a', 'r-b', 'r-c']);
  assert.equal(store.sessions, 4, 'the relay\'s session and a peer for each display');
  assert.equal(env.central.sockets.size, 0, 'nothing dialled for any display: none has written');
  const stats = env.relay.stats();
  assert.equal(stats.sockets.fan, 3);
  assert.deepEqual(stats.link, { state: 'open', shared: 1, clients: 3 });
  for (const db of [a, b, c]) {
    const [answer] = env.answers(db);
    assert.equal(answer.epoch, store.epoch, 'answered under the server\'s epoch');
    assert.equal(answer.seq, 0, 'by the relay, which acknowledges nothing');
    assert.equal(db.relayed, false);
  }

  const before = store.stats().sent.patch?.messages ?? 0;
  store.patch({ tasks: { x: { id: 'x', title: 'from the server' } } });
  await env.until(() => [a, b, c].every(db => db.state.tasks.x), 'everyone has it');
  assert.equal(store.stats().sent.patch.messages - before, 1, 'one patch sent for three displays');
  assert.deepEqual(a.presence.map(u => u.id).sort(), ['a', 'b', 'c'], 'the displays see each other');
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());
});

test('writes go up as their author\'s own, on a socket of its own, and the ack lands after its patch; the socket closes when idle', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  const users = [];
  store.observe('op', e => users.push(e.user?.id));
  a.state.tasks.x = { id: 'x', title: 'from a' };
  await env.until(() => b.state.tasks.x && a.pending === 0, 'crossed and acknowledged');
  assert.deepEqual(users, ['a'], 'the op is a\'s at the server');
  const [hello] = clientHellos(env, 'a');
  assert.equal(hello.follow, false, 'a write-only session, under a\'s own credential');
  assert.deepEqual(store.presence().map(u => u.id).sort(), ['a', 'b'], 'and it is listed once, as the relay\'s peer');
  const patchAt = a.heard.findIndex(m => m.t === 'patch' && m.diff?.tasks?.x);
  const ackAt = a.heard.findIndex(m => m.t === 'ack');
  assert.ok(patchAt !== -1 && ackAt > patchAt, 'the ack after the patch, as on a direct socket');
  assert.equal(a.heard[ackAt].v, store.version, 'the ack says the version with the op in it');
  await env.until(() => env.central.sockets.size === 0, 'the write socket closed when there was nothing to wait for');
  b.state.tasks.x.title = 'from b';
  await env.until(() => a.state.tasks.x.title === 'from b' && b.pending === 0, 'b wrote too');
  assert.deepEqual(users, ['a', 'b']);
});

test('answers from the copy: a delta out of its log for a display a little behind, a snapshot for one further behind than the log reaches', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b away');
  for (let i = 0; i < 3; i++) store.patch({ tasks: { [`p${i}`]: { id: `p${i}`, n: i } } });
  await env.until(() => env.relay.copy('main').v === store.version, 'the copy has them');
  let from = b.heard.length;
  b.link.goOnline();
  b.socket.connection.connect();
  await env.until(() => b.status === 'online' && b.state.tasks.p2, 'b back');
  let [answer] = env.answers(b, from);
  assert.equal(answer.t, 'delta');
  assert.equal(answer.patches.length, 3, 'the three it missed, from the relay\'s log');
  assert.equal(answer.v, store.version);
  assert.equal(clientHellos(env, 'b').length, 0, 'nothing of b\'s went to the server but its vouch');

  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b away again');
  for (let i = 0; i < 1005; i++) store.patch({ tasks: { many: { id: 'many', n: i } } });
  await env.until(() => env.relay.copy('main').v === store.version, 'the copy has them');
  from = b.heard.length;
  b.link.goOnline();
  b.socket.connection.connect();
  await env.until(() => b.status === 'online' && b.state.tasks.many?.n === 1004, 'b back');
  [answer] = env.answers(b, from);
  assert.equal(answer.t, 'snapshot', 'more than the log keeps');
  assert.deepEqual(JSON.parse(JSON.stringify(b.state)), store.snapshot());
});

test('a display with edits pending says hello up its own socket: the server answers it and takes the edits as its own, and the copy brings it along', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b away');
  b.state.tasks.y = { id: 'y', title: 'from b, while away' };
  store.patch({ tasks: { z: { id: 'z', title: 'the server, meanwhile' } } });
  const from = b.heard.length;
  b.link.goOnline();
  b.socket.connection.connect();
  await env.until(() => b.status === 'online' && b.pending === 0 && a.state.tasks.y && b.state.tasks.z, 'b back, its edit through');
  const [answer] = env.answers(b, from);
  assert.ok(answer.seq >= 1, 'answered by the server, which acknowledged the hello\'s op');
  const [hello] = clientHellos(env, 'b');
  assert.equal(hello.ops.length, 1);
  assert.equal(hello.follow, false);
  assert.equal(hello.share, undefined);
  store.patch({ tasks: { w: { id: 'w', title: 'after' } } });
  await env.until(() => b.state.tasks.w && a.state.tasks.w, 'both follow on');
  assert.deepEqual(JSON.parse(JSON.stringify(b.state)), store.snapshot());
});

test('an ack heard after later patches corrects to what the copy holds then, not to what the server held when it sent it', async t => {
  const env = fanLan();
  t.after(() => env.close());
  // b's clock runs a second ahead of a's
  const a = env.display('a', { now: fakeTime(1_000_000) });
  const b = env.display('b', { now: fakeTime(1_001_000) });
  await env.until(online(a, b), 'online');
  b.state.tasks.x = { id: 'x', title: 'b0' };
  await env.until(() => a.state.tasks.x?.title === 'b0' && b.pending === 0, 'b0 everywhere');
  // The link is slow: nothing new reaches the relay for a while, so a
  // writes without having heard b's b1, and loses to it; and a's own
  // socket up is slower still, so its ack comes after b's b2
  env.central.filter = (message, user, via) => !(via === 'link' && message.t === 'patch') && !(via === 'client' && user?.id === 'a' && message.t === 'ack');
  b.state.tasks.x.title = 'b1';
  await env.until(() => env.central.store().snapshot().tasks.x.title === 'b1', 'the server has b1');
  const conflicts = [];
  a.on('conflict', c => conflicts.push(c));
  a.state.tasks.x.title = 'a, older than b1';
  await env.until(() => env.central.withheld.some(e => e.message.t === 'ack'), 'a\'s op lost, its ack withheld');
  assert.equal(env.central.withheld.find(e => e.message.t === 'ack').message.correction.tasks.x.title, 'b1', 'the server corrected a to b1, as it was then');
  b.state.tasks.x.title = 'b2';
  await env.until(() => env.central.store().snapshot().tasks.x.title === 'b2', 'the server has b2');
  env.central.filter = null;
  env.central.release(e => e.via === 'link');
  await env.until(() => a.state.tasks.x.title === 'b2', 'b1 and b2 reached a');
  env.central.release();
  await env.until(() => a.pending === 0 && b.pending === 0, 'the acks reached a and b');
  assert.equal(a.state.tasks.x.title, 'b2', 'not rolled back to b1');
  assert.equal(conflicts.length, 1);
  assert.equal(conflicts[0].lost[0].theirs, 'b2');
  assert.deepEqual(JSON.parse(JSON.stringify(a.state)), env.central.store().snapshot());
});

test('revoked: a display whose access goes hears forbidden, is not served that store offline after, and is let in again once it may', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  env.central.deny.add('b:main');
  let revoked = 0;
  for (const endpoint of env.central.endpoints) revoked += await endpoint.revalidate();
  assert.equal(revoked, 1);
  await env.until(() => b.closed?.code === 'forbidden', 'b told');
  assert.deepEqual(env.central.store().presence().map(u => u.id), ['a']);
  // Offline, b is not answered from the copy: the server's word holds there too
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed, 'a answered by the relay');
  b.connect();
  await sleep(30);
  await env.until(() => true, 'settled');
  assert.equal(b.status, 'connecting', 'b waits for the server');
  env.central.deny.delete('b:main');
  env.central.goUp();
  await env.until(() => online(a, b)() && !a.relayed && !b.relayed, 'both through again');
});

test('signed out: the server ends a display\'s sessions (4001) and it comes back, judged afresh; a credential turned away gets 4401', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const before = vouches(env).length;
  let count = 0;
  for (const endpoint of env.central.endpoints) count += endpoint.disconnect(user => user?.id === 'a');
  assert.equal(count, 1);
  await env.until(() => a.heard.some(m => m.t === '#close' && m.code === 4001), 'a\'s socket closed with 4001');
  await env.until(() => a.status === 'online' && vouches(env).length > before, 'a back, vouched for again');

  // Its credential no longer works: the relay turns the socket away as the server would
  env.central.users.delete('key-a');
  for (const endpoint of env.central.endpoints) endpoint.disconnect(user => user?.id === 'a');
  await env.until(() => a.closed?.code === 'unauthorized', 'a turned away');
  assert.ok(a.heard.some(m => m.t === '#close' && m.code === 4401));
  assert.equal(b.status, 'online', 'b is not touched');
});

test('refusals: a stranger\'s socket is turned away (4401), a store the display may not open is closed forbidden, an unknown one unknown-store', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const mallory = env.display('mallory', { stranger: true });
  const a = env.display('a');
  env.central.deny.add('a:private');
  const priv = env.display('a', { store: 'private', replicaId: 'r-a-private', socket: a.socket });
  const unknown = env.display('a', { store: 'unknown-1', replicaId: 'r-a-unknown', socket: a.socket });
  await env.until(() => mallory.closed?.code === 'unauthorized' && priv.closed?.code === 'forbidden' && unknown.closed?.code === 'unknown-store' && a.status === 'online', 'each told');
  assert.deepEqual(env.central.store().presence().map(u => u.id), ['a']);
});

test('presence: a display\'s share goes to the server as its own and reaches the others; leaving takes it off', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  a.share({ cursor: 3 });
  await env.until(() => b.peers.find(p => p.replicaId === 'r-a')?.data?.cursor === 3, 'b sees a\'s share');
  assert.equal(env.central.store().peers().find(p => p.replicaId === 'r-a').data.cursor, 3, 'the server holds it as a\'s');
  a.disconnect();
  await env.until(() => !b.peers.some(p => p.replicaId === 'r-a'), 'a gone from b\'s list');
  assert.deepEqual(env.central.store().presence().map(u => u.id), ['b']);
});

test('the link lost for less than grace: the displays see nothing; an edit up a socket already open goes on, a first one waits for the link', async t => {
  const env = fanLan({ relay: { grace: 60_000, writeIdle: 60_000 } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  a.state.tasks.w = { id: 'w', title: 'a writes before' };
  await env.until(() => a.pending === 0 && b.state.tasks.w, 'a has a socket up');
  env.central.relayLink.goOffline();
  await env.until(() => env.relay.stats().link.state !== 'open', 'the link is down');
  store.patch({ tasks: { x: { id: 'x', title: 'while the link was down' } } });
  a.state.tasks.y = { id: 'y', title: 'from a, meanwhile' };
  b.state.tasks.z = { id: 'z', title: 'from b, meanwhile' };
  await env.until(() => store.snapshot().tasks.y, 'a\'s edit went up the socket it had');
  assert.equal(store.snapshot().tasks.z, undefined, 'b\'s first waits for the server\'s word on b, which went with the link');
  env.central.relayLink.goOnline();
  await env.until(() => env.relay.stats().link.state === 'open' && [a, b].every(db => db.state.tasks.x && db.state.tasks.y && db.state.tasks.z && db.pending === 0), 'caught up');
  assert.equal(env.relay.mode, 'through');
  assert.ok(!a.heard.some(m => m.t === 'closed') && !b.heard.some(m => m.t === 'closed'), 'nobody was told to start over');
  for (const db of [a, b]) assert.deepEqual(JSON.parse(JSON.stringify(db.state)), store.snapshot());
});

test('the server away for longer: the relay answers on its own, and on its way back every display that edited meanwhile says hello again, its edits reaching the server as its own', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  const c = env.display('c');
  await env.until(online(a, b, c), 'online');
  const store = env.central.store();
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && [a, b, c].every(db => db.relayed), 'answered by the relay');
  a.state.tasks.x = { id: 'x', title: 'offline, from a' };
  await env.until(() => b.state.tasks.x && c.state.tasks.x, 'the edit crossed on the LAN');
  assert.equal(a.pending, 1);
  const users = [];
  store.observe('op', e => users.push(e.user?.id));
  env.central.goUp();
  env.relay.upstreamUp();
  await env.until(() => [a, b, c].every(db => db.status === 'online' && !db.relayed && db.pending === 0) && store.snapshot().tasks.x, 'through again');
  assert.deepEqual(users, ['a'], 'a\'s edit, as a\'s');
  for (const db of [a, b, c]) assert.deepEqual(JSON.parse(JSON.stringify(db.state)), store.snapshot());
  assert.deepEqual(env.relay.copy('main').state, store.snapshot());
});

test('a server whose relay route turns the link away (4401), or that answers the displays but not the link: the displays are passed through', async t => {
  for (const how of ['refused', 'broken']) {
    const env = fanLan({ relay: { grace: 20 } });
    if (how === 'refused') {
      env.relay.close();
      env.relay = env.makeRelay({
        link: () => {
          const t = { onopen: null, onmessage: null, onclose: null, send() {}, close() {} };
          setTimeout(() => t.onclose?.({ code: 4401, reason: 'Unauthorized' }), 1);
          return t;
        }
      });
    } else {
      env.central.relayLink.goOffline();
    }
    const a = env.display('a');
    const b = env.display('b');
    await env.until(() => online(a, b)() && env.central.sockets.size === 2, `${how}: both passed through`);
    assert.equal(env.relay.stats().link.state, how);
    assert.equal(env.relay.stats().sockets.through, 2);
    a.state.tasks.x = { id: 'x' };
    await env.until(() => b.state.tasks.x && a.pending === 0, `${how}: edits cross`);
    if (how === 'refused') assert.ok(env.errors.some(err => /4401/.test(err.message)), 'the refusal is reported');
    env.close();
  }
});

test('the store: a write-only session is only for a replica a relay serves; a relay\'s session writes nothing; a peer claims its replica as a hello would', () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const sent = [];
  // Nobody reads unlisted: follow:false from a replica no relay serves is closed, not final
  const lone = store.session({ send: m => sent.push(m), user: { id: 'u1' } });
  lone.receive({ t: 'hello', replicaId: 'r1', ops: [], follow: false });
  assert.equal(sent.at(-1).t, 'closed');
  assert.equal(sent.at(-1).code, 'unavailable');
  assert.equal(store.sessions, 0);

  const peer = store.peer({ user: { id: 'u1' }, replicaId: 'r1', via: 'hub' });
  assert.deepEqual(store.presence(), [{ id: 'u1' }]);
  assert.throws(() => store.peer({ user: { id: 'u2' }, replicaId: 'r1' }), err => err.code === 'replica-taken');
  const write = [];
  const writer = store.session({ send: m => write.push(m), user: { id: 'u1' } });
  writer.receive({ t: 'hello', replicaId: 'r1', ops: [], follow: false, share: { x: 1 } });
  assert.ok(write.some(m => m.t === 'error' && m.code === 'forbidden'), 'a write-only session shares nothing');
  assert.ok(write.some(m => m.t === 'snapshot'), 'answered');
  assert.equal(store.peers().length, 1, 'listed once, as the peer');
  const relaySent = [];
  const relay = store.session({ send: m => relaySent.push(m), user: { id: 'hub' }, relay: true });
  relay.receive({ t: 'hello', replicaId: 'relay', ops: [{ replicaId: 'r9', seq: 1, ts: [1, 0, 'r9'], diff: { tasks: { a: { id: 'a' } } } }] });
  relay.receive({ t: 'op', op: { replicaId: 'r9', seq: 2, ts: [1, 0, 'r9'], diff: { tasks: { b: { id: 'b' } } } } });
  assert.deepEqual(store.snapshot().tasks, {}, 'a relay\'s session writes nothing');
  assert.equal(relaySent.filter(m => m.t === 'error' && m.code === 'forbidden').length, 2);
  assert.deepEqual(store.presence(), [{ id: 'u1' }], 'nor is it listed');

  writer.receive({ t: 'op', op: { replicaId: 'r1', seq: 1, ts: [Date.now(), 0, 'r1'], diff: { tasks: { w: { id: 'w' } } } } });
  const ack = write.find(m => m.t === 'ack');
  assert.equal(ack.v, store.version, 'the ack carries the version');
  assert.ok(relaySent.some(m => m.t === 'patch'), 'the relay hears the patch');
  assert.ok(!write.some(m => m.t === 'patch'), 'the write-only session does not');
  peer.share({ at: 1 });
  assert.deepEqual(store.peers()[0].data, { at: 1 });
  store.closeSessions(s => s.user?.id === 'u1');
  assert.equal(peer.closed, true, 'evicted with the rest');
  assert.deepEqual(store.presence(), []);
  store.dispose();
});

test('the relay link: its own session is let in only while a client is, closed `linger` after the last, and at once when its access goes; revalidate judges the relay too', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const sent = [];
  let carry = true;
  const link = createRelayLink(() => store, {
    send: m => sent.push(m),
    relay: { id: 'hub' },
    admit: credential => (credential?.headers?.authorization === 'ok' ? { user: { id: 'u1' }, expires: null } : null),
    authorizeRelay: () => carry,
    linger: 20
  });
  const last = () => sent.at(-1);
  link.receive({ t: 'hello', store: 'main', replicaId: 'relay' });
  assert.deepEqual([last().t, last().code], ['closed', 'unused'], 'no client let in: not yet');
  link.receive({ t: 'vouch', grant: 'g1', store: 'main', replicaId: 'r1', credential: { headers: { authorization: 'nope' } } });
  assert.deepEqual([last().t, last().code], ['refused', 'unauthorized']);
  link.receive({ t: 'vouch', grant: 'g1', socket: 's1', store: 'main', replicaId: 'r1', credential: { headers: { authorization: 'ok' } } });
  assert.equal(last().t, 'admitted');
  assert.deepEqual(last().peer, { replicaId: 'r1', user: { id: 'u1' }, key: 'u1' });
  link.receive({ t: 'hello', store: 'main', replicaId: 'relay' });
  assert.equal(sent.find(m => m.t === 'snapshot').store, 'main');
  assert.deepEqual(link.stores, ['main']);
  const [relayEntry, client] = relaySockets(link, { buffered: 0, idleMs: 1, openMs: 2 });
  assert.deepEqual(relayEntry.relay, { id: 'hub' });
  assert.deepEqual([client.user, client.stores, client.via], [{ id: 'u1' }, ['main'], { id: 'hub' }]);

  link.receive({ t: 'unvouch', grant: 'g1' });
  assert.deepEqual(link.stores, ['main'], 'lingers');
  await sleep(40);
  assert.deepEqual(link.stores, [], 'gone after linger');
  assert.deepEqual([last().t, last().code], ['closed', 'unused']);

  link.receive({ t: 'vouch', grant: 'g2', store: 'main', replicaId: 'r1', credential: { headers: { authorization: 'ok' } } });
  link.receive({ t: 'hello', store: 'main', replicaId: 'relay' });
  carry = false;
  assert.equal(await link.revalidate(), 1, 'the relay may no longer carry it: its client there revoked');
  assert.ok(sent.some(m => m.t === 'revoked' && m.grants.includes('g2') && m.code === 'forbidden'));
  assert.ok(sent.some(m => m.t === 'closed' && m.store === 'main' && m.code === 'forbidden'), 'and its session there closed');
  assert.deepEqual(store.presence(), []);
  link.close();
  store.dispose();
});

test('a vouch\'s credential is made a request of only what says who a client is; a vouch withdrawn before its verdict admits nobody', async () => {
  const req = credentialRequest({ headers: { Authorization: 'Bearer x', cookie: 'c=1', 'user-agent': 'kiosk', origin: 'https://evil', 'x-forwarded-for': '1.2.3.4', host: 'central' }, query: '?t=1' }, '/sync');
  assert.equal(req.headers.get('authorization'), 'Bearer x');
  assert.equal(req.headers.get('cookie'), 'c=1');
  assert.equal(req.headers.get('user-agent'), 'kiosk');
  assert.equal(req.headers.get('origin'), null);
  assert.equal(req.headers.get('x-forwarded-for'), null);
  assert.equal(new URL(req.url).pathname + new URL(req.url).search, '/sync?t=1');
  assert.equal(credentialRequest({ headers: { 'x-tenant': 'a' } }, '/sync', ['X-Tenant']).headers.get('x-tenant'), 'a');

  const store = createStore({ initial: INITIAL, presence: true });
  const sent = [];
  let answer;
  const link = createRelayLink(() => store, { send: m => sent.push(m), relay: 'hub', admit: () => new Promise(resolve => { answer = resolve; }) });
  link.receive({ t: 'vouch', grant: 'g1', store: 'main', replicaId: 'r1', credential: {} });
  link.receive({ t: 'unvouch', grant: 'g1' });
  answer({ user: { id: 'u1' }, expires: null });
  await sleep(1);
  assert.equal(sent.length, 0, 'nothing said');
  assert.deepEqual(store.presence(), [], 'nobody admitted');
  link.close();
  store.dispose();
});

test('the Node adapter\'s relay route: a relay let in as itself, each client vouched for by the same hooks, cut off by disconnect, judged by revalidate', async () => {
  const { serve } = await import('../src/server/node.js');
  const { upstreamSocket } = await import('../src/relay/bun.js');
  const { WebSocket: WsSocket } = await import('ws');
  const { once } = await import('node:events');
  const stores = createStores(() => createStore({ initial: INITIAL, presence: true }));
  const seen = [];
  let carry = true;
  const server = serve({
    stores,
    port: 0,
    path: '/sync',
    authenticate: (req, context) => {
      const token = req.headers.get('authorization')?.replace('Bearer ', '');
      seen.push({ token, relay: context?.relay ?? null, origin: req.headers.get('origin') });
      return token?.startsWith('user-') ? { id: token } : null;
    },
    expiresAt: () => Date.now() + 3_600_000,
    relays: {
      authenticate: req => (req.headers.get('authorization') === 'Bearer relay' ? { id: 'hub' } : null),
      expiresAt: () => Date.now() + 3_600_000,
      authorize: () => carry
    }
  });
  await once(server, 'listening');
  const port = server.address().port;
  try {
    // A relay with a wrong credential is turned away (4401)
    const refused = await new Promise(resolve => {
      const ws = new WsSocket(`ws://localhost:${port}/sync/relay`, { headers: { authorization: 'Bearer nope' } });
      ws.on('close', code => resolve(code));
      ws.on('error', () => {});
    });
    assert.equal(refused, 4401);

    const errors = [];
    const relay = createRelay({ grace: 1000, keepalive: false, jitter: 0, dialTimeout: 2000, onError: err => errors.push(err), link: upstreamSocket(`ws://localhost:${port}/sync/relay`, { headers: { authorization: 'Bearer relay' }, WebSocket: WsSocket }) });
    const net = createNetwork({
      session: ({ send, user, onEvict }) => relay.accept({
        send,
        close: (code, reason) => onEvict(code, reason),
        key: user.key,
        upstream: upstreamSocket(`ws://localhost:${port}/sync`, { headers: { authorization: `Bearer ${user.key}` }, WebSocket: WsSocket }),
        credential: { headers: { authorization: `Bearer ${user.key}`, origin: 'https://made.up' } }
      })
    });
    const display = name => {
      const link = net.link({ user: { id: name, key: `user-${name}` } });
      const db = createClient({ connection: createConnection({ transport: link.factory, reconnect: { min: 5, max: 20 }, keepalive: false, wake: false }), store: 'main', initial: INITIAL, replicaId: `r-${name}` });
      db.connect();
      return db;
    };
    const a = display('a');
    const b = display('b');
    const settle = async (pred, label) => {
      for (let i = 0; i < 500; i++) {
        await net.settle();
        if (pred()) return;
        await sleep(10);
      }
      throw new Error(`timeout: ${label}`);
    };
    await settle(() => a.status === 'online' && b.status === 'online', 'online');
    assert.ok(seen.some(s => s.token === 'user-a' && s.relay?.id === 'hub' && s.origin === null), 'judged by authenticate, told the relay, the made-up origin left out');
    const listed = server.sockets();
    assert.equal(listed.filter(s => s.relay).length, 1);
    assert.deepEqual(listed.filter(s => s.via).map(s => s.user.id).sort(), ['user-a', 'user-b']);
    assert.equal(server.socketStats().clients, 2);
    a.state.tasks.x = { id: 'x' };
    await settle(() => b.state.tasks.x && a.pending === 0, 'crossed');

    // The relay no longer carries the store: its clients there hear forbidden
    carry = false;
    assert.equal(await server.revalidate(), 2);
    await settle(() => a.closed?.code === 'forbidden' && b.closed?.code === 'forbidden', 'told');
    carry = true;
    // Cut off: the relay's link goes (4001), and it comes back
    assert.equal(server.disconnect(user => user?.id === 'hub'), 1);
    await settle(() => relay.stats().link.state !== 'open', 'the link closed');
    await settle(() => relay.stats().link.state === 'open', 'the link back');
    relay.close();
  } finally {
    await server.shutdown();
  }
});

test('the relay link turns away what does not add up, and ends a client as the store does: unloaded, evicted, run out', async () => {
  let locked = false;
  const made = new Map();
  const resolveStore = id => {
    if (id === 'broken') throw new Error('the factory failed');
    if (locked && id === 'main') throw Object.assign(new Error('Served by another process'), { code: 'store-locked' });
    if (id.startsWith('unknown')) return null;
    if (!made.has(id) || made.get(id).disposed) made.set(id, createStore({ initial: INITIAL, presence: true }));
    return made.get(id);
  };
  const sent = [];
  const faults = [];
  const link = createRelayLink(resolveStore, {
    send: m => sent.push(m),
    relay: 'hub',
    admit: async credential => {
      const token = credential?.headers?.authorization;
      if (token === 'throws') throw new Error('the auth service failed');
      if (token === 'soon') return { user: { id: 'soon' }, expires: Date.now() + 30 };
      return token ? { user: { id: token }, expires: null } : null;
    },
    authorize: user => {
      if (user.id === 'odd') throw new Error('refused, with a reason');
      return true;
    },
    rate: { burst: 2, perSecond: 1 },
    linger: 10,
    onError: err => faults.push(err)
  });
  const last = () => sent.at(-1);
  const vouch = (grant, store, token, replicaId = `r-${grant}`) => link.receive({ t: 'vouch', grant, store, replicaId, credential: token === undefined ? {} : { headers: { authorization: token } } });
  link.receive({ t: 'vouch', store: 'main', replicaId: 'r' });
  assert.equal(last().t, 'error', 'a vouch needs a grant id');
  vouch('g0', '../x', 'u');
  assert.equal(last().code, 'invalid-store');
  link.receive({ t: 'vouch', grant: 'g0', store: 'main', credential: {} });
  assert.equal(last().code, 'invalid');
  vouch('g1', 'main', 'throws');
  await sleep(1);
  assert.deepEqual([last().t, last().code], ['refused', 'unauthorized']);
  vouch('g2', 'main', 'odd');
  await sleep(1);
  assert.deepEqual([last().code, last().message], ['forbidden', 'refused, with a reason']);
  vouch('g3', 'broken', 'u');
  await sleep(1);
  assert.equal(last().code, 'unknown-store');
  assert.equal(faults.length, 1, 'a factory that throws is the server\'s fault, and said');
  vouch('g4', 'unknown-1', 'u');
  await sleep(1);
  assert.equal(last().code, 'unknown-store');
  locked = true;
  vouch('g5', 'main', 'u');
  await sleep(1);
  assert.equal(last().code, 'unavailable', 'another process serves it for now: not final');
  locked = false;
  // Credentials turned away empty the bucket; then every vouch waits a moment
  vouch('g6', 'main', undefined);
  await sleep(1);
  assert.equal(last().code, 'unauthorized');
  vouch('g7', 'main', 'u');
  assert.deepEqual([last().code, typeof last().retryAfter], ['rate-limited', 'number']);
  await sleep(1100);

  // A share refused, and one for a client not let in
  link.receive({ t: 'share', grant: 'nobody', data: 1 });
  assert.equal(last().code, 'unavailable');
  vouch('g8', 'main', 'u8');
  await sleep(1);
  assert.equal(last().t, 'admitted');
  link.receive({ t: 'share', grant: 'g8', data: 'x'.repeat(5000) });
  assert.equal(last().code, 'too-large');
  // The same name again, for another store: the first let go
  vouch('g8', 'side', 'u8');
  await sleep(1);
  assert.deepEqual([last().t, last().store], ['admitted', 'side']);
  assert.deepEqual(made.get('main').presence(), []);

  // Evicted by the store: revoked as that
  vouch('g9', 'main', 'u9');
  await sleep(1);
  made.get('main').closeSessions(s => s.user?.id === 'u9');
  assert.deepEqual([last().t, last().grants, last().code], ['revoked', ['g9'], 'evicted']);
  // Unloaded under it: revoked 'unavailable', to be judged afresh
  vouch('g10', 'main', 'u10');
  await sleep(1);
  link.receive({ t: 'hello', store: 'main', replicaId: 'relay' });
  made.get('main').dispose();
  assert.ok(sent.some(m => m.t === 'revoked' && m.grants.includes('g10') && m.code === 'unavailable'));
  assert.ok(sent.some(m => m.t === 'closed' && m.store === 'main' && m.code === 'unavailable'), 'the relay\'s session there too');
  // A client's session runs out: revoked, to come back judged afresh
  vouch('g11', 'main', 'soon');
  await sleep(60);
  assert.ok(sent.some(m => m.t === 'revoked' && m.grants.includes('g11') && m.code === 'reauthenticate'));
  // disconnect picks by user
  vouch('g12', 'main', 'u12');
  await sleep(1);
  assert.equal(link.disconnect(user => user.id === 'u12'), 1);
  // A message the relay's session has no hello for, and one with no store
  link.receive({ t: 'op', store: 'other', op: {} });
  assert.deepEqual([last().t, last().code], ['closed', 'unused']);
  link.receive({ t: 'hello', replicaId: 'relay' });
  assert.equal(last().code, 'invalid-store');
  link.receive('nonsense');
  assert.equal(last().t, 'error');
  link.receive({ t: 'ping' });
  assert.equal(last().t, 'pong');
  link.close();
  link.receive({ t: 'ping' });
  assert.notEqual(last().t, 'error');
  for (const store of made.values()) if (!store.disposed) store.dispose();
});

test('a credential\'s header that no request may carry is left out; a query that is no query is left out too', () => {
  const req = credentialRequest({ headers: { authorization: 'Bearer ok', cookie: 'bad\nvalue', 'user-agent': 7 }, query: 'no-question-mark' }, '/sync');
  assert.equal(req.headers.get('authorization'), 'Bearer ok');
  assert.equal(req.headers.get('cookie'), null);
  assert.equal(req.headers.get('user-agent'), null);
  assert.equal(new URL(req.url).search, '');
  assert.equal(credentialRequest(null, '/sync').headers.get('authorization'), null);
});

test('the relay\'s own session is refused where the relay may not carry the store, and while its verdict is out a revalidate starts it over', async () => {
  const store = createStore({ initial: INITIAL, presence: true });
  const sent = [];
  let verdicts = 0;
  let answer;
  const link = createRelayLink(() => store, {
    send: m => sent.push(m),
    relay: 'hub',
    admit: () => ({ user: { id: 'u' }, expires: null }),
    authorizeRelay: () => {
      verdicts++;
      return new Promise(resolve => { answer = resolve; });
    }
  });
  link.receive({ t: 'vouch', grant: 'g1', store: 'main', replicaId: 'r1', credential: {} });
  link.receive({ t: 'hello', store: 'main', replicaId: 'relay' });
  link.receive({ t: 'ping' });
  const first = answer;
  await link.revalidate();
  assert.equal(verdicts, 2, 'judged again');
  first(true);
  await sleep(1);
  assert.deepEqual(link.stores, [], 'the verdict from before the revalidate opens nothing');
  answer(false);
  await sleep(1);
  assert.ok(sent.some(m => m.t === 'closed' && m.code === 'forbidden'));
  link.close();
  store.dispose();
});

test('a client\'s socket up: dropped with an op in flight, the client says hello again and nothing is lost; turned away (4401), the client is too', async t => {
  const env = fanLan({ relay: { writeIdle: 60_000 } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  a.state.tasks.x = { id: 'x', title: 'one' };
  await env.until(() => a.pending === 0 && b.state.tasks.x, 'a has a socket up');
  // The socket up drops with an op on its way: a says hello again, and its outbox gets there
  const up = [...env.central.sockets].find(entry => entry.user?.id === 'a');
  env.central.filter = (message, user, via) => !(via === 'client' && user?.id === 'a' && message.t === 'ack');
  a.state.tasks.x.title = 'two';
  await env.until(() => env.central.withheld.length === 1, 'the ack withheld');
  env.central.filter = null;
  env.central.withheld = [];
  up.onEvict(1011, 'Gone');
  await env.until(() => a.status === 'online' && a.pending === 0 && b.state.tasks.x.title === 'two', 'a said hello again');
  assert.ok(a.heard.some(m => m.t === 'closed' && m.code === 'unavailable'));
  assert.equal(store.snapshot().tasks.x.title, 'two');
  // Turned away up its own socket: its credential is no longer good, so its socket to the relay is closed as the server would
  const again = [...env.central.sockets].find(entry => entry.user?.id === 'a');
  a.state.tasks.x.title = 'three';
  await env.until(() => a.pending === 0, 'acknowledged');
  again.onEvict(4401, 'Unauthorized');
  await env.until(() => a.heard.some(m => m.t === '#close' && m.code === 4401), 'a turned away');
  assert.equal(b.status, 'online');
});

test('offline edits through a fanned-out socket are applied on the LAN, a stranger to the server waits, and every author says hello again when the server is back; a restore reaches everyone as a reset', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed && b.relayed, 'local');
  a.state.tasks.x = { id: 'x', title: 'offline' };
  await env.until(() => b.state.tasks.x, 'crossed on the LAN');
  // A client the server never let in is not answered from the copy: it waits for the server
  const c = env.display('c');
  await sleep(30);
  await env.until(() => true, 'settled');
  assert.equal(c.status, 'connecting');
  b.state.tasks.y = { id: 'y', title: 'from b, offline' };
  await env.until(() => a.state.tasks.y, 'crossed');
  env.central.goUp();
  env.relay.upstreamUp();
  await env.until(() => [a, b, c].every(db => db.status === 'online' && !db.relayed && db.pending === 0) && store.snapshot().tasks.y, 'through again');
  for (const db of [a, b, c]) assert.deepEqual(JSON.parse(JSON.stringify(db.state)), store.snapshot());
  // A restore at the server: a new epoch, which each hears as a reset
  const resets = [];
  for (const db of [a, b, c]) db.on('reset', () => resets.push(db.name));
  const backup = store.export();
  store.patch({ tasks: { z: { id: 'z', title: 'after the backup' } } });
  await env.until(() => a.state.tasks.z, 'z everywhere');
  env.central.stores.restore('main', backup);
  await env.until(() => resets.length === 3 && [a, b, c].every(db => db.status === 'online' && !db.state.tasks.z), 'restored everywhere');
  assert.deepEqual(JSON.parse(JSON.stringify(a.state)), env.central.store().snapshot());
});

test('the relay gone local while its link is up lets go of its clients at the server, and brings them along when it goes through again', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  env.relay.upstreamDown();
  await env.until(() => a.relayed && b.relayed, 'local');
  await env.until(() => store.presence().length === 0, 'the server let go of them');
  store.patch({ tasks: { x: { id: 'x', title: 'while the relay was on its own' } } });
  env.relay.upstreamUp();
  await env.until(() => [a, b].every(db => !db.relayed && db.state.tasks.x), 'brought along');
  assert.deepEqual(store.presence().map(u => u.id).sort(), ['a', 'b'], 'listed once each');
  assert.equal(store.peers().length, 2);
});

test('a relay\'s link that goes silent is taken for gone, and a display ahead of the copy is answered by the server itself', async t => {
  const env = fanLan({ relay: { keepalive: 20, grace: 60_000 } });
  t.after(() => env.close());
  const a = env.display('a');
  await env.until(online(a), 'online');
  const store = env.central.store();
  // The link stops answering: two keepalives later it is dialled again
  env.central.filter = (message, user, via) => via !== 'link';
  await env.until(() => env.relay.stats().link.state !== 'open', 'taken for gone');
  env.central.filter = null;
  env.central.withheld = [];
  await env.until(() => env.relay.stats().link.state === 'open' && a.status === 'online', 'back');
  // A display that saw the server further along than the copy (its own hello names a version the copy has not reached)
  store.patch({ tasks: { w: { id: 'w' } } });
  await env.until(() => a.state.tasks.w, 'a has w');
  const b = env.display('b', { replicaId: 'r-b' });
  await env.until(online(b), 'b online');
  const copyV = env.relay.copy('main').v;
  b.link.goOffline();
  await env.until(() => b.status === 'offline', 'b away');
  const hellosBefore = clientHellos(env, 'b').length;
  env.central.filter = (message, user, via) => !(via === 'link' && message.t === 'patch');
  store.patch({ tasks: { v: { id: 'v' } } });
  await env.until(() => env.central.withheld.length >= 1, 'the relay does not hear it yet');
  // b hears of it from a server of its own, as it were: its version says so
  b.link.goOnline();
  b.socket.connection.connect();
  await env.until(() => b.status === 'online', 'b back');
  env.central.filter = null;
  env.central.release();
  await env.until(() => a.state.tasks.v && b.state.tasks.v, 'both have v');
  assert.ok(env.relay.copy('main').v > copyV);
  assert.ok(clientHellos(env, 'b').length >= hellosBefore);
});

test('a fanned-out display behind the copy when the relay goes local (patches of a display passed through, while its own re-vouch was out) says hello again, and has them', async t => {
  const env = fanLan({ relay: { grace: 60_000 } });
  t.after(() => env.close());
  const a = env.display('a');
  const p = env.display('p', { through: true });
  await env.until(online(a, p), 'online');
  // The link goes: a is not let in again until it is back, while p, passed through, goes on with the server
  env.central.relayLink.goOffline();
  await env.until(() => env.relay.stats().link.state !== 'open', 'the link is down');
  p.state.tasks.x = { id: 'x', title: 'from p, the link down' };
  await env.until(() => env.relay.copy('main').state.tasks.x, 'the copy has it, from p\'s socket');
  assert.equal(a.state.tasks.x, undefined, 'a is not let in again yet');
  // Then the server is away too: the relay answers on its own, and a is brought to the copy by a hello of its own
  env.central.goDown();
  env.relay.upstreamDown();
  await env.until(() => a.status === 'online' && a.relayed && a.state.tasks.x, 'a has x, from the copy');
  assert.ok(a.heard.some(m => m.t === 'closed' && m.code === 'unavailable'), 'told to say hello again');
});

// --- Presence on the relay's session: asked for only while a display behind it wants it ---------------

/** The relay's own hellos up its link on a store */
const relayHellos = (env, store = 'main') => env.central.heard.filter(h => h.via === 'link' && h.message.t === 'hello' && h.message.store === store).map(h => h.message);
/** What central sends down the relay's link from now on (every message let through) */
function tapLink(env) {
  const down = [];
  env.central.filter = (message, user, via) => {
    if (via === 'link') down.push(message);
  };
  return down;
}
const idsOf = users => users.map(u => u.id).sort();
const presenceHeard = db => db.heard.filter(m => m.t === 'presence' && m.store === db.store);
/** A little longer than a presence flush takes to come, if one were coming */
const quiet = async env => {
  await sleep(10);
  await env.until(() => true, 'settled');
};

test('presence: displays that none of them want it: the relay\'s session says so in its hello, and no presence comes down the link as displays come and go; the server lists each still', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const down = tapLink(env);
  const a = env.display('a', { presence: false });
  const b = env.display('b', { presence: false });
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  assert.deepEqual(relayHellos(env).map(h => h.presence), [false], 'one hello, saying presence: false');
  const c = env.display('c', { presence: false });
  await env.until(() => c.status === 'online' && store.presence().length === 3, 'c in');
  assert.deepEqual(idsOf(store.presence()), ['a', 'b', 'c'], 'each display listed at the server all the same');
  b.disconnect();
  await env.until(() => store.presence().length === 2, 'b gone');
  await quiet(env);
  assert.deepEqual(idsOf(store.presence()), ['a', 'c']);
  assert.ok(down.some(m => m.t === 'admitted' && m.presence === true), 'the server said the store has presence');
  assert.deepEqual(down.filter(m => m.t === 'presence'), [], 'no presence down the link, joins and leaves included');
  assert.equal(relayHellos(env).length, 1, 'no hello said again');
  for (const db of [a, b, c]) {
    assert.deepEqual(presenceHeard(db), [], `${db.name} hears no presence`);
    assert.deepEqual(db.presence, []);
  }
});

test('presence: a display that wants it, behind displays that do not: the relay says hello again once, asking, and the whole list comes down to that display alone', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const down = tapLink(env);
  const a = env.display('a', { presence: false });
  const b = env.display('b', { presence: false });
  await env.until(online(a, b), 'online');
  const store = env.central.store();
  assert.deepEqual(relayHellos(env).map(h => h.presence), [false]);
  const c = env.display('c');
  await env.until(() => c.status === 'online' && c.presence.length === 3, 'c sees every display');
  assert.deepEqual(idsOf(c.presence), ['a', 'b', 'c']);
  const hellos = relayHellos(env);
  assert.deepEqual(hellos.map(h => h.presence), [false, true], 'one hello said again, asking for presence');
  assert.deepEqual([hellos[1].epoch, typeof hellos[1].since], [store.epoch, 'number'], 'from where the copy stands');
  const lists = down.filter(m => m.t === 'presence' && Array.isArray(m.peers));
  assert.equal(lists.length, 1, 'the whole list, once');
  assert.ok(['r-a', 'r-b'].every(r => lists[0].peers.some(p => p.replicaId === r)), 'the displays already there in it');
  // Another that does not want it: c hears of it, the others hear nothing
  const d = env.display('d', { presence: false });
  await env.until(() => d.status === 'online' && c.presence.length === 4, 'c sees d');
  await quiet(env);
  assert.equal(relayHellos(env).length, 2, 'no more hellos');
  assert.deepEqual(idsOf(store.presence()), ['a', 'b', 'c', 'd']);
  for (const db of [a, b, d]) {
    assert.deepEqual(presenceHeard(db), [], `${db.name} hears no presence`);
    assert.deepEqual(db.presence, []);
  }
});

test('presence: a store with presence off at the server: the relay never asks for it, and a display that wants it hears none', async t => {
  const env = fanLan({ store: { presence: false } });
  t.after(() => env.close());
  const down = tapLink(env);
  const a = env.display('a');
  await env.until(online(a), 'a online');
  const b = env.display('b', { presence: false });
  const c = env.display('c');
  await env.until(online(a, b, c), 'online');
  await quiet(env);
  const admitted = down.filter(m => m.t === 'admitted');
  assert.equal(admitted.length, 3);
  assert.ok(admitted.every(m => m.presence === false), 'the server says the store has none');
  assert.deepEqual(relayHellos(env).map(h => h.presence), [false], 'one hello, not asking; none said again for c');
  assert.deepEqual(down.filter(m => m.t === 'presence'), []);
  for (const db of [a, c]) {
    assert.deepEqual(presenceHeard(db), [], `${db.name} hears no presence`);
    assert.deepEqual(db.presence, []);
  }
});

test('presence: a display turns it on and off while it runs: the relay asks the server once, the list reaches it, and off it hears no more', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const down = tapLink(env);
  const a = env.display('a', { presence: false });
  const b = env.display('b', { presence: false });
  await env.until(online(a, b), 'online');
  assert.deepEqual(relayHellos(env).map(h => h.presence), [false]);
  a.wantPresence(true);
  assert.equal(a.wantsPresence, true);
  await env.until(() => a.presence.length === 2, 'a sees both');
  assert.deepEqual(idsOf(a.presence), ['a', 'b']);
  assert.deepEqual(relayHellos(env).map(h => h.presence), [false, true], 'the relay asked, once');
  const events = [];
  a.on('presence', list => events.push(list));
  a.wantPresence(false);
  assert.deepEqual(a.presence, [], 'emptied at once');
  assert.deepEqual(a.peers, []);
  assert.deepEqual(events, [[]], 'and said so');
  await env.until(() => a.status === 'online', 'a answered again');
  const heardBefore = presenceHeard(a).length;
  const c = env.display('c', { presence: false });
  await env.until(() => c.status === 'online' && env.central.store().presence().length === 3, 'c in');
  await quiet(env);
  assert.ok(down.some(m => m.t === 'presence' && m.joined?.some(p => p.replicaId === 'r-c')), 'the relay\'s session hears presence still (it keeps it while it lives)');
  assert.equal(presenceHeard(a).length, heardBefore, 'but a is sent none of it');
  assert.deepEqual(a.presence, []);
  assert.equal(relayHellos(env).length, 2, 'the relay does not say hello again to turn it off');
});

test('the relay keeps its own displays\' users alone: a client straight on the server is in a display\'s presence, never in the relay\'s saved copy, and not in the list the relay makes offline', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b', { presence: false });
  await env.until(online(a, b), 'online');
  const link = env.central.net.link({ user: { id: 'direct' } });
  const direct = createClient({ connection: createConnection({ transport: link.factory, reconnect: { min: 2, max: 10 }, keepalive: false, wake: false }), store: 'main', initial: INITIAL, replicaId: 'r-direct', now: env.time });
  direct.connect();
  env.displays.push(direct);
  await env.until(() => direct.status === 'online' && a.presence.length === 3, 'a sees the client on the server');
  assert.deepEqual(idsOf(a.presence), ['a', 'b', 'direct']);
  direct.share({ at: 1 });
  await env.until(() => a.peers.find(p => p.replicaId === 'r-direct')?.data?.at === 1, 'and what it shares');
  env.central.store().patch({ tasks: { x: { id: 'x' } } });
  await env.until(() => a.state.tasks.x && b.state.tasks.x, 'the copy written again');
  env.relay.flush();
  const saved = env.storage.load().copies.find(doc => doc.store === 'main');
  assert.deepEqual(saved.users.map(([replicaId]) => replicaId).sort(), ['r-a', 'r-b'], 'the relay\'s own displays alone');
  assert.deepEqual(Object.fromEntries(saved.users.map(([replicaId, { user }]) => [replicaId, user])), { 'r-a': { id: 'a' }, 'r-b': { id: 'b' } });
  // Offline the relay lists its own displays, as the server showed their users
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed && a.presence.length === 2, 'a on the relay\'s own list');
  assert.deepEqual(idsOf(a.presence), ['a', 'b']);
  assert.ok(!a.peers.some(p => p.replicaId === 'r-direct'));
});

test('offline presence from what admitted said: displays the relay\'s session never heard presence for are listed with their users, after a restart of the relay too', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const down = tapLink(env);
  const [a, b, c] = ['a', 'b', 'c'].map(name => env.display(name, { presence: false }));
  await env.until(online(a, b, c), 'online');
  await quiet(env);
  assert.deepEqual(down.filter(m => m.t === 'presence'), [], 'the relay\'s session heard no presence');
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && [a, b, c].every(db => db.relayed), 'answered by the relay');
  a.wantPresence(true);
  await env.until(() => a.presence.length === 3, 'a sees the relay\'s displays');
  assert.deepEqual(idsOf(a.presence), ['a', 'b', 'c']);
  assert.deepEqual(a.peers.map(p => [p.replicaId, p.user?.id]).sort(), [['r-a', 'a'], ['r-b', 'b'], ['r-c', 'c']], 'each under its user, as the server showed it');
  // The relay restarted while the server is still away: its saved copy knows them. (a says hello
  // last, so its list is the whole of it: see the skipped test below for one who comes back after it)
  env.relay.close();
  env.relay = env.makeRelay();
  await env.until(() => env.relay.mode === 'local' && [a, b, c].every(db => db.status === 'online' && db.relayed), 'back on the relay started again');
  a.disconnect();
  a.connect();
  await env.until(() => a.status === 'online' && a.relayed && a.presence.length === 3, 'a sees them from the relay started again');
  assert.deepEqual(a.peers.map(p => [p.replicaId, p.user?.id]).sort(), [['r-a', 'a'], ['r-b', 'b'], ['r-c', 'c']]);
  assert.deepEqual(presenceHeard(b), []);
});

test('the users a copy keeps: a display whose access was taken away is dropped from the saved copy at its next write; one passed through is kept', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b', { presence: false });
  const p = env.display('p', { through: true });
  await env.until(() => online(a, b, p)() && a.presence.length === 3, 'a sees every display');
  const savedUsers = () => {
    env.relay.flush();
    return env.storage.load().copies.find(doc => doc.store === 'main').users.map(([replicaId]) => replicaId).sort();
  };
  assert.deepEqual(savedUsers(), ['r-a', 'r-b', 'r-p']);
  env.central.deny.add('b:main');
  for (const endpoint of env.central.endpoints) await endpoint.revalidate();
  await env.until(() => b.closed?.code === 'forbidden' && a.presence.length === 2, 'b told, and gone from a\'s list');
  env.central.store().patch({ tasks: { x: { id: 'x' } } });
  await env.until(() => a.state.tasks.x && p.state.tasks.x, 'the copy changed');
  assert.deepEqual(savedUsers(), ['r-a', 'r-p'], 'b is no longer the relay\'s: its user goes with the write');
});

test('a client passed through whose presence reaches the relay before its answer does still has its user kept', async t => {
  const env = fanLan();
  t.after(() => env.close());
  let hold = false;
  const down = [];
  env.central.filter = (message, user, via) => {
    if (via === 'link') down.push(message);
    if (hold && via === 'client' && user?.id === 'p' && (message.t === 'snapshot' || message.t === 'delta')) return false;
  };
  const a = env.display('a');
  await env.until(online(a), 'a online');
  hold = true;
  const p = env.display('p', { through: true, presence: false });
  await env.until(() => env.central.withheld.length === 1 && down.some(m => m.t === 'presence' && m.joined?.some(peer => peer.replicaId === 'r-p')), 'p joined, its answer held');
  await quiet(env);
  hold = false;
  env.central.release();
  await env.until(() => online(a, p)() && a.presence.length === 2, 'p answered');
  env.central.store().patch({ tasks: { x: { id: 'x' } } });
  await env.until(() => a.state.tasks.x && p.state.tasks.x, 'the copy changed');
  env.relay.flush();
  const saved = env.storage.load().copies.find(doc => doc.store === 'main');
  assert.deepEqual(saved.users.map(([replicaId]) => replicaId).sort(), ['r-a', 'r-p']);
});

test('offline, a fanned-out display that comes back is listed again for the others', async t => {
  const env = fanLan();
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b', { presence: false });
  await env.until(online(a, b), 'online');
  env.central.goDown();
  await env.until(() => env.relay.mode === 'local' && a.relayed && b.relayed && a.presence.length === 2, 'offline, a sees b');
  b.link.goOffline();
  await env.until(() => b.status === 'offline' && a.presence.length === 1, 'b gone from a\'s list');
  b.link.goOnline();
  b.socket.connection.connect();
  await env.until(() => b.status === 'online' && b.relayed, 'b back on the relay');
  await env.until(() => a.presence.length === 2, 'b on a\'s list again');
  assert.deepEqual(idsOf(a.presence), ['a', 'b']);
});

test('the link lost for less than grace: a display let in again after the relay\'s session has the server\'s list anew is sent that list, not left with the one from before', async t => {
  const env = fanLan({ relay: { grace: 60_000, writeIdle: 60_000 } });
  t.after(() => env.close());
  const a = env.display('a');
  const b = env.display('b');
  await env.until(online(a, b), 'online');
  await env.until(() => [a, b].every(db => db.peers.length === 2), 'each lists both');
  const store = env.central.store();
  // b's word from the server comes last, once a has the relay's new list
  env.central.filter = (message, user, via) => !(via === 'link' && message.t === 'admitted' && message.peer?.replicaId === 'r-b');
  env.central.relayLink.goOffline();
  await env.until(() => env.relay.stats().link.state !== 'open', 'the link is down');
  // Someone comes to the store straight to the server meanwhile: the relay's session is not there to hear it
  const x = store.session({ send: () => {}, user: { id: 'x' } });
  x.receive({ t: 'hello', replicaId: 'r-x', ops: [] });
  env.central.relayLink.goOnline();
  await env.until(() => a.peers.some(p => p.replicaId === 'r-x'), 'a has the new list');
  assert.ok(!b.peers.some(p => p.replicaId === 'r-x'), 'b is not let in yet');
  env.central.filter = null;
  env.central.release();
  const ids = list => list.map(p => p.replicaId).sort();
  await env.until(() => b.peers.some(p => p.replicaId === 'r-x'), 'b let in, and sent the list');
  assert.deepEqual(ids(b.peers), ids(store.peers()));
  x.close();
  await env.until(() => !b.peers.some(p => p.replicaId === 'r-x') && !a.peers.some(p => p.replicaId === 'r-x'), 'x gone from both');
});
