// shared.test.js - One socket per browser: tabs elect a leader that runs the replica, the others follow it
import 'fake-indexeddb/auto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createHub, memoryStorage } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createConnection } from '../src/client/connection.js';
import { sharedConnection } from '../src/client/shared.js';
import { messagePortTransport } from '../src/client/transport.js';
import { portConnection } from '../src/client/port.js';
import { memoryOutbox } from '../src/client/storage.js';
import { indexedDBStorage } from '../src/client/indexeddb.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * The Web Locks API for one process: a queue per name, the holder keeping the
 * lock until its callback settles; query() lists them; forceRelease(name) lets
 * a held lock go the way a closed tab would
 */
function fakeLocks() {
  const queues = new Map();
  return {
    request(name, options, callback) {
      const queue = queues.get(name) ?? [];
      queues.set(name, queue);
      return new Promise(resolve => {
        const entry = {
          done: null,
          run() {
            const forced = new Promise(r => { entry.done = r; });
            Promise.race([Promise.resolve().then(callback), forced]).finally(() => {
              queue.shift();
              resolve();
              queue[0]?.run();
            });
          }
        };
        options?.signal?.addEventListener('abort', () => {
          const at = queue.indexOf(entry);
          if (at > 0) {
            queue.splice(at, 1);
            resolve();
          }
        });
        queue.push(entry);
        if (queue.length === 1) entry.run();
      });
    },
    async query() {
      const live = [...queues.entries()].filter(([, q]) => q.length > 0);
      return { held: live.map(([name]) => ({ name })), pending: live.flatMap(([name, q]) => q.slice(1).map(() => ({ name }))) };
    },
    forceRelease(name) { queues.get(name)?.[0]?.done?.(); }
  };
}

/** A browser: storage per store shared by its tabs, a lock manager, a channel name; `tab()` opens a tab with a client on 'main' */
function browser(net, { user = { id: 'u1', name: 'Ann' }, name = `b${Math.random().toString(36).slice(2)}`, storage, ...shared } = {}) {
  const locks = fakeLocks();
  const persisted = new Map();
  storage ??= store => persisted.get(store) ?? persisted.set(store, memoryOutbox()).get(store);
  const tabs = [];
  const b = {
    name,
    locks,
    persisted,
    tabs,
    tab(tabId, options = {}) {
      const link = net.link({ user });
      const connection = sharedConnection({ name, transport: link.factory, storage, locks, tabId, reconnect: { min: 10, max: 50 }, keepalive: false, ...shared });
      const db = createClient({ connection, store: 'main', initial: INITIAL, replicaId: `tab-${tabId}`, ...options });
      db.connect();
      const t = { id: tabId, link, connection, db, close() { db.dispose(); connection.dispose(); tabs.splice(tabs.indexOf(t), 1); } };
      tabs.push(t);
      return t;
    },
    /** Drain the server network and the channel until `pred` holds */
    async until(pred, label) {
      for (let i = 0; i < 400; i++) {
        await net.settle();
        if (pred()) return;
        await sleep(5);
      }
      throw new Error(`timeout: ${label}`);
    },
    close() { for (const t of [...tabs]) t.close(); }
  };
  return b;
}

test('two tabs are one session, one replica, one peer on the server; edits in either reach the other and the server', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both tabs online');
  assert.notEqual(a.connection.leader, c.connection.leader, 'exactly one leader');
  assert.equal(store.sessions, 1, 'one session for the browser');
  assert.equal(store.peers().length, 1);
  assert.equal(a.db.peers.length, 1, 'a tab sees the browser as one peer');
  assert.deepEqual(a.db.presence, [{ id: 'u1', name: 'Ann' }]);

  a.db.state.tasks.x = { id: 'x', title: 'from a' };
  await b.until(() => c.db.state.tasks.x?.title === 'from a' && store.snapshot().tasks.x?.title === 'from a', 'a\'s edit reached c and the server');
  c.db.state.tasks.x.done = true;
  await b.until(() => a.db.state.tasks.x?.done === true && store.snapshot().tasks.x?.done === true, 'c\'s edit reached a and the server');
  assert.equal(store.replicas.length, 1, 'the server knows one replica for the browser');
  assert.ok(!store.replicas.includes('tab-a') && !store.replicas.includes('tab-c'), 'and it is not a tab\'s');
  assert.equal(a.db.pending + c.db.pending, 0);

  // Each tab's undo is its own: a takes back its record, c keeps its own to take back
  c.db.state.tasks.y = { id: 'y', title: 'from c' };
  await b.until(() => a.db.state.tasks.y?.title === 'from c', 'c\'s record reached a');
  a.db.undo();
  await b.until(() => a.db.state.tasks.x === undefined && c.db.state.tasks.x === undefined, 'a undid its record write, in both tabs');
  assert.equal(c.db.state.tasks.y.title, 'from c', 'c\'s record stands');
  assert.equal(c.db.canUndo, true);
  c.db.undo();
  await b.until(() => a.db.state.tasks.y === undefined && store.snapshot().tasks.y === undefined, 'c undid its own, everywhere');
});

test('offline, every tab reports the browser\'s status and pending, while edits still reach the replica and each other', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const leader = a.connection.leader ? a : c;
  const follower = leader === a ? c : a;
  const statuses = [];
  const trace = [];
  follower.db.on('status', s => { statuses.push(s); trace.push(`${s} (link ${follower.connection.status}, socket ${follower.connection.upstream}, leader's socket ${leader.connection.upstream})`); });

  leader.link.goOffline();
  await b.until(() => leader.db.status === 'offline' && follower.db.status === 'offline', 'both tabs offline with the socket');
  assert.equal(follower.connection.upstream, 'offline');
  assert.deepEqual(follower.db.peers, [], 'nobody to see while offline');
  follower.db.state.tasks.x = { id: 'x', title: 'typed offline in the follower' };
  leader.db.state.tasks.y = { id: 'y', title: 'typed offline in the leader' };
  await b.until(() => leader.db.state.tasks.x?.title && follower.db.state.tasks.y?.title, 'the tabs still see each other\'s edits, through the replica');
  assert.equal(store.snapshot().tasks.x, undefined, 'the server has neither yet');
  assert.equal(follower.db.pending, 2, 'the replica holds both, and every tab counts them');
  assert.equal(leader.db.pending, 2);
  assert.equal(b.persisted.get('main').load().ops.length, 2, 'persisted in the browser\'s outbox at once');

  leader.link.goOnline();
  await b.until(() => store.snapshot().tasks.x && store.snapshot().tasks.y && follower.db.pending === 0, 'both landed when the socket came back');
  assert.equal(follower.db.status, 'online');
  // Each of the socket's retries shows as connecting in between, as it would on any client; what stands is offline, then online
  const settled = statuses.filter((s, i) => s !== 'connecting' && s !== statuses[i - 1]);
  assert.deepEqual(settled, ['offline', 'online'], `the socket's way back, as the tab saw it:\n${trace.join('\n')}`);
  assert.ok(statuses.includes('connecting'), 'and the socket connecting showed');
});

test('when the leader tab closes, the next takes over from the persisted outbox: same replica, held edits delivered, followers back', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const leader = a.connection.leader ? a : c;
  const follower = leader === a ? c : a;
  leader.db.state.tasks.x = { id: 'x', title: 'before' };
  await b.until(() => store.snapshot().tasks.x?.title === 'before', 'an edit, so the server knows the replica');
  const [replica] = store.replicas;
  assert.equal(b.persisted.get('main').load().replicaId, replica, 'the replica id is persisted for whoever leads next');

  leader.link.goOffline();
  await b.until(() => follower.db.status === 'offline', 'offline');
  follower.db.state.tasks.y = { id: 'y', title: 'while down' };
  await b.until(() => follower.db.pending === 1, 'held in the replica');
  assert.equal(store.snapshot().tasks.y, undefined, 'not on the server yet');

  // The leader tab closes while still offline: the follower becomes the leader and loads the replica, held edit included
  leader.close();
  await b.until(() => follower.connection.leader, 'the follower leads');
  await b.until(() => follower.db.status === 'online' && store.snapshot().tasks.y?.title === 'while down', 'the held edit landed');
  assert.deepEqual(store.replicas, [replica], 'the same replica id continued');
  assert.equal(store.sessions, 1);
  assert.equal(follower.db.pending, 0);

  const d = b.tab('d');
  await b.until(() => d.db.status === 'online' && d.db.state.tasks.y?.title === 'while down', 'a new tab follows the new leader');
  assert.equal(d.connection.leader, false);
  assert.equal(store.sessions, 1);
});

test('an edit made offline lives in the browser\'s replica: the tab that made it can close, and the next leader delivers it', async t => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  await b.until(() => a.db.status === 'online', 'online');
  assert.equal(a.connection.leader, true, 'alone, a tab leads');
  const saved = b.persisted.get('main').load();
  assert.ok(saved && saved.replicaId, 'the replica wrote its identity at once, before any edit');

  a.link.goOffline();
  await b.until(() => a.db.status === 'offline', 'offline');
  a.db.state.tasks.z = { id: 'z', title: 'typed offline' };
  await b.until(() => a.db.pending === 1 && b.persisted.get('main').load().ops.length === 1, 'in the persisted outbox');

  const c = b.tab('c');
  a.close();
  await b.until(() => c.connection.leader && c.db.status === 'online', 'c leads and is online');
  await b.until(() => store.snapshot().tasks.z?.title === 'typed offline', 'the edit the closed tab made arrived');
  assert.deepEqual(store.replicas, [saved.replicaId], 'under the identity the first leader wrote');
  assert.equal(c.db.state.tasks.z.title, 'typed offline');
});

test('the replica may live in IndexedDB: the tabs\' messages wait while it opens, and a handoff reloads it', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const name = `idb-${Math.random().toString(36).slice(2)}`;
  const b = browser(net, { storage: s => indexedDBStorage(`${name}-${s}`) });
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  assert.equal(store.sessions, 1);
  a.db.state.tasks.x = { id: 'x', title: 'in indexeddb' };
  await b.until(() => c.db.state.tasks.x?.title === 'in indexeddb' && store.snapshot().tasks.x, 'synced');
  const [replica] = store.replicas;

  const leader = a.connection.leader ? a : c;
  const follower = leader === a ? c : a;
  leader.link.goOffline();
  await b.until(() => follower.db.status === 'offline', 'offline');
  follower.db.state.tasks.y = { id: 'y', title: 'held in indexeddb' };
  await b.until(() => follower.db.pending === 1, 'held');
  await sleep(50);   // the replica's IndexedDB writes land
  leader.close();
  await b.until(() => follower.connection.leader && follower.db.status === 'online' && store.snapshot().tasks.y?.title === 'held in indexeddb', 'the next leader opened the replica from IndexedDB and delivered');
  assert.deepEqual(store.replicas, [replica], 'the same replica');
  assert.equal(follower.db.state.tasks.x.title, 'in indexeddb');
});

test('eviction and a closed socket reach every tab; a tab may opt out of presence; shares are the browser\'s', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c', { presence: false });
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  assert.deepEqual(c.db.peers, [], 'opted out');
  assert.equal(a.db.peers.length, 1);

  c.db.share({ editing: 'x' });
  await b.until(() => store.peers()[0]?.data?.editing === 'x', 'a tab\'s share is the browser\'s');
  a.db.share({ editing: 'y' });
  await b.until(() => store.peers()[0]?.data?.editing === 'y', 'the last tab to share wins');

  const closed = [];
  a.db.on('closed', info => closed.push(['a', info.code]));
  c.db.on('closed', info => closed.push(['c', info.code]));
  assert.equal(store.closeSessions(() => true, 'bye'), 1);
  await b.until(() => closed.length === 2, 'both tabs hear the eviction');
  assert.deepEqual(closed.sort(), [['a', 'evicted'], ['c', 'evicted']]);
});

test('two browsers are two replicas and two peers; without locks or a channel a tab is its own leader', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const one = browser(net, { user: { id: 'u1' } });
  const two = browser(net, { user: { id: 'u2' } });
  t.after(() => { one.close(); two.close(); });
  const a = one.tab('a');
  const a2 = one.tab('a2');
  const z = two.tab('z');
  await one.until(() => [a, a2, z].every(tab => tab.db.status === 'online'), 'all online');
  assert.equal(store.sessions, 2);
  assert.equal(store.peers().length, 2);
  a2.db.state.tasks.p = { id: 'p', title: 'from browser one' };
  z.db.state.tasks.r = { id: 'r', title: 'from browser two' };
  await one.until(() => store.snapshot().tasks.p && store.snapshot().tasks.r && a.db.state.tasks.r, 'both edits landed and crossed');
  assert.equal(store.replicas.length, 2, 'one replica per browser');

  const link = net.link({ user: { id: 'u3' } });
  const alone = sharedConnection({ name: 'alone', transport: link.factory, locks: null, channel: null, reconnect: { min: 10, max: 50 }, keepalive: false });
  assert.equal(alone.leader, true);
  const db = createClient({ connection: alone, store: 'main', initial: INITIAL });
  db.connect();
  await one.until(() => db.status === 'online', 'alone online');
  db.state.tasks.q = { id: 'q' };
  await one.until(() => store.snapshot().tasks.q !== undefined, 'syncs like any client');
  assert.equal(store.sessions, 3);
  db.dispose();
  alone.dispose();
  assert.throws(() => sharedConnection({ transport: link.factory }), /name/);
});

test('a store no tab has open anymore is let go after a moment, its session with it; a reload comes back in time; a tab that dies without a word is found by its lock', async t => {
  const stores = createStores(id => createStore({ initial: INITIAL, presence: true }));
  const net = createNetwork({ session: ({ send, user }) => createHub(id => stores.get(id), { send, user }) });
  const b = browser(net, { linger: 60, sweepEvery: 20 });
  t.after(() => b.close());
  const a = b.tab('a', { store: 'team-1' });
  const c = b.tab('c', { store: 'team-2' });
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  assert.equal(a.connection.leader, true, 'the first tab leads');
  assert.deepEqual([stores.get('team-1').sessions, stores.get('team-2').sessions], [1, 1], 'one session per store, over one socket');
  const closes = [];
  stores.get('team-2').observe('session', e => { if (e.event === 'close') closes.push(e); });

  // A reload: the tab goes and comes back within the linger; the replica never left
  c.close();
  const c2 = b.tab('c2', { store: 'team-2' });
  await b.until(() => c2.db.status === 'online', 'back');
  await sleep(100);
  await net.settle();
  assert.deepEqual(closes, [], 'the server never saw the store close');
  assert.equal(stores.get('team-2').sessions, 1);

  // Gone for good: after the linger the replica lets the store go, and only that store
  c2.close();
  await b.until(() => stores.get('team-2').sessions === 0, 'let go');
  assert.equal(stores.get('team-1').sessions, 1, 'the leader\'s own store stays');
  assert.equal(closes.length, 1);

  // A tab that dies without saying goodbye: its lock goes with it, and the sweep finds it
  const d = b.tab('d', { store: 'team-2' });
  await b.until(() => d.db.status === 'online' && stores.get('team-2').sessions === 1, 'a tab on team-2 again');
  b.locks.forceRelease(`lazy-storage:${b.name}:tab:d`);
  await b.until(() => stores.get('team-2').sessions === 0, 'swept, then let go');
  assert.equal(a.db.status, 'online', 'the leader is untouched');
  assert.equal(b.persisted.has('team-2'), true, 'the replica\'s storage for the store stays');
});

test('a page on a MessagePort follows the replica for the stores its host allows, in a session of its own, through a change of leader', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const c = b.tab('c');   // first in, so it leads
  const a = b.tab('a');   // follows, and hosts the page
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  assert.equal(c.connection.leader, true);

  const { port1, port2 } = new MessageChannel();
  const stop = a.connection.follow(port1, [{ store: 'main', initial: INITIAL }]);
  const link = portConnection(port2, { reconnect: { min: 10, max: 50 } });
  const page = createClient({ connection: link, store: 'main', initial: INITIAL, replicaId: 'page-1' });
  const other = createClient({ connection: link, store: 'other', initial: {}, replicaId: 'page-1' });
  page.connect();
  other.connect();
  await b.until(() => page.status === 'online', 'the page is online through the port, via the leader in the other tab');
  assert.equal(store.sessions, 1, 'still the browser\'s one session on the server');

  page.state.tasks.p = { title: 'from the page' };
  await b.until(() => store.snapshot().tasks.p?.title === 'from the page', 'its edit reached the server');
  await b.until(() => c.db.state.tasks.p?.title === 'from the page' && a.db.state.tasks.p?.title === 'from the page', 'and both tabs');
  c.db.state.tasks.q = { title: 'from a tab' };
  await b.until(() => page.state.tasks.q?.title === 'from a tab', 'a tab\'s edit reached the page');
  await sleep(60);
  assert.notEqual(other.status, 'online', 'a store the host did not allow never answers');

  // The page's status is the browser's socket's, and its pending the replica's, as the host says
  c.link.goOffline();
  await b.until(() => page.status === 'offline' && a.db.status === 'offline', 'offline in the page as in the tabs');
  page.state.tasks.held = { title: 'while offline' };
  await b.until(() => page.pending === 1 && a.db.pending === 1, 'the edit waits in the browser\'s outbox, counted in the page');
  c.link.goOnline();
  await b.until(() => page.status === 'online' && page.pending === 0, 'online again, the edit sent');
  assert.equal(store.snapshot().tasks.held?.title, 'while offline');

  // The leader goes; the host tab takes over, and the page says hello again to the relay there
  c.close();
  await b.until(() => a.connection.leader, 'the host tab leads');
  await b.until(() => page.status === 'online', 'the page is back');
  page.state.tasks.r = { title: 'after the change' };
  await b.until(() => store.snapshot().tasks.r?.title === 'after the change', 'its edits still reach the server');
  a.db.state.tasks.s = { title: 'from the host' };
  await b.until(() => page.state.tasks.s?.title === 'from the host', 'and the host\'s reach it');

  // Ended by the host: what the page says goes nowhere
  stop();
  page.state.tasks.t = { title: 'too late' };
  await sleep(60);
  await net.settle();
  assert.equal(store.snapshot().tasks.t, undefined);
  page.dispose();
  other.dispose();
  link.close();
  port1.close();
  port2.close();
  // A plain connection on the port works too, without the socket's status
  const spare = new MessageChannel();
  const plain = createConnection({ transport: messagePortTransport(spare.port2), keepalive: false });
  assert.equal(plain.upstream, undefined);
  spare.port1.close();
  spare.port2.close();
});

test('a key deleted inside a register by a tab, or by a page on a port, goes from the replica, the server and the other tab', async t => {
  const store = createStore({ initial: { ...INITIAL, profile: {} }, registers: ['profile'], presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a', { registers: ['profile'] });
  const c = b.tab('c', { registers: ['profile'] });
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  a.db.state.profile = { name: 'Ann', nick: 'annie' };
  await b.until(() => c.db.state.profile?.nick === 'annie', 'the register reached the other tab');
  delete a.db.state.profile.nick;
  await b.until(() => store.snapshot().profile?.nick === undefined && c.db.state.profile?.nick === undefined, 'and its deletion, on the server and in the other tab');
  assert.deepEqual(store.snapshot().profile, { name: 'Ann' });

  const { port1, port2 } = new MessageChannel();
  const stop = a.connection.follow(port1, [{ store: 'main', initial: INITIAL, registers: ['profile'] }]);
  const page = createClient({ connection: portConnection(port2, { reconnect: { min: 10, max: 50 } }), store: 'main', initial: INITIAL, registers: ['profile'], replicaId: 'page-2' });
  page.connect();
  await b.until(() => page.status === 'online' && page.state.profile?.name === 'Ann', 'the page has the register');
  page.state.profile.nick = 'A';
  await b.until(() => c.db.state.profile?.nick === 'A', 'its write reached the other tab');
  delete page.state.profile.nick;
  await b.until(() => store.snapshot().profile?.nick === undefined && c.db.state.profile?.nick === undefined && a.db.state.profile?.nick === undefined, 'and its deletion, everywhere');
  stop();
  page.dispose();
  port1.close();
  port2.close();
});

test('connect() from any tab prods the browser\'s socket, so a tab looked at again reconnects at once rather than at the next backoff step', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net, { reconnect: { min: 5000, max: 5000 } });   // left to itself, the socket would wait five seconds
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const leader = a.connection.leader ? a : c;
  const follower = leader === a ? c : a;
  leader.link.goOffline();
  await b.until(() => follower.db.status === 'offline', 'offline');
  leader.link.goOnline();
  await sleep(60);
  await net.settle();
  assert.equal(follower.db.status, 'offline', 'nothing happened on its own yet');
  follower.connection.connect();
  await b.until(() => follower.db.status === 'online' && leader.db.status === 'online', 'the follower\'s nudge brought the socket back');
  assert.equal(store.sessions, 1);
});

test('a lock manager that refuses the request leaves the tab leading on its own, rather than waiting for a leader that never comes', async t => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const refusing = { request: () => Promise.reject(new Error('locks are not available here')), query: async () => ({ held: [], pending: [] }) };
  const link = net.link({ user: { id: 'u1' } });
  const connection = sharedConnection({ name: 'refused', transport: link.factory, locks: refusing, reconnect: { min: 10, max: 50 }, keepalive: false });
  const db = createClient({ connection, store: 'main', initial: INITIAL });
  t.after(() => { db.dispose(); connection.dispose(); });
  db.connect();
  for (let i = 0; i < 200 && db.status !== 'online'; i++) {
    await net.settle();
    await sleep(5);
  }
  assert.equal(db.status, 'online');
  assert.equal(connection.leader, true);
  db.state.tasks.x = { id: 'x' };
  for (let i = 0; i < 200 && !store.snapshot().tasks.x; i++) {
    await net.settle();
    await sleep(5);
  }
  assert.ok(store.snapshot().tasks.x, 'and syncs');
});

test('the socket turned away reaches every tab once, and a tab that signs in again from inside the event brings the browser back', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const leader = a.connection.leader ? a : c;
  const closed = [];
  // Every tab has credentials by then and comes straight back, from inside the event
  for (const tab of [a, c]) tab.db.on('closed', info => { closed.push([tab.id, info.code, tab.db.status]); tab.db.connect(); });
  // What the server sends before closing with 4401 (server/wire.js)
  leader.link.current.onmessage({ t: 'closed', code: 'unauthorized', message: 'Unauthorized' });
  await b.until(() => closed.length === 2 && a.db.status === 'online' && c.db.status === 'online', 'both back');
  await sleep(20);
  await net.settle();
  assert.deepEqual(closed.sort(), [['a', 'unauthorized', 'offline'], ['c', 'unauthorized', 'offline']], 'each tab heard it once, offline');
  assert.equal(store.sessions, 1, 'one socket');
  assert.equal(a.db.closed, null);
  assert.equal(c.db.closed, null);
});

test('storage that cannot be opened leaves no tab stranded: the replica runs from memory and every tab is told', async t => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const failures = [];
  const broken = () => ({
    load: () => Promise.reject(new Error('quota exceeded')),
    commit() {}, replace() {}, saveOp() {}, removeOp() {}, dropOps() {}
  });
  const b = browser(net, { storage: broken, onError: err => failures.push(err.message) });
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  const heard = { a: [], c: [] };
  a.db.on('error', err => heard.a.push(err.code));
  c.db.on('error', err => heard.c.push(err.code));
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both tabs online, not stuck connecting');
  assert.deepEqual(failures, ['quota exceeded']);
  a.db.state.tasks.x = { id: 'x', title: 'kept in memory' };
  await b.until(() => c.db.state.tasks.x && store.snapshot().tasks.x, 'edits still sync');
  assert.ok(heard.a.includes('storage-unavailable') || heard.c.includes('storage-unavailable'), 'the tabs hear why');
});

test('a follower\'s op is acknowledged once the replica has stored it, not before', async t => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  let release;
  let gate = Promise.resolve();
  const slow = () => {
    const inner = memoryOutbox();
    return { ...inner, settled: () => gate };   // storage whose writes land when the gate opens
  };
  const posted = [];
  const channel = name => {
    const ch = new BroadcastChannel(name);
    const post = ch.postMessage.bind(ch);
    ch.postMessage = message => { posted.push(message); post(message); };
    return ch;
  };
  const b = browser(net, { storage: slow, channel });
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const follower = a.connection.leader ? c : a;
  const acks = () => posted.filter(m => m.kind === 'down' && m.to === follower.id && m.message?.t === 'ack').length;

  gate = new Promise(resolve => { release = resolve; });
  const before = acks();
  follower.db.state.tasks.y = { id: 'y' };
  await b.until(() => store.snapshot().tasks.y, 'the op reached the replica and the server');
  await sleep(20);
  assert.equal(acks(), before, 'no acknowledgement while the replica\'s write is in flight');
  release();
  await b.until(() => acks() === before + 1, 'acknowledged once it landed');
});

test('what the browser\'s replica had refused, or lost, reaches every tab', async t => {
  const store = createStore({ initial: INITIAL, readOnly: ['tasks/*/locked'] });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const heard = { a: [], c: [] };
  a.db.on('rejected', r => heard.a.push(r.code));
  c.db.on('rejected', r => heard.c.push(r.code));
  a.db.state.tasks.x = { id: 'x', locked: true };
  await b.until(() => heard.a.length && heard.c.length, 'both tabs heard the refusal');
  assert.deepEqual([heard.a, heard.c], [['forbidden'], ['forbidden']]);
});

test('a store put back from a copy reaches every tab as one reset: the replica tells, by the epoch the tabs never see', async t => {
  const stores = createStores(() => createStore({ initial: INITIAL, storage: memoryStorage() }));
  const net = createNetwork({ session: ({ send, user }) => createHub(id => stores.get(id), { send, user }) });
  const b = browser(net);
  t.after(() => { b.close(); stores.dispose(); });
  const a = b.tab('a');
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online', 'both online');
  const heard = { a: [], c: [] };
  a.db.on('reset', r => heard.a.push(r));
  c.db.on('reset', r => heard.c.push(r));
  a.db.state.tasks.x = { id: 'x' };
  await b.until(() => stores.get('main').snapshot().tasks.x, 'x on the server');
  const copy = stores.get('main').export();
  c.db.state.tasks.y = { id: 'y' };
  await b.until(() => stores.get('main').snapshot().tasks.y, 'y on the server');

  stores.restore('main', copy);
  await b.until(() => heard.a.length && heard.c.length && a.db.state.tasks.y === undefined && c.db.state.tasks.y === undefined, 'both tabs heard, and hold the copy');
  assert.deepEqual([heard.a.length, heard.c.length], [1, 1]);
  assert.deepEqual(heard.c[0].previous.state, { tasks: { x: { id: 'x' }, y: { id: 'y' } } });
  assert.equal(heard.a[0].epoch, stores.get('main').epoch);
});

/**
 * A network whose server end is watched: every message a socket's hub took
 * in ('up') or sent ('down'), with the socket's user, the socket's number
 * and its place on the wire (`at`), in the order they passed. `said` and
 * `heard` pick a user's messages of a type on a store ('main' unless named)
 */
function watchedNetwork(storeOf) {
  const wire = [];
  let sockets = 0;
  const net = createNetwork({
    session: ({ send, user }) => {
      const socket = ++sockets;
      const note = (way, message) => wire.push({ way, socket, user: user?.id, at: wire.length, message: structuredClone(message) });
      const hub = createHub(storeOf, { send: message => { note('down', message); send(message); }, user });
      return {
        receive(message) { note('up', message); hub.receive(message); },
        close: () => hub.close()
      };
    }
  });
  const pick = way => (user, type, store = 'main') => wire.filter(w => w.way === way && w.user === user && w.message.t === type && w.message.store === store);
  return { net, wire, said: pick('up'), heard: pick('down'), get sockets() { return sockets; } };
}

/** The users a list names, by id, in order: 'u1,u2' */
const ids = users => users.map(u => u.id).sort().join();
/** The whole lists among presence messages (the rest are changes) */
const wholeLists = heard => heard.filter(h => Array.isArray(h.message.peers));

test('a tab that wants no presence: the browser\'s replica says so from its very first hello, the server sends its socket none, and still lists the browser', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const { net, said, heard } = watchedNetwork(() => store);
  const z = net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const b = browser(net);
  t.after(() => { b.close(); z.dispose(); });
  const a = b.tab('a', { presence: false });
  await b.until(() => a.db.status === 'online' && z.status === 'online' && ids(z.presence) === 'u1,u2', 'the tab online, and the other user sees the browser');
  assert.equal(a.connection.leader, true);
  const hellos = said('u1', 'hello');
  assert.equal(hellos.length, 1, 'one hello from the browser');
  assert.equal(hellos[0].message.presence, false, 'the replica\'s first hello already says no');
  assert.deepEqual(heard('u1', 'presence'), [], 'no list sent, and no change since');
  assert.deepEqual(store.peers().map(p => p.user.id).sort(), ['u1', 'u2'], 'the server still lists the browser');
  assert.equal(store.sessions, 2);
  assert.deepEqual(a.db.presence, []);
  assert.deepEqual(a.db.peers, []);
  assert.equal(a.db.wantsPresence, false);

  // Others come and share: the other user hears all of it, the browser's socket none
  z.share({ editing: 'x' });
  const y = net.client({ replicaId: 'y', initial: INITIAL }, { user: { id: 'u3', name: 'Cy' } });
  t.after(() => y.dispose());
  await b.until(() => ids(z.presence) === 'u1,u2,u3' && z.peers.some(p => p.data?.editing === 'x'), 'the other user heard the newcomer and its own share');
  await net.settle();
  assert.deepEqual(heard('u1', 'presence'), [], 'none of it reached the browser');
  assert.deepEqual(a.db.presence, []);
});

test('a second tab that wants presence turns it on for the browser\'s replica: the server sends it the whole list, that tab sees everyone, the other still nobody', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const { net, said, heard } = watchedNetwork(() => store);
  const z = net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const b = browser(net);
  t.after(() => { b.close(); z.dispose(); });
  const a = b.tab('a', { presence: false });
  await b.until(() => a.db.status === 'online' && z.status === 'online' && ids(z.presence) === 'u1,u2', 'online');
  const quiet = [];
  a.db.on('peers', peers => quiet.push(['peers', peers]));
  a.db.on('presence', users => quiet.push(['presence', users]));
  assert.deepEqual(heard('u1', 'presence'), []);

  const c = b.tab('c');
  await b.until(() => c.db.status === 'online' && ids(c.db.presence) === 'u1,u2', 'the new tab sees both users');
  assert.equal(c.connection.leader, false, 'it follows');
  const hellos = said('u1', 'hello');
  assert.equal(hellos.length, 2, 'the replica said hello again');
  assert.equal(hellos[0].message.presence, false);
  assert.notEqual(hellos[1].message.presence, false, 'wanting presence now');
  const lists = wholeLists(heard('u1', 'presence'));
  assert.equal(lists.length, 1, 'the server sent the whole list, as to a newcomer');
  assert.ok(lists[0].at > hellos[1].at, 'in answer to that hello');
  assert.deepEqual(lists[0].message.peers.map(p => p.user.id).sort(), ['u1', 'u2']);
  assert.equal(c.db.peers.length, 2);
  assert.deepEqual(a.db.presence, [], 'the tab that wants none has none');
  assert.deepEqual(a.db.peers, []);

  // Changes reach the replica now, and the tab that wants them
  z.share({ editing: 'x' });
  await b.until(() => c.db.peers.some(p => p.data?.editing === 'x'), 'the other user\'s share reached the tab that wants presence');
  assert.ok(heard('u1', 'presence').some(h => h.message.shared), 'through the browser\'s socket');
  await net.settle();
  assert.deepEqual(a.db.peers, []);
  assert.deepEqual(quiet, [], 'the tab that wants none heard nothing of it');
  assert.equal(store.sessions, 2, 'still one session for the browser');
  assert.deepEqual(store.peers().map(p => p.user.id).sort(), ['u1', 'u2']);
});

test('when the last tab that wants presence leaves, the browser\'s replica stops hearing it, and says so again after a reconnect; the browser stays listed', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const w = watchedNetwork(() => store);
  const z = w.net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const b = browser(w.net, { sweepEvery: 20 });
  t.after(() => { b.close(); z.dispose(); });
  const a = b.tab('a', { presence: false });
  const c = b.tab('c');
  await b.until(() => a.db.status === 'online' && c.db.status === 'online' && ids(c.db.presence) === 'u1,u2', 'the tab that wants presence sees both users');
  assert.equal(a.connection.leader, true);
  const before = w.wire.length;

  c.close();
  await b.until(() => w.said('u1', 'hello').some(h => h.at >= before && h.message.presence === false), 'the replica said hello again, wanting none');
  const off = w.said('u1', 'hello').find(h => h.at >= before && h.message.presence === false).at;
  z.share({ editing: 'x' });
  const y = w.net.client({ replicaId: 'y', initial: INITIAL }, { user: { id: 'u3', name: 'Cy' } });
  t.after(() => y.dispose());
  await b.until(() => ids(z.presence) === 'u1,u2,u3' && z.peers.some(p => p.data?.editing === 'x'), 'the other user heard the newcomer and its own share');
  await w.net.settle();
  assert.deepEqual(w.heard('u1', 'presence').filter(h => h.at > off), [], 'the browser\'s socket heard none of it');
  assert.deepEqual(a.db.presence, []);
  assert.deepEqual(store.peers().map(p => p.user.id).sort(), ['u1', 'u2', 'u3'], 'the browser is still listed');

  // The socket drops and comes back: the replica's hello on the new one still says no
  const sockets = w.sockets;
  a.link.goOffline();
  await b.until(() => a.db.status === 'offline', 'offline');
  a.link.goOnline();
  await b.until(() => a.db.status === 'online' && w.said('u1', 'hello').some(h => h.socket > sockets), 'back, and the replica said hello');
  await b.until(() => ids(z.presence) === 'u1,u2,u3', 'listed again');
  await w.net.settle();
  const again = w.said('u1', 'hello').filter(h => h.socket > sockets);
  assert.ok(again.every(h => h.message.presence === false), 'every hello on the new socket says no');
  assert.deepEqual(w.heard('u1', 'presence').filter(h => h.socket > sockets), [], 'and the server sent it no list');
});

test('a replica made for a tab that wants no presence says so in its very first hello: after a handoff, opened from IndexedDB, the server sends the new socket no list', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const w = watchedNetwork(() => store);
  const z = w.net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const name = `idb-${Math.random().toString(36).slice(2)}`;
  const b = browser(w.net, { storage: s => indexedDBStorage(`${name}-${s}`) });
  t.after(() => { b.close(); z.dispose(); });
  const a = b.tab('a');   // leads, and wants presence
  const c = b.tab('c', { presence: false });
  await b.until(() => a.db.status === 'online' && c.db.status === 'online' && ids(a.db.presence) === 'u1,u2', 'both online, the leader seeing both users');
  assert.equal(a.connection.leader, true);
  assert.equal(wholeLists(w.heard('u1', 'presence')).length, 1, 'the first replica heard the list, for the leader\'s own tab');
  const first = w.sockets;

  a.close();
  await b.until(() => c.connection.leader && c.db.status === 'online', 'the tab that wants none leads');
  await b.until(() => w.said('u1', 'hello').some(h => h.socket > first) && ids(z.presence) === 'u1,u2', 'the new replica said hello, and the browser is listed again');
  await w.net.settle();
  const hellos = w.said('u1', 'hello').filter(h => h.socket > first);
  assert.equal(hellos[0].message.presence, false, 'its first hello already says no');
  assert.ok(hellos.every(h => h.message.presence === false));
  assert.deepEqual(w.heard('u1', 'presence').filter(h => h.socket > first), [], 'no list first, nor anything since');
  assert.deepEqual(c.db.presence, []);
  assert.deepEqual(store.peers().map(p => p.user.id).sort(), ['u1', 'u2']);
});

test('a page on a port that wants no presence counts as a tab that wants none: the replica it makes says no, a page that wants it turns it on, and it goes off when that page is let go', async t => {
  const stores = createStores(() => createStore({ initial: INITIAL, presence: true }));
  const w = watchedNetwork(id => stores.get(id));
  const z = w.net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const b = browser(w.net);
  t.after(() => { b.close(); z.dispose(); stores.dispose(); });
  const c = b.tab('c', { store: 'side' });   // leads, with nothing on 'main'
  const a = b.tab('a', { store: 'side' });   // follows, and hosts the pages
  await b.until(() => c.db.status === 'online' && a.db.status === 'online' && z.status === 'online', 'online');
  assert.equal(c.connection.leader, true);
  assert.deepEqual(w.said('u1', 'hello'), [], 'the browser has nothing on main yet');

  const page = (replicaId, options = {}) => {
    const { port1, port2 } = new MessageChannel();
    const stop = a.connection.follow(port1, [{ store: 'main', initial: INITIAL }]);
    const link = portConnection(port2, { reconnect: { min: 10, max: 50 } });
    const db = createClient({ connection: link, store: 'main', initial: INITIAL, replicaId, ...options });
    db.connect();
    let closed = false;
    return { db, close() { if (closed) return; closed = true; stop(); db.dispose(); link.close(); port1.close(); port2.close(); } };
  };
  const quiet = page('page-1', { presence: false });
  t.after(() => quiet.close());
  await b.until(() => quiet.db.status === 'online' && ids(z.presence) === 'u1,u2', 'the page is online, and the browser listed on main');
  const hellos = w.said('u1', 'hello');
  assert.equal(hellos[0].message.presence, false, 'the replica the page made says no from its first hello');
  assert.deepEqual(w.heard('u1', 'presence'), []);
  assert.deepEqual(quiet.db.presence, []);

  const keen = page('page-2');
  t.after(() => keen.close());
  await b.until(() => ids(keen.db.presence) === 'u1,u2', 'the page that wants presence sees both users');
  const lists = wholeLists(w.heard('u1', 'presence'));
  assert.equal(lists.length, 1, 'the server sent the replica the whole list');
  assert.notEqual(w.said('u1', 'hello').at(-1).message.presence, false);
  assert.deepEqual(quiet.db.presence, [], 'the page that wants none has none');
  assert.deepEqual(quiet.db.peers, []);

  // The host lets the keen page go: its session leaves the replica, which wants none again
  const before = w.wire.length;
  keen.close();
  await b.until(() => w.said('u1', 'hello').some(h => h.at >= before && h.message.presence === false), 'the replica said hello again, wanting none');
  const off = w.said('u1', 'hello').find(h => h.at >= before && h.message.presence === false).at;
  z.share({ editing: 'x' });
  await b.until(() => z.peers.some(p => p.data?.editing === 'x'), 'the other user heard its own share');
  await w.net.settle();
  assert.deepEqual(w.heard('u1', 'presence').filter(h => h.at > off), [], 'the browser\'s socket heard none of it');
  assert.deepEqual(quiet.db.presence, []);
  assert.deepEqual(stores.get('main').peers().map(p => p.user.id).sort(), ['u1', 'u2'], 'the browser is still listed');
  quiet.db.state.tasks.q = { id: 'q' };
  await b.until(() => stores.get('main').snapshot().tasks.q, 'and the quiet page\'s edits still go up');
});

test('a tab that changes its mind with wantPresence changes the replica\'s: off, the server stops sending the browser presence; on, it sends the whole list again', async t => {
  const store = createStore({ initial: INITIAL, presence: true });
  const w = watchedNetwork(() => store);
  const z = w.net.client({ replicaId: 'z', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  const b = browser(w.net);
  t.after(() => { b.close(); z.dispose(); });
  const a = b.tab('a');
  await b.until(() => a.db.status === 'online' && ids(a.db.presence) === 'u1,u2', 'the tab sees both users');
  assert.equal(wholeLists(w.heard('u1', 'presence')).length, 1);

  let before = w.wire.length;
  a.db.wantPresence(false);
  assert.equal(a.db.wantsPresence, false);
  assert.deepEqual(a.db.presence, [], 'off at once');
  assert.deepEqual(a.db.peers, []);
  await b.until(() => w.said('u1', 'hello').some(h => h.at >= before && h.message.presence === false), 'the replica said hello again, wanting none');
  const off = w.said('u1', 'hello').find(h => h.at >= before && h.message.presence === false).at;
  z.share({ editing: 'x' });
  await b.until(() => z.peers.some(p => p.data?.editing === 'x'), 'the other user heard its own share');
  await w.net.settle();
  assert.deepEqual(w.heard('u1', 'presence').filter(h => h.at > off), [], 'the browser\'s socket heard none of it');
  assert.deepEqual(a.db.presence, []);

  before = w.wire.length;
  a.db.wantPresence(true);
  await b.until(() => ids(a.db.presence) === 'u1,u2' && a.db.peers.some(p => p.data?.editing === 'x'), 'the tab sees both users again, the share included');
  assert.equal(a.db.wantsPresence, true);
  const hello = w.said('u1', 'hello').find(h => h.at >= before);
  assert.notEqual(hello.message.presence, false, 'the replica asked again');
  assert.equal(wholeLists(w.heard('u1', 'presence').filter(h => h.at >= before)).length, 1, 'and the server sent the whole list once');
  assert.equal(store.sessions, 2);
});

test('a page let go of a store it never opened makes no replica of it: no hello there, no session on the server', async t => {
  const stores = createStores(() => createStore({ initial: INITIAL, presence: true }));
  const w = watchedNetwork(id => stores.get(id));
  const b = browser(w.net, { linger: 30 });
  t.after(() => { b.close(); stores.dispose(); });
  const c = b.tab('c');
  await b.until(() => c.db.status === 'online', 'online');
  const { port1, port2 } = new MessageChannel();
  const stop = c.connection.follow(port1, [{ store: 'main', initial: INITIAL }, { store: 'aux', initial: {} }]);
  const link = portConnection(port2, { reconnect: { min: 10, max: 50 } });
  const page = createClient({ connection: link, store: 'main', initial: INITIAL, replicaId: 'page-3' });
  t.after(() => { page.dispose(); link.close(); port1.close(); port2.close(); });
  page.connect();
  await b.until(() => page.status === 'online', 'the page is online on main');
  stop();
  await sleep(100);   // well past the linger
  await w.net.settle();
  assert.deepEqual(w.said('u1', 'hello', 'aux'), [], 'the replica never said hello on aux');
  assert.equal(stores.get('aux').sessions, 0, 'and holds no session there');
});

test('a tab is online only once the browser\'s replica has the store from the server: its state is current by then, as a plain client\'s is', async t => {
  const store = createStore({ initial: INITIAL });
  store.patch({ tasks: { x: { id: 'x', title: 'on the server' } } });
  const net = createNetwork(store);
  const b = browser(net);
  t.after(() => b.close());
  const a = b.tab('a');
  const atOnline = [];
  a.db.on('status', status => { if (status === 'online') atOnline.push(a.db.state.tasks.x?.title); });
  await b.until(() => a.db.status === 'online', 'the tab online');
  assert.deepEqual(atOnline, ['on the server'], 'online with the server\'s state, not the replica\'s empty one');
  a.db.state.tasks.x.done = true;
  await b.until(() => store.snapshot().tasks.x?.done === true, 'an edit made at online reaches the server');
});
