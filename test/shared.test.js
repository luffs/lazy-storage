// shared.test.js - One socket per browser: tabs elect a leader that runs the replica, the others follow it
import 'fake-indexeddb/auto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createHub } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { sharedConnection } from '../src/client/shared.js';
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
