// shared.test.js - One socket per browser: tabs elect a leader that runs the browser's replica, the others follow it
import 'fake-indexeddb/auto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { sharedConnection } from '../src/client/shared.js';
import { portConnection } from '../src/client/port.js';
import { memoryOutbox } from '../src/client/storage.js';
import { indexedDBStorage } from '../src/client/indexeddb.js';
import { createNetwork, fakeTime } from './helpers.js';

const SHAPE = { initial: { tasks: {} } };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const snap = db => LazyWatch.snapshot(db.state);

/** The Web Locks API for one process: a queue per name; forceRelease lets a held lock go as a closed tab would */
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
      return { held: live.map(([name]) => ({ name })) };
    },
    forceRelease(name) { queues.get(name)?.[0]?.done?.(); }
  };
}

function browser(net, { storage, now } = {}) {
  const name = `b${Math.random().toString(36).slice(2)}`;
  const locks = fakeLocks();
  const persisted = new Map();
  storage ??= store => persisted.get(store) ?? persisted.set(store, memoryOutbox()).get(store);
  const links = [];
  const tabs = [];
  return {
    links,
    tabs,
    tab(tabId) {
      const link = net.link({ user: { id: 'u1' } });
      links.push(link);
      const connection = sharedConnection({ name, transport: link.factory, storage, locks, tabId, replicaId: `browser-${name}`, reconnect: { min: 10, max: 50 }, keepalive: false });
      const db = createClient({ connection, store: 'main', replicaId: `tab-${tabId}`, now, ...SHAPE });
      db.connect();
      const t = { id: tabId, link, connection, db, close() { db.dispose(); connection.dispose(); tabs.splice(tabs.indexOf(t), 1); } };
      tabs.push(t);
      return t;
    },
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
}

test('two tabs share one socket and one replica, and see each other\'s edits at once', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const b = browser(net, { now });
  const one = b.tab('one');
  const two = b.tab('two');
  await b.until(() => one.db.status === 'online' && two.db.status === 'online', 'online');
  assert.equal(one.connection.leader, true);
  assert.equal(two.connection.leader, false);
  one.db.state.tasks.k = { title: 'Milk' };
  await b.until(() => two.db.state.tasks.k?.title === 'Milk' && store.state.tasks.k, 'tab two and the server have it');
  two.db.state.tasks.k.done = true;
  await b.until(() => one.db.state.tasks.k.done === true && store.state.tasks.k.done === true, 'back');
  assert.equal(store.sessions, 1, 'one socket for the browser');
  b.close();
});

test('with the socket down a tab\'s edits are kept in the browser\'s replica and reach the server when it is back', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const b = browser(net, { now });
  const one = b.tab('one');
  const two = b.tab('two');
  await b.until(() => one.db.status === 'online' && two.db.status === 'online', 'online');
  for (const link of b.links) link.goOffline();
  await b.until(() => two.db.status === 'offline', 'offline');
  two.db.state.tasks.x = { n: 1 };
  await b.until(() => one.db.state.tasks.x?.n === 1, 'the other tab sees it');
  assert.equal(two.db.pending, 1, 'the browser\'s outbox holds it');
  for (const link of b.links) link.goOnline();
  one.connection.connect();
  await b.until(() => store.state.tasks.x?.n === 1 && two.db.pending === 0, 'the server has it');
  b.close();
});

test('when the leader goes, the next tab leads from what the replica kept, and sends what waited', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const db = `shared-${Math.random()}`;
  const b = browser(net, { now, storage: id => indexedDBStorage(`${db}:${id}`) });
  const one = b.tab('one');
  const two = b.tab('two');
  await b.until(() => one.db.status === 'online' && two.db.status === 'online', 'online');
  for (const link of b.links) link.goOffline();
  await b.until(() => one.db.status === 'offline', 'offline');
  one.db.state.tasks.y = { n: 2 };
  await b.until(() => two.db.state.tasks.y?.n === 2, 'seen');
  await sleep(300);   // the replica's records are written once changes settle
  one.close();
  for (const link of b.links) link.goOnline();
  await b.until(() => two.connection.leader, 'two leads');
  two.connection.connect();
  await b.until(() => store.state.tasks.y?.n === 2 && two.db.status === 'online', 'the server has it');
  assert.deepEqual(snap(two.db), store.snapshot());
  b.close();
});

test('a page on a port follows the browser\'s replica for the stores it is given', async () => {
  const now = fakeTime();
  const store = createStore({ ...SHAPE, now });
  const net = createNetwork(store);
  const b = browser(net, { now });
  const one = b.tab('one');
  await b.until(() => one.db.status === 'online', 'online');
  const { port1, port2 } = new MessageChannel();
  const stop = one.connection.follow(port1, [{ store: 'main', ...SHAPE }]);
  const page = createClient({ connection: portConnection(port2), store: 'main', replicaId: 'page', now, ...SHAPE });
  page.connect();
  await b.until(() => page.status === 'online', 'page online');
  page.state.tasks.p = { from: 'page' };
  await b.until(() => one.db.state.tasks.p?.from === 'page' && store.state.tasks.p, 'everyone has it');
  stop();
  page.dispose();
  port1.close();
  port2.close();
  b.close();
});
