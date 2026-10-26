// fetch.test.js - A large snapshot fetched over HTTP: the hello is answered
// with where to fetch it, what the socket delivers meanwhile waits and
// lands on top, and a fetch that fails falls back to the snapshot inline
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gunzipSync, brotliDecompressSync } from 'node:zlib';
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub, memoryStorage, snapshotResponse } from '../src/server/index.js';
import { snapshotOptions } from '../src/server/snapshot.js';
import { createClient } from '../src/client/index.js';
import { sharedConnection } from '../src/client/shared.js';
import { memoryOutbox } from '../src/client/storage.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
const snap = client => LazyWatch.snapshot(client.state);
/** Let the network drain and the fetch promises settle, a few times over */
const settle = async net => {
  for (let i = 0; i < 6; i++) {
    await net.settle();
    await new Promise(resolve => setImmediate(resolve));
  }
};

/**
 * A store on a network whose hubs offer the snapshot route (from
 * `threshold` bytes; 0 makes every snapshot a fetch), and links whose
 * transports can fetch it, from the route itself unless `fetch` says
 * otherwise. `sent` is everything the server sent, `fetches` every path fetched
 */
function setup({ threshold = 0, fetch, ...storeOptions } = {}) {
  const store = createStore({ initial: INITIAL, storage: memoryStorage(), ...storeOptions });
  const sent = [];
  const fetches = [];
  const net = createNetwork({
    session: ({ send, user }) => createHub(() => store, { send: m => { sent.push(m); send(m); }, user, httpSnapshots: { url: id => `/ws/snapshot/${id}`, threshold } })
  });
  const route = path => Promise.resolve(snapshotResponse(store, new Request(`http://localhost${path}`)));
  const link = ({ canFetch = true } = {}) => {
    const l = net.link();
    if (canFetch) {
      l.factory.fetch = path => {
        fetches.push(path);
        return (fetch ?? route)(path);
      };
    }
    return l;
  };
  const client = (l, replicaId) => {
    const c = createClient({ transport: l.factory, reconnect: false, store: 'main', initial: INITIAL, replicaId });
    c.connect();
    return c;
  };
  return { store, net, sent, fetches, link, client };
}

test('a large snapshot is fetched: the hello is answered with the route and no state, the fetched state lands, and sync goes on over the socket', async () => {
  const { store, net, sent, fetches, link, client } = setup();
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const a = client(link(), 'a');
  await settle(net);

  assert.deepEqual(fetches, ['/ws/snapshot/main']);
  const answer = sent.find(m => m.t === 'snapshot');
  assert.equal(answer.fetch, '/ws/snapshot/main');
  assert.equal(answer.state, undefined, 'the answer carries no state');
  assert.equal(a.status, 'online');
  assert.deepEqual(snap(a), store.snapshot());
  assert.equal(a.version, store.version);

  a.collection('tasks').add({ id: 'b', title: 'second' });
  await settle(net);
  assert.equal(store.state.tasks.b.title, 'second', 'an edit after the fetch syncs as ever');
  store.patch({ tasks: { a: { done: true } } });
  await settle(net);
  assert.equal(a.state.tasks.a.done, true, 'a patch after the fetch arrives on the socket');
  assert.equal(a.pending, 0);
  a.dispose();
});

test('a client that takes its snapshots without the records\' clocks is pointed at the state-only document, cached and tagged on its own', async () => {
  const { store, net, sent, fetches, link } = setup();
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const a = createClient({ transport: link().factory, reconnect: false, store: 'main', initial: INITIAL, replicaId: 'a', clocks: false });
  a.connect();
  await settle(net);
  assert.deepEqual(fetches, ['/ws/snapshot/main?clocks=0']);
  assert.equal(sent.find(m => m.t === 'snapshot').fetch, '/ws/snapshot/main?clocks=0');
  assert.equal(a.status, 'online');
  assert.deepEqual(snap(a), store.snapshot());
  assert.deepEqual(a.clockOf('tasks', 'a'), { live: {}, gone: {} }, 'no clock came');

  const request = (path, headers = {}) => new Request(`http://localhost${path}`, { headers });
  const bare = snapshotResponse(store, request('/ws/snapshot/main?clocks=0'));
  assert.deepEqual(await bare.json(), { v: store.version, epoch: store.epoch, state: store.snapshot() }, 'the state alone');
  const etag = bare.headers.get('etag');
  assert.equal(etag, `"${store.epoch}:${store.version}:state"`);
  assert.equal(snapshotResponse(store, request('/ws/snapshot/main?clocks=0', { 'if-none-match': etag })).status, 304);
  const full = snapshotResponse(store, request('/ws/snapshot/main', { 'if-none-match': etag }));
  assert.equal(full.status, 200, 'the document with the clocks is another');
  assert.equal('clocks' in await full.json(), true);
  assert.equal(snapshotResponse(store, request('/ws/snapshot/main?clocks=false')).headers.get('etag'), etag, 'false says the same');
  a.dispose();
});

test('what the socket delivers during the fetch waits and lands on top of the fetched state, whichever of the two is newer', async () => {
  for (const bodyAfterPatch of [false, true]) {
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    const { store, net, link, client } = setup({
      fetch: async path => {
        // The body is taken when the fetch starts, or once the store has moved on meanwhile
        if (!bodyAfterPatch) {
          const response = snapshotResponse(store, new Request(`http://localhost${path}`));
          await gate;
          return response;
        }
        await gate;
        return snapshotResponse(store, new Request(`http://localhost${path}`));
      }
    });
    store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
    const a = client(link(), 'a');
    await settle(net);
    assert.equal(a.status, 'connecting', 'not online until the snapshot has landed');
    // A patch while the fetch is in flight: delivered on the socket, held back
    store.patch({ tasks: { b: { id: 'b', title: 'meanwhile' } } });
    await settle(net);
    assert.equal(a.state.tasks.b, undefined, 'held, not applied ahead of the snapshot');
    release();
    await settle(net);
    assert.equal(a.status, 'online');
    assert.deepEqual(snap(a), store.snapshot(), bodyAfterPatch ? 'the fetched state had the patch; the held one is skipped' : 'the held patch lands on top of the older fetched state');
    assert.equal(a.version, store.version);
    a.dispose();
  }
});

test('a fetch that fails is reported and answered inline on the next hello; a new socket tries the fetch again', async () => {
  let fail = true;
  const { store, net, sent, fetches, link, client } = setup({
    fetch: path => (fail ? Promise.resolve(new Response('nope', { status: 503 })) : Promise.resolve(snapshotResponse(store, new Request(`http://localhost${path}`)))),
    deltaLog: 0   // every hello is answered with a snapshot, a reconnect's too
  });
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const l = link();
  const a = client(l, 'a');
  const errors = [];
  a.on('error', err => errors.push(err));
  await settle(net);

  assert.equal(fetches.length, 1);
  assert.deepEqual(errors.map(e => e.code), ['snapshot-fetch']);
  assert.match(errors[0].message, /503/);
  const answers = sent.filter(m => m.t === 'snapshot');
  assert.equal(answers.length, 2, 'a second hello was answered');
  assert.equal(answers[0].state, undefined);
  assert.ok(answers[1].state, 'this time with the state inline');
  assert.equal(a.status, 'online');
  assert.deepEqual(snap(a), store.snapshot());

  // The next socket asks to fetch again (the store moved on, so the hello is answered with a snapshot), and this time it works
  fail = false;
  l.goOffline();
  await settle(net);
  store.patch({ tasks: { c: { id: 'c', title: 'while away' } } });
  l.goOnline();
  a.connect();
  await settle(net);
  assert.equal(fetches.length, 2, 'fetched again on the new socket');
  assert.equal(a.status, 'online');
  assert.deepEqual(snap(a), store.snapshot());
  a.dispose();
});

test('the snapshot goes inline for a client that cannot fetch, and for one that can when the state is under the threshold', async () => {
  const cannot = setup();
  cannot.store.patch({ tasks: { a: { id: 'a' } } });
  const a = cannot.client(cannot.link({ canFetch: false }), 'a');
  await settle(cannot.net);
  assert.equal(cannot.fetches.length, 0);
  assert.ok(cannot.sent.find(m => m.t === 'snapshot').state, 'inline: the client did not say it could fetch');
  assert.deepEqual(snap(a), cannot.store.snapshot());
  a.dispose();

  const small = setup({ threshold: 1024 * 1024 });
  small.store.patch({ tasks: { a: { id: 'a' } } });
  const b = small.client(small.link(), 'b');
  await settle(small.net);
  assert.equal(small.fetches.length, 0);
  assert.ok(small.sent.find(m => m.t === 'snapshot').state, 'inline: under the threshold');
  assert.deepEqual(snap(b), small.store.snapshot());
  b.dispose();
});

test('snapshotResponse: the state with its version and epoch, brotli or gzip as accepted, an ETag that answers 304 while the store is unchanged, HEAD without a body', async () => {
  const store = createStore({ initial: INITIAL, storage: memoryStorage() });
  store.patch({ tasks: { a: { id: 'a', title: 'x'.repeat(3000) } } });
  const request = (headers = {}, method = 'GET') => new Request('http://localhost/ws/snapshot/main', { method, headers });

  const plain = snapshotResponse(store, request());
  assert.equal(plain.status, 200);
  assert.equal(plain.headers.get('content-type'), 'application/json');
  assert.equal(plain.headers.get('content-encoding'), null);
  assert.equal(plain.headers.get('access-control-allow-origin'), '*');
  // `origins`: an allowlist echoes a listed origin and no other, false sends no CORS header
  const listed = snapshotResponse(store, request({ origin: 'https://app.example' }), { origins: ['https://app.example'] });
  assert.equal(listed.headers.get('access-control-allow-origin'), 'https://app.example');
  assert.match(listed.headers.get('vary'), /origin/);
  assert.equal(snapshotResponse(store, request({ origin: 'https://evil.example' }), { origins: ['https://app.example'] }).headers.get('access-control-allow-origin'), null);
  assert.equal(snapshotResponse(store, request({ origin: 'https://app.example' }), { origins: false }).headers.get('access-control-allow-origin'), null);
  assert.throws(() => snapshotOptions({ origins: 'https://app.example' }), TypeError, 'a bare string is neither * nor a list');
  const document = await plain.json();
  assert.deepEqual(document, { v: store.version, epoch: store.epoch, state: store.snapshot(), clocks: JSON.parse(store.clocksJSON()) }, 'the state and the clocks a client judges its pending edits by');
  const etag = plain.headers.get('etag');
  assert.equal(etag, `"${store.epoch}:${store.version}"`);

  const gzipped = snapshotResponse(store, request({ 'accept-encoding': 'gzip, deflate' }));
  assert.equal(gzipped.headers.get('content-encoding'), 'gzip');
  const bytes = Buffer.from(await gzipped.arrayBuffer());
  assert.ok(bytes.length < 1000, `gzipped to ${bytes.length} bytes`);
  assert.deepEqual(JSON.parse(gunzipSync(bytes).toString()), document);

  const brotli = snapshotResponse(store, request({ 'accept-encoding': 'gzip, deflate, br' }));
  assert.equal(brotli.headers.get('content-encoding'), 'br', 'brotli comes first when the request takes it');
  assert.deepEqual(JSON.parse(brotliDecompressSync(Buffer.from(await brotli.arrayBuffer())).toString()), document);
  assert.equal(snapshotResponse(store, request({ 'accept-encoding': 'br;q=0, gzip;q=0.5' })).headers.get('content-encoding'), 'gzip', 'a brotli refused by its weight falls back to gzip');
  assert.equal(snapshotResponse(store, request({ 'accept-encoding': 'identity' })).headers.get('content-encoding'), null, 'plain for a request that takes neither');

  assert.equal(snapshotResponse(store, request({ 'if-none-match': etag })).status, 304);
  assert.equal(snapshotResponse(store, request({ 'if-none-match': `W/${etag}, "other"` })).status, 304, 'weak and listed tags match too');
  const head = snapshotResponse(store, request({}, 'HEAD'));
  assert.equal(head.status, 200);
  assert.equal(head.body, null);

  store.patch({ tasks: { a: { done: true } } });
  const changed = snapshotResponse(store, request({ 'if-none-match': etag }));
  assert.equal(changed.status, 200, 'the store moved on: a new body');
  assert.notEqual(changed.headers.get('etag'), etag);
  assert.equal((await changed.json()).state.tasks.a.done, true);
  store.dispose();
});

test('the snapshot route asks authorizeId before it loads the store', async () => {
  const { serveSnapshot } = await import('../src/server/snapshot.js');
  const loaded = [];
  const resolveStore = id => { loaded.push(id); return createStore({ initial: { tasks: {} } }); };
  const request = new Request('http://x/ws/snapshot/team-2');
  const refused = await serveSnapshot(request, 'team-2', { resolveStore, authorizeId: (user, id) => id === 'team-1' });
  assert.equal(refused.status, 403);
  assert.deepEqual(loaded, [], 'nothing loaded');
  const ok = await serveSnapshot(new Request('http://x/ws/snapshot/team-1'), 'team-1', { resolveStore, authorizeId: (user, id) => id === 'team-1' });
  assert.equal(ok.status, 200);
  assert.deepEqual(loaded, ['team-1']);
});

/** A browser's replica (a shared connection, one tab leading on its own) on a link of the setup, and a tab's client on it */
function browserTab(link, { onError, storage = () => memoryOutbox(), linger } = {}) {
  const connection = sharedConnection({ name: `fetch-${Math.random()}`, transport: link.factory, storage, locks: null, channel: null, replicaId: 'browser', keepalive: false, reconnect: false, onError, ...(linger === undefined ? {} : { linger }) });
  const tab = createClient({ connection, store: 'main', initial: INITIAL });
  tab.connect();
  return { connection, tab, close() { tab.dispose(); connection.dispose(); } };
}

test("a browser's replica fetches a large snapshot too, with the clocks, and its tab is online with it", async () => {
  const { store, net, sent, fetches, link } = setup();
  store.patch({ tasks: { a: { id: 'a', title: 'first' }, b: { id: 'b', title: 'second' } } });
  const b = browserTab(link());
  await settle(net);
  assert.deepEqual(fetches, ['/ws/snapshot/main'], 'fetched once, with the clocks (the replica decides for its tabs)');
  const answer = sent.find(m => m.t === 'snapshot');
  assert.equal(answer.state, undefined, 'nothing of the state on the socket');
  assert.equal(b.tab.status, 'online');
  assert.deepEqual(snap(b.tab), store.snapshot());
  store.patch({ tasks: { a: { done: true } } });
  b.tab.collection('tasks').add({ id: 'c', title: 'third' });
  await settle(net);
  assert.equal(b.tab.state.tasks.a.done, true, 'a patch after the fetch arrives on the socket');
  assert.equal(store.state.tasks.c.title, 'third', 'and an edit goes up');
  b.close();
});

test("what the socket brings while a browser's replica fetches waits for the snapshot, and what it already has is passed over", async () => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const fetched = [];
  const { store, net, link } = setup({
    fetch: async path => {
      await gate;
      const response = snapshotResponse(store, new Request(`http://localhost${path}`));
      fetched.push(store.version);
      return response;
    }
  });
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const b = browserTab(link());
  await settle(net);
  assert.notEqual(b.tab.status, 'online', 'waiting for the snapshot');
  store.patch({ tasks: { a: { title: 'renamed while fetching' } } });
  await settle(net);
  store.patch({ tasks: { d: { id: 'd', title: 'after the fetch began' } } });
  release();
  await settle(net);
  assert.equal(b.tab.status, 'online');
  assert.deepEqual(snap(b.tab), store.snapshot());
  b.close();
});

test("a browser's replica whose fetch fails says so and takes the snapshot inline", async () => {
  const errors = [];
  const { store, net, sent, fetches, link } = setup({ fetch: () => Promise.resolve(new Response('no', { status: 500 })) });
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const b = browserTab(link(), { onError: err => errors.push(err) });
  await settle(net);
  assert.deepEqual(fetches, ['/ws/snapshot/main']);
  assert.deepEqual(errors.map(e => e.code), ['snapshot-fetch']);
  assert.ok(sent.some(m => m.t === 'snapshot' && m.state !== undefined), 'asked again, it got the state inline');
  assert.equal(b.tab.status, 'online');
  assert.deepEqual(snap(b.tab), store.snapshot());
  b.close();
});

/** A fetch that waits until `release()`, and a replica's storage that keeps every copy it is asked to write */
function gatedSetup() {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const env = setup({ fetch: async path => { await gate; return snapshotResponse(env.store, new Request(`http://localhost${path}`)); } });
  const written = [];
  const storage = () => {
    const inner = memoryOutbox();
    return { ...inner, saveState: copy => { written.push(copy); inner.saveState(copy); } };
  };
  const wrote = id => written.some(copy => (copy.records ?? []).some(([c, rid]) => c === 'tasks' && rid === id));
  return { ...env, release, storage, wrote };
}

test("a snapshot fetched for a store the server has since closed is not taken by a browser's replica", async () => {
  const { store, net, link, release, storage, wrote } = gatedSetup();
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const b = browserTab(link(), { storage });
  await settle(net);
  assert.notEqual(b.tab.status, 'online', 'fetching');
  store.closeSessions(() => true, 'removed from the team');
  await settle(net);
  release();
  await settle(net);
  await new Promise(resolve => setTimeout(resolve, 300));   // the replica writes its copy once changes settle
  assert.equal(b.tab.closed?.code, 'evicted');
  assert.deepEqual(snap(b.tab), { tasks: {} }, 'nothing of it reached the tab');
  assert.equal(wrote('a'), false, "nor the replica's storage");
  b.close();
});

test("a snapshot fetched for a store a browser's replica has let go of is not taken, nor its copy written", async () => {
  const { store, net, link, release, storage, wrote } = gatedSetup();
  store.patch({ tasks: { a: { id: 'a', title: 'first' } } });
  const b = browserTab(link(), { storage, linger: 0 });
  await settle(net);
  b.tab.dispose();   // the last tab leaves: the replica lets go of the store
  await settle(net);
  await new Promise(resolve => setTimeout(resolve, 50));
  release();
  await settle(net);
  await new Promise(resolve => setTimeout(resolve, 300));
  assert.equal(wrote('a'), false);
  b.connection.dispose();
});
