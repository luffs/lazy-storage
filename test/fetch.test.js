// fetch.test.js - A large snapshot fetched over HTTP: the hello is answered
// with where to fetch it, what the socket delivers meanwhile waits and
// lands on top, and a fetch that fails falls back to the snapshot inline
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gunzipSync, brotliDecompressSync } from 'node:zlib';
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub, memoryStorage, snapshotResponse } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
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
  const document = await plain.json();
  assert.deepEqual(document, { v: store.version, epoch: store.epoch, state: store.snapshot() });
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
