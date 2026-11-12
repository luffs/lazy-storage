// inspector.test.js - The inspector: who it lets in, what its requests answer, and the page it serves
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { connect } from 'node:net';
import { createStore, createStores, createInspector } from '../src/server/index.js';
import { serve, createHandlers, toRequest } from '../src/server/node.js';

const SHAPE = { initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], parents: { subtasks: { taskId: 'tasks' } } };
const BASE = '/ws/inspect';
/** What the page asks, it asks with a header of its own */
const ASKED = { 'x-lazy-inspect': '1' };

function setup({ history = true, authorize = () => true } = {}) {
  const faults = [];
  const stores = createStores(id => (id.startsWith('team-') ? createStore({ ...SHAPE, history, presence: true }) : null));
  const store = stores.get('team-1');
  const inspector = createInspector({ stores, authorize, onError: err => faults.push(err) });
  const get = async (path, headers) => {
    const response = await inspector.handle(new Request(`http://x${BASE}${path}`, { headers }), BASE);
    return { status: response.status, headers: response.headers, body: response.headers.get('content-type')?.startsWith('application/json') ? await response.json() : await response.text() };
  };
  const post = async (path, body, headers = { 'content-type': 'application/json', 'x-lazy-inspect': '1' }) => {
    const response = await inspector.handle(new Request(`http://x${BASE}${path}`, { method: 'POST', headers, body: typeof body === 'string' ? body : JSON.stringify(body) }), BASE);
    return { status: response.status, body: await response.json().catch(() => null) };
  };
  return { stores, store, inspector, get, post, faults };
}

test('there is no inspector without someone to say who may look, and it lets nobody in whom that does not', async () => {
  const stores = createStores(() => createStore(SHAPE));
  assert.throws(() => createInspector({ stores }), /needs authorize/);
  assert.throws(() => createInspector({ authorize: () => true }), /needs stores/);
  assert.throws(() => serve({ stores, port: 0, inspector: {} }), /needs authorize/);

  const asked = [];
  const { get, post, faults, store } = setup({ authorize: (request, { write }) => { asked.push([request.method, write]); return request.headers.get('cookie') === 'admin=1' && (!write || request.headers.get('x-role') === 'owner'); } });
  store.patch({ tasks: { k: { title: 'Milk' } } });
  for (const path of ['', '/api/stores', '/api/store?store=team-1', '/api/records?store=team-1&c=tasks']) assert.equal((await get(path)).status, 401, path);
  assert.equal((await get('/api/stores', { cookie: 'admin=1' })).status, 200);
  const headers = { cookie: 'admin=1', 'content-type': 'application/json', 'x-lazy-inspect': '1' };
  assert.equal((await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 1 }, headers)).status, 401, 'to look is not to change');
  assert.equal((await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 1 }, { ...headers, 'x-role': 'owner' })).status, 200);
  assert.deepEqual(asked.slice(-2), [['POST', true], ['POST', true]]);

  const thrower = setup({ authorize: () => { throw new Error('the session store is down'); } });
  assert.equal((await thrower.get('/api/stores')).status, 401, 'one that throws lets nobody in');
  assert.equal(thrower.faults[0].message, 'the session store is down');
  assert.deepEqual(faults, []);
  const later = setup({ authorize: async () => true });
  assert.equal((await later.get('/api/stores')).status, 200, 'it may answer through a promise');
  assert.equal(await later.inspector.handle(new Request('http://x/ws/other'), BASE), null, 'a path that is not its own is not answered');
  assert.equal(await later.inspector.handle(new Request('http://x/ws/inspector'), BASE), null);
  assert.equal((await later.inspector.handle(new Request(`http://x${BASE}`, { method: 'DELETE' }), BASE)).status, 405);
});

test('the page is one document that lets nothing else load or run, and takes the path it is served under', async () => {
  const { get } = setup();
  const page = await get('');
  assert.equal(page.status, 200);
  assert.match(page.headers.get('content-type'), /^text\/html/);
  const policy = page.headers.get('content-security-policy');
  const nonce = /script-src 'nonce-([^']+)'/.exec(policy)[1];
  assert.match(policy, /^default-src 'none'/);
  assert.match(policy, /frame-ancestors 'none'/);
  assert.ok(!/unsafe/.test(policy));
  assert.equal(page.body.match(/<script[^>]*>/g).length, 1);
  assert.ok(page.body.includes(`<script nonce="${nonce}">`) && page.body.includes(`<style nonce="${nonce}">`));
  assert.ok(page.body.includes(`const BASE = "${BASE}";`));
  assert.ok(!/<script[^>]*src=|<link[^>]*stylesheet|innerHTML|document\.write/.test(page.body), 'nothing fetched, and nothing written as markup');
  assert.notEqual(/nonce-([^']+)/.exec((await get('')).headers.get('content-security-policy'))[1], nonce, 'a nonce of its own each time');
  assert.equal(page.headers.get('cache-control'), 'no-store');
  // The script is JavaScript that parses
  const script = /<script[^>]*>([\s\S]*)<\/script>/.exec(page.body)[1];
  assert.doesNotThrow(() => new Function(`return async () => {${script}}`));
});

test('the stores, a store, its records a page at a time, and a record with its clock', async () => {
  const { get, store, stores } = setup();
  const tasks = {};
  for (let i = 0; i < 250; i++) tasks[`t${String(i).padStart(3, '0')}`] = { title: i === 7 ? 'The Needle' : `Task ${i}`, n: i };
  store.patch({ tasks, settings: { theme: 'dark' } });
  stores.get('team-2');

  const list = (await get('/api/stores')).body;
  assert.deepEqual(list, { listed: true, stores: [{ id: 'team-1', version: 1, sessions: 0 }, { id: 'team-2', version: 0, sessions: 0 }] });
  const overview = (await get('/api/store?store=team-1')).body;
  assert.deepEqual([overview.id, overview.version, overview.history, overview.records, overview.sessions], ['team-1', 1, true, 250, 0]);
  assert.deepEqual(overview.collections, [{ c: 'tasks', records: 250 }, { c: 'subtasks', records: 0 }, { c: 'settings', singleton: true }]);
  assert.ok(!('peers' in overview), 'who is connected is asked for apart: the page looks at a store every two seconds');

  const first = (await get('/api/records?store=team-1&c=tasks')).body;
  assert.deepEqual([first.records.length, first.records[0].id, first.more, first.total], [100, 't000', 't099', 250]);
  const second = (await get(`/api/records?store=team-1&c=tasks&after=${first.more}&limit=500`)).body;
  assert.deepEqual([second.records.length, second.records[0].id, second.more], [150, 't100', undefined], 'two hundred at most, and here the rest');
  assert.deepEqual((await get('/api/records?store=team-1&c=tasks&q=needle')).body.records, [{ id: 't007', record: { title: 'The Needle', n: 7 } }], 'found in the record');
  assert.deepEqual((await get('/api/records?store=team-1&c=tasks&q=T24')).body.records.map(r => r.id), ['t240', 't241', 't242', 't243', 't244', 't245', 't246', 't247', 't248', 't249'], 'or in the id');
  assert.deepEqual((await get('/api/records?store=team-1&c=settings')).body, { singleton: true, record: { theme: 'dark' } });

  const record = (await get('/api/record?store=team-1&c=tasks&id=t007')).body;
  assert.deepEqual([record.there, record.record, Object.keys(record.clock).sort()], [true, { title: 'The Needle', n: 7 }, ['at', 'gone', 'live']]);
  assert.deepEqual((await get('/api/record?store=team-1&c=tasks&id=nope')).body, { c: 'tasks', id: 'nope', there: false });
  assert.deepEqual((await get('/api/record?store=team-1&c=settings')).body.record, { theme: 'dark' });

  // A resolver has no list of what it serves
  const byFunction = createInspector({ stores: id => stores.get(id), authorize: () => true });
  assert.deepEqual(await (await byFunction.handle(new Request(`http://x${BASE}/api/stores`), BASE)).json(), { listed: false, stores: [] });
  assert.equal((await (await byFunction.handle(new Request(`http://x${BASE}/api/store?store=team-1`), BASE)).json()).records, 250);
});

test('a record\'s history, the store\'s activity, and a record put back from the page', async () => {
  const { get, post, store } = setup();
  store.patch({ tasks: { k: { title: 'Milk' } }, subtasks: { s: { taskId: 'k', n: 1 } } });
  store.apply({ replica: 'r1', seq: 1, ts: store.now(), changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }] }, { user: { id: 'alice' } });
  store.apply({ replica: 'r2', seq: 1, ts: store.now(), changes: [{ c: 'tasks', id: 'k', del: true }] }, { user: { id: 'bob' } });

  const history = (await get('/api/history?store=team-1&c=tasks&id=k')).body.entries;
  assert.deepEqual(history.map(e => [e.v, e.user, e.record]), [[3, 'bob', undefined], [2, 'alice', { title: 'Oat milk' }], [1, undefined, { title: 'Milk' }]]);
  assert.deepEqual((await get(`/api/history?store=team-1&c=tasks&id=k&limit=1&cursor=${history[0].v}.${history[0].n}`)).body.entries.map(e => e.v), [2]);
  assert.equal((await get('/api/history?store=team-1&c=settings')).body.entries.length, 1);

  const all = (await get('/api/activity?store=team-1')).body;
  assert.equal(all.version, 3);
  assert.deepEqual(all.entries.map(e => `${e.c}/${e.id ?? ''}@${e.v}`), ['tasks/k@3', 'subtasks/s@3', 'tasks/k@2', 'tasks/k@1', 'subtasks/s@1', 'settings/@0']);
  assert.deepEqual((await get('/api/activity?store=team-1&user=alice')).body.entries.map(e => e.v), [2]);
  assert.deepEqual((await get('/api/activity?store=team-1&deleted=1&collection=subtasks')).body.entries.map(e => `${e.c}/${e.id}`), ['subtasks/s']);
  assert.deepEqual((await get('/api/activity?store=team-1&after=1&oldestFirst=1&limit=2')).body.entries.map(e => e.v), [2, 3], 'what has landed since a version, in order: what the page follows by');
  assert.deepEqual((await get(`/api/activity?store=team-1&limit=2&cursor=${all.entries[1].v}.${all.entries[1].n}`)).body.entries.map(e => e.v), [2, 1]);

  // Put back as it was before it was deleted: the children its deletion took come with it
  const back = await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 2 });
  assert.equal(back.status, 200);
  assert.equal(back.body.version, 4);
  assert.deepEqual([store.state.tasks.k, store.state.subtasks.s], [{ title: 'Oat milk' }, { taskId: 'k', n: 1 }]);
  await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 3, children: false });
  assert.equal(store.state.tasks.k, undefined);
  store.patch({ settings: { theme: 'dark' } });
  await post('/api/revert', { store: 'team-1', c: 'settings', v: 0 });
  assert.equal(store.state.settings.theme, 'light', 'a singleton');
});

test('what cannot be answered as asked is said so, and is no fault; a change is only taken from the page', async () => {
  const { get, post, store, faults } = setup();
  store.patch({ tasks: { k: { title: 'Milk' } } });
  const said = async path => { const { status, body } = await get(path); return [status, body.error]; };
  assert.deepEqual(await said('/api/store?store=nope'), [404, 'Store "nope" is not loaded: no client has it open']);
  assert.deepEqual(await said('/api/store?store=../etc'), [400, 'store is a store id']);
  assert.deepEqual(await said('/api/store'), [400, 'store is a store id']);
  assert.deepEqual(await said('/api/records?store=team-1&c=nope'), [400, '"nope" is not a collection or singleton of this store']);
  assert.deepEqual(await said('/api/records?store=team-1&c=tasks&limit=x'), [400, 'limit is a whole number']);
  assert.deepEqual(await said('/api/history?store=team-1&c=tasks'), [400, 'A record of "tasks" is named by its id']);
  assert.deepEqual(await said('/api/history?store=team-1&c=tasks&id=k&cursor=x'), [400, 'cursor is v.n: a version and a place in it']);
  assert.deepEqual(await said('/api/activity?store=team-1&after=1.5'), [400, 'after is a whole number']);
  assert.deepEqual(await said('/api/nope'), [404, 'Not found']);
  assert.deepEqual(await said('/api/constructor'), [404, 'Not found']);
  assert.deepEqual(await said('/api/revert'), [404, 'Not found']);

  const plain = { status: 400, body: { error: 'A change is asked for by the inspector\'s own page' } };
  assert.deepEqual(await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 1 }, { 'content-type': 'application/json' }), plain, 'without the header a form cannot set');
  assert.deepEqual(await post('/api/revert', 'store=team-1', { 'content-type': 'application/x-www-form-urlencoded', 'x-lazy-inspect': '1' }), plain);
  assert.equal((await post('/api/revert', 'not json')).status, 400);
  assert.equal((await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k' })).status, 400);
  assert.equal((await post('/api/revert', { store: 'nope', c: 'tasks', id: 'k', v: 1 })).status, 404);
  assert.equal((await post('/api/revert', 'x'.repeat(70_000))).status, 413);
  assert.equal((await post('/api/stores', {})).status, 404);
  assert.deepEqual(store.state.tasks.k, { title: 'Milk' });
  assert.deepEqual(faults, []);

  // A store that keeps no history says so where history is asked for, and shows the rest
  const without = setup({ history: false });
  without.store.patch({ tasks: { k: { title: 'Milk' } } });
  assert.equal((await without.get('/api/store?store=team-1')).body.history, false);
  assert.deepEqual((await without.get('/api/history?store=team-1&c=tasks&id=k')).body, { error: 'History is off on this store: make it with history: true' });
  assert.equal((await without.get('/api/activity?store=team-1')).status, 400);
  assert.equal((await without.get('/api/records?store=team-1&c=tasks')).body.records.length, 1);
});

test('the Node server serves the inspector at <path>/inspect when it is given one, and not otherwise', async () => {
  const stores = createStores(() => createStore({ ...SHAPE, history: true }));
  stores.get('main').patch({ tasks: { k: { title: 'Milk' } } });
  stores.get('main').patch({ tasks: { k: { title: 'Oat milk' } } });
  const server = serve({ stores, port: 0, inspector: { authorize: request => new URL(request.url).searchParams.get('key') === 'let-me-in' || request.headers.get('x-key') === 'let-me-in' } });
  const without = serve({ stores, port: 0 });
  await Promise.all([once(server, 'listening'), once(without, 'listening')]);
  const url = `http://localhost:${server.address().port}/ws/inspect`;
  try {
    assert.equal((await fetch(url)).status, 401);
    const page = await fetch(`${url}?key=let-me-in`);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /lazy-storage inspector/);
    const headers = { 'x-key': 'let-me-in' };
    assert.deepEqual((await (await fetch(`${url}/api/stores`, { headers })).json()).stores.map(s => s.id), ['main']);
    assert.deepEqual((await (await fetch(`${url}/api/history?store=main&c=tasks&id=k`, { headers })).json()).entries.map(e => e.record.title), ['Oat milk', 'Milk']);
    const reverted = await fetch(`${url}/api/revert`, { method: 'POST', headers: { ...headers, 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: JSON.stringify({ store: 'main', c: 'tasks', id: 'k', v: 1 }) });
    assert.equal(reverted.status, 200);
    assert.equal(stores.get('main').state.tasks.k.title, 'Milk');
    assert.equal((await fetch(`${url}/api/revert`, { method: 'POST', headers: { ...headers, 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: 'x'.repeat(70_000) })).status, 413);
    assert.equal((await fetch(`http://localhost:${without.address().port}/ws/inspect`)).status, 404, 'no inspector unless the server is given one');
  } finally {
    await server.shutdown();
    await new Promise(resolve => without.close(resolve));
  }
});

test('a revert is told in the history as whoever authorize says is looking; a change is shown with the record it replaced', async () => {
  for (const [who, key] of [[{ id: 'admin-7', name: 'Ada' }, 'admin-7'], ['ops', 'ops'], [true, undefined]]) {
    const { get, post, store } = setup({ authorize: () => who });
    store.patch({ tasks: { k: { title: 'Milk', n: 1 } } });
    store.patch({ tasks: { k: { title: 'Oat milk' } } });
    await post('/api/revert', { store: 'team-1', c: 'tasks', id: 'k', v: 1 });
    const [reverted, edited, made] = (await get('/api/history?store=team-1&c=tasks&id=k')).body.entries;
    assert.deepEqual([reverted.replicaId, reverted.user], ['server', key], `as ${JSON.stringify(who)}`);
    assert.deepEqual([edited.user, made.user], [undefined, undefined], 'the server\'s own writes are nobody\'s');
    assert.deepEqual([reverted.was, edited.was, made.was], [{ title: 'Oat milk', n: 1 }, { title: 'Milk', n: 1 }, undefined], 'each with the record before it');
    assert.deepEqual((await get('/api/activity?store=team-1&limit=1')).body.entries[0].was, { title: 'Oat milk', n: 1 });
  }
  // The store's own call, for an endpoint of the app's
  const store = createStore({ ...SHAPE, history: true });
  store.patch({ tasks: { k: { n: 1 } } });
  store.patch({ tasks: { k: { n: 2 } } });
  store.revert('tasks', 'k', { v: 1 }, { by: { id: 'support-3' } });
  assert.deepEqual(store.historyOf('tasks', 'k', { limit: 1 }).map(e => [e.replicaId, e.user, e.record]), [['server', 'support-3', { n: 1 }]]);
  assert.deepEqual(store.activity({ user: 'support-3' }).length, 1);
});

test('a search looks into five thousand records for one request and says where it stopped; a page is a place in the ids, sorted once a version', async () => {
  const { get, store } = setup();
  const tasks = {};
  for (let i = 0; i < 12_000; i++) tasks[`t${String(i).padStart(5, '0')}`] = { n: i };
  tasks.t11000.title = 'needle';
  store.patch({ tasks });
  const first = (await get('/api/records?store=team-1&c=tasks&q=needle')).body;
  assert.deepEqual([first.records, first.searching, first.more], [[], true, 't04999']);
  const second = (await get(`/api/records?store=team-1&c=tasks&q=needle&after=${first.more}`)).body;
  assert.deepEqual([second.records, second.searching, second.more], [[], true, 't09999']);
  const third = (await get(`/api/records?store=team-1&c=tasks&q=needle&after=${second.more}`)).body;
  assert.deepEqual([third.records.map(r => r.id), third.searching, third.more], [['t11000'], undefined, undefined]);
  // A page after an id that is not there (deleted since) starts at the next
  store.del('tasks', 't00002');
  assert.deepEqual((await get('/api/records?store=team-1&c=tasks&after=t00002&limit=2')).body.records.map(r => r.id), ['t00003', 't00004']);
  assert.deepEqual((await get('/api/records?store=team-1&c=tasks&after=t00001&limit=2')).body.records.map(r => r.id), ['t00003', 't00004'], 'and the ids are those of the store as it is now');
  assert.equal((await get('/api/records?store=team-1&c=tasks&after=zzz')).body.records.length, 0);
});

test('the server\'s sockets are told, each by its user\'s key, and a user\'s are closed by it', async () => {
  const closed = [];
  const stores = createStores(() => createStore(SHAPE));
  const inspector = createInspector({
    stores,
    authorize: (request, { write }) => !write || request.headers.get('x-role') === 'owner',
    sockets: () => [
      { user: { id: 7, token: 'secret' }, stores: ['a'], buffered: 10, idleMs: 5, openMs: 50, expiresAt: 1234 },
      { user: 'plain', stores: [], buffered: 0, idleMs: 1, openMs: 2 },
      { user: undefined, stores: ['a'], buffered: 0, idleMs: 1, openMs: 2 },
      { relay: { id: 'edge' }, stores: ['a'], buffered: 0, idleMs: 1, openMs: 2, expiresAt: null }
    ],
    disconnect: filter => { const picked = [{ id: 7 }, 'plain', undefined, { id: 8 }].filter(filter); closed.push(picked); return picked.length; }
  });
  const told = await (await inspector.handle(new Request(`http://x${BASE}/api/sockets`), BASE)).json();
  assert.deepEqual(told, {
    listed: true,
    disconnects: true,
    total: 4,
    found: 4,
    sockets: [
      { key: '7', stores: ['a'], buffered: 10, idleMs: 5, openMs: 50, expiresAt: 1234 },
      { key: '"plain"', stores: [], buffered: 0, idleMs: 1, openMs: 2, expiresAt: null },
      { key: null, stores: ['a'], buffered: 0, idleMs: 1, openMs: 2, expiresAt: null },
      { relay: true, key: 'edge', stores: ['a'], buffered: 0, idleMs: 1, openMs: 2, expiresAt: null }
    ]
  }, 'a user is told by its key, and nothing else of it');
  const narrowed = await (await inspector.handle(new Request(`http://x${BASE}/api/sockets?store=a&q=ED`), BASE)).json();
  assert.deepEqual([narrowed.total, narrowed.found, narrowed.sockets.map(socket => socket.key)], [4, 1, ['edge']], 'to a store\'s, and to a user\'s');
  const post = (body, role) => inspector.handle(new Request(`http://x${BASE}/api/disconnect`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-lazy-inspect': '1', ...(role ? { 'x-role': role } : {}) }, body: JSON.stringify(body) }), BASE);
  assert.equal((await post({ key: '7' })).status, 401, 'to look is not to close');
  assert.deepEqual(await (await post({ key: '7' }, 'owner')).json(), { closed: 1 });
  assert.deepEqual(closed, [[{ id: 7 }]]);
  assert.equal((await post({}, 'owner')).status, 400);
  assert.equal((await post([], 'owner')).status, 400);

  const bare = createInspector({ stores, authorize: () => true });
  assert.deepEqual(await (await bare.handle(new Request(`http://x${BASE}/api/sockets`), BASE)).json(), { listed: false, total: 0, found: 0, sockets: [] });
  const refused = await bare.handle(new Request(`http://x${BASE}/api/disconnect`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: '{"key":"7"}' }), BASE);
  assert.equal(refused.status, 400);
});

test('the Node server tells its inspector of its sockets, and closes a user\'s from it', async () => {
  const stores = createStores(() => createStore(SHAPE));
  const server = serve({ stores, port: 0, authenticate: request => ({ id: new URL(request.url).searchParams.get('user') }), inspector: { authorize: () => true } });
  await once(server, 'listening');
  const port = server.address().port;
  const url = `http://localhost:${port}/ws/inspect`;
  try {
    const sockets = ['alice', 'bob'].map(user => new WebSocket(`ws://localhost:${port}/ws?user=${user}`));
    await Promise.all(sockets.map(ws => new Promise(resolve => { ws.onopen = resolve; })));
    const told = async () => (await (await fetch(`${url}/api/sockets`)).json()).sockets.map(s => s.key).sort();
    assert.deepEqual(await told(), ['alice', 'bob']);
    const gone = new Promise(resolve => { sockets[0].onclose = resolve; });
    const closed = await fetch(`${url}/api/disconnect`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: JSON.stringify({ key: 'alice' }) });
    assert.deepEqual(await closed.json(), { closed: 1 });
    await gone;
    assert.deepEqual(await told(), ['bob']);
    sockets[1].close();
  } finally {
    await server.shutdown();
  }
});

test('a server of thousands of sockets is told a hundred at a time, the furthest behind first; who is connected to a store likewise', async () => {
  const stores = createStores(() => createStore({ ...SHAPE, presence: true }));
  const store = stores.get('team-1');
  for (let i = 0; i < 1500; i++) {
    const session = store.session({ send() {}, user: { id: `user-${i}` } });
    session.receive({ t: 'hello', proto: 3, replica: `r${i}`, ops: [] });
  }
  const many = Array.from({ length: 1000 }, (_, i) => ({ user: { id: `user-${i}` }, stores: Array.from({ length: i === 999 ? 40 : 1 }, (_, n) => (n ? `s${n}` : 'team-1')), buffered: i * 10, idleMs: i, openMs: 1000 }));
  const inspector = createInspector({ stores, authorize: () => true, sockets: () => many });
  const ask = async path => (await inspector.handle(new Request(`http://x${BASE}${path}`), BASE)).json();

  const told = await ask('/api/sockets');
  assert.deepEqual([told.total, told.found, told.sockets.length, told.sockets[0].key, told.sockets[99].key], [1000, 1000, 100, 'user-999', 'user-900']);
  assert.deepEqual([told.sockets[0].stores.length, told.sockets[0].moreStores], [5, 35], 'and of a socket with many stores open, the first few');
  const one = await ask('/api/sockets?q=user-77');
  assert.deepEqual([one.found, one.sockets.map(socket => socket.key)], [11, ['user-779', 'user-778', 'user-777', 'user-776', 'user-775', 'user-774', 'user-773', 'user-772', 'user-771', 'user-770', 'user-77']]);

  const people = await ask('/api/peers?store=team-1');
  assert.deepEqual([people.total, people.found, people.peers.length], [1500, 1500, 100]);
  const found = await ask('/api/peers?store=team-1&q=USER-1499');
  assert.deepEqual([found.found, found.peers.map(peer => peer.key)], [1, ['user-1499']]);
  // What the page asks every two seconds carries none of them
  assert.ok(JSON.stringify(await ask('/api/store?store=team-1')).length < 1000);
  store.dispose();
});

// --- Found in review ---------------------------------------------------------------------------

/** What a raw request to a server is answered with: its status line, or null when the connection just went */
async function raw(port, text) {
  const socket = connect(port, '127.0.0.1');
  await once(socket, 'connect');
  socket.write(text);
  let reply = '';
  socket.on('data', chunk => { reply += chunk; });
  socket.on('error', () => {});
  await Promise.race([once(socket, 'close'), new Promise(resolve => setTimeout(resolve, 1500))]);
  socket.destroy();
  return reply.split('\r\n')[0] || null;
}

test('a Host header that is no host, or that would move the path, neither ends the Node server nor reaches anything but a refusal', async () => {
  const faults = [];
  const rejections = [];
  const onRejection = err => rejections.push(err);
  const onException = err => rejections.push(err);
  process.on('unhandledRejection', onRejection);
  process.on('uncaughtException', onException);
  const stores = createStores(() => createStore(SHAPE));
  const seen = [];
  const server = serve({ stores, port: 0, onError: err => faults.push(err), inspector: { authorize: request => { seen.push(new URL(request.url).pathname); return false; } } });
  await once(server, 'listening');
  const port = server.address().port;
  try {
    const body = '{}';
    const post = host => `POST /ws/inspect/api/revert HTTP/1.1\r\nHost: ${host}\r\nContent-Type: application/json\r\nContent-Length: ${body.length}\r\nConnection: close\r\n\r\n${body}`;
    for (const host of ['a:99999', 'x/y?', 'x/../../etc', '[', '']) {
      assert.equal(await raw(port, post(host)), 'HTTP/1.1 401 Unauthorized', `Host: ${host}`);
      assert.equal(await raw(port, `GET /ws/inspect/api/stores HTTP/1.1\r\nHost: ${host}\r\nConnection: close\r\n\r\n`), 'HTTP/1.1 401 Unauthorized', `GET, Host: ${host}`);
    }
    assert.ok(seen.every(pathname => pathname.startsWith('/ws/inspect')), 'the request is the one that was routed, whatever its Host says');
    assert.equal(await raw(port, post('localhost')), 'HTTP/1.1 401 Unauthorized', 'and the server is still there');
    await new Promise(resolve => setTimeout(resolve, 20));
    assert.deepEqual([faults, rejections], [[], []]);
    // The Request an upgrade's authenticate is given, likewise
    assert.equal(new URL(toRequest({ headers: { host: 'a:99999' }, url: '/ws?token=x', method: 'GET' }).url).href, 'http://localhost/ws?token=x');
    assert.equal(new URL(toRequest({ headers: { host: 'app.example:8443' }, url: '/ws?token=x', method: 'GET' }).url).href, 'http://app.example:8443/ws?token=x');
  } finally {
    process.off('unhandledRejection', onRejection);
    process.off('uncaughtException', onException);
    await server.shutdown();
  }
});

test('a request that is turned away has none of its body read, and one that is too large is let go of where it passes the mark', async () => {
  const stores = createStores(() => createStore({ ...SHAPE, history: true }));
  stores.get('main').patch({ tasks: { k: { n: 1 } } });
  const server = serve({ stores, port: 0, inspector: { authorize: request => request.headers.get('x-key') === 'in' } });
  await once(server, 'listening');
  const port = server.address().port;
  try {
    // Says it will send a megabyte, and sends none of it: answered at once, not waited for
    const started = Date.now();
    assert.equal(await raw(port, 'POST /ws/inspect/api/revert HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1000000\r\n\r\n{'), 'HTTP/1.1 401 Unauthorized');
    assert.ok(Date.now() - started < 1000, 'turned away before its body');
    // Let in, with a body that says it is too large, and one that is without saying so
    const headers = 'Host: x\r\nx-key: in\r\nx-lazy-inspect: 1\r\nContent-Type: application/json\r\n';
    assert.equal(await raw(port, `POST /ws/inspect/api/revert HTTP/1.1\r\n${headers}Content-Length: 1000000\r\n\r\n{`), 'HTTP/1.1 413 Payload Too Large');
    assert.equal(await raw(port, `POST /ws/inspect/api/revert HTTP/1.1\r\n${headers}Transfer-Encoding: chunked\r\n\r\n${(70_000).toString(16)}\r\n${'x'.repeat(70_000)}\r\n`), 'HTTP/1.1 413 Payload Too Large');
    const body = JSON.stringify({ store: 'main', c: 'tasks', id: 'k', v: 0 });
    assert.equal(await raw(port, `POST /ws/inspect/api/revert HTTP/1.1\r\n${headers}Content-Length: ${body.length}\r\nConnection: close\r\n\r\n${body}`), 'HTTP/1.1 200 OK', 'and one of a size is taken');
  } finally {
    await server.shutdown();
  }
  // The same of a Request whose body is a stream of no stated length (as Bun hands one over)
  const inspector = createInspector({ stores: createStores(() => createStore(SHAPE)), authorize: () => true });
  let pulled = 0;
  const stream = new ReadableStream({ pull(controller) { pulled++; controller.enqueue(new Uint8Array(16_384)); } });
  const response = await inspector.handle(new Request(`http://x${BASE}/api/revert`, { method: 'POST', headers: { 'content-type': 'application/json', ...ASKED }, body: stream, duplex: 'half' }), BASE);
  assert.equal(response.status, 413);
  assert.ok(pulled < 12, `read ${pulled} chunks of an endless body, and no further`);
});

test('to look is not to load: a store no client has open is not made by a request to look at it, and is loaded by one that may change', async () => {
  const made = [];
  const stores = createStores(id => { made.push(id); return id.startsWith('team-') ? createStore(SHAPE) : null; });
  stores.get('team-1');
  const inspector = createInspector({ stores, authorize: (request, { write }) => !write || request.headers.get('x-role') === 'owner' });
  const get = async (path, headers = {}) => { const r = await inspector.handle(new Request(`http://x${BASE}${path}`, { headers }), BASE); return [r.status, await r.json()]; };
  const post = async (path, body, role) => { const r = await inspector.handle(new Request(`http://x${BASE}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...ASKED, ...(role ? { 'x-role': role } : {}) }, body: JSON.stringify(body) }), BASE); return [r.status, await r.json().catch(() => null)]; };
  for (const path of ['/api/store?store=team-2', '/api/records?store=team-2&c=tasks', '/api/glance?store=team-2', '/api/activity?store=team-2', '/api/peers?store=team-2']) {
    assert.deepEqual(await get(path), [404, { error: 'Store "team-2" is not loaded: no client has it open', notLoaded: true }], path);
  }
  assert.deepEqual(made, ['team-1'], 'the factory was not asked');
  assert.deepEqual((await post('/api/revert', { store: 'team-2', c: 'tasks', id: 'k', v: 1 }, 'owner'))[0], 404, 'nor by a change that is not to load it');
  assert.equal((await post('/api/load', { store: 'team-2' }))[0], 401, 'to load is to change');
  assert.deepEqual(await post('/api/load', { store: 'team-2' }, 'owner'), [200, { version: 0 }]);
  assert.deepEqual((await get('/api/store?store=team-2'))[0], 200);
  assert.deepEqual(await post('/api/load', { store: 'nope' }, 'owner'), [404, { error: 'No store "nope"' }]);
  assert.equal((await post('/api/load', { store: '../x' }, 'owner'))[0], 400);

  // What is asked from a page of another site is refused, to look as to change: a browser says where a request
  // comes from, and a look is not had with a link or an image and the credentials it adds to them
  for (const path of ['/api/stores', '/api/store?store=team-1', '/api/records?store=team-1&c=tasks', '/api/sockets']) {
    for (const site of ['cross-site', 'same-site']) assert.deepEqual(await get(path, { 'sec-fetch-site': site }), [403, { error: 'Asked for from another site' }], `${path} ${site}`);
    for (const headers of [{}, { 'sec-fetch-site': 'same-origin' }, { 'sec-fetch-site': 'none' }]) assert.equal((await get(path, headers))[0], 200, `${path} ${JSON.stringify(headers)}`);
  }
  const crossed = await inspector.handle(new Request(`http://x${BASE}/api/load`, { method: 'POST', headers: { 'content-type': 'application/json', ...ASKED, 'x-role': 'owner', 'sec-fetch-site': 'cross-site' }, body: '{"store":"team-1"}' }), BASE);
  assert.equal(crossed.status, 403);
  // A program says nothing of where it asks from; a change still takes the page's header, for a browser that says nothing either
  const bare = await inspector.handle(new Request(`http://x${BASE}/api/load`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-role': 'owner' }, body: '{"store":"team-1"}' }), BASE);
  assert.equal(bare.status, 400);
  assert.equal((await inspector.handle(new Request(`http://x${BASE}`, { headers: { 'sec-fetch-site': 'cross-site' } }), BASE)).status, 200, 'the page itself is a link like any other');
});

test('what the page asks every two seconds counts nothing; a count is made once for a version of the store, and a limit is one or more', async () => {
  const { get, store } = setup();
  const tasks = {};
  for (let i = 0; i < 300; i++) tasks[`t${i}`] = { n: i };
  store.patch({ tasks });
  let counted = 0;
  let stats = 0;
  const realStats = store.stats;
  store.stats = () => { stats++; return realStats(); };
  const realKeys = Object.keys;
  Object.keys = value => { if (value === store.state.tasks) counted++; return realKeys(value); };
  try {
    for (let i = 0; i < 5; i++) assert.deepEqual((await get('/api/glance?store=team-1')).body, { version: 1, sessions: 0 });
    assert.deepEqual([counted, stats], [0, 0], 'a glance looks at no record and counts nothing');
    for (let i = 0; i < 5; i++) assert.equal((await get('/api/store?store=team-1')).body.records, 300);
    assert.deepEqual([counted, stats], [1, 0], 'the records are counted once for the version, however many look');
    store.patch({ tasks: { more: { n: 1 } } });
    assert.equal((await get('/api/store?store=team-1')).body.records, 301);
    assert.equal(counted, 2, 'and again when the store is written');
    assert.deepEqual(Object.keys((await get('/api/stores')).body.stores[0]).sort(), ['id', 'sessions', 'version']);
    assert.equal(stats, 0, 'nor does the list of stores count');
  } finally {
    Object.keys = realKeys;
    store.stats = realStats;
  }
  for (const path of ['/api/records?store=team-1&c=tasks&limit=-1', '/api/records?store=team-1&c=tasks&limit=0', '/api/history?store=team-1&c=tasks&id=t1&limit=0', '/api/activity?store=team-1&limit=-5']) {
    const { status, body } = await get(path);
    assert.deepEqual([status, body.error], [400, 'limit is a whole number of one or more'], path);
  }
  assert.equal((await get('/api/records?store=team-1&c=tasks&limit=100000')).body.records.length, 200);
});

test('authorize may answer with a Response of its own; who it says is looking is told by a key, and a session with none is not handed to the history', async () => {
  // A 401 that asks for credentials, or a way to where one signs in
  const { get } = setup({ authorize: request => (request.headers.get('authorization') ? true : new Response('Sign in', { status: 401, headers: { 'www-authenticate': 'Basic realm="inspector"' } })) });
  const refused = await get('/api/stores');
  assert.deepEqual([refused.status, refused.headers.get('www-authenticate'), refused.body], [401, 'Basic realm="inspector"', 'Sign in']);
  assert.equal((await get('/api/stores', { authorization: 'Basic eDp5' })).status, 200);

  for (const [who, key] of [[{ token: 'secret-session', expires: 5 }, undefined], [{ id: '' }, undefined], [{ id: 7 }, '7'], ['ops', 'ops'], [true, undefined]]) {
    // A store that keys its users otherwise than by id: the one who is looking is told by the key authorize gave
    const stores = createStores(() => createStore({ ...SHAPE, history: true, presence: { key: user => user.email } }));
    const store = stores.get('team-1');
    store.patch({ tasks: { k: { n: 1 } } });
    store.patch({ tasks: { k: { n: 2 } } });
    const inspector = createInspector({ stores, authorize: () => who });
    await inspector.handle(new Request(`http://x${BASE}/api/revert`, { method: 'POST', headers: { 'content-type': 'application/json', ...ASKED }, body: JSON.stringify({ store: 'team-1', c: 'tasks', id: 'k', v: 1 }) }), BASE);
    const [reverted] = store.historyOf('tasks', 'k', { limit: 1 });
    assert.deepEqual([reverted.record, reverted.user], [{ n: 1 }, key], JSON.stringify(who));
    assert.ok(!JSON.stringify(store.activity()).includes('secret-session'));
  }
});

test('a singleton is reverted with options as a record is, without an id; an inherited name is no record; a base with a $ in it is the base', async () => {
  const store = createStore({ ...SHAPE, history: true });
  store.patch({ settings: { theme: 'dark' } });
  store.patch({ settings: { theme: 'blue' } });
  store.revert('settings', { v: 1 }, { by: 'support-3' });
  assert.equal(store.state.settings.theme, 'dark');
  assert.equal(store.historyOf('settings', { limit: 1 })[0].user, 'support-3');
  store.revert('settings', undefined, { v: 2 }, { by: { id: 'admin' } });
  assert.deepEqual([store.state.settings.theme, store.historyOf('settings', { limit: 1 })[0].user], ['blue', 'admin']);
  store.revert('settings', { v: 1 });
  assert.equal(store.state.settings.theme, 'dark');

  const { get } = setup();
  for (const id of ['__proto__', 'constructor', 'toString', 'hasOwnProperty']) {
    assert.deepEqual((await get(`/api/record?store=team-1&c=tasks&id=${id}`)).body, { c: 'tasks', id, there: false }, id);
  }
  assert.deepEqual((await get('/api/record?store=team-1&c=tasks')).body.there, false);

  const inspector = createInspector({ stores: createStores(() => createStore(SHAPE)), authorize: () => true });
  for (const base of ["/admin/$&/x", '/a/$1/b', "/it's/$'/x", '/a/$$/b']) {
    const html = await (await inspector.handle(new Request(`http://x${base}`), base)).text();
    assert.ok(html.includes(`const BASE = ${JSON.stringify(base)};`), base);
    assert.ok(!html.includes('%BASE%'));
  }
});

test('createHandlers.request answers a request it took, whatever the inspector made of it, and resolves', async () => {
  const stores = createStores(() => createStore(SHAPE));
  const handlers = createHandlers({ stores, inspector: { authorize: () => true } });
  const answered = [];
  const res = { headersSent: false, writeHead(status, headers) { answered.push([status, headers.connection]); }, end() {}, once() {} };
  const req = (host, method = 'GET') => ({ url: '/ws/inspect/api/stores', method, headers: { host }, complete: true });
  assert.equal(await handlers.request(req('x/y?'), res), true);
  assert.equal(await handlers.request(req('a:99999'), res), true);
  assert.deepEqual(answered.map(([status]) => status), [200, 200], 'the request that was routed, answered');
  await handlers.close();
});

test('the script of the page declares nothing by a name the window has of its own: a browser refuses to run a script that declares top', async () => {
  const { get } = setup();
  const script = /<script nonce="[^"]+">([\s\S]*?)<\/script>/.exec((await get('')).body)[1];
  // What a script declares at its top level becomes a property of the window, and these the window does not let be
  // redeclared (top, window, document, location) or are better left as they are; a DOM in a test lets them all by
  const taken = new Set(['top', 'window', 'document', 'location', 'self', 'parent', 'frames', 'history', 'name', 'status', 'length', 'closed', 'opener', 'origin', 'event', 'screen', 'navigator']);
  const declared = [...script.matchAll(/^(?:async\s+)?(?:function\*?|const|let|var|class)\s+([A-Za-z_$][\w$]*)/gm)].map(([, name]) => name);
  assert.ok(declared.length > 30, 'the declarations were found');
  assert.deepEqual(declared.filter(name => taken.has(name)), []);
});
