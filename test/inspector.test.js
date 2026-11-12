// inspector.test.js - The inspector: who it lets in, what its requests answer, and the page it serves
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createStore, createStores, createInspector } from '../src/server/index.js';
import { serve } from '../src/server/node.js';

const SHAPE = { initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], parents: { subtasks: { taskId: 'tasks' } } };
const BASE = '/ws/inspect';

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
  assert.deepEqual(list, { listed: true, stores: [{ id: 'team-1', version: 1, sessions: 0, records: 250 }, { id: 'team-2', version: 0, sessions: 0, records: 0 }] });
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
  assert.deepEqual(await said('/api/store?store=nope'), [404, 'No store "nope"']);
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
      { key: 'plain', stores: [], buffered: 0, idleMs: 1, openMs: 2, expiresAt: null },
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
