// inspector-page.test.js - The inspector's page itself, run in a DOM against the inspector it is served by
import { native } from './native.js';
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createInspector } from '../src/server/index.js';

const SHAPE = { initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], parents: { subtasks: { taskId: 'tasks' } } };
const BASE = '/ws/inspect';
const placeAgain = () => dispatchEvent(new Event('popstate'));
// The page is quiet when nothing it asked is unanswered, and has stayed so for a few turns of the event loop
let unanswered = 0;
const settle = async () => {
  for (let quiet = 0, turns = 0; quiet < 12 && turns < 2000; turns++) {
    await new Promise(resolve => setImmediate(resolve));
    quiet = unanswered === 0 ? quiet + 1 : 0;
  }
};

// A page listens on the window (the address changing, a key): the one before is taken off it, or every page
// a test has opened would go on answering in the one document they share
let listening = [];

/** The page loaded into the document, its requests answered by the inspector; `tick()` is what its timer does */
async function open({ hash = '', sockets, disconnect } = {}) {
  for (const [type, fn] of listening) removeEventListener(type, fn);
  listening = [];
  // The handler is a server's: it has Node's Request and Response, and the page has the DOM's document
  Object.assign(globalThis, native);
  const stores = createStores(id => (id.startsWith('team-') ? createStore({ ...SHAPE, history: true, presence: true }) : null));
  const store = stores.get('team-1');
  const inspector = createInspector({ stores, authorize: () => ({ id: 'admin' }), sockets, disconnect });
  const asked = [];
  const wire = { cut: false };
  globalThis.fetch = async (url, init) => {
    if (wire.cut) throw new TypeError('fetch failed');
    asked.push(`${init?.method ?? 'GET'} ${url}`);
    unanswered++;
    try {
      return await inspector.handle(new native.Request(new URL(url, 'http://localhost'), init), BASE);
    } finally {
      unanswered--;
    }
  };
  let tick = () => {};
  globalThis.setInterval = fn => { tick = fn; return 0; };
  globalThis.confirm = () => { throw new Error('the page asks in itself, not with the browser\'s box'); };
  const html = await (await inspector.handle(new native.Request(`http://localhost${BASE}`), BASE)).text();
  history.replaceState(null, '', `${BASE}${hash}`);
  document.documentElement.innerHTML = /<html[^>]*>([\s\S]*)<\/html>/.exec(html.replace(/<script[\s\S]*<\/script>/, ''))[1];
  return {
    store,
    stores,
    asked,
    wire,
    tick: async () => { tick(); await settle(); },
    run: async () => {
      const add = globalThis.addEventListener;
      globalThis.addEventListener = (type, fn, ...rest) => { listening.push([type, fn]); return add.call(globalThis, type, fn, ...rest); };
      try {
        new Function(/<script[^>]*>([\s\S]*)<\/script>/.exec(html)[1])();
      } finally {
        globalThis.addEventListener = add;
      }
      await settle();
    }
  };
}
const text = selector => document.querySelector(selector)?.textContent ?? '';
/** The value a tree on the page shows */
const valueOf = selector => JSON.parse(document.querySelector(selector).dataset.json);
/** A change as the page lays it out: what it does, what was there (-), what is there now (+) */
const changes = entry => [...entry.querySelectorAll('.change')].map(change => [...change.children].map(node => node.textContent));
const click = async node => { node.click(); await settle(); };
/** What the page is asking, laid out: its question and the changes it shows; and the answer given to it */
const asking = () => (document.getElementById('ask').hidden ? null : { title: text('#ask h2'), body: text('#ask .box'), changes: changes(document.getElementById('ask')) });
const answer = yes => click(document.querySelector(yes ? '#ask button.yes' : '#ask button.no'));
const byText = (selector, label) => [...document.querySelectorAll(selector)].find(node => node.textContent.trim() === label);

test('the page shows a store\'s records, a record with its history and what each write replaced, and puts it back as the one who is looking', async () => {
  const page = await open();
  page.store.patch({ tasks: { k: { title: 'Milk' }, '<b>x</b>': { title: '<img src=x onerror=alert(1)>' } }, subtasks: { s: { taskId: 'k' } } });
  page.store.apply({ replica: 'r1', seq: 1, ts: page.store.now(), changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }] }, { user: { id: 'alice' } });
  await page.run();

  assert.equal(document.getElementById('stores').value, 'team-1');
  assert.match(text('#meta'), /version 2 · 3 records · 0 sessions · history on/);
  assert.deepEqual([...document.querySelectorAll('main section:first-child li')].map(li => li.textContent), ['tasks2', 'subtasks1', 'settingsone']);
  // What a client wrote is shown as it was written, and is no markup of the page's
  assert.equal(document.querySelector('main img'), null);
  assert.equal(document.querySelector('main b'), null);
  assert.ok(byText('main li.pick span', '<b>x</b>'));

  await click(byText('main li.pick span', 'k').parentElement);
  assert.deepEqual(valueOf('main section:last-child .json'), { title: 'Oat milk' });
  // The record is a tree: a field a line, coloured by its kind, an object folded and unfolded
  const tree = document.querySelector('main section:last-child .json');
  assert.deepEqual([...tree.querySelectorAll('.k')].map(node => node.textContent), ['"title"']);
  assert.equal(tree.querySelector('.s').textContent, '"Oat milk"');
  assert.equal(tree.querySelector('button.copy').textContent, 'Copy');
  const entries = () => [...document.querySelectorAll('main section:last-child .entry')];
  assert.equal(entries().length, 2);
  assert.match(entries()[0].textContent, /v2 · alice \(r1\)/);
  assert.match(entries()[0].querySelector('strong').textContent, /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/, 'a time as 2026-11-12 10:16:12');
  assert.deepEqual(changes(entries()[0]), [['set title', '- "Milk"', '+ "Oat milk"']], 'a change as what was there and what is there now');
  assert.match(entries()[0].querySelector('.ago').textContent, /^(just now|\d+ s ago)$/, 'and how long ago, beside when');
  assert.match(entries()[1].textContent, /v1 · the server/);
  assert.deepEqual(changes(entries()[1]), [['make the record', '+ {\n+   "title": "Milk"\n+ }']]);
  // What a write left is laid out when it is asked for
  const left = entries()[1].querySelector('details');
  assert.equal(left.querySelector('.json'), null);
  left.open = true;
  left.dispatchEvent(new Event('toggle'));
  assert.deepEqual(JSON.parse(left.querySelector('.json').dataset.json), { title: 'Milk' });
  assert.equal(entries()[0].querySelector('button.danger'), null, 'the newest is what the record is: nothing to put back to');
  assert.equal(location.hash, '#store=team-1&tab=records&c=tasks&id=k', 'where the page is, is in its address');

  // Asked in the page, with what it will write; and not done when the answer is no
  await click(entries()[1].querySelector('button.danger'));
  assert.equal(asking().title, 'Revert tasks / k to version 1?');
  assert.deepEqual(asking().changes, [['set title', '- "Oat milk"', '+ "Milk"']]);
  await answer(false);
  assert.equal(asking(), null);
  assert.deepEqual(page.store.state.tasks.k, { title: 'Oat milk' });
  assert.ok(!page.asked.includes(`POST ${BASE}/api/revert`));
  await click(entries()[1].querySelector('button.danger'));
  dispatchEvent(Object.assign(new Event('keydown'), { key: 'Escape' }));
  await settle();
  assert.equal(asking(), null, 'Escape is no');
  await click(entries()[1].querySelector('button.danger'));
  await answer(true);
  assert.deepEqual(page.store.state.tasks.k, { title: 'Milk' });
  assert.ok(page.asked.includes(`POST ${BASE}/api/revert`));
  assert.match(entries()[0].textContent, /v3 · admin, through the server/, 'told in the history as the one who is looking');
  assert.deepEqual(changes(entries()[0]), [['set title', '- "Oat milk"', '+ "Milk"']]);
  assert.equal(text('#note'), '');
});

test('the page opens where its address says, follows the activity as it lands, and restores what was deleted', async () => {
  const page = await open({ hash: '#store=team-1&tab=records&c=settings' });
  page.store.patch({ tasks: { k: { title: 'Milk' } }, subtasks: { s: { taskId: 'k' } } });
  await page.run();
  assert.equal(text('main section:last-child h2'), 'settings', 'a singleton, straight from a link');
  assert.deepEqual(valueOf('main section:last-child .json'), { theme: 'light' });

  // The way back is the browser's: the address changes, and the page goes where it says
  history.pushState(null, '', `${BASE}#store=team-1&tab=activity`);
  dispatchEvent(new Event('popstate'));
  await settle();
  assert.equal(document.querySelector('#tabs button[aria-pressed="true"]').textContent, 'Activity');
  const feed = () => [...document.querySelectorAll('main .entry')].map(entry => entry.querySelector('header').textContent);
  assert.equal(feed().length, 3);
  assert.match(feed()[0], /v1 · the server.*tasks \/ k/);

  // An entry someone has open stays open when more lands above it
  const open1 = document.querySelector('main .entry details');
  open1.open = true;
  page.store.apply({ replica: 'r2', seq: 1, ts: page.store.now(), changes: [{ c: 'tasks', id: 'k', del: true }] }, { user: { id: 'bob' } });
  await page.tick();
  assert.equal(feed().length, 5);
  assert.match(feed()[0], /v2 · bob \(r2\).*deleted.*Restore/);
  assert.match(feed()[1], /subtasks \/ s.*deleted/);
  assert.ok(open1.isConnected && open1.open);
  assert.match(text('#meta'), /version 2 · 0 records/);

  await click(byText('main .entry button.danger', 'Restore'));
  assert.equal(asking().title, 'Restore tasks / k to version 1?');
  assert.deepEqual(asking().changes, [['make the record', '+ {\n+   "title": "Milk"\n+ }']]);
  assert.match(asking().body, /The records its deletion took with it come back too/);
  await answer(true);
  assert.deepEqual([page.store.state.tasks.k, page.store.state.subtasks.s], [{ title: 'Milk' }, { taskId: 'k' }], 'with the child its deletion took');
  assert.equal(location.hash, '#store=team-1&tab=records&c=tasks&id=k', 'and the page is on the record');

  // Narrowed to what was deleted
  await click(byText('#tabs button', 'Activity'));
  const deletions = document.querySelector('main input[type="checkbox"]');
  deletions.checked = true;
  deletions.dispatchEvent(new Event('change'));
  await settle();
  assert.deepEqual(feed().map(line => /(tasks \/ k|subtasks \/ s)/.exec(line)[0]), ['tasks / k', 'subtasks / s']);
  assert.equal(text('#note'), '');
});

test('the sessions: the server\'s sockets, and a user\'s closed from the page', async () => {
  const closed = [];
  const page = await open({
    hash: '#store=team-1&tab=sessions',
    sockets: () => [
      { user: { id: 'alice', name: 'Alice' }, stores: ['team-1', 'team-2'], buffered: 0, idleMs: 1500, openMs: 125_000, expiresAt: null },
      { user: { id: 'bob' }, stores: ['team-1'], buffered: 200_000, idleMs: 20, openMs: 4_000_000, expiresAt: null },
      { relay: { id: 'edge-1' }, stores: ['team-1'], buffered: 0, idleMs: 5, openMs: 900, expiresAt: null }
    ],
    disconnect: filter => { closed.push(['alice', 'bob', undefined].filter(id => filter(id && { id }))); return 1; }
  });
  await page.run();
  const rows = () => [...document.querySelectorAll('main table')][0].querySelectorAll('tbody tr');
  assert.deepEqual([...rows()].map(row => [...row.children].slice(0, 5).map(cell => cell.textContent)), [
    ['bob', 'team-1', '1 h', '20 ms', '200000 B'],
    ['alice', 'team-1, team-2', '2 min', '2 s', '0 B'],
    ['relay edge-1', 'team-1', '900 ms', '5 ms', '0 B']
  ], 'the furthest behind first');
  assert.equal(rows()[2].querySelector('button'), null, 'a relay is not a user to close');
  await click(rows()[0].querySelector('button'));
  assert.equal(asking().title, 'Close the sockets of bob?');
  await answer(false);
  assert.deepEqual(closed, []);
  await click(rows()[0].querySelector('button'));
  await answer(true);
  assert.deepEqual(closed, [['bob']], 'the sockets of that user, and no other');
  assert.ok(page.asked.includes(`POST ${BASE}/api/disconnect`));

  // Narrowed by what is typed
  const find = document.querySelector('main input[type="search"]');
  find.value = 'ali';
  find.dispatchEvent(new Event('change'));
  await settle();
  assert.deepEqual([...rows()].map(row => row.children[0].textContent), ['alice']);
  assert.match(text('main'), /1 of 1 that match, of 3 sockets/);

  // A server that tells the inspector of no sockets says so
  const bare = await open({ hash: '#store=team-1&tab=sessions' });
  await bare.run();
  assert.match(text('main'), /does not tell the inspector of its sockets/);
});

test('what the server refuses is said on the page, and a search that has more to look in goes on from where it stopped', async () => {
  const page = await open();
  const tasks = {};
  for (let i = 0; i < 6000; i++) tasks[`t${String(i).padStart(4, '0')}`] = { n: i };
  tasks.t5900.title = 'the needle';
  page.store.patch({ tasks });
  await page.run();
  const search = document.querySelector('main input[type="search"]');
  search.value = 'needle';
  search.dispatchEvent(new Event('change'));
  await settle();
  const more = byText('main button', 'Search further');
  assert.ok(more && !more.hidden, 'five thousand records looked into, and more to look in');
  assert.match(text('main section:nth-child(2) ul'), /Nothing matches so far/);
  await click(more);
  assert.ok(byText('main li.pick span', 't5900'));

  history.pushState(null, '', `${BASE}#store=other&tab=records`);
  dispatchEvent(new Event('popstate'));
  await settle();
  assert.match(text('main'), /No store "other"/, 'said where the store would have been');
  // And a way to try again, which goes where the address then says
  history.replaceState(null, '', `${BASE}#store=team-1&tab=records`);
  placeAgain();
  await click(byText('main button', 'Try again'));
  assert.ok(byText('main li.pick span', 'tasks'));
});

test('a record that is written while it is looked at is shown as it is now; nested values fold, and a value is copied as JSON', async () => {
  const page = await open({ hash: '#store=team-1&tab=records&c=tasks&id=k' });
  page.store.patch({ tasks: { k: { title: 'Milk', done: false, n: 3, none: null, meta: { tags: ['a', 'b'], empty: {} } } } });
  const copied = [];
  Object.defineProperty(navigator, 'clipboard', { value: { writeText: async value => { copied.push(value); } }, configurable: true });
  await page.run();
  const tree = () => document.querySelector('main section:last-child .json');
  assert.deepEqual(['k', 's', 'b', 'n', 'leaf > .z'].map(kind => tree().querySelector(`.${kind}`).textContent), ['"title"', '"Milk"', 'false', '3', 'null'], 'each kind its own colour');
  const folds = [...tree().querySelectorAll('details')];
  assert.deepEqual(folds.map(fold => fold.querySelector('summary').textContent), ['{ 5 fields }', '"meta": { 2 fields }', '"tags": [ 2 items ],'], 'an object and a list fold, and say what is folded; an empty one is a line');
  assert.ok(folds.every(fold => fold.open));
  await click(tree().querySelector('button.copy'));
  assert.deepEqual(copied.map(text => JSON.parse(text)), [page.store.state.tasks.k]);
  assert.equal(tree().querySelector('button.copy').textContent, 'Copied');

  // Nothing has been written: the page looks, and leaves what is shown alone
  const before = tree();
  await page.tick();
  assert.equal(tree(), before);
  // Written, and another record made beside it
  page.store.patch({ tasks: { k: { done: true }, other: { title: 'Eggs' } } });
  await page.tick();
  assert.notEqual(tree(), before);
  assert.equal(JSON.parse(tree().dataset.json).done, true);
  assert.ok(tree().classList.contains('changed'), 'and marked as changed');
  assert.equal(document.querySelectorAll('main section:last-child .entry').length, 2, 'with the write in its history');
  assert.equal(document.querySelector('li.pick[data-c="tasks"]').lastElementChild.textContent, '2', 'and the collection\'s count');
  // A write of the value a field had already is said as that, once
  page.store.patch({ tasks: { k: { title: 'Milk' } } });
  await page.tick();
  page.store.apply({ replica: 'r', seq: 1, ts: page.store.now(), changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'Milk' }] }, {});
  await click(byText('main li.pick span', 'k').parentElement);
  assert.deepEqual(changes(document.querySelector('main section:last-child .entry')), [['set title (as it was)', '  "Milk"']]);
  assert.equal(document.getElementById('note').textContent, '');
});

test('a server that stops answering is waited for, a longer while each time, and the page says so and is shown afresh when it is back', async () => {
  const page = await open({ hash: '#store=team-1&tab=records&c=tasks&id=k' });
  page.store.patch({ tasks: { k: { title: 'Milk' } } });
  await page.run();
  const real = Date.now;
  let clock = real();
  Date.now = () => clock;
  try {
    const offline = () => (document.getElementById('offline').hidden ? null : text('#offline'));
    assert.equal(offline(), null);
    page.wire.cut = true;
    page.store.patch({ tasks: { k: { title: 'Oat milk' } } });
    await page.tick();
    assert.match(offline(), /The server is not answering\. What is shown is as it was; asking again in 4 s\./);
    assert.deepEqual(valueOf('main section:last-child .json'), { title: 'Milk' }, 'what is shown stays');
    // Not asked again before its time
    page.wire.cut = false;
    const asked = page.asked.length;
    clock += 2000;
    await page.tick();
    assert.equal(page.asked.length, asked);
    page.wire.cut = true;
    clock += 2500;
    await page.tick();
    assert.match(offline(), /asking again in 8 s/, 'a longer while each time');
    clock += 8000;
    page.wire.cut = false;
    await page.tick();
    assert.equal(offline(), null);
    assert.deepEqual(valueOf('main section:last-child .json'), { title: 'Oat milk' }, 'and shown afresh');
    // Thirty seconds at the most
    page.wire.cut = true;
    for (let i = 0; i < 8; i++) {
      clock += 60_000;
      await page.tick();
    }
    assert.match(offline(), /asking again in 30 s/);
  } finally {
    Date.now = real;
  }
});

test('a part of the page that cannot be filled says why in its own place, and what a button could not do is said at the top', async () => {
  const page = await open();
  page.store.patch({ tasks: { k: { title: 'Milk' }, j: { title: 'Eggs' } } });
  page.store.patch({ tasks: { k: { title: 'Oat milk' } } });
  await page.run();
  // The record cannot be read: the list stays, and the record's place says so
  page.wire.cut = true;
  await click(byText('main li.pick span', 'k').parentElement);
  assert.match(text('main section:last-child'), /The server is not answering/);
  assert.ok(byText('main li.pick span', 'j'), 'the rest of the page is as it was');
  assert.equal(document.querySelector('main [aria-busy]'), null);
  page.wire.cut = false;
  await click(byText('main section:last-child button', 'Try again'));
  assert.deepEqual(valueOf('main section:last-child .json'), { title: 'Oat milk' });
  // A revert the server refuses (the version is one the history does not have)
  page.store.revert = () => { throw new TypeError('the store would not'); };
  await click(document.querySelectorAll('main section:last-child .entry')[1].querySelector('button.danger'));
  await answer(true);
  assert.equal(text('#note'), 'the store would not');
  assert.deepEqual(valueOf('main section:last-child .json'), { title: 'Oat milk' });
});
