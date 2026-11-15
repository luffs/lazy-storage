// inspector-page.test.js - The inspector's page itself, run in a DOM against the inspector it is served by
import { native } from './native.js';
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createStores, createInspector } from '../src/server/index.js';

const SHAPE = { initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } }, singletons: ['settings'], parents: { subtasks: { taskId: 'tasks' } } };
const BASE = '/ws/inspect';
const placeAgain = () => dispatchEvent(new Event('popstate'));
/** Run with a clock that is moved by hand: `later(ms)` moves it on */
async function clocked(fn) {
  const real = Date.now;
  let now = real();
  Date.now = () => now;
  try {
    return await fn(ms => { now += ms; });
  } finally {
    Date.now = real;
  }
}
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
  // `cut`: the server is not reached. `hold`: a promise every request waits on before it is answered
  const wire = { cut: false, hold: null };
  globalThis.fetch = async (url, init) => {
    if (wire.cut) throw new TypeError('fetch failed');
    asked.push(`${init?.method ?? 'GET'} ${url}`);
    unanswered++;
    try {
      await wire.hold;
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
    /** What the timer does, not waited for: for two looks that overlap */
    tickNow: () => tick(),
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
/** The store picker opened, its offer answered; an offered store by its id */
const openPicker = async () => { document.getElementById('pickStore').click(); await settle(); };
const offered = id => [...document.querySelectorAll('#pickList [role="option"]')].find(li => li.firstChild.textContent === id);
const picked = () => document.getElementById('pickedStore').textContent;
const byText = (selector, label) => [...document.querySelectorAll(selector)].find(node => node.textContent.trim() === label);

test('the page shows a store\'s records, a record with its history and what each write replaced, and puts it back as the one who is looking', async () => {
  const page = await open();
  page.store.patch({ tasks: { k: { title: 'Milk' }, '<b>x</b>': { title: '<img src=x onerror=alert(1)>' } }, subtasks: { s: { taskId: 'k' } } });
  page.store.apply({ replica: 'r1', seq: 1, ts: page.store.now(), changes: [{ c: 'tasks', id: 'k', path: ['title'], set: 'Oat milk' }] }, { user: { id: 'alice' } });
  await page.run();

  assert.equal(picked(), 'team-1');
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
  assert.equal(document.querySelector('#tabs [role="tab"][aria-selected="true"]').textContent, 'Activity');
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
  assert.match(text('#meta'), /version 2 · /, 'the version as it is');

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
  // A store no client has open is not loaded for a look: the page says so where the store would have been,
  // and loads it when it is asked to
  assert.match(text('main'), /Store "other" is not loaded: no client has it open/);
  assert.ok(!page.stores.has('other'));
  await click(byText('main button', 'Load it'));
  assert.equal(text('#note'), 'No store "other"', 'one the server has not, it says so of');
  history.pushState(null, '', `${BASE}#store=team-2&tab=records`);
  placeAgain();
  await settle();
  assert.ok(!page.stores.has('team-2'), 'a look has not loaded it');
  await click(byText('main button', 'Load it'));
  assert.ok(page.stores.has('team-2'));
  assert.ok(byText('main li.pick span', 'tasks'), 'and it is shown');
  assert.ok(page.asked.includes(`POST ${BASE}/api/load`));
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
  assert.equal(document.querySelector('li.pick[data-c="tasks"]').lastElementChild.textContent, '1', 'the count is of a moment ago: to count is to look at every record');
  await clocked(async later => {
    later(11_000);
    await page.tick();
    assert.equal(document.querySelector('li.pick[data-c="tasks"]').lastElementChild.textContent, '2', 'and is counted again in a while');
    assert.match(text('#meta'), /version 2 · 2 records/);
  });
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

// --- Found in review ---------------------------------------------------------------------------

test('the activity is followed row by row: a write of several records that a page of two hundred ends in the middle of is told whole', async () => {
  const page = await open({ hash: '#store=team-1&tab=activity' });
  await page.run();
  for (let i = 0; i < 199; i++) page.store.patch({ tasks: { [`t${i}`]: { n: i } } });
  page.store.patch({ tasks: { a1: { n: 1 }, a2: { n: 2 }, a3: { n: 3 } } });   // rows 200, 201 and 202
  const told = () => [...document.querySelectorAll('main .entry header button:not(.danger)')].map(button => button.textContent);
  await page.tick();
  assert.equal(told().length, 201, 'two hundred, above the one entry that was there');
  assert.ok(told().includes('tasks / a1') && !told().includes('tasks / a2'), 'the page of two hundred ends inside the write');
  await page.tick();
  assert.deepEqual(told().slice(0, 3), ['tasks / a1', 'tasks / a2', 'tasks / a3'], 'and the rest of it comes with the next, in its place');
  assert.equal(told().length, 203);
  assert.equal(new Set(told()).size, 203, 'none told twice');
});

test('the feed keeps five hundred entries on the page, not only in its memory', async () => {
  const page = await open({ hash: '#store=team-1&tab=activity' });
  await page.run();
  for (let round = 0; round < 4; round++) {
    for (let i = 0; i < 180; i++) page.store.patch({ tasks: { [`r${round}-${i}`]: { n: i } } });
    await page.tick();
  }
  const shown = document.querySelectorAll('main .entry');
  assert.equal(shown.length, 500);
  assert.match(shown[0].textContent, /tasks \/ r3-179/, 'the newest at the top');
  assert.ok(byText('main button', 'Older'), 'and the way to what is older is still there');
});

test('two looks that overlap tell an entry once', async () => {
  const page = await open({ hash: '#store=team-1&tab=activity' });
  await page.run();
  page.store.patch({ tasks: { k: { n: 1 } } });
  // The server takes its time: the timer comes round again before it has answered
  let release;
  page.wire.hold = new Promise(resolve => { release = resolve; });
  page.tickNow();
  page.tickNow();
  page.tickNow();
  page.wire.hold = null;
  release();
  await settle();
  assert.equal([...document.querySelectorAll('main .entry')].filter(entry => /tasks \/ k/.test(entry.textContent)).length, 1);
});

test('opened at its plain address, the page is left by going back; a place within it is a step back undoes', async () => {
  const page = await open();
  page.store.patch({ tasks: { k: { n: 1 } } });
  const steps = history.length;
  await page.run();
  assert.equal(location.hash, '#store=team-1&tab=records');
  assert.equal(history.length, steps, 'the first place takes the place of the address it was opened at');
  await click(byText('main li.pick span', 'k').parentElement);
  assert.equal(history.length, steps + 1, 'and the next is a step');
  await click(byText('#tabs button', 'Activity'));
  assert.equal(history.length, steps + 2);
});

test('a question that is open is taken back when the page goes elsewhere, and a revert is of the store it was asked in', async () => {
  const page = await open({ hash: '#store=team-1&tab=records&c=settings' });
  const other = page.stores.get('team-2');
  for (const store of [page.store, other]) {
    store.patch({ settings: { theme: 'dark' } });
    store.patch({ settings: { theme: 'light' } });
  }
  await page.run();
  await click(document.querySelectorAll('main section:last-child .entry')[1].querySelector('button.danger'));
  assert.equal(asking().title, 'Revert settings to version 1?');
  // Back, or a link, while it is open: the page is on the other store, whose settings are at the same versions
  history.pushState(null, '', `${BASE}#store=team-2&tab=records&c=settings`);
  placeAgain();
  await settle();
  assert.equal(asking(), null, 'the question is not left open over another store');
  assert.ok(!page.asked.includes(`POST ${BASE}/api/revert`));
  assert.deepEqual([page.store.state.settings.theme, other.state.settings.theme], ['light', 'light']);

  // Answered while the page is being taken elsewhere: the revert is of the store it was asked in
  history.pushState(null, '', `${BASE}#store=team-1&tab=records&c=settings`);
  placeAgain();
  await settle();
  await click(document.querySelectorAll('main section:last-child .entry')[1].querySelector('button.danger'));
  await openPicker();
  let release;
  page.wire.hold = new Promise(resolve => { release = resolve; });
  document.querySelector('#ask button.yes').click();
  offered('team-2').click();
  page.wire.hold = null;
  release();
  await settle();
  assert.deepEqual([page.store.state.settings.theme, other.state.settings.theme], ['dark', 'light'], 'team-1 is put back, and team-2 is left as it is');
  assert.equal(picked(), 'team-2', 'and the page stays where it was taken');
});

test('what comes back for a store the page has left is not shown in the one it is on', async () => {
  const page = await open({ hash: '#store=team-1&tab=activity' });
  const other = page.stores.get('team-2');
  await page.run();
  page.store.patch({ tasks: { one: { n: 1 } } });
  other.patch({ tasks: { two: { n: 2 } } });
  let release;
  page.wire.hold = new Promise(resolve => { release = resolve; });
  page.tickNow();                     // asks after team-1
  history.pushState(null, '', `${BASE}#store=team-2&tab=activity`);
  placeAgain();
  page.wire.hold = null;
  release();
  await settle();
  const told = [...document.querySelectorAll('main .entry header button:not(.danger)')].map(button => button.textContent);
  assert.ok(told.includes('tasks / two') && !told.includes('tasks / one'), JSON.stringify(told));
  assert.match(text('#meta'), new RegExp(`version ${other.version} `));
});

test('all stores side by side: a place of its own, sorted by a column\'s header, asked again, a number that changed lit', async () => {
  const page = await open({ hash: '#tab=all' });
  page.store.patch({ tasks: { k: { title: 'Milk' } } });
  page.stores.get('team-2').patch({ tasks: { a: { title: 'A' }, b: { title: 'B' } } });
  await page.run();

  assert.equal(document.getElementById('all').getAttribute('aria-selected'), 'true');
  assert.ok(!text('main').includes('null'), 'nothing written as "null" where nothing is shown');
  // What its history holds, as the other counts: its size, and beneath it how many writes; how old the oldest
  // is on pointing at it
  const history = [...document.querySelectorAll('main tbody tr')].find(tr => tr.firstChild.textContent === 'team-2').children[9];
  assert.match(history.textContent, /^\d+ B\d+ writes?$/);
  assert.match(history.querySelector('.why').textContent, /^\d+ writes?$/);
  assert.match(history.querySelector('button').title, /^Its activity · the oldest kept (just now|\d+ s ago), \d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/);
  assert.ok(document.getElementById('one').classList.contains('aside'), 'the store and its tabs set aside');
  assert.equal(text('#meta'), '');
  const ids = () => [...document.querySelectorAll('main tbody tr td:first-child')].map(td => td.textContent);
  assert.deepEqual(ids(), ['team-1', 'team-2'], 'most sessions first, the id breaking ties');

  await click(byText('main th button', 'Records'));
  assert.deepEqual(ids(), ['team-2', 'team-1']);
  assert.equal(location.hash, '#tab=all&sort=records', 'the order in the address');
  assert.equal(document.querySelector('th[aria-sort]').textContent, 'Records');

  // Asked again every other look; a number that changed since is lit
  page.store.patch({ tasks: { j: { title: 'Eggs' }, m: { title: 'Oats' } } });
  await page.tick();
  await page.tick();
  const row = [...document.querySelectorAll('main tbody tr')].find(tr => tr.firstChild.textContent === 'team-1');
  assert.equal(row.children[2].textContent, '3');
  assert.ok(row.children[2].classList.contains('changed'));
  assert.ok(!row.children[1].classList.contains('changed'), 'what did not change is not lit');

  // A store's id goes to its records, and the store picked is shown again
  await click(byText('main td button', 'team-2'));
  assert.equal(location.hash, '#store=team-2&tab=records');
  assert.equal(picked(), 'team-2');
  assert.ok(!document.getElementById('one').classList.contains('aside'));
});

test('the store picker: a button naming the store, a panel to find another in, the keys to go along it', async () => {
  const page = await open({ hash: '#store=team-1&tab=records' });
  page.stores.get('team-2');
  page.stores.get('team-30');
  await page.run();
  const panel = document.getElementById('pickPanel');
  assert.equal(picked(), 'team-1');
  assert.ok(panel.hidden);

  // Opened: the store shown first, marked, and the field to type in has the focus
  await openPicker();
  assert.ok(!panel.hidden);
  assert.equal(document.getElementById('pickStore').getAttribute('aria-expanded'), 'true');
  assert.equal(document.activeElement.id, 'pickFind');
  const options = () => [...document.querySelectorAll('#pickList [role="option"]')].map(li => li.firstChild.textContent);
  assert.equal(options()[0], 'team-1');
  assert.ok(offered('team-1').classList.contains('current'));

  // Typed: those that have it; down and Enter goes to the one marked
  const find = document.getElementById('pickFind');
  find.value = 'team-3';
  find.dispatchEvent(new Event('input'));
  await new Promise(resolve => setTimeout(resolve, 200));
  await settle();
  assert.deepEqual(options(), ['team-30']);
  find.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
  await settle();
  assert.equal(location.hash, '#store=team-30&tab=records');
  assert.ok(panel.hidden);
  assert.equal(picked(), 'team-30');

  // Escape leaves it as it was; nothing is gone to until one is picked
  await openPicker();
  find.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
  assert.equal(document.querySelector('#pickList [aria-selected="true"]').firstChild.textContent, options()[1]);
  find.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
  await settle();
  assert.ok(panel.hidden);
  assert.equal(location.hash, '#store=team-30&tab=records');
  assert.equal(document.activeElement.id, 'pickStore', 'the focus back on the button');

  // / opens it from anywhere; Show all stores goes to All stores, found by what was typed
  document.body.dispatchEvent(new KeyboardEvent('keydown', { key: '/', bubbles: true }));
  await settle();
  assert.ok(!panel.hidden);
  find.value = 'team-2';
  await click(document.getElementById('pickAll'));
  assert.equal(location.hash, '#tab=all');
  assert.equal(document.querySelector('main input[type="search"]').value, 'team-2');
  assert.deepEqual([...document.querySelectorAll('main tbody tr td:first-child')].map(td => td.textContent), ['team-2']);
});
test('all stores: what grows is told a minute, and a number goes to what it counts', async () => {
  await clocked(async later => {
    const page = await open({ hash: '#tab=all' });
    await page.run();
    const row = () => [...document.querySelectorAll('main tbody tr')].find(tr => tr.firstChild.textContent === 'team-1');
    assert.match(row().children[6].textContent, /^–/, 'no rate at the first look');
    // Two writes of the server's in the half minute to the next look: four a minute
    page.store.patch({ tasks: { a: { title: 'A' } } });
    page.store.patch({ tasks: { b: { title: 'B' } } });
    later(30_000);
    await page.tick();
    await page.tick();
    assert.match(row().children[6].textContent, /^4\/min/);

    // Refused: the store's last refusals, with why
    assert.throws(() => page.store.apply({ replica: 'r1', seq: 1, ts: page.store.now(), changes: [{ c: 'tasks', id: 'k', path: [], put: 'x' }] }, { user: { id: 'u1' } }));
    await page.tick();
    await page.tick();
    await click(row().children[8].querySelector('button'));
    assert.match(text('main .refusals h2'), /Refused in team-1/);
    assert.equal(document.querySelectorAll('main .refusals tbody tr').length, 1);
    assert.match(text('main .refusals tbody tr'), /u1/);

    // History goes to the store's activity
    await click(row().children[9].querySelector('button'));
    assert.equal(location.hash, '#store=team-1&tab=activity');
  });
});

test('the header is a tab bar: one tab shown at a time, and the arrow keys go to the next', async () => {
  const page = await open({ hash: '#store=team-1&tab=records' });
  await page.run();
  const selected = () => [...document.querySelectorAll('header [role="tab"][aria-selected="true"]')].map(tab => tab.textContent);
  assert.deepEqual(selected(), ['Records']);
  const records = byText('#tabs [role="tab"]', 'Records');
  records.focus();
  records.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true }));
  await settle();
  assert.deepEqual(selected(), ['Activity']);
  assert.equal(document.activeElement.textContent, 'Activity', 'the focus goes with it');
  document.activeElement.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowLeft', bubbles: true }));
  document.activeElement.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowLeft', bubbles: true }));
  await settle();
  assert.deepEqual(selected(), ['All stores'], 'round to All stores');
  assert.equal(location.hash, '#tab=all');
});

test('a record that is not there is shown without a clock, and nothing is written as "undefined"', async () => {
  const page = await open({ hash: '#store=team-1&tab=records&c=tasks&id=gone' });
  page.store.patch({ tasks: { gone: { title: 'Milk' } } });
  page.store.patch({ tasks: { gone: null } });
  await page.run();
  const detail = text('main section:last-child');
  assert.match(detail, /tasks \/ gone/);
  // The DOM writes a null or an undefined put in as text, run on to the next word: looked for as it is
  assert.ok(!detail.includes('undefined') && !detail.includes('null'), detail);
  assert.ok(!/Clock: when each path was written/.test(detail), 'no clock for a record that is not there');
});

test('all stores is gone through a hundred at a time: the page in the address, on and back by button or by [ and ]', async () => {
  const page = await open({ hash: '#tab=all&sort=id' });
  for (let i = 2; i <= 205; i++) page.stores.get(`team-${i}`);
  await page.run();
  const ids = () => [...document.querySelectorAll('main tbody tr td:first-child')].map(td => td.textContent);
  const line = () => text('main .pager span');
  assert.match(line(), /^1–100 of 205 live/);
  assert.equal(ids()[0], 'team-1');
  assert.equal(byText('main .pager button', '‹ Previous').disabled, true);

  await click(byText('main .pager button', 'Next ›'));
  assert.equal(location.hash, '#tab=all&sort=id&page=2');
  assert.match(line(), /^101–200 of 205 live/);
  assert.equal(ids()[0], 'team-101');

  document.body.dispatchEvent(new KeyboardEvent('keydown', { key: ']', bubbles: true }));
  await settle();
  assert.deepEqual(ids(), ['team-201', 'team-202', 'team-203', 'team-204', 'team-205']);
  assert.equal(byText('main .pager button', 'Next ›').disabled, true, 'no page past the last');
  document.body.dispatchEvent(new KeyboardEvent('keydown', { key: ']', bubbles: true }));
  await settle();
  assert.equal(location.hash, '#tab=all&sort=id&page=3');

  // Another order goes back to the first page
  await click(byText('main th button', 'Records'));
  assert.equal(location.hash, '#tab=all&sort=records');
  assert.match(line(), /^1–100 of 205 live/);

  // Opened at a page, it is that page
  history.pushState(null, '', `${BASE}#tab=all&sort=id&page=2`);
  placeAgain();
  await settle();
  assert.equal(ids()[0], 'team-101');
});
