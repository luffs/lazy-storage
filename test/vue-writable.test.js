// vue-writable.test.js - lazy-storage/vue's writable state: reads db.state, is tracked through the mirror, writes to the client
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createApp, defineComponent, effectScope, h, isReactive, nextTick, reactive, watchEffect } from 'vue';
import { createStore } from '../src/server/index.js';
import { useClient } from '../src/vue/index.js';
import { inOrder } from '../src/client/list.js';
import { createNetwork } from './helpers.js';

/** Lets lazy-watch close the current batch */
const tick = () => new Promise(resolve => setImmediate(resolve));
const plain = value => JSON.parse(JSON.stringify(value));
// An app's state as Mageek keeps it: one record of any depth, and a list of records
const SHAPE = { initial: { doc: { title: '', style: {} }, tasks: {} }, singletons: ['doc'] };

async function setup() {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  const b = net.client({ replicaId: 'b', ...SHAPE });
  await net.settle();
  const scope = effectScope();
  const view = scope.run(() => useClient(a, { writable: true }));
  return { net, a, b, scope, view, state: view.state };
}

test('a write is read back at once, goes to the client, and Vue re-runs when its batch lands, or a teammate\'s', async () => {
  const { net, b, scope, view, state } = await setup();
  const seen = [];
  scope.run(() => watchEffect(() => { seen.push(state.doc.title); }));
  state.doc.title = 'mine';
  assert.equal(state.doc.title, 'mine', 'read back before the batch closes');
  assert.equal(view.pending.value, 0, 'the refs come along as ever');
  await net.settle();
  assert.deepEqual(seen, ['', 'mine']);
  assert.equal(b.state.doc.title, 'mine', 'it went to the client, and on');
  b.state.doc.title = 'theirs';
  await net.settle();
  assert.equal(state.doc.title, 'theirs');
  assert.deepEqual(seen, ['', 'mine', 'theirs']);
  scope.stop();
});

test('two writers deep in one record both keep theirs, through the view', async () => {
  const { net, b, scope, state } = await setup();
  state.doc.style.color = 'red';
  b.state.doc.style.size = 2;
  await net.settle();
  assert.deepEqual(plain(state.doc.style), { color: 'red', size: 2 });
  assert.deepEqual(plain(b.state.doc.style), { color: 'red', size: 2 });
  scope.stop();
});

test('a list through the view: tracked iteration in order, a teammate\'s edit, the list API', async () => {
  const { net, a, b, scope, state } = await setup();
  const titles = [];
  scope.run(() => watchEffect(() => { titles.push(inOrder(state.tasks).map(task => task.title).join(',')); }));
  a.list('tasks').add({ title: 'b' });
  a.list('tasks').add({ title: 'a' });
  await net.settle();
  assert.equal(titles.at(-1), 'b,a');
  b.list('tasks').all()[0].title = 'c';
  await net.settle();
  assert.equal(titles.at(-1), 'c,a', 'a teammate\'s change of one item re-runs the iteration');
  a.list('tasks').move(a.list('tasks').ids()[1], { at: 0 });
  await net.settle();
  assert.equal(titles.at(-1), 'a,c');
  const first = inOrder(state.tasks)[0];
  assert.equal(first, state.tasks[first.id], 'the same path is the same object');
  scope.stop();
});

test('objects: keys, in, delete, spread, JSON; a view or a Vue object written in is copied', async () => {
  const { net, a, scope, state } = await setup();
  const keys = [];
  scope.run(() => watchEffect(() => { keys.push(Object.keys(state.doc.style).join(',')); }));
  state.doc.style.color = 'red';
  state.doc.style.size = 2;
  await net.settle();
  assert.equal(keys.at(-1), 'color,size');
  assert.equal('color' in state.doc.style, true);
  delete state.doc.style.color;
  await net.settle();
  assert.equal(keys.at(-1), 'size');
  assert.deepEqual({ ...state.doc.style }, { size: 2 });
  assert.equal(JSON.stringify(state.doc.style), '{"size":2}');
  state.tasks.x = { title: 'x', tags: ['one'] };
  await tick();
  state.doc.copy = state.tasks.x;
  state.doc.fromVue = reactive({ nested: { n: 1 }, list: [1, 2] });
  await net.settle();
  assert.deepEqual(plain(a.state.doc.copy), plain(a.state.tasks.x), 'a view written in is its data');
  assert.deepEqual(plain(a.state.doc.fromVue), { nested: { n: 1 }, list: [1, 2] });
  state.doc.copy.title = 'changed';
  assert.equal(a.state.tasks.x.title, 'x', 'a copy, not the same record');
  scope.stop();
});

test('undo goes through the client; a path that went is undefined to read and an error to write', async () => {
  const { net, a, scope, state } = await setup();
  const seen = [];
  scope.run(() => watchEffect(() => { seen.push(state.doc.title); }));
  state.doc.title = 'one';
  await net.settle();
  a.undo();
  await net.settle();
  assert.equal(state.doc.title, '');
  assert.deepEqual(seen, ['', 'one', '']);
  state.doc.style.box = { width: 1 };
  await tick();
  const box = state.doc.style.box;
  delete state.doc.style.box;
  await tick();
  assert.equal(box.width, undefined, 'a view of what went reads nothing');
  assert.throws(() => { box.width = 2; }, /nothing to write to/);
  scope.stop();
});

test('one view per client, shared; Vue leaves it as it is; the read-only mirror is unchanged', async () => {
  const { scope, a, state } = await setup();
  const again = scope.run(() => useClient(a, { writable: true }));
  assert.equal(again.state, state, 'one writable view per client');
  const mirror = scope.run(() => useClient(a));
  assert.notEqual(mirror.state, state, 'without writable, the read-only mirror as before');
  assert.equal(reactive({ state }).state, state, 'not wrapped by reactive()');
  assert.equal(isReactive(state), false);
  scope.stop();
});

test('a component edits through it in data(), and renders its own edits and a teammate\'s', async () => {
  const store = createStore(SHAPE);
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', ...SHAPE });
  const b = net.client({ replicaId: 'b', ...SHAPE });
  await net.settle();
  const List = defineComponent({
    props: { db: { type: Object, required: true } },
    data() { return useClient(this.db, { writable: true }); },
    render() {
      return h('div', [
        h('input', { value: this.state.doc.title, onInput: event => { this.state.doc.title = event.target.value; } }),
        h('ul', inOrder(this.state.tasks).map(task => h('li', { key: task.id, onClick: () => { task.done = !task.done; } }, `${task.title}${task.done ? '!' : ''}`)))
      ]);
    }
  });
  const el = document.createElement('div');
  const app = createApp(List, { db: a });
  app.mount(el);
  b.list('tasks').add({ title: 'one', done: false });
  await net.settle();
  await nextTick();
  assert.equal(el.querySelector('ul').innerHTML, '<li>one</li>');
  const input = el.querySelector('input');
  input.value = 'typed';
  input.dispatchEvent(new Event('input'));
  el.querySelector('li').dispatchEvent(new Event('click'));
  await net.settle();
  await nextTick();
  assert.equal(b.state.doc.title, 'typed');
  assert.equal(b.list('tasks').all()[0].done, true);
  assert.equal(el.querySelector('ul').innerHTML, '<li>one!</li>');
  app.unmount();
});
