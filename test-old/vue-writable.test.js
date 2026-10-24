// vue-writable.test.js - lazy-storage/vue's writable state: reads db.state, is tracked through the mirror, writes to the client
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createApp, defineComponent, effectScope, h, isReactive, nextTick, reactive, watchEffect } from 'vue';
import { createStore } from '../src/server/index.js';
import { useClient } from '../src/vue/index.js';
import { createNetwork } from './helpers.js';

/** Lets lazy-watch close the current batch */
const tick = () => new Promise(resolve => setImmediate(resolve));
const plain = value => JSON.parse(JSON.stringify(value));

async function setup() {
  const initial = { title: '', tasks: [], style: {} };
  const store = createStore({ initial: { title: '', tasks: {}, style: {} } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial, lists: ['tasks'] });
  const b = net.client({ replicaId: 'b', initial, lists: ['tasks'] });
  await net.settle();
  const scope = effectScope();
  const view = scope.run(() => useClient(a, { writable: true }));
  return { net, a, b, scope, view, state: view.state };
}

test('a write is read back at once, goes to the client, and Vue re-runs when its batch lands, or a teammate\'s', async () => {
  const { net, b, scope, view, state } = await setup();
  const seen = [];
  scope.run(() => watchEffect(() => { seen.push(state.title); }));
  state.title = 'mine';
  assert.equal(state.title, 'mine', 'read back before the batch closes');
  assert.equal(view.pending.value, 0, 'the refs come along as ever');
  await net.settle();
  assert.deepEqual(seen, ['', 'mine']);
  assert.equal(b.state.title, 'mine', 'it went to the client, and on');
  b.state.title = 'theirs';
  await net.settle();
  assert.equal(state.title, 'theirs');
  assert.deepEqual(seen, ['', 'mine', 'theirs']);
  scope.stop();
});

test('a list through the view: push, tracked iteration, sort, splice, indexOf by path', async () => {
  const { net, a, b, scope, state } = await setup();
  const titles = [];
  scope.run(() => watchEffect(() => { titles.push(state.tasks.map(task => task.title).join(',')); }));
  assert.equal(Array.isArray(state.tasks), true);
  assert.equal(state.tasks.push({ title: 'b' }, { title: 'a' }), 2, 'a mutator answers as the client\'s array does');
  assert.equal(state.tasks.length, 2);
  await net.settle();
  assert.equal(typeof a.state.tasks[0].id, 'string', 'the client minted the ids');
  assert.equal(titles.at(-1), 'b,a');
  b.state.tasks[0].title = 'c';
  await net.settle();
  assert.equal(titles.at(-1), 'c,a', 'a teammate\'s change of one item re-runs the iteration');
  assert.equal(state.tasks.sort((x, y) => x.title.localeCompare(y.title)), state.tasks, 'sort answers the view');
  await net.settle();
  assert.equal(titles.at(-1), 'a,c');
  assert.deepEqual(b.state.tasks.map(task => task.title), ['a', 'c']);
  const first = state.tasks.find(task => task.title === 'a');
  assert.equal(first, state.tasks[0], 'the same path is the same object');
  assert.equal(state.tasks.indexOf(first), 0);
  state.tasks.splice(0, 1);
  await net.settle();
  assert.equal(titles.at(-1), 'c');
  assert.deepEqual([...state.tasks].map(task => task.title), ['c']);
  assert.equal(Object.keys(state.tasks).length, 1);
  scope.stop();
});

test('objects: keys, in, delete, spread, JSON; a view or a Vue object written in is copied', async () => {
  const { net, a, scope, state } = await setup();
  const keys = [];
  scope.run(() => watchEffect(() => { keys.push(Object.keys(state.style).join(',')); }));
  state.style.color = 'red';
  state.style.size = 2;
  await net.settle();
  assert.equal(keys.at(-1), 'color,size');
  assert.equal('color' in state.style, true);
  assert.equal(Object.prototype.hasOwnProperty.call(state.style, 'size'), true);
  delete state.style.color;
  await net.settle();
  assert.equal(keys.at(-1), 'size');
  assert.equal('color' in state.style, false);
  assert.deepEqual({ ...state.style }, { size: 2 });
  assert.equal(JSON.stringify(state.style), '{"size":2}');
  state.tasks.push({ title: 'x', tags: ['one'] });
  await tick();
  state.copy = state.tasks[0];
  state.fromVue = reactive({ nested: { n: 1 }, list: [1, 2] });
  await net.settle();
  assert.deepEqual(plain(a.state.copy), plain(a.state.tasks[0]), 'a view written in is its data');
  assert.deepEqual(plain(a.state.fromVue), { nested: { n: 1 }, list: [1, 2] });
  state.copy.title = 'changed';
  assert.equal(a.state.tasks[0].title, 'x', 'a copy, not the same record');
  scope.stop();
});

test('undo goes through the client; a path that went is undefined to read and an error to write', async () => {
  const { net, a, scope, state } = await setup();
  const seen = [];
  scope.run(() => watchEffect(() => { seen.push(state.title); }));
  state.title = 'one';
  await net.settle();
  a.undo();
  await net.settle();
  assert.equal(state.title, '');
  assert.deepEqual(seen, ['', 'one', '']);

  state.style.box = { width: 1 };
  await tick();
  const box = state.style.box;
  delete state.style.box;
  await tick();
  assert.equal(box.width, undefined, 'a view of what went reads nothing');
  assert.equal('width' in box, false);
  assert.deepEqual(Object.keys(box), []);
  assert.throws(() => { box.width = 2; }, /nothing to write to at style\.box/);
  assert.throws(() => { delete box.width; }, /nothing to write to/);
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
  assert.equal(state[Symbol.toStringTag], undefined);
  scope.stop();
});

test('a path whose object became a list is viewed as a list', async () => {
  const { net, scope, state } = await setup();
  state.style.shape = { a: 1 };
  await tick();
  assert.equal(Array.isArray(state.style.shape), false);
  state.style.shape = [1, 2];
  await net.settle();
  assert.equal(Array.isArray(state.style.shape), true);
  assert.deepEqual([...state.style.shape], [1, 2]);
  scope.stop();
});

test('a component edits through it in data(), and renders its own edits and a teammate\'s', async () => {
  const store = createStore({ initial: { title: '', tasks: {} } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: { title: '', tasks: [] }, lists: ['tasks'] });
  const b = net.client({ replicaId: 'b', initial: { title: '', tasks: [] }, lists: ['tasks'] });
  await net.settle();
  const List = defineComponent({
    props: { db: { type: Object, required: true } },
    data() { return useClient(this.db, { writable: true }); },
    render() {
      return h('div', [
        h('input', { value: this.state.title, onInput: event => { this.state.title = event.target.value; } }),
        h('ul', this.state.tasks.map(task => h('li', { key: task.id, onClick: () => { task.done = !task.done; } }, `${task.title}${task.done ? '!' : ''}`)))
      ]);
    }
  });
  const el = document.createElement('div');
  const app = createApp(List, { db: a });
  app.mount(el);
  b.state.tasks.push({ title: 'one', done: false });
  await net.settle();
  await nextTick();
  assert.equal(el.querySelector('ul').innerHTML, '<li>one</li>');
  const input = el.querySelector('input');
  input.value = 'typed';
  input.dispatchEvent(new Event('input'));
  el.querySelector('li').dispatchEvent(new Event('click'));
  await net.settle();
  await nextTick();
  assert.equal(b.state.title, 'typed');
  assert.equal(b.state.tasks[0].done, true);
  assert.equal(el.querySelector('ul').innerHTML, '<li>one!</li>');
  app.unmount();
});
