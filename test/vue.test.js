// vue.test.js - lazy-storage/vue: a reactive mirror that follows local and remote batches, stopping with its scope
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createApp, defineComponent, effectScope, h, nextTick, watchEffect } from 'vue';
import { createStore } from '../src/server/index.js';
import { useClient } from '../src/vue/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {} };
/** Lets lazy-watch close the current batch */
const tick = () => new Promise(resolve => setImmediate(resolve));
const plain = value => JSON.parse(JSON.stringify(value));

test('useClient: a reactive mirror of the state and refs for the rest, remote batches included; the effect scope stops it', async () => {
  const store = createStore({ initial: INITIAL });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1', name: 'Ann' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2', name: 'Bo' } });
  await net.settle();

  const scope = effectScope();
  const seen = [];
  const view = scope.run(() => {
    const view = useClient(a);
    watchEffect(() => { seen.push(view.state.tasks.x?.title ?? null); });
    return view;
  });
  assert.equal(view.status.value, 'online');
  assert.deepEqual(view.presence.value.map(u => u.name).sort(), ['Ann', 'Bo']);
  assert.equal(view.pending.value, 0);
  assert.equal(view.canUndo.value, false);
  assert.equal(view.restored, false);

  a.state.tasks.x = { id: 'x', title: 'mine', done: false };
  assert.equal(view.state.tasks.x, undefined, 'the mirror follows once the batch closes');
  await tick();
  assert.deepEqual(plain(view.state), { tasks: { x: { id: 'x', title: 'mine', done: false } } });
  assert.equal(view.pending.value, 1);
  assert.equal(view.canUndo.value, true);
  await net.settle();
  assert.equal(view.pending.value, 0);

  b.state.tasks.x.title = 'theirs';
  await net.settle();
  assert.equal(view.state.tasks.x.title, 'theirs', 'a remote batch reaches the mirror');
  await nextTick();
  assert.deepEqual(seen, [null, 'mine', 'theirs'], 'and Vue tracked every step');

  a.state.tasks.x.done = true;
  await tick();
  a.undo();
  await tick();
  assert.equal(view.state.tasks.x.done, false, 'undo reaches the mirror');
  assert.equal(view.canRedo.value, true);

  a.link.goOffline();
  await net.settle();
  assert.equal(view.status.value, 'offline');
  assert.deepEqual(view.presence.value, []);

  scope.stop();
  a.link.goOnline();
  a.connect();
  await net.settle();
  assert.equal(a.status, 'online');
  assert.equal(view.status.value, 'offline', 'stopped with the scope: no longer following');
  b.state.tasks.x.title = 'later';
  await net.settle();
  assert.equal(a.state.tasks.x.title, 'later');
  assert.equal(view.state.tasks.x.title, 'theirs');
});

let captured = null;
const components = {
  'Options API': defineComponent({
    props: { db: { type: Object, required: true } },
    data() {
      captured = useClient(this.db);
      return { ...captured, title: '' };
    },
    render() {
      return h('ul', this.state.tasks.map(task => h('li', { key: task.id }, `${task.title}${task.done ? '!' : ''}`)));
    }
  }),
  'Composition API': defineComponent({
    props: { db: { type: Object, required: true } },
    setup(props) {
      captured = useClient(props.db);
      const { state } = captured;
      return () => h('ul', state.tasks.map(task => h('li', { key: task.id }, `${task.title}${task.done ? '!' : ''}`)));
    }
  })
};

for (const [api, List] of Object.entries(components)) {
  test(`${api}: a component renders the mirror of a list, for its own edits and a teammate's, and stops following when unmounted`, async () => {
    const store = createStore({ initial: INITIAL });
    const net = createNetwork(store);
    const a = net.client({ replicaId: 'a', initial: { tasks: [] }, lists: ['tasks'] });
    const b = net.client({ replicaId: 'b', initial: { tasks: [] }, lists: ['tasks'] });
    await net.settle();

    const el = document.createElement('div');
    const app = createApp(List, { db: a });
    app.mount(el);
    assert.equal(el.innerHTML, '<ul></ul>');

    a.state.tasks.push({ title: 'one', done: false });
    await net.settle();
    await nextTick();
    assert.equal(el.innerHTML, '<ul><li>one</li></ul>');
    assert.equal(typeof a.state.tasks[0].id, 'string', 'the minted id was written back');

    b.state.tasks.push({ title: 'two', done: false });
    await net.settle();
    await nextTick();
    assert.equal(el.innerHTML, '<ul><li>one</li><li>two</li></ul>');

    const [moved] = b.state.tasks.splice(1, 1);
    b.state.tasks.unshift(moved);
    await net.settle();
    await nextTick();
    assert.equal(el.innerHTML, '<ul><li>two</li><li>one</li></ul>', 'a remote reorder');

    a.state.tasks[1].done = true;
    await net.settle();
    await nextTick();
    assert.equal(el.innerHTML, '<ul><li>two</li><li>one!</li></ul>');

    app.unmount();
    b.state.tasks.push({ title: 'three', done: false });
    await net.settle();
    await nextTick();
    assert.equal(a.state.tasks.length, 3);
    assert.equal(captured.state.tasks.length, 2, 'unmounted: the mirror no longer follows');
  });
}
