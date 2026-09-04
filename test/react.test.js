// react.test.js - lazy-storage/react: components re-render on every change of the client, through one subscription
import './dom.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { createStore } from '../src/server/index.js';
import { useClient, trackClient } from '../src/react/index.js';
import { createNetwork } from './helpers.js';

const h = React.createElement;
/** Lets lazy-watch close the current batch */
const tick = () => new Promise(resolve => setImmediate(resolve));

test('useClient re-renders for local and remote batches, undo, the outbox, status, and presence; one subscription per client, closed with the last component', async () => {
  const store = createStore({ initial: { tasks: {} }, presence: true });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: { tasks: [] }, lists: ['tasks'] }, { user: { id: 'u1', name: 'Ann' } });
  const b = net.client({ replicaId: 'b', initial: { tasks: [] }, lists: ['tasks'] }, { user: { id: 'u2', name: 'Bo' } });
  await net.settle();
  let subscriptions = 0;
  const watch = a.watch;
  a.watch = fn => {
    subscriptions++;
    const off = watch(fn);
    return () => { subscriptions--; off(); };
  };

  let renders = 0;
  function Facts({ db }) {
    const { status, presence, pending, canUndo } = useClient(db);
    return h('p', null, `${status} ${pending} ${canUndo} ${presence.map(u => u.name).sort().join(',')}`);
  }
  function List({ db }) {
    const { state } = useClient(db);
    renders++;
    return h('ul', null, state.tasks.map(task => h('li', { key: task.id }, `${task.title}${task.done ? '!' : ''}`)));
  }
  const el = document.createElement('div');
  const root = createRoot(el);
  const facts = () => el.querySelector('p').textContent;
  const list = () => el.querySelector('ul').innerHTML;
  await act(async () => { root.render(h('div', null, h(Facts, { db: a }), h(List, { db: a }))); });
  assert.equal(subscriptions, 1, 'two components, one subscription');
  assert.equal(facts(), 'online 0 false Ann,Bo');
  assert.equal(list(), '');

  await act(async () => { a.state.tasks.push({ title: 'one', done: false }); await net.settle(); });
  assert.equal(list(), '<li>one</li>');
  assert.equal(facts(), 'online 0 true Ann,Bo');
  await act(async () => { b.state.tasks.push({ title: 'two', done: false }); await net.settle(); });
  assert.equal(list(), '<li>one</li><li>two</li>', 'a teammate\'s edit');
  await act(async () => { a.state.tasks[0].done = true; await net.settle(); });
  assert.equal(list(), '<li>one!</li><li>two</li>');
  await act(async () => { a.undo(); await net.settle(); });
  assert.equal(list(), '<li>one</li><li>two</li>', 'undo');

  await act(async () => { a.link.goOffline(); await net.settle(); });
  assert.equal(facts(), 'offline 0 true ');
  await act(async () => { a.state.tasks[1].title = 'two, edited'; await net.settle(); });
  assert.equal(facts(), 'offline 1 true ', 'the outbox grew');
  assert.equal(list(), '<li>one</li><li>two, edited</li>');

  const before = renders;
  await act(async () => { root.unmount(); });
  assert.equal(subscriptions, 0, 'the last component gone, the subscription closes');
  await act(async () => { a.link.goOnline(); a.connect(); await net.settle(); });
  assert.equal(a.status, 'online');
  assert.equal(renders, before);
});

test('trackClient hands React one snapshot per change, and re-reads the facts for a component mounting while nothing was subscribed', async () => {
  const store = createStore({ initial: { tasks: {} } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: { tasks: {} } });
  await net.settle();
  const { subscribe, getSnapshot } = trackClient(a);
  assert.equal(trackClient(a), trackClient(a), 'one tracker per client');
  const first = getSnapshot();
  assert.equal(getSnapshot(), first, 'stable until something changes');
  assert.equal(first.state, a.state);
  assert.equal(first.status, 'online');

  a.link.goOffline();
  await net.settle();
  const second = getSnapshot();
  assert.notEqual(second, first, 'nothing subscribed: a change is picked up on the next read');
  assert.equal(second.status, 'offline');
  assert.equal(getSnapshot(), second);

  let notified = 0;
  const off = subscribe(() => { notified++; });
  a.state.tasks.x = { id: 'x' };
  await tick();
  assert.ok(notified >= 1, 'subscribed: told of the batch');
  const third = getSnapshot();
  assert.notEqual(third, second, 'a batch makes a new snapshot');
  assert.equal(third.pending, 1);
  assert.equal(third.state, a.state, 'the state is the same proxy every time');
  off();
});
