// conflicts.test.js - What a client tells its app about its own edits: a write that lost, an op refused, what is pending
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createNetwork, fakeTime } from './helpers.js';

const INITIAL = { tasks: {} };
const START = 1_000_000;

function setup(storeOptions = {}) {
  const time = fakeTime(START);
  const store = createStore({ initial: INITIAL, now: time, ...storeOptions });
  store.patch({ tasks: { x: { id: 'x', title: 'first', done: false } } });
  const net = createNetwork(store);
  const aTime = fakeTime(START + 1000);   // a's clock runs ahead: its writes win
  const bTime = fakeTime(START);
  const a = net.client({ replicaId: 'a', initial: INITIAL, now: aTime });
  const b = net.client({ replicaId: 'b', initial: INITIAL, now: bTime });
  const heard = { conflict: [], rejected: [] };
  b.on('conflict', c => heard.conflict.push(c));
  b.on('rejected', r => heard.rejected.push(r));
  return { store, net, a, b, heard, time, aTime, bTime };
}

test('a write that lost is reported with what this client wrote and what won, the state showing the winner', async () => {
  const { net, a, b, heard } = setup();
  await net.settle();
  // Both write before either has heard of the other; a's clock runs ahead, so a's stamp is later
  a.state.tasks.x.title = 'from a';
  b.state.tasks.x.title = 'from b';
  b.state.tasks.x.done = true;        // nobody else wrote this: it wins
  await net.settle();
  assert.equal(b.state.tasks.x.title, 'from a');
  assert.equal(b.state.tasks.x.done, true);
  assert.deepEqual(heard.conflict, [{ seq: 1, lost: [{ path: ['tasks', 'x', 'title'], mine: 'from b', theirs: 'from a' }] }]);
  assert.deepEqual(heard.rejected, []);
});

test('a write made offline that lost is reported once the hello is answered', async () => {
  const { net, a, b, heard } = setup();
  await net.settle();
  b.link.goOffline();
  await net.settle();
  b.state.tasks.x.title = 'offline b';
  await net.settle();
  a.state.tasks.x.title = 'online a';
  await net.settle();
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.equal(b.state.tasks.x.title, 'online a');
  assert.deepEqual(heard.conflict, [{ seq: 1, lost: [{ path: ['tasks', 'x', 'title'], mine: 'offline b', theirs: 'online a' }] }]);
});

test('an edit to a record someone deleted is reported with nothing having won: theirs is null', async () => {
  const { net, a, b, heard } = setup();
  await net.settle();
  b.link.goOffline();
  await net.settle();
  b.state.tasks.x.title = 'edited while it went';
  await net.settle();
  delete a.state.tasks.x;
  await net.settle();
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.equal(b.state.tasks.x, undefined);
  assert.deepEqual(heard.conflict, [{ seq: 1, lost: [{ path: ['tasks', 'x', 'title'], mine: 'edited while it went', theirs: null }] }]);
});

test('an op the server refuses is reported as rejected, with its code and its diff; so is a batch the model refuses', async () => {
  const { net, b, heard } = setup({ readOnly: ['tasks/*/done'] });
  await net.settle();
  const errors = [];
  b.on('error', err => errors.push(err.code));
  b.state.tasks.x.done = true;
  await net.settle();
  assert.deepEqual(heard.rejected, [{ seq: 1, code: 'forbidden', message: '"tasks/x/done" is read-only', diff: { tasks: { x: { done: true } } } }]);
  assert.equal(b.state.tasks.x.done, false, 'and the state fell back in line');
  assert.deepEqual(errors, ['forbidden'], 'the error event still comes too');

  b.state.tasks.x.list = [{ an: 'object' }];   // arrays of objects are refused before anything is sent
  await net.settle();
  assert.equal(heard.rejected.length, 2);
  assert.equal(heard.rejected[1].seq, null);
  assert.equal(heard.rejected[1].code, 'invalid');
  assert.equal(b.state.tasks.x.list, undefined);
});

test('isPending says whether an unacknowledged edit writes at, under, or over a path', async () => {
  const { net, b } = setup();
  await net.settle();
  b.link.goOffline();
  await net.settle();
  b.state.tasks.x.title = 'pending';
  b.state.tasks.y = { id: 'y', title: 'new' };
  await net.settle();
  assert.equal(b.isPending('tasks/x/title'), true, 'at');
  assert.equal(b.isPending(['tasks', 'x']), true, 'under');
  assert.equal(b.isPending('tasks/y/title'), true, 'over: the record was written whole');
  assert.equal(b.isPending('tasks/x/done'), false);
  assert.equal(b.isPending('tasks/z'), false);
  delete b.state.tasks.x;
  await net.settle();
  assert.equal(b.isPending('tasks/x/done'), true, 'a deletion covers every field of the record');
  b.link.goOnline();
  b.connect();
  await net.settle();
  assert.equal(b.isPending('tasks'), false, 'nothing once acknowledged');
});
