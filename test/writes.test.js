// writes.test.js - Write authorization: read-only paths and the validate hook
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { createNetwork } from './helpers.js';

const INITIAL = { tasks: {}, team: {} };

/** A client on its own link that has not connected yet, so it can edit offline first */
function offlineClient(net, options, linkOptions) {
  const link = net.link(linkOptions);
  const client = createClient({ transport: link.factory, reconnect: false, store: 'main', initial: INITIAL, ...options });
  client.link = link;
  return client;
}

test('a leaf under a read-only path refuses the whole op; the client drops it, resyncs, and reports forbidden', async () => {
  const store = createStore({ initial: INITIAL, readOnly: ['team'] });
  store.patch({ team: { name: 'Blue', members: { u1: true } } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL }, { user: { id: 'u1' } });
  const b = net.client({ replicaId: 'b', initial: INITIAL }, { user: { id: 'u2' } });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push([err.code, err.message]));

  // One batch, hence one op: the task is lost with the team edit
  a.state.team.name = 'Red';
  a.state.tasks.t1 = { id: 't1', title: 'came along' };
  await net.settle();
  assert.deepEqual(errors, [['forbidden', '"team/name" is read-only']]);
  assert.equal(store.snapshot().team.name, 'Blue');
  assert.equal(store.snapshot().tasks.t1, undefined);
  assert.equal(a.state.team.name, 'Blue', 'the snapshot put the client back in line');
  assert.equal(a.state.tasks.t1, undefined);
  assert.equal(b.state.team.name, 'Blue');
  assert.equal(a.pending, 0);
  assert.equal(a.status, 'online');

  a.collection('tasks').add({ id: 't2', title: 'on its own' });
  await net.settle();
  assert.equal(store.snapshot().tasks.t2.title, 'on its own', 'ordinary writes still land');

  store.patch({ team: { name: 'Green' } });
  await net.settle();
  assert.equal(a.state.team.name, 'Green', 'the server itself is not bound by readOnly');
  assert.equal(b.state.team.name, 'Green');
});

test('a wildcard read-only field cannot be written or supplied on a new record, while the record itself can still be deleted', async () => {
  const store = createStore({ initial: INITIAL, readOnly: ['tasks/*/createdAt'] });
  store.patch({ tasks: { t1: { id: 't1', title: 'seeded', createdAt: 5 } } });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err.message));

  a.state.tasks.t1.title = 'renamed';
  await net.settle();
  assert.equal(store.snapshot().tasks.t1.title, 'renamed', 'sibling fields are writable');

  a.state.tasks.t1.createdAt = 6;
  await net.settle();
  assert.equal(store.snapshot().tasks.t1.createdAt, 5);
  assert.equal(a.state.tasks.t1.createdAt, 5);

  a.collection('tasks').add({ id: 't2', createdAt: 1 });
  await net.settle();
  assert.equal(store.snapshot().tasks.t2, undefined, 'a new record may not bring the field either');

  delete a.state.tasks.t1;
  await net.settle();
  assert.equal(store.snapshot().tasks.t1, undefined, 'deleting the record is not a write under the pattern');
  assert.deepEqual(errors, ['"tasks/t1/createdAt" is read-only', '"tasks/t2/createdAt" is read-only']);
});

test('validate refuses with false or by throwing, sees the session user, and never judges the server', async () => {
  const seen = [];
  let store;
  store = createStore({
    initial: INITIAL,
    validate(diff, context) {
      seen.push([context.replicaId, context.user?.id, context.store === store]);
      if (context.user?.role !== 'editor') return false;
      for (const task of Object.values(diff.tasks ?? {})) {
        if (task && task.title === 'bad') throw new Error('No bad titles');
      }
    }
  });
  const net = createNetwork(store);
  const editor = net.client({ replicaId: 'e', initial: INITIAL }, { user: { id: 'u1', role: 'editor' } });
  const viewer = net.client({ replicaId: 'v', initial: INITIAL }, { user: { id: 'u2', role: 'viewer' } });
  await net.settle();
  const errors = [];
  editor.on('error', err => errors.push(['editor', err.code, err.message]));
  viewer.on('error', err => errors.push(['viewer', err.code, err.message]));

  viewer.collection('tasks').add({ id: 'v1', title: 'from a viewer' });
  editor.collection('tasks').add({ id: 'e1', title: 'from an editor' });
  await net.settle();
  editor.collection('tasks').add({ id: 'e2', title: 'bad' });
  await net.settle();

  assert.deepEqual(Object.keys(store.snapshot().tasks), ['e1']);
  assert.deepEqual(errors, [
    ['viewer', 'forbidden', 'The op was refused'],
    ['editor', 'forbidden', 'No bad titles']
  ]);
  assert.equal(viewer.state.tasks.v1, undefined, 'the refused edit was rolled back by the snapshot');
  assert.equal(editor.state.tasks.e2, undefined);
  assert.deepEqual(seen, [['v', 'u2', true], ['e', 'u1', true], ['e', 'u1', true]]);

  store.patch({ tasks: { s1: { id: 's1', title: 'bad' } } });
  assert.equal(store.snapshot().tasks.s1.title, 'bad', 'patch is the server talking to itself');
  assert.equal(seen.length, 3);
});

test('a validator that returns a trimmed diff has the rest accepted; the client is corrected on what was stripped, silently', async () => {
  const store = createStore({
    initial: INITIAL,
    validate(diff) {
      const out = structuredClone(diff);
      for (const task of Object.values(out.tasks ?? {})) if (task && typeof task === 'object') delete task.secret;
      return out;
    }
  });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  const b = net.client({ replicaId: 'b', initial: INITIAL });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push(err));

  a.collection('tasks').add({ id: 't1', title: 'shown', secret: 'hidden' });
  await net.settle();
  assert.deepEqual(store.snapshot().tasks.t1, { id: 't1', title: 'shown' });
  assert.deepEqual({ ...a.state.tasks.t1 }, { id: 't1', title: 'shown' }, 'the ack corrected the stripped leaf away');
  assert.deepEqual({ ...b.state.tasks.t1 }, { id: 't1', title: 'shown' });
  assert.deepEqual(errors, []);
  assert.equal(a.pending, 0);

  a.state.tasks.t1.secret = 'again';
  a.state.tasks.t1.title = 'still shown';
  await net.settle();
  assert.deepEqual(store.snapshot().tasks.t1, { id: 't1', title: 'still shown' });
  assert.equal(a.state.tasks.t1.secret, undefined);
});

test('a validator that returns something other than a boolean or a diff is a server bug, reported as invalid', async () => {
  const store = createStore({ initial: INITIAL, validate: () => 'yes' });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL });
  await net.settle();
  const errors = [];
  a.on('error', err => errors.push([err.code, err.message]));
  a.collection('tasks').add({ id: 't1' });
  await net.settle();
  assert.deepEqual(errors, [['invalid', 'validate must return true, false, or a diff']]);
  assert.equal(store.snapshot().tasks.t1, undefined);
});

test('refusals inside a hello leave the other queued ops standing and end in one consistent snapshot', async () => {
  const store = createStore({ initial: INITIAL, readOnly: ['team'] });
  store.patch({ team: { name: 'Blue' } });
  const net = createNetwork(store);
  const a = offlineClient(net, { replicaId: 'a' });
  a.collection('tasks').add({ id: 't1', title: 'first, fine' });
  await net.settle();
  a.state.team.name = 'Red';
  await net.settle();
  a.collection('tasks').add({ id: 't2', title: 'third, fine' });
  await net.settle();
  assert.equal(a.pending, 3);
  const errors = [];
  a.on('error', err => errors.push(err.code));

  a.connect();
  await net.settle();
  assert.deepEqual(errors, ['forbidden']);
  assert.deepEqual(Object.keys(store.snapshot().tasks).sort(), ['t1', 't2']);
  assert.equal(store.snapshot().team.name, 'Blue');
  assert.equal(a.state.team.name, 'Blue');
  assert.deepEqual(Object.keys(a.state.tasks).sort(), ['t1', 't2']);
  assert.equal(a.pending, 0);
  assert.equal(a.status, 'online');
});

test('apply without a session is the server and skips the gates; the trusted path is for migrations and tests', () => {
  const store = createStore({ initial: INITIAL, readOnly: ['team'], validate: () => false });
  const r = store.apply({ replicaId: 'import', seq: 1, ts: [Date.now(), 0, 'import'], diff: { team: { name: 'Imported' } } });
  assert.notEqual(r.accepted, null);
  assert.equal(store.snapshot().team.name, 'Imported');
});
