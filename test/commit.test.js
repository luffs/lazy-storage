// commit.test.js - Group commit: what one turn merged is stored in one go, and nobody hears of it before
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore, createHub, memoryStorage } from '../src/server/index.js';

const INITIAL = { tasks: {} };
const turn = () => new Promise(resolve => setImmediate(resolve));
const op = (seq, id) => ({ t: 'op', op: { replicaId: 'r', seq, ts: [Date.now(), seq, 'r'], diff: { tasks: { [id]: { id } } } } });

/** Memory storage that records its commits, as batches, and can be made to fail */
function recording() {
  const storage = memoryStorage();
  const batches = [];
  let failing = false;
  return {
    batches,
    fail() { failing = true; },
    storage: {
      ...storage,
      commit(change) {
        if (failing) throw new Error('disk full');
        batches.push([change.version]);
        storage.commit(change);
      },
      commitMany(changes) {
        if (failing) throw new Error('disk full');
        batches.push(changes.map(c => c.version));
        for (const change of changes) storage.commit(change);
      }
    }
  };
}

test('a turn\'s changes are stored in one commit, and the acks and patches wait for it, in the order they were made', async () => {
  const { storage, batches } = recording();
  const store = createStore({ initial: INITIAL, storage });
  const sent = [];
  const watcher = [];
  const s = store.session({ send: m => sent.push(m) });
  store.session({ send: m => watcher.push(m) }).receive({ t: 'hello', replicaId: 'w', ops: [] });
  s.receive({ t: 'hello', replicaId: 'r', ops: [] });
  const before = batches.length;
  sent.length = 0;
  watcher.length = 0;

  // One poll of the sockets: three ops, and a patch of the server's own
  s.receive(op(1, 'a'));
  s.receive(op(2, 'b'));
  store.patch({ tasks: { c: { id: 'c' } } });
  s.receive(op(3, 'd'));
  assert.equal(store.version, 4, 'merged at once');
  assert.equal(batches.length, before, 'stored at the end of the turn');
  assert.deepEqual([sent, watcher], [[], []], 'and nobody heard of them yet');

  await turn();
  assert.deepEqual(batches.slice(before), [[1, 2, 3, 4]], 'one commit for the turn');
  assert.deepEqual(sent.map(m => `${m.t} ${m.v}`), ['patch 1', 'ack 1', 'patch 2', 'ack 2', 'patch 3', 'patch 4', 'ack 4'], 'then everything, in the order it was made');
  assert.deepEqual(watcher.map(m => `${m.t} ${m.v}`), ['patch 1', 'patch 2', 'patch 3', 'patch 4']);

  // A quiet store sends at once: nothing is pending to wait for
  sent.length = 0;
  s.receive({ t: 'ping' });
  assert.deepEqual(sent, [{ t: 'pong' }]);
  store.dispose();
});

test('a commit that fails unloads the store, and what it would have told is never sent', async () => {
  const { storage, fail } = recording();
  const errors = [];
  const store = createStore({ initial: INITIAL, storage, onError: err => errors.push(err.message) });
  const sent = [];
  const s = store.session({ send: m => sent.push(m) });
  s.receive({ t: 'hello', replicaId: 'r', ops: [] });
  sent.length = 0;
  fail();
  s.receive(op(1, 'a'));
  store.patch({ tasks: { b: { id: 'b' } } });   // returns: the failure comes with the commit
  await turn();
  assert.deepEqual(errors, ['disk full']);
  assert.equal(store.disposed, true);
  assert.deepEqual(sent.map(m => [m.t, m.code]), [['closed', 'unavailable']], 'no ack and no patch: the op stays in the client\'s outbox, for the store loaded afresh');
});

test('flush() stores and sends at once, and throws a commit that failed; groupCommit: false stores every change as it is made', () => {
  const grouped = recording();
  const store = createStore({ initial: INITIAL, storage: grouped.storage, onError: () => {} });
  const sent = [];
  store.session({ send: m => sent.push(m) }).receive({ t: 'hello', replicaId: 'r', ops: [] });
  const before = grouped.batches.length;
  store.patch({ tasks: { a: { id: 'a' } } });
  store.patch({ tasks: { b: { id: 'b' } } });
  store.flush();
  assert.deepEqual(grouped.batches.slice(before), [[1, 2]]);
  assert.deepEqual(sent.filter(m => m.t === 'patch').map(m => m.v), [1, 2]);
  grouped.fail();
  store.patch({ tasks: { c: { id: 'c' } } });
  assert.throws(() => store.flush(), err => err.code === 'unavailable');
  assert.equal(store.disposed, true);

  const each = recording();
  const direct = createStore({ initial: INITIAL, storage: each.storage, groupCommit: false, onError: () => {} });
  const heard = [];
  direct.session({ send: m => heard.push(m) }).receive({ t: 'hello', replicaId: 'r', ops: [] });
  direct.patch({ tasks: { a: { id: 'a' } } });
  direct.patch({ tasks: { b: { id: 'b' } } });
  assert.deepEqual(each.batches.slice(-2), [[1], [2]], 'one commit a change, at once');
  assert.deepEqual(heard.filter(m => m.t === 'patch').map(m => m.v), [1, 2], 'and sent at once');
  each.fail();
  assert.throws(() => direct.patch({ tasks: { c: { id: 'c' } } }), err => err.code === 'unavailable', 'a failed commit throws to the call that made it');
});

test('a socket that opens a store mid-turn hears no patch from before its hello: its answer holds it', async () => {
  const store = createStore({ initial: INITIAL });
  // A stand-in for Bun's topics: a publish reaches the sockets subscribed at the time
  const topic = new Set();
  const channelFor = socket => ({
    subscribe: () => topic.add(socket),
    unsubscribe: () => topic.delete(socket),
    publish: (id, message) => { for (const s of topic) s.push(message); }
  });
  const early = [];
  const late = [];
  createHub(() => store, { send: m => early.push(m), channel: channelFor(early) }).receive({ t: 'hello', store: 'main', replicaId: 'e', ops: [] });

  store.patch({ tasks: { a: { id: 'a' } } });   // held until the turn ends
  createHub(() => store, { send: m => late.push(m), channel: channelFor(late) }).receive({ t: 'hello', store: 'main', replicaId: 'l', ops: [] });
  await turn();
  assert.deepEqual(early.map(m => `${m.t} ${m.v}`), ['snapshot 0', 'patch 1']);
  // A socket subscribed before its session was made would hear the held patch ahead of its snapshot
  assert.deepEqual(late.map(m => `${m.t} ${m.v}`), ['snapshot 1'], 'the snapshot, with the patch in it, and nothing before it');
  assert.deepEqual(late[0].state.tasks, { a: { id: 'a' } });
  store.dispose();
});
