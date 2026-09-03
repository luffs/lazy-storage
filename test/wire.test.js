// wire.test.js - One serialization per broadcast, however many sockets
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { toJSON, tagStore } from '../src/server/wire.js';
import { createStore, createHub } from '../src/server/index.js';

test('toJSON encodes a message once and tagStore splices the store id in without encoding the payload again', () => {
  const original = JSON.stringify;
  const encoded = [];
  JSON.stringify = value => { encoded.push(value); return original(value); };
  try {
    const message = { t: 'patch', diff: { tasks: { a: { title: 'x', tags: ['a', 'b'] } } }, ts: [1, 0, 'r'], v: 3 };
    const json = toJSON(message);
    assert.equal(toJSON(message), json, 'remembered');
    const tagged = ['team-x', 'te"am', 'z'].map(id => tagStore(message, id));
    for (const [i, id] of ['team-x', 'te"am', 'z'].entries()) {
      assert.deepEqual(tagged[i], { ...message, store: id });
      assert.deepEqual(JSON.parse(toJSON(tagged[i])), { ...message, store: id }, 'the spliced JSON parses back to the tagged message');
    }
    assert.equal(encoded.filter(v => v === message).length, 1, 'the payload was encoded once');
    assert.deepEqual(encoded.filter(v => typeof v === 'string'), ['team-x', 'te"am', 'z'], 'only the ids were encoded for the copies');
    assert.equal('store' in message, false, 'the original is untouched');
    assert.deepEqual(Object.keys(tagged[0]), ['t', 'diff', 'ts', 'v', 'store'], 'the cache is not an enumerable key');
    assert.deepEqual(structuredClone(tagged[0]), { ...message, store: 'team-x' });
  } finally {
    JSON.stringify = original;
  }
});

test('a message that already names a store, or was never encoded, is encoded the ordinary way', () => {
  const withStore = { t: 'x', store: 'a' };
  toJSON(withStore);
  const retagged = tagStore(withStore, 'b');
  assert.deepEqual(JSON.parse(toJSON(retagged)), { t: 'x', store: 'b' });
  const fresh = tagStore({ t: 'pong' }, 'c');
  assert.deepEqual(JSON.parse(toJSON(fresh)), { t: 'pong', store: 'c' });
});

test('through a hub, every socket receives the same JSON for a broadcast, produced from one encoding of the payload', () => {
  const store = createStore({ initial: { tasks: {} } });
  const out = new Map();
  const hubs = ['s1', 's2', 's3'].map(socket => {
    out.set(socket, []);
    return createHub(() => store, { send: message => out.get(socket).push(toJSON(message)) });
  });
  for (const [i, hub] of hubs.entries()) hub.receive({ t: 'hello', store: 'main', replicaId: `r${i}`, ops: [] });
  const original = JSON.stringify;
  let payloads = 0;
  JSON.stringify = value => { if (value && typeof value === 'object' && value.t === 'patch') payloads++; return original(value); };
  try {
    store.patch({ tasks: { a: { id: 'a', title: 'to everyone' } } });
  } finally {
    JSON.stringify = original;
  }
  const patches = [...out.values()].map(lines => lines.map(JSON.parse).find(m => m.t === 'patch'));
  assert.equal(patches.length, 3);
  for (const patch of patches) assert.deepEqual(patch, { ...patches[0] });
  assert.deepEqual(patches[0], { t: 'patch', diff: { tasks: { a: { id: 'a', title: 'to everyone' } } }, ts: patches[0].ts, v: 1, store: 'main' });
  assert.equal(payloads, 1, 'three sockets, one encoding of the patch');
});

test('a snapshot is encoded from the cached state, spliced into the message, and decoded only when something reads it', () => {
  const original = JSON.stringify;
  let encodings = 0;
  const seen = [];
  JSON.stringify = value => { if (value && typeof value === 'object' && value.tasks) encodings++; seen.push(value); return original(value); };
  try {
    const store = createStore({ initial: { tasks: {} } });
    store.patch({ tasks: { a: { id: 'a', title: 'one' } } });
    const sent = [];
    const session = store.session({ send: m => sent.push(m) });
    session.receive({ t: 'hello', replicaId: 'r1', ops: [] });
    session.receive({ t: 'hello', replicaId: 'r2', ops: [] });
    const snapshots = sent.filter(m => m.t === 'snapshot');
    assert.equal(snapshots.length, 2);
    assert.equal(encodings, 1, 'two hellos on a quiet store: the state was encoded once');

    const tagged = tagStore(snapshots[1], 'main');
    const json = toJSON(tagged);
    assert.equal(encodings, 1, 'sending never re-encodes the state');
    assert.equal(seen.includes(snapshots[1]), false, 'nor the message');
    assert.deepEqual(JSON.parse(json), { t: 'snapshot', store: 'main', state: { tasks: { a: { id: 'a', title: 'one' } } }, ts: snapshots[1].ts, seq: 0, registers: [], v: 1, epoch: store.epoch });
    assert.deepEqual(snapshots[1].state, { tasks: { a: { id: 'a', title: 'one' } } }, 'reading the object decodes it');
    assert.equal(Object.getOwnPropertyDescriptor(tagged, 'state').get !== undefined, true, 'tagStore kept the getter');

    store.patch({ tasks: { a: { title: 'two' } } });
    session.receive({ t: 'hello', replicaId: 'r3', ops: [] });
    assert.equal(encodings, 2, 'an op drops the cache; the next hello encodes again');
    assert.equal(sent.at(-1).state.tasks.a.title, 'two');
    assert.deepEqual(snapshots[1].state.tasks.a.title, 'one', 'an earlier message keeps the state it carried');
  } finally {
    JSON.stringify = original;
  }
});
