// hostile.test.js - What a client that does not play by the protocol can
// and cannot do to a store: every session is authenticated, but nothing it
// sends is taken on trust
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { leaves, rebuild, ModelError } from '../src/core/model.js';
import { registerSet, setAt } from '../src/core/paths.js';

/** A raw session on the store, speaking the protocol by hand */
function rawSession(store, replicaId = 'r1', user = { id: 'u' }) {
  const sent = [];
  const session = store.session({ send: m => sent.push(m), user });
  session.receive({ t: 'hello', replicaId, ops: [] });
  return { session, sent, last: () => sent.at(-1) };
}

test('a reserved name in an op is refused and never reaches Object.prototype', () => {
  const store = createStore({ initial: { tasks: {} }, registers: ['meta'] });
  const { session, last } = rawSession(store);
  const hostile = [
    '{"__proto__":{"polluted":"yes"}}',
    '{"tasks":{"__proto__":{"polluted":"yes"}}}',
    '{"tasks":{"x":{"constructor":{"prototype":{"polluted":"yes"}}}}}',
    '{"meta":{"__proto__":{"polluted":"yes"}}}',
    '{"tasks":{"x":{"$length":3}}}'
  ];
  hostile.forEach((json, i) => {
    session.receive({ t: 'op', op: { replicaId: 'r1', seq: i + 1, ts: [Date.now(), 0, 'r1'], diff: JSON.parse(json) } });
    assert.equal(last().t, 'error', json);
    assert.equal(last().code, 'invalid', json);
  });
  assert.equal(({}).polluted, undefined);
  assert.deepEqual(store.state, { tasks: {} });
  store.dispose();
});

test('leaves, setAt and rebuild refuse the prototype machinery as a path segment', () => {
  const NONE = registerSet([]);
  assert.throws(() => leaves(JSON.parse('{"a":{"__proto__":{"b":1}}}'), NONE), ModelError);
  assert.throws(() => leaves({ a: { prototype: 1 } }, NONE), ModelError);
  assert.throws(() => leaves({ tags: JSON.parse('[{"__proto__":{"b":1}}]') }, registerSet(['tags'])), ModelError);
  assert.throws(() => setAt({}, ['__proto__', 'polluted'], 'yes'), TypeError);
  assert.throws(() => rebuild({}, [['["__proto__","polluted"]', 'yes']]), TypeError);
  assert.equal(({}).polluted, undefined);
});
