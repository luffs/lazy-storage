import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mergeOp, compactTombstones } from '../src/core/merge.js';
import { leaves, expandRegisters, ModelError } from '../src/core/model.js';
import { registerSet } from '../src/core/paths.js';

const T = (ms, id = 'x') => [ms, 0, id];
const NONE = registerSet([]);

test('leaves flattens objects, keeps null as a deletion, takes primitive arrays whole, and rejects arrays of records outside registers', () => {
  assert.deepEqual(
    leaves({ tasks: { a: { title: 'A', done: false }, b: null }, n: 1 }, NONE),
    [[['tasks', 'a', 'title'], 'A'], [['tasks', 'a', 'done'], false], [['tasks', 'b'], null], [['n'], 1]]
  );
  assert.deepEqual(leaves({ tags: ['x'] }, NONE), [[['tags'], ['x']]], 'an array of primitives is one leaf anywhere');
  assert.throws(() => leaves({ tags: [{ id: 'x' }] }, NONE), ModelError);
  assert.throws(() => leaves({ tags: { 0: 'x', $length: 1 } }, NONE), ModelError);
  const regs = registerSet(['order']);
  assert.deepEqual(leaves({ order: ['b', 'a'] }, regs), [[['order'], ['b', 'a']]]);
  assert.throws(() => leaves({ order: { 1: 'a', $length: 2 } }, regs), ModelError, 'a register must travel whole');
});

test('register patterns may use * for one segment, declaring a register per record', () => {
  const regs = registerSet(['tasks/*/subtaskOrder', 'order']);
  assert.equal(regs.matches(['tasks', 'a', 'subtaskOrder']), true);
  assert.equal(regs.matches(['tasks', 'subtaskOrder']), false, 'the depth must match');
  assert.equal(regs.matches(['tasks', 'a', 'title']), false);
  assert.equal(regs.matches(['order']), true);
  assert.deepEqual(
    leaves({ tasks: { a: { subtaskOrder: ['s2', 's1'], title: 'A' } }, order: ['a'] }, regs),
    [[['tasks', 'a', 'subtaskOrder'], ['s2', 's1']], [['tasks', 'a', 'title'], 'A'], [['order'], ['a']]]
  );
  assert.throws(() => leaves({ tasks: { a: { subtasks: [{ id: 'x' }] } } }, regs), ModelError, 'an array of records at a non-matching path is refused');
  const state = { tasks: { a: { subtaskOrder: ['s1', 's2', 's3'] } } };
  assert.deepEqual(
    expandRegisters({ tasks: { a: { subtaskOrder: { 2: 's3', $length: 3 }, title: 'A' } } }, regs, state),
    { tasks: { a: { subtaskOrder: ['s1', 's2', 's3'], title: 'A' } } }
  );
  assert.throws(() => registerSet(['tasks//x']), TypeError);
});

test('expandRegisters replaces a register fragment with the live whole value', () => {
  const regs = registerSet(['order']);
  const state = { order: ['a', 'b', 'c'], tasks: {} };
  assert.deepEqual(expandRegisters({ order: { 2: 'c', $length: 3 }, tasks: { a: { done: true } } }, regs, state),
    { order: ['a', 'b', 'c'], tasks: { a: { done: true } } });
  assert.deepEqual(expandRegisters({ order: null }, regs, { tasks: {} }), { order: null }, 'a deleted register stays a deletion');
});

test('a newer write wins per leaf and an older one loses, whatever the arrival order', () => {
  const clocks = new Map();
  let r = mergeOp(clocks, T(20), { tasks: { a: { title: 'newer', done: true } } }, NONE);
  assert.deepEqual(r.accepted, { tasks: { a: { title: 'newer', done: true } } });
  r = mergeOp(clocks, T(10), { tasks: { a: { title: 'older', priority: 2 } } }, NONE);
  assert.deepEqual(r.accepted, { tasks: { a: { priority: 2 } } }, 'the untouched leaf still lands');
  assert.deepEqual(r.rejected, [['tasks', 'a', 'title']]);
});

test('a deletion wins over older writes below it and loses to a newer one', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { tasks: { a: { title: 'A', done: false } } }, NONE);
  let r = mergeOp(clocks, T(20), { tasks: { a: null } }, NONE);
  assert.deepEqual(r.accepted, { tasks: { a: null } });
  // An older field edit arriving late does not resurrect a partial record
  r = mergeOp(clocks, T(15), { tasks: { a: { done: true } } }, NONE);
  assert.equal(r.accepted, null);
  // Nor does a NEWER field edit: a deleted record's field must not come back alone
  r = mergeOp(clocks, T(25), { tasks: { a: { done: true } } }, NONE);
  assert.equal(r.accepted, null);
  // A newer record write (an object with an id) re-adds it wholesale
  r = mergeOp(clocks, T(30), { tasks: { a: { id: 'a', title: 'A again', done: false } } }, NONE);
  assert.deepEqual(r.accepted, { tasks: { a: { id: 'a', title: 'A again', done: false } } });
  // And now a deletion older than that record loses
  r = mergeOp(clocks, T(28), { tasks: { a: null } }, NONE);
  assert.equal(r.accepted, null);
});

test('a deletion older than a field below it loses (the edit is newer)', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { tasks: { a: { title: 'A' } } }, NONE);
  mergeOp(clocks, T(30), { tasks: { a: { done: true } } }, NONE);
  const r = mergeOp(clocks, T(20), { tasks: { a: null } }, NONE);
  assert.equal(r.accepted, null);
  assert.deepEqual(r.rejected, [['tasks', 'a']]);
});

test('registers are one unit: the newest whole value wins', () => {
  const regs = registerSet(['order']);
  const clocks = new Map();
  mergeOp(clocks, T(20), { order: ['b', 'a'] }, regs);
  const r = mergeOp(clocks, T(10), { order: ['a', 'b'] }, regs);
  assert.equal(r.accepted, null);
  assert.deepEqual(mergeOp(clocks, T(30), { order: ['c'] }, regs).accepted, { order: ['c'] });
});

test('turning a container into a leaf drops the entries beneath it', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { cfg: { theme: 'dark', size: 3 } }, NONE);
  mergeOp(clocks, T(20), { cfg: 'off' }, NONE);
  assert.equal([...clocks.keys()].filter(k => k.startsWith('["cfg",')).length, 0);
  const r = mergeOp(clocks, T(15), { cfg: { theme: 'light' } }, NONE);
  assert.deepEqual(r.accepted, { cfg: { theme: 'light' } }, 'no entry blocks a fresh write below the leaf (it is newer than nothing)');
});

test('compactTombstones forgets old tombstones and keeps live entries', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { a: 1, b: 2 }, NONE);
  mergeOp(clocks, T(20), { a: null }, NONE);
  assert.deepEqual(compactTombstones(clocks, T(15)), [], 'the tombstone is newer than the cutoff');
  assert.deepEqual(compactTombstones(clocks, T(25)), ['["a"]']);
  assert.deepEqual([...clocks.keys()], ['["b"]']);
});

test('mergeOp reports the leaves it won and the entries it dropped, for row-oriented storage', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { tasks: { a: { title: 'A', done: false, sub: { x: 1 } } } }, NONE);
  // Deleting the record drops its three leaf entries and sets one tombstone
  let r = mergeOp(clocks, T(20), { tasks: { a: null } }, NONE);
  assert.deepEqual(r.won, [[['tasks', 'a'], null]]);
  assert.deepEqual(r.dropped.sort(), ['["tasks","a","done"]', '["tasks","a","sub","x"]', '["tasks","a","title"]']);
  // Re-adding lifts the tombstone (dropped) and wins the new leaves
  r = mergeOp(clocks, T(30), { tasks: { a: { id: 'a', title: 'B' } } }, NONE);
  assert.deepEqual(r.dropped, ['["tasks","a"]']);
  assert.deepEqual(r.won, [[['tasks', 'a', 'id'], 'a'], [['tasks', 'a', 'title'], 'B']]);
  // A losing op wins nothing and drops nothing
  r = mergeOp(clocks, T(5), { tasks: { a: { title: 'old' } } }, NONE);
  assert.deepEqual(r.won, []);
  assert.deepEqual(r.dropped, []);
});
