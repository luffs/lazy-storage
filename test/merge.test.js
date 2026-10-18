import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mergeOp, compactTombstones, settleBarriers } from '../src/core/merge.js';
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

test('the authority is never held back by a tombstone: an object it writes at a deleted path re-adds it, id or not', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { procs: { web: { state: 'online', pid: 1 } } }, NONE);
  mergeOp(clocks, T(20), { procs: { web: null } }, NONE);
  // A replica's newer field write stays refused: a deleted record must not come back as one field
  assert.equal(mergeOp(clocks, T(25), { procs: { web: { state: 'online' } } }, NONE).accepted, null);
  // The authority's lands, the tombstone lifted into a barrier, no id needed, stamped with the version its patch becomes
  let r = mergeOp(clocks, T(30), { procs: { web: { state: 'stopped', pid: 2 } } }, NONE, { authority: true, v: 7 });
  assert.deepEqual(r.accepted, { procs: { web: { state: 'stopped', pid: 2 } } });
  assert.deepEqual(r.barriers, ['["procs","web"]']);
  assert.deepEqual(clocks.get('["procs","web"]'), { ts: T(30), made: true, since: T(20), v: 7 });
  // The record is live again: a replica that has applied the re-add edits it on timestamps alone
  assert.deepEqual(mergeOp(clocks, T(35), { procs: { web: { state: 'online' } } }, NONE, { seen: 7 }).accepted, { procs: { web: { state: 'online' } } });
  // Only the id requirement is waived, not the clock: an authority write older than the deletion still loses
  mergeOp(clocks, T(40), { procs: { web: null } }, NONE);
  r = mergeOp(clocks, T(38), { procs: { web: { state: 'online' } } }, NONE, { authority: true, v: 9 });
  assert.equal(r.accepted, null);
  assert.deepEqual(r.dropped, []);
  assert.deepEqual(r.barriers, []);
  // No barrier holds the authority back either, whatever it had seen
  mergeOp(clocks, T(50), { procs: { web: { state: 'up', pid: 3 } } }, NONE, { authority: true, v: 10 });
  assert.deepEqual(mergeOp(clocks, T(55), { procs: { web: { state: 'down' } } }, NONE, { authority: true, v: 11 }).accepted, { procs: { web: { state: 'down' } } });
});

test('compactTombstones forgets old tombstones and keeps live entries; settleBarriers turns old barriers into containers written empty', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { a: 1, b: 2, c: { x: 1 } }, NONE);
  mergeOp(clocks, T(20), { a: null, c: null }, NONE);
  mergeOp(clocks, T(22), { c: { x: 2 } }, NONE, { made: [['c']], v: 3 });
  assert.deepEqual(compactTombstones(clocks, T(15)), [], 'the tombstone is newer than the cutoff');
  assert.deepEqual(settleBarriers(clocks, T(15)), [], 'so is the barrier');
  assert.deepEqual(compactTombstones(clocks, T(25)), ['["a"]']);
  assert.deepEqual(clocks.get('["c"]'), { ts: T(22), made: true, since: T(20), v: 3 }, 'a barrier is no tombstone');
  assert.deepEqual(settleBarriers(clocks, T(25)), ['["c"]']);
  assert.deepEqual(clocks.get('["c"]'), { ts: T(22) }, 'the entry of a container written empty at the lift');
  assert.deepEqual([...clocks.keys()].sort(), ['["b"]', '["c","x"]', '["c"]']);
  // Settled, the container is edited on timestamps alone, whatever the writer had seen
  assert.deepEqual(mergeOp(clocks, T(30), { c: { y: 1 } }, NONE).accepted, { c: { y: 1 } });
});

test('mergeOp reports the leaves it won and the entries it dropped, for row-oriented storage', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { tasks: { a: { title: 'A', done: false, sub: { x: 1 } } } }, NONE);
  // Deleting the record drops its three leaf entries and sets one tombstone
  let r = mergeOp(clocks, T(20), { tasks: { a: null } }, NONE);
  assert.deepEqual(r.won, [[['tasks', 'a'], null]]);
  assert.deepEqual(r.dropped.sort(), ['["tasks","a","done"]', '["tasks","a","sub","x"]', '["tasks","a","title"]']);
  // Re-adding lifts the tombstone into a barrier (a row of its own) and wins the new leaves
  r = mergeOp(clocks, T(30), { tasks: { a: { id: 'a', title: 'B' } } }, NONE);
  assert.deepEqual(r.dropped, []);
  assert.deepEqual(r.barriers, ['["tasks","a"]']);
  assert.deepEqual(r.won, [[['tasks', 'a', 'id'], 'a'], [['tasks', 'a', 'title'], 'B']]);
  // A losing op wins nothing and drops nothing
  r = mergeOp(clocks, T(5), { tasks: { a: { title: 'old' } } }, NONE);
  assert.deepEqual(r.won, []);
  assert.deepEqual(r.dropped, []);
  assert.deepEqual(r.barriers, []);
});

test('a container its author made is re-added at a deleted path; beneath it, only what was made in sight of the re-add lands', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { state: { lists: { turns: { t1: { text: 'x' } } } } }, NONE);
  mergeOp(clocks, T(20), { state: { lists: null } }, NONE);
  // A newer write under the tombstone is refused as ever: nothing says it is not a stale edit
  assert.equal(mergeOp(clocks, T(25), { state: { lists: { turns: { t2: { text: 'y' } } } } }, NONE).accepted, null);
  // Said to be made, it lands, and the tombstone becomes a barrier: the op's timestamp, the deletion's, the version the op becomes
  let r = mergeOp(clocks, T(25), { state: { lists: { turns: { t2: { text: 'y' } } } } }, NONE, { made: [['state', 'lists']], v: 4 });
  assert.deepEqual(r.accepted, { state: { lists: { turns: { t2: { text: 'y' } } } } });
  assert.deepEqual(r.barriers, ['["state","lists"]']);
  assert.deepEqual(clocks.get('["state","lists"]'), { ts: T(25), made: true, since: T(20), v: 4 });
  // An edit made without having applied the re-add is an edit of the deleted container, however late it was
  // made: a write, or a deletion, from a replica at version 3 or none
  assert.equal(mergeOp(clocks, T(22), { state: { lists: { turns: { t2: { color: 'red' } } } } }, NONE, { seen: 3 }).accepted, null);
  assert.equal(mergeOp(clocks, T(90), { state: { lists: { turns: { t2: { color: 'red' } } } } }, NONE).accepted, null, 'a later timestamp says nothing of what was seen');
  assert.equal(mergeOp(clocks, T(90), { state: { lists: { turns: { t2: null } } } }, NONE, { seen: 3 }).accepted, null);
  // Made in sight of the re-add, an edit is judged on its timestamp alone
  assert.deepEqual(mergeOp(clocks, T(30), { state: { lists: { turns: { t2: { color: 'red' } } } } }, NONE, { seen: 4 }).accepted, { state: { lists: { turns: { t2: { color: 'red' } } } } });
  assert.equal(mergeOp(clocks, T(24), { state: { lists: { turns: { t2: { text: 'older' } } } } }, NONE, { seen: 9 }).accepted, null, 'older than what is there');
  // Made at a path, the author had nothing under it either: a tombstone below is lifted too
  mergeOp(clocks, T(40), { state: { lists: { turns: null } } }, NONE, { seen: 4 });
  r = mergeOp(clocks, T(45), { state: { lists: { turns: { t3: { text: 'z' } } } } }, NONE, { made: [['state']], seen: 5, v: 6 });
  assert.deepEqual(r.accepted, { state: { lists: { turns: { t3: { text: 'z' } } } } });
  assert.deepEqual(r.barriers, ['["state","lists","turns"]']);
  // A tombstone newer than the op stays, made or not; what is not a path is ignored
  mergeOp(clocks, T(60), { state: { lists: null } }, NONE, { seen: 6 });
  r = mergeOp(clocks, T(55), { state: { lists: { turns: { t4: { text: 'w' } } } } }, NONE, { made: [['state', 'lists'], 'junk', [1], null], seen: 6, v: 8 });
  assert.equal(r.accepted, null);
  assert.deepEqual(r.barriers, []);
});

test('a re-added record leaves a barrier too: an edit from a replica that had not seen the re-add is refused', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { tasks: { a: { id: 'a', title: 'A' } } }, NONE);
  mergeOp(clocks, T(20), { tasks: { a: null } }, NONE);
  assert.deepEqual(mergeOp(clocks, T(30), { tasks: { a: { id: 'a', title: 'B' } } }, NONE, { v: 3 }).barriers, ['["tasks","a"]']);
  // A field of the old record, written by a replica that had not heard of the deletion: not part of the new one
  assert.equal(mergeOp(clocks, T(15), { tasks: { a: { color: 'red' } } }, NONE, { seen: 1 }).accepted, null);
  assert.equal(mergeOp(clocks, T(35), { tasks: { a: { color: 'red' } } }, NONE, { seen: 2 }).accepted, null);
  assert.deepEqual(mergeOp(clocks, T(35), { tasks: { a: { color: 'red' } } }, NONE, { seen: 3 }).accepted, { tasks: { a: { color: 'red' } } });
  // The barrier's own path is written as any other by a replica that saw it: a newer deletion takes it
  assert.deepEqual(mergeOp(clocks, T(40), { tasks: { a: null } }, NONE, { seen: 3 }).accepted, { tasks: { a: null } });
  assert.deepEqual(clocks.get('["tasks","a"]'), { ts: T(40), deleted: true });
});

test('nothing is lifted beneath a tombstone that stays, and a re-add older than the deletion lifts nothing', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { a: { b: { c: { x: 1 } } } }, NONE);
  mergeOp(clocks, T(20), { a: { b: { c: null } } }, NONE);
  mergeOp(clocks, T(30), { a: { b: null } }, NONE);
  // The op makes `c` under a `b` deleted after it: `b` stays, so nothing is lifted or set beneath it, and the write is refused whole
  let r = mergeOp(clocks, T(25), { a: { b: { c: { x: 2 } } } }, NONE, { made: [['a', 'b', 'c']], v: 4 });
  assert.equal(r.accepted, null);
  assert.deepEqual(r.barriers, []);
  assert.deepEqual([...clocks].sort(), [['["a","b"]', { ts: T(30), deleted: true }]], 'the tombstone, and nothing under it');
  // Re-added and deleted again, twice: a re-add older than the last deletion lifts nothing, however many lifts came before
  mergeOp(clocks, T(31), { a: { b: { id: 'b', n: 1 } } }, NONE, { v: 5 });
  mergeOp(clocks, T(32), { a: { b: null } }, NONE, { seen: 5 });
  mergeOp(clocks, T(33), { a: { b: { id: 'b', n: 2 } } }, NONE, { v: 7 });
  mergeOp(clocks, T(34), { a: { b: null } }, NONE, { seen: 7 });
  r = mergeOp(clocks, T(33, 'q'), { a: { b: { id: 'b', n: 2 } } }, NONE, { v: 9 });
  assert.equal(r.accepted, null, 'older than the deletion at every leaf');
  assert.deepEqual(r.barriers, []);
  assert.deepEqual(clocks.get('["a","b"]'), { ts: T(34), deleted: true });
});

test('an empty object re-adding a container leaves the barrier standing: the container is there, as it says', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { settings: { theme: 'dark' } }, NONE);
  mergeOp(clocks, T(20), { settings: null }, NONE);
  // The usual way to re-create one: `settings ??= {}`, filled in later
  let r = mergeOp(clocks, T(25), { settings: {} }, NONE, { made: [['settings']], v: 3 });
  assert.deepEqual(r.accepted, { settings: {} }, 'the ensure lands: the container exists from here');
  assert.deepEqual(r.barriers, ['["settings"]'], 'and the row at that path is the barrier\'s');
  assert.deepEqual(clocks.get('["settings"]'), { ts: T(25), made: true, since: T(20), v: 3 });
  // Filled in later by a replica that saw it; an edit of the old container from one that had not, still refused
  assert.deepEqual(mergeOp(clocks, T(30), { settings: { theme: 'light' } }, NONE, { seen: 3 }).accepted, { settings: { theme: 'light' } });
  assert.equal(mergeOp(clocks, T(31), { settings: { size: 2 } }, NONE, { seen: 2 }).accepted, null);
  // A later ensure at the barrier, newer or older, seen or not: nothing to apply, the barrier stays
  r = mergeOp(clocks, T(35), { settings: {} }, NONE, { seen: 3 });
  assert.deepEqual(r.accepted, { settings: {} });
  assert.deepEqual(r.barriers, ['["settings"]']);
  r = mergeOp(clocks, T(23), { settings: {} }, NONE);
  assert.deepEqual(r.accepted, { settings: {} });
  assert.deepEqual(r.barriers, ['["settings"]']);
  assert.deepEqual(clocks.get('["settings"]'), { ts: T(25), made: true, since: T(20), v: 3 });
});

test('two replicas re-adding the same deleted container both land, whichever arrives first, and the barrier is the first to arrive', () => {
  const ops = [
    [T(30, 'a'), { lists: { turns: { t2: { text: 'from a' } } } }],
    [T(32, 'b'), { lists: { turns: { t3: { text: 'from b' } } } }]
  ];
  const outcomes = [[0, 1], [1, 0]].map(order => {
    const clocks = new Map();
    mergeOp(clocks, T(10), { lists: { turns: { t1: { text: 'x' } } } }, NONE);
    mergeOp(clocks, T(20), { lists: null }, NONE);
    const accepted = order.map((i, n) => mergeOp(clocks, ops[i][0], ops[i][1], NONE, { made: [['lists']], v: 3 + n }).accepted);
    assert.ok(accepted.every(Boolean), `both land, arriving ${order.join(' then ')}`);
    assert.deepEqual(clocks.get('["lists"]'), { ts: ops[order[0]][0], made: true, since: T(20), v: 3 }, 'the barrier is the first re-add');
    return clocks;
  });
  // A re-add older than the deletion loses to it, lift or not: the deletion was the newer
  const clocks = outcomes[0];
  assert.equal(mergeOp(clocks, T(15, 'c'), { lists: { turns: { t4: { text: 'stale' } } } }, NONE, { made: [['lists']], v: 5 }).accepted, null);
  // A plain write by a replica that saw neither re-add is not a re-add: refused, whatever its timestamp
  assert.equal(mergeOp(clocks, T(25, 'c'), { lists: { turns: { t2: { color: 'red' } } } }, NONE, { seen: 2 }).accepted, null);
  assert.equal(mergeOp(clocks, T(45, 'c'), { lists: { turns: { t2: { color: 'red' } } } }, NONE, { seen: 2 }).accepted, null);
  // Having seen the first re-add is enough: the barrier is its version
  assert.deepEqual(mergeOp(clocks, T(45, 'c'), { lists: { turns: { t2: { color: 'red' } } } }, NONE, { seen: 3 }).accepted, { lists: { turns: { t2: { color: 'red' } } } });
});

test('a deletion refused under a barrier is moot when nothing is there: refused by nothing, and nothing lost', () => {
  const clocks = new Map();
  mergeOp(clocks, T(10), { lists: { turns: { t1: { text: 'x' }, t2: { text: 'y' } } } }, NONE);
  mergeOp(clocks, T(20), { lists: null }, NONE);
  mergeOp(clocks, T(30), { lists: { turns: { t2: { text: 'z' } } } }, NONE, { made: [['lists']], v: 3 });
  // t1 is gone already: the deletion's intent holds, and the replica hears nothing of it
  let r = mergeOp(clocks, T(15), { lists: { turns: { t1: null } } }, NONE, { seen: 2 });
  assert.equal(r.accepted, null);
  assert.deepEqual(r.rejected, []);
  assert.deepEqual(r.won, []);
  // t2 is there again, unseen by the replica: the deletion lost, and the replica is told
  r = mergeOp(clocks, T(35), { lists: { turns: { t2: null } } }, NONE, { seen: 2 });
  assert.deepEqual(r.rejected, [['lists', 'turns', 't2']]);
});
