// clocks.test.js - The clock table's children index agrees with a scan, through every kind of change
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ClockMap } from '../src/core/clocks.js';
import { mergeOp, compactTombstones } from '../src/core/merge.js';
import { pathKey, registerSet, descendantPrefix } from '../src/core/paths.js';
import { seededRandom } from './helpers.js';

const T = (ms, id = 'x') => [ms, 0, id];
// Every entry is under the root; below it, keys share the ancestor's prefix
const scan = (map, path) => (path.length ? [...map.keys()].filter(k => k.startsWith(descendantPrefix(path))) : [...map.keys()]).sort();
const indexed = (map, path) => [...map.descendants(path)].sort();

test('descendants come from the index and match a scan, as entries are set, deleted, and re-added', () => {
  const clocks = new ClockMap([[pathKey(['tasks', 'a', 'title']), { ts: T(1) }]]);
  clocks.set(pathKey(['tasks', 'a', 'sub', 's1', 'done']), { ts: T(2) });
  clocks.set(pathKey(['tasks', 'b']), { ts: T(3), deleted: true });
  clocks.set(pathKey(['order']), { ts: T(4) });
  for (const path of [[], ['tasks'], ['tasks', 'a'], ['tasks', 'a', 'sub'], ['tasks', 'b'], ['order'], ['nothing']]) {
    assert.deepEqual(indexed(clocks, path), scan(clocks, path), path.join('/'));
  }
  assert.deepEqual(indexed(clocks, ['tasks', 'a']), [pathKey(['tasks', 'a', 'sub', 's1', 'done']), pathKey(['tasks', 'a', 'title'])]);

  clocks.delete(pathKey(['tasks', 'a', 'sub', 's1', 'done']));
  assert.deepEqual(indexed(clocks, ['tasks', 'a']), [pathKey(['tasks', 'a', 'title'])], 'an unlinked branch is gone from the index');
  assert.deepEqual(indexed(clocks, ['tasks', 'a', 'sub']), []);
  clocks.delete(pathKey(['tasks', 'a', 'title']));
  assert.deepEqual(indexed(clocks, ['tasks']), [pathKey(['tasks', 'b'])]);
  clocks.set(pathKey(['tasks', 'a', 'title']), { ts: T(5) });
  assert.deepEqual(indexed(clocks, ['tasks']), [pathKey(['tasks', 'a', 'title']), pathKey(['tasks', 'b'])].sort());
  assert.equal(clocks.delete('missing'), false);
  clocks.clear();
  assert.deepEqual(indexed(clocks, []), []);
  assert.equal(clocks.size, 0);
});

test('a container entry and its descendants coexist in the index, and deleting the container keeps the children reachable', () => {
  const clocks = new ClockMap();
  clocks.set(pathKey(['tasks', 'a']), { ts: T(1), deleted: true });   // a tombstone at the container
  clocks.set(pathKey(['tasks', 'a', 'x']), { ts: T(2) });             // re-added under it (as the merge does after lifting)
  assert.deepEqual(indexed(clocks, ['tasks']), [pathKey(['tasks', 'a']), pathKey(['tasks', 'a', 'x'])].sort());
  clocks.delete(pathKey(['tasks', 'a']));
  assert.deepEqual(indexed(clocks, ['tasks']), [pathKey(['tasks', 'a', 'x'])]);
  assert.deepEqual(indexed(clocks, ['tasks', 'a']), [pathKey(['tasks', 'a', 'x'])]);
});

test('the merge decides the same with an indexed table as with a plain map, over random ops', () => {
  const regs = registerSet(['order']);
  const rng = seededRandom(7);
  const plain = new Map();
  const fast = new ClockMap();
  const ids = ['a', 'b', 'c', 'd'];
  const fields = ['title', 'done', 'n'];
  let t = 0;
  for (let step = 0; step < 2000; step++) {
    const ts = T(++t, rng.pick(['r1', 'r2']));
    const id = rng.pick(ids);
    const roll = rng.next();
    let diff;
    if (roll < 0.3) diff = { tasks: { [id]: { id, [rng.pick(fields)]: step } } };
    else if (roll < 0.5) diff = { tasks: { [id]: { [rng.pick(fields)]: step } } };
    else if (roll < 0.65) diff = { tasks: { [id]: null } };
    else if (roll < 0.75) diff = { tasks: { [id]: { sub: { [rng.pick(ids)]: { done: true } } } } };
    else if (roll < 0.85) diff = { tasks: { [id]: { sub: { [rng.pick(ids)]: null } } } };
    else if (roll < 0.95) diff = { tasks: { [id]: { sub: {} } } };
    else diff = { order: [id] };
    const a = mergeOp(plain, ts, structuredClone(diff), regs);
    const b = mergeOp(fast, ts, structuredClone(diff), regs);
    assert.deepEqual({ ...b, dropped: [...b.dropped].sort() }, { ...a, dropped: [...a.dropped].sort() }, `step ${step}`);
    if (step % 97 === 0) {
      const olderThan = T(t - 50, '');
      assert.deepEqual(compactTombstones(fast, olderThan).sort(), compactTombstones(plain, olderThan).sort());
    }
    assert.deepEqual([...fast].sort(), [...plain].sort());
  }
  for (const path of [['tasks'], ['tasks', 'a'], ['tasks', 'b', 'sub'], []]) assert.deepEqual(indexed(fast, path), scan(fast, path));
});
