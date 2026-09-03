import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createClock, compareTs } from '../src/core/hlc.js';
import { fakeTime } from './helpers.js';

test('local timestamps are strictly increasing, within one millisecond too', () => {
  const now = fakeTime(100);
  const clock = createClock('a', now);
  const a = clock.now();
  const b = clock.now();
  now.advance(1);
  const c = clock.now();
  assert.ok(compareTs(a, b) < 0 && compareTs(b, c) < 0);
  assert.deepEqual(a, [100, 0, 'a']);
  assert.deepEqual(b, [100, 1, 'a']);
  assert.deepEqual(c, [101, 0, 'a']);
});

test('receiving a timestamp from a fast clock pulls the local clock forward', () => {
  const clock = createClock('slow', fakeTime(100));
  clock.receive([5000, 3, 'fast']);
  const next = clock.now();
  assert.ok(compareTs(next, [5000, 3, 'fast']) > 0, 'later local events sort after what was seen');
  assert.equal(next[0], 5000);
});

test('replica id breaks ties so no two replicas produce equal timestamps', () => {
  assert.ok(compareTs([1, 0, 'a'], [1, 0, 'b']) < 0);
  assert.ok(compareTs([1, 0, 'b'], [1, 0, 'a']) > 0);
  assert.equal(compareTs([1, 0, 'a'], [1, 0, 'a']), 0);
  assert.ok(compareTs(undefined, [1, 0, 'a']) < 0, 'a missing timestamp sorts first');
});

test('a replica id is required', () => {
  assert.throws(() => createClock(''), TypeError);
});
