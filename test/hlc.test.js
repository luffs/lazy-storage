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

test('rewind falls back to the wall clock after it was corrected, but never behind what was received', () => {
  const now = fakeTime(10_000);
  const clock = createClock('a', now);
  clock.receive([9_000, 4, 'server']);
  const ahead = clock.now();                       // stamped while an hour ahead
  assert.equal(ahead[0], 10_000);
  now.set(5_000);                                  // the clock is corrected backwards
  clock.rewind();
  const after = clock.now();
  assert.deepEqual(after, [9_000, 6, 'a'], 'not behind the newest received stamp, and after it');
  assert.ok(compareTs(after, [9_000, 4, 'server']) > 0);

  // Nothing received: a rewind goes straight to the wall clock
  const wall = fakeTime(10_000);
  const alone = createClock('c', wall);
  alone.now();
  wall.set(2_000);
  alone.rewind();
  assert.deepEqual(alone.now(), [2_000, 1, 'c']);
  alone.rewind();
  assert.deepEqual(alone.peek(), [2_000, 1, 'c'], 'a rewind to where the clock already is changes nothing');
});
