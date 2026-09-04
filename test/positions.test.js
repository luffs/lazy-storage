// positions.test.js - Position keys: always a key between two, compact at the ends, valid and ordered under random use
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { keyBetween, keysBetween, isPositionKey, comparePositions } from '../src/core/positions.js';
import { seededRandom } from './helpers.js';

test('the first key, counting up at the end, counting down at the front, and bisecting between', () => {
  assert.equal(keyBetween(null, null), 'a0');
  assert.equal(keyBetween('a0', null), 'a1');
  assert.equal(keyBetween('az', null), 'b00', 'a longer integer when the digits run out');
  assert.equal(keyBetween('b00', null), 'b01');
  assert.equal(keyBetween(null, 'a0'), 'Zz', 'negative integers count down');
  assert.equal(keyBetween(null, 'Zz'), 'Zy');
  assert.equal(keyBetween(null, 'Z0'), 'Yzz');
  assert.equal(keyBetween('a0', 'a1'), 'a0V', 'a fraction between two adjacent integers');
  assert.equal(keyBetween('a0', 'a0V'), 'a0G');
  assert.equal(keyBetween('a0V', 'a1'), 'a0l');
  assert.equal(keyBetween('a0', 'a2'), 'a1', 'an integer fits between when there is room');
  assert.equal(keyBetween('a0V', null), 'a1', 'after a fraction, the next integer');
  assert.equal(keyBetween(null, 'a0V'), 'a0', 'before a fraction, its integer');
  assert.throws(() => keyBetween('a1', 'a0'), /not below/);
  assert.throws(() => keyBetween('a0', 'a0'), /not below/);
});

test('validation: digits only, a sane integer part, no trailing zero in the fraction', () => {
  for (const ok of ['a0', 'Zz', 'a0V', 'b00', 'A' + '0'.repeat(26) + 'V']) assert.equal(isPositionKey(ok), true, ok);
  for (const bad of ['', 'a', 'a0.', 'a00', 'a0V0', '0', '-1', 'A' + '0'.repeat(26), 42, null]) assert.equal(isPositionKey(bad), false, String(bad));
  assert.throws(() => keyBetween('a00', null), /trailing|ends in 0/);
});

test('keysBetween hands out n ordered keys, compact at an open end and bisected between bounds', () => {
  const tail = keysBetween('a5', null, 4);
  assert.deepEqual(tail, ['a6', 'a7', 'a8', 'a9']);
  const head = keysBetween(null, 'a0', 3);
  assert.deepEqual(head, ['Zx', 'Zy', 'Zz']);
  const first = keysBetween(null, null, 3);
  assert.deepEqual(first, ['a0', 'a1', 'a2']);
  const between = keysBetween('a0', 'a1', 5);
  for (let i = 1; i < between.length; i++) assert.ok(between[i - 1] < between[i]);
  assert.ok('a0' < between[0] && between.at(-1) < 'a1');
  assert.deepEqual(keysBetween('a0', 'a1', 0), []);
});

test('random inserts keep the list strictly ordered with valid keys, and keys stay short where they should', () => {
  const rng = seededRandom(3);
  const keys = [];
  for (let step = 0; step < 3000; step++) {
    const at = rng.int(keys.length + 1);
    const key = keyBetween(keys[at - 1] ?? null, keys[at] ?? null);
    assert.ok(isPositionKey(key), key);
    keys.splice(at, 0, key);
  }
  for (let i = 1; i < keys.length; i++) assert.ok(keys[i - 1] < keys[i], `${keys[i - 1]} < ${keys[i]}`);
  assert.equal(new Set(keys).size, keys.length);

  const appended = [];
  let last = null;
  for (let i = 0; i < 5000; i++) appended.push(last = keyBetween(last, null));
  assert.ok(Math.max(...appended.map(k => k.length)) <= 4, 'five thousand appends fit in four characters');
  const prepended = [];
  let first = null;
  for (let i = 0; i < 5000; i++) prepended.push(first = keyBetween(null, first));
  assert.ok(Math.max(...prepended.map(k => k.length)) <= 4);

  let a = 'a0';
  let b = 'a1';
  const grown = [];
  for (let i = 0; i < 30; i++) { const key = keyBetween(a, b); grown.push(key); b = key; }   // always at the very same spot
  assert.ok(grown.at(-1).length <= 12, `thirty inserts at one spot: ${grown.at(-1).length} characters`);
});

test('comparePositions orders by key, then id, with unkeyed records last', () => {
  assert.ok(comparePositions('a0', 'x', 'a1', 'a') < 0);
  assert.ok(comparePositions('a1', 'a', 'a0', 'x') > 0);
  assert.ok(comparePositions('a0', 'a', 'a0', 'b') < 0, 'a tie falls to the id');
  assert.ok(comparePositions(undefined, 'a', 'a0', 'z') > 0, 'no key sorts last');
  assert.ok(comparePositions(undefined, 'a', undefined, 'b') < 0);
  assert.equal(comparePositions('a0', 'a', 'a0', 'a'), 0);
});
