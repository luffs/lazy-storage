// helpers.js - What the suite shares: the in-memory network and fake clock
// the package publishes as `lazy-storage/testing`, plus a seeded random for
// the fuzzers
export { createNetwork, fakeTime } from '../src/testing/index.js';

export function seededRandom(seed) {
  let s = seed >>> 0 || 1;
  const rng = {
    next() { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; },
    int: n => Math.floor(rng.next() * n),
    chance: p => rng.next() < p,
    pick: arr => arr[rng.int(arr.length)]
  };
  return rng;
}
