import { test } from 'node:test';
import { runFuzz } from './fuzz/convergence.js';
import { runHostile } from './fuzz/hostile.js';

test('replicas converge under random offline edits, deletions, reorders, and undo, two stores per socket (fixed seed)', async () => {
  await runFuzz({ seed: 1, runs: 30, steps: 30 });
});

test('a client that does not play by the protocol cannot pollute, forge, lock out, or desync anyone (fixed seed)', async () => {
  await runHostile({ seed: 1, runs: 30, steps: 40 });
});
