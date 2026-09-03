import { test } from 'node:test';
import { runFuzz } from './fuzz/convergence.js';

test('replicas converge under random offline edits, deletions, reorders, and undo, two stores per socket (fixed seed)', async () => {
  await runFuzz({ seed: 1, runs: 30, steps: 30 });
});
