import { test } from 'node:test';
import { runFuzz } from './fuzz/convergence.js';

test('replicas converge under random offline edits, deletions, reorders, and undo (fixed seed)', async () => {
  await runFuzz({ seed: 1, runs: 25, steps: 30 });
});
