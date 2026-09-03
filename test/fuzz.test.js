import { test } from 'node:test';
import { runFuzz } from './fuzz/convergence.js';

test('replicas converge under random offline edits, deletions, reorders, and undo (fixed seed, one socket each)', async () => {
  await runFuzz({ seed: 1, runs: 25, steps: 30, mode: 'single' });
});

test('replicas converge when they share multiplexed connections (fixed seed, hub mode)', async () => {
  await runFuzz({ seed: 2, runs: 25, steps: 30, mode: 'hub' });
});
