// Fixed-seed runs of the convergence fuzzer, with clients straight on the store and behind a relay
// whose link drops; `npm run fuzz` runs a longer, random campaign
import { test } from 'node:test';
import { runFuzz, runRelayFuzz } from './fuzz/convergence.js';

test('clients converge on the store through edits made offline', async () => {
  await runFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});

test('clients behind a relay converge through outages of its link', async () => {
  await runRelayFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});
