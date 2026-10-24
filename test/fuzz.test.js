// Fixed-seed runs of the fuzzers: convergence, with clients straight on the store and behind a relay
// whose link drops, and hostile, with an attacker beside honest clients; `npm run fuzz` runs a longer,
// random campaign
import { test } from 'node:test';
import { runFuzz, runRelayFuzz } from './fuzz/convergence.js';
import { runHostile } from './fuzz/hostile.js';

test('clients converge on the store through edits made offline', async () => {
  await runFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});

test('clients behind a relay converge through outages of its link', async () => {
  await runRelayFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});

test('an attacker beside honest clients changes nothing it may not, and they converge', async () => {
  await runHostile({ seed: 7, runs: 20, steps: 60 });
});
