// Fixed-seed runs of the fuzzers: convergence, with clients straight on the store and behind a relay
// whose link drops (a site's, and a browser's), each also with the store's rate limit on and small, and
// hostile, with an attacker beside honest clients; `npm run fuzz` runs a longer, random campaign
import { test } from 'node:test';
import { runFuzz, runRelayFuzz } from './fuzz/convergence.js';
import { runHostile } from './fuzz/hostile.js';

test('clients converge on the store through edits made offline', async () => {
  await runFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});

test('clients that take their snapshots without the records\' clocks converge with those that do', async () => {
  await runFuzz({ seed: 7, runs: 40, steps: 60, clients: 4, clocks: 'mixed' });
});

test('clients behind a relay converge through outages of its link', async () => {
  await runRelayFuzz({ seed: 7, runs: 60, steps: 60, clients: 4 });
});

test('clients a small rate limit keeps refusing converge once their retries have gone through', async () => {
  await runFuzz({ seed: 7, runs: 40, steps: 60, clients: 4, rate: true });
});

test('clients behind a relay converge through outages of its link under a small rate limit', async () => {
  await runRelayFuzz({ seed: 7, runs: 40, steps: 60, clients: 4, rate: true });
});

test("the tabs of a browser's relay converge through outages of its link, with the rate limit off and on", async () => {
  await runRelayFuzz({ seed: 7, runs: 40, steps: 60, clients: 4, local: true });
  await runRelayFuzz({ seed: 7, runs: 40, steps: 60, clients: 4, local: true, rate: true });
});

test('an attacker beside honest clients changes nothing it may not, and they converge', async () => {
  await runHostile({ seed: 7, runs: 20, steps: 60 });
});
