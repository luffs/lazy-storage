// run.js - CLI for the convergence fuzzer
//   node test/fuzz/run.js [--seed N] [--runs N] [--steps N] [--clients N]
import { runFuzz } from './convergence.js';

const args = Object.fromEntries(process.argv.slice(2).map((a, i, all) => a.startsWith('--') ? [a.slice(2), Number(all[i + 1])] : null).filter(Boolean));
const options = { seed: args.seed ?? Date.now() % 100000, runs: args.runs ?? 100, steps: args.steps ?? 40, clients: args.clients ?? 3 };
const started = Date.now();
try {
  const operations = await runFuzz(options);
  console.log(`ok: seed ${options.seed}, ${options.runs} runs x ${options.steps} steps, ${operations.toLocaleString('en-US')} operations, ${Date.now() - started} ms`);
} catch (err) {
  console.error(err.message);
  process.exit(1);
}
