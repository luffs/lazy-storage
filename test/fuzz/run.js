// run.js - CLI for the fuzzers
//   node test/fuzz/run.js [--mode convergence|relay] [--seed N] [--runs N] [--steps N] [--clients N]
import { runFuzz, runRelayFuzz } from './convergence.js';

const argv = process.argv.slice(2);
const args = Object.fromEntries(argv.map((a, i) => (a.startsWith('--') ? [a.slice(2), argv[i + 1]] : null)).filter(Boolean));
const number = (value, fallback) => (value === undefined ? fallback : Number(value));
const mode = args.mode ?? 'convergence';
const options = { seed: number(args.seed, Date.now() % 100000), runs: number(args.runs, 100), steps: number(args.steps, 40), clients: number(args.clients, 3), only: args.run === undefined ? undefined : Number(args.run), trace: argv.includes('--trace') };
const started = Date.now();
try {
  const operations = await (mode === 'relay' ? runRelayFuzz(options) : runFuzz(options));
  console.log(`ok (${mode}): seed ${options.seed}, ${options.runs} runs x ${options.steps} steps, ${operations.toLocaleString('en-US')} operations, ${Date.now() - started} ms`);
} catch (err) {
  console.error(err.message);
  process.exit(1);
}
