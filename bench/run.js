// run.js - A benchmark for the paths that matter: the merge, the gates, a
// broadcast to many sockets, a client's local op with each kind of
// storage, and a reconnect answered with a snapshot versus a delta.
//   npm run bench            (node bench/run.js [--rounds 5])
//
// Every case runs `rounds` times and reports the median, so a noisy
// machine does not skew the numbers. Compare runs on the same machine;
// the absolute figures say little across machines.
import { LazyWatch } from 'lazy-watch';
import { createStore, toJSON } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { localStorageOutbox } from '../src/client/storage.js';
import { createClock } from '../src/core/hlc.js';

const args = Object.fromEntries(process.argv.slice(2).map((a, i, all) => (a.startsWith('--') ? [a.slice(2), Number(all[i + 1]) || true] : [])).filter(x => x.length));
const ROUNDS = Number(args.rounds) || 5;
const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const results = [];

function bench(name, { setup, run, iterations, unit = 'op' }) {
  const times = [];
  for (let round = 0; round < ROUNDS; round++) {
    const context = setup ? setup() : undefined;
    const started = process.hrtime.bigint();
    run(context, iterations);
    times.push(Number(process.hrtime.bigint() - started) / 1e6);
    context?.dispose?.();
  }
  times.sort((a, b) => a - b);
  const ms = times[Math.floor(times.length / 2)];
  results.push({ name, iterations, unit, ms, perOp: (ms / iterations) * 1000, perSec: (iterations / ms) * 1000 });
}

const ids = n => Array.from({ length: n }, (_, i) => `t${i.toString(36).padStart(6, '0')}`);

/** A store seeded with `n` tasks, plus a clock and ids for ops against it */
function seeded(n, options = {}) {
  const store = createStore({ initial: INITIAL, registers: REGISTERS, rateLimit: false, ...options });
  const all = ids(n);
  const tasks = {};
  for (const id of all) tasks[id] = { id, title: `task ${id}`, done: false, n: 1 };
  store.patch({ tasks, order: all });
  return { store, ids: all, clock: createClock('bench'), seq: 0, dispose: () => store.dispose() };
}

// --- The merge -----------------------------------------------------------------------

bench('merge: one-leaf op into a 10k-task store', {
  setup: () => seeded(10_000),
  iterations: 5000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) c.store.apply({ replicaId: 'bench', seq: ++c.seq, ts: c.clock.now(), diff: { tasks: { [c.ids[i % c.ids.length]]: { title: `edit ${i}` } } } });
  }
});

bench('merge: ten-leaf record add', {
  setup: () => seeded(10_000),
  iterations: 5000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.store.apply({ replicaId: 'bench', seq: ++c.seq, ts: c.clock.now(), diff: { tasks: { [`new${i}`]: { id: `new${i}`, title: 'x', done: false, a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7 } } } });
    }
  }
});

bench('merge: register write, 1000 ids', {
  setup: () => seeded(1000),
  iterations: 2000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) c.store.apply({ replicaId: 'bench', seq: ++c.seq, ts: c.clock.now(), diff: { order: i % 2 ? c.ids : [...c.ids].reverse() } });
  }
});

bench('session: one-leaf op through the gates (read-only paths, validate, skew, retention)', {
  setup: () => {
    const c = seeded(10_000, { readOnly: ['team'], validate: () => true });
    c.session = c.store.session({ send: () => {} });
    return c;
  },
  iterations: 5000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) c.session.receive({ t: 'op', op: { replicaId: 'bench', seq: ++c.seq, ts: c.clock.now(), diff: { tasks: { [c.ids[i % c.ids.length]]: { title: `edit ${i}` } } } } });
  }
});

// --- Broadcast ---------------------------------------------------------------------------

for (const sockets of [100, 1000]) {
  bench(`broadcast: one patch to ${sockets} sockets (encoded once)`, {
    unit: 'patch',
    setup: () => {
      const c = seeded(1000);
      for (let i = 0; i < sockets; i++) c.store.session({ send: message => { toJSON(message); } });
      return c;
    },
    iterations: 200,
    run: (c, n) => {
      for (let i = 0; i < n; i++) c.store.patch({ tasks: { [c.ids[i % c.ids.length]]: { title: `edit ${i}` } } });
    }
  });
}

// --- The client ------------------------------------------------------------------------------

const idle = () => ({ send() {}, close() {}, onopen: null, onmessage: null, onclose: null });

/** A localStorage stand-in so the document adapter pays for JSON like a browser would */
function fakeLocalStorage() {
  const map = new Map();
  globalThis.localStorage = { getItem: k => map.get(k) ?? null, setItem: (k, v) => map.set(k, String(v)), removeItem: k => map.delete(k) };
}

/** An in-memory row adapter that clones like IndexedDB does */
function memoryRows() {
  const leaves = new Map();
  const ops = new Map();
  return {
    load: () => null,
    commit({ puts, deletes }) {
      for (const key of deletes) leaves.delete(key);
      for (const [key, value] of puts) leaves.set(key, structuredClone(value));
    },
    replace({ rows }) { leaves.clear(); for (const [k, v] of rows) leaves.set(k, structuredClone(v)); },
    saveOp(op) { ops.set(op.seq, structuredClone(op)); },
    dropOps(seq) { for (const k of ops.keys()) if (k <= seq) ops.delete(k); }
  };
}

function clientWith(storage, n) {
  const client = createClient({ transport: idle, reconnect: false, store: 'bench', initial: INITIAL, registers: REGISTERS, storage, replicaId: 'bench' });
  const all = ids(n);
  const tasks = {};
  for (const id of all) tasks[id] = { id, title: `task ${id}`, done: false };
  LazyWatch.patch(client.state, { tasks, order: all }, { origin: 'remote' });
  return { client, ids: all, dispose: () => client.dispose() };
}

bench('client: local op, document adapter (localStorage-like), 1k-task state', {
  setup: () => { fakeLocalStorage(); return clientWith(localStorageOutbox('bench'), 1000); },
  iterations: 2000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.client.state.tasks[c.ids[i % c.ids.length]].title = `edit ${i}`;
      LazyWatch.flush(c.client.state);     // emit now rather than on the microtask
    }
  }
});

bench('client: local op, row adapter (IndexedDB-like), 1k-task state', {
  setup: () => clientWith(memoryRows(), 1000),
  iterations: 2000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.client.state.tasks[c.ids[i % c.ids.length]].title = `edit ${i}`;
      LazyWatch.flush(c.client.state);
    }
  }
});

// --- Reconnects -----------------------------------------------------------------------------

bench('hello: answered with a snapshot, 10k-task store (encoded)', {
  unit: 'hello',
  setup: () => {
    const c = seeded(10_000);
    c.session = c.store.session({ send: message => { toJSON(message); } });
    return c;
  },
  iterations: 20,
  run: (c, n) => {
    for (let i = 0; i < n; i++) c.session.receive({ t: 'hello', replicaId: `r${i}`, ops: [] });
  }
});

bench('hello: answered with a delta of 10 ops, 10k-task store (encoded)', {
  unit: 'hello',
  setup: () => {
    const c = seeded(10_000);
    for (let i = 0; i < 10; i++) c.store.patch({ tasks: { [c.ids[i]]: { title: `late edit ${i}` } } });
    c.since = c.store.version - 10;
    c.session = c.store.session({ send: message => { toJSON(message); } });
    return c;
  },
  iterations: 200,
  run: (c, n) => {
    for (let i = 0; i < n; i++) c.session.receive({ t: 'hello', replicaId: `r${i}`, ops: [], since: c.since, epoch: c.store.epoch });
  }
});

// --- Report ------------------------------------------------------------------------------------

const width = Math.max(...results.map(r => r.name.length));
const fmt = (n, digits = 0) => n.toLocaleString('en-US', { maximumFractionDigits: digits });
console.log(`lazy-storage benchmark (median of ${ROUNDS} rounds, ${process.release?.name ?? 'js'} ${process.version})\n`);
console.log(`${'case'.padEnd(width)}  ${'per op'.padStart(10)}  ${'per second'.padStart(12)}`);
for (const r of results) {
  const per = r.perOp >= 1000 ? `${fmt(r.perOp / 1000, 2)} ms` : `${fmt(r.perOp, 1)} µs`;
  console.log(`${r.name.padEnd(width)}  ${per.padStart(10)}  ${fmt(r.perSec).padStart(12)} ${r.unit}/s`);
}
