// client.js - A benchmark for what an app feels on the client: re-renders in
// React and Vue as the store changes, the array view of a large list, the
// document adapter under remote traffic and a long offline spell, and
// lazy-watch's cost for an object write.
//   npm run bench:client     (node bench/client.js [--rounds 3] [--only <text in a case's name>] [--verbose] [--check])
//
// --check also holds every case to its guard (GUARDS, at the end) and
// exits with 1 when one fails: npm run bench:check, in CI.
//
// Every case runs `rounds` times and reports the median, with the counts
// that explain the time: renders per batch, bytes written, and so on.
// Compare runs on the same machine; the absolute figures say little
// across machines.
import { GlobalRegistrator } from '@happy-dom/global-registrator';
import { LazyWatch } from 'lazy-watch';
import { createStore } from '../src/server/index.js';
import { createClient } from '../src/client/index.js';
import { localStorageOutbox } from '../src/client/storage.js';
import { createNetwork } from '../src/testing/index.js';
import { keyBetween, keysBetween } from '../src/core/positions.js';

GlobalRegistrator.register({ url: 'http://localhost' });
globalThis.IS_REACT_ACT_ENVIRONMENT = true;
const React = (await import('react')).default;
const { act } = await import('react');
const { createRoot } = await import('react-dom/client');
const Vue = await import('vue');
const { useClient: useReactClient, useClientSelector } = await import('../src/react/index.js');
const { useClient: useVueClient } = await import('../src/vue/index.js');

const argv = process.argv.slice(2);
const args = Object.fromEntries(argv.map((a, i) => {
  if (!a.startsWith('--')) return [];
  const next = argv[i + 1];
  if (next === undefined || next.startsWith('--')) return [a.slice(2), true];
  return [a.slice(2), Number.isNaN(Number(next)) ? next : Number(next)];
}).filter(x => x.length));
const ROUNDS = Number(args.rounds) || 3;
const REMOTE = { origin: 'remote' };
const results = [];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Run a case `rounds` times: `setup` builds a context, `run(context, n)`
 * does `n` operations and may return counts to report; the median time
 * wins, and its round's counts are the ones shown
 */
async function bench(name, { setup, run, iterations, unit = 'op', timed = true, note = () => '' }) {
  if (typeof args.only === 'string' && !name.includes(args.only)) return;
  if (args.verbose) console.error(`… ${name}`);
  const rounds = [];
  for (let round = 0; round < ROUNDS; round++) {
    const context = setup ? await setup() : undefined;
    const started = process.hrtime.bigint();
    const counts = (await run(context, iterations)) ?? {};
    rounds.push({ ms: Number(process.hrtime.bigint() - started) / 1e6, counts });
    await context?.dispose?.();
  }
  rounds.sort((a, b) => a.ms - b.ms);
  const { ms, counts } = rounds[Math.floor(rounds.length / 2)];
  results.push({ name, unit, timed, counts, iterations, perOp: (ms / iterations) * 1000, perSec: (iterations / ms) * 1000, note: note(counts, iterations) });
}

const ids = n => Array.from({ length: n }, (_, i) => `t${i.toString(36).padStart(6, '0')}`);
/** Tasks keyed by id, each with a position key, in id order */
const tasksOf = all => {
  const keys = keysBetween(null, null, all.length);
  return Object.fromEntries(all.map((id, i) => [id, { id, title: `task ${id}`, done: false, pos: keys[i] }]));
};

/** A store with `n` tasks and a client linked to it in memory, online and current */
async function linked(n, clientOptions = {}, storeOptions = {}) {
  const all = ids(n);
  const store = createStore({ initial: { tasks: {} }, rateLimit: false, ...storeOptions });
  store.patch({ tasks: tasksOf(all) });
  const net = createNetwork(store);
  const db = net.client({ replicaId: 'bench', initial: { tasks: {} }, ...clientOptions }, { user: { id: 'me' } });
  await net.settle();
  return { store, net, db, ids: all };
}

// --- React ----------------------------------------------------------------------------------
// Every row of a list uses the hook and reads one task, the way an app
// splits a list into components. A change to one task, or a peer's
// share, is what the rows re-render for

const h = React.createElement;

async function reactTree(rows, { selector = false } = {}) {
  const c = await linked(1000, {}, { presence: true });
  c.renders = 0;
  const Row = selector
    ? ({ db, id }) => {
      const title = useClientSelector(db, state => state.tasks[id]?.title);
      c.renders++;
      return h('li', null, title);
    }
    : ({ db, id }) => {
      const { state } = useReactClient(db);
      c.renders++;
      return h('li', null, state.tasks[id]?.title);
    };
  const List = ({ db, rowIds }) => h('ul', null, rowIds.map(id => h(Row, { key: id, db, id })));
  c.el = document.createElement('div');
  c.root = createRoot(c.el);
  await act(async () => { c.root.render(h(List, { db: c.db, rowIds: c.ids.slice(0, rows) })); });
  c.renders = 0;
  c.dispose = async () => {
    await act(async () => { c.root.unmount(); });
    c.db.dispose();
    c.store.dispose();
  };
  return c;
}

await bench('react: remote edit of one task, 500 rows each on useClient', {
  unit: 'batch',
  setup: () => reactTree(500),
  iterations: 40,
  run: async (c, n) => {
    for (let i = 0; i < n; i++) {
      await act(async () => {
        c.store.patch({ tasks: { [c.ids[i % 500]]: { title: `edit ${i}` } } });
        await c.net.settle();
      });
    }
    return { renders: c.renders };
  },
  note: ({ renders }, n) => `${Math.round(renders / n)} row renders per batch`
});

await bench('react: a peer shares a cursor, 500 rows that never read peers', {
  unit: 'share',
  setup: async () => {
    const c = await reactTree(500);
    c.peer = c.net.client({ replicaId: 'peer', initial: { tasks: {} } }, { user: { id: 'peer' } });
    await act(async () => { await c.net.settle(); });
    c.renders = 0;
    const dispose = c.dispose;
    c.dispose = async () => { c.peer.dispose(); await dispose(); };
    return c;
  },
  iterations: 40,
  run: async (c, n) => {
    for (let i = 0; i < n; i++) {
      await act(async () => {
        c.peer.share({ cursor: i });
        await c.net.settle();
      });
    }
    return { renders: c.renders };
  },
  note: ({ renders }, n) => `${Math.round(renders / n)} row renders per share`
});

await bench('react: remote edit of one task, 500 rows each on useClientSelector', {
  unit: 'batch',
  setup: () => reactTree(500, { selector: true }),
  iterations: 40,
  run: async (c, n) => {
    for (let i = 0; i < n; i++) {
      await act(async () => {
        c.store.patch({ tasks: { [c.ids[i % 500]]: { title: `edit ${i}` } } });
        await c.net.settle();
      });
    }
    return { renders: c.renders };
  },
  note: ({ renders }, n) => `${Math.round(renders / n)} row renders per batch`
});

await bench('react: a peer shares a cursor, 500 rows on useClientSelector', {
  unit: 'share',
  setup: async () => {
    const c = await reactTree(500, { selector: true });
    c.peer = c.net.client({ replicaId: 'peer', initial: { tasks: {} } }, { user: { id: 'peer' } });
    await act(async () => { await c.net.settle(); });
    c.renders = 0;
    const dispose = c.dispose;
    c.dispose = async () => { c.peer.dispose(); await dispose(); };
    return c;
  },
  iterations: 40,
  run: async (c, n) => {
    for (let i = 0; i < n; i++) {
      await act(async () => {
        c.peer.share({ cursor: i });
        await c.net.settle();
      });
    }
    return { renders: c.renders };
  },
  note: ({ renders }, n) => `${Math.round(renders / n)} row renders per share`
});

// --- Vue ------------------------------------------------------------------------------------
// The same list as Vue components, each calling useClient in setup

async function vueTree(rows) {
  const c = await linked(1000);
  c.clones = 0;
  const clone = globalThis.structuredClone;
  globalThis.structuredClone = value => { c.clones++; return clone(value); };
  const Row = Vue.defineComponent({
    props: ['db', 'id'],
    setup(props) {
      const { state } = useVueClient(props.db);
      return () => Vue.h('li', null, state.tasks[props.id]?.title);
    }
  });
  const List = Vue.defineComponent({
    props: ['db', 'rowIds'],
    setup: props => () => Vue.h('ul', null, props.rowIds.map(id => Vue.h(Row, { key: id, db: props.db, id })))
  });
  c.el = document.createElement('div');
  c.mount = () => {
    c.app = Vue.createApp(List, { db: c.db, rowIds: c.ids.slice(0, rows) });
    c.app.mount(c.el);
  };
  c.dispose = () => {
    globalThis.structuredClone = clone;
    c.app?.unmount();
    c.db.dispose();
    c.store.dispose();
  };
  return c;
}

await bench('vue: mount 200 rows each on useClient, 1k-task state', {
  unit: 'mount',
  setup: () => vueTree(200),
  iterations: 1,
  run: async c => {
    c.clones = 0;
    c.mount();
    await Vue.nextTick();
    return { clones: c.clones };
  },
  note: ({ clones }) => `${clones} deep copies of the state`
});

await bench('vue: remote edit of one task, 200 rows each on useClient', {
  unit: 'batch',
  setup: async () => {
    const c = await vueTree(200);
    c.mount();
    await Vue.nextTick();
    return c;
  },
  iterations: 40,
  run: async (c, n) => {
    for (let i = 0; i < n; i++) {
      c.store.patch({ tasks: { [c.ids[i % 200]]: { title: `edit ${i}` } } });
      await c.net.settle();
      await Vue.nextTick();
    }
  }
});

// --- Lists ----------------------------------------------------------------------------------
// A 5k-record list declared as a list (db.state.tasks is an array), and
// the same through db.list, which works on the keyed map directly

async function listed(n) {
  const c = await linked(n, { initial: { tasks: [] }, lists: ['tasks'] });
  c.dispose = () => { c.db.dispose(); c.store.dispose(); };
  return c;
}
/** Close the view's batch and the wire's it made, as the microtasks would */
const settleView = db => { LazyWatch.flush(db.state); LazyWatch.flush(db.wire); };

await bench('list view: push one record onto a 5k-record array', {
  setup: () => listed(5000),
  iterations: 10,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.db.state.tasks.push({ title: `new ${i}`, done: false });
      settleView(c.db);
    }
  }
});

await bench('list view: move one record within a 5k-record array (splice out, splice in)', {
  setup: () => listed(5000),
  iterations: 10,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      const [moved] = c.db.state.tasks.splice(10 + i, 1);
      c.db.state.tasks.splice(4000 - i, 0, moved);
      settleView(c.db);
    }
  }
});

await bench('list view: remote field edit, 5k-record array', {
  setup: () => listed(5000),
  iterations: 200,
  run: (c, n) => {
    for (let i = 0; i < n; i++) LazyWatch.patch(c.db.wire, { tasks: { [c.ids[(i * 97) % 5000]]: { title: `remote ${i}` } } }, REMOTE);
  }
});

// 2k records and 200 places, not 5k and thousands: a remote move shifts
// every record it passes, a splice of the whole array each, which at 5k
// records and 1000 places takes half a minute a move
await bench('list view: remote move 200 places (a new position), 2k-record array', {
  setup: async () => {
    const c = await listed(2000);
    c.keys = c.ids.map(id => c.db.wire.tasks[id].pos);
    return c;
  },
  iterations: 1,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      const at = 200 + i;   // between two neighbours 200 places from the record's own
      LazyWatch.patch(c.db.wire, { tasks: { [c.ids[i]]: { pos: keyBetween(c.keys[at], c.keys[at + 1]) } } }, REMOTE);
    }
  }
});

await bench('db.list: add one record at the end of 5k (the keyed map, no array view)', {
  setup: async () => {
    const c = await linked(5000);
    c.list = c.db.list('tasks');
    c.dispose = () => { c.db.dispose(); c.store.dispose(); };
    return c;
  },
  iterations: 50,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.list.add({ title: `new ${i}`, done: false });
      LazyWatch.flush(c.db.state);
    }
  }
});

// --- Persistence ----------------------------------------------------------------------------
// The document adapter (localStorageOutbox) against a localStorage that
// counts what it is handed, as a browser would have to serialize it

function countingLocalStorage() {
  const map = new Map();
  const counts = { writes: 0, bytes: 0 };
  // Over happy-dom's own, which is a getter
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    writable: true,
    value: {
      getItem: k => map.get(k) ?? null,
      setItem: (k, v) => { counts.writes++; counts.bytes += String(v).length; map.set(k, String(v)); },
      removeItem: k => map.delete(k)
    }
  });
  return counts;
}

for (const size of [1000, 10_000]) {
  await bench(`persistence: remote patches every 5 ms for a second, localStorageOutbox, ${size / 1000}k-task state`, {
    unit: 'patch',
    timed: false,   // the time is the waits; what it wrote is the measure
    setup: async () => {
      const counts = countingLocalStorage();
      const c = await linked(size, { storage: localStorageOutbox('bench') });
      await sleep(100);   // the first snapshot's write is not what is measured
      counts.writes = counts.bytes = 0;
      c.counts = counts;
      c.dispose = () => { c.db.dispose(); c.store.dispose(); };
      return c;
    },
    iterations: 200,
    run: async (c, n) => {
      for (let i = 0; i < n; i++) {
        LazyWatch.patch(c.db.wire, { tasks: { [c.ids[i % c.ids.length]]: { title: `remote ${i}` } } }, REMOTE);
        await sleep(5);
      }
      await sleep(100);
      return { ...c.counts };
    },
    note: ({ writes, bytes }) => `${writes} writes, ${(bytes / 1e6).toFixed(1)} MB serialized`
  });
}

await bench('persistence: 1000 local ops offline, localStorageOutbox, 1k-task state', {
  setup: async () => {
    const counts = countingLocalStorage();
    const c = await linked(1000, { storage: localStorageOutbox('bench-offline'), cache: false });
    c.db.link.goOffline();
    await c.net.settle();
    counts.writes = counts.bytes = 0;
    c.counts = counts;
    c.dispose = () => { c.db.dispose(); c.store.dispose(); };
    return c;
  },
  iterations: 1000,
  run: (c, n) => {
    const times = [];
    for (let i = 0; i < n; i++) {
      const started = process.hrtime.bigint();
      c.db.state.tasks[c.ids[i % c.ids.length]].title = `offline ${i}`;   // each op a different record: none makes another moot
      LazyWatch.flush(c.db.state);
      times.push(Number(process.hrtime.bigint() - started) / 1e3);
    }
    const mean = list => list.reduce((a, b) => a + b, 0) / list.length;
    return { ...c.counts, first: mean(times.slice(0, 100)), last: mean(times.slice(-100)) };
  },
  note: ({ bytes, first, last }) => `${(bytes / 1e6).toFixed(1)} MB of outbox written; the first 100 ops ${first.toFixed(0)} µs each, the last 100 ${last.toFixed(0)} µs`
});

// --- lazy-watch -----------------------------------------------------------------------------
// What every write through db.state costs underneath, on its own

await bench('lazy-watch: assign a 5-field object (a record write), then flush', {
  setup: () => {
    const state = new LazyWatch({ tasks: tasksOf(ids(1000)) });
    return { state, dispose: () => LazyWatch.dispose(state) };
  },
  iterations: 20_000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.state.tasks[`t${i % 1000}`] = { id: `t${i}`, title: 'x', done: false, n: i, tags: ['a'] };
      if (i % 100 === 99) LazyWatch.flush(c.state);
    }
  }
});

await bench('lazy-watch: assign one leaf (a field write), then flush', {
  setup: () => {
    const state = new LazyWatch({ tasks: tasksOf(ids(1000)) });
    return { state, all: ids(1000), dispose: () => LazyWatch.dispose(state) };
  },
  iterations: 20_000,
  run: (c, n) => {
    for (let i = 0; i < n; i++) {
      c.state.tasks[c.all[i % 1000]].title = `x${i}`;
      if (i % 100 === 99) LazyWatch.flush(c.state);
    }
  }
});

// --- Report ---------------------------------------------------------------------------------

const width = Math.max(...results.map(r => r.name.length));
const fmt = (n, digits = 0) => n.toLocaleString('en-US', { maximumFractionDigits: digits });
console.log(`lazy-storage client benchmark (median of ${ROUNDS} rounds, ${process.release?.name ?? 'js'} ${process.version})\n`);
console.log(`${'case'.padEnd(width)}  ${'per op'.padStart(10)}  ${'per second'.padStart(14)}  what it did`);
for (const r of results) {
  const per = !r.timed ? '—' : r.perOp >= 1000 ? `${fmt(r.perOp / 1000, 2)} ms` : `${fmt(r.perOp, 1)} µs`;
  const rate = r.timed ? `${fmt(r.perSec)} ${r.unit}/s` : '—';
  console.log(`${r.name.padEnd(width)}  ${per.padStart(10)}  ${rate.padStart(14)}  ${r.note}`);
}

// --- Guards (--check) -----------------------------------------------------------------------
// Per case, a ceiling on the median time per op, about ten times what a
// laptop measures: a slow CI runner stays under it, and a regression of
// the kind this file found (a list move of seconds) does not. And, where
// a case counts something, a bound on the count, which no machine
// changes: the renders a selector saves, the state copies a mount makes,
// what the outbox writes

const perBatch = what => ({ renders }, n) => renders <= n * what || `${renders / n} row renders per batch, over ${what}`;
const GUARDS = {
  'react: remote edit of one task, 500 rows each on useClient': { ms: 80 },
  'react: a peer shares a cursor, 500 rows that never read peers': { ms: 30 },
  'react: remote edit of one task, 500 rows each on useClientSelector': { ms: 6, counts: perBatch(1) },
  'react: a peer shares a cursor, 500 rows on useClientSelector': { ms: 2, counts: perBatch(0) },
  'vue: mount 200 rows each on useClient, 1k-task state': { ms: 80, counts: ({ clones }) => clones <= 1 || `${clones} deep copies of the state, over 1` },
  'vue: remote edit of one task, 200 rows each on useClient': { ms: 2 },
  'list view: push one record onto a 5k-record array': { ms: 60 },
  'list view: move one record within a 5k-record array (splice out, splice in)': { ms: 60 },
  'list view: remote field edit, 5k-record array': { ms: 2 },
  'list view: remote move 200 places (a new position), 2k-record array': { ms: 20 },
  'db.list: add one record at the end of 5k (the keyed map, no array view)': { ms: 30 },
  'persistence: remote patches every 5 ms for a second, localStorageOutbox, 1k-task state': { counts: ({ writes }) => writes <= 10 || `${writes} writes, over 10` },
  'persistence: remote patches every 5 ms for a second, localStorageOutbox, 10k-task state': { counts: ({ writes }) => writes <= 10 || `${writes} writes, over 10` },
  'persistence: 1000 local ops offline, localStorageOutbox, 1k-task state': { ms: 0.6, counts: ({ bytes }) => bytes <= 2e6 || `${(bytes / 1e6).toFixed(1)} MB written, over 2` },
  'lazy-watch: assign a 5-field object (a record write), then flush': { ms: 0.03 },
  'lazy-watch: assign one leaf (a field write), then flush': { ms: 0.01 }
};

if (args.check) {
  let failed = 0;
  console.log('\nguards (median per op; ceiling)');
  for (const r of results) {
    const guard = GUARDS[r.name];
    const problems = [];
    if (!guard) problems.push('no guard: add one to GUARDS');
    if (guard?.ms !== undefined && r.perOp / 1000 > guard.ms) problems.push(`${fmt(r.perOp / 1000, 3)} ms per op, over ${guard.ms}`);
    const counted = guard?.counts?.(r.counts, r.iterations);
    if (typeof counted === 'string') problems.push(counted);
    if (problems.length) failed++;
    const shown = guard?.ms !== undefined ? `${fmt(r.perOp / 1000, 3)} ms (${guard.ms})` : r.note;
    console.log(`${problems.length ? ' FAIL' : ' ok  '} ${r.name}: ${problems.length ? problems.join('; ') : shown}`);
  }
  if (failed) {
    console.log(`\n${failed} failed`);
    process.exitCode = 1;
  }
}
await GlobalRegistrator.unregister();
