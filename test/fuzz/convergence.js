// convergence.js - Seeded multi-replica convergence fuzzer
//
// Several clients with skewed clocks add, edit, and remove keyed records,
// rewrite a register, go offline and come back, in random interleavings.
// After every settle, every online client's state equals the server's; at
// the end everyone is online and all states are identical. Deterministic
// from the seed.
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub } from '../../src/server/index.js';
import { createClient } from '../../src/client/index.js';
import { createConnection } from '../../src/client/connection.js';
import { createNetwork, fakeTime, seededRandom } from '../helpers.js';

const INITIAL = { tasks: {}, order: [] };
const REGISTERS = ['order'];
const canon = value => JSON.stringify(value, Object.keys(flatten(value)).sort());

function flatten(value, prefix = '', out = {}) {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    for (const k of Object.keys(value)) { out[prefix + k] = true; flatten(value[k], prefix + k + '.', out); }
  }
  return out;
}

/**
 * @param {Object} options
 * @param {'single'|'hub'} [options.mode] - 'single': each client on its own
 *   socket to the store; 'hub': clients share two multiplexed connections,
 *   so an offline toggle drops every client on that socket
 */
export async function runFuzz({ seed = 1, runs = 20, steps = 30, clients: clientCount = 3, mode = 'single', log = () => {} } = {}) {
  let operations = 0;
  for (let run = 0; run < runs; run++) {
    const rng = seededRandom(seed * 1000 + run);
    const store = createStore({ initial: INITIAL, registers: REGISTERS, now: fakeTime(1_000_000) });
    // hub mode: every client has its own socket carrying two stores — the
    // shared one under test and a private side store — so multiplexing is
    // exercised and an offline toggle drops both stores on that socket
    const sideStores = new Map();
    const resolveStore = id => {
      if (id === 'main') return store;
      if (!sideStores.has(id)) sideStores.set(id, createStore({ initial: { notes: {} }, now: fakeTime(1_000_000) }));
      return sideStores.get(id);
    };
    const net = mode === 'hub'
      ? createNetwork({ session: ({ send }) => createHub(resolveStore, { send }) })
      : createNetwork(store);
    const clients = [];
    for (let i = 0; i < clientCount; i++) {
      const time = fakeTime(1_000_000 + rng.int(5000)); // skewed wall clocks
      let c;
      if (mode === 'hub') {
        const link = net.link();
        const connection = createConnection({ transport: link.factory, reconnect: false });
        c = createClient({ connection, store: 'main', replicaId: `r${i}`, initial: INITIAL, registers: REGISTERS, now: time });
        c.side = createClient({ connection, store: `side-${i}`, replicaId: `s${i}`, initial: { notes: {} }, now: time });
        c.link = link;
        c.connect();
        c.side.connect();
      } else {
        c = net.client({ replicaId: `r${i}`, initial: INITIAL, registers: REGISTERS, now: time });
      }
      c.time = time;
      c.errors = [];
      c.on('error', e => c.errors.push(e));
      clients.push(c);
    }
    await net.settle();

    const ops = [];
    const fail = message => new Error(`Convergence failure (seed ${seed}, run ${run}, mode ${mode}):\n  ${message}\n  ops:\n    ${ops.slice(-25).join('\n    ')}\n  reproduce: node test/fuzz/run.js --seed ${seed} --runs ${run + 1} --steps ${steps} --mode ${mode}`);

    for (let step = 0; step < steps; step++) {
      const c = rng.pick(clients);
      c.time.advance(1 + rng.int(50));
      const tasks = c.collection('tasks');
      const roll = rng.next();
      if (roll < 0.25) {
        const id = tasks.add({ title: `t${step}`, done: false, n: rng.int(100) });
        ops.push(`${c.replicaId} add ${id}`);
      } else if (roll < 0.5 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.update(id, rng.chance(0.5) ? { done: rng.chance(0.5) } : { title: `e${step}`, n: rng.int(100) });
        ops.push(`${c.replicaId} update ${id}`);
      } else if (roll < 0.62 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.remove(id);
        ops.push(`${c.replicaId} remove ${id}`);
      } else if (roll < 0.72) {
        const ids = tasks.ids();
        for (let i = ids.length - 1; i > 0; i--) { const j = rng.int(i + 1); [ids[i], ids[j]] = [ids[j], ids[i]]; }
        c.state.order = ids;
        ops.push(`${c.replicaId} order ${ids.length}`);
      } else if (roll < 0.82) {
        if (c.status === 'offline') { c.link.goOnline(); c.connect(); ops.push(`${c.replicaId} online`); }
        else { c.link.goOffline(); ops.push(`${c.replicaId} offline`); }
      } else if (roll < 0.9 && c.canUndo) {
        c.undo();
        ops.push(`${c.replicaId} undo`);
      } else if (c.side && roll < 0.95) {
        c.side.collection('notes').add({ text: `n${step}` });
        ops.push(`${c.replicaId} side note`);
      } else if (c.canRedo) {
        c.redo();
        ops.push(`${c.replicaId} redo`);
      } else {
        tasks.add({ title: `x${step}`, done: true });
        ops.push(`${c.replicaId} add`);
      }
      operations++;
      await net.settle();

      for (const other of clients) {
        if (other.errors.length) throw fail(`${other.replicaId} reported: ${other.errors[0].message}`);
        if (other.status === 'online' && other.pending === 0 && canon(LazyWatch.snapshot(other.state)) !== canon(store.snapshot())) {
          throw fail(`online client ${other.replicaId} diverged after step ${step}\n  server: ${canon(store.snapshot())}\n  client: ${canon(LazyWatch.snapshot(other.state))}`);
        }
      }
    }

    for (const c of clients) { if (c.status === 'offline') { c.link.goOnline(); c.connect(); c.side?.connect(); } }
    await net.settle();
    const expected = canon(store.snapshot());
    for (const c of clients) {
      if (c.pending !== 0) throw fail(`${c.replicaId} still has ${c.pending} pending ops`);
      if (canon(LazyWatch.snapshot(c.state)) !== expected) {
        throw fail(`${c.replicaId} did not converge\n  server: ${expected}\n  client: ${canon(LazyWatch.snapshot(c.state))}`);
      }
      if (c.side) {
        const sideExpected = canon(resolveStore(c.side.store).snapshot());
        if (c.side.pending !== 0 || canon(LazyWatch.snapshot(c.side.state)) !== sideExpected) {
          throw fail(`${c.replicaId}'s side store did not converge\n  server: ${sideExpected}\n  client: ${canon(LazyWatch.snapshot(c.side.state))}`);
        }
      }
    }
    for (const c of clients) { c.side?.dispose(); c.dispose(); }
    for (const s of sideStores.values()) s.dispose();
    store.dispose();
    log(`run ${run}: ${steps} steps ok`);
  }
  return operations;
}
