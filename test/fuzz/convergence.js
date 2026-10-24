// convergence.js - Random edits by clients that go offline and come back, behind a relay or not
//
// Every run builds a store, clients connected to it directly and, with
// `relay`, clients behind a relay whose link to the store drops and comes
// back, all on one fake clock. Each step picks an action: an edit of a
// record (made, written deep, written whole, a field deleted, the record
// deleted), a child made under a parent that may not be there, a singleton
// or a register written, an undo, a server's own patch, a client or the
// relay's link going away or coming back, time moving on (or not, for
// ties). At the end everything comes back online and settles, and:
//
// - every client, and the relay's copy, holds the store's state
// - no child names a parent that is not there
// - no outbox holds anything
// - no op was applied twice
// - nothing the shape refused came from a client (a compile fault), and no
//   store, relay or client reported an error
//
// A failure throws with the seed, the run, and the last actions.
import { LazyWatch } from 'lazy-watch';
import { isDeepStrictEqual } from 'node:util';
import { createStore } from '../../src/server/index.js';
import { createRelay } from '../../src/relay/index.js';
import { createNetwork, fakeTime, seededRandom } from '../helpers.js';

const SHAPE = {
  initial: { tasks: {}, subtasks: {}, settings: { theme: 'light', size: 1 } },
  singletons: ['settings'],
  registers: ['tasks/*/geo'],
  parents: { subtasks: { taskId: 'tasks' } }
};
const IDS = ['t0', 't1', 't2', 't3'];
const SUB_IDS = ['s0', 's1', 's2'];

async function oneRun(rng, { steps, clients: count, relay: withRelay, trace }, trail) {
  const now = fakeTime(1_000_000);
  const errors = [];
  const onError = err => errors.push(err);
  const store = createStore({ ...SHAPE, now, onError, rateLimit: false });
  const applied = new Set();
  store.observe('op', ({ replicaId, seq, changes }) => {
    if (seq === null || !changes.length) return;
    const key = `${replicaId}:${seq}`;
    if (applied.has(key)) errors.push(new Error(`op ${key} applied twice`));
    applied.add(key);
  });
  const central = createNetwork(store, { vouch: credential => credential });
  let uplink = null;
  let relay = null;
  let site = null;
  if (withRelay) {
    uplink = central.link({ user: { id: 'relay' } });
    const traced = !trace ? uplink.factory : () => {
      const t = uplink.factory();
      const send = t.send;
      t.send = m => { if (m.as || m.t === 'hello') console.log('  relay->server', m.t, m.as ?? '', m.op ? `${m.op.replica}:${m.op.seq}` : ''); send(m); };
      queueMicrotask(() => {
        const onmessage = t.onmessage;
        t.onmessage = m => { if (m.as || ['snapshot', 'delta', 'closed'].includes(m.t)) console.log('  server->relay', m.t, m.as ?? '', m.seq ?? '', m.code ?? ''); onmessage?.(m); };
        const onclose = t.onclose;
        t.onclose = e => { console.log('  link closed'); onclose?.(e); };
      });
      return t;
    };
    relay = createRelay({ shapeOf: () => SHAPE, link: traced, grace: 0, reconnect: false, keepalive: false, now, onError });
    site = createNetwork(relay);
  }
  const settle = async () => {
    for (let i = 0; i < 50; i++) {
      await central.settle();
      if (site) await site.settle();
      if (central.pending === 0 && (!site || site.pending === 0)) return;
    }
  };
  const clients = [];
  for (let i = 0; i < count; i++) {
    const behind = withRelay && i % 2 === 1;
    const net = behind ? site : central;
    const client = net.client({ replicaId: `c${i}`, ...SHAPE, now }, { user: { id: `u${i}` } });
    client.behind = behind;
    client.on('error', err => { if (err.code !== 'rate-limited') errors.push(err); });
    client.on('rejected', e => errors.push(new Error(`rejected ${e.code}: ${e.message}`)));
    clients.push(client);
  }
  await settle();

  const pick = arr => arr[rng.int(arr.length)];
  const value = () => rng.int(100);
  const act = (label, fn) => {
    trail.push(label);
    if (trace) console.log(label);
    if (trail.length > 30) trail.shift();
    fn();
  };

  for (let step = 0; step < steps; step++) {
    const c = pick(clients);
    const s = c.state;
    const id = pick(IDS);
    const sub = pick(SUB_IDS);
    const roll = rng.int(22);
    const task = s.tasks[id];
    if (roll === 0) act(`${c.replicaId} makes ${id}`, () => { s.tasks[id] = { title: `t${value()}`, meta: { a: value(), b: value() }, tags: [value()] }; });
    else if (roll === 1 && task) act(`${c.replicaId} ${id}.meta.a`, () => { task.meta = task.meta ?? {}; task.meta.a = value(); });
    else if (roll === 2 && task) act(`${c.replicaId} ${id}.meta whole`, () => { task.meta = { b: value() }; });
    else if (roll === 3 && task) act(`${c.replicaId} deletes ${id}.meta`, () => { delete task.meta; });
    else if (roll === 4 && task) act(`${c.replicaId} ${id}.title`, () => { task.title = `t${value()}`; });
    else if (roll === 5 && task) act(`${c.replicaId} deletes ${id}`, () => { delete s.tasks[id]; });
    else if (roll === 6 && task) act(`${c.replicaId} ${id}.tags push`, () => { task.tags = Array.isArray(task.tags) ? [...task.tags, value()] : [value()]; });
    else if (roll === 7 && task) act(`${c.replicaId} ${id}.geo`, () => { task.geo = { x: value(), y: value() }; });
    else if (roll === 8 && task?.geo) act(`${c.replicaId} ${id}.geo.x`, () => { task.geo.x = value(); });
    else if (roll === 9) act(`${c.replicaId} makes ${sub} under ${id}`, () => { s.subtasks[sub] = { taskId: id, n: value() }; });
    else if (roll === 10 && s.subtasks[sub]) act(`${c.replicaId} ${sub}.n`, () => { s.subtasks[sub].n = value(); });
    else if (roll === 11 && s.subtasks[sub]) act(`${c.replicaId} deletes ${sub}`, () => { delete s.subtasks[sub]; });
    else if (roll === 12) act(`${c.replicaId} theme`, () => { s.settings.theme = pick(['light', 'dark', 'blue']); });
    else if (roll === 13) act(`${c.replicaId} undo`, () => { c.undo(); });
    else if (roll === 14) act(`server patches ${id}`, () => { store.patch({ tasks: { [id]: { srv: value() } } }); });
    else if (roll === 15) act(`${c.replicaId} goes offline`, () => { c.link.goOffline(); });
    else if (roll === 16) act(`${c.replicaId} comes back`, () => { c.link.goOnline(); c.connect(); });
    else if (roll === 17 && uplink) act('relay link drops', () => { uplink.goOffline(); });
    else if (roll === 18 && uplink) act('relay link back', () => { uplink.goOnline(); relay.upstreamUp(); });
    else if (roll === 19) act('time moves', () => { now.advance(1 + rng.int(20)); });
    else if (roll === 20 && task) act(`${c.replicaId} ${id}.meta.c new`, () => { if (task.meta) task.meta.c = { deep: value() }; });
    else act('nothing', () => {});
    if (rng.chance(0.6)) await settle();
    else await new Promise(resolve => setImmediate(resolve));
  }

  // Everything back, and quiet
  await settle();
  if (uplink) { uplink.goOnline(); relay.upstreamUp(); }
  for (const c of clients) c.link.goOnline();
  await settle();
  for (const c of clients) c.connect();
  await settle();
  now.advance(1);
  await settle();

  const truth = store.snapshot();
  const fail = message => { throw new Error(message); };
  if (errors.length) fail(`errors: ${errors.map(e => e.message).join(' | ')}`);
  for (const [sid, record] of Object.entries(truth.subtasks)) {
    if (typeof record.taskId === 'string' && !truth.tasks[record.taskId]) fail(`orphan ${sid} -> ${record.taskId}`);
  }
  for (const c of clients) {
    if (c.pending) fail(`${c.replicaId}${c.behind ? ' (behind the relay)' : ''} still has ${c.pending} pending (${c.status}${relay ? `, relay ${JSON.stringify(relay.stats())}` : ''})`);
    const mine = LazyWatch.snapshot(c.state);
    if (!isDeepStrictEqual(mine, truth)) fail(`${c.replicaId}${c.behind ? ' (behind the relay)' : ''} diverged (v ${c.version} of ${store.version}, ${c.status}, base ${isDeepStrictEqual(c.base, truth) ? 'right' : 'wrong'}${relay ? `, relay copy ${isDeepStrictEqual(relay.copy('main'), truth) ? 'right' : 'wrong'}` : ''}):\n  client ${JSON.stringify(mine)}\n  store  ${JSON.stringify(truth)}`);
  }
  if (relay && !isDeepStrictEqual(relay.copy('main'), truth)) fail(`the relay's copy diverged:\n  relay ${JSON.stringify(relay.copy('main'))}\n  store ${JSON.stringify(truth)}`);
  for (const c of clients) c.dispose();
  relay?.close();
  store.dispose();
  return steps;
}

async function campaign({ seed = 1, runs = 20, steps = 40, clients = 3, relay = false, only, trace = false } = {}) {
  let operations = 0;
  for (let run = only ?? 0; run < (only === undefined ? runs : only + 1); run++) {
    const runSeed = seed * 1000 + run;
    const rng = seededRandom(runSeed);
    const trail = [];
    try {
      operations += await oneRun(rng, { steps, clients, relay, trace }, trail);
    } catch (err) {
      const mode = relay ? ' --mode relay' : '';
      throw new Error(`fuzz failed (seed ${seed}, run ${run}): ${err.message}\nlast actions: ${trail.join(', ')}\nreproduce: node test/fuzz/run.js${mode} --seed ${seed} --runs ${run + 1} --steps ${steps} --clients ${clients}`, { cause: err });
    }
  }
  return operations;
}

export const runFuzz = options => campaign({ ...options, relay: false });
export const runRelayFuzz = options => campaign({ ...options, relay: true });
