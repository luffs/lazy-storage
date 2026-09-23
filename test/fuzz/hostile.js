// hostile.js - Seeded fuzzer with a client that does not play by the protocol
//
// Honest clients edit a shared store through the in-memory network, going
// offline and back, while an attacker (a signed-in user with a socket of
// its own) sends whatever it likes: ops under other replicas' ids and the
// server's, seqs far ahead, poisoned timestamps, reserved names, fragments,
// deletions of the skeleton, oversized hellos, shares, churn, and garbage.
// The attacker may write only its own namespace (`notes`); `validate` keeps
// it out of `tasks`. After every step, whatever the attacker did:
//
// - Object.prototype is untouched
// - the server's own writes still land
// - no honest client has heard an error, and every online one that has
//   nothing pending holds exactly the server's state
// - the skeleton (`initial`'s top-level containers) is there
// - no store was loaded for an id nobody may open
// - sessions do not outnumber the connections that hold them
//
// The attacker is told the honest clients' replica ids (presence shows
// them, and it tries them outright): knowing one must get it nothing, so
// an honest client is never refused its replica or has an edit dropped.
//
// At the end every honest client is online, all of them and the server
// agree, and a store loaded afresh from the same storage equals the live
// one. Deterministic from the seed.
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub, memoryStorage } from '../../src/server/index.js';
import { createClient } from '../../src/client/index.js';
import { createConnection } from '../../src/client/connection.js';
import { createNetwork, fakeTime, seededRandom } from '../helpers.js';

const INITIAL = { tasks: {}, notes: {} };
const USERS = ['alice', 'bob', 'carol'];
const ATTACKER = { id: 'mallory' };
const canon = value => JSON.stringify(sortKeys(value));
const sortKeys = value => (value && typeof value === 'object' && !Array.isArray(value)
  ? Object.fromEntries(Object.keys(value).sort().map(k => [k, sortKeys(value[k])]))
  : value);

/** An object with `key` as an own property, whatever the key (`__proto__` included), as JSON.parse makes it */
function own(key, value, base = {}) {
  Object.defineProperty(base, key, { value, enumerable: true, writable: true, configurable: true });
  return base;
}

/** What goes over a socket: the JSON of it, parsed back */
const wire = value => JSON.parse(JSON.stringify(value));

export async function runHostile({ seed = 1, runs = 20, steps = 40, log = () => {} } = {}) {
  let operations = 0;
  const prototypeNames = Object.getOwnPropertyNames(Object.prototype).sort().join();
  for (let run = 0; run < runs; run++) {
    const rng = seededRandom(seed * 7919 + run);
    const time = fakeTime(1_000_000_000);
    const storage = memoryStorage();
    const storeOptions = {
      initial: INITIAL,
      registers: ['order'],
      readOnly: ['locked'],
      presence: true,
      maxLeaves: 200,
      now: time,
      validate: (diff, { user }) => user?.id !== ATTACKER.id || !Object.hasOwn(diff, 'tasks')
    };
    const store = createStore({ ...storeOptions, storage });
    const loaded = [];
    const resolveStore = id => {
      loaded.push(id);
      return id === 'main' ? store : null;
    };
    const authorizeId = (user, id) => id === 'main';
    const hubOptions = { authorizeId };
    const net = createNetwork({ session: ({ send, user }) => createHub(resolveStore, { send, user, ...hubOptions }) });

    const honest = USERS.map((name, i) => {
      const link = net.link({ user: { id: name } });
      const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: false });
      // Replica ids no generated value could contain by accident
      const c = createClient({ connection, store: 'main', replicaId: `honest-${name}-${seed}-${run}`, initial: INITIAL, registers: ['order'], now: fakeTime(1_000_000_000 + rng.int(3000)) });
      c.link = link;
      c.name = name;
      c.errors = [];
      c.on('error', err => c.errors.push(err));
      c.on('closed', info => c.errors.push(Object.assign(new Error(`closed: ${info.code} ${info.message}`), { code: info.code })));
      c.connect();
      return c;
    });
    const honestIds = honest.map(c => c.replicaId);
    await net.settle();

    // The attacker drives a hub session by hand, as a socket of its own
    const attacker = { hub: null, heard: [], seq: 0, replicaId: `m-${run}` };
    const reconnectAttacker = () => {
      attacker.hub?.close();
      attacker.hub = createHub(resolveStore, { send: m => attacker.heard.push(wire(m)), user: ATTACKER, ...hubOptions });
    };
    reconnectAttacker();
    const send = message => attacker.hub.receive(wire(message));

    const ops = [];
    const fail = message => new Error(`Hostile fuzz failure (seed ${seed}, run ${run}):\n  ${message}\n  ops:\n    ${ops.slice(-25).join('\n    ')}\n  reproduce: node test/fuzz/run.js --mode hostile --seed ${seed} --runs ${run + 1} --steps ${steps}`);

    const someReplica = () => rng.pick([attacker.replicaId, attacker.replicaId, 'server', rng.pick(honestIds), `x${rng.int(1e6)}`]);
    const someSeq = () => rng.pick([++attacker.seq, attacker.seq, 1, 1e12, Number.MAX_SAFE_INTEGER, 0, -1, 1.5, '3']);
    const someTs = replicaId => rng.pick([
      [time(), rng.int(5), replicaId],
      [time(), rng.int(5), replicaId],
      [time() + 60_000, Number.MAX_SAFE_INTEGER, replicaId],
      [time() + 60_000, 2 ** 40, replicaId],
      [time() + 10 * 60_000, 0, replicaId],
      [time() - 60 * 86_400_000, 0, replicaId],
      [time(), 0, rng.pick(honestIds)],
      [time(), 0, 'server'],
      [-5, 0, replicaId],
      'yesterday',
      null
    ]);
    const someDiff = () => {
      const k = `k${rng.int(20)}`;
      switch (rng.int(16)) {
        case 0: return own('__proto__', { polluted: 'yes' });
        case 1: return { notes: own('__proto__', { polluted: 'yes' }) };
        case 2: return { notes: { [k]: { constructor: { prototype: { polluted: 'yes' } } } } };
        case 3: return { notes: { [k]: { $length: 2, 0: 'a' } } };
        case 4: return { notes: { [k]: { $splice: [[0, 0, ['x']]] } } };
        case 5: return { tasks: null };
        case 6: return { notes: null };
        case 7: return { tasks: { [k]: { id: k, title: 'forged' } } };
        case 8: return { locked: { x: 1 } };
        case 9: return { notes: { [k]: {} } };
        case 10: return { notes: { [k]: [{ nested: 'object' }] } };
        case 11: return { notes: Object.fromEntries(Array.from({ length: 250 }, (_, i) => [`n${i}`, i])) };
        case 12: return { order: own('__proto__', ['x']) };
        case 13: return { notes: { [k]: { deep: { deeper: { value: rng.int(9) } } } } };
        case 14: return 'not a diff';
        default: return { notes: { [k]: rng.int(100) } };
      }
    };
    const someOp = () => {
      const replicaId = someReplica();
      return { replicaId, seq: someSeq(), ts: someTs(replicaId), diff: someDiff() };
    };

    const attack = () => {
      const roll = rng.int(10);
      if (roll < 4) {
        const op = someOp();
        send({ t: 'op', store: 'main', op });
        return `attacker op ${op.replicaId}#${op.seq}`;
      }
      if (roll < 6) {
        const replicaId = someReplica();
        const many = rng.chance(0.1);
        const list = Array.from({ length: many ? 1200 : rng.int(4) }, () => ({ ...someOp(), replicaId }));
        send({ t: 'hello', store: 'main', replicaId, ops: list, since: rng.pick([0, 5, 1e9, 'x', undefined]), epoch: rng.pick(['bogus', null, undefined, store.epoch]), share: rng.pick([undefined, { cursor: 1 }, 'x'.repeat(5000), own('__proto__', { p: 1 })]) });
        return `attacker hello as ${replicaId} with ${list.length} ops`;
      }
      if (roll < 7) {
        send({ t: 'share', store: 'main', data: rng.pick([{ cursor: rng.int(9) }, null, 'x'.repeat(5000), own('constructor', { prototype: 1 })]) });
        return 'attacker share';
      }
      if (roll < 8) {
        send({ t: 'leave', store: 'main' });
        send({ t: 'hello', store: 'main', replicaId: someReplica(), ops: [] });
        return 'attacker leave + hello';
      }
      if (roll < 9) {
        const garbage = rng.pick([
          { t: 'op', store: 'main' },
          { t: 'op', store: 'main', op: 'x' },
          { t: 'hello', store: 'main' },
          { t: 'nonsense', store: 'main' },
          { t: 'hello', store: 'secret', replicaId: attacker.replicaId, ops: [] },
          { t: 'hello', store: '../etc/passwd', replicaId: attacker.replicaId, ops: [] },
          { t: 'hello', replicaId: attacker.replicaId, ops: [] },
          { store: 'main' },
          own('__proto__', { t: 'hello' }, { store: 'main' })
        ]);
        send(garbage);
        return `attacker garbage ${JSON.stringify(garbage).slice(0, 40)}`;
      }
      reconnectAttacker();
      return 'attacker reconnects';
    };

    const behave = c => {
      const tasks = c.collection('tasks');
      const roll = rng.next();
      if (roll < 0.35) {
        const id = tasks.add({ title: `t${operations}`, done: false });
        return `${c.name} add ${id}`;
      }
      if (roll < 0.55 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.update(id, { done: rng.chance(0.5), n: rng.int(100) });
        return `${c.name} update ${id}`;
      }
      if (roll < 0.65 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.remove(id);
        return `${c.name} remove ${id}`;
      }
      if (roll < 0.72) {
        c.state.order = tasks.ids().slice(0, 3);
        return `${c.name} order`;
      }
      if (roll < 0.8) {
        c.share({ at: rng.int(9) });
        return `${c.name} share`;
      }
      if (c.status === 'offline') {
        c.link.goOnline();
        c.connect();
        return `${c.name} online`;
      }
      c.link.goOffline();
      return `${c.name} offline`;
    };

    const check = step => {
      if (Object.getOwnPropertyNames(Object.prototype).sort().join() !== prototypeNames || ({}).polluted !== undefined) {
        throw fail(`Object.prototype was changed after step ${step}`);
      }
      const mark = `mark-${step}`;
      store.patch({ serverMark: mark });
      if (store.state.serverMark !== mark) throw fail(`the server's own write did not land after step ${step}`);
      for (const c of honest) {
        if (c.errors.length) throw fail(`${c.name} heard: ${c.errors[0].code ?? ''} ${c.errors[0].message}`);
      }
      if (!LazyWatch.Utils.isPlainObject(store.state.tasks) || !LazyWatch.Utils.isPlainObject(store.state.notes)) {
        throw fail(`a top-level container of initial is gone after step ${step}`);
      }
      const strangers = loaded.filter(id => id !== 'main');
      if (strangers.length) throw fail(`a store was loaded for ${strangers[0]}, which nobody may open`);
      const connections = honest.filter(c => c.link.current).length + 1;
      if (store.stats().sessions > connections) throw fail(`${store.stats().sessions} sessions for ${connections} connections after step ${step}`);
    };
    const converged = (c, where) => {
      if (canon(LazyWatch.snapshot(c.state)) !== canon(store.snapshot())) {
        throw fail(`${c.name} diverged ${where}\n  server: ${canon(store.snapshot())}\n  client: ${canon(LazyWatch.snapshot(c.state))}`);
      }
    };

    for (let step = 0; step < steps; step++) {
      time.advance(1 + rng.int(200));
      try {
        ops.push(rng.chance(0.55) ? attack() : behave(rng.pick(honest)));
      } catch (err) {
        throw fail(`step ${step} threw: ${err?.stack ?? err}`);
      }
      operations++;
      await net.settle();
      check(step);
      await net.settle();   // the server's mark reaches the clients
      for (const c of honest) if (c.status === 'online' && c.pending === 0) converged(c, `after step ${step}`);
    }

    for (const c of honest) {
      if (c.status === 'offline') {
        c.link.goOnline();
        c.connect();
      }
    }
    await net.settle();
    check('end');
    await net.settle();
    for (const c of honest) {
      if (c.pending !== 0) throw fail(`${c.name} still has ${c.pending} pending ops`);
      converged(c, 'at the end');
    }
    const reloaded = createStore({ ...storeOptions, storage });
    if (canon(reloaded.snapshot()) !== canon(store.snapshot())) {
      throw fail(`a store loaded afresh differs from the live one\n  live:   ${canon(store.snapshot())}\n  loaded: ${canon(reloaded.snapshot())}`);
    }
    reloaded.dispose();
    attacker.hub.close();
    for (const c of honest) c.dispose();
    store.dispose();
    log(`run ${run}: ${steps} steps ok`);
  }
  return operations;
}
