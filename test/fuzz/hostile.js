// hostile.js - Seeded fuzzer with a client that does not play by the protocol
//
// Honest clients edit a shared store through the in-memory network, going
// offline and back, while an attacker (a signed-in user with a socket of
// its own) sends whatever it likes: ops under other replicas' ids and the
// server's, seqs far ahead, timestamps far ahead, far behind and not
// numbers at all, reserved names as collections, ids and path segments,
// changes of every wrong shape, deletions of collections and singletons,
// oversized ops and hellos, a follower's hello and ops under clients nobody
// vouched for, shares, churn, and garbage. The attacker may write `notes`
// only: a rule keeps it out of `tasks`, and `locked` is the server's. After
// every step, whatever the attacker did:
//
// - Object.prototype is untouched
// - the server's own writes still land
// - no honest client has heard an error or had an op refused, and every
//   online one that has nothing pending holds exactly the server's state
// - the collections and the singleton are there
// - no store was loaded for an id nobody may open
// - sessions do not outnumber the connections that hold them
//
// A timestamp far ahead is clamped to the server's time, so it wins
// nothing: an honest edit made after it is newer. The attacker is told the
// honest clients' replica ids (presence shows them, and it tries them
// outright): knowing one must get it nothing, so an honest client is never
// refused its replica or has an edit dropped.
//
// At the end every honest client is online, all of them and the server
// agree, and a store loaded afresh from the same storage equals the live
// one. Deterministic from the seed.
import { LazyWatch } from 'lazy-watch';
import { isDeepStrictEqual } from 'node:util';
import { createStore, createHub, memoryStorage } from '../../src/server/index.js';
import { createClient } from '../../src/client/index.js';
import { createConnection } from '../../src/client/connection.js';
import { createNetwork, fakeTime, seededRandom } from '../helpers.js';
import { PROTOCOL } from '../../src/core/index.js';

const SHAPE = {
  initial: { tasks: {}, notes: {}, locked: {}, settings: { order: [] } },
  singletons: ['settings']
};
const USERS = ['alice', 'bob', 'carol'];
const ATTACKER = { id: 'mallory' };
const DAY = 86_400_000;

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
      ...SHAPE,
      rules: { tasks: user => user?.id !== ATTACKER.id },
      readOnly: ['locked'],
      presence: true,
      maxChanges: 200,
      now: time
    };
    const store = createStore({ ...storeOptions, storage });
    const loaded = [];
    const resolveStore = id => {
      loaded.push(id);
      return id === 'main' ? store : null;
    };
    const authorizeId = (user, id) => id === 'main';
    const net = createNetwork({ session: ({ send, user }) => createHub(resolveStore, { send, user, authorizeId }) });

    const honest = USERS.map(name => {
      const link = net.link({ user: { id: name } });
      const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: false });
      // Replica ids no generated value could contain by accident, and clocks a few seconds apart
      const c = createClient({ connection, store: 'main', replicaId: `honest-${name}-${seed}-${run}`, ...SHAPE, now: fakeTime(1_000_000_000 + rng.int(3000)) });
      c.link = link;
      c.name = name;
      c.errors = [];
      c.on('error', err => c.errors.push(err));
      c.on('rejected', e => c.errors.push(Object.assign(new Error(`rejected: ${e.message}`), { code: e.code })));
      c.on('closed', info => c.errors.push(Object.assign(new Error(`closed: ${info.message}`), { code: info.code })));
      c.connect();
      return c;
    });
    const honestIds = honest.map(c => c.replicaId);
    await net.settle();

    // The attacker drives a hub session by hand, as a socket of its own
    const attacker = { hub: null, heard: [], seq: 0, replica: null, lives: 0 };
    // A new socket, and a new replica: one an op far ahead in seq has spent stays spent
    const reconnectAttacker = () => {
      attacker.hub?.close();
      attacker.replica = `m-${run}-${attacker.lives++}`;
      attacker.seq = 0;
      attacker.hub = createHub(resolveStore, { send: m => attacker.heard.push(wire(m)), user: ATTACKER, authorizeId });
    };
    const send = message => attacker.hub.receive(wire(message));
    // Most of the time it says hello as itself first, as a client would, so its ops are its own
    const freshAttacker = () => {
      reconnectAttacker();
      if (rng.chance(0.8)) send({ t: 'hello', proto: PROTOCOL, store: 'main', replica: attacker.replica, ops: [] });
    };
    freshAttacker();

    const trail = [];
    const fail = message => new Error(`Hostile fuzz failure (seed ${seed}, run ${run}):\n  ${message}\n  ops:\n    ${trail.slice(-25).join('\n    ')}\n  reproduce: node test/fuzz/run.js --mode hostile --seed ${seed} --runs ${run + 1} --steps ${steps}`);

    const someReplica = () => rng.pick([attacker.replica, attacker.replica, 'server', rng.pick(honestIds), `x${rng.int(1e6)}`]);
    const someSeq = () => rng.pick([++attacker.seq, attacker.seq, 1, 1e12, Number.MAX_SAFE_INTEGER, 0, -1, 1.5, '3']);
    const someTs = () => rng.pick([
      time(), time(), time() - rng.int(1000),
      time() + 60_000, time() + 365 * DAY, Number.MAX_SAFE_INTEGER,
      time() - 60 * DAY,
      -5, 1.5, 'yesterday', null, [time(), 0, 'r']
    ]);
    const someChanges = () => {
      const k = `k${rng.int(20)}`;
      switch (rng.int(32)) {   // a third of them the plain write the attacker may make
        case 0: return [{ c: '__proto__', id: k, put: { polluted: 'yes' } }];
        case 1: return [{ c: 'notes', id: '__proto__', put: { polluted: 'yes' } }];
        case 2: return [{ c: 'notes', id: k, path: ['constructor', 'prototype'], set: 'yes' }];
        case 3: return [{ c: 'notes', id: k, put: own('__proto__', { polluted: 'yes' }) }];
        case 4: return [{ c: 'notes', id: k, put: { $length: 2, 0: 'a' } }];
        case 5: return [{ c: 'tasks', del: true }];
        case 6: return [{ c: 'settings', del: true }];
        case 7: return [{ c: 'tasks', id: k, put: { title: 'forged' } }];
        case 8: return [{ c: 'locked', id: k, put: { x: 1 } }];
        case 9: return [{ c: 'notes', id: k, put: {} }];
        case 10: return [{ c: 'notes', id: k, path: ['list'], set: [{ nested: 'object' }, own('__proto__', { p: 1 })] }];
        case 11: return Array.from({ length: 250 }, (_, i) => ({ c: 'notes', id: `n${i}`, put: { i } }));
        case 12: return [{ c: 'settings', path: ['order'], set: own('__proto__', ['x']) }];
        case 13: return [{ c: 'notes', id: k, path: ['deep', 'deeper'], put: { value: rng.int(9) } }];
        case 14: return 'not changes';
        case 15: return [{ c: 'notes', id: k, put: { a: 1 }, set: 2 }];
        case 16: return [{ c: 'nope', id: k, put: {} }];
        case 17: return [{ c: 'settings', id: k, put: {} }];
        case 18: return [{ c: 'notes', put: { a: 1 } }];
        case 19: return [{ c: 'notes', id: k, path: 'a', set: 1 }];
        case 20: return [{ c: 'notes', id: k, set: 'a record that is not an object' }];
        default: return [{ c: 'notes', id: k, put: { n: rng.int(100) } }];
      }
    };
    // Half are well formed but for what they change and when, so that what the attacker may write lands
    const someOp = () => (rng.chance(0.5)
      ? { replica: attacker.replica, seq: ++attacker.seq, ts: rng.pick([time(), time() - rng.int(1000), time() + 60_000, time() + 365 * DAY, time() - 60 * DAY]), changes: someChanges() }
      : { replica: someReplica(), seq: someSeq(), ts: someTs(), changes: someChanges() });

    const attack = () => {
      const roll = rng.int(11);
      if (roll < 4) {
        const op = someOp();
        send({ t: 'op', store: 'main', op });
        return `attacker op ${op.replica}#${op.seq}`;
      }
      if (roll < 6) {
        const replica = someReplica();
        const many = rng.chance(0.1);
        const list = Array.from({ length: many ? 1200 : rng.int(4) }, () => ({ ...someOp(), replica }));
        send({ t: 'hello', proto: PROTOCOL, store: 'main', replica, ops: list, since: rng.pick([0, 5, 1e9, 'x', undefined]), epoch: rng.pick(['bogus', null, undefined, store.epoch]), clocks: rng.chance(0.5), share: rng.pick([undefined, { cursor: 1 }, 'x'.repeat(5000), own('__proto__', { p: 1 })]) });
        return `attacker hello as ${replica} with ${list.length} ops`;
      }
      if (roll < 7) {
        send({ t: 'share', store: 'main', data: rng.pick([{ cursor: rng.int(9) }, null, 'x'.repeat(5000), own('constructor', { prototype: 1 })]) });
        return 'attacker share';
      }
      if (roll < 8) {
        send({ t: 'leave', store: 'main' });
        send({ t: 'hello', proto: PROTOCOL, store: 'main', replica: someReplica(), ops: [] });
        return 'attacker leave + hello';
      }
      if (roll < 9) {
        // A follower's hello: nobody it names was vouched for, and this hub vouches for nobody
        const as = `as${rng.int(3)}`;
        send({ t: 'leave', store: 'main' });
        send({ t: 'hello', proto: PROTOCOL, store: 'main', replica: attacker.replica, ops: [], follower: true });
        send({ t: 'vouch', store: 'main', as, credential: { headers: { authorization: 'Bearer alice' } }, replica: rng.pick(honestIds) });
        send({ t: 'op', store: 'main', as, op: { replica: rng.pick(honestIds), seq: 1e9, ts: time(), changes: [{ c: 'tasks', id: 'x', put: { title: 'as alice' } }] } });
        send({ t: 'share', store: 'main', as, data: { cursor: 1 } });
        return `attacker as a follower, speaking as ${as}`;
      }
      if (roll < 10) {
        const garbage = rng.pick([
          { t: 'op', store: 'main' },
          { t: 'op', store: 'main', op: 'x' },
          { t: 'hello', proto: PROTOCOL, store: 'main' },
          { t: 'hello', proto: 1, store: 'main', replica: attacker.replica, ops: [] },
          { t: 'nonsense', store: 'main' },
          { t: 'hello', proto: PROTOCOL, store: 'secret', replica: attacker.replica, ops: [] },
          { t: 'hello', proto: PROTOCOL, store: '../etc/passwd', replica: attacker.replica, ops: [] },
          { t: 'hello', proto: PROTOCOL, replica: attacker.replica, ops: [] },
          { store: 'main' },
          own('__proto__', { t: 'hello' }, { store: 'main' })
        ]);
        send(garbage);
        return `attacker garbage ${JSON.stringify(garbage).slice(0, 40)}`;
      }
      freshAttacker();
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
      if (roll < 0.62 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.remove(id);
        return `${c.name} remove ${id}`;
      }
      if (roll < 0.68) {
        c.state.settings.order = tasks.ids().slice(0, 3);
        return `${c.name} order`;
      }
      if (roll < 0.74) {
        const k = `k${rng.int(20)}`;
        c.state.notes[k] = { by: c.name, n: rng.int(100) };
        return `${c.name} note ${k}`;
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
      store.patch({ settings: { mark } });
      if (store.state.settings.mark !== mark) throw fail(`the server's own write did not land after step ${step}`);
      for (const c of honest) {
        if (c.errors.length) throw fail(`${c.name} heard: ${c.errors[0].code ?? ''} ${c.errors[0].message}`);
      }
      for (const key of Object.keys(SHAPE.initial)) {
        if (!LazyWatch.Utils.isPlainObject(store.state[key])) throw fail(`${key} is gone after step ${step}`);
      }
      const strangers = loaded.filter(id => id !== 'main');
      if (strangers.length) throw fail(`a store was loaded for ${strangers[0]}, which nobody may open`);
      if (Object.keys(store.state.locked).length) throw fail(`a client wrote the server's own collection after step ${step}`);
      for (const task of Object.values(store.state.tasks)) {
        if (task.title === 'forged' || task.title === 'as alice') throw fail(`the attacker wrote a task after step ${step}`);
      }
      const connections = honest.filter(c => c.link.current).length + 1;
      if (store.stats().sessions > connections) throw fail(`${store.stats().sessions} sessions for ${connections} connections after step ${step}`);
    };
    const converged = (c, where) => {
      const mine = LazyWatch.snapshot(c.state);
      const truth = store.snapshot();
      if (!isDeepStrictEqual(mine, truth)) {
        throw fail(`${c.name} diverged ${where}\n  server: ${JSON.stringify(truth)}\n  client: ${JSON.stringify(mine)}`);
      }
    };

    for (let step = 0; step < steps; step++) {
      time.advance(1 + rng.int(200));
      try {
        trail.push(rng.chance(0.55) ? attack() : behave(rng.pick(honest)));
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
    store.flush();
    const reloaded = createStore({ ...storeOptions, storage });
    if (!isDeepStrictEqual(reloaded.snapshot(), store.snapshot())) {
      throw fail(`a store loaded afresh differs from the live one\n  live:   ${JSON.stringify(store.snapshot())}\n  loaded: ${JSON.stringify(reloaded.snapshot())}`);
    }
    reloaded.dispose();
    attacker.hub.close();
    for (const c of honest) c.dispose();
    store.dispose();
    log(`run ${run}: ${steps} steps ok`);
  }
  return operations;
}
