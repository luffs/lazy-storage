// relay.js - Seeded fuzzer: displays behind a relay, through outages and relay restarts
//
// Displays with skewed clocks edit two stores (tasks with a register, and a
// notes store) through a relay on their LAN, which passes their sockets
// through to the server or, while the server is away, answers them from
// its copies. Between the edits: a display's LAN link drops and comes back,
// every socket on the LAN blips (a hello at once, with the outbox), the
// server goes away and comes back, backs a store up and restores it (a new
// epoch), and the relay restarts, cleanly (it writes its copies as it
// closes) or as a crash (it starts from copies it wrote some time before).
//
// - Whenever the server is up and everything has settled, every display
//   that is on the LAN is online, has nothing pending, and equals the
//   server; the relay's copy of each store follows the server (a display
//   has it open) and equals it, epoch and version included
// - While the server is away, every display the relay answered that has
//   nothing pending equals the relay's copy (unless the copy was not
//   current when the server went, or the relay crashed since: a display
//   ahead of the copy keeps its newer state)
// - At the end everyone is back, and the first holds
//
// With `fanOut`, the relay reads each store once on a link of its own and
// fans it out (see src/relay and src/server/relays.js): every display but
// the first is vouched for with its credential, the first is passed
// through as before, and the steps also take the link alone away and
// back, and have the server sign a display out (it comes back judged
// afresh). The same checks hold.
//
// Deterministic from the seed, apart from the clients' own retry timers
// (a store unloaded under them says hello again in a moment), which only
// delay.
import { LazyWatch } from 'lazy-watch';
import { createStore, createHub, memoryStorage } from '../../src/server/index.js';
import { createRelayLink } from '../../src/server/relays.js';
import { createClient } from '../../src/client/index.js';
import { createConnection } from '../../src/client/connection.js';
import { createRelay, memoryCopies } from '../../src/relay/index.js';
import { createNetwork, fakeTime, seededRandom } from '../helpers.js';

const TASKS = { tasks: {}, order: [] };
const NOTES = { notes: {} };
const REGISTERS = ['order'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const canon = value => JSON.stringify(sortKeys(value));
const sortKeys = value => (value && typeof value === 'object' && !Array.isArray(value)
  ? Object.fromEntries(Object.keys(value).sort().map(k => [k, sortKeys(value[k])]))
  : value);

/** A memoryCopies holding what another had written at some point: what a crashed relay finds */
function copiesFrom(saved) {
  const storage = memoryCopies();
  for (const doc of saved?.copies ?? []) storage.write(doc.store, doc);
  if (saved?.known) storage.writeKnown(saved.known);
  return storage;
}

export async function runRelayFuzz({ seed = 1, runs = 20, steps = 30, clients: clientCount = 3, fanOut = false, log = () => {} } = {}) {
  let operations = 0;
  for (let run = 0; run < runs; run++) {
    const rng = seededRandom(seed * 104729 + run);
    const time = fakeTime(1_000_000);
    // The server's stores, loaded afresh from their storage after a restore
    const options = {
      main: { initial: TASKS, registers: REGISTERS, presence: true, rateLimit: false, now: time, storage: memoryStorage() },
      notes: { initial: NOTES, rateLimit: false, now: time, storage: memoryStorage() }
    };
    const stores = { main: createStore(options.main), notes: createStore(options.notes) };
    const storeOf = id => {
      if (!options[id]) return null;
      if (stores[id].disposed) stores[id] = createStore(options[id]);
      return stores[id];
    };
    let backup = null;
    const links = new Set();
    let down = false;
    // The server's sockets, each its user's and a way to close it with a code (a sign-out: 4001)
    const centralSockets = new Set();
    const centralNet = createNetwork({
      session: ({ send, user, onEvict }) => {
        const hub = createHub(storeOf, { send, user });
        const entry = { user, onEvict };
        centralSockets.add(entry);
        return { receive: m => hub.receive(m), close() { centralSockets.delete(entry); hub.close(); } };
      }
    });
    const upstreamFor = user => {
      const link = centralNet.link({ user });
      links.add(link);
      if (down) link.goOffline();
      return link.factory;
    };
    // Fan-out: the server's relay route, which knows each display by its credential
    const byKey = new Map();
    const relayLinks = new Set();
    const linkNet = createNetwork({
      session: ({ send, user }) => {
        const endpoint = createRelayLink(storeOf, { send, relay: user, admit: credential => (byKey.has(credential?.headers?.authorization) ? { user: byKey.get(credential.headers.authorization), expires: null } : null), rate: false, onError: err => { throw err; } });
        relayLinks.add(endpoint);
        return { receive: m => endpoint.receive(m), close() { relayLinks.delete(endpoint); endpoint.close(); } };
      }
    });
    const relayLink = linkNet.link({ user: { id: 'relay' } });
    let linkAway = false;
    const upLink = () => {
      if (down || linkAway) relayLink.goOffline();
      else relayLink.goOnline();
    };
    let storage = memoryCopies();
    let written = storage.load();   // what a crash would leave: the copies as last flushed
    const makeRelay = () => createRelay({ storage, grace: 0, probe: false, keepalive: false, jitter: 0, saveDelay: 60_000, now: time, onError: err => { throw err; }, ...(fanOut ? { link: relayLink.factory, linger: 50 } : {}) });
    let relay = makeRelay();
    const lanNet = createNetwork({
      session: ({ send, user, onEvict }) => relay.accept({ send, close: (code, reason) => onEvict(code, reason), key: user.key, upstream: upstreamFor(user), credential: fanOut && user.fan ? { headers: { authorization: user.key } } : undefined })
    });

    const displays = [];
    for (let i = 0; i < clientCount; i++) {
      // Fan-out: the first display is passed through as before, beside the others
      const user = { id: `d${i}`, key: `key-${i}`, fan: i > 0 || clientCount === 1 };
      byKey.set(user.key, { id: user.id, key: user.key });
      const link = lanNet.link({ user });
      const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: false, wake: false });
      const now = fakeTime(1_000_000 + rng.int(5000));   // skewed wall clocks
      const d = createClient({ connection, store: 'main', replicaId: `r${i}`, initial: TASKS, registers: REGISTERS, now });
      d.notes = createClient({ connection, store: 'notes', replicaId: `n${i}`, initial: NOTES, now });
      Object.assign(d, { link, time: now, name: `d${i}`, away: false, errors: [], fan: fanOut && user.fan });
      for (const c of [d, d.notes]) c.on('error', err => d.errors.push(err));
      d.connect();
      d.notes.connect();
      displays.push(d);
    }

    const ops = [];
    // What the run went through, for the log: steps with the server away in
    // which a display was answered by the relay, and offline edits the copies took
    const went = { answered: 0, applied: 0, compared: 0, fanned: 0 };
    const fail = message => new Error(`Relay fuzz failure (seed ${seed}, run ${run}):\n  ${message}\n  ops:\n    ${ops.slice(-30).join('\n    ')}\n  reproduce: node test/fuzz/run.js --mode ${fanOut ? 'fanout' : 'relay'} --seed ${seed} --runs ${run + 1} --steps ${steps} --clients ${clientCount}`);

    /** Deliver everything, and bring back every display whose socket closed (the relay closes them to switch modes), until quiet */
    async function settle() {
      for (let round = 0; round < 50; round++) {
        await centralNet.settle();
        await linkNet.settle();
        await lanNet.settle();
        await centralNet.settle();
        await linkNet.settle();
        let reconnected = false;
        for (const d of displays) {
          if (d.away || d.connection.status !== 'offline') continue;
          d.connection.connect();
          reconnected = true;
        }
        if (!reconnected && centralNet.pending === 0 && lanNet.pending === 0 && linkNet.pending === 0) return;
      }
      throw fail('the networks did not settle');
    }

    /** With the server up: wait (the clients' own retry timers) until every display that is there is online with nothing pending, then compare */
    async function converged(where) {
      const there = displays.filter(d => !d.away);
      for (let i = 0; i < 400; i++) {
        await settle();
        if (there.every(d => d.status === 'online' && d.notes.status === 'online' && d.pending === 0 && d.notes.pending === 0 && !d.relayed)) break;
        await sleep(5);
      }
      for (const d of displays) if (d.errors.length) throw fail(`${d.name} reported: ${d.errors[0].code ?? ''} ${d.errors[0].message}`);
      for (const d of there) {
        for (const [client, id] of [[d, 'main'], [d.notes, 'notes']]) {
          if (client.status !== 'online' || client.pending !== 0) throw fail(`${d.name} on ${id} is ${client.status} with ${client.pending} pending ${where}`);
          if (canon(LazyWatch.snapshot(client.wire)) !== canon(storeOf(id).snapshot())) {
            throw fail(`${d.name} on ${id} differs from the server ${where}\n  server:  ${canon(storeOf(id).snapshot())}\n  display: ${canon(LazyWatch.snapshot(client.wire))}`);
          }
        }
      }
      // Fan-out: the displays that are fanned out are read on the relay's
      // link, once per store, and listed at the server as themselves
      if (fanOut) {
        const stats = relay.stats();
        const fanned = there.filter(d => d.link.current && d.fan);
        if (fanned.length && stats.link.state !== 'open') throw fail(`the relay's link is ${stats.link.state} ${where}`);
        if (stats.sockets.fan < fanned.length) throw fail(`${stats.sockets.fan} sockets fanned out for ${fanned.length} displays ${where}`);
        if (fanned.length && stats.link.shared !== 2) throw fail(`the relay reads ${stats.link.shared} stores on its link ${where}`);
        const listed = new Set(storeOf('main').peers().map(p => p.replicaId));
        for (const d of fanned) if (!listed.has(d.replicaId)) throw fail(`${d.name} is not in the server's presence ${where}`);
        went.fanned = Math.max(went.fanned, fanned.length);
      }
      // A copy that someone on the LAN has open follows the server, and equals it
      for (const id of Object.keys(stores)) {
        const copy = relay.copy(id);
        if (there.length && !copy?.live) throw fail(`the relay's copy of ${id} does not follow the server ${where}`);
        if (copy?.live && (canon(copy.state) !== canon(storeOf(id).snapshot()) || copy.epoch !== storeOf(id).epoch || copy.v !== storeOf(id).version)) {
          throw fail(`the relay's copy of ${id} follows the server but differs ${where}\n  server: ${storeOf(id).epoch} v${storeOf(id).version} ${canon(storeOf(id).snapshot())}\n  copy:   ${copy.epoch} v${copy.v} ${canon(copy.state)}`);
        }
      }
    }

    /**
     * With the server away: every display the relay answered that has
     * nothing pending (its own edits laid over the copy's would differ)
     * holds what the relay's copy holds
     */
    function inStep(where) {
      for (const d of displays) {
        if (d.away) continue;
        for (const [client, id] of [[d, 'main'], [d.notes, 'notes']]) {
          if (client.status !== 'online' || !client.relayed || client.pending !== 0) continue;
          const copy = relay.copy(id);
          went.compared++;
          if (canon(LazyWatch.snapshot(client.wire)) !== canon(copy.state)) {
            throw fail(`${d.name} on ${id}, answered by the relay, differs from its copy ${where}\n  copy:    ${canon(copy.state)}\n  display: ${canon(LazyWatch.snapshot(client.wire))}`);
          }
        }
      }
    }
    let trusted = true;

    await settle();
    await converged('at the start');

    // A device the server let in that does not play by the protocol: its
    // own client said hello through the relay once (so the relay knows its
    // credential and replica), and from then on a raw socket of its own
    // sends whatever it likes, through or local. What it gets away with
    // offline shows on the LAN until the server is back, and no further
    const mallory = { user: { id: 'mallory', key: 'key-m', fan: true }, session: null, relay: null, seq: 0 };
    byKey.set('key-m', { id: 'mallory', key: 'key-m' });
    {
      const link = lanNet.link({ user: mallory.user });
      const connection = createConnection({ transport: link.factory, reconnect: false, keepalive: false, wake: false });
      const own = [createClient({ connection, store: 'main', replicaId: 'rm', initial: TASKS, registers: REGISTERS, now: time }), createClient({ connection, store: 'notes', replicaId: 'nm', initial: NOTES, now: time })];
      for (const c of own) c.connect();
      await settle();
      for (const c of own) c.dispose();
    }
    const attacker = () => {
      if (mallory.relay !== relay || mallory.session.state === 'closed') {
        mallory.relay = relay;
        mallory.session = relay.accept({ send() {}, close() {}, key: mallory.user.key, upstream: upstreamFor(mallory.user), credential: fanOut ? { headers: { authorization: rng.pick(['key-m', 'key-m', 'key-0', 'nobody']) } } : undefined });
      }
      return mallory.session;
    };
    const own = (key, value, base = {}) => Object.defineProperty(base, key, { value, enumerable: true, writable: true, configurable: true });
    const someTs = () => rng.pick([[time(), rng.int(5), 'rm'], [time(), rng.int(5), 'rm'], [time() + 4 * 60_000, 0, 'rm'], [time() + 10 * 60_000, 0, 'rm'], [time(), 0, 'r0'], 'yesterday', [-1, 0, 'rm']]);
    const someDiff = () => {
      const k = `m${rng.int(4)}`;
      switch (rng.int(9)) {
        case 0: return own('__proto__', { polluted: 'yes' });
        case 1: return { tasks: own('__proto__', { polluted: 'yes' }) };
        case 2: return { tasks: { [k]: { constructor: { prototype: { polluted: 'yes' } } } } };
        case 3: return { tasks: null };
        case 4: return { tasks: { [k]: { $splice: [[0, 0, ['y']]] } } };
        case 5: return { tasks: { [k]: [{ nested: 'object' }] } };
        case 6: return 'not a diff';
        case 7: return { order: [k, 'x'] };
        default: return { tasks: { [k]: { id: k, title: `mallory ${rng.int(9)}` } } };
      }
    };
    const someOp = () => ({ replicaId: rng.pick(['rm', 'rm', 'rm', 'r0', 'server']), seq: rng.pick([++mallory.seq, mallory.seq, 1, 1e12, -1, 1.5]), ts: someTs(), diff: someDiff() });
    const attack = () => {
      const s = attacker();
      const store = rng.pick(['main', 'main', 'notes', '../etc', undefined]);
      const message = rng.pick([
        () => ({ t: 'hello', store, replicaId: rng.pick(['rm', 'rm', 'r0', 'nm', 'x']), ops: Array.from({ length: rng.int(3) }, someOp), since: rng.pick([0, 1e9, undefined, 'x']), epoch: rng.pick([null, 'bogus', undefined]), share: rng.pick([undefined, { at: 1 }, 'x'.repeat(5000)]) }),
        () => ({ t: 'op', store, op: someOp() }),
        () => ({ t: 'op', store, op: someOp() }),
        () => ({ t: 'share', store, data: rng.pick([{ at: rng.int(9) }, null, 'x'.repeat(5000), own('__proto__', { p: 1 })]) }),
        () => ({ t: 'leave', store }),
        () => rng.pick([null, 'text', [], { t: 'op' }, { t: 'nonsense', store: 'main' }, own('__proto__', { t: 'hello' }, { store: 'main' })])
      ])();
      s.receive(JSON.parse(JSON.stringify(message)));   // as a socket delivers it
      return `mallory sends ${JSON.stringify(message)?.slice(0, 60)}`;
    };
    const prototypeNames = Object.getOwnPropertyNames(Object.prototype).sort().join();

    for (let step = 0; step < steps; step++) {
      const d = rng.pick(displays);
      d.time.advance(1 + rng.int(50));
      time.advance(1 + rng.int(20));
      const tasks = d.collection('tasks');
      const roll = rng.next();
      if (roll < 0.22) {
        ops.push(`${d.name} add ${tasks.add({ title: `t${step}`, done: false, n: rng.int(100) })}`);
      } else if (roll < 0.4 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.update(id, rng.chance(0.5) ? { done: rng.chance(0.5) } : { title: `e${step}`, n: rng.int(100) });
        ops.push(`${d.name} update ${id}`);
      } else if (roll < 0.48 && tasks.ids().length) {
        const id = rng.pick(tasks.ids());
        tasks.remove(id);
        ops.push(`${d.name} remove ${id}`);
      } else if (roll < 0.55) {
        const ids = tasks.ids();
        for (let i = ids.length - 1; i > 0; i--) { const j = rng.int(i + 1); [ids[i], ids[j]] = [ids[j], ids[i]]; }
        d.state.order = ids;
        ops.push(`${d.name} order ${ids.length}`);
      } else if (roll < 0.58) {
        d.notes.collection('notes').add({ text: `n${step}` });
        ops.push(`${d.name} note`);
      } else if (roll < 0.62) {
        for (let i = 1 + rng.int(4); i > 0; i--) ops.push(attack());
      } else if (roll < 0.67 && d.canUndo) {
        d.undo();
        ops.push(`${d.name} undo`);
      } else if (roll < 0.71) {
        d.away = !d.away;
        if (d.away) d.link.goOffline();
        else d.link.goOnline();
        ops.push(`${d.name} ${d.away ? 'leaves the LAN' : 'is back on the LAN'}`);
      } else if (roll < 0.76) {
        // The socket drops and comes straight back: a hello at once, the outbox in it
        for (const x of displays) {
          if (x.away) continue;
          x.link.goOffline();
          x.link.goOnline();
        }
        ops.push('every socket on the LAN blips');
      } else if (roll < 0.78) {
        // A backup taken, or put back: a new epoch, which every display hears as a reset and the relay's copy takes
        if (!backup || rng.chance(0.5)) {
          backup = storeOf('main').export();
          ops.push('the server backs main up');
        } else {
          storeOf('main').restore(backup);
          ops.push('the server restores main from its backup');
        }
      } else if (fanOut && roll < 0.8 && !down) {
        if (rng.chance(0.5)) {
          // The relay's link alone drops, and comes back: the relay answers on its own meanwhile (grace is 0 here)
          linkAway = !linkAway;
          // With the server still up, a display's edits go on up its own socket and are acknowledged while the
          // copy hears nothing of them: it may be ahead of the copy once the relay answers on its own
          if (linkAway) trusted = false;
          upLink();
          // The host's word, as when its own watch on the server says so
          if (!linkAway) relay.upstreamUp();
          ops.push(linkAway ? 'the relay\'s link drops' : 'the relay\'s link is back');
        } else {
          // The server signs a display out: its sockets end, through the relay and its own, and it comes back judged afresh
          const who = rng.pick(displays);
          for (const endpoint of relayLinks) endpoint.disconnect(u => u?.id === who.name);
          for (const entry of [...centralSockets]) if (entry.user?.id === who.name) entry.onEvict(4001, 'Reauthenticate');
          ops.push(`the server signs ${who.name} out`);
        }
      } else if (roll < 0.85) {
        down = !down;
        if (down) {
          // Offline, a display may be ahead of a copy that was not following
          // (nobody had it open) or was written before a crash: the local
          // check below holds only for copies that were current
          trusted = Object.keys(stores).every(id => relay.copy(id)?.live);
          for (const link of links) link.goOffline();
          upLink();
          relay.upstreamDown();
        } else {
          for (const link of links) link.goOnline();
          upLink();
          relay.upstreamUp();
        }
        ops.push(down ? 'the server goes away' : 'the server is back');
      } else if (roll < 0.9) {
        relay.flush();
        written = storage.load();
        ops.push('the relay writes its copies');
      } else if (roll < 0.95) {
        relay.close();
        relay = makeRelay();
        if (down) relay.upstreamDown();
        ops.push('the relay restarts');
      } else {
        relay.close();
        storage = copiesFrom(written);
        relay = makeRelay();
        if (down) relay.upstreamDown();
        trusted = false;
        ops.push('the relay crashes, and starts from what it last wrote');
      }
      operations++;
      await settle();
      if (Object.getOwnPropertyNames(Object.prototype).sort().join() !== prototypeNames || ({}).polluted !== undefined) {
        throw fail(`Object.prototype was changed after step ${step}`);
      }
      if (!down && !linkAway) await converged(`after step ${step}`);
      else {
        if (displays.some(x => x.relayed && x.status === 'online')) went.answered++;
        went.applied = Math.max(went.applied, (relay.copy('main')?.local ?? 0) + (relay.copy('notes')?.local ?? 0));
        if (trusted) inStep(`after step ${step}`);
      }
    }

    if (down || linkAway) {
      down = false;
      linkAway = false;
      for (const link of links) link.goOnline();
      upLink();
      relay.upstreamUp();
    }
    for (const d of displays) {
      if (!d.away) continue;
      d.away = false;
      d.link.goOnline();
    }
    await converged('at the end');
    for (const d of displays) { d.notes.dispose(); d.dispose(); }
    relay.close();
    for (const store of Object.values(stores)) store.dispose();
    log(`run ${run}: ${steps} steps ok, ${went.answered} of them answered by the relay, up to ${went.applied} offline edits in its copies, ${went.compared} displays compared with a copy${fanOut ? `, up to ${went.fanned} displays fanned out` : ''}`);
  }
  return operations;
}
