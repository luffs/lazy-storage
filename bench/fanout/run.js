// run.js - How a store that every client follows behaves when the server
// publishes to it, and when its clients write to it, with thousands of
// clients that do not hear presence (all but a few admins'), straight to
// the server and through fan-out relays.
//   bun bench/fanout/run.js [--n 4000] [--procs 16] [--topologies direct,hub,hubs]
//     [--hubs 50] [--hub-procs 8] [--admins 2] [--rates 5,20,50,100,200,500]
//     [--seconds 5] [--size 150] [--big 4096] [--burst 200] [--storage sqlite]
//     [--presence-every 250] [--writers 400] [--write-rates 50,200,500,1000]
//     [--phases idle,publish,big,burst,write,storm] [--idle-seconds 3] [--busy 0.5]
//     [--profile <dir>] [--remote host:port] [--json out.json]
//   bun bench/fanout/run.js --url wss://host/path [--clients 1000,5000,10000]
//     [--store loadtest] [--stores 1] [--auth-param id] [--query k=v&...]
//     [--per-proc 1000] [--insecure] [--writers 400] [--write-rates 1,10,100]
//     [--size 150] [--phases idle,write,storm] ...
//   A topology may name its store's presence: `hubs:off`, `direct:0` (a flush a
//   turn); otherwise --presence-every holds (250 ms; 'off' for none)
//
// The write phases have `--writers` of the clients (spread over the client
// processes, and so over the relays) write the store between them at each
// rate, as live ops on their own sockets: straight to the server, or
// through their relay, which sends each writer's ops up a write-only socket
// of the writer's own. Every op is a patch every client hears, as the
// server's are, and its author times its ack besides. The server's CPU per
// op is what one store costs its process for each write, fan-out included;
// to see the write path alone, run it with few readers (`--n 200 --writers
// 200 --phases write`). `--profile <dir>` has the server and the relay
// processes write a CPU profile each there (Bun's --cpu-prof-md), over the
// whole run: narrow it with --topologies and --phases
//
// `--remote host:port` puts the server on another machine, where agent.js
// runs (see its header) and starts a fresh one for each topology; the relays
// and the clients stay here and dial it there. Latency compares the times a
// patch carries with the clock of the client that hears it, so the server's
// clock is measured against this one's (the round trip to it that took
// least, of 20) and the server stamps its patches in this machine's time:
// good to half that round trip, which the header of each topology says. The
// machine readout is then this machine's, the relays' and the clients'; the
// server's CPU is its own, on its own machine
//
// `--url` runs clients alone, against a deployment already up (a server, or
// Caddy in front of relays: whatever its clients would dial), to see how many
// it takes: `--clients` steps up how many are connected, and each step runs
// the phases with all of them (the server's own phases, publish, big and
// burst, need the bench's server, so they are not among them). The clients
// open `--store` (or `--stores` of them, `<store>-0` and on, the clients
// dealt round them), named by `--auth-param` in the query as the deployment's
// `authenticate` reads it, with `--query` added (a token of a test user, say);
// `--insecure` takes any TLS certificate (Caddy's own, on localhost). A step
// says how long its new clients took to be answered; its writes, the patches
// every client of the store hears and the acks, timed on this machine alone;
// and its storm (`storm` in --phases, in the other mode too) every client
// dropped at once and back as a client comes back after a drop, answered
// with the delta since its version: how long until all were answered again.
// What the deployment spends doing it is its own machine's to watch (`top`,
// `podman stats`), step by step. The writers write `feed.w-<id>` in the
// stores, which the deployment keeps: point it at stores nobody reads. One
// machine opens some 16 000 sockets to one address on Windows and 28 000 on
// Linux before its ports run out (then sockets fail to open): past that, more
// machines, or a wider range of ports (Windows: netsh int ipv4 set
// dynamicport tcp start=10000 num=55000). A socket this machine closes keeps
// its port a minute or two after (TIME_WAIT, the closing side's), so a storm
// needs ports for twice the clients, and a run straight after another finds
// the last one's still held: wait two minutes between runs. (A real storm,
// the server restarting, leaves those on the server's machine.)
//
// Every part is a process of its own (the server, each process of relays, each
// process of clients), so each one's CPU is its own (on Windows its cycle count,
// as process.cpuUsage() there moves in 15.6 ms steps); the machine's cores are
// shared, so read CPU seconds, not wall time alone, and check that the clients
// had room (their busiest process is reported). Every phase also says how many
// of the machine's logical CPUs its processes kept busy between them, and one
// past `--busy` of them (half: with two threads a core, as on most machines and
// most VMs' vCPUs, the cores themselves are all in use by then) is flagged
// MACHINE BUSY: what it measured is the machine, not the server or the
// relays. The relays and the clients cost the most of it: every patch is a
// write to each of thousands of sockets and a read from each, whichever relays
// make them, and each relay parses, applies and logs the patch itself besides.
// A patch carries the time it was due and the time it went out, so a server
// behind its schedule shows as latency, and what delivery took shows apart.
// Before each phase whatever is still on its way is let arrive, and a phase
// counts only its own versions.
// A rate is given up on (and the higher ones skipped) once a patch is missing,
// a client is cut off or told to say hello again, or the p99 passes 2 s.
//   direct: every client a socket of the server's
//   hub:    every client behind one relay (one store's hub with all of them)
//   hubs:   the clients spread over many relays (a chain: a hub per shop),
//           many relays to a process here, where each would be a box of its own.
//           What the server does behind many relays shows with few clients to
//           each, so the relays' own work leaves it room: --topologies hubs
//           --n 400 --hubs 200
import { tmpdir } from 'node:os';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { parseArgs, newHistogram, addInto, percentile, cycleRate, clock, controlLine, logicalCpus } from './common.js';

const args = parseArgs(process.argv.slice(2));
const N = Number(args.n || 4000);
const PROCS = Number(args.procs || 16);
const TOPOLOGIES = String(args.topologies || 'direct,hub,hubs').split(',');
const HUBS = Number(args.hubs || 50);
const HUB_PROCS = Number(args['hub-procs'] || 8);
const ADMINS = Number(args.admins ?? 2);
const RATES = String(args.rates || '5,20,50,100,200,500').split(',').map(Number);
const SECONDS = Number(args.seconds || 5);
const SIZE = Number(args.size || 150);
const BIG = Number(args.big || 4096);
const BURST = Number(args.burst || 200);
const PRESENCE = String(args['presence-every'] ?? 250);
// A deployment's address (--url): clients alone, no server or relays of the bench's own
const TARGET = typeof args.url === 'string' ? args.url : null;
const WRITERS = Number(args.writers || 400);
const WRITE_RATES = String(args['write-rates'] || (TARGET ? '1,10,100' : '50,200,500,1000')).split(',').map(Number);
const PHASES = new Set(String(args.phases || (TARGET ? 'idle,write,storm' : 'idle,publish,big,burst,write')).split(','));
const IDLE = Number(args['idle-seconds'] || 3);
const PROFILE = typeof args.profile === 'string' ? resolve(args.profile) : null;
const CPUS = logicalCpus();
const BUSY = Number(args.busy || 0.5);
// agent.js's address on the server's machine, host:port
const REMOTE = typeof args.remote === 'string' ? (([, host, port]) => ({ host, port: Number(port) }))(args.remote.match(/^(.+):(\d+)$/) ?? []) : null;
if (REMOTE && !(REMOTE.host && REMOTE.port)) throw new Error(`--remote takes the agent's host:port, not ${args.remote}`);
if (TARGET && REMOTE) throw new Error('--url runs clients alone, against a server already up: --remote starts one, so not both');
if (TARGET && !/^wss?:\/\//.test(TARGET)) throw new Error(`--url takes a ws:// or wss:// address, not ${TARGET}`);
const STEPS = String(args.clients || '1000').split(',').map(Number);
if (TARGET && !STEPS.every((n, k) => Number.isInteger(n) && n > 0 && (k === 0 || n > STEPS[k - 1]))) throw new Error(`--clients takes counts going up, not ${args.clients}`);
const STORE_COUNT = Number(args.stores || 1);
const STORES = STORE_COUNT > 1 ? Array.from({ length: STORE_COUNT }, (_, k) => `${args.store || 'loadtest'}-${k}`) : [String(args.store || 'loadtest')];
const AUTH = String(args['auth-param'] || 'id');
const QUERY = typeof args.query === 'string' ? args.query : '';
const PER_PROC = Number(args['per-proc'] || 1000);
// In every replica's id, so a store the deployment kept from an earlier run takes this one's ops as new
const RUN = Date.now().toString(36);
const here = import.meta.dir;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const CYCLE_HZ = cycleRate();
if (PROFILE) mkdirSync(PROFILE, { recursive: true });

/** GET a control route (see common.js's control), answered as JSON */
const getter = (host, port) => path => fetch(`http://${host}:${port}${path}`, { signal: AbortSignal.timeout(60_000) }).then(r => r.json());

const children = [];
/** A child process, and its control server once it says its port; `profile` names the CPU profile it writes (see --profile) */
async function spawn(file, env, profile) {
  const flags = PROFILE && profile ? ['--cpu-prof-md', `--cpu-prof-dir=${PROFILE}`, `--cpu-prof-name=${profile}.md`] : [];
  const child = Bun.spawn([process.execPath, ...flags, join(here, file)], {
    env: { ...process.env, ...(CYCLE_HZ ? { CYCLE_HZ: String(CYCLE_HZ) } : {}), ...env },
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'inherit'
  });
  children.push(child);
  const info = await controlLine(child, file);
  return { ...info, host: '127.0.0.1', child, get: getter('127.0.0.1', info.control) };
}

// The server's machine (--remote): agent.js there starts a server for each
// topology, and stops it after, as spawn and stopAll do the processes here
const agent = REMOTE ? { host: REMOTE.host, get: getter(REMOTE.host, REMOTE.port) } : null;
let remoteUp = false;
/** The server, on this machine or on the agent's */
async function startServer(env, profile) {
  if (!agent) return spawn('server.js', env, profile);
  // What the server is told of the environment it runs in here, when set; its
  // database, ports and CPU profile's place are the agent's to choose
  const { DB: _db, ...rest } = env;
  const passed = Object.fromEntries(['KEYS', 'DEFLATE'].filter(k => process.env[k] !== undefined).map(k => [k, process.env[k]]));
  const query = new URLSearchParams({ ...passed, ...rest, ...(PROFILE ? { profile } : {}) });
  remoteUp = true;
  const info = await agent.get(`/start?${query}`);
  if (info.error) throw new Error(`the agent could not start the server: ${info.error}`);
  return { ...info, host: agent.host, get: getter(agent.host, info.control) };
}

/**
 * The server's clock against this one: of 20 round trips to it, the one
 * that took least, its reading taken to fall halfway through. The server
 * then stamps its patches in this machine's time (see server.js's /skew),
 * so the clients here read their latency off their own clock
 */
async function alignClock(server) {
  let best = null;
  for (let i = 0; i < 20; i++) {
    const sent = clock();
    const { now } = await server.get('/clock');
    const back = clock();
    if (!best || back - sent < best.rtt) best = { rtt: back - sent, offset: now - (sent + back) / 2 };
  }
  await server.get(`/skew?ms=${best.offset}`);
  return best;
}

async function stopAll() {
  const stopping = children.splice(0);
  for (const child of stopping) {
    try { child.stdin.end(); } catch { /* gone */ }
  }
  const remote = remoteUp ? agent.get('/stop').catch(err => console.error(`the agent could not stop the server: ${err?.message || err}`)) : null;
  remoteUp = false;
  // Each ends itself once its pipe closes (see control), writing its profile
  // if it takes one; what has not ended by then is killed
  await Promise.race([Promise.all(stopping.map(child => child.exited)), sleep(PROFILE ? 30_000 : 2000)]);
  for (const child of stopping) child.kill();
  await remote;
  await sleep(1000);
}
process.on('SIGINT', async () => { await stopAll(); process.exit(130); });

const sum = (list, key) => list.reduce((a, s) => a + (s[key] || 0), 0);
const all = list => Promise.all(list.map(p => p.get('/count')));

/**
 * Let whatever is still on its way arrive: the server done publishing, and
 * every client at its version. Against a deployment, whose version nobody
 * here asks: no writer going or waiting on an ack, and each store's clients
 * all at one version
 */
async function drain({ server, clients }, capMs = 60_000) {
  const started = performance.now();
  for (;;) {
    const run = server ? await server.get('/run') : null;
    const counts = await all(clients);
    if (run ? run.finished && counts.every(c => c.minV === null || (c.minV === run.v && c.maxV === run.v)) : settled(counts)) return true;
    if (performance.now() - started > capMs) return false;
    await sleep(250);
  }
}

function settled(counts) {
  if (counts.some(c => c.writing || c.pending)) return false;
  const stores = {};
  for (const c of counts) {
    for (const [store, [min, max]] of Object.entries(c.stores)) {
      const held = stores[store] ??= [min, max];
      held[0] = Math.min(held[0], min);
      held[1] = Math.max(held[1], max);
    }
  }
  return Object.values(stores).every(([min, max]) => min === max);
}

/** The processes' `key`s ({ store: n }), added up by store */
function byStore(counts, key) {
  const total = {};
  for (const c of counts) for (const [store, n] of Object.entries(c[key] || {})) total[store] = (total[store] || 0) + n;
  return total;
}

const written = counts => Object.values(byStore(counts, 'written')).reduce((a, b) => a + b, 0);

/** Against a deployment: every op written reaches every client of its store */
function heardBy(counts) {
  const members = byStore(counts, 'members');
  return Object.entries(byStore(counts, 'written')).reduce((a, [store, n]) => a + n * (members[store] || 0), 0);
}

let phases = 0;
/**
 * Each phase's window, at the clients (and the relays' peaks): the versions
 * after the server's with a server of the bench's own, or against a
 * deployment the patches that carry the phase's number, which its writers
 * put in their ops
 */
async function resetAll({ server, relays, clients }, count) {
  const reset = server ? `/reset?from=${(await server.get('/run')).v}&count=${count}` : `/reset?phase=${++phases}`;
  await Promise.all([...clients.map(c => c.get(reset)), ...relays.map(r => r.get('/reset'))]);
}

/**
 * One phase: `act` publishes (or not) `count` patches, or has the writers
 * write them (`writes`); measured until every client has every one of them
 * (and every writer the ack of every op it sent), or `seconds` plus 20 s
 * have gone
 */
async function measure(parts, { count, seconds, paced = false, writes = false, act }) {
  const { server, relays, clients } = parts;
  const drained = await drain(parts);
  await resetAll(parts, count);
  // The processes measured: the server (when the bench has one), the relays, the clients
  const measured = [...(server ? [server] : []), ...relays, ...clients];
  const o = server ? 1 : 0;
  const before = await Promise.all(measured.map(p => p.get('/stats')));
  const started = performance.now();
  const job = await act();
  if (job?.refused) throw new Error(job.refused);
  let peakServer = 0;
  for (;;) {
    await sleep(250);
    const run = server ? await server.get('/run') : null;
    peakServer = Math.max(peakServer, run?.largest || 0);
    const counts = await all(clients);
    const finished = !paced || (writes ? counts.every(c => !c.writing) : run.finished);
    // Against a deployment, what was written (a writer between sockets skips its turn), each op acknowledged or refused
    const acked = !writes || (server ? sum(counts, 'acked') >= count : sum(counts, 'acked') + sum(counts, 'refused') >= written(counts));
    if ((sum(counts, 'received') >= parts.expected(counts, count) && finished && acked) || performance.now() - started > seconds * 1000 + 20_000) break;
  }
  const wall = (performance.now() - started) / 1000;
  const after = await Promise.all(measured.map(p => p.get('/stats')));
  const d = i => after[i].cpu - before[i].cpu;
  const relayCpus = relays.map((r, k) => d(o + k));
  const clientCpus = clients.map((c, k) => d(o + relays.length + k) / wall);
  // Every process's CPU on this machine over the wall time: the logical CPUs they kept busy between them
  const machine = ((server && !agent ? d(0) : 0) + relayCpus.reduce((a, b) => a + b, 0)) / wall + clientCpus.reduce((a, b) => a + b, 0);
  const cs = after.slice(o + relays.length);
  const histogram = cs.reduce((h, s) => addInto(h, s.histogram), newHistogram());
  const delivery = cs.reduce((h, s) => addInto(h, s.delivery), newHistogram());
  const received = sum(cs, 'received');
  const expected = parts.expected(cs, count);
  const run = server ? after[0].run : null;
  // The writers' runs, one a process: the rate they kept between them, and the furthest any fell behind
  const runs = writes ? cs.map(s => s.writes).filter(w => w && w.finished !== null && w.done > 0) : [];
  const span = runs.length ? (Math.max(...runs.map(w => w.last)) - Math.min(...runs.map(w => w.first))) / 1000 : 0;
  const acks = cs.reduce((h, s) => addInto(h, s.acks), newHistogram());
  return {
    drained,
    expected,
    received,
    delivered: expected ? received / expected : 1,
    stale: sum(cs, 'stale'),
    gaps: sum(cs, 'gaps'),
    closes: sum(cs, 'closes') - sum(before.slice(o + relays.length), 'closes'),
    failed: sum(cs, 'failed') - sum(before.slice(o + relays.length), 'failed'),
    retold: sum(cs, 'retold'),
    errors: sum(cs, 'errors'),
    p50: percentile(histogram, 0.5),
    p99: percentile(histogram, 0.99),
    max: percentile(histogram, 1),
    deliveryP50: percentile(delivery, 0.5),
    deliveryP99: percentile(delivery, 0.99),
    wall,
    serverCpu: server ? d(0) : null,
    relayCpu: relayCpus.reduce((a, b) => a + b, 0),
    relayCpuMax: relayCpus.length ? Math.max(...relayCpus) : 0,
    clientBusiest: Math.max(...clientCpus),
    machine,
    achieved: writes ? (span > 0 ? (sum(runs, 'done') - 1) / span : null)
      : paced && run.finished && run.done > 1 ? (run.done - 1) / ((run.last - run.first) / 1000) : null,
    lagMax: writes ? (runs.length ? Math.max(...runs.map(w => w.lagMax)) : null) : paced ? run.lagMax : null,
    writes,
    count,
    acked: sum(cs, 'acked'),
    refused: sum(cs, 'refused'),
    skipped: sum(cs, 'skipped'),
    ackP50: percentile(acks, 0.5),
    ackP99: percentile(acks, 0.99),
    serverPerOp: server && count ? (d(0) / count) * 1e6 : null,
    serverSockets: server ? after[0].sockets?.sockets : null,
    peakServerBuffered: peakServer,
    peakRelayBuffered: relays.length ? Math.max(...after.slice(o, o + relays.length).map(s => s.peakBuffered)) : 0
  };
}

/**
 * The storm: every client's socket dropped at once and each dialled again
 * as a client does after a drop (see clients.js's /storm), answered with the
 * delta since its version. How long until all `total` were answered again,
 * and each one's time from dialling to its answer
 */
async function storm(parts, total) {
  const { server, relays, clients } = parts;
  const drained = await drain(parts);
  await resetAll(parts, 0);
  const measured = [...(server ? [server] : []), ...relays, ...clients];
  const o = server ? 1 : 0;
  const before = await Promise.all(measured.map(p => p.get('/stats')));
  const started = performance.now();
  const dropped = sum(await Promise.all(clients.map(c => c.get('/storm'))), 'dropped');
  let back = null;
  let answered = 0;
  for (;;) {
    await sleep(100);
    answered = sum(await all(clients), 'answered');
    if (answered >= total) {
      back = (performance.now() - started) / 1000;
      break;
    }
    if (performance.now() - started > 180_000) break;
  }
  const wall = (performance.now() - started) / 1000;
  const after = await Promise.all(measured.map(p => p.get('/stats')));
  const d = i => after[i].cpu - before[i].cpu;
  const relayCpus = relays.map((r, k) => d(o + k));
  const clientCpus = clients.map((c, k) => d(o + relays.length + k) / wall);
  const cs = after.slice(o + relays.length);
  const was = before.slice(o + relays.length);
  const storms = cs.reduce((h, s) => addInto(h, s.storms), newHistogram());
  return {
    drained,
    total,
    dropped,
    answered,
    back,
    deltas: sum(cs, 'deltas'),
    snapshots: sum(cs, 'back') - sum(cs, 'deltas'),
    p50: percentile(storms, 0.5),
    p99: percentile(storms, 0.99),
    max: percentile(storms, 1),
    closes: sum(cs, 'closes') - sum(was, 'closes'),
    failed: sum(cs, 'failed') - sum(was, 'failed'),
    retold: sum(cs, 'retold'),
    errors: sum(cs, 'errors'),
    wall,
    serverCpu: server ? d(0) : null,
    relayCpu: relayCpus.reduce((a, b) => a + b, 0),
    relayCpuMax: relayCpus.length ? Math.max(...relayCpus) : 0,
    clientBusiest: Math.max(...clientCpus),
    machine: ((server && !agent ? d(0) : 0) + relayCpus.reduce((a, b) => a + b, 0)) / wall + clientCpus.reduce((a, b) => a + b, 0)
  };
}

async function topology(spec, results) {
  const [kind, presence = PRESENCE] = spec.split(':');
  const db = mkdtempSync(join(tmpdir(), 'lazy-fanout-'));
  const out = { spec, kind, presence, n: N, admins: ADMINS, rates: [] };
  results.push(out);
  try {
    const name = spec.replace(/[^\w-]/g, '-');
    const server = await startServer({ STORAGE: args.storage || 'sqlite', DB: join(db, 'bench.sqlite'), PRESENCE_EVERY: presence }, `server-${name}`);
    const central = `ws://${server.host}:${server.sync}`;
    if (agent) {
      out.clock = await alignClock(server);
      out.server = { host: server.host, cpus: server.cpus, platform: server.platform };
    }
    const relays = [];
    if (kind === 'hub') relays.push(await spawn('relays.js', { CENTRAL: central, COUNT: '1', NAME: 'hub' }, `relays-${name}`));
    if (kind === 'hubs') {
      for (let p = 0; p < HUB_PROCS; p++) {
        const count = Math.floor(HUBS / HUB_PROCS) + (p < HUBS % HUB_PROCS ? 1 : 0);
        if (count) relays.push(await spawn('relays.js', { CENTRAL: central, COUNT: String(count), NAME: `h${p}-` }, `relays-${name}-${p}`));
      }
    }
    const targets = kind === 'direct' ? [`${central}/sync`] : relays.flatMap(r => r.ports.map(port => `ws://127.0.0.1:${port}/sync`));
    out.relays = kind === 'direct' ? 0 : targets.length;
    const connectStarted = performance.now();
    const clients = [];
    const per = Math.ceil(N / PROCS);
    for (let p = 0; p < PROCS; p++) {
      const n = Math.min(per, N - p * per);
      if (n > 0) clients.push(await spawn('clients.js', { TARGETS: targets.join(','), N: String(n), OFFSET: String(p * per) }));
    }
    // The admins are the server's own: straight to it, hearing presence
    if (ADMINS) clients.push(await spawn('clients.js', { TARGETS: `${central}/sync`, N: String(ADMINS), OFFSET: '0', ADMIN: '1' }));
    for (;;) {
      const answered = sum(await all(clients), 'answered');
      if (answered >= N + ADMINS) break;
      if (performance.now() - connectStarted > 180_000) throw new Error(`${spec}: only ${answered} of ${N + ADMINS} answered in 3 minutes`);
      await sleep(250);
    }
    out.connectS = (performance.now() - connectStarted) / 1000;
    await sleep(3000);
    const parts = { server, relays, clients, expected: (counts, count) => count * (N + ADMINS) };
    const s = await server.get('/stats');
    if (kind !== 'direct') {
      const rs = await Promise.all(relays.map(r => r.get('/stats')));
      // Every relay with clients has its link open, and the server holds those links (a relay nobody came to dials none)
      const used = rs.flatMap(r => r.links.map((link, j) => ({ link, clients: r.clients[j] }))).filter(r => r.clients > 0);
      const shut = used.filter(r => r.link !== 'open').length;
      if (shut || s.sockets.relays !== used.length) throw new Error(`${spec}: fan-out is not on (links not open: ${shut}, relays at the server ${s.sockets.relays} of ${used.length})`);
    }
    Object.assign(out, { serverSockets: s.sockets.sockets, listed: s.listed });
    out.processes = children.length;
    const where = agent
      ? `the server on ${server.host} (${server.cpus} logical CPUs, ${server.platform}; its clock ${f(out.clock.offset)} ms from this one's, to within ${f(out.clock.rtt / 2, 2)} ms), ${children.length} processes here on ${CPUS}`
      : `${children.length} processes on ${CPUS} logical CPUs`;
    console.log(`\n${spec}: ${N} clients answered in ${out.connectS.toFixed(1)} s; the server holds ${s.sockets.sockets} sockets (${out.relays} relays), lists ${s.listed} in presence (presence ${presence}); ${where}`);

    if (PHASES.has('idle')) out.idle = await idle(spec, parts);
    for (const rate of PHASES.has('publish') ? RATES : []) {
      // At least 50 patches, so the p99 is not one slow patch
      const seconds = Math.max(SECONDS, 50 / rate);
      const count = Math.round(rate * seconds);
      const r = await measure(parts, { count, seconds, paced: true, act: () => server.get(`/publish?rate=${rate}&seconds=${seconds}&size=${SIZE}`) });
      out.rates.push({ rate, ...r });
      print(spec, `${rate}/s`, r);
      if (r.received < r.expected || r.gaps || r.closes || r.retold || r.errors || (r.p99 ?? 0) > 2000) {
        console.log(`  ${spec}: given up at ${rate}/s`);
        break;
      }
    }
    if (PHASES.has('big')) {
      out.big = await measure(parts, { count: 20 * SECONDS, seconds: SECONDS, paced: true, act: () => server.get(`/publish?rate=20&seconds=${SECONDS}&size=${BIG}`) });
      print(spec, `20/s of ${BIG} B`, out.big);
    }
    if (PHASES.has('burst')) {
      out.burst = await measure(parts, { count: BURST, seconds: 0, act: () => server.get(`/burst?count=${BURST}&size=${SIZE}`) });
      print(spec, `${BURST} at once`, out.burst);
    }
    if (PHASES.has('write')) await writePhases(spec, parts, out);
    if (PHASES.has('storm')) out.storm = await stormPhase(spec, parts, N + ADMINS);
  } catch (err) {
    out.error = String(err?.message || err);
    console.log(`  ${spec} failed: ${out.error}`);
  } finally {
    await stopAll();
    try { rmSync(db, { recursive: true, force: true }); } catch { /* the server may still hold the file a moment */ }
  }
}

/** An idle stretch, for what the processes use doing nothing (keepalives, sweeps, timers) */
async function idle(spec, parts) {
  const r = await measure(parts, { count: 0, seconds: IDLE, act: async () => { await sleep(IDLE * 1000); return {}; } });
  print(spec, `idle ${IDLE} s`, r);
  return r;
}

/** The write phases, rate by rate: each client process (the admins' aside) writes its share of the rate, over its share of the writers */
async function writePhases(spec, parts, out) {
  const writing = parts.clients.filter(c => !c.admin);
  const writersEach = writing.map(c => Math.max(1, Math.min(c.n, Math.round(WRITERS / writing.length))));
  // Where each process's writers start in the stores (see clients.js's writersFrom)
  const firsts = writersEach.map((_, k) => writersEach.slice(0, k).reduce((a, b) => a + b, 0));
  out.writers = writersEach.reduce((a, b) => a + b, 0);
  out.writes = [];
  console.log(`  ${spec.padEnd(9)} writes: ${out.writers} writers`);
  for (const rate of WRITE_RATES) {
    const seconds = Math.max(SECONDS, 50 / rate);
    const each = writing.map(() => Math.round((rate / writing.length) * seconds));
    const count = each.reduce((a, b) => a + b, 0);
    const r = await measure(parts, {
      count, seconds, paced: true, writes: true,
      act: async () => {
        const answers = await Promise.all(writing.map((c, k) => c.get(`/write?rate=${rate / writing.length}&count=${each[k]}&writers=${writersEach[k]}&size=${SIZE}&first=${firsts[k]}`)));
        return answers.find(a => a.refused) ?? {};
      }
    });
    out.writes.push({ rate, ...r });
    print(spec, `write ${rate}/s`, r);
    if (r.received < r.expected || r.acked < r.count || r.gaps || r.closes || r.retold || r.errors || (r.p99 ?? 0) > 2000) {
      console.log(`  ${spec}: given up writing at ${rate}/s`);
      break;
    }
  }
}

async function stormPhase(spec, parts, total) {
  const r = await storm(parts, total);
  const trouble = ['failed', 'closes', 'retold', 'errors'].filter(k => r[k]).map(k => `${k.toUpperCase()} ${r[k]}`).join(' ');
  const cpu = r.serverCpu === null ? '' : `server ${f(r.serverCpu, 2)} s, relays ${f(r.relayCpu, 2)} s (busiest process ${f(r.relayCpuMax, 2)}), `;
  console.log(`  ${spec.padEnd(9)} ${'storm'.padEnd(14)} ${r.dropped} dropped, ` +
    (r.back !== null ? `all ${r.total} answered again in ${f(r.back, 2)} s` : `only ${r.answered} of ${r.total} answered again in ${f(r.wall, 0)} s`) +
    ` (${r.deltas} with a delta, ${r.snapshots} a snapshot); dialled→answered p50 ${f(r.p50)} p99 ${f(r.p99)} max ${f(r.max)} ms  cpu: ${cpu}clients' busiest ${f(r.clientBusiest * 100, 0)}%, machine ${f(r.machine)} of ${CPUS} CPUs` +
    (r.drained ? '' : '  UNDRAINED') + (r.machine > BUSY * CPUS ? '  MACHINE BUSY' : '') + (trouble ? `  ${trouble}` : ''));
  return r;
}

/** --url: clients alone, against a deployment, `--clients` at a time */
async function deployment(results, save) {
  const out = { url: TARGET, stores: STORES.length, steps: [] };
  results.push(out);
  const clients = [];
  const parts = { server: null, relays: [], clients, expected: heardBy };
  const left = ['publish', 'big', 'burst'].filter(p => PHASES.has(p));
  if (left.length) console.log(`${left.join(', ')}: the server's own phases, which need the bench's server; left out`);
  let total = 0;
  try {
    for (const target of STEPS) {
      const spec = String(target);
      const step = { clients: target };
      out.steps.push(step);
      const added = target - total;
      // The sockets that failed to open before this step, so the step says its own
      const existing = clients.length;
      const failedBefore = sum(await all(clients), 'failed');
      const connectStarted = performance.now();
      while (total < target) {
        const n = Math.min(PER_PROC, target - total);
        clients.push(await spawn('clients.js', {
          TARGETS: TARGET, N: String(n), OFFSET: String(total), STORES: STORES.join(','), AUTH, QUERY, RUN,
          INSECURE: args.insecure === true ? '1' : ''
        }));
        total += n;
      }
      // Three minutes, and more for many (a slow link, a TLS handshake each)
      const cap = 180_000 + added * 10;
      for (;;) {
        const counts = await all(clients);
        const answered = sum(counts, 'answered');
        if (answered >= total) break;
        // Not one answered and the sockets failing (the address, the certificate) is no load to wait out
        if (performance.now() - connectStarted > cap || (answered === 0 && sum(counts, 'failed') >= Math.min(added, 100))) {
          const first = counts.find(c => c.firstError)?.firstError;
          throw new Error(`only ${answered} of ${total} answered in ${Math.round((performance.now() - connectStarted) / 1000)} s; ${sum(counts, 'failed')} sockets failed to open` +
            (first ? ` (the first: ${first})` : '') + (total > 14_000 ? '; past some 16 000 sockets, see the header on this machine\'s ports' : ''));
        }
        await sleep(250);
      }
      step.connectS = (performance.now() - connectStarted) / 1000;
      step.processes = clients.length;
      const counts = await all(clients);
      step.failed = sum(counts, 'failed') - failedBefore;
      // A new process's error first: an older one's may be a step old
      const error = [...counts.slice(existing), ...counts.slice(0, existing)].find(c => c.firstError)?.firstError;
      const where = STORES.length === 1 ? `the store ${STORES[0]}` : `${STORES.length} stores (${STORES[0]} to ${STORES.at(-1)})`;
      console.log(`\n${TARGET}: ${total} clients on ${where}, the ${added} new ones answered in ${step.connectS.toFixed(1)} s (${f(added / step.connectS, 0)} a second); ${clients.length} processes on ${CPUS} logical CPUs` +
        (step.failed ? `  FAILED ${step.failed} sockets, dialled again${error ? ` (one: ${error})` : ''}: see the header on this machine's ports` : ''));
      await sleep(3000);
      if (PHASES.has('idle')) step.idle = await idle(spec, parts);
      if (PHASES.has('write')) await writePhases(spec, parts, step);
      if (PHASES.has('storm')) step.storm = await stormPhase(spec, parts, total);
      await save();
    }
  } catch (err) {
    out.error = String(err?.message || err);
    console.log(`  ${TARGET} failed: ${out.error}`);
  } finally {
    await stopAll();
  }
}

const f = (x, d = 1) => (x === null || x === undefined || !Number.isFinite(x) ? '-' : x.toFixed(d));
function print(spec, label, r) {
  const trouble = ['gaps', 'closes', 'failed', 'retold', 'errors', 'refused', 'skipped', 'stale'].filter(k => r[k]).map(k => `${k.toUpperCase()} ${r[k]}`).join(' ');
  const cpu = r.serverCpu === null
    ? ''
    : `server ${f(r.serverCpu, 2)} s${r.writes ? ` (${f(r.serverPerOp, 0)} µs an op, ${r.serverSockets} sockets)` : ''}, relays ${f(r.relayCpu, 2)} s (busiest process ${f(r.relayCpuMax, 2)}), `;
  console.log(`  ${spec.padEnd(9)} ${label.padEnd(14)} ${(r.delivered * 100).toFixed(2)}%  due→heard p50 ${f(r.p50)} p99 ${f(r.p99)} max ${f(r.max)} ms, sent→heard p50 ${f(r.deliveryP50)} p99 ${f(r.deliveryP99)} ms  ` +
    (r.writes ? `acked ${r.acked}/${r.count} sent→ack p50 ${f(r.ackP50)} p99 ${f(r.ackP99)} ms  ` : '') +
    `cpu: ${cpu}clients' busiest ${f(r.clientBusiest * 100, 0)}%` +
    `, machine ${f(r.machine)} of ${CPUS} CPUs` +
    (r.achieved !== null ? `  rate ${f(r.achieved)}/s lag ${f(r.lagMax)} ms` : '') +
    (r.peakServerBuffered || r.peakRelayBuffered ? `  unsent peak: server ${Math.round(r.peakServerBuffered / 1024)} KB, relay ${Math.round(r.peakRelayBuffered / 1024)} KB` : '') +
    (r.drained ? '' : '  UNDRAINED') + (r.machine > BUSY * CPUS ? '  MACHINE BUSY' : '') + (trouble ? `  ${trouble}` : ''));
}

if (agent) {
  const info = await agent.get('/info').catch(err => {
    throw new Error(`no agent answers at ${REMOTE.host}:${REMOTE.port} (bun bench/fanout/agent.js there): ${err?.message || err}`);
  });
  console.log(`the server's machine: ${agent.host}, ${info.cpus} logical CPUs, ${info.platform}, Bun ${info.bun}`);
}

const results = [];
// (The query only as set or not: it may carry a test user's token)
const settings = TARGET
  ? { TARGET, STEPS, STORES, AUTH, QUERY: QUERY ? '(set)' : '', PER_PROC, INSECURE: args.insecure === true, WRITERS, WRITE_RATES, SECONDS, SIZE, IDLE, PHASES: [...PHASES], CPUS, BUSY, CYCLE_HZ }
  : { N, PROCS, HUBS, HUB_PROCS, ADMINS, RATES, SECONDS, SIZE, BIG, BURST, PRESENCE, WRITERS, WRITE_RATES, IDLE, PHASES: [...PHASES], CPUS, BUSY, REMOTE, CYCLE_HZ };
const save = async () => {
  if (args.json) await Bun.write(String(args.json), JSON.stringify({ args: settings, results }, null, 1));
};
try {
  if (TARGET) await deployment(results, save);
  else {
    for (const spec of TOPOLOGIES) {
      await topology(spec, results);
      await save();
    }
  }
} finally {
  await stopAll();
}
