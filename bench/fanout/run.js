// run.js - How a store that every client follows behaves when the server
// publishes to it, and when its clients write to it, with thousands of
// clients that do not hear presence (all but a few admins'), straight to
// the server and through fan-out relays.
//   bun bench/fanout/run.js [--n 4000] [--procs 16] [--topologies direct,hub,hubs]
//     [--hubs 200] [--hub-procs 8] [--admins 2] [--rates 5,20,50,100,200,500]
//     [--seconds 5] [--size 150] [--big 4096] [--burst 200] [--storage sqlite]
//     [--presence-every 250] [--writers 400] [--write-rates 50,200,500,1000]
//     [--phases idle,publish,big,burst,write] [--profile <dir>] [--json out.json]
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
// Every part is a process of its own (the server, each process of relays, each
// process of clients), so each one's CPU is its own (on Windows its cycle count,
// as process.cpuUsage() there moves in 15.6 ms steps); the machine's cores are
// shared, so read CPU seconds, not wall time alone, and check that the clients
// had room (their busiest process is reported). A patch carries the time it
// was due and the time it went out, so a server behind its schedule shows as
// latency, and what delivery took shows apart. Before each phase whatever is
// still on its way is let arrive, and a phase counts only its own versions.
// A rate is given up on (and the higher ones skipped) once a patch is missing,
// a client is cut off or told to say hello again, or the p99 passes 2 s.
//   direct: every client a socket of the server's
//   hub:    every client behind one relay (one store's hub with all of them)
//   hubs:   the clients spread over many relays (a chain: a hub per shop),
//           many relays to a process here, where each would be a box of its own
import { tmpdir } from 'node:os';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { parseArgs, newHistogram, addInto, percentile, cycleRate } from './common.js';

const args = parseArgs(process.argv.slice(2));
const N = Number(args.n || 4000);
const PROCS = Number(args.procs || 16);
const TOPOLOGIES = String(args.topologies || 'direct,hub,hubs').split(',');
const HUBS = Number(args.hubs || 200);
const HUB_PROCS = Number(args['hub-procs'] || 8);
const ADMINS = Number(args.admins ?? 2);
const RATES = String(args.rates || '5,20,50,100,200,500').split(',').map(Number);
const SECONDS = Number(args.seconds || 5);
const SIZE = Number(args.size || 150);
const BIG = Number(args.big || 4096);
const BURST = Number(args.burst || 200);
const PRESENCE = String(args['presence-every'] ?? 250);
const WRITERS = Number(args.writers || 400);
const WRITE_RATES = String(args['write-rates'] || '50,200,500,1000').split(',').map(Number);
const PHASES = new Set(String(args.phases || 'idle,publish,big,burst,write').split(','));
const PROFILE = typeof args.profile === 'string' ? resolve(args.profile) : null;
const here = import.meta.dir;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const CYCLE_HZ = cycleRate();
if (PROFILE) mkdirSync(PROFILE, { recursive: true });

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
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  let buffered = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) throw new Error(`${file} ended before it said its port`);
    buffered += decoder.decode(value, { stream: true });
    const lines = buffered.split('\n');
    buffered = lines.pop();
    const line = lines.find(l => l.startsWith('{"control"'));
    if (!line) continue;
    // Keep draining what it prints, so it never blocks on a full pipe
    (async () => { for (;;) { const { done: over } = await reader.read(); if (over) return; } })();
    const info = JSON.parse(line);
    return { ...info, child, get: path => fetch(`http://127.0.0.1:${info.control}${path}`, { signal: AbortSignal.timeout(60_000) }).then(r => r.json()) };
  }
}
async function stopAll() {
  const stopping = children.splice(0);
  for (const child of stopping) {
    try { child.stdin.end(); } catch { /* gone */ }
  }
  // Each ends itself once its pipe closes (see control), writing its profile
  // if it takes one; what has not ended by then is killed
  await Promise.race([Promise.all(stopping.map(child => child.exited)), sleep(PROFILE ? 30_000 : 2000)]);
  for (const child of stopping) child.kill();
  await sleep(1000);
}
process.on('SIGINT', async () => { await stopAll(); process.exit(130); });

const sum = (list, key) => list.reduce((a, s) => a + (s[key] || 0), 0);
const all = list => Promise.all(list.map(p => p.get('/count')));

/** Let whatever is still on its way arrive: the server done publishing, and every client at its version */
async function drain({ server, clients }, capMs = 60_000) {
  const started = performance.now();
  for (;;) {
    const run = await server.get('/run');
    const counts = await all(clients);
    if (run.finished && counts.every(c => c.minV === null || (c.minV === run.v && c.maxV === run.v))) return true;
    if (performance.now() - started > capMs) return false;
    await sleep(250);
  }
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
  const from = (await server.get('/run')).v;
  await Promise.all([...clients.map(c => c.get(`/reset?from=${from}&count=${count}`)), ...relays.map(r => r.get('/reset'))]);
  const before = await Promise.all([server, ...relays, ...clients].map(p => p.get('/stats')));
  const started = performance.now();
  const job = await act();
  if (job?.refused) throw new Error(job.refused);
  const expected = count * (N + ADMINS);
  let peakServer = 0;
  for (;;) {
    await sleep(250);
    const run = await server.get('/run');
    peakServer = Math.max(peakServer, run.largest || 0);
    const counts = await all(clients);
    const finished = !paced || (writes ? counts.every(c => !c.writing) : run.finished);
    const acked = !writes || sum(counts, 'acked') >= count;
    if ((sum(counts, 'received') >= expected && finished && acked) || performance.now() - started > seconds * 1000 + 20_000) break;
  }
  const wall = (performance.now() - started) / 1000;
  const after = await Promise.all([server, ...relays, ...clients].map(p => p.get('/stats')));
  const d = i => after[i].cpu - before[i].cpu;
  const relayCpus = relays.map((r, k) => d(1 + k));
  const clientCpus = clients.map((c, k) => d(1 + relays.length + k) / wall);
  const cs = after.slice(1 + relays.length);
  const histogram = cs.reduce((h, s) => addInto(h, s.histogram), newHistogram());
  const delivery = cs.reduce((h, s) => addInto(h, s.delivery), newHistogram());
  const received = sum(cs, 'received');
  const run = after[0].run;
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
    closes: sum(cs, 'closes') - sum(before.slice(1 + relays.length), 'closes'),
    retold: sum(cs, 'retold'),
    errors: sum(cs, 'errors'),
    p50: percentile(histogram, 0.5),
    p99: percentile(histogram, 0.99),
    max: percentile(histogram, 1),
    deliveryP50: percentile(delivery, 0.5),
    deliveryP99: percentile(delivery, 0.99),
    wall,
    serverCpu: d(0),
    relayCpu: relayCpus.reduce((a, b) => a + b, 0),
    relayCpuMax: relayCpus.length ? Math.max(...relayCpus) : 0,
    clientBusiest: Math.max(...clientCpus),
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
    serverPerOp: count ? (d(0) / count) * 1e6 : null,
    serverSockets: after[0].sockets?.sockets,
    peakServerBuffered: peakServer,
    peakRelayBuffered: relays.length ? Math.max(...after.slice(1, 1 + relays.length).map(s => s.peakBuffered)) : 0
  };
}

async function topology(spec, results) {
  const [kind, presence = PRESENCE] = spec.split(':');
  const db = mkdtempSync(join(tmpdir(), 'lazy-fanout-'));
  const out = { spec, kind, presence, n: N, admins: ADMINS, rates: [] };
  results.push(out);
  try {
    const name = spec.replace(/[^\w-]/g, '-');
    const server = await spawn('server.js', { STORAGE: args.storage || 'sqlite', DB: join(db, 'bench.sqlite'), PRESENCE_EVERY: presence }, `server-${name}`);
    const central = `ws://127.0.0.1:${server.sync}`;
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
    const parts = { server, relays, clients };
    const s = await server.get('/stats');
    if (kind !== 'direct') {
      const rs = await Promise.all(relays.map(r => r.get('/stats')));
      // Every relay with clients has its link open, and the server holds those links (a relay nobody came to dials none)
      const used = rs.flatMap(r => r.links.map((link, j) => ({ link, clients: r.clients[j] }))).filter(r => r.clients > 0);
      const shut = used.filter(r => r.link !== 'open').length;
      if (shut || s.sockets.relays !== used.length) throw new Error(`${spec}: fan-out is not on (links not open: ${shut}, relays at the server ${s.sockets.relays} of ${used.length})`);
    }
    Object.assign(out, { serverSockets: s.sockets.sockets, listed: s.listed });
    console.log(`\n${spec}: ${N} clients answered in ${out.connectS.toFixed(1)} s; the server holds ${s.sockets.sockets} sockets (${out.relays} relays), lists ${s.listed} in presence (presence ${presence})`);

    // An idle stretch, for what the processes use doing nothing (keepalives, sweeps, timers)
    if (PHASES.has('idle')) {
      out.idle = await measure(parts, { count: 0, seconds: 3, act: async () => { await sleep(3000); return {}; } });
      print(spec, 'idle 3 s', out.idle);
    }
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
    if (PHASES.has('write')) {
      // Each client process (the admins' aside) writes its share of the rate, over its share of the writers
      const writing = clients.filter(c => !c.admin);
      const writersEach = writing.map(c => Math.max(1, Math.min(c.n, Math.round(WRITERS / writing.length))));
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
            const answers = await Promise.all(writing.map((c, k) => c.get(`/write?rate=${rate / writing.length}&count=${each[k]}&writers=${writersEach[k]}&size=${SIZE}`)));
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
  } catch (err) {
    out.error = String(err?.message || err);
    console.log(`  ${spec} failed: ${out.error}`);
  } finally {
    await stopAll();
    try { rmSync(db, { recursive: true, force: true }); } catch { /* the server may still hold the file a moment */ }
  }
}

const f = (x, d = 1) => (x === null || x === undefined || !Number.isFinite(x) ? '-' : x.toFixed(d));
function print(spec, label, r) {
  const trouble = ['gaps', 'closes', 'retold', 'errors', 'refused', 'skipped', 'stale'].filter(k => r[k]).map(k => `${k.toUpperCase()} ${r[k]}`).join(' ');
  console.log(`  ${spec.padEnd(9)} ${label.padEnd(14)} ${(r.delivered * 100).toFixed(2)}%  due→heard p50 ${f(r.p50)} p99 ${f(r.p99)} max ${f(r.max)} ms, sent→heard p50 ${f(r.deliveryP50)} p99 ${f(r.deliveryP99)} ms  ` +
    (r.writes ? `acked ${r.acked}/${r.count} sent→ack p50 ${f(r.ackP50)} p99 ${f(r.ackP99)} ms  ` : '') +
    `cpu: server ${f(r.serverCpu, 2)} s${r.writes ? ` (${f(r.serverPerOp, 0)} µs an op, ${r.serverSockets} sockets)` : ''}, relays ${f(r.relayCpu, 2)} s (busiest process ${f(r.relayCpuMax, 2)}), clients' busiest ${f(r.clientBusiest * 100, 0)}%` +
    (r.achieved !== null ? `  rate ${f(r.achieved)}/s lag ${f(r.lagMax)} ms` : '') +
    (r.peakServerBuffered || r.peakRelayBuffered ? `  unsent peak: server ${Math.round(r.peakServerBuffered / 1024)} KB, relay ${Math.round(r.peakRelayBuffered / 1024)} KB` : '') +
    (r.drained ? '' : '  UNDRAINED') + (trouble ? `  ${trouble}` : ''));
}

const results = [];
try {
  for (const spec of TOPOLOGIES) {
    await topology(spec, results);
    if (args.json) await Bun.write(String(args.json), JSON.stringify({ args: { N, PROCS, HUBS, HUB_PROCS, ADMINS, RATES, SECONDS, SIZE, BIG, BURST, PRESENCE, WRITERS, WRITE_RATES, PHASES: [...PHASES], CYCLE_HZ }, results }, null, 1));
  }
} finally {
  await stopAll();
}
