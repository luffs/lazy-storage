// agent.js - The server's machine in a two-machine run of the fan-out bench: starts
// and stops its server (server.js) for a run.js on another machine
//
// On one machine the bench measures the machine as much as the server: the
// relays and the clients share its cores, and each socket's writes and reads
// are the kernel's work at both ends of the loopback. Two machines give the
// server one of its own, and the relays and the clients the other:
//   here:   bun bench/fanout/agent.js [--port 36700] [--host 0.0.0.0] [--profile <dir>]
//   there:  bun bench/fanout/run.js --remote <this host>:36700 [run.js's other options]
// run.js asks for a fresh server for each topology (/start, with the store's
// storage and presence) and stops it after (/stop), as it spawns and stops a
// server of its own; one runs at a time, and a /start stops the last. The
// server takes the next two ports, its sockets on port + 1 and its control
// server on port + 2, so a firewall lets the three in. Its database is a
// fresh directory in the system's temporary one, removed with it; with
// --profile, the server writes its CPU profile there when run.js takes them.
//
// Anyone who reaches the port can start and stop the bench's server (and
// nothing else: the server's settings are checked, and the rest are the
// agent's): run it on a network you trust, for as long as the run takes.
import { tmpdir } from 'node:os';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { parseArgs, cycleRate, controlLine, logicalCpus } from './common.js';

const args = parseArgs(process.argv.slice(2));
const PORT = Number(args.port || 36700);
const HOST = typeof args.host === 'string' ? args.host : '0.0.0.0';
const PROFILE = typeof args.profile === 'string' ? resolve(args.profile) : null;
const CYCLE_HZ = cycleRate();
const sleep = ms => new Promise(done => setTimeout(done, ms));
if (PROFILE) mkdirSync(PROFILE, { recursive: true });

/** What run.js may set for the server, and what each may be: nothing else reaches it */
const SETTINGS = {
  STORAGE: /^(sqlite|memory)$/,
  PRESENCE_EVERY: /^(off|\d+)$/,
  KEYS: /^\d+$/,
  DEFLATE: /^off$/
};

let current = null;   // the server running: { child, dir }
// Starts and stops one at a time, in the order they were asked for
let queue = Promise.resolve();
const serial = fn => {
  const next = queue.then(fn);
  queue = next.catch(() => {});
  return next;
};

/** Stop the server, if one runs; true if one did */
async function stop() {
  const running = current;
  current = null;
  if (!running) return false;
  try { running.child.stdin.end(); } catch { /* gone */ }
  // It ends itself once its pipe closes (see common.js's control), writing
  // its profile if it takes one; one that has not by then is killed
  await Promise.race([running.child.exited, sleep(PROFILE ? 30_000 : 5000)]);
  running.child.kill();
  await running.child.exited;
  try { rmSync(running.dir, { recursive: true, force: true }); } catch { /* the database may be held a moment longer */ }
  return true;
}

/** A fresh server with the settings asked for, the last one stopped first */
async function start(params) {
  await stop();
  const settings = {};
  for (const [key, allowed] of Object.entries(SETTINGS)) {
    const value = params.get(key);
    if (value === null) continue;
    if (!allowed.test(value)) throw new Error(`${key} cannot be ${JSON.stringify(value)}`);
    settings[key] = value;
  }
  const profile = params.get('profile');
  if (profile !== null && !/^[\w-]{1,100}$/.test(profile)) throw new Error(`a profile cannot be named ${JSON.stringify(profile)}`);
  const dir = mkdtempSync(join(tmpdir(), 'lazy-fanout-agent-'));
  const flags = PROFILE && profile ? ['--cpu-prof-md', `--cpu-prof-dir=${PROFILE}`, `--cpu-prof-name=${profile}.md`] : [];
  const child = Bun.spawn([process.execPath, ...flags, join(import.meta.dir, 'server.js')], {
    env: {
      ...process.env,
      ...(CYCLE_HZ ? { CYCLE_HZ: String(CYCLE_HZ) } : {}),
      ...settings,
      DB: join(dir, 'bench.sqlite'),
      HOST,
      SYNC_PORT: String(PORT + 1),
      CONTROL_PORT: String(PORT + 2)
    },
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'inherit'
  });
  current = { child, dir };
  try {
    const info = await controlLine(child, 'server.js');
    console.log(`agent: a server on ${PORT + 1}, ${JSON.stringify(settings)}`);
    return { control: info.control, sync: info.sync, cpus: logicalCpus(), platform: process.platform };
  } catch (err) {
    await stop();
    throw err;
  }
}

Bun.serve({
  hostname: HOST,
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    try {
      if (url.pathname === '/info') return Response.json({ cpus: logicalCpus(), platform: process.platform, bun: Bun.version, running: current !== null });
      if (url.pathname === '/start') return Response.json(await serial(() => start(url.searchParams)));
      if (url.pathname === '/stop') return Response.json({ stopped: await serial(stop) });
      return new Response('Not found', { status: 404 });
    } catch (err) {
      return Response.json({ error: String(err?.message || err) }, { status: 500 });
    }
  }
});
console.log(`agent: listening on ${HOST}:${PORT}; the servers it starts take ${PORT + 1} (sockets) and ${PORT + 2} (control)`);
process.on('SIGINT', async () => {
  await stop();
  process.exit(130);
});
