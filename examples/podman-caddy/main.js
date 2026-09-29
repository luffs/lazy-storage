// The container's entry: the server and its relays, each a process of its own, on one machine.
//   PORT=3200 bun examples/podman-caddy/main.js      (then http://localhost:3200)
//
// A store lives in one process and uses one core, and each change it makes
// costs a write to every socket that hears it. So the clients never
// connect to the server: they connect to RELAYS relays (relay.js), which
// share $PORT (the kernel spreads the connections over them) and each read
// every store once from the server, on a link of their own, and pass it on
// to their clients. The server (server.js, on 127.0.0.1:SERVER_PORT, not
// published) merges, stores and judges who may read what; the relays, on
// the other cores, do the writing to clients. See README.md.
//
// The relays' link is let in by a token made here, for this container's
// life. `podman stop` is passed on: the relays close their sockets, the
// server writes what is pending and closes its file. A process that dies
// takes the others with it, and podman's restart policy brings them back.
//
// PROFILE=<dir> has each process write a CPU profile there when it stops
// (Bun's --cpu-prof-md: server.md, relay-1.md and on), for finding what a
// load test (bench/fanout/run.js --url) spends the cores on
import { randomBytes } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';

const RELAYS = Number(process.env.RELAYS ?? 2);
const env = {
  ...process.env,
  PORT: process.env.PORT ?? '3200',
  SERVER_PORT: process.env.SERVER_PORT ?? '3201',
  RELAY_TOKEN: randomBytes(24).toString('hex')
};
const PROFILE = process.env.PROFILE ? resolve(process.env.PROFILE) : null;
if (PROFILE) mkdirSync(PROFILE, { recursive: true });
const spawn = (file, name) => {
  const flags = PROFILE ? ['--cpu-prof-md', `--cpu-prof-dir=${PROFILE}`, `--cpu-prof-name=${name}.md`] : [];
  return Bun.spawn([process.execPath, ...flags, join(import.meta.dir, file)], { env, stdout: 'inherit', stderr: 'inherit' });
};
const children = [spawn('server.js', 'server'), ...Array.from({ length: RELAYS }, (_, k) => spawn('relay.js', `relay-${k + 1}`))];

let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    stopping = true;
    for (const child of children) child.kill('SIGTERM');
  });
}
await Promise.race(children.map(child => child.exited));
for (const child of children) child.kill('SIGTERM');
await Promise.all(children.map(child => child.exited));
process.exit(stopping ? 0 : 1);
