// relays.js - COUNT fan-out relays in one process, each on a port of its own,
// each with a link of its own to the server: a store's hubs, many to a machine
// here, where each would be a box of its own. Their save timers are staggered
// (saveDelay 1000 + 37 ms a relay) so the copies the relays of a process keep
// are not all written in the same turn, which a box of its own never shares
//   env: CENTRAL (ws://127.0.0.1:<port>), COUNT (1), NAME (a prefix),
//        SAVE_DELAY (ms, for all), DEFLATE ('off')
// Control: /stats, /reset (the peak of what a socket held unsent, sampled every 250 ms)
import { createRelay, memoryCopies } from '../../src/relay/index.js';
import { createRelayHandlers, upstreamSocket } from '../../src/relay/bun.js';
import { cpuSeconds, control } from './common.js';

const { CENTRAL } = process.env;
const COUNT = Number(process.env.COUNT || 1);
const NAME = process.env.NAME || 'r';
const errors = [];
const relays = [];
for (let j = 0; j < COUNT; j++) {
  const name = `${NAME}${j}`;
  const relay = createRelay({
    storage: memoryCopies(),
    saveDelay: process.env.SAVE_DELAY ? Number(process.env.SAVE_DELAY) : 1000 + j * 37,
    onError: err => errors.length < 20 && errors.push(`${name}: ${err?.message || err}`),
    link: upstreamSocket(`${CENTRAL}/sync/relay?relay=${name}`)
  });
  const handlers = createRelayHandlers({
    relay,
    path: '/sync',
    ...(process.env.DEFLATE === 'off' ? { perMessageDeflate: false } : {}),
    key: req => new URL(req.url).searchParams.get('id'),
    upstream: req => upstreamSocket(`${CENTRAL}/sync${new URL(req.url).search}`),
    // What the server judges each client by: the query naming it, as its own socket would carry it
    credential: req => ({ query: new URL(req.url).search }),
    onError: err => errors.length < 20 && errors.push(`${name} handlers: ${err?.message || err}`)
  });
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(req, srv) {
      return (await handlers.upgrade(req, srv)) ?? new Response('Not found', { status: 404 });
    },
    websocket: handlers.websocket
  });
  relays.push({ name, relay, handlers, port: server.port });
}

let peakBuffered = 0;
setInterval(() => {
  for (const { handlers } of relays) for (const s of handlers.sockets()) if (s.buffered > peakBuffered) peakBuffered = s.buffered;
}, 250);

control({
  '/stats': () => {
    const states = {};
    const clients = relays.map(() => 0);
    relays.forEach(({ handlers }, j) => {
      for (const s of handlers.sockets()) {
        states[s.state] = (states[s.state] || 0) + 1;
        clients[j]++;
      }
    });
    return {
      cpu: cpuSeconds(),
      rss: process.memoryUsage().rss,
      links: relays.map(({ relay }) => relay.stats().link?.state),
      modes: relays.map(({ relay }) => relay.mode),
      clients,
      states,
      peakBuffered,
      errors
    };
  },
  '/reset': () => {
    peakBuffered = 0;
    return {};
  }
}, { role: 'relays', ports: relays.map(r => r.port) });
