// A relay: what the clients connect to, on $PORT, beside the other relays
// (reusePort: the kernel spreads the connections over them). It reads each
// store once from the server, on its link, and passes every change on to
// its clients; a client's edits go up a socket of the client's own, judged
// by the server as the client's. It also serves the basic example's page
// and the library's source, so the example has a page to open.
//   env: PORT, SERVER_PORT, RELAY_TOKEN (main.js sets all three)
import { join } from 'node:path';
import { createRelay, memoryCopies } from '../../src/relay/index.js';
import { createRelayHandlers, upstreamSocket } from '../../src/relay/bun.js';

const root = join(import.meta.dir, '..', '..');
const SERVER = `ws://127.0.0.1:${process.env.SERVER_PORT}/ws`;

const relay = createRelay({
  // The copies answer the clients while the server is away; the server's
  // disk has the data, so a relay keeps its copies in memory
  storage: memoryCopies(),
  link: upstreamSocket(`${SERVER}/relay`, { headers: { authorization: `Bearer ${process.env.RELAY_TOKEN}` } })
});

// Who a client is, as the server's `authenticate` reads it: here the name in
// the query string. A real app passes on the header that carries its token
// or cookie instead, in all three: `key` (a fingerprint of it, for answering
// the client while the server is away), `upstream` (the socket its edits go
// up) and `credential` (what the server judges it by)
const name = req => new URL(req.url).searchParams.get('name');
const handlers = createRelayHandlers({
  relay,
  path: '/ws',
  key: req => (name(req) ? new Bun.CryptoHasher('sha256').update(name(req)).digest('hex') : null),
  upstream: req => upstreamSocket(SERVER + new URL(req.url).search),
  credential: req => ({ query: new URL(req.url).search })
});

const server = Bun.serve({
  port: Number(process.env.PORT),
  reusePort: true,
  async fetch(req, srv) {
    const upgraded = await handlers.upgrade(req, srv);
    if (upgraded !== null) return upgraded;
    const { pathname } = new URL(req.url);
    if (pathname === '/') return new Response(Bun.file(join(root, 'examples', 'basic', 'index.html')));
    if (pathname.startsWith('/lib/lazy-storage/')) return new Response(Bun.file(join(root, 'src', pathname.slice('/lib/lazy-storage/'.length))));
    if (pathname.startsWith('/lib/lazy-watch/')) return new Response(Bun.file(join(root, 'node_modules', 'lazy-watch', 'src', pathname.slice('/lib/lazy-watch/'.length))));
    return new Response('Not found', { status: 404 });
  },
  websocket: handlers.websocket
});

// podman stop (or Ctrl+C): the clients' sockets closed (they reconnect to the next process up)
let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    if (stopping) return;
    stopping = true;
    await handlers.close();
    relay.close();
    server.stop(true);
    process.exit(0);
  });
}
console.log(`relay ${process.pid}: clients on :${server.port}`);
