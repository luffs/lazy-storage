// The server: the stores, on SQLite, and the route the relays link to. Only
// the relays connect here, on 127.0.0.1 (see main.js).
//   env: SERVER_PORT, RELAY_TOKEN (main.js sets both), DATA_DIR
import { join } from 'node:path';
import { mkdirSync } from 'node:fs';
import { createStore, createStores } from '../../src/server/index.js';
import { createHandlers } from '../../src/server/bun.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';

const dataDir = process.env.DATA_DIR ?? join(import.meta.dir, 'data');
mkdirSync(dataDir, { recursive: true });
const sqlite = sqliteStorage(join(dataDir, 'stores.sqlite'));

// As in the basic example: a keyed map of tasks, which the page shows as a
// list, and who is here. Presence goes out at most every 250 ms, so a burst
// of clients (all of them back after a deploy) is a few messages to each
const stores = createStores(id => createStore({
  initial: { tasks: {} },
  storage: sqlite.store(id),
  presence: { every: 250 }
}), { idle: 30 * 60_000 });

const handlers = createHandlers({
  stores,
  path: '/ws',
  // Only the relays connect, over loopback: compressing what goes to them
  // would spend the store's one core on bandwidth nobody lacks
  perMessageDeflate: false,
  // Who is asking: the name in the query string, as the basic example has
  // it. A real app checks a token or a session cookie here, and the relays
  // pass on the header that carries it (see relay.js)
  authenticate: req => new URL(req.url).searchParams.get('name') || null,
  // The relays, let in by the token main.js made for them
  relays: {
    authenticate: req => (req.headers.get('authorization') === `Bearer ${process.env.RELAY_TOKEN}` ? { id: 'relay' } : null)
  }
});

const server = Bun.serve({
  hostname: '127.0.0.1',
  port: Number(process.env.SERVER_PORT),
  async fetch(req, srv) {
    return (await handlers.upgrade(req, srv)) ?? new Response('Not found', { status: 404 });
  },
  websocket: handlers.websocket
});

// podman stop (or Ctrl+C): the sockets closed, every store's pending commit written, then the file closed
let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    if (stopping) return;
    stopping = true;
    await handlers.close();
    server.stop(true);
    sqlite.close();
    process.exit(0);
  });
}
console.log(`server: the stores on 127.0.0.1:${server.port}`);
