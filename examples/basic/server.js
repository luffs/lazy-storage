// A Bun server for the shared list: one store, SQLite storage, a name from
// the query string as the user. Serves the page and the library's source.
//   bun examples/basic/server.js
import { dirname, join } from 'node:path';
import { createStore, createStores } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';

const here = dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
const root = join(here, '..', '..');
const sqlite = sqliteStorage(join(here, 'state.sqlite'));

// Every store starts as a keyed map of tasks; each record carries a
// position, and the page sees the map as an array. `shared` is the one
// the page uses
const stores = createStores(id => createStore({
  initial: { tasks: {} },
  storage: sqlite.store(id)
}), { idle: 30 * 60_000 });

const server = serve({
  port: Number(process.env.PORT ?? 3200),
  stores,
  // Who is asking: the name in the query string (a real app checks a token)
  authenticate: req => new URL(req.url).searchParams.get('name') || null,
  // Static files: the page, and the library plus lazy-watch under /lib
  fetch(req) {
    const { pathname } = new URL(req.url);
    if (pathname === '/') return new Response(Bun.file(join(here, 'index.html')));
    if (pathname.startsWith('/lib/lazy-storage/')) return new Response(Bun.file(join(root, 'src', pathname.slice('/lib/lazy-storage/'.length))));
    if (pathname.startsWith('/lib/lazy-watch/')) return new Response(Bun.file(join(root, 'node_modules', 'lazy-watch', 'src', pathname.slice('/lib/lazy-watch/'.length))));
    return null;
  }
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => {
    await server.shutdown();
    sqlite.close();
    process.exit(0);
  });
}
console.log(`shared list at http://localhost:${server.port}`);
