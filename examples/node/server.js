// The shared list served by Node: `ws` for sockets, node:sqlite for
// storage, the same page as examples/basic. Node 22.13 or later.
//   node examples/node/server.js
import { once } from 'node:events';
import { createReadStream, existsSync } from 'node:fs';
import { dirname, join, extname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createStore, createStores } from '../../src/server/index.js';
import { serve } from '../../src/server/node.js';
import { sqliteStorage } from '../../src/server/sqlite-node.js';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..', '..');
const page = join(here, '..', 'basic', 'index.html');
const sqlite = sqliteStorage(join(here, 'state.sqlite'));

const stores = createStores(id => createStore({
  initial: { tasks: {} },
  storage: sqlite.store(id)
}), { idle: 30 * 60_000 });

const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8' };
const send = (res, file) => {
  if (!existsSync(file)) { res.statusCode = 404; return res.end('Not found'); }
  res.setHeader('content-type', types[extname(file)] ?? 'application/octet-stream');
  createReadStream(file).pipe(res);
};

const server = serve({
  port: Number(process.env.PORT ?? 3200),
  stores,
  // `authenticate` sees a Web Request, the same as on Bun
  authenticate: req => new URL(req.url).searchParams.get('name') || null,
  request(req, res) {
    const { pathname } = new URL(req.url, 'http://localhost');
    if (pathname === '/') return send(res, page);
    if (pathname.startsWith('/lib/lazy-storage/')) return send(res, join(root, 'src', pathname.slice('/lib/lazy-storage/'.length)));
    if (pathname.startsWith('/lib/lazy-watch/')) return send(res, join(root, 'node_modules', 'lazy-watch', 'src', pathname.slice('/lib/lazy-watch/'.length)));
    res.statusCode = 404;
    res.end('Not found');
  }
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => {
    await server.shutdown();
    sqlite.close();
    process.exit(0);
  });
}
await once(server, 'listening');
console.log(`shared list at http://localhost:${server.address().port}`);
