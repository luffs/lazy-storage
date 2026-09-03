// bun.js - Serve stores over WebSockets with Bun.serve
//
// Each socket gets one session on one store; messages are JSON. With a
// single `store`, clients connect to `path` (default /ws). With `stores`
// (a registry from createStores, or a function id -> store|null), clients
// connect to `path/<storeId>`; the id sits in the URL rather than in the
// protocol so an authenticating wrapper can inspect it before a session
// exists. Everything else (state, merge, persistence) lives in the store,
// so this file is the only runtime-specific one on the server side.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';

/**
 * @param {Object} options
 * @param {Object} [options.store] - a single store (served at `path`)
 * @param {Object|((id: string) => Object|null)} [options.stores] - a registry
 *   or resolver (served at `path/<id>`)
 * @param {number} [options.port=3200]
 * @param {string} [options.path='/ws'] - WebSocket path (or path prefix)
 * @param {(req: Request) => Response|null|Promise<Response|null>} [options.fetch]
 *   handles other requests; return null to fall through to 404
 */
export function serve({ store, stores, port = 3200, path = '/ws', fetch: fetchHandler } = {}) {
  if (!store && !stores) throw new TypeError('serve requires a store or stores');
  const resolveStore = typeof stores === 'function' ? stores : stores ? id => stores.get(id) : null;
  const sessions = new Map();

  function target(url) {
    if (store && url.pathname === path) return { store };
    if (resolveStore && url.pathname.startsWith(path + '/')) {
      let id;
      try {
        id = decodeURIComponent(url.pathname.slice(path.length + 1));
      } catch {
        return { error: new Response('Invalid store id', { status: 400 }) };
      }
      if (!isStoreId(id)) return { error: new Response('Invalid store id', { status: 400 }) };
      const found = resolveStore(id);
      return found ? { store: found } : { error: new Response('Unknown store', { status: 404 }) };
    }
    return null;
  }

  const server = Bun.serve({
    port,
    async fetch(req, server) {
      const url = new URL(req.url);
      const t = target(url);
      if (t) {
        if (t.error) return t.error;
        return server.upgrade(req, { data: { store: t.store } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
      }
      if (fetchHandler) {
        const res = await fetchHandler(req);
        if (res) return res;
      }
      return new Response('Not found', { status: 404 });
    },
    websocket: {
      open(ws) {
        sessions.set(ws, ws.data.store.session({ send: message => ws.send(JSON.stringify(message)) }));
      },
      message(ws, raw) {
        let msg;
        try {
          msg = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
        } catch {
          return ws.send(JSON.stringify({ t: 'error', message: 'Expected JSON' }));
        }
        if (!LazyWatch.Utils.isPlainObject(msg)) return ws.send(JSON.stringify({ t: 'error', message: 'Expected a message object' }));
        sessions.get(ws)?.receive(msg);
      },
      close(ws) {
        sessions.get(ws)?.close();
        sessions.delete(ws);
      }
    }
  });

  return server;
}
