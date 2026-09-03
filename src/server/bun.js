// bun.js - Serve stores over WebSockets with Bun.serve
//
// Messages are JSON. Routes:
//   serve({ store })    path            one store, a plain session per socket
//   serve({ stores })   path            a hub: many stores over one socket,
//                                       messages tagged with `store`
//                       path/<storeId>  one store from the registry, a plain
//                                       session per socket (the single-store
//                                       client protocol, unchanged)
// The store id sits in the URL for the per-store route and in the
// messages for the hub; an authenticating wrapper can inspect either
// before a session exists. Everything else (state, merge, persistence)
// lives in the stores, so this file is the only runtime-specific one on
// the server side.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';
import { createHub } from './hub.js';

/**
 * @param {Object} options
 * @param {Object} [options.store] - a single store, served at `path`
 * @param {Object|((id: string) => Object|null)} [options.stores] - a registry
 *   or resolver: a hub at `path`, plain sessions at `path/<id>`
 * @param {number} [options.port=3200]
 * @param {string} [options.path='/ws'] - WebSocket path (or path prefix)
 * @param {(req: Request) => Response|null|Promise<Response|null>} [options.fetch]
 *   handles other requests; return null to fall through to 404
 */
export function serve({ store, stores, port = 3200, path = '/ws', fetch: fetchHandler } = {}) {
  if (!store && !stores) throw new TypeError('serve requires a store or stores');
  if (store && stores) throw new TypeError('serve takes either a single store or stores, not both');
  const resolveStore = typeof stores === 'function' ? stores : stores ? id => stores.get(id) : null;
  const sessions = new Map();

  // What a URL asks for: a session on one store, a hub, or an error response
  function route(url) {
    if (url.pathname === path) {
      return store ? { open: send => store.session({ send }) } : { open: send => createHub(resolveStore, { send }) };
    }
    if (resolveStore && url.pathname.startsWith(path + '/')) {
      let id;
      try {
        id = decodeURIComponent(url.pathname.slice(path.length + 1));
      } catch {
        return { error: new Response('Invalid store id', { status: 400 }) };
      }
      if (!isStoreId(id)) return { error: new Response('Invalid store id', { status: 400 }) };
      const found = resolveStore(id);
      return found ? { open: send => found.session({ send }) } : { error: new Response('Unknown store', { status: 404 }) };
    }
    return null;
  }

  const server = Bun.serve({
    port,
    async fetch(req, server) {
      const url = new URL(req.url);
      const r = route(url);
      if (r) {
        if (r.error) return r.error;
        return server.upgrade(req, { data: { open: r.open } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
      }
      if (fetchHandler) {
        const res = await fetchHandler(req);
        if (res) return res;
      }
      return new Response('Not found', { status: 404 });
    },
    websocket: {
      open(ws) {
        sessions.set(ws, ws.data.open(message => ws.send(JSON.stringify(message))));
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
