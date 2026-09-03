// bun.js - Serve stores over WebSockets with Bun
//
// Messages are JSON. Routes:
//   { store }    path            one store, a plain session per socket
//   { stores }   path            a hub: many stores over one socket,
//                                messages tagged with `store`
//                path/<storeId>  one store from the registry, a plain
//                                session per socket (the single-store
//                                client protocol, unchanged)
//
// `authenticate(req)` runs at upgrade and yields the user (401 when it
// returns null or undefined); `authorize(user, storeId, store)` runs before
// a store session exists — at upgrade for the per-store route (403), and
// per store on a hub (a `closed` message with code 'forbidden'). Both may
// return promises. Evicted sessions get their socket closed on the
// per-store route and their hub entry removed on a hub.
//
// `createHandlers` returns the pieces to mount inside your own Bun.serve
// (call `upgrade` from your fetch; pass `websocket` through); `serve` is
// the convenience wrapper that does it for you.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';
import { createHub } from './hub.js';

/**
 * @param {Object} options
 * @param {Object} [options.store] - a single store, served at `path`
 * @param {Object|((id: string) => Object|null)} [options.stores] - a registry
 *   or resolver: a hub at `path`, plain sessions at `path/<id>`
 * @param {string} [options.path='/ws'] - WebSocket path (or path prefix)
 * @param {(req: Request) => any} [options.authenticate] - the user for a
 *   request, or null/undefined to refuse (401); may return a promise
 * @param {(user: any, storeId: string|undefined, store: Object) => boolean|Promise<boolean>} [options.authorize]
 * @returns {{ upgrade: (req: Request, server: any) => Promise<Response|undefined|null>, websocket: Object }}
 *   `upgrade` resolves to null when the URL is not one of ours, undefined
 *   after a successful upgrade, or an error Response
 */
export function createHandlers({ store, stores, path = '/ws', authenticate, authorize } = {}) {
  if (!store && !stores) throw new TypeError('createHandlers requires a store or stores');
  if (store && stores) throw new TypeError('createHandlers takes either a single store or stores, not both');
  const resolveStore = typeof stores === 'function' ? stores : stores ? id => stores.get(id) : null;
  const sessions = new Map();

  // What a URL asks for: { store, id } for a plain session, { hub: true }, or { error }
  function route(url) {
    if (url.pathname === path) return store ? { store, id: undefined } : { hub: true };
    if (resolveStore && url.pathname.startsWith(path + '/')) {
      let id;
      try {
        id = decodeURIComponent(url.pathname.slice(path.length + 1));
      } catch {
        return { error: new Response('Invalid store id', { status: 400 }) };
      }
      if (!isStoreId(id)) return { error: new Response('Invalid store id', { status: 400 }) };
      const found = resolveStore(id);
      return found ? { store: found, id } : { error: new Response('Unknown store', { status: 404 }) };
    }
    return null;
  }

  const sendOf = ws => message => ws.send(JSON.stringify(message));

  async function upgrade(req, server) {
    const r = route(new URL(req.url));
    if (!r) return null;
    if (r.error) return r.error;

    let user;
    if (authenticate) {
      user = await authenticate(req);
      if (user === null || user === undefined) return new Response('Unauthorized', { status: 401 });
    }
    let open;
    if (r.hub) {
      open = ws => createHub(resolveStore, { send: sendOf(ws), user, authorize });
    } else {
      if (authorize && !(await authorize(user, r.id, r.store))) return new Response('Forbidden', { status: 403 });
      open = ws => r.store.session({ send: sendOf(ws), user, onEvict: () => ws.close(4403, 'Session closed by the server') });
    }
    return server.upgrade(req, { data: { open } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
  }

  const websocket = {
    open(ws) {
      sessions.set(ws, ws.data.open(ws));
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
  };

  return { upgrade, websocket };
}

/**
 * Bun.serve with the handlers mounted.
 * @param {Object} options - createHandlers options plus:
 * @param {number} [options.port=3200]
 * @param {(req: Request) => Response|null|Promise<Response|null>} [options.fetch]
 *   handles other requests; return null to fall through to 404
 */
export function serve({ port = 3200, fetch: fetchHandler, ...options } = {}) {
  const handlers = createHandlers(options);
  return Bun.serve({
    port,
    async fetch(req, server) {
      const res = await handlers.upgrade(req, server);
      if (res !== null) return res;
      if (fetchHandler) {
        const own = await fetchHandler(req);
        if (own) return own;
      }
      return new Response('Not found', { status: 404 });
    },
    websocket: handlers.websocket
  });
}
