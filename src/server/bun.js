// bun.js - Serve stores over WebSockets with Bun
//
// One route: a hub at `path`. Every socket carries any number of stores,
// with messages tagged by store id (see hub.js). `authenticate(req)` runs
// at upgrade and yields the user (401 when it returns null or undefined);
// `authorize(user, storeId, store)` runs per store, before its session
// exists, and a refusal reaches the client as a `closed` message with code
// 'forbidden' for that store alone. Both may return promises.
//
// `createHandlers` returns the pieces to mount inside your own Bun.serve
// (call `upgrade` from your fetch; pass `websocket` through); `serve` is
// the convenience wrapper that does it for you.
import { LazyWatch } from 'lazy-watch';
import { createHub } from './hub.js';
import { toJSON } from './wire.js';

/**
 * @param {Object} options
 * @param {Object|((id: string) => Object|null)} options.stores - a registry
 *   from createStores, or a resolver; for a single store, `() => store`
 * @param {string} [options.path='/ws'] - WebSocket path
 * @param {(req: Request) => any} [options.authenticate] - the user for a
 *   request, or null/undefined to refuse (401); may return a promise
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 * @returns {{ upgrade: (req: Request, server: any) => Promise<Response|undefined|null>, websocket: Object }}
 *   `upgrade` resolves to null when the URL is not ours, undefined after a
 *   successful upgrade, or an error Response
 */
export function createHandlers({ stores, path = '/ws', authenticate, authorize } = {}) {
  if (!stores) throw new TypeError('createHandlers requires stores (a registry or a resolver function)');
  const resolveStore = typeof stores === 'function' ? stores : id => stores.get(id);
  const hubs = new Map();

  async function upgrade(req, server) {
    if (new URL(req.url).pathname !== path) return null;
    let user;
    if (authenticate) {
      user = await authenticate(req);
      if (user === null || user === undefined) return new Response('Unauthorized', { status: 401 });
    }
    return server.upgrade(req, { data: { user } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
  }

  const websocket = {
    open(ws) {
      // A broadcast is encoded once for every socket it reaches (see wire.js)
      hubs.set(ws, createHub(resolveStore, { send: message => ws.send(toJSON(message)), user: ws.data.user, authorize }));
    },
    message(ws, raw) {
      let msg;
      try {
        msg = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
      } catch {
        return ws.send(JSON.stringify({ t: 'error', message: 'Expected JSON' }));
      }
      if (!LazyWatch.Utils.isPlainObject(msg)) return ws.send(JSON.stringify({ t: 'error', message: 'Expected a message object' }));
      // A bug below this point must not take the process down with it
      try {
        hubs.get(ws)?.receive(msg);
      } catch (err) {
        console.error('lazy-storage: unhandled error while handling a message:', err);
        ws.send(JSON.stringify({ t: 'error', store: msg.store, message: 'Something went wrong' }));
      }
    },
    close(ws) {
      hubs.get(ws)?.close();
      hubs.delete(ws);
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
