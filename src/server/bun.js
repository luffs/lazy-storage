// bun.js - Serve stores over WebSockets with Bun
//
// One route: a hub at `path`. Every socket carries any number of stores,
// with messages tagged by store id (see hub.js). `authenticate(req)` runs
// at upgrade and yields the user; when it returns null or undefined the
// socket is told so and closed (see closeUnauthorized in wire.js), a plain
// request gets a 401. `authorize(user, storeId, store)` runs per store, before its session
// exists, and a refusal reaches the client as a `closed` message with code
// 'forbidden' for that store alone. Both may return promises.
//
// `createHandlers` returns the pieces to mount inside your own Bun.serve
// (call `upgrade` from your fetch; pass `websocket` through); `serve` is
// the convenience wrapper that does it for you.
//
// Shutting down: `handlers.close()` refuses new sockets (503), closes the
// open ones with WebSocket code 1001 "going away" so clients reconnect at
// once instead of waiting out a dead connection, and disposes the store
// registry, which flushes every store's storage. With a persisted delta
// log the reconnect to the next process is a delta. `serve()` adds a
// `shutdown()` that does this and then stops the server.
import { LazyWatch } from 'lazy-watch';
import { createHub } from './hub.js';
import { toJSON, closeUnauthorized } from './wire.js';

/**
 * @param {Object} options
 * @param {Object|((id: string) => Object|null)} options.stores - a registry
 *   from createStores, or a resolver; for a single store, `() => store`
 * @param {string} [options.path='/ws'] - WebSocket path
 * @param {(req: Request) => any} [options.authenticate] - the user for a
 *   request, or null/undefined to turn it away: a socket is told so in a
 *   `closed` message with code 'unauthorized' and closed with code 4401,
 *   a plain request gets a 401; may return a promise
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 * @param {number} [options.maxPayload=4194304] - the largest message (bytes)
 *   a socket may send; Bun closes a socket that exceeds it. A hello carries
 *   at most 1000 ops, so a long offline spell stays well under 4 MB
 * @param {(error: any) => void} [options.onError] - server faults: a
 *   store factory that threw, a bug while handling a message; default console
 * @returns {{ upgrade: (req: Request, server: any) => Promise<Response|undefined|null>, websocket: Object, close: (options?: { reason?: string }) => Promise<void>, get closing(): boolean }}
 *   `upgrade` resolves to null when the URL is not ours, undefined after a
 *   successful upgrade, or an error Response; `close` is the graceful
 *   shutdown described above
 */
export function createHandlers({
  stores,
  path = '/ws',
  authenticate,
  authorize,
  maxPayload = 4 * 1024 * 1024,
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (!stores) throw new TypeError('createHandlers requires stores (a registry or a resolver function)');
  const resolveStore = typeof stores === 'function' ? stores : id => stores.get(id);
  const hubs = new Map();
  let closing = false;

  async function upgrade(req, server) {
    if (new URL(req.url).pathname !== path) return null;
    if (closing) return new Response('Server shutting down', { status: 503, headers: { 'retry-after': '1' } });
    let user;
    if (authenticate) {
      user = await authenticate(req);
      if (user === null || user === undefined) {
        // A handshake is completed only to be told why it was turned away
        // (see closeUnauthorized); a plain request gets the 401 itself
        return server.upgrade(req, { data: { unauthorized: true } }) ? undefined : new Response('Unauthorized', { status: 401 });
      }
    }
    return server.upgrade(req, { data: { user } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
  }

  const websocket = {
    maxPayloadLength: maxPayload,
    open(ws) {
      if (ws.data.unauthorized) return closeUnauthorized(ws);
      // A broadcast is encoded once for every socket it reaches (see wire.js)
      hubs.set(ws, createHub(resolveStore, { send: message => ws.send(toJSON(message)), user: ws.data.user, authorize, onError }));
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
        onError(err);
        ws.send(JSON.stringify({ t: 'error', store: msg.store, message: 'Something went wrong' }));
      }
    },
    close(ws) {
      hubs.get(ws)?.close();
      hubs.delete(ws);
    }
  };

  /**
   * Graceful shutdown: no new sockets, the open ones told to go away, the
   * stores flushed (through the registry's dispose, when `stores` is one;
   * a resolver function's stores are the caller's to flush). Resolves once
   * every socket has closed.
   */
  async function close({ reason = 'Server shutting down' } = {}) {
    closing = true;
    const sockets = [...hubs.keys()];
    const gone = Promise.all(sockets.map(ws => new Promise(resolve => {
      const hub = hubs.get(ws);
      hubs.set(ws, { receive() {}, close() { hub?.close(); resolve(); } });
    })));
    for (const ws of sockets) {
      try {
        ws.close(1001, reason);
      } catch (err) {
        onError(err);
      }
    }
    // A socket that never reports its close (already gone) must not hold the shutdown
    await Promise.race([gone, new Promise(resolve => setTimeout(resolve, 1000))]);
    for (const hub of hubs.values()) hub.close();
    hubs.clear();
    if (typeof stores.dispose === 'function') stores.dispose();
  }

  return { upgrade, websocket, close, get closing() { return closing; } };
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
  const server = Bun.serve({
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
  /** Graceful shutdown (see createHandlers' close), then stop the server */
  server.shutdown = async options => {
    await handlers.close(options);
    server.stop(true);
  };
  return server;
}
