// bun.js - Serve a store over WebSockets with Bun.serve
//
// Each socket gets one session; messages are JSON. Everything else (state,
// merge, persistence) lives in the store, so this file is the only
// runtime-specific one on the server side.
import { LazyWatch } from 'lazy-watch';

/**
 * @param {Object} options
 * @param {Object} options.store - a store from createStore
 * @param {number} [options.port=3200]
 * @param {string} [options.path='/ws'] - WebSocket path
 * @param {(req: Request) => Response|null|Promise<Response|null>} [options.fetch]
 *   handles non-WebSocket requests; return null to fall through to 404
 */
export function serve({ store, port = 3200, path = '/ws', fetch: fetchHandler } = {}) {
  if (!store) throw new TypeError('serve requires a store');
  const sessions = new Map();

  const server = Bun.serve({
    port,
    async fetch(req, server) {
      const url = new URL(req.url);
      if (url.pathname === path) {
        return server.upgrade(req) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
      }
      if (fetchHandler) {
        const res = await fetchHandler(req);
        if (res) return res;
      }
      return new Response('Not found', { status: 404 });
    },
    websocket: {
      open(ws) {
        sessions.set(ws, store.session({ send: message => ws.send(JSON.stringify(message)) }));
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
