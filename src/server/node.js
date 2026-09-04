// node.js - Serve stores over WebSockets with Node and the `ws` package
//
// The same shape as the Bun adapter: one route, a hub per socket, the
// `authenticate(req)` and `authorize(user, storeId, store)` hooks, a
// payload ceiling, graceful `close()`. `authenticate` receives a Web
// `Request` built from the incoming Node request, so the same function
// serves both runtimes. `ws` is an optional peer dependency: install it
// to use this entry.
//
//   import { serve } from 'lazy-storage/server/node';
//   const server = serve({ stores, authenticate, authorize, port: 3200 });
//
// or mount inside your own http server:
//
//   const lazy = createHandlers({ stores, authenticate, authorize });
//   httpServer.on('upgrade', (req, socket, head) => { lazy.upgrade(req, socket, head); });
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { LazyWatch } from 'lazy-watch';
import { createHub } from './hub.js';
import { toJSON, closeUnauthorized } from './wire.js';

/** A Web Request for an incoming Node request, headers included */
export function toRequest(req) {
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) for (const v of value) headers.append(name, v);
    else if (value !== undefined) headers.set(name, value);
  }
  return new Request(`http://${req.headers.host ?? 'localhost'}${req.url}`, { method: req.method, headers });
}

/** Answer an upgrade request with a plain HTTP response and close the socket */
function refuse(socket, status, text) {
  if (socket.destroyed) return;
  socket.write(`HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${Buffer.byteLength(text)}\r\n\r\n${text}`);
  socket.destroy();
}

/**
 * @param {Object} options - as the Bun adapter's createHandlers
 * @returns {{ upgrade: (req: import('node:http').IncomingMessage, socket: import('node:stream').Duplex, head: Buffer) => Promise<boolean>, close: (options?: { reason?: string }) => Promise<void>, closing: boolean, wss: WebSocketServer }}
 *   `upgrade` resolves to false when the URL is not ours (answer it
 *   yourself), true when it took the socket
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
  const wss = new WebSocketServer({ noServer: true, maxPayload });
  const hubs = new Map();
  let closing = false;

  function open(ws, user) {
    // A broadcast is encoded once for every socket it reaches (see wire.js)
    const hub = createHub(resolveStore, {
      send: message => { if (ws.readyState === ws.OPEN) ws.send(toJSON(message)); },
      user,
      authorize,
      onError
    });
    hubs.set(ws, hub);
    ws.on('message', raw => {
      let msg;
      try {
        msg = JSON.parse(typeof raw === 'string' ? raw : raw.toString('utf8'));
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
    });
    ws.on('close', () => {
      hubs.get(ws)?.close();
      hubs.delete(ws);
    });
    // A socket error (a reset, a message over maxPayload) is the client's
    // affair and is followed by 'close'; the handler only keeps it from
    // becoming an unhandled event
    ws.on('error', () => {});
  }

  async function upgrade(req, socket, head) {
    if (new URL(req.url, 'http://localhost').pathname !== path) return false;
    try {
      if (closing) {
        refuse(socket, 503, 'Server shutting down');
        return true;
      }
      let user;
      if (authenticate) {
        user = await authenticate(toRequest(req));
        if (user === null || user === undefined) {
          // The handshake is completed only to be told why it was turned away (see wire.js)
          wss.handleUpgrade(req, socket, head, closeUnauthorized);
          return true;
        }
      }
      wss.handleUpgrade(req, socket, head, ws => open(ws, user));
    } catch (err) {
      onError(err);
      refuse(socket, 500, 'Upgrade failed');
    }
    return true;
  }

  /** Graceful shutdown, as the Bun adapter's: no new sockets, open ones told to go away, stores flushed */
  async function close({ reason = 'Server shutting down' } = {}) {
    closing = true;
    const sockets = [...hubs.keys()];
    const gone = Promise.all(sockets.map(ws => new Promise(resolve => {
      if (ws.readyState === ws.CLOSED) return resolve();
      ws.once('close', resolve);
    })));
    for (const ws of sockets) {
      try {
        ws.close(1001, reason);
      } catch (err) {
        onError(err);
      }
    }
    await Promise.race([gone, new Promise(resolve => setTimeout(resolve, 1000))]);
    for (const hub of hubs.values()) hub.close();
    hubs.clear();
    wss.close();
    if (typeof stores.dispose === 'function') stores.dispose();
  }

  return { upgrade, close, get closing() { return closing; }, wss };
}

/**
 * An http server with the handlers mounted.
 * @param {Object} options - createHandlers options plus:
 * @param {number} [options.port=3200]
 * @param {string} [options.host]
 * @param {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => void} [options.request]
 *   handles other requests (default: 404)
 * @returns {import('node:http').Server & { shutdown: (options?: { reason?: string }) => Promise<void> }}
 *   listening has been started; `await once(server, 'listening')` before
 *   reading `server.address().port`
 */
export function serve({ port = 3200, host, request, ...options } = {}) {
  const handlers = createHandlers(options);
  const server = createServer((req, res) => {
    if (request) return request(req, res);
    res.statusCode = 404;
    res.setHeader('content-type', 'text/plain');
    res.end('Not found');
  });
  server.on('upgrade', (req, socket, head) => {
    handlers.upgrade(req, socket, head).then(ours => { if (!ours) refuse(socket, 404, 'Not found'); });
  });
  /** Graceful shutdown (see createHandlers' close), then close the server */
  server.shutdown = async closeOptions => {
    await handlers.close(closeOptions);
    server.closeAllConnections?.();
    await new Promise(resolve => server.close(() => resolve()));
  };
  server.listen(port, host);
  return server;
}
