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
import { toJSON, closeUnauthorized, deflateOptions, TOO_FAR_BEHIND, REAUTHENTICATE, rollUpSockets } from './wire.js';
import { snapshotOptions, snapshotId, serveSnapshot } from './snapshot.js';

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
 * @param {Object} options - as the Bun adapter's createHandlers, plus:
 * @param {number|false} [options.idleTimeout=120000] - close a socket that
 *   has sent nothing for this long (ms), as Bun's own idle timeout does: a
 *   half-open connection would otherwise hold its sessions, and its place
 *   in presence, until the OS gave up on it. A client pings every 30 s, so
 *   a live one is never idle. false keeps sockets however quiet
 * @param {number|false} [options.maxBuffered=16777216] - close a socket
 *   whose unsent output passes this many bytes: a client that stopped
 *   reading would otherwise hold every patch in memory. It reconnects and
 *   catches up with a delta. false lets the buffer grow
 * @returns {{ upgrade: (req: import('node:http').IncomingMessage, socket: import('node:stream').Duplex, head: Buffer) => Promise<boolean>, request: (req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => Promise<boolean>, close: (options?: { reason?: string }) => Promise<void>, closing: boolean, sockets: () => Array<Object>, socketStats: () => Object, disconnect: (filter?: (user: any) => boolean) => number, revalidate: (filter?: (user: any, storeId: string) => boolean) => Promise<number>, wss: WebSocketServer }}
 *   `upgrade` resolves to false when the URL is not ours (answer it
 *   yourself), true when it took the socket; `request` likewise for a
 *   plain request, which is ours when it is the snapshot route.
 *   `sockets()` lists the open sockets, each `{ user, stores, buffered,
 *   idleMs, openMs }` (bytes unsent, time since it last sent something,
 *   time since it opened), and `socketStats()` rolls them up (see
 *   rollUpSockets in wire.js). `disconnect` and `revalidate` are the Bun
 *   adapter's
 */
export function createHandlers({
  stores,
  path = '/ws',
  authenticate,
  authorizeId,
  authorize,
  maxPayload = 4 * 1024 * 1024,
  perMessageDeflate = true,
  httpSnapshots = true,
  idleTimeout = 120_000,
  maxBuffered = 16 * 1024 * 1024,
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (!stores) throw new TypeError('createHandlers requires stores (a registry or a resolver function)');
  const resolveStore = typeof stores === 'function' ? stores : id => stores.get(id);
  const snapshots = snapshotOptions(httpSnapshots);
  const snapshotRoute = snapshots && { threshold: snapshots.threshold, url: id => `${path}/snapshot/${id}` };
  // Compression is decided per message, above `threshold` only, as the Bun
  // adapter does. ws is asked for no context takeover both ways, which
  // every client accepts, and takes any of its own options from the object
  // form. The window stays at the standard 15 bits: a smaller one would
  // compress this JSON a little better and hold less memory per socket,
  // but has to be negotiated, and ws turns away a client whose offer does
  // not carry the parameter. A socket that has received a compressed
  // message keeps a deflate stream (some 75 KB) for its lifetime, and one
  // that has sent one an inflate stream (some 100 KB)
  const deflate = deflateOptions(perMessageDeflate);
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload,
    perMessageDeflate: deflate === null
      ? false
      : { serverNoContextTakeover: true, clientNoContextTakeover: true, ...(deflate.runtime === true ? {} : deflate.runtime), threshold: deflate.threshold }
  });
  const send = (ws, message) => {
    if (ws.readyState !== ws.OPEN) return;
    if (maxBuffered && ws.bufferedAmount > maxBuffered) {
      // 1013, try again later: the client reconnects and its hello asks for
      // what it missed
      cutOff++;
      shut(ws, TOO_FAR_BEHIND);
      return;
    }
    const json = toJSON(message);
    ws.send(json, { compress: deflate !== null && Buffer.byteLength(json) >= deflate.threshold });
  };
  /**
   * Close a socket with a code, and drop it after a second if it has not
   * answered: one that stopped reading never does, and ws would wait that
   * out for 30 s with its session still attached
   */
  const shut = (ws, [code, reason]) => {
    ws.close(code, reason);
    const drop = setTimeout(() => ws.terminate(), 1000);
    if (typeof drop.unref === 'function') drop.unref();
  };
  const hubs = new Map();
  const heard = new Map();   // socket -> when it last sent something, for idleTimeout
  const opened = new Map();  // socket -> when it opened
  let closing = false;
  let cutOff = 0;            // sockets closed for falling behind
  let disconnected = 0;      // sockets closed by disconnect()
  let revoked = 0;           // store sessions closed by revalidate()
  const sweeper = idleTimeout ? setInterval(() => {
    const now = Date.now();
    for (const [ws, at] of heard) if (now - at > idleTimeout) ws.terminate();
  }, Math.max(100, Math.min(idleTimeout / 4, 30_000))) : null;
  if (typeof sweeper?.unref === 'function') sweeper.unref();

  function open(ws, user) {
    // A broadcast is encoded once for every socket it reaches (see wire.js)
    const hub = createHub(resolveStore, {
      send: message => send(ws, message),
      user,
      authorizeId,
      authorize,
      httpSnapshots: snapshotRoute,
      onError
    });
    hubs.set(ws, hub);
    heard.set(ws, Date.now());
    opened.set(ws, Date.now());
    ws.on('message', raw => {
      heard.set(ws, Date.now());
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
      heard.delete(ws);
      opened.delete(ws);
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

  /**
   * Handle a plain request when it is ours, the snapshot route (see
   * snapshot.js); resolves to false when it is not, for the app to answer
   */
  async function request(req, res) {
    const id = snapshots ? snapshotId(new URL(req.url, 'http://localhost').pathname, path) : null;
    if (id === null) return false;
    let response;
    if (closing) {
      response = new Response('Server shutting down', { status: 503, headers: { 'retry-after': '1' } });
    } else {
      try {
        response = await serveSnapshot(toRequest(req), id, { resolveStore, authenticate, authorizeId, authorize, onError, origins: snapshots.origins });
      } catch (err) {
        onError(err);
        response = new Response('Something went wrong', { status: 500 });
      }
    }
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(response.body === null ? undefined : Buffer.from(await response.arrayBuffer()));
    return true;
  }

  /** Graceful shutdown, as the Bun adapter's: no new sockets, open ones told to go away, stores flushed */
  async function close({ reason = 'Server shutting down' } = {}) {
    closing = true;
    clearInterval(sweeper);
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

  /** Close the sockets whose user `filter(user)` picks, to authenticate afresh, as the Bun adapter's */
  function disconnect(filter) {
    if (closing) return 0;
    let count = 0;
    for (const [ws, hub] of [...hubs]) {
      if (filter && !filter(hub.user)) continue;
      hub.close();
      hubs.delete(ws);
      heard.delete(ws);
      opened.delete(ws);
      shut(ws, REAUTHENTICATE);
      count++;
    }
    disconnected += count;
    return count;
  }

  /** Judge the open store sessions again, as the Bun adapter's; resolves to how many were closed */
  async function revalidate(filter) {
    if (closing) return 0;
    let count = 0;
    for (const closedOnes of await Promise.all([...hubs.values()].map(hub => hub.revalidate(filter)))) count += closedOnes;
    revoked += count;
    return count;
  }

  /** The open sockets, how far behind each is (see the returns above) */
  function sockets() {
    const now = Date.now();
    return [...hubs].map(([ws, hub]) => ({
      user: hub.user,
      stores: hub.stores,
      buffered: ws.bufferedAmount,
      idleMs: now - heard.get(ws),
      openMs: now - opened.get(ws)
    }));
  }

  return { upgrade, request, close, get closing() { return closing; }, sockets, socketStats: () => rollUpSockets(sockets(), { cutOff, disconnected, revoked }), disconnect, revalidate, wss };
}

/**
 * An http server with the handlers mounted.
 * @param {Object} options - createHandlers options plus:
 * @param {number} [options.port=3200]
 * @param {string} [options.host]
 * @param {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => void} [options.request]
 *   handles other requests (default: 404); the snapshot route is answered first
 * @returns {import('node:http').Server & { shutdown: (options?: { reason?: string }) => Promise<void> }}
 *   listening has been started; `await once(server, 'listening')` before
 *   reading `server.address().port`
 */
export function serve({ port = 3200, host, request, ...options } = {}) {
  const handlers = createHandlers(options);
  const server = createServer((req, res) => {
    handlers.request(req, res).then(ours => {
      if (ours) return;
      if (request) return request(req, res);
      res.statusCode = 404;
      res.setHeader('content-type', 'text/plain');
      res.end('Not found');
    });
  });
  server.on('upgrade', (req, socket, head) => {
    handlers.upgrade(req, socket, head).then(ours => { if (!ours) refuse(socket, 404, 'Not found'); });
  });
  server.sockets = handlers.sockets;
  server.socketStats = handlers.socketStats;
  server.disconnect = handlers.disconnect;
  server.revalidate = handlers.revalidate;
  /** Graceful shutdown (see createHandlers' close), then close the server */
  server.shutdown = async closeOptions => {
    await handlers.close(closeOptions);
    server.closeAllConnections?.();
    await new Promise(resolve => server.close(() => resolve()));
  };
  server.listen(port, host);
  return server;
}
