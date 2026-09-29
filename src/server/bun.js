// bun.js - Serve stores over WebSockets with Bun
//
// One route: a hub at `path`. Every socket carries any number of stores,
// with messages tagged by store id (see hub.js). `authenticate(req)` runs
// at upgrade and yields the user; when it returns null or undefined the
// socket is told so and closed (see closeUnauthorized in wire.js), a plain
// request gets a 401. `authorize(user, storeId, store)` runs per store, before its session
// exists, and a refusal reaches the client as a `closed` message with code
// 'forbidden' for that store alone. Both may return promises. A socket
// keeps its user until it closes: `expiresAt(user, req)` says when that
// session runs out, `disconnect(filter)` ends it now, and either way the
// client reconnects and authenticates afresh; `revalidate(filter)` judges
// its open stores again.
//
// Relays: with `relays`, a relay on a LAN that carries many clients (see
// src/relay and relays.js) connects at `<path>/relay`, authenticated as
// itself by `relays.authenticate(req)`, and reads each store once for all
// its clients, each of whom the server judges by the same hooks as its own
// socket. `disconnect`, `revalidate` and expiry reach those clients too.
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
import { createRelayLink, credentialRequest, relaySockets } from './relays.js';
import { toJSON, closeUnauthorized, deflateOptions, TOO_FAR_BEHIND, REAUTHENTICATE, MIN_SESSION_MS, expiryOf, runAt, rollUpSockets } from './wire.js';
import { snapshotOptions, snapshotId, serveSnapshot } from './snapshot.js';

/**
 * @param {Object} options
 * @param {Object|((id: string) => Object|null)} options.stores - a registry
 *   from createStores, or a resolver; for a single store, `() => store`
 * @param {string} [options.path='/ws'] - WebSocket path
 * @param {(req: Request) => any} [options.authenticate] - the user for a
 *   request, or null/undefined to turn it away: a socket is told so in a
 *   `closed` message with code 'unauthorized' and closed with code 4401,
 *   a plain request gets a 401; may return a promise
 * @param {(user: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeId]
 *   whether the user may open a store, judged before the store is loaded:
 *   false (or a throw) closes it with 'forbidden' and nothing is loaded,
 *   whether the store exists or not. Prefer it to `authorize` for any
 *   check that needs only the user and the id
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 *   the same, judged once the store is loaded, for a check that needs it
 * @param {(user: any, req: Request) => number|Date|null|undefined|Promise<number|Date|null|undefined>} [options.expiresAt]
 *   when the session `authenticate` let in runs out (ms since the epoch, or
 *   a Date; nothing for never), read at upgrade. The socket is closed then
 *   with code 4001, as `disconnect` does, and the client reconnects and
 *   authenticates afresh with whatever credentials it has by then. A
 *   session with less than `minSession` left is turned away as unauthorized
 * @param {number} [options.minSession=30000] - the shortest session (ms)
 *   an `expiresAt` may leave: a token that runs out sooner would have its
 *   socket closed again in a moment, over and over. Lower it for tokens
 *   that live about a minute
 * @param {number} [options.maxPayload=4194304] - the largest message (bytes)
 *   a socket may send; Bun closes a socket that exceeds it. A hello carries
 *   at most 1000 ops, so a long offline spell stays well under 4 MB
 * @param {boolean|Object} [options.perMessageDeflate=true] - offer the
 *   permessage-deflate extension, so a browser that takes it receives
 *   large messages compressed (the JSON of a big store shrinks about
 *   tenfold, at some 3 ms of CPU per megabyte per client). Messages under
 *   `threshold` bytes (default 1024) go plain: compressing a hundred-byte
 *   patch costs ten times its encoding and makes it larger. An object
 *   sets `threshold` and any of Bun's own options (`compress`,
 *   `decompress`); false turns it off
 * @param {boolean|{ threshold?: number }} [options.httpSnapshots=true] -
 *   serve snapshots over HTTP at `<path>/snapshot/<store id>`, compressed
 *   once per change (brotli or gzip, as the client accepts): a client
 *   that can fetch (webSocketTransport can) is
 *   pointed there in place of a snapshot of `threshold` bytes or more
 *   (default 64 KB) instead of having the state compressed and buffered
 *   for its socket alone. The route authenticates and authorizes like an
 *   upgrade. `origins` says which origins may fetch it cross-origin: '*'
 *   (the default: credentials travel in the URL as for the socket, and a
 *   browser withholds cookies under '*'), an array of origins to echo and
 *   no other, or false for no CORS header at all. false turns the route
 *   off; every client then gets its snapshot inline
 * @param {Object} [options.relays] - let relays carry clients (see relays.js):
 *   `authenticate(req)` the relay a request is (null turns it away, as for
 *   a client), `expiresAt(relay, req)` when that runs out (as `expiresAt`),
 *   `authorize(relay, storeId)` whether it may carry a store at all
 *   (default: any its clients may read; judged again by `revalidate`),
 *   `path` (default `<path>/relay`), `linger` (ms its session on a store
 *   outlives the store's last client there, default 30000), `headers` (more
 *   names of a client's headers its credential may carry, besides
 *   authorization, cookie and user-agent), `rate` (vouches turned away as
 *   unauthorized before every vouch waits: { burst, perSecond }, default
 *   100 and 10). A client a relay vouches for is judged by `authenticate`
 *   and `expiresAt` given `{ relay }` as their last argument.
 *   `disconnect(filter)` cuts off a relay the filter picks (it is handed the
 *   relay as the user) with its clients
 * @param {number|false} [options.maxBuffered=16777216] - close a socket
 *   whose unsent output passes this many bytes, with code 1013: a client
 *   that stopped reading would otherwise have Bun drop what it cannot
 *   buffer (past its own `backpressureLimit`) and keep the socket open. It
 *   reconnects and catches up with a delta. Checked on every write (what
 *   one task sent the socket, written together) and, for store broadcasts
 *   (a topic publish, which Bun fans out itself), once a second. false
 *   leaves it to Bun, which drops past 16 MB
 * @param {(error: any) => void} [options.onError] - server faults: a
 *   store factory that threw, a bug while handling a message; default console
 * @returns {{ upgrade: (req: Request, server: any) => Promise<Response|undefined|null>, websocket: Object, close: (options?: { reason?: string }) => Promise<void>, get closing(): boolean, sockets: () => Array<Object>, socketStats: () => Object, disconnect: (filter?: (user: any) => boolean) => number, revalidate: (filter?: (user: any, storeId: string) => boolean) => Promise<number> }}
 *   `upgrade` resolves to null when the URL is not ours, undefined after a
 *   successful upgrade, or a Response (the snapshot route's, or an error);
 *   `close` is the graceful shutdown described above. `sockets()` lists
 *   the open sockets, each `{ user, stores, buffered, idleMs, openMs,
 *   expiresAt }` (bytes unsent, time since it last sent something, time
 *   since it opened, when its session runs out or null), and `socketStats()` rolls them up (see rollUpSockets in
 *   wire.js). `disconnect(filter)` closes the sockets of the users it
 *   picks, which reconnect and authenticate afresh (after a logout, a
 *   changed role, a deleted account); `revalidate(filter)` runs the
 *   authorize hooks again on the open stores it picks and closes those
 *   now refused (after a change to who may open a store)
 */
export function createHandlers({
  stores,
  path = '/ws',
  authenticate,
  authorizeId,
  authorize,
  expiresAt,
  minSession = MIN_SESSION_MS,
  maxPayload = 4 * 1024 * 1024,
  perMessageDeflate = true,
  httpSnapshots = true,
  maxBuffered = 16 * 1024 * 1024,
  relays,
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (!stores) throw new TypeError('createHandlers requires stores (a registry or a resolver function)');
  if (relays !== undefined && typeof relays?.authenticate !== 'function') throw new TypeError('relays needs authenticate(req): the relay a request is');
  const relayPath = relays ? relays.path ?? `${path}/relay` : null;
  const resolveStore = typeof stores === 'function' ? stores : id => stores.get(id);
  const deflate = deflateOptions(perMessageDeflate);
  const snapshots = snapshotOptions(httpSnapshots);
  // Where the hub points a client that can fetch for a large snapshot
  const snapshotRoute = snapshots && { threshold: snapshots.threshold, url: id => `${path}/snapshot/${id}` };
  // Bun compresses a frame only when asked per send; every message is
  // encoded once (see wire.js) and sent compressed when it is large enough.
  // What goes to a socket in one task goes out in one write (ws.cork), once
  // the task is done (a microtask): a socket costs a system call for every
  // send whatever it carries, and a store sends a turn's acks and answers
  // together, after the turn's commit (see its groupCommit), outside the
  // callback in which Bun would cork them itself. Each socket hears what it
  // is sent in the order it was sent: a topic publish goes out at once, so
  // the sockets that hear it get what waits for them first (see publish),
  // and a socket the server closes gets what waits for it before it goes
  const queued = new Map();     // socket -> [json, compress][] not yet written
  const topicsOf = new Map();   // socket -> the stores it hears the broadcasts of
  let flushing = false;
  const write = (ws, messages) => {
    if (behind(ws)) return;
    try {
      if (messages.length === 1) ws.send(messages[0][0], messages[0][1]);
      else ws.cork(() => { for (const [json, compress] of messages) ws.send(json, compress); });
    } catch (err) {
      onError(err);
    }
  };
  /** What waits for this socket, written now */
  const writeQueued = ws => {
    const messages = queued.get(ws);
    if (!messages) return;
    queued.delete(ws);
    write(ws, messages);
  };
  const flush = () => {
    flushing = false;
    for (const ws of [...queued.keys()]) writeQueued(ws);
  };
  const send = (ws, message) => {
    const json = toJSON(message);
    const entry = [json, deflate !== null && json.length >= deflate.threshold];
    const waiting = queued.get(ws);
    if (waiting) waiting.push(entry);
    else queued.set(ws, [entry]);
    if (!flushing) {
      flushing = true;
      queueMicrotask(flush);
    }
  };
  const hubs = new Map();
  const links = new Map();  // relay sockets -> their link (see relays.js)
  const seen = new Map();   // socket -> { opened, heard, expires, cancel }: when it opened, last sent something, runs out; the expiry's timer
  let closing = false;
  let cutOff = 0;           // sockets closed for falling behind
  let disconnected = 0;     // sockets closed by disconnect()
  let expired = 0;          // sockets closed as their session ran out
  let revoked = 0;          // store sessions closed by revalidate()
  /** Close a socket over maxBuffered; true when it was (1013: the client reconnects and asks for what it missed) */
  const behind = ws => {
    if (!maxBuffered || ws.getBufferedAmount() <= maxBuffered) return false;
    cutOff++;
    ws.close(...TOO_FAR_BEHIND);
    return true;
  };
  // A store broadcast is one topic publish, fanned out by Bun past the
  // send above, so every socket is also looked at once a second
  const sweeper = maxBuffered ? setInterval(() => { for (const ws of [...hubs.keys(), ...links.keys()]) behind(ws); }, 1000) : null;
  if (typeof sweeper?.unref === 'function') sweeper.unref();
  let bunServer = null;   // captured at the first upgrade, for topic broadcasts
  const topic = id => `lz:${id}`;
  const shouldCompress = json => deflate !== null && json.length >= deflate.threshold;

  async function upgrade(req, server) {
    bunServer = server;
    const { pathname } = new URL(req.url);
    const snapshotOf = snapshots ? snapshotId(pathname, path) : null;
    if (pathname !== path && snapshotOf === null && pathname !== relayPath) return null;
    if (closing) return new Response('Server shutting down', { status: 503, headers: { 'retry-after': '1' } });
    if (pathname === relayPath) {
      const turnAwayRelay = () => (server.upgrade(req, { data: { unauthorized: true } }) ? undefined : new Response('Unauthorized', { status: 401 }));
      const relay = await relays.authenticate(req);
      if (relay === null || relay === undefined) return turnAwayRelay();
      const expires = relays.expiresAt ? expiryOf(await relays.expiresAt(relay, req)) : null;
      if (expires !== null && expires - Date.now() < minSession) return turnAwayRelay();
      return server.upgrade(req, { data: { relay, expires } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
    }
    if (snapshotOf !== null) {
      try {
        return await serveSnapshot(req, snapshotOf, { resolveStore, authenticate, authorizeId, authorize, onError, origins: snapshots.origins });
      } catch (err) {
        onError(err);
        return new Response('Something went wrong', { status: 500 });
      }
    }
    // A handshake is completed only to be told why it was turned away (see
    // closeUnauthorized); a plain request gets the 401 itself
    const turnAway = () => (server.upgrade(req, { data: { unauthorized: true } }) ? undefined : new Response('Unauthorized', { status: 401 }));
    let user;
    if (authenticate) {
      user = await authenticate(req);
      if (user === null || user === undefined) return turnAway();
    }
    const expires = expiresAt ? expiryOf(await expiresAt(user, req)) : null;
    if (expires !== null && expires - Date.now() < minSession) return turnAway();
    return server.upgrade(req, { data: { user, expires } }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
  }

  /**
   * Who a client a relay vouches for is, as its own socket would have been
   * judged: `authenticate` and `expiresAt` over the request it would have
   * made (see credentialRequest in relays.js), told which relay asks
   * (`{ relay }`, their last argument); null when turned away
   */
  async function admit(credential, relay) {
    const req = credentialRequest(credential, path, relays.headers ?? []);
    let user;
    if (authenticate) {
      user = await authenticate(req, { relay });
      if (user === null || user === undefined) return null;
    }
    const expires = expiresAt ? expiryOf(await expiresAt(user, req, { relay })) : null;
    if (expires !== null && expires - Date.now() < minSession) return null;
    return { user, expires };
  }

  const websocket = {
    maxPayloadLength: maxPayload,
    perMessageDeflate: deflate === null ? false : deflate.runtime,
    // Bun drops what a socket cannot buffer past this; above maxBuffered,
    // so the socket is closed (and the client catches up) before it would
    ...(maxBuffered ? { backpressureLimit: maxBuffered * 2 } : {}),
    open(ws) {
      if (ws.data.unauthorized) return closeUnauthorized(ws);
      // Each socket subscribes per store; a broadcast goes out once as a topic
      // publish, which Bun fans out natively, rather than a send per socket from
      // JavaScript. Bun still writes (and compresses, past the threshold) per subscriber
      const channel = {
        subscribe: id => {
          ws.subscribe(topic(id));
          if (!topicsOf.has(ws)) topicsOf.set(ws, new Set());
          topicsOf.get(ws).add(id);
        },
        unsubscribe: id => {
          topicsOf.get(ws)?.delete(id);
          try { ws.unsubscribe(topic(id)); } catch { /* a closing socket is already gone */ }
        },
        publish: (id, message) => {
          // A socket that hears the store gets what was queued for it first (see send)
          for (const other of [...queued.keys()]) if (topicsOf.get(other)?.has(id)) writeQueued(other);
          const json = toJSON(message);
          bunServer.publish(topic(id), json, shouldCompress(json));
        }
      };
      if (ws.data.relay !== undefined) {
        links.set(ws, createRelayLink(resolveStore, { send: message => send(ws, message), relay: ws.data.relay, admit, authorizeRelay: relays.authorize, authorizeId, authorize, channel, linger: relays.linger, rate: relays.rate, onError }));
        const relayTimes = { opened: Date.now(), heard: Date.now(), expires: ws.data.expires ?? null, cancel: null };
        seen.set(ws, relayTimes);
        // A relay's own session runs out as a client's does: closed, and it comes back judged afresh
        if (relayTimes.expires !== null) {
          relayTimes.cancel = runAt(relayTimes.expires, () => {
            if (closing || !links.has(ws)) return;
            expired++;
            reauthenticate(ws);
          });
        }
        return;
      }
      hubs.set(ws, createHub(resolveStore, { send: message => send(ws, message), user: ws.data.user, authorizeId, authorize, channel, httpSnapshots: snapshotRoute, onError }));
      const times = { opened: Date.now(), heard: Date.now(), expires: ws.data.expires ?? null, cancel: null };
      seen.set(ws, times);
      if (times.expires !== null) {
        times.cancel = runAt(times.expires, () => {
          if (closing || !hubs.has(ws)) return;
          expired++;
          reauthenticate(ws);
        });
      }
    },
    message(ws, raw) {
      const times = seen.get(ws);
      if (times) times.heard = Date.now();
      let msg;
      try {
        msg = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
      } catch {
        return send(ws, { t: 'error', message: 'Expected JSON' });
      }
      if (!LazyWatch.Utils.isPlainObject(msg)) return send(ws, { t: 'error', message: 'Expected a message object' });
      // A bug below this point must not take the process down with it
      try {
        (hubs.get(ws) ?? links.get(ws))?.receive(msg);
      } catch (err) {
        onError(err);
        send(ws, { t: 'error', store: msg.store, message: 'Something went wrong' });
      }
    },
    close(ws) {
      hubs.get(ws)?.close();
      hubs.delete(ws);
      links.get(ws)?.close();
      links.delete(ws);
      seen.get(ws)?.cancel?.();
      seen.delete(ws);
      // Gone: nothing more is written to it
      queued.delete(ws);
      topicsOf.delete(ws);
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
    clearInterval(sweeper);
    for (const times of seen.values()) times.cancel?.();
    const sockets = [...hubs.keys(), ...links.keys()];
    const gone = Promise.all(sockets.map(ws => new Promise(resolve => {
      const map = hubs.has(ws) ? hubs : links;
      const hub = map.get(ws);
      map.set(ws, { receive() {}, close() { hub?.close(); resolve(); } });
    })));
    flush();   // what the sockets were sent before goes before the close
    for (const ws of sockets) {
      try {
        ws.close(1001, reason);
      } catch (err) {
        onError(err);
      }
    }
    // A socket that never reports its close (already gone) must not hold the shutdown
    await Promise.race([gone, new Promise(resolve => setTimeout(resolve, 1000))]);
    for (const hub of [...hubs.values(), ...links.values()]) hub.close();
    hubs.clear();
    links.clear();
    if (typeof stores.dispose === 'function') stores.dispose();
  }

  /**
   * Close the sockets whose user `filter(user)` picks (every one, without
   * a filter) for their clients to reconnect and authenticate afresh (see
   * REAUTHENTICATE in wire.js). Their sessions end at once: nothing more
   * reaches these sockets or is taken from them. Returns how many
   */
  function disconnect(filter) {
    if (closing) return 0;
    let count = 0;
    for (const [ws, hub] of [...hubs]) {
      if (filter && !filter(hub.user)) continue;
      reauthenticate(ws);
      count++;
    }
    // A relay the filter picks is cut off, its clients with it; the others'
    // clients it picks are revoked, and their relay closes their sockets as
    // this would have
    for (const [ws, link] of [...links]) {
      if (filter && !filter(link.relay)) {
        count += link.disconnect(filter);
        continue;
      }
      reauthenticate(ws);
      count++;
    }
    disconnected += count;
    return count;
  }

  /** End a socket's sessions now and close it for the client to authenticate afresh */
  function reauthenticate(ws) {
    hubs.get(ws)?.close();
    hubs.delete(ws);
    links.get(ws)?.close();
    links.delete(ws);
    seen.get(ws)?.cancel?.();
    seen.delete(ws);
    writeQueued(ws);   // what it was sent before goes before the close
    ws.close(...REAUTHENTICATE);
  }

  /**
   * Judge the open store sessions again, those `filter(user, storeId)`
   * picks (see the hub's revalidate); resolves to how many were closed
   */
  async function revalidate(filter) {
    if (closing) return 0;
    let count = 0;
    for (const closedOnes of await Promise.all([...hubs.values(), ...links.values()].map(hub => hub.revalidate(filter)))) count += closedOnes;
    revoked += count;
    return count;
  }

  /** The open sockets, how far behind each is (see the returns above) */
  function sockets() {
    const now = Date.now();
    const out = [];
    for (const [ws, hub] of hubs) {
      const times = seen.get(ws);
      if (!times) continue;   // a handshake completed only to be refused
      out.push({ user: hub.user, stores: hub.stores, buffered: ws.getBufferedAmount(), idleMs: now - times.heard, openMs: now - times.opened, expiresAt: times.expires });
    }
    for (const [ws, link] of links) {
      const times = seen.get(ws);
      if (times) out.push(...relaySockets(link, { buffered: ws.getBufferedAmount(), idleMs: now - times.heard, openMs: now - times.opened }, now));
    }
    return out;
  }

  return { upgrade, websocket, close, get closing() { return closing; }, sockets, socketStats: () => rollUpSockets(sockets(), { cutOff, disconnected, expired, revoked }), disconnect, revalidate };
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
  server.sockets = handlers.sockets;
  server.socketStats = handlers.socketStats;
  server.disconnect = handlers.disconnect;
  server.revalidate = handlers.revalidate;
  /** Graceful shutdown (see createHandlers' close), then stop the server */
  server.shutdown = async options => {
    await handlers.close(options);
    server.stop(true);
  };
  return server;
}
