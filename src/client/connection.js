// connection.js - One socket, shared by any number of clients
//
// A connection owns the transport lifecycle: opening, reconnecting with
// backoff, closing, keepalive pings. Clients attach to it under a store
// id; every outgoing message is tagged with that id and every incoming
// message is routed by it, so any number of stores travel over one socket.
// The server side of this is the hub. A single-store app is one connection
// with one client attached.
//
//   const connection = createConnection({ transport: webSocketTransport('wss://host/ws') });
//   const teamA = createClient({ connection, store: 'team-a', initial });
//   const teamB = createClient({ connection, store: 'team-b', initial });
//   teamA.connect(); teamB.connect();   // one socket, two stores
//
// A server that turns the socket away (the request did not authenticate)
// says so in a `closed` message without a store, and closes with code
// 4401. That ends the connection until connect() is called again: no
// retry, `closed` holds the reason, and every attached client reports it
// as its own `closed`. A browser cannot read the status of a refused
// handshake, which is why the server completes it just to say this.
import { LazyWatch } from 'lazy-watch';

const { Utils } = LazyWatch;

/** The close code the server uses when it turns a socket away (server/wire.js) */
const UNAUTHORIZED = 4401;

/**
 * @param {Object} options
 * @param {() => Object} options.transport - transport factory (see transport.js)
 * @param {{min: number, max: number}|false} [options.reconnect] - retry
 *   backoff after an unexpected close; false disables automatic reconnects
 * @param {number|false} [options.keepalive=30000] - ping interval in ms while
 *   open, so idle sockets survive proxies and server idle timeouts; false
 *   disables it
 */
export function createConnection({ transport, reconnect = { min: 500, max: 10_000 }, keepalive = 30_000 } = {}) {
  if (typeof transport !== 'function') throw new TypeError('createConnection requires a transport factory');
  const handlers = new Map(); // store id -> { handler, link }
  const listeners = { status: new Set(), closed: new Set() };
  let conn = null;
  let status = 'offline';
  let ended = null;       // { code, message } after the server turned the socket away, until connect()
  let closedByUser = true;
  let retryDelay = reconnect ? reconnect.min : 0;
  let retryTimer = null;
  let keepaliveTimer = null;

  function startKeepalive() {
    stopKeepalive();
    if (!keepalive) return;
    keepaliveTimer = setInterval(() => {
      if (conn && status === 'open') conn.send({ t: 'ping' });
    }, keepalive);
    // Never keep a Node process alive just for pings
    if (typeof keepaliveTimer?.unref === 'function') keepaliveTimer.unref();
  }

  function stopKeepalive() {
    clearInterval(keepaliveTimer);
    keepaliveTimer = null;
  }

  function notify(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error('Error in lazy-storage connection listener:', err);
      }
    }
  }

  function setStatus(next) {
    if (status === next) return;
    status = next;
    notify('status', status);
  }

  /**
   * The socket is gone: offline for everyone, then either a retry or,
   * when the server turned it away, the reason, which every client hears
   * once its status already says offline
   */
  function dropped() {
    conn = null;
    stopKeepalive();
    setStatus('offline');
    for (const { handler } of [...handlers.values()]) handler.onClose();
    if (ended) {
      for (const { handler } of [...handlers.values()]) handler.onClosed?.(ended);
      notify('closed', ended);
    } else if (!closedByUser && reconnect) {
      scheduleRetry();
    }
  }

  function open() {
    setStatus('connecting');
    const c = transport();
    conn = c;
    c.onopen = () => {
      if (conn !== c) return;
      retryDelay = reconnect ? reconnect.min : 0;
      setStatus('open');
      startKeepalive();
      for (const { handler, link } of [...handlers.values()]) handler.onOpen(link);
    };
    c.onmessage = msg => {
      if (conn !== c || !Utils.isPlainObject(msg) || msg.t === 'pong') return;
      if (msg.store === undefined) {
        // About the socket itself: the server turning it away. Its close
        // follows in a moment; the socket is dropped here already so the
        // status and the reason land together
        if (msg.t === 'closed' && !ended) {
          ended = { code: msg.code, message: msg.message };
          c.close();
          dropped();
        }
        return;
      }
      const entry = handlers.get(msg.store);
      if (!entry) return;
      const { store, ...inner } = msg;
      entry.handler.onMessage(inner);
    };
    c.onclose = info => {
      if (conn !== c) return;
      // The close code says it too, should the message not have made it
      if (info?.code === UNAUTHORIZED && !ended) ended = { code: 'unauthorized', message: info.reason || 'Unauthorized' };
      dropped();
    };
  }

  function scheduleRetry() {
    clearTimeout(retryTimer);
    retryTimer = setTimeout(open, retryDelay);
    retryDelay = Math.min(retryDelay * 2 || reconnect.min, reconnect.max);
  }

  return {
    get status() { return status; },
    /** Number of attached clients */
    get attached() { return handlers.size; },
    /** Why the server turned the socket away ({ code, message }), or null; cleared by connect() */
    get closed() { return ended; },

    /** 'status' (offline | connecting | open) and 'closed' (the server turned the socket away) */
    on(event, fn) {
      if (!listeners[event]) throw new TypeError(`Unknown connection event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },

    /**
     * Open the socket (idempotent); attached clients say hello when it
     * opens. After the server turned the socket away this is the way back
     * in: the transport factory runs afresh, so a URL built by a function
     * carries whatever credentials the app has now
     */
    connect() {
      closedByUser = false;
      ended = null;
      clearTimeout(retryTimer);
      if (!conn) open();
    },

    /** Close the socket for every attached client; no automatic reconnect */
    close() {
      closedByUser = true;
      clearTimeout(retryTimer);
      stopKeepalive();
      const c = conn;
      conn = null;
      c?.close();
      setStatus('offline');
      for (const { handler } of [...handlers.values()]) handler.onClose();
    },

    /**
     * Attach a client under a store id. The handler gets onOpen(link) (send
     * your hello through it), onMessage(message), onClose. Returns the same
     * link, to send through and detach with. On an already-open connection
     * onOpen fires synchronously, which is why it carries the link: the
     * caller has not received the return value yet.
     */
    attach(storeId, handler) {
      if (typeof storeId !== 'string' || !storeId) throw new TypeError('attach requires a store id');
      if (handlers.has(storeId)) throw new Error(`A client is already attached to store "${storeId}" on this connection`);
      const link = {
        send(message) {
          if (conn && status === 'open') conn.send({ ...message, store: storeId });
        },
        detach() {
          if (handlers.get(storeId)?.handler !== handler) return;
          handlers.delete(storeId);
          if (conn && status === 'open') conn.send({ t: 'leave', store: storeId });
        }
      };
      handlers.set(storeId, { handler, link });
      if (status === 'open') handler.onOpen(link);
      return link;
    }
  };
}
