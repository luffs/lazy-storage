// connection.js - One socket, shared by any number of clients
//
// A connection owns the transport lifecycle: opening, reconnecting with
// backoff, closing. Clients attach to it under a store id; on a
// multiplexed connection every outgoing message is tagged with that id
// and every incoming message is routed by it, so several stores travel
// over one socket (the server side of this is the hub). A client created
// with its own `transport` gets a private, untagged connection, which is
// what a single-store server expects.
//
//   const connection = createConnection({ transport: webSocketTransport('wss://host/ws') });
//   const teamA = createClient({ connection, store: 'team-a', initial });
//   const teamB = createClient({ connection, store: 'team-b', initial });
//   teamA.connect(); teamB.connect();   // one socket, two stores
import { LazyWatch } from 'lazy-watch';

const { Utils } = LazyWatch;

/**
 * @param {Object} options
 * @param {() => Object} options.transport - transport factory (see transport.js)
 * @param {{min: number, max: number}|false} [options.reconnect] - retry
 *   backoff after an unexpected close; false disables automatic reconnects
 * @param {boolean} [options.multiplex=true] - tag and route messages by
 *   store id (false for a private connection to a single-store endpoint)
 * @param {number|false} [options.keepalive=30000] - ping interval in ms while
 *   open, so idle sockets survive proxies and server idle timeouts; false
 *   disables it
 */
export function createConnection({ transport, reconnect = { min: 500, max: 10_000 }, multiplex = true, keepalive = 30_000 } = {}) {
  if (typeof transport !== 'function') throw new TypeError('createConnection requires a transport factory');
  const handlers = new Map();
  const statusListeners = new Set();
  let conn = null;
  let status = 'offline';
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

  function setStatus(next) {
    if (status === next) return;
    status = next;
    for (const fn of statusListeners) {
      try {
        fn(status);
      } catch (err) {
        console.error('Error in lazy-storage connection listener:', err);
      }
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
      const entry = handlers.get(multiplex ? msg.store : '');
      if (!entry) return;
      if (multiplex) {
        const { store, ...inner } = msg;
        entry.handler.onMessage(inner);
      } else {
        entry.handler.onMessage(msg);
      }
    };
    c.onclose = () => {
      if (conn !== c) return;
      conn = null;
      stopKeepalive();
      setStatus('offline');
      for (const { handler } of [...handlers.values()]) handler.onClose();
      if (!closedByUser && reconnect) scheduleRetry();
    };
  }

  function scheduleRetry() {
    clearTimeout(retryTimer);
    retryTimer = setTimeout(open, retryDelay);
    retryDelay = Math.min(retryDelay * 2 || reconnect.min, reconnect.max);
  }

  return {
    get status() { return status; },
    get multiplex() { return multiplex; },
    /** Number of attached clients */
    get attached() { return handlers.size; },

    on(event, fn) {
      if (event !== 'status') throw new TypeError(`Unknown connection event "${event}"`);
      statusListeners.add(fn);
      return () => statusListeners.delete(fn);
    },

    /** Open the socket (idempotent); attached clients say hello when it opens */
    connect() {
      closedByUser = false;
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
      const key = multiplex ? storeId : '';
      if (multiplex && (typeof storeId !== 'string' || !storeId)) throw new TypeError('A multiplexed connection needs a store id per client');
      if (handlers.has(key)) throw new Error(multiplex ? `A client is already attached to store "${storeId}" on this connection` : 'A private connection carries one client');
      const link = {
        send(message) {
          if (conn && status === 'open') conn.send(multiplex ? { ...message, store: storeId } : message);
        },
        detach() {
          if (handlers.get(key)?.handler !== handler) return;
          handlers.delete(key);
          if (multiplex && conn && status === 'open') conn.send({ t: 'leave', store: storeId });
        }
      };
      handlers.set(key, { handler, link });
      if (status === 'open') handler.onOpen(link);
      return link;
    }
  };
}
