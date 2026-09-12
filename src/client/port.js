// port.js - The connection of a page that follows a browser's replica over a MessagePort
//
// The other end of `sharedConnection.follow(port, stores)`: a plain connection on a
// MessagePort transport, plus what a tab's clients get from their shared connection and
// a plain connection cannot know: the browser's socket status (`upstream`, which a
// client reports as its own) and the replica's unsent ops per store (`pending`, counted
// into the client's), both told by the host over the port as it learns them.

import { createConnection } from './connection.js';
import { messagePortTransport } from './transport.js';

/**
 * @param {import('../../types/client.js').MessagePortLike} port
 * @param {{ reconnect?: { min: number, max: number } | false, keepalive?: number | false }} [options]
 */
export function portConnection(port, { reconnect, keepalive = false } = {}) {
  let upstream = 'connecting';
  const pendingByStore = new Map();
  const listeners = { status: new Set(), sync: new Set() };
  const notify = (event, payload) => {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error('Error in lazy-storage port connection listener:', err);
      }
    }
  };
  const transport = messagePortTransport(port, {
    onControl(message) {
      if (message.lazy === 'status' && typeof message.status === 'string') {
        if (upstream === message.status) return;
        upstream = message.status;
        notify('status', inner.status);
      } else if (message.lazy === 'pending' && typeof message.store === 'string') {
        const n = Number.isInteger(message.n) ? message.n : 0;
        if (pendingByStore.get(message.store) === n) return;
        pendingByStore.set(message.store, n);
        notify('sync');
      }
    }
  });
  const inner = createConnection({ transport, ...(reconnect === undefined ? {} : { reconnect }), keepalive });
  inner.on('status', status => notify('status', status));

  return {
    get status() { return inner.status; },
    get attached() { return inner.attached; },
    get closed() { return inner.closed; },
    /** The browser's socket ('offline' | 'connecting' | 'online'), as the host last said; a client reports it as its status */
    get upstream() { return upstream; },
    /** The replica's unsent ops for a store, as the host last said */
    pending(storeId) { return pendingByStore.get(storeId) ?? 0; },
    on(event, fn) {
      if (event === 'closed') return inner.on('closed', fn);
      if (!listeners[event]) throw new TypeError(`Unknown connection event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    connect: () => inner.connect(),
    close: () => inner.close(),
    attach: (storeId, handler) => inner.attach(storeId, handler)
  };
}
