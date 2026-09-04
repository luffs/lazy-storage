// helpers.js - An in-memory network between clients and a store
//
// Deliveries are queued and drained by `settle()`, which also yields to
// the event loop between hops so lazy-watch's microtask batches emit. A
// link can be taken offline, which closes its current connection and
// refuses new ones until it is back online.
import { createClient } from '../src/client/index.js';
import { createHub } from '../src/server/hub.js';

// A macrotask boundary: lazy-watch's microtask batches have emitted by then,
// and setImmediate has no timer granularity floor (setTimeout(0) costs ~15 ms
// on Windows, which made the fuzzer crawl)
const tick = () => new Promise(resolve => setImmediate(resolve));

/**
 * @param {Object} target - a store (served through a hub that maps every
 *   id to it) or an endpoint `{ session({ send, user }) }` such as a hub factory
 */
export function createNetwork(target) {
  const store = target.state !== undefined
    ? { session: ({ send, user }) => createHub(() => target, { send, user }) }
    : target;
  const queue = [];

  const net = {
    get pending() { return queue.length; },

    /** Deliver everything, repeatedly, until the network and microtasks are quiet */
    async settle() {
      for (let round = 0; round < 10_000; round++) {
        await tick();
        if (queue.length === 0) {
          await tick();
          if (queue.length === 0) return;
        }
        while (queue.length) queue.shift()();
      }
      throw new Error('network did not settle');
    },

    /**
     * A link for one client: a transport factory plus offline/online control.
     * `user` is attached to the sessions this link opens (as an authenticated
     * transport would); eviction closes the link's connection.
     */
    link({ user } = {}) {
      const link = { online: true, current: null };
      link.factory = () => {
        let session = null;
        const t = { onopen: null, onmessage: null, onclose: null, open: false };
        t.send = message => {
          if (!t.open) return;
          const copy = structuredClone(message);
          queue.push(() => { if (t.open) session.receive(copy); });
        };
        t.close = () => {
          if (!t.open) return;
          t.open = false;
          session.close();
          if (link.current === t) link.current = null;
          queue.push(() => t.onclose?.());
        };
        queue.push(() => {
          if (!link.online) { t.onclose?.(); return; }
          // Open before the session exists: a real socket can send from the
          // moment the server's open handler runs
          t.open = true;
          session = store.session({
            // Gate at send time, not delivery: like a real socket, messages
            // written before a close are still delivered, in order, ahead of
            // the close event (an eviction's `closed` message relies on it)
            send: message => {
              if (!t.open) return;
              const copy = structuredClone(message);
              queue.push(() => t.onmessage?.(copy));
            },
            user,
            onEvict: () => t.close()
          });
          link.current = t;
          t.onopen?.();
        });
        return t;
      };
      link.goOffline = () => { link.online = false; link.current?.close(); };
      link.goOnline = () => { link.online = true; };
      return link;
    },

    /** A connected client on its own link (store 'main' unless given); `reconnect` is off so tests drive it */
    client(options = {}, linkOptions = {}) {
      const link = net.link(linkOptions);
      const client = createClient({ transport: link.factory, reconnect: false, store: 'main', ...options });
      client.link = link;
      client.connect();
      return client;
    }
  };
  return net;
}

/** A controllable wall clock for hybrid logical clocks */
export function fakeTime(start = 1_000_000) {
  let t = start;
  const now = () => t;
  now.advance = ms => { t += ms; return t; };
  now.set = ms => { t = ms; };
  return now;
}

export function seededRandom(seed) {
  let s = seed >>> 0 || 1;
  const rng = {
    next() { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; },
    int: n => Math.floor(rng.next() * n),
    chance: p => rng.next() < p,
    pick: arr => arr[rng.int(arr.length)]
  };
  return rng;
}
