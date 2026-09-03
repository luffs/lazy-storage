// registry.js - Many stores per server
//
// A registry creates stores on first use through a factory and keeps them
// live; the transport adapter resolves a store by id from the URL. Store
// ids are restricted to a URL- and filename-safe alphabet so an id can
// name a file, a table key, or a path segment without escaping.
//
// With `idle` set, a store that has had no session for that long is
// released (disposed, its storage flushed) by a periodic sweep, so a
// server's memory follows the stores in use rather than every store ever
// opened; the next request for it loads it from storage again. Off by
// default: a store on memoryStorage would lose its data.

const STORE_ID = /^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$/;

export const isStoreId = id => typeof id === 'string' && STORE_ID.test(id);

/**
 * @param {(id: string) => Object|null} factory - builds the store for an id
 *   (typically createStore with a per-id storage adapter); return null to
 *   refuse an id
 * @param {Object} [options]
 * @param {number} [options.idle=Infinity] - release a store after this many
 *   ms without a session
 * @param {number} [options.sweepEvery=60000] - how often to look (ms)
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createStores(factory, { idle = Infinity, sweepEvery = 60_000, now = Date.now } = {}) {
  if (typeof factory !== 'function') throw new TypeError('createStores requires a factory function');
  const live = new Map();
  const idleSince = new Map();  // id -> when the store was last seen without sessions
  let timer = null;
  if (Number.isFinite(idle)) {
    timer = setInterval(() => stores.sweep(), Math.max(1, Math.min(sweepEvery, idle)));
    if (typeof timer?.unref === 'function') timer.unref();
  }

  const stores = {
    /** The live store for `id`, created on first use; null for an invalid or refused id */
    get(id) {
      if (!isStoreId(id)) return null;
      idleSince.delete(id);
      let store = live.get(id);
      if (store) return store;
      store = factory(id);
      if (!store) return null;
      live.set(id, store);
      return store;
    },
    has: id => live.has(id),
    /** Ids of the stores currently live in this process */
    ids: () => [...live.keys()],
    /** Dispose a live store (its sessions close; persisted data stays) */
    release(id) {
      const store = live.get(id);
      if (!store) return false;
      live.delete(id);
      idleSince.delete(id);
      store.dispose();
      return true;
    },
    /** Release every store idle for `idle` or longer; returns their ids. Runs on its own every `sweepEvery` */
    sweep() {
      const wall = now();
      const released = [];
      for (const [id, store] of [...live]) {
        if (store.sessions > 0) {
          idleSince.delete(id);
          continue;
        }
        const since = idleSince.get(id) ?? wall;
        idleSince.set(id, since);
        if (wall - since >= idle) {
          stores.release(id);
          released.push(id);
        }
      }
      return released;
    },
    dispose() {
      clearInterval(timer);
      timer = null;
      for (const id of [...live.keys()]) stores.release(id);
    }
  };
  return stores;
}
