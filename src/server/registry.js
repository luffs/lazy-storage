// registry.js - Many stores per server
//
// A registry creates stores on first use through a factory and keeps them
// live; the transport adapter resolves a store by id from the URL. Store
// ids are restricted to a URL- and filename-safe alphabet so an id can
// name a file, a table key, or a path segment without escaping.

const STORE_ID = /^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$/;

export const isStoreId = id => typeof id === 'string' && STORE_ID.test(id);

/**
 * @param {(id: string) => Object|null} factory - builds the store for an id
 *   (typically createStore with a per-id storage adapter); return null to
 *   refuse an id
 */
export function createStores(factory) {
  if (typeof factory !== 'function') throw new TypeError('createStores requires a factory function');
  const live = new Map();

  const stores = {
    /** The live store for `id`, created on first use; null for an invalid or refused id */
    get(id) {
      if (!isStoreId(id)) return null;
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
      store.dispose();
      return true;
    },
    dispose() {
      for (const id of [...live.keys()]) stores.release(id);
    }
  };
  return stores;
}
