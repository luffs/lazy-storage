// storage.js - Client outbox persistence
//
// The outbox is what survives a reload while offline: the replica id (so
// sequence numbers stay continuous and the server can dedupe), the last
// sequence number, and the unacknowledged ops.

export function memoryOutbox() {
  let data = null;
  return {
    load: () => data,
    save: next => { data = next; }
  };
}

/**
 * Outbox in `localStorage` under `key`. Reads and writes are guarded: with
 * storage unavailable the client simply does not survive a reload offline.
 */
export function localStorageOutbox(key = 'lazy-storage') {
  return {
    load() {
      try {
        const raw = globalThis.localStorage?.getItem(key);
        return raw ? JSON.parse(raw) : null;
      } catch {
        return null;
      }
    },
    save(data) {
      try {
        globalThis.localStorage?.setItem(key, JSON.stringify(data));
      } catch {
        /* storage full or unavailable: the outbox lives in memory only */
      }
    }
  };
}
