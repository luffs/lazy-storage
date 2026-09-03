// storage.js - Client outbox and state-cache persistence
//
// The outbox is what survives a reload while offline: the replica id (so
// sequence numbers stay continuous and the server can dedupe), the last
// sequence number, and the unacknowledged ops. It is small and written
// synchronously with every local op. The state cache is the whole state
// plus the server version it reflects; the client writes it debounced,
// since it costs a serialization of everything. An adapter keeps the two
// apart so an op never pays for the state.
//
//   load()             -> null | { replicaId, seq, ops, state?, version? }
//   save(outbox)       -> void   outbox = { replicaId, seq, ops }
//   saveState(cache)   -> void   cache = { state, version } (optional; without
//                                it the client puts the state in `save`)

export function memoryOutbox() {
  let outbox = null;
  let cache = null;
  return {
    load: () => (outbox ? { ...outbox, ...(cache ?? {}) } : null),
    save: next => { outbox = next; },
    saveState: next => { cache = next; }
  };
}

/**
 * Outbox in `localStorage` under `key`, the state cache under `key:state`.
 * Reads and writes are guarded: with storage unavailable the client simply
 * does not survive a reload offline.
 */
export function localStorageOutbox(key = 'lazy-storage') {
  const stateKey = `${key}:state`;
  const read = k => {
    try {
      const raw = globalThis.localStorage?.getItem(k);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };
  const write = (k, data) => {
    try {
      globalThis.localStorage?.setItem(k, JSON.stringify(data));
    } catch {
      /* storage full or unavailable: this part lives in memory only */
    }
  };
  return {
    load() {
      const outbox = read(key);
      if (!outbox) return null;
      const cache = read(stateKey);
      // A document from before the split carried the state itself
      return cache && typeof cache === 'object' ? { ...outbox, ...cache } : outbox;
    },
    save: outbox => write(key, outbox),
    saveState: cache => write(stateKey, cache)
  };
}
