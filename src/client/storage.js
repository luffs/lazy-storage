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
//   load()             -> null | { replicaId, seq, ops, state?, version?, epoch? }
//   save(outbox)       -> void   outbox = { replicaId, seq, ops }
//   saveState(cache)   -> void   cache = { state, version, epoch }
//   clear()            -> void   forget both, for a store that is gone for good
//   close()            -> void   optional: this client is done with it
//
// Nothing is deleted on its own: a store closed for us, a team left, a
// replica retired all leave their data where it is until the app says
// clear() (or destroy(), for IndexedDB). The same closed codes can come
// from a misconfigured server or a lapsed token, and pending edits are
// worth more than the kilobytes.
import { randomId } from '../core/ids.js';

/** How long a tab's claim on a localStorage key lasts without renewal, and how often it is renewed */
const LEASE = 5_000;
const RENEW = 1_500;

export function memoryOutbox() {
  let outbox = null;
  let cache = null;
  return {
    load: () => (outbox ? { ...outbox, ...(cache ?? {}) } : null),
    save: next => { outbox = next; },
    saveState: next => { cache = next; },
    clear() {
      outbox = null;
      cache = null;
    }
  };
}

/**
 * Outbox in `localStorage` under `key`, the state cache under `key:state`.
 * Reads and writes are guarded: with storage unavailable the client simply
 * does not survive a reload offline. A write that fails (the quota is full,
 * storage is disabled) is handed to `onError`, so the app can tell the user
 * that edits made offline are no longer kept across a reload.
 *
 * One tab at a time: the outbox holds a replica id and its sequence
 * numbers, and two tabs loading the same one would number their ops alike,
 * so the server would drop one tab's as duplicates of the other's. The tab
 * that loads the key first holds it, by a lease under `key:lease` it renews
 * while open and gives up on `pagehide` or `close()`; a tab that finds it
 * held starts afresh (a replica of its own) and keeps nothing, and hears an
 * error with code `storage-in-use`. Tabs that should share one replica use
 * `sharedConnection`
 * @param {string} [key]
 * @param {{ onError?: (error: any) => void }} [options]
 */
export function localStorageOutbox(key = 'lazy-storage', { onError = () => {} } = {}) {
  const stateKey = `${key}:state`;
  const leaseKey = `${key}:lease`;
  const tab = randomId();
  let yielded = false;   // another tab holds the key: this one reads and writes nothing
  let renewal = null;
  const onPageHide = () => release();

  /** Hold the key for this tab: false when another tab's lease is live */
  function claim() {
    const ls = globalThis.localStorage;
    if (!ls) return true;
    const held = read(leaseKey);
    if (held && held.tab !== tab && Date.now() - held.at < LEASE) return false;
    try {
      ls.setItem(leaseKey, JSON.stringify({ tab, at: Date.now() }));
    } catch { /* unavailable: nothing to guard */ }
    return true;
  }

  function yieldKey() {
    yielded = true;
    stopRenewing();
    const err = new Error(`Another tab is using the storage key "${key}": this tab keeps its edits in memory only. Give each tab a key of its own, or share one replica with sharedConnection`);
    err.code = 'storage-in-use';
    onError(err);
  }

  function stopRenewing() {
    clearInterval(renewal);
    renewal = null;
    globalThis.removeEventListener?.('pagehide', onPageHide);
  }

  function release() {
    stopRenewing();
    if (yielded) return;
    try {
      if (read(leaseKey)?.tab === tab) globalThis.localStorage?.removeItem(leaseKey);
    } catch { /* unavailable */ }
  }
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
    } catch (err) {
      // Storage full or unavailable: this part lives in memory only
      onError(err);
    }
  };
  return {
    load() {
      if (yielded) return null;
      if (!claim()) {
        yieldKey();
        return null;
      }
      if (!renewal) {
        renewal = setInterval(() => { if (!claim()) yieldKey(); }, RENEW);
        if (typeof renewal?.unref === 'function') renewal.unref();
        globalThis.addEventListener?.('pagehide', onPageHide);
      }
      const outbox = read(key);
      if (!outbox) return null;
      const cache = read(stateKey);
      return cache && typeof cache === 'object' ? { ...outbox, ...cache } : outbox;
    },
    save: outbox => { if (!yielded) write(key, outbox); },
    saveState: cache => { if (!yielded) write(stateKey, cache); },
    /** This client is done with the key: another tab may have it */
    close: release,
    /** Hold the key whatever lease is there: for a tab that knows it is the only one (a sharedConnection leader holds a lock) */
    takeOver() {
      yielded = false;
      try {
        globalThis.localStorage?.setItem(leaseKey, JSON.stringify({ tab, at: Date.now() }));
      } catch { /* unavailable: nothing to guard */ }
    },
    /** Remove both keys: for a store that is gone for good */
    clear() {
      try {
        globalThis.localStorage?.removeItem(key);
        globalThis.localStorage?.removeItem(stateKey);
      } catch {
        /* unavailable: nothing was stored */
      }
    }
  };
}
