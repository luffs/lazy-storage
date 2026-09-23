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
// An adapter may also take the outbox op by op, as a row adapter does,
// and is then handed each change instead of the whole outbox (a long
// offline spell otherwise rewrites every pending op with every new one):
//
//   saveOp(op, meta)     an op new, or rewritten after a newer one pruned it
//   removeOp(seq, meta)  an op a newer one emptied
//   dropOps(seq, meta)   the ops up to an acknowledged seq
//
// where meta is { replicaId, seq, version, epoch }. `save` is still
// called, for a whole outbox at once.
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
  // The outbox is kept op by op, so an op costs a write of that op and
  // not of every op pending: `key` holds { replicaId, seq, first } (first:
  // the oldest pending seq), and `key:op:<seq>` each op. What a client of
  // an earlier version wrote, the outbox as one document, is taken over on
  // load
  const opKey = seq => `${key}:op:${seq}`;
  const remove = k => {
    try {
      globalThis.localStorage?.removeItem(k);
    } catch { /* unavailable: nothing was stored */ }
  };
  const pending = new Set();   // seqs with an op key
  let first = null;            // the smallest of them, null when none
  const lowest = () => (pending.size ? Math.min(...pending) : null);
  const writeMeta = meta => write(key, { replicaId: meta.replicaId, seq: meta.seq, first });
  function putOp(op) {
    write(opKey(op.seq), op);
    pending.add(op.seq);
    if (first === null || op.seq < first) first = op.seq;
  }
  function takeOp(seq, settle = true) {
    remove(opKey(seq));
    if (pending.delete(seq) && seq === first && settle) first = lowest();
  }

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
      const doc = read(key);
      if (!doc) return null;
      pending.clear();
      first = null;
      let ops = [];
      if (Array.isArray(doc.ops)) {
        // One document, as an earlier version wrote it: taken apart once
        for (const op of doc.ops) putOp(op);
        ops = doc.ops;
        writeMeta(doc);
      } else if (Number.isInteger(doc.first) && Number.isInteger(doc.seq)) {
        for (let s = doc.first; s <= doc.seq; s++) {
          const op = read(opKey(s));
          if (op) {
            ops.push(op);
            pending.add(s);
          }
        }
        first = lowest();
      }
      const outbox = { replicaId: doc.replicaId, seq: doc.seq, ops };
      const cache = read(stateKey);
      return cache && typeof cache === 'object' ? { ...outbox, ...cache } : outbox;
    },
    /** The whole outbox at once (what an adapter without the calls below is handed) */
    save(outbox) {
      if (yielded) return;
      const keep = new Set(outbox.ops.map(op => op.seq));
      for (const seq of [...pending]) if (!keep.has(seq)) takeOp(seq);
      for (const op of outbox.ops) putOp(op);
      writeMeta(outbox);
    },
    /** An op new or rewritten */
    saveOp(op, meta) {
      if (yielded) return;
      putOp(op);
      writeMeta(meta);
    },
    /** An op a newer one emptied */
    removeOp(seq, meta) {
      if (yielded) return;
      takeOp(seq);
      writeMeta(meta);
    },
    /** The ops up to an acknowledged seq */
    dropOps(seq, meta) {
      if (yielded) return;
      for (const s of [...pending]) if (s <= seq) takeOp(s, false);
      first = lowest();
      writeMeta(meta);
    },
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
    /** Remove the outbox, its ops, and the state: for a store that is gone for good */
    clear() {
      const doc = read(key);
      if (doc && Number.isInteger(doc.first) && Number.isInteger(doc.seq)) for (let s = doc.first; s <= doc.seq; s++) remove(opKey(s));
      for (const seq of [...pending]) remove(opKey(seq));
      pending.clear();
      first = null;
      remove(key);
      remove(stateKey);
    }
  };
}
