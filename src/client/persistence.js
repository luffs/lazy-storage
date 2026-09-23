// persistence.js - What a client writes to its storage adapter, and how
//
// Two kinds of adapter, told apart by whether `commit` exists:
//
// A DOCUMENT adapter ({ load, save, saveState }) keeps the outbox as one
// document, written whenever it changes, and the state as another. One
// that also takes the outbox op by op (saveOp, removeOp, dropOps, as
// localStorageOutbox does) is handed each change instead, so an op costs
// its own write rather than a rewrite of every op pending. The state costs
// a serialization of everything, so it is written once changes have
// settled for `cacheDelay` (and at least every ten of those under traffic
// that never settles), when the page is hidden or goes away, and on
// dispose. It may lag: it is saved with the version it reflects, a restore
// replays the outbox over it, and the reconnect asks for what came since.
//
// A ROW adapter ({ load, commit, replace, saveOp, removeOp, dropOps })
// keeps one row per leaf and one per pending op, so a batch costs the
// leaves it touched: every diff the state sees is walked into leaves
// (registers as whole values) and handed over as puts and deletes, where
// a delete removes the path and everything under it. When a snapshot
// lands the rows are replaced from the state, which also heals any row a
// failed write left behind. An op is written with `saveOp` (again when a
// newer op pruned it), removed one at a time with `removeOp` when a newer
// op emptied it, and dropped up to an acknowledged seq with `dropOps`.
// Every write carries the client's meta ({ replicaId, seq, version,
// epoch }) so the adapter stores it in the same transaction.
// A row adapter's load() may return a promise (IndexedDB does), which is
// what openClient() is for.
import { LazyWatch } from 'lazy-watch';
import { leaves, expandRegisters } from '../core/model.js';
import { pathKey } from '../core/paths.js';

export const isRowAdapter = storage => typeof storage?.commit === 'function';

/** A diff as row puts and deletes; register fragments are read whole from the live state */
function toRows(diff, regs, state) {
  const puts = [];
  const deletes = [];
  for (const [path, value] of leaves(expandRegisters(diff, regs, state), regs)) {
    if (value === null) deletes.push(pathKey(path));
    else puts.push([pathKey(path), value]);
  }
  return { puts, deletes };
}

/** Every leaf of a plain state as a row */
export const stateRows = (state, regs) => leaves(state, regs).map(([path, value]) => [pathKey(path), value]);

/**
 * @param {Object} deps
 * @param {Object} deps.storage - the adapter
 * @param {boolean} deps.cache - whether the state is persisted at all
 * @param {Object} deps.regs - the register matcher
 * @param {() => Object} deps.state - the live state
 * @param {() => {replicaId: string, seq: number, version: number, epoch: string|null}} deps.meta
 * @param {() => Object[]} deps.ops - the outbox
 * @param {(error: any) => void} deps.onError - a batch that could not be persisted
 * @param {number} [deps.cacheDelay=1000] - a document adapter's state is
 *   written once changes have settled for this long (ms)
 */
export function createPersistence({ storage, cache, regs, state, meta, ops, onError, cacheDelay = 1000 }) {
  if (isRowAdapter(storage)) {
    // The adapter lost what it held (IndexedDB deleted by another tab):
    // everything is written again, the state's rows and every pending op
    const stopReset = typeof storage.onReset === 'function' ? storage.onReset(() => {
      const m = meta();
      try {
        if (cache) storage.replace({ rows: stateRows(LazyWatch.snapshot(state()), regs), meta: m });
        for (const op of ops()) storage.saveOp(op, m);
      } catch (err) {
        onError(err);
      }
    }) : null;
    return {
      rows: true,
      /** A new op, after what it superseded in older ones: those rewritten, and the seqs of those emptied */
      op(op, superseded) {
        const m = meta();
        for (const older of superseded?.changed ?? []) storage.saveOp(older, m);
        for (const seq of superseded?.removed ?? []) storage.removeOp(seq, m);
        storage.saveOp(op, m);
      },
      drop: seq => storage.dropOps(seq, meta()),
      /** A batch the state just applied; `batchMeta.snapshot` marks a whole new state */
      batch(diff, batchMeta) {
        if (!cache) return;
        try {
          if (batchMeta?.snapshot) {
            storage.replace({ rows: stateRows(LazyWatch.snapshot(state()), regs), meta: meta() });
          } else {
            const { puts, deletes } = toRows(diff, regs, state());
            if (puts.length || deletes.length) storage.commit({ puts, deletes, meta: meta() });
          }
        } catch (err) {
          // A state the model cannot express in rows (registers that
          // differ from the server's, reported separately); the next
          // snapshot replaces the rows wholesale
          onError(err);
        }
      },
      /** The version moved without the state changing: the next drop or op carries it */
      version() {},
      flush() {},
      dispose() {
        if (typeof stopReset === 'function') stopReset();
      }
    };
  }

  const incremental = typeof storage.saveOp === 'function';
  // The first outbox write of a life goes whole, so an adapter still
  // holding another replica's ops (a client started with an id of its
  // own) is brought in line; op by op after that
  let whole = true;
  let timer = null;
  let dirtySince = null;   // when the state changed first since it was last written
  function writeOutbox() {
    const { replicaId, seq } = meta();
    storage.save({ replicaId, seq, ops: ops() });
  }
  function writeState() {
    clearTimeout(timer);
    timer = null;
    dirtySince = null;
    if (!cache) return;
    const { version, epoch } = meta();
    storage.saveState({ state: LazyWatch.snapshot(state()), version, epoch });
  }
  /** Write the state once changes settle, and no later than ten delays after the first */
  function stateSoon() {
    if (!cache) return;
    const now = Date.now();
    if (dirtySince === null) dirtySince = now;
    clearTimeout(timer);
    timer = setTimeout(writeState, Math.max(0, Math.min(cacheDelay, dirtySince + 10 * cacheDelay - now)));
    if (typeof timer?.unref === 'function') timer.unref();
  }
  const flush = () => { if (timer) writeState(); };
  // A page being hidden may be the last chance it gets: what is pending goes now
  const onHide = event => {
    if (event?.type === 'pagehide' || globalThis.document?.visibilityState === 'hidden') flush();
  };
  if (cache) {
    globalThis.addEventListener?.('pagehide', onHide);
    globalThis.document?.addEventListener?.('visibilitychange', onHide);
  }
  return {
    rows: false,
    op(op, superseded) {
      if (!incremental || whole) {
        whole = false;
        return writeOutbox();
      }
      const m = meta();
      for (const seq of superseded?.removed ?? []) storage.removeOp(seq, m);
      for (const older of superseded?.changed ?? []) storage.saveOp(older, m);
      storage.saveOp(op, m);
    },
    drop(seq) {
      if (!incremental || whole) {
        whole = false;
        return writeOutbox();
      }
      storage.dropOps(seq, meta());
    },
    batch: () => stateSoon(),
    version: () => stateSoon(),
    flush,
    dispose() {
      globalThis.removeEventListener?.('pagehide', onHide);
      globalThis.document?.removeEventListener?.('visibilitychange', onHide);
    }
  };
}
