// persistence.js - What a client writes to its storage adapter, and how
//
// Two kinds of adapter, told apart by whether `commit` exists:
//
// A DOCUMENT adapter ({ load, save, saveState }) keeps the outbox as one
// document, written whenever it changes (it is small), and the state as
// another, written debounced because it costs a serialization of
// everything.
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
 */
export function createPersistence({ storage, cache, regs, state, meta, ops, onError }) {
  if (isRowAdapter(storage)) {
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
      flush() {}
    };
  }

  let timer = null;
  function writeOutbox() {
    const { replicaId, seq } = meta();
    storage.save({ replicaId, seq, ops: ops() });
  }
  function writeState() {
    clearTimeout(timer);
    timer = null;
    if (!cache) return;
    const { version, epoch } = meta();
    storage.saveState({ state: LazyWatch.snapshot(state()), version, epoch });
  }
  function stateSoon() {
    if (!cache || timer) return;
    timer = setTimeout(writeState, 50);
    if (typeof timer?.unref === 'function') timer.unref();
  }
  return {
    rows: false,
    op: () => writeOutbox(),
    drop: () => writeOutbox(),
    batch: () => stateSoon(),
    version: () => stateSoon(),
    flush() {
      if (timer) writeState();
    }
  };
}
