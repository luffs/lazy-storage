// persistence.js - What a client writes to its storage adapter, and how
//
// A client keeps two things: its OUTBOX (the ops the server has not
// acknowledged) and its BASE (the state as the server last told it, with
// the version and epoch it is at). The view the app sees is the base with
// the outbox replayed on top, so nothing else needs keeping: a reload
// builds the view the same way.
//
// Two kinds of adapter, told apart by whether `commit` exists:
//
// A DOCUMENT adapter ({ load, save, saveState }) keeps the outbox as one
// document, written whenever it changes (op by op when it has saveOp,
// removeOp and dropOps, as localStorageOutbox does), and the base as
// another, `saveState({ state, clocks, version, epoch })`. The base costs a
// serialization of everything, so it is written once changes have settled
// for `cacheDelay` (at least every ten of those under traffic that never
// settles), when the page is hidden, and on dispose. It may lag: it is
// saved with the version it reflects, and the reconnect asks for what came
// since.
//
// A ROW adapter ({ load, commit, replace, saveOp, removeOp, dropOps }) keeps
// one row per record of the base and one per pending op, so a patch costs
// the records it touched: `commit({ puts: [[c, id, data, clock]], deletes:
// [[c, id]], meta })`, and `replace({ records, meta })` when a snapshot lands.
// Every write carries the client's meta ({ replicaId, seq, version, epoch })
// so the adapter stores it in the same transaction. Its load() may return a
// promise (IndexedDB does), which is what openClient() is for.

export const isRowAdapter = storage => typeof storage?.commit === 'function';

/**
 * @param {Object} deps
 * @param {Object} deps.storage - the adapter
 * @param {boolean} deps.cache - whether the base is persisted at all
 * @param {Object} deps.shape
 * @param {() => Object} deps.base - the base (a doc: state and clocks)
 * @param {() => {replicaId: string, seq: number, version: number, epoch: string|null}} deps.meta
 * @param {() => Object[]} deps.ops - the outbox
 * @param {(error: any) => void} deps.onError
 * @param {number} [deps.cacheDelay=1000]
 */
export function createPersistence({ storage, cache, shape, base, meta, ops, onError, cacheDelay = 1000 }) {
  const guard = fn => (...args) => {
    try {
      return fn(...args);
    } catch (err) {
      onError(err);
    }
  };
  const allRecords = () => [...base().records()].map(([c, id, data, clock]) => [c, id ?? null, structuredClone(data), clock]);

  if (isRowAdapter(storage)) {
    const rewrite = () => {
      const m = meta();
      if (cache) storage.replace({ records: allRecords(), meta: m });
      for (const op of ops()) storage.saveOp(op, m);
    };
    const stopReset = typeof storage.onReset === 'function' ? storage.onReset(guard(rewrite)) : null;
    return {
      rows: true,
      op: guard((op, superseded) => {
        const m = meta();
        for (const older of superseded?.changed ?? []) storage.saveOp(older, m);
        for (const seq of superseded?.removed ?? []) storage.removeOp(seq, m);
        storage.saveOp(op, m);
      }),
      drop: guard(seq => storage.dropOps(seq, meta())),
      remove: guard(seq => storage.removeOp(seq, meta())),
      /** The base changed for these records ([c, id] pairs) */
      base: guard(touched => {
        if (!cache || !touched.length) return;
        const puts = [];
        const deletes = [];
        for (const [c, id] of touched) {
          const record = base().get(c, id);
          if (record === undefined) deletes.push([c, id ?? null]);
          else puts.push([c, id ?? null, structuredClone(record), base().clock(c, id)]);
        }
        storage.commit({ puts, deletes, meta: meta() });
      }),
      snapshot: guard(() => { if (cache) storage.replace({ records: allRecords(), meta: meta() }); }),
      version: guard(() => { if (cache) storage.commit({ puts: [], deletes: [], meta: meta() }); }),
      flush() {},
      dispose() {
        if (typeof stopReset === 'function') stopReset();
      }
    };
  }

  const incremental = typeof storage.saveOp === 'function';
  let whole = true;
  let timer = null;
  let dirtySince = null;
  const writeOutbox = () => {
    const { replicaId, seq } = meta();
    storage.save({ replicaId, seq, ops: ops() });
  };
  const writeBase = guard(() => {
    clearTimeout(timer);
    timer = null;
    dirtySince = null;
    if (!cache) return;
    const { version, epoch } = meta();
    const doc = base();
    storage.saveState({ state: structuredClone(doc.state), clocks: [...doc.records()].map(([c, id, , clock]) => [c, id ?? null, clock]), version, epoch });
  });
  function soon() {
    if (!cache) return;
    const now = Date.now();
    if (dirtySince === null) dirtySince = now;
    clearTimeout(timer);
    timer = setTimeout(writeBase, Math.max(0, Math.min(cacheDelay, dirtySince + 10 * cacheDelay - now)));
    if (typeof timer?.unref === 'function') timer.unref();
  }
  const flush = () => { if (timer) writeBase(); };
  const onHide = event => {
    if (event?.type === 'pagehide' || globalThis.document?.visibilityState === 'hidden') flush();
  };
  if (cache) {
    globalThis.addEventListener?.('pagehide', onHide);
    globalThis.document?.addEventListener?.('visibilitychange', onHide);
  }
  return {
    rows: false,
    op: guard((op, superseded) => {
      if (!incremental || whole) {
        whole = false;
        return writeOutbox();
      }
      const m = meta();
      for (const seq of superseded?.removed ?? []) storage.removeOp(seq, m);
      for (const older of superseded?.changed ?? []) storage.saveOp(older, m);
      storage.saveOp(op, m);
    }),
    drop: guard(seq => {
      if (!incremental || whole) {
        whole = false;
        return writeOutbox();
      }
      storage.dropOps(seq, meta());
    }),
    remove: guard(seq => {
      if (!incremental || whole) {
        whole = false;
        return writeOutbox();
      }
      storage.removeOp(seq, meta());
    }),
    base: () => soon(),
    snapshot: () => soon(),
    version: () => soon(),
    flush,
    dispose() {
      globalThis.removeEventListener?.('pagehide', onHide);
      globalThis.document?.removeEventListener?.('visibilitychange', onHide);
    }
  };
}
