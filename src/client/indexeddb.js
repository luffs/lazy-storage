// indexeddb.js - Row persistence in IndexedDB (browsers)
//
// One database per adapter, three object stores: `leaves` (key: the path
// key, value: the leaf), `ops` (key: seq, value: the op), and `meta` (one
// record). A batch is one readwrite transaction of exactly the rows it
// touched, so an edit costs its leaves rather than the state; a deletion
// removes the path and, through a key range over the descendant prefix,
// everything under it (descendant keys share the ancestor's key minus
// its closing bracket, plus a comma — see core/paths.js). A snapshot
// clears the leaves and writes them afresh.
//
// load() returns a promise, so a client on this adapter is opened with
// openClient(). Writes are fire-and-forget: IndexedDB runs overlapping
// readwrite transactions in the order they were created, so they apply
// in the order made. A write that fails (quota, a database closed
// underneath us) is handed to `onError` and otherwise ignored, like the
// localStorage adapter: the client keeps working from memory, and the
// next snapshot replaces the rows wholesale.
//
//   const db = await openClient({ store: 'team-1', storage: indexedDBStorage('app:team-1'), ... });
//
// Use one name per store (and per replica: a tab is a replica), and
// `destroy()` to drop a database a closed tab left behind.

const STORES = ['leaves', 'ops', 'meta'];
const META_KEY = 'meta';

/** The key range of every strict descendant of a path key */
const descendants = (key, IDBKeyRange) => {
  const prefix = key.slice(0, -1) + ',';
  return IDBKeyRange.bound(prefix, prefix + '￿');
};

/**
 * @param {string} [name='lazy-storage'] - the database name
 * @param {Object} [options]
 * @param {IDBFactory} [options.indexedDB] - defaults to the global
 * @param {typeof IDBKeyRange} [options.IDBKeyRange] - defaults to the global
 * @param {(error: any) => void} [options.onError] - a failed write
 */
export function indexedDBStorage(name = 'lazy-storage', {
  indexedDB: idb = globalThis.indexedDB,
  IDBKeyRange: KeyRange = globalThis.IDBKeyRange,
  onError = () => {}
} = {}) {
  if (!idb) throw new TypeError('indexedDBStorage: no IndexedDB implementation available');
  let opening = null;
  let db = null;
  const inFlight = new Set();

  const open = () => {
    opening ??= new Promise((resolve, reject) => {
      const request = idb.open(name, 1);
      request.onupgradeneeded = () => {
        const d = request.result;
        for (const store of STORES) if (!d.objectStoreNames.contains(store)) d.createObjectStore(store);
      };
      request.onsuccess = () => {
        db = request.result;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      request.onerror = () => reject(request.error);
    });
    return opening;
  };

  const result = request => new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });

  /** One readwrite transaction over `stores`; `fn` issues its requests synchronously */
  function write(stores, fn) {
    const done = open().then(d => new Promise(resolve => {
      const tx = d.transaction(stores, 'readwrite');
      tx.oncomplete = resolve;
      tx.onerror = () => { onError(tx.error); resolve(); };
      tx.onabort = () => { onError(tx.error); resolve(); };
      fn(Object.fromEntries(stores.map(s => [s, tx.objectStore(s)])));
    })).catch(err => onError(err));
    inFlight.add(done);
    done.finally(() => inFlight.delete(done));
  }

  return {
    async load() {
      const d = await open();
      const tx = d.transaction(STORES, 'readonly');
      const [meta, keys, values, ops] = await Promise.all([
        result(tx.objectStore('meta').get(META_KEY)),
        result(tx.objectStore('leaves').getAllKeys()),
        result(tx.objectStore('leaves').getAll()),
        result(tx.objectStore('ops').getAll())
      ]);
      if (!meta) return null;
      return { ...meta, ops, rows: keys.map((key, i) => [key, values[i]]) };
    },
    commit({ puts, deletes, meta }) {
      write(['leaves', 'meta'], stores => {
        for (const key of deletes) {
          stores.leaves.delete(key);
          stores.leaves.delete(descendants(key, KeyRange));
        }
        for (const [key, value] of puts) stores.leaves.put(value, key);
        stores.meta.put(meta, META_KEY);
      });
    },
    replace({ rows, meta }) {
      write(['leaves', 'meta'], stores => {
        stores.leaves.clear();
        for (const [key, value] of rows) stores.leaves.put(value, key);
        stores.meta.put(meta, META_KEY);
      });
    },
    saveOp(op, meta) {
      write(['ops', 'meta'], stores => {
        stores.ops.put(op, op.seq);
        stores.meta.put(meta, META_KEY);
      });
    },
    dropOps(seq, meta) {
      write(['ops', 'meta'], stores => {
        stores.ops.delete(KeyRange.upperBound(seq));
        stores.meta.put(meta, META_KEY);
      });
    },
    /** Resolves once every write issued so far has landed (or failed) */
    settled: () => Promise.all([...inFlight]).then(() => {}),
    close() {
      db?.close();
      db = null;
      opening = null;
    },
    /** Close and delete the database */
    async destroy() {
      await this.settled();
      this.close();
      await new Promise((resolve, reject) => {
        const request = idb.deleteDatabase(name);
        request.onsuccess = resolve;
        request.onblocked = resolve;
        request.onerror = () => reject(request.error);
      });
    }
  };
}
