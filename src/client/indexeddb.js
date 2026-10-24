// indexeddb.js - Row persistence in IndexedDB (browsers)
//
// One database per adapter, three object stores: `records` (key: the
// record's [collection, id] as JSON, value: [c, id, data, clock]), `ops`
// (key: seq, value: the op), and `meta` (one record). A patch is one
// readwrite transaction of exactly the records it touched, so an edit
// costs its records rather than the state. A snapshot clears the records
// and writes them afresh. A database from before 1.0 (version 1, a row per
// leaf, ops as diffs) is cleared on open: the client takes a snapshot.
//
// load() returns a promise, so a client on this adapter is opened with
// openClient(). Writes are fire-and-forget: IndexedDB runs overlapping
// readwrite transactions in the order they were created, so they apply
// in the order made. A write that fails (quota, a database closed
// underneath us) is handed to `onError` and otherwise ignored, like the
// localStorage adapter: the client keeps working from memory, and the
// next snapshot replaces the rows wholesale.
//
// Another tab may close the database under us (its `destroy()`, or a
// newer version of the app upgrading it): the connection is let go, and
// the next write opens it again. When that finds the database gone and
// made anew, what it held is lost, so the adapter tells `onReset`
// listeners, and the client writes its whole state and every pending op
// again: a reload must not come back from half the rows, or without the
// edits it had not sent.
//
//   const db = await openClient({ store: 'team-1', storage: indexedDBStorage('app:team-1'), ... });
//
// Use one name per store (and per replica: a tab is a replica), and
// `destroy()` to drop a database a closed tab left behind.

const STORES = ['records', 'ops', 'meta'];
const META_KEY = 'meta';
const VERSION = 2;
const keyOf = (c, id) => JSON.stringify([c, id ?? null]);

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
  let opened = 0;             // connections made so far
  const inFlight = new Set();
  const resetListeners = new Set();

  /** Let go of a connection that was closed under us, so the next write opens a new one */
  const forget = d => {
    if (db !== d) return;
    db = null;
    opening = null;
  };

  const open = () => {
    opening ??= new Promise((resolve, reject) => {
      const request = idb.open(name, VERSION);
      let created = false;
      request.onupgradeneeded = event => {
        created = event.oldVersion === 0;
        const d = request.result;
        // Before 1.0: leaves, and ops this version cannot send. What was there is let go
        if (event.oldVersion > 0 && event.oldVersion < VERSION) {
          for (const store of ['leaves', 'ops', 'meta']) if (d.objectStoreNames.contains(store)) d.deleteObjectStore(store);
        }
        for (const store of STORES) if (!d.objectStoreNames.contains(store)) d.createObjectStore(store);
      };
      request.onsuccess = () => {
        const d = request.result;
        db = d;
        // Another tab deleting or upgrading the database: step aside, and reopen on the next write
        d.onversionchange = () => {
          d.close();
          forget(d);
        };
        d.onclose = () => forget(d);
        const lost = created && opened > 0;
        opened++;
        resolve(d);
        // Made anew under us: what it held is gone (the writes waiting on
        // this open go first; what the listeners write lands after them)
        if (lost) for (const listener of resetListeners) listener();
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
      const [meta, records, ops] = await Promise.all([
        result(tx.objectStore('meta').get(META_KEY)),
        result(tx.objectStore('records').getAll()),
        result(tx.objectStore('ops').getAll())
      ]);
      if (!meta) return null;
      return { ...meta, ops, records };
    },
    commit({ puts, deletes, meta }) {
      write(['records', 'meta'], stores => {
        for (const [c, id] of deletes) stores.records.delete(keyOf(c, id));
        for (const record of puts) stores.records.put(record, keyOf(record[0], record[1]));
        stores.meta.put(meta, META_KEY);
      });
    },
    replace({ records, meta }) {
      write(['records', 'meta'], stores => {
        stores.records.clear();
        for (const record of records) stores.records.put(record, keyOf(record[0], record[1]));
        stores.meta.put(meta, META_KEY);
      });
    },
    saveOp(op, meta) {
      write(['ops', 'meta'], stores => {
        stores.ops.put(op, op.seq);
        stores.meta.put(meta, META_KEY);
      });
    },
    removeOp(seq, meta) {
      write(['ops', 'meta'], stores => {
        stores.ops.delete(seq);
        stores.meta.put(meta, META_KEY);
      });
    },
    dropOps(seq, meta) {
      write(['ops', 'meta'], stores => {
        stores.ops.delete(KeyRange.upperBound(seq));
        stores.meta.put(meta, META_KEY);
      });
    },
    /**
     * `listener` is called when the database had to be made anew under us
     * (another tab deleted it): the client writes everything again. Returns
     * what stops it
     */
    onReset(listener) {
      resetListeners.add(listener);
      return () => resetListeners.delete(listener);
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
