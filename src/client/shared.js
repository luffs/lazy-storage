// shared.js - One socket per browser, however many tabs
//
// Every tab of an app is a client, but only one of them should talk to the
// server: one socket, one replica, one persisted outbox per browser. The
// tabs elect a leader with the Web Locks API (the lock's holder; when that
// tab goes, the next in line gets it), and the leader runs the browser's
// replica: a relay in 'local' mode (src/relay) on the real socket, its copy
// of each store and its outbox persisted through `storage(storeId)`. The
// app's own clients, in every tab including the leader's, are ordinary
// clients of that relay: their messages reach it over a BroadcastChannel
// (directly, in the leader's own tab), it acknowledges their ops once they
// are in the browser's outbox, socket or no socket, and sends every change
// to every tab. A tab's status and pending count are the browser's: the
// socket's status, and the replica's unsent ops, which the leader tells the
// tabs.
//
// When the leader tab closes, the next tab takes the lock, loads the
// replica from `storage`, connects, and announces itself; the other tabs
// say hello to it again, with whatever the old leader had not acknowledged.
// A tab that closes without a word is found by the lock it held. Without
// Web Locks or BroadcastChannel a tab is its own leader.
//
//   const connection = sharedConnection({
//     name: 'app',                                            // one per app; names the channel and the locks
//     transport: webSocketTransport(() => `${url}?token=${token()}`),
//     storage: store => indexedDBStorage(`app:${store}`)      // the browser's replica, per store
//   });
//   const db = createClient({ connection, store: 'team-1', initial });   // in every tab, as ever
import { LazyWatch } from 'lazy-watch';
import { createConnection } from './connection.js';
import { memoryOutbox } from './storage.js';
import { createRelay } from '../relay/index.js';
import { randomId } from '../core/ids.js';

const { Utils } = LazyWatch;

/**
 * The relay's copies (relay/storage.js) kept in client storage adapters,
 * one per store: a row adapter (IndexedDB) holds the records and the
 * outbox as rows, a document adapter the copy as its cache and the outbox
 * as its outbox. The outbox is written at once; the records once changes
 * settle (and on flush)
 */
function copiesIn(adapterOf, { delay = 200, onError }) {
  const adapters = new Map();
  const written = new Map();   // store -> seqs of the ops written
  const timers = new Map();
  const latest = new Map();
  const adapterFor = id => {
    if (!adapters.has(id)) adapters.set(id, adapterOf(id));
    return adapters.get(id);
  };
  const metaOf = copy => ({ replicaId: copy.replicaId, seq: copy.own.seq, version: copy.v, epoch: copy.epoch, fepoch: copy.fepoch, fv: copy.fv, admitted: copy.admitted });
  const writeBase = id => {
    clearTimeout(timers.get(id));
    timers.delete(id);
    const copy = latest.get(id);
    if (!copy) return;
    const adapter = adapterFor(id);
    try {
      if (typeof adapter.commit === 'function') adapter.replace({ records: copy.records, meta: metaOf(copy) });
      else adapter.saveState({ records: copy.records, version: copy.v, epoch: copy.epoch, fepoch: copy.fepoch, fv: copy.fv, admitted: copy.admitted });
    } catch (err) {
      onError(err);
    }
  };
  let replicaId = null;
  return {
    loadMeta: () => (replicaId ? { replicaId } : null),
    saveMeta: meta => { replicaId = meta.replicaId; },
    load(id) {
      const adapter = adapterFor(id);
      const toCopy = saved => {
        if (!Utils.isPlainObject(saved)) return null;
        written.set(id, new Set((saved.ops ?? []).map(op => op.seq)));
        return {
          records: Array.isArray(saved.records) ? saved.records : null,
          v: saved.version ?? 0,
          epoch: saved.epoch ?? null,
          fepoch: saved.fepoch,
          fv: saved.fv,
          admitted: saved.admitted ?? {},
          own: { seq: saved.seq ?? 0, ops: Array.isArray(saved.ops) ? saved.ops : [] }
        };
      };
      const saved = adapter.load();
      return saved && typeof saved.then === 'function' ? saved.then(toCopy) : toCopy(saved);
    },
    save(id, copy) {
      const adapter = adapterFor(id);
      const full = { ...copy, replicaId };
      latest.set(id, full);
      const meta = metaOf(full);
      const before = written.get(id) ?? new Set();
      const now = new Set(copy.own.ops.map(op => op.seq));
      try {
        if (typeof adapter.saveOp === 'function') {
          for (const op of copy.own.ops) if (!before.has(op.seq)) adapter.saveOp(op, meta);
          for (const seq of before) if (!now.has(seq)) adapter.removeOp(seq, meta);
        } else {
          adapter.save({ replicaId, seq: copy.own.seq, ops: copy.own.ops });
        }
      } catch (err) {
        onError(err);
      }
      written.set(id, now);
      if (!timers.has(id)) {
        const timer = setTimeout(() => writeBase(id), delay);
        if (typeof timer?.unref === 'function') timer.unref();
        timers.set(id, timer);
      }
    },
    flush() {
      for (const id of [...timers.keys()]) writeBase(id);
    }
  };
}

/** The relay's replica id, the same in every tab and across leaders: in localStorage when there is one */
function replicaOf(name) {
  const key = `lazy-storage:${name}:replica`;
  try {
    const known = globalThis.localStorage?.getItem(key);
    if (known) return known;
    const id = randomId();
    globalThis.localStorage?.setItem(key, id);
    return id;
  } catch {
    return randomId();
  }
}

/**
 * @param {Object} options
 * @param {string} options.name - names the channel and the locks: one per app
 * @param {() => Object} options.transport - the browser's socket; opened by the leader only
 * @param {(storeId: string) => Object} [options.storage] - the browser replica's persistence per store:
 *   any client adapter, IndexedDB included (default: memory, so nothing survives a reload)
 * @param {(storeId: string) => Object|null} [options.shapeOf] - a store's shape, for a store no tab of
 *   this leader has declared yet (the tabs' clients declare theirs)
 * @param {{min: number, max: number}|false} [options.reconnect] - for the socket
 * @param {number|false} [options.keepalive] - for the socket
 * @param {((name: string) => Object)|null} [options.channel] - default BroadcastChannel; null for none
 * @param {Object|null} [options.locks] - default navigator.locks; null for none
 * @param {string} [options.tabId]
 * @param {string} [options.replicaId] - the browser's replica (default: kept in localStorage)
 * @param {number} [options.sweepEvery=10000] - how often the leader looks for tabs that closed without a word
 * @param {(error: any) => void} [options.onError]
 */
export function sharedConnection({
  name,
  transport,
  storage = () => memoryOutbox(),
  shapeOf,
  reconnect = { min: 500, max: 10_000 },
  keepalive = 30_000,
  channel = typeof BroadcastChannel === 'function' ? channelName => new BroadcastChannel(channelName) : null,
  locks = typeof navigator !== 'undefined' && navigator.locks ? navigator.locks : null,
  tabId = randomId(),
  replicaId,
  sweepEvery = 10_000,
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('sharedConnection requires a name');
  if (typeof transport !== 'function') throw new TypeError('sharedConnection requires a transport factory');
  if (typeof storage !== 'function') throw new TypeError('sharedConnection: storage must be a function of the store id');

  const bus = channel ? channel(`lazy-storage:${name}`) : null;
  const shapes = new Map();          // store -> the shape a client declared, here or in another tab
  const ports = new Map();           // port id -> { port, allowed }
  let nextPort = 0;
  const listeners = { status: new Set(), sync: new Set() };
  let relay = null;                  // the browser's replica, while this tab leads
  const sessions = new Map();        // while leading: tab or port id -> its relay session
  let upstream = 'connecting';       // the socket's status, as the leader last said
  let pendingByStore = new Map();    // the replica's unsent ops per store, as the leader last said
  let current = null;                // this tab's transport to the relay, while open
  let closedByUser = true;
  let disposed = false;
  let releaseLock = null;
  let releaseTabLock = null;
  let sweeper = null;
  const aborter = typeof AbortController === 'function' ? new AbortController() : null;
  const tabLockPrefix = `lazy-storage:${name}:tab:`;

  function notify(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error('Error in lazy-storage connection listener:', err);
      }
    }
  }

  const post = message => bus?.postMessage({ from: tabId, ...message });

  // --- Leading ---------------------------------------------------------------------------

  /** A tab's or a port's messages, to the relay here, on a session of its own */
  function intoRelay(from, message) {
    if (!relay || !Utils.isPlainObject(message)) return;
    if (message.ctl === 'store') {
      if (typeof message.store === 'string' && Utils.isPlainObject(message.shape)) shapes.set(message.store, message.shape);
      return;
    }
    if (message.ctl === 'connect') return relay.upstreamUp();
    let session = sessions.get(from);
    if (!session) {
      session = relay.session({ send: reply => toTab(from, reply), credential: null, key: 'browser' });
      sessions.set(from, session);
    }
    session.receive(message);
    // A tab saying hello learns where the socket and the replica's outbox stand
    if (message.t === 'hello') {
      tell(from, { lazy: 'status', status: relay.status });
      tell(from, { lazy: 'pending', store: message.store, n: relay.pending(message.store) });
    }
  }

  /** From the relay to a tab or a port */
  function toTab(to, message) {
    if (to === tabId) current?.onmessage?.(structuredClone(message));
    else if (ports.has(to)) ports.get(to).port.postMessage(message);
    else post({ kind: 'down', to, message });
  }

  /** A control message to a tab or a port (to every one, without `to`) */
  function tell(to, message) {
    if (to === undefined) {
      post({ kind: 'ctl', message });
      control(message);
      for (const { port, allowed } of ports.values()) if (message.store === undefined || allowed.has(message.store)) port.postMessage(message);
      return;
    }
    if (to === tabId) return control(message);
    if (ports.has(to)) return ports.get(to).port.postMessage(message);
    post({ kind: 'ctl', to, message });
  }

  function lead() {
    if (disposed || relay) return;
    relay = createRelay({
      acknowledge: 'local',
      shapeOf: id => shapes.get(id) ?? shapeOf?.(id) ?? null,
      link: transport,
      storage: copiesIn(storage, { onError }),
      replicaId: replicaId ?? replicaOf(name),
      grace: 0,
      reconnect,
      keepalive,
      onError
    });
    relay.on('status', status => tell(undefined, { lazy: 'status', status }));
    relay.on('sync', ({ store, pending }) => tell(undefined, { lazy: 'pending', store, n: pending }));
    pendingByStore = new Map();
    startSweeping();
    post({ kind: 'ctl', message: { lazy: 'leader' } });
    // This tab's clients were talking to the old leader, or to nobody: they start over here
    current?.close();
    reconnectSoon();
    for (const { port } of ports.values()) port.postMessage({ lazy: 'reconnect' });
  }

  function startSweeping() {
    if (sweeper || !locks || typeof locks.query !== 'function') return;
    sweeper = setInterval(async () => {
      if (!relay) return;
      try {
        const { held } = await locks.query();
        const alive = new Set(held.map(lock => lock.name).filter(n => n.startsWith(tabLockPrefix)).map(n => n.slice(tabLockPrefix.length)));
        for (const [id, session] of [...sessions]) {
          const owner = id.split('/port/')[0];
          if (owner === tabId || alive.has(owner)) continue;
          session.close();
          sessions.delete(id);
        }
      } catch { /* a query that failed tells nothing */ }
    }, sweepEvery);
    if (typeof sweeper.unref === 'function') sweeper.unref();
  }

  // --- Following: this tab's clients ----------------------------------------------------------

  /** This tab's clients to the relay: here when this tab leads, else over the channel */
  function up(message, from = tabId) {
    if (relay) intoRelay(from, structuredClone(message));
    else post({ kind: 'up', from, message });
  }

  /** What the leader says of the socket and the outbox, which this tab's clients report as theirs */
  function control(message) {
    if (message.lazy === 'status' && typeof message.status === 'string') {
      if (upstream === message.status) return;
      upstream = message.status;
      notify('status', follower.status);
    } else if (message.lazy === 'pending' && typeof message.store === 'string') {
      const n = Number.isInteger(message.n) ? message.n : 0;
      if (pendingByStore.get(message.store) === n) return;
      pendingByStore.set(message.store, n);
      notify('sync');
    } else if (message.lazy === 'leader' && !relay) {
      // A new leader: start over with it, resending what the old one had not acknowledged
      upstream = 'connecting';
      current?.close();
      reconnectSoon();
      for (const { port } of ports.values()) port.postMessage({ lazy: 'reconnect' });
    }
  }

  function followerTransport() {
    const t = {
      onopen: null,
      onmessage: null,
      onclose: null,
      open: false,
      send(message) { if (t.open) up(message); },
      close() {
        if (!t.open) return;
        t.open = false;
        if (current === t) current = null;
        queueMicrotask(() => t.onclose?.());
      }
    };
    queueMicrotask(() => {
      if (disposed) return;
      t.open = true;
      current = t;
      // What this tab's clients declared, so the leader knows the shape of a store it has not seen
      for (const [store, shape] of shapes) up({ ctl: 'store', store, shape });
      t.onopen?.();
    });
    return t;
  }

  const follower = createConnection({ transport: followerTransport, reconnect: { min: 50, max: 2000 }, keepalive: false });
  follower.on('status', status => notify('status', status));
  function reconnectSoon() {
    if (!closedByUser && !disposed) queueMicrotask(() => follower.connect());
  }

  if (bus) {
    bus.onmessage = event => {
      const m = event.data;
      if (!Utils.isPlainObject(m) || m.from === tabId) return;
      if (m.kind === 'up') return intoRelay(m.from, m.message);
      if (m.kind === 'down') {
        if (m.to === tabId) current?.onmessage?.(m.message);
        else ports.get(m.to)?.port.postMessage(m.message);
        return;
      }
      if (m.kind === 'ctl' && !relay && (m.to === undefined || m.to === tabId || ports.has(m.to))) {
        if (ports.has(m.to)) return ports.get(m.to).port.postMessage(m.message);
        control(m.message);
        if (m.to === undefined) for (const { port, allowed } of ports.values()) if (m.message.store === undefined || allowed.has(m.message.store)) port.postMessage(m.message);
      }
    };
    if (typeof bus.unref === 'function') bus.unref();
  }

  if (locks && typeof locks.request === 'function') {
    locks.request(tabLockPrefix + tabId, { signal: aborter?.signal }, () => new Promise(resolve => {
      releaseTabLock = resolve;
    })).catch(() => {});
    locks.request(`lazy-storage:${name}`, { signal: aborter?.signal }, () => new Promise(resolve => {
      releaseLock = resolve;
      lead();
    })).catch(() => lead());
  } else {
    lead();
  }

  return {
    get status() { return follower.status; },
    get attached() { return follower.attached; },
    get closed() { return follower.closed; },
    /** The browser's socket ('offline' | 'connecting' | 'online'), which this tab's clients report as their status */
    get upstream() { return relay ? relay.status : upstream; },
    /** Whether this tab runs the browser's replica */
    get leader() { return relay !== null; },
    tabId,
    /** The replica's unsent ops for a store, counted into this tab's clients' `pending` */
    pending(storeId) { return relay ? relay.pending(storeId) : pendingByStore.get(storeId) ?? 0; },
    on(event, fn) {
      if (event === 'closed') return follower.on('closed', fn);
      if (!listeners[event]) throw new TypeError(`Unknown connection event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    /** Open this tab's way to the replica, and prod the browser's socket if it is down */
    connect() {
      closedByUser = false;
      follower.connect();
      up({ ctl: 'connect' });
    },
    close() {
      closedByUser = true;
      follower.close();
    },
    attach(storeId, handler, shape) {
      if (shape) {
        shapes.set(storeId, shape);
        if (current?.open) up({ ctl: 'store', store: storeId, shape });
      }
      return follower.attach(storeId, handler);
    },
    /**
     * Let the page on the other end of a MessagePort (an iframe, a worker)
     * have clients on the browser's replica for the stores named and no
     * other, under this tab's rights. The other end runs an ordinary client
     * on `portConnection(port)`. `stores` are { store, ...shape }. Returns
     * what ends it
     */
    follow(port, stores) {
      if (!port || typeof port.postMessage !== 'function') throw new TypeError('follow needs a MessagePort');
      if (!Array.isArray(stores) || stores.length === 0) throw new TypeError('follow needs the stores the port may use');
      const id = `${tabId}/port/${++nextPort}`;
      const allowed = new Set();
      for (const { store, ...shape } of stores) {
        if (typeof store !== 'string' || !store) throw new TypeError('follow: each store needs an id');
        allowed.add(store);
        if (!shapes.has(store)) {
          shapes.set(store, shape);
          up({ ctl: 'store', store, shape });
        }
      }
      const entry = { port, allowed };
      ports.set(id, entry);
      port.postMessage({ lazy: 'status', status: relay ? relay.status : upstream });
      for (const store of allowed) port.postMessage({ lazy: 'pending', store, n: relay ? relay.pending(store) : pendingByStore.get(store) ?? 0 });
      port.onmessage = event => {
        const message = event?.data;
        if (!Utils.isPlainObject(message) || message.ctl !== undefined) return;
        if (message.t !== 'ping' && !allowed.has(message.store)) return;
        up(message, id);
      };
      if (typeof port.start === 'function') port.start();
      return () => {
        if (ports.get(id) !== entry) return;
        ports.delete(id);
        port.onmessage = null;
        for (const store of allowed) up({ t: 'leave', store }, id);
      };
    },
    /** This tab is done: its clients disconnect and, if it led, the replica closes and another tab takes over */
    dispose() {
      disposed = true;
      for (const { port } of ports.values()) port.onmessage = null;
      ports.clear();
      follower.close();
      clearInterval(sweeper);
      sweeper = null;
      for (const session of sessions.values()) session.close();
      sessions.clear();
      relay?.close();
      relay = null;
      aborter?.abort();
      releaseLock?.();
      releaseTabLock?.();
      bus?.close();
    }
  };
}
