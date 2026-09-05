// shared.js - One socket per browser, however many tabs
//
// Every tab of an app is a client, but only one of them should talk to the
// server: one socket, one replica, one persisted outbox per browser, rather
// than one of each per tab. The tabs elect a leader with the Web Locks API
// (the lock's holder; when that tab goes, the next in line gets it), and
// the leader runs the browser's replica: a hidden client per store on the
// real connection, persisted through `storage(storeId)`. The app's own
// clients, in every tab including the leader's, are followers: ordinary
// clients whose transport reaches the leader over a BroadcastChannel (or
// directly, in the leader's own tab), where a relay speaks the server
// protocol to them on the replica's behalf. A follower's hello is answered
// with the replica's state, its ops are applied to the replica (and so go
// upstream under the browser's replica id, and into the browser's
// persisted outbox at once, socket or no socket) and acknowledged, every
// batch the replica sees goes to the followers as a patch, and presence is
// passed along. A follower's undo history is its own. Its status and its
// pending count are the browser's: the socket's status and the replica's
// unsent ops, which the leader tells the tabs (see `upstream` and
// `pending` below, which the client reads).
//
// When the leader tab closes, the next tab acquires the lock, loads the
// replica's outbox and state from `storage` (asynchronously, for an
// adapter such as IndexedDB; the tabs' messages wait), connects, and
// announces itself; the followers reconnect to it and resend whatever the
// old leader had not acknowledged. Without Web Locks or BroadcastChannel
// (an old browser, a test) a tab is its own leader and this is one
// connection.
//
//   const connection = sharedConnection({
//     name: 'app',                                            // one per app; names the channel and the lock
//     transport: webSocketTransport(() => `${url}?token=${token()}`),
//     storage: store => localStorageOutbox(`app:${store}`)   // the browser's replica, per store
//   });
//   const db = createClient({ connection, store: 'team-1', initial, lists });   // in every tab, as ever
import { LazyWatch } from 'lazy-watch';
import { createConnection } from './connection.js';
import { createClient, openClient } from './index.js';
import { memoryOutbox } from './storage.js';
import { createClock } from '../core/hlc.js';
import { randomId } from '../core/ids.js';
import { registerSet } from '../core/paths.js';

const { Utils } = LazyWatch;

/**
 * @param {Object} options
 * @param {string} options.name - names the channel and the lock: one per app
 * @param {() => Object} options.transport - the browser's socket (see
 *   transport.js); opened by the leader only
 * @param {(storeId: string) => Object} [options.storage] - the browser
 *   replica's persistence per store: any adapter, IndexedDB included
 *   (default: memory, so nothing survives a reload)
 * @param {{min: number, max: number}|false} [options.reconnect] - for the socket
 * @param {number|false} [options.keepalive] - for the socket
 * @param {((name: string) => Object)|null} [options.channel] - makes the
 *   channel between tabs (default: BroadcastChannel; null for none)
 * @param {Object|null} [options.locks] - a Web Locks manager (default:
 *   navigator.locks; null for none, and this tab leads on its own)
 * @param {string} [options.tabId]
 * @param {number} [options.linger=5000] - how long (ms) the replica keeps a
 *   store no tab has open anymore, so a tab reloading finds it as it was;
 *   then its client is disposed and its session on the server ends
 * @param {number} [options.sweepEvery=10000] - how often (ms) the leader
 *   looks for tabs that closed without a word, by the lock each tab holds
 * @param {(error: any) => void} [options.onError] - a replica's storage
 *   that failed to open; default console
 */
export function sharedConnection({
  name,
  transport,
  storage = () => memoryOutbox(),
  reconnect = { min: 500, max: 10_000 },
  keepalive = 30_000,
  channel = typeof BroadcastChannel === 'function' ? channelName => new BroadcastChannel(channelName) : null,
  locks = typeof navigator !== 'undefined' && navigator.locks ? navigator.locks : null,
  tabId = randomId(),
  linger = 5000,
  sweepEvery = 10_000,
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (typeof name !== 'string' || !name) throw new TypeError('sharedConnection requires a name');
  if (typeof transport !== 'function') throw new TypeError('sharedConnection requires a transport factory');
  if (typeof storage !== 'function') throw new TypeError('sharedConnection: storage must be a function of the store id');

  const bus = channel ? channel(`lazy-storage:${name}`) : null;
  const infos = new Map();            // store -> { initial, registers } this tab's clients declared
  const listeners = { status: new Set(), sync: new Set() };
  let relay = null;                   // the browser's replica, while this tab leads
  let upstream = 'connecting';        // the socket's status, as the leader last told this tab
  let pendingByStore = new Map();     // the replica's unsent ops per store, as the leader last told this tab
  let current = null;                 // this tab's transport to the relay, while open
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

  // --- This tab's clients: followers of the browser's replica ---------------------------------------

  const post = message => bus?.postMessage({ from: tabId, ...message });

  /** From this tab's clients to the relay: in this tab when it leads, else over the channel */
  function up(message) {
    if (relay) relay.receive(tabId, structuredClone(message));
    else post({ kind: 'up', message });
  }

  /** From the relay to this tab's clients */
  const down = message => current?.onmessage?.(message);

  /** The transport a follower connection opens: instant, the relay answering from whatever the replica has */
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
      // What this tab's clients declared, so the relay can build the replica's client for a store it has not seen
      for (const [store, info] of infos) up({ ctl: 'store', store, ...info });
      t.onopen?.();
    });
    return t;
  }

  const follower = createConnection({ transport: followerTransport, reconnect: { min: 50, max: 2000 }, keepalive: false });
  follower.on('status', status => notify('status', status));

  function reconnectSoon() {
    if (!closedByUser && !disposed) queueMicrotask(() => follower.connect());
  }

  /** The socket's status as the leader tells it; this tab's clients report it as theirs */
  function socketStatus(status) {
    upstream = status;
    notify('status', follower.status);
  }

  function pendingChanged(store, n) {
    if (pendingByStore.get(store) === n) return;
    pendingByStore.set(store, n);
    notify('sync');
  }

  if (bus) {
    bus.onmessage = event => {
      const m = event.data;
      if (!Utils.isPlainObject(m) || m.from === tabId) return;
      if (m.kind === 'up') {
        if (!relay) return;
        relay.receive(m.from, m.message);
        // A tab saying hello learns where the socket and the replica's outbox stand
        if (Utils.isPlainObject(m.message) && m.message.t === 'hello') {
          post({ kind: 'ctl', ctl: 'status', to: m.from, status: relay.status });
          post({ kind: 'ctl', ctl: 'pending', to: m.from, store: m.message.store, n: relay.pending(m.message.store) });
        }
        return;
      }
      if (m.kind === 'down') return void (m.to === tabId && down(m.message));
      if (m.kind !== 'ctl' || relay || (m.to !== undefined && m.to !== tabId)) return;   // a leader hears no other leader
      if (m.ctl === 'leader') {
        // A new leader: start over with it, resending what the old one had not acknowledged
        upstream = 'connecting';
        current?.close();
        reconnectSoon();
      } else if (m.ctl === 'status') {
        socketStatus(m.status);
      } else if (m.ctl === 'pending') {
        pendingChanged(m.store, m.n);
      }
    };
    if (typeof bus.unref === 'function') bus.unref();
  }

  // --- Leading: the browser's replica lives in this tab ---------------------------------------------

  /**
   * A tab that closes rarely gets to say goodbye, but the lock it holds
   * goes with it: the leader looks now and then for sessions whose tab
   * holds no lock anymore, and drops them
   */
  function startSweeping() {
    if (sweeper || !locks || typeof locks.query !== 'function') return;
    sweeper = setInterval(async () => {
      if (!relay) return;
      try {
        const { held } = await locks.query();
        const alive = new Set(held.map(lock => lock.name).filter(n => n.startsWith(tabLockPrefix)).map(n => n.slice(tabLockPrefix.length)));
        relay?.sweep(alive);
      } catch {
        // A query that failed tells nothing; the next one may
      }
    }, sweepEvery);
    if (typeof sweeper.unref === 'function') sweeper.unref();
  }

  function lead() {
    if (disposed || relay) return;
    relay = createRelay({
      tabId, transport, storage, reconnect, keepalive, infos, onError, linger,
      send: (tab, message) => { if (tab === tabId) down(structuredClone(message)); else post({ kind: 'down', to: tab, message }); },
      onStatus: status => { post({ kind: 'ctl', ctl: 'status', status }); socketStatus(status); },
      onPending: (store, n) => { post({ kind: 'ctl', ctl: 'pending', store, n }); notify('sync'); }
    });
    pendingByStore = new Map();
    startSweeping();
    post({ kind: 'ctl', ctl: 'leader' });
    // Our own clients were talking to the old leader (or to nobody); they start over with the relay here
    current?.close();
    reconnectSoon();
  }

  if (locks && typeof locks.request === 'function') {
    // Held for this tab's lifetime, so the leader can tell a closed tab from a quiet one
    locks.request(tabLockPrefix + tabId, { signal: aborter?.signal }, () => new Promise(resolve => {
      releaseTabLock = resolve;
    })).catch(() => {});
    locks.request(`lazy-storage:${name}`, { signal: aborter?.signal }, () => new Promise(resolve => {
      releaseLock = resolve;
      lead();
    })).catch(() => {});
  } else {
    lead();
  }

  return {
    get status() { return follower.status; },
    get attached() { return follower.attached; },
    get closed() { return follower.closed; },
    /** The browser's socket ('offline' | 'connecting' | 'open'), which this tab's clients report as their status */
    get upstream() { return relay ? relay.status : upstream; },
    /** Whether this tab runs the browser's replica */
    get leader() { return relay !== null; },
    tabId,
    /** The replica's unsent ops for a store, counted into this tab's clients' `pending` */
    pending(storeId) { return relay ? relay.pending(storeId) : pendingByStore.get(storeId) ?? 0; },
    /** 'status' (this tab's link, and the socket's status behind it), 'sync' (the replica's outbox changed), 'closed' (the socket was turned away) */
    on(event, fn) {
      if (event === 'closed') return follower.on('closed', fn);
      if (!listeners[event]) throw new TypeError(`Unknown connection event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    /** Open this tab's way to the replica, and prod the browser's socket if it is down (a tab just looked at, say) */
    connect() {
      closedByUser = false;
      follower.connect();
      if (relay) relay.wake();
      else post({ kind: 'up', message: { ctl: 'connect' } });
    },
    /** Close this tab's clients' way to the replica; the replica itself stays up for the other tabs */
    close() {
      closedByUser = true;
      follower.close();
    },
    attach(storeId, handler, info) {
      if (info) {
        infos.set(storeId, { initial: info.initial ?? {}, registers: info.registers ?? [] });
        if (current?.open) up({ ctl: 'store', store: storeId, ...infos.get(storeId) });
      }
      return follower.attach(storeId, handler);
    },
    /** This tab is done: its clients disconnect and, if it led, the replica closes and another tab takes over */
    dispose() {
      disposed = true;
      follower.close();
      clearInterval(sweeper);
      sweeper = null;
      relay?.dispose();
      relay = null;
      aborter?.abort();
      releaseLock?.();
      releaseTabLock?.();
      bus?.close();
    }
  };
}

/** A fresh replica writes its identity at once, so a tab taking over before any edit continues it rather than minting another */
function persistIdentity(adapter, client) {
  if (typeof adapter.commit === 'function') adapter.commit({ puts: [], deletes: [], meta: { replicaId: client.replicaId, seq: 0, version: 0, epoch: null } });
  else adapter.save({ replicaId: client.replicaId, seq: 0, ops: [] });
}

/**
 * The browser's replica and the server protocol spoken to the followers on
 * its behalf: a hidden client per store on the real connection, and per
 * follower and store a session that is answered like a server would.
 */
function createRelay({ tabId, transport, storage, reconnect, keepalive, infos, linger, send, onStatus, onPending, onError }) {
  const socket = createConnection({ transport, reconnect, keepalive });
  const entries = new Map();   // store -> { client, registers, sessions: Map<tab, session>, queue, stops, timer }
  const clock = createClock(`relay-${tabId}`);
  // The socket opens with the first hidden client; until it has been tried it is connecting, not down
  let attempted = false;
  const stops = [
    socket.on('status', status => {
      attempted = true;
      onStatus(status);
    }),
    // The socket was turned away (not signed in): final for every follower's connection too
    socket.on('closed', info => {
      for (const entry of entries.values()) for (const s of entry.sessions.values()) send(s.tab, { t: 'closed', code: info.code, message: info.message });
    })
  ];

  /**
   * The replica's client for a store, made on first use. An adapter that
   * loads asynchronously (IndexedDB) makes it a moment later; the tabs'
   * messages for the store wait in the entry's queue meanwhile
   */
  function entryFor(store) {
    let entry = entries.get(store);
    if (entry) return entry;
    const info = infos.get(store) ?? { initial: {}, registers: [] };
    entry = { store, client: null, registers: registerSet(info.registers).patterns.map(p => p.join('/')), sessions: new Map(), queue: [], stops: [], timer: null };
    entries.set(store, entry);
    const adapter = storage(store);
    const options = { connection: socket, store, initial: structuredClone(info.initial), registers: info.registers, storage: adapter, undo: false };
    const ready = (saved, client) => {
      if (entries.get(store) !== entry) return void client.dispose();   // let go while it was opening
      if (!saved) persistIdentity(adapter, client);
      entry.client = client;
      const all = message => { for (const s of entry.sessions.values()) send(s.tab, { ...message, store }); };
      entry.stops = [
        // Every batch the replica sees, its followers' own included, reaches every follower
        client.watch(diff => all({ t: 'patch', diff, ts: clock.now(), v: client.version })),
        client.on('peers', peers => { for (const s of entry.sessions.values()) if (s.presence) send(s.tab, { t: 'presence', peers, store }); }),
        client.on('closed', c => all({ t: 'closed', code: c.code, message: c.message })),
        client.on('error', err => all({ t: 'error', code: err.code, message: err.message })),
        client.on('sync', () => onPending(store, client.pending))
      ];
      client.connect();
      const queued = entry.queue;
      entry.queue = [];
      for (const [tab, message] of queued) relay.receive(tab, message);
    };
    const loaded = adapter.load();
    if (loaded && typeof loaded.then === 'function') {
      loaded.then(saved => openClient(options).then(client => ready(saved, client))).catch(err => {
        onError(err);
        entries.delete(store);
      });
    } else {
      ready(loaded, createClient(options));
    }
    return entry;
  }

  /**
   * A follower left a store. With nobody left on it the replica lingers a
   * moment (a tab reloading comes straight back), then its client is
   * disposed, which ends its session on the server; the persisted outbox
   * and cache stay for the next tab to open the store
   */
  function dropSession(entry, tab) {
    if (!entry.sessions.delete(tab) || entry.sessions.size > 0) return;
    clearTimeout(entry.timer);
    entry.timer = setTimeout(() => release(entry.store), linger);
    if (typeof entry.timer.unref === 'function') entry.timer.unref();
  }

  function release(store) {
    const entry = entries.get(store);
    if (!entry || entry.sessions.size > 0) return;
    entries.delete(store);
    for (const stop of entry.stops) stop();
    entry.client?.dispose();
  }

  /** Apply a follower's op to the replica (once per seq) */
  function apply(entry, session, op) {
    if (!Utils.isPlainObject(op) || !Number.isInteger(op.seq)) return;
    if (op.seq > session.lastSeq) {
      session.lastSeq = op.seq;
      if (Utils.isPlainObject(op.diff)) LazyWatch.patch(entry.client.wire, op.diff);
    }
  }

  const relay = {
    get status() { return attempted ? socket.status : 'connecting'; },
    /** The replica's unsent ops for a store */
    pending(store) { return entries.get(store)?.client?.pending ?? 0; },
    /** A tab asked for the socket: reconnect now rather than at the next backoff step, unless it was turned away */
    wake() {
      if (attempted && socket.status === 'offline' && !socket.closed) socket.connect();
    },
    /** A message from a follower tab: a store declaration, a nudge, or a wire message for one of its stores */
    receive(tab, message) {
      if (!Utils.isPlainObject(message)) return;
      if (message.ctl === 'store') {
        if (typeof message.store === 'string' && !infos.has(message.store)) infos.set(message.store, { initial: message.initial ?? {}, registers: message.registers ?? [] });
        return;
      }
      if (message.ctl === 'connect') return void relay.wake();
      const { store } = message;
      if (typeof store !== 'string' || !store) return;
      const entry = entryFor(store);
      if (!entry.client) return void entry.queue.push([tab, message]);
      const { client } = entry;
      let session = entry.sessions.get(tab);
      if (message.t === 'leave') return void dropSession(entry, tab);
      if (!session) {
        session = { tab, replicaId: null, lastSeq: 0, presence: true };
        entry.sessions.set(tab, session);
        clearTimeout(entry.timer);
        entry.timer = null;
      }
      switch (message.t) {
        case 'hello':
          if (typeof message.replicaId !== 'string') return;
          if (session.replicaId !== message.replicaId) {
            session.replicaId = message.replicaId;
            session.lastSeq = 0;
          }
          session.presence = message.presence !== false;
          // Answered from whatever the replica has, socket or no socket; a store the server had closed for us is tried again
          if (client.closed) client.connect();
          for (const op of Array.isArray(message.ops) ? message.ops : []) apply(entry, session, op);
          if (message.share !== undefined) client.share(message.share);
          send(tab, { t: 'snapshot', store, state: LazyWatch.snapshot(client.wire), ts: clock.now(), seq: session.lastSeq, registers: entry.registers, v: client.version, epoch: null });
          if (session.presence) send(tab, { t: 'presence', store, peers: client.peers });
          return;
        case 'op':
          apply(entry, session, message.op);
          if (Utils.isPlainObject(message.op)) send(tab, { t: 'ack', store, seq: message.op.seq, ts: clock.now(), correction: null });
          return;
        case 'share':
          client.share(message.data ?? null);
          return;
        case 'ping':
          send(tab, { t: 'pong' });
          return;
      }
    },
    /** Drop the sessions of tabs no longer alive (their locks gone); `alive` holds the tab ids that are */
    sweep(alive) {
      for (const entry of [...entries.values()]) {
        for (const tab of [...entry.sessions.keys()]) if (tab !== tabId && !alive.has(tab)) dropSession(entry, tab);
      }
    },
    dispose() {
      for (const stop of stops) stop();
      for (const entry of entries.values()) {
        clearTimeout(entry.timer);
        for (const stop of entry.stops) stop();
        entry.client?.dispose();
      }
      entries.clear();
      socket.close();
    }
  };
  return relay;
}
