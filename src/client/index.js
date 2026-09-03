// client/index.js - A synced, offline-capable mirror of a store
//
// The client wraps a LazyWatch instance. Local writes are ordinary
// property writes on `state`; each emitted batch becomes an op with a
// hybrid-logical-clock timestamp, is appended to a persisted outbox, and
// is sent when online. Everything from the server is applied tagged
// { origin: 'remote' }, which is how the listener tells its own edits from
// the server's, and how the undo manager keeps remote changes out of
// history.
//
// On (re)connect the client sends its whole outbox in a `hello`, with
// the store version it last saw; the server merges the ops and answers
// with a delta of what happened since (applied as patches) or a snapshot
// (applied with `overwrite`). Edits made while the hello was in flight
// are re-applied on top and sent.
//
// The storage adapter keeps the outbox and a cache of the state, so a
// client restarted offline (or before its first snapshot lands) starts
// from what it last saw, pending edits included, instead of from nothing.
// A document adapter holds the state as one document written debounced;
// a row adapter holds one row per leaf and is written per batch (see
// persistence.js). A row adapter that loads asynchronously (IndexedDB)
// is opened with openClient().
//
// An op the server refuses comes back as an error with the op's seq and a
// code. 'clock-skew' means this device's clock runs ahead: the client
// adopts the server's time as an offset, re-stamps its pending ops, and
// sends them again, so nothing is lost. Any other refusal ('forbidden',
// 'expired', 'invalid') drops the op and resyncs from a snapshot, since
// the local state already reflects an edit the server will never hold.
//
// A client attaches to a connection under its store id. Pass a shared
// `connection` to carry several stores over one socket, or a `transport`
// to have the client own a connection of its own; the protocol is the same
// either way.
import { LazyWatch } from 'lazy-watch';
import { createClock, compareTs } from '../core/hlc.js';
import { registerSet } from '../core/paths.js';
import { leaves, expandRegisters, rebuild } from '../core/model.js';
import { randomId } from '../core/ids.js';
import { memoryOutbox } from './storage.js';
import { createConnection } from './connection.js';
import { createPersistence, isRowAdapter } from './persistence.js';

const { Utils } = LazyWatch;
const REMOTE = { origin: 'remote' };
const SNAPSHOT = { origin: 'remote', snapshot: true };
const RESTORE = { origin: 'restore' };
const HISTORY = new Set(['undo', 'redo']);
const EVENTS = ['status', 'error', 'sync', 'closed', 'presence'];
// A hello carries at most this many ops, so it stays under any payload
// limit after a long offline spell; the rest go as ops once the answer
// lands, the way edits made during the hello do
const HELLO_LIMIT = 1000;

/**
 * @param {Object} options
 * @param {string} options.store - the store id
 * @param {Object} [options.connection] - a shared connection from
 *   createConnection; either this or `transport`
 * @param {() => Object} [options.transport] - transport factory (see
 *   transport.js) for a connection this client owns
 * @param {Object} [options.initial] - state before the first snapshot
 * @param {Array<string|string[]>} [options.registers] - whole-value paths
 *   (arrays may live only here); must match the server's, which reports a
 *   mismatch as an error with code 'registers-mismatch' on every snapshot
 * @param {string} [options.replicaId] - defaults to the persisted one, else random
 * @param {Object} [options.storage] - outbox and state-cache persistence
 *   (default: memory). An adapter whose load() returns a promise needs
 *   openClient()
 * @param {boolean} [options.cache=true] - persist the state with the
 *   outbox and start from it on the next load; false keeps only the outbox
 * @param {boolean} [options.undo=true] - attach an undo manager
 * @param {number} [options.undoLimit=100]
 * @param {{min: number, max: number}|false} [options.reconnect] - retry
 *   backoff for an owned connection; false disables automatic reconnects
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createClient(options = {}) {
  const storage = options.storage ?? memoryOutbox();
  const saved = storage.load();
  if (saved && typeof saved.then === 'function') {
    throw new TypeError('This storage adapter loads asynchronously; open the client with openClient() instead');
  }
  return build({ ...options, storage }, saved);
}

/**
 * createClient for a storage adapter that loads asynchronously (IndexedDB):
 * resolves to the client once the cached state and outbox are in.
 * @returns {Promise<Object>}
 */
export function openClient(options = {}) {
  const storage = options.storage ?? memoryOutbox();
  return Promise.resolve(storage.load()).then(saved => build({ ...options, storage }, saved));
}

function build({
  store: storeId,
  connection,
  transport,
  initial = {},
  registers = [],
  replicaId,
  storage,
  cache = true,
  undo = true,
  undoLimit = 100,
  reconnect = { min: 500, max: 10_000 },
  now
}, saved) {
  if (typeof storeId !== 'string' || !storeId) throw new TypeError('createClient requires a store id');
  if (connection && transport) throw new TypeError('createClient takes either a connection or a transport, not both');
  if (!connection && typeof transport !== 'function') throw new TypeError('createClient requires a connection or a transport factory');
  const ownsConnection = !connection;
  connection = connection ?? createConnection({ transport, reconnect });
  const regs = registerSet(registers);
  const declared = regs.patterns.map(p => p.join('/')).sort();
  const rows = isRowAdapter(storage);

  if (!Utils.isPlainObject(saved)) saved = null;
  replicaId = replicaId ?? saved?.replicaId ?? randomId();
  const continuing = saved?.replicaId === replicaId;
  let seq = continuing ? saved.seq : 0;
  let outbox = continuing && Array.isArray(saved.ops) ? [...saved.ops] : [];
  // The cached state is the right starting point as long as it is this
  // replica's; `initial` underneath supplies any container added since.
  // A document cache is written debounced, so it may predate the last few
  // pending ops: replaying the outbox over it (idempotent for diffs)
  // brings it current
  const restored = cache && continuing && (rows ? Array.isArray(saved.rows) : Utils.isPlainObject(saved.state));
  const state = new LazyWatch(
    restored
      ? (rows ? rebuild(initial, saved.rows) : { ...structuredClone(initial), ...structuredClone(saved.state) })
      : structuredClone(initial),
    { inverse: true }
  );
  if (restored) for (const op of outbox) LazyWatch.patch(state, op.diff, RESTORE);
  // The store version (and the store's epoch) the state reflects, so a
  // reconnect can ask for what happened since rather than everything
  let known = { epoch: restored && typeof saved.epoch === 'string' ? saved.epoch : null, v: restored && Number.isInteger(saved.version) ? saved.version : 0 };

  // The clock runs on local time plus an offset the server corrects when
  // it refuses an op for running ahead (see 'clock-skew' below)
  const wall = now ?? Date.now;
  let offset = 0;
  const clock = createClock(replicaId, () => wall() + offset);
  const restamped = new Map(); // seq -> how many times the op was re-stamped after a skew refusal
  const listeners = Object.fromEntries(EVENTS.map(e => [e, new Set()]));
  let link = null;        // attachment to the connection while connected
  let synced = false;     // this store's snapshot has landed on this socket
  let lastStatus = 'offline';
  let presence = [];      // distinct users with a live session on this store
  let ended = null;       // { code, message } after the server closed this store for us
  let retryTimer = null;  // a hello scheduled after a rate-limit refusal

  const persistence = createPersistence({
    storage,
    cache,
    regs,
    state: () => state,
    ops: () => outbox,
    meta: () => ({ replicaId, seq, version: known.v, epoch: known.epoch }),
    onError: err => emit('error', err)
  });

  // Own batches (meta undefined) and undo/redo replays are history; remote
  // and rejected batches are not
  const undoManager = undo
    ? LazyWatch.createUndoManager(state, { limit: undoLimit, record: meta => !meta || HISTORY.has(meta.origin) })
    : null;

  function emit(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error(`Error in lazy-storage "${event}" listener:`, err);
      }
    }
  }

  const status = () => (!link || connection.status === 'offline' ? 'offline' : synced ? 'online' : 'connecting');

  function refreshStatus() {
    const next = status();
    if (next === lastStatus) return;
    lastStatus = next;
    emit('status', next);
  }
  const stopStatus = connection.on('status', refreshStatus);

  function setPresence(users) {
    if (users.length === 0 && presence.length === 0) return;
    presence = users;
    emit('presence', presence);
  }

  /** Drop acknowledged ops (seq and below) from the outbox and its persistence */
  function acknowledge(upTo) {
    outbox = outbox.filter(op => op.seq > upTo);
    for (const s of restamped.keys()) if (s <= upTo) restamped.delete(s);
    persistence.drop(upTo);
  }

  // Every local batch becomes an op. A batch that breaks the model is
  // reverted in place (tagged 'rejected', so it is neither sent nor
  // recorded) and reported. Remote and restore batches only reach
  // persistence.
  LazyWatch.on(state, (diff, inverse, meta) => {
    if (meta && !HISTORY.has(meta.origin)) {
      if (meta.origin !== 'rejected') persistence.batch(diff, meta);
      return;
    }
    let expanded;
    try {
      expanded = expandRegisters(diff, regs, state);
      leaves(expanded, regs);
    } catch (err) {
      if (inverse) LazyWatch.patch(state, inverse, { origin: 'rejected', error: err });
      emit('error', err);
      return;
    }
    const op = { replicaId, seq: ++seq, ts: clock.now(), diff: expanded };
    outbox.push(op);
    persistence.op(op);
    persistence.batch(expanded, meta);
    if (status() === 'online') link.send({ t: 'op', op });
    emit('sync');
  });

  /**
   * The (re)connect message: the outbox (its first HELLO_LIMIT ops) and,
   * unless a full snapshot is wanted, where our knowledge of the store ends
   */
  function hello({ full = false } = {}) {
    const message = { t: 'hello', replicaId, ops: outbox.slice(0, HELLO_LIMIT) };
    if (!full) Object.assign(message, { since: known.v, epoch: known.epoch });
    link?.send(message);
  }

  function checkRegisters(theirs) {
    if (!Array.isArray(theirs)) return;
    const sorted = [...theirs].sort();
    if (JSON.stringify(sorted) !== JSON.stringify(declared)) {
      const err = new Error(`Register paths differ: this client declares [${declared.join(', ')}], the server [${sorted.join(', ')}]`);
      err.code = 'registers-mismatch';
      emit('error', err);
    }
  }

  /**
   * The server's answer to a hello has arrived: a snapshot to overwrite
   * with, or a delta of diffs to apply. Either way the acknowledged ops
   * leave the outbox, edits made since the hello went out go back on top
   * and off to the server, and this store is online
   */
  function caughtUp(msg, applyServerState) {
    clock.receive(msg.ts);
    checkRegisters(msg.registers);
    // The version first, so what persistence writes for the batches below
    // is stamped with the version the state is about to reflect
    if (Number.isInteger(msg.v)) known = { epoch: typeof msg.epoch === 'string' ? msg.epoch : null, v: msg.v };
    acknowledge(msg.seq);
    applyServerState();
    for (const op of outbox) {
      LazyWatch.patch(state, op.diff, REMOTE);
      link.send({ t: 'op', op });
    }
    persistence.version();
    synced = true;
    refreshStatus();
    emit('sync');
  }

  function handle(msg) {
    switch (msg.t) {
      case 'snapshot':
        return caughtUp(msg, () => LazyWatch.overwrite(state, msg.state, SNAPSHOT));
      case 'delta':
        return caughtUp(msg, () => {
          for (const diff of Array.isArray(msg.patches) ? msg.patches : []) LazyWatch.patch(state, diff, REMOTE);
        });
      case 'patch':
        clock.receive(msg.ts);
        if (Number.isInteger(msg.v)) known.v = msg.v;
        LazyWatch.patch(state, msg.diff, REMOTE);
        return;
      case 'ack':
        clock.receive(msg.ts);
        acknowledge(msg.seq);
        if (msg.correction) LazyWatch.patch(state, msg.correction, REMOTE);
        emit('sync');
        return;
      case 'presence':
        setPresence(Array.isArray(msg.users) ? msg.users : []);
        return;
      case 'closed': {
        // Final for this store: evicted, forbidden, or unknown. Detach and
        // stay offline until connect() is called again
        ended = { code: msg.code, message: msg.message };
        if (link) {
          link.detach();
          link = null;
        }
        synced = false;
        if (ownsConnection) connection.close();
        setPresence([]);
        refreshStatus();
        emit('closed', ended);
        return;
      }
      case 'error': {
        if (msg.code === 'clock-skew' && correctClock(msg)) return;
        const err = new Error(msg.message);
        if (msg.code) err.code = msg.code;
        emit('error', err);
        if (msg.code === 'rate-limited') {
          // Nothing is dropped: the op stays in the outbox and a hello after
          // the server's retryAfter resends everything still pending
          if (retryTimer) return;
          retryTimer = setTimeout(() => {
            retryTimer = null;
            if (synced) hello();
          }, Number.isInteger(msg.retryAfter) ? msg.retryAfter : 1000);
          if (typeof retryTimer?.unref === 'function') retryTimer.unref();
          return;
        }
        // The server refused an op we already applied locally: drop it and
        // resync from a snapshot so this replica falls back in line (when a
        // hello is in flight its snapshot is already on the way)
        if (Number.isInteger(msg.seq)) {
          acknowledge(msg.seq);
          // Ask for a snapshot, not a delta: the delta would leave the
          // refused edit in place
          if (synced) hello({ full: true });
        }
        return;
      }
    }
  }

  /**
   * The server refused an op for being stamped too far ahead of its clock.
   * Adopt the server's time, re-stamp this op and every pending op after it
   * (they were stamped by the same clock), and send them again; with a
   * hello in flight the snapshot handler resends them instead. Returns
   * false when the error is stale (about a stamp already replaced) or the
   * correction has failed twice, in which case the op is given up on.
   */
  function correctClock(msg) {
    if (!Number.isInteger(msg.seq) || !Number.isInteger(msg.now)) return false;
    const refused = outbox.find(op => op.seq === msg.seq);
    if (!refused) return true;
    if (Array.isArray(msg.ts) && compareTs(refused.ts, msg.ts) !== 0) return true;
    const attempts = (restamped.get(refused.seq) ?? 0) + 1;
    if (attempts > 2) return false;
    offset = msg.now - wall();
    clock.rewind();
    const behind = outbox.filter(op => op.seq >= refused.seq);
    for (const op of behind) {
      op.ts = clock.now();
      restamped.set(op.seq, attempts);
      persistence.op(op);
    }
    if (synced) for (const op of behind) link.send({ t: 'op', op });
    return true;
  }

  const handler = {
    // Receives the link because on an already-open connection this fires
    // inside attach(), before connect() has stored the return value
    onOpen(attached) {
      link = attached;
      synced = false;
      hello();
      refreshStatus();
    },
    onMessage: handle,
    onClose() {
      synced = false;
      setPresence([]);
      refreshStatus();
    }
  };

  function connect() {
    ended = null;
    if (!link) link = connection.attach(storeId, handler);
    connection.connect();
    refreshStatus();
  }

  /** Detach this store; an owned connection closes, a shared one stays up for the others */
  function disconnect() {
    if (link) {
      link.detach();
      link = null;
    }
    if (ownsConnection) connection.close();
    synced = false;
    setPresence([]);
    refreshStatus();
  }

  /** Records keyed by id under `state[name]` */
  function collection(name) {
    const root = () => {
      if (!Utils.isPlainObject(state[name])) state[name] = {};
      return state[name];
    };
    const ids = () => (Utils.isPlainObject(state[name]) ? Object.keys(state[name]) : []);
    return {
      /** Add a record; its `id` is minted unless provided. Returns the id */
      add(record) {
        if (!Utils.isPlainObject(record)) throw new TypeError('collection.add expects a plain object');
        const id = record.id != null ? String(record.id) : randomId();
        root()[id] = { ...record, id };
        return id;
      },
      /** Merge fields into an existing record; false when it does not exist */
      update(id, fields) {
        const record = state[name]?.[id];
        if (!Utils.isPlainObject(record)) return false;
        Object.assign(record, fields);
        return true;
      },
      remove(id) {
        if (!Utils.isPlainObject(state[name]) || !Object.hasOwn(state[name], id)) return false;
        delete state[name][id];
        return true;
      },
      get: id => (Utils.isPlainObject(state[name]) ? state[name][id] : undefined),
      has: id => Utils.isPlainObject(state[name]) && Object.hasOwn(state[name], id),
      ids,
      all: () => ids().map(id => state[name][id])
    };
  }

  return {
    /** The mirrored state: read and write it like a plain object */
    state,
    replicaId,
    store: storeId,
    connection,
    get status() { return status(); },
    /** Unacknowledged local ops */
    get pending() { return outbox.length; },
    /** The store version this client has seen everything up to */
    get version() { return known.v; },
    /** Distinct users with a live session on this store (empty while offline) */
    get presence() { return presence; },
    /** Why the server closed this store for us ({ code, message }), or null */
    get closed() { return ended; },
    connect,
    disconnect,
    collection,
    /** Subscribe to state changes (a LazyWatch listener; meta.origin tells remote from local) */
    watch: (listener, options) => LazyWatch.on(state, listener, options),
    /**
     * Lifecycle events: 'status' (string), 'error' (Error, with `code` when
     * the server gave one), 'sync' (outbox changed), 'presence' (users),
     * 'closed' ({ code, message }: the server ended this store for us)
     */
    on(event, fn) {
      if (!listeners[event]) throw new TypeError(`Unknown event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    undo: () => (undoManager ? undoManager.undo() : false),
    redo: () => (undoManager ? undoManager.redo() : false),
    get canUndo() { return undoManager ? undoManager.canUndo : false; },
    get canRedo() { return undoManager ? undoManager.canRedo : false; },
    checkpoint: () => undoManager?.checkpoint(),
    group: fn => (undoManager ? undoManager.group(fn) : fn()),
    clearHistory: () => undoManager?.clear(),
    /** True when this client started from a cached state rather than `initial` */
    restored,
    dispose() {
      disconnect();
      clearTimeout(retryTimer);
      persistence.flush();
      stopStatus();
      undoManager?.dispose();
      LazyWatch.dispose(state);
    }
  };
}
