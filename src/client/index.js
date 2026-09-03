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
// On (re)connect the client sends its whole outbox in a `hello`; the
// server merges it and answers with a snapshot that already contains the
// result, which the client applies with `overwrite`. Edits made while the
// hello was in flight are re-applied on top and sent.
import { LazyWatch } from 'lazy-watch';
import { createClock } from '../core/hlc.js';
import { registerSet } from '../core/paths.js';
import { leaves, expandRegisters } from '../core/model.js';
import { randomId } from '../core/ids.js';
import { memoryOutbox } from './storage.js';

const { Utils } = LazyWatch;
const REMOTE = { origin: 'remote' };
const HISTORY = new Set(['undo', 'redo']);
const EVENTS = ['status', 'error', 'sync'];

/**
 * @param {Object} options
 * @param {() => Object} options.transport - transport factory (see transport.js)
 * @param {Object} [options.initial] - state before the first snapshot
 * @param {Array<string|string[]>} [options.registers] - whole-value paths
 *   (arrays may live only here); must match the server's
 * @param {string} [options.replicaId] - defaults to the persisted one, else random
 * @param {Object} [options.storage] - outbox persistence (default: memory)
 * @param {boolean} [options.undo=true] - attach an undo manager
 * @param {number} [options.undoLimit=100]
 * @param {{min: number, max: number}|false} [options.reconnect] - retry
 *   backoff after an unexpected close; false disables automatic reconnects
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createClient({
  transport,
  initial = {},
  registers = [],
  replicaId,
  storage = memoryOutbox(),
  undo = true,
  undoLimit = 100,
  reconnect = { min: 500, max: 10_000 },
  now
} = {}) {
  if (typeof transport !== 'function') throw new TypeError('createClient requires a transport factory');
  const regs = registerSet(registers);

  const saved = storage.load();
  replicaId = replicaId ?? saved?.replicaId ?? randomId();
  let seq = saved?.replicaId === replicaId ? saved.seq : 0;
  let outbox = saved?.replicaId === replicaId ? saved.ops : [];

  const state = new LazyWatch(structuredClone(initial), { inverse: true });
  const clock = createClock(replicaId, now);
  const listeners = Object.fromEntries(EVENTS.map(e => [e, new Set()]));
  let status = 'offline';
  let conn = null;
  let closedByUser = true;
  let retryDelay = reconnect ? reconnect.min : 0;
  let retryTimer = null;

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

  function setStatus(next) {
    if (status === next) return;
    status = next;
    emit('status', status);
  }

  function persistOutbox() {
    storage.save({ replicaId, seq, ops: outbox });
  }

  // Every local batch becomes an op. A batch that breaks the model is
  // reverted in place (tagged 'rejected', so it is neither sent nor
  // recorded) and reported.
  LazyWatch.on(state, (diff, inverse, meta) => {
    if (meta && !HISTORY.has(meta.origin)) return;
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
    persistOutbox();
    if (status === 'online') conn.send({ t: 'op', op });
    emit('sync');
  });

  function handle(msg) {
    if (!Utils.isPlainObject(msg)) return;
    switch (msg.t) {
      case 'snapshot': {
        clock.receive(msg.ts);
        outbox = outbox.filter(op => op.seq > msg.seq);
        LazyWatch.overwrite(state, msg.state, REMOTE);
        // Edits made since the hello went out: back on top, and off to the server
        for (const op of outbox) {
          LazyWatch.patch(state, op.diff, REMOTE);
          conn.send({ t: 'op', op });
        }
        persistOutbox();
        retryDelay = reconnect ? reconnect.min : 0;
        setStatus('online');
        emit('sync');
        return;
      }
      case 'patch':
        clock.receive(msg.ts);
        LazyWatch.patch(state, msg.diff, REMOTE);
        return;
      case 'ack':
        clock.receive(msg.ts);
        outbox = outbox.filter(op => op.seq > msg.seq);
        persistOutbox();
        if (msg.correction) LazyWatch.patch(state, msg.correction, REMOTE);
        emit('sync');
        return;
      case 'error': {
        emit('error', new Error(msg.message));
        // The server refused an op we already applied locally: drop it and
        // resync from a snapshot so this replica falls back in line
        if (Number.isInteger(msg.seq)) {
          outbox = outbox.filter(op => op.seq > msg.seq);
          persistOutbox();
          conn?.send({ t: 'hello', replicaId, ops: outbox });
        }
        return;
      }
      case 'pong':
        return;
    }
  }

  function open() {
    setStatus('connecting');
    const c = transport();
    conn = c;
    c.onopen = () => {
      if (conn === c) c.send({ t: 'hello', replicaId, ops: outbox });
    };
    c.onmessage = msg => {
      if (conn === c) handle(msg);
    };
    c.onclose = () => {
      if (conn !== c) return;
      conn = null;
      setStatus('offline');
      if (!closedByUser && reconnect) scheduleRetry();
    };
  }

  function scheduleRetry() {
    clearTimeout(retryTimer);
    retryTimer = setTimeout(open, retryDelay);
    retryDelay = Math.min(retryDelay * 2 || reconnect.min, reconnect.max);
  }

  function connect() {
    closedByUser = false;
    clearTimeout(retryTimer);
    if (!conn) open();
  }

  function disconnect() {
    closedByUser = true;
    clearTimeout(retryTimer);
    const c = conn;
    conn = null;
    c?.close();
    setStatus('offline');
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
    get status() { return status; },
    /** Unacknowledged local ops */
    get pending() { return outbox.length; },
    connect,
    disconnect,
    collection,
    /** Subscribe to state changes (a LazyWatch listener; meta.origin tells remote from local) */
    watch: (listener, options) => LazyWatch.on(state, listener, options),
    /** Lifecycle events: 'status' (string), 'error' (Error), 'sync' (outbox changed) */
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
    dispose() {
      disconnect();
      undoManager?.dispose();
      LazyWatch.dispose(state);
    }
  };
}
