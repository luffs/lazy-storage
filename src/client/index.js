// client/index.js - A synced, offline-capable mirror of a store
//
// A client keeps the server's state and its own edits apart. The BASE is
// the state as the server last told it (a snapshot, then every patch on
// top). The OUTBOX is this replica's ops the server has not acknowledged.
// `state`, what the app reads and writes, is a LazyWatch proxy holding the
// base with the outbox replayed on top, optimistically: a pending change
// shows where its record and parent are there, as the server would take it.
//
// A local batch on `state` becomes an op ({ replica, seq, ts, changes },
// core/compile.js), goes in the outbox and to the server. A patch from the
// server goes to the base, and every record it touched is worked out again
// (its base value, with the pending changes that touch it replayed) and
// written into `state`, so the app sees only what changed. An ack takes its
// op out of the outbox and works its records out again: a change of ours
// that lost now shows the base's value, which came with the patch that won.
// Nothing is corrected by hand, and the `lost` event says what lost and why.
//
// A new op also takes over from what older pending ops wrote at or beneath
// the paths it writes (`supersede`), so typing into one field keeps one op
// pending rather than one per keystroke.
//
// On (re)connect the client sends its outbox in a `hello` with the version
// it last saw; the server answers with a delta (applied to the base as
// patches are) or a snapshot (which replaces the base). The storage adapter
// keeps the outbox and the base, so a client restarted offline starts from
// what it last saw, its edits on top.
import { LazyWatch } from 'lazy-watch';
import { createClock } from '../core/clock.js';
import { PROTOCOL } from '../core/protocol.js';
import { createShape } from '../core/shape.js';
import { docFromRecords } from '../core/doc.js';
import { compile, within } from '../core/compile.js';
import { registerKey } from '../core/paths.js';
import { randomId } from '../core/ids.js';
import { memoryOutbox } from './storage.js';
import { createConnection } from './connection.js';
import { createPersistence, isRowAdapter } from './persistence.js';
import { createList } from './list.js';

const { Utils } = LazyWatch;
const isObj = Utils.isPlainObject;
const REMOTE = { origin: 'remote' };
const SNAPSHOT = { origin: 'remote', snapshot: true };
const HISTORY = new Set(['undo', 'redo']);
const EVENTS = ['status', 'error', 'sync', 'closed', 'presence', 'peers', 'history', 'lost', 'rejected', 'reset', 'relay'];
const HELLO_LIMIT = 1000;
const clone = value => (value === null || typeof value !== 'object' ? value : structuredClone(value));
const plain = value => (value === null || typeof value !== 'object' ? value : LazyWatch.isProxy(value) ? LazyWatch.snapshot(value) : structuredClone(value));

/**
 * @param {Object} options
 * @param {string} options.store - the store id
 * @param {Object} [options.connection] - a connection from createConnection (several stores, one socket); or
 * @param {() => Object} [options.transport] - a transport factory for a connection this client owns
 * @param {Object} [options.initial] - the store's top level (shape.js), as the server declares it
 * @param {string[]} [options.singletons]
 * @param {Array<string|string[]>} [options.registers]
 * @param {Object} [options.parents]
 * @param {string} [options.position='pos'] - the position field of list records
 * @param {string} [options.replicaId] - defaults to the persisted one, else random
 * @param {Object} [options.storage] - outbox and base (default: memory); one that loads asynchronously needs openClient()
 * @param {boolean} [options.mirror=false] - a client that only reads: cache, undo and presence default to false
 * @param {boolean} [options.cache=true]
 * @param {number} [options.cacheDelay=1000]
 * @param {boolean} [options.undo=true]
 * @param {number} [options.undoLimit=100]
 * @param {{min: number, max: number}|false} [options.reconnect]
 * @param {boolean} [options.presence=true]
 * @param {() => number} [options.now]
 */
export function createClient(options = {}) {
  const storage = options.storage ?? memoryOutbox();
  const saved = storage.load();
  if (saved && typeof saved.then === 'function') {
    throw new TypeError('This storage adapter loads asynchronously; open the client with openClient() instead');
  }
  return build({ ...options, storage }, saved);
}

/** createClient for a storage adapter that loads asynchronously (IndexedDB) */
export function openClient(options = {}) {
  const storage = options.storage ?? memoryOutbox();
  return Promise.resolve(storage.load()).then(saved => build({ ...options, storage }, saved));
}

function build({
  store: storeId,
  connection,
  transport,
  initial = {},
  singletons = [],
  registers = [],
  parents = {},
  position = 'pos',
  replicaId,
  storage,
  mirror = false,
  cache = !mirror,
  cacheDelay = 1000,
  undo = !mirror,
  undoLimit = 100,
  reconnect = { min: 500, max: 10_000 },
  presence: wantsPresence = !mirror,
  now
}, saved) {
  if (typeof storeId !== 'string' || !storeId) throw new TypeError('createClient requires a store id');
  if (connection && transport) throw new TypeError('createClient takes either a connection or a transport, not both');
  if (!connection && typeof transport !== 'function') throw new TypeError('createClient requires a connection or a transport factory');
  const ownsConnection = !connection;
  connection = connection ?? createConnection({ transport, reconnect });
  const shape = createShape({ initial, singletons, registers, parents });
  const rows = isRowAdapter(storage);
  if (rows) {
    for (const method of ['load', 'commit', 'replace', 'saveOp', 'removeOp', 'dropOps']) {
      if (typeof storage[method] !== 'function') throw new TypeError(`A row storage adapter needs ${method}`);
    }
  }

  if (!isObj(saved)) saved = null;
  replicaId = replicaId ?? saved?.replicaId ?? randomId();
  const continuing = saved?.replicaId === replicaId;
  let seq = continuing && Number.isInteger(saved.seq) ? saved.seq : 0;
  let outbox = continuing && Array.isArray(saved.ops) ? saved.ops.filter(op => isObj(op) && Array.isArray(op.changes)) : [];
  const restored = cache && continuing && (rows ? Array.isArray(saved.records) : isObj(saved.state));

  // The base: the server's state as last told, with each record's clock, so
  // pending changes are judged over it as the server will judge them
  function baseFromState(snapshotState, clocks) {
    const byKey = new Map((Array.isArray(clocks) ? clocks : []).map(([c, id, clockJSON]) => [shape.recordKey(c, id ?? undefined), clockJSON]));
    const records = [];
    if (isObj(snapshotState)) {
      for (const c of shape.collections) for (const id of Object.keys(isObj(snapshotState[c]) ? snapshotState[c] : {})) records.push([c, id, snapshotState[c][id], byKey.get(shape.recordKey(c, id))]);
      for (const c of shape.singletons) if (Object.hasOwn(snapshotState, c)) records.push([c, null, snapshotState[c], byKey.get(shape.recordKey(c))]);
    }
    return docFromRecords(shape, structuredClone(records));
  }
  let base = !restored ? baseFromState(null)
    : rows ? docFromRecords(shape, saved.records.map(([c, id, data, clockJSON]) => [c, id, data, clockJSON]))
      : baseFromState(saved.state, saved.clocks);
  let known = { epoch: restored && typeof saved.epoch === 'string' ? saved.epoch : null, v: restored && Number.isInteger(saved.version) ? saved.version : 0 };

  /** The base with every pending change replayed: what `state` shows */
  function replayedAll() {
    const scratch = docFromRecords(shape, structuredClone([...base.records()]));
    for (const op of outbox) for (const change of op.changes) scratch.apply(change, op.ts, 'replay');
    return scratch.state;
  }

  const state = new LazyWatch(replayedAll(), { inverse: true });
  const clock = createClock({ now });
  const listeners = Object.fromEntries(EVENTS.map(e => [e, new Set()]));
  let link = null;
  let synced = false;
  let answered = 0;       // ops up to this seq went in the hello now in flight; later ones go live once it is answered
  let lastStatus = 'offline';
  let presence = [];
  let peerMap = new Map();
  let peers = [];
  let shared;
  let ended = null;
  let retryTimer = null;
  let throttled = false;
  let relayed = false;
  let answeredByRelay = false;
  let fetching = 0;
  let held = null;
  let fetchFailed = false;
  const sentAt = new Map();
  let ackMs = null;

  function emit(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error(`Error in lazy-storage "${event}" listener:`, err);
      }
    }
  }

  const persistence = createPersistence({
    storage,
    cache,
    shape,
    base: () => base,
    ops: () => outbox,
    meta: () => ({ replicaId, seq, version: known.v, epoch: known.epoch }),
    onError: err => emit('error', err),
    cacheDelay
  });

  const undoManager = undo
    ? LazyWatch.createUndoManager(state, { limit: undoLimit, record: meta => !meta || HISTORY.has(meta.origin) })
    : null;
  const history = () => ({ canUndo: undoManager.canUndo, canRedo: undoManager.canRedo });

  // --- Working records out -----------------------------------------------------------

  const keyOf = change => shape.recordKey(change.c, change.id);

  /** One record as `state` should show it: its base value, with the pending changes that touch it */
  function replayed(c, id) {
    const key = shape.recordKey(c, id);
    const list = [];
    for (const op of outbox) for (const change of op.changes) if (keyOf(change) === key) list.push([change, op.ts]);
    const record = base.get(c, id);
    if (!list.length) return clone(record);
    const scratch = docFromRecords(shape, record === undefined ? [] : [[c, id ?? null, clone(record), base.clock(c, id)]]);
    for (const [change, ts] of list) scratch.apply(change, ts, 'replay');
    return scratch.get(c, id);
  }

  /** Write a record into `state` as `value` (undefined: gone), so lazy-watch sends only what changed */
  function writeRecord(c, id, value, meta) {
    const single = shape.isSingleton(c);
    const current = single ? state[c] : (Object.hasOwn(state[c], id) ? state[c][id] : undefined);
    if (value === undefined) {
      if (current !== undefined && !single) LazyWatch.patch(state, { [c]: { [id]: null } }, meta);
      return;
    }
    if (isObj(current) && isObj(value)) {
      LazyWatch.overwrite(current, value, meta);
      return;
    }
    if (current === value) return;
    LazyWatch.patch(state, single ? { [c]: value } : { [c]: { [id]: value } }, meta);
  }

  /** Work these records out again ([c, id] pairs, or change-like objects) and show them */
  function refresh(records, meta = REMOTE) {
    const seen = new Map();
    for (const r of records) {
      const c = Array.isArray(r) ? r[0] : r.c;
      const id = Array.isArray(r) ? r[1] : r.id;
      seen.set(shape.recordKey(c, id), [c, id ?? undefined]);
    }
    for (const [c, id] of seen.values()) writeRecord(c, id, replayed(c, id), meta);
    return [...seen.values()];
  }

  // --- Local batches -----------------------------------------------------------------

  /**
   * A new op takes over from what older pending ops wrote at or beneath the
   * paths it sets or deletes, in the same record: those writes could never
   * decide a value again. Returns { changed, removed } for persistence
   */
  function supersede(op) {
    const written = op.changes.filter(ch => !Object.hasOwn(ch, 'put')).map(ch => [keyOf(ch), ch.path ?? []]);
    if (!written.length) return null;
    const changed = [];
    const removed = [];
    outbox = outbox.filter(older => {
      const before = older.changes.length;
      older.changes = older.changes.filter(ch => !written.some(([key, path]) => key === keyOf(ch) && within(ch.path ?? [], path)));
      if (older.changes.length === before) return true;
      if (older.changes.length) {
        changed.push(older);
        return true;
      }
      removed.push(older.seq);
      return false;
    });
    return { changed, removed };
  }

  const sendOp = op => {
    sentAt.set(op.seq, performance.now());
    link.send({ t: 'op', op });
  };
  const live = () => Boolean(link) && synced && !throttled && connection.status !== 'offline';

  LazyWatch.on(state, (diff, inverse, meta) => {
    if (meta && !HISTORY.has(meta.origin)) return;
    let changes;
    try {
      changes = compile(shape, diff, inverse, state);
    } catch (err) {
      if (inverse) LazyWatch.patch(state, inverse, { origin: 'rejected', error: err });
      emit('error', err);
      emit('rejected', { seq: null, code: err.code ?? 'invalid', message: err.message, changes: null });
      return;
    }
    if (!changes.length) return;
    for (const change of changes) if (change.path && change.path.length === 0) delete change.path;
    const op = { replica: replicaId, seq: ++seq, ts: clock.stamp(), changes };
    const superseded = supersede(op);
    outbox.push(op);
    persistence.op(op, superseded);
    if (live()) sendOp(op);
    emit('sync');
    if (undoManager) emit('history', history());
  });

  // --- Status and presence -----------------------------------------------------------

  const linked = () => Boolean(link) && connection.status !== 'offline';
  // Behind a shared connection (a browser's tabs, a page on a port) the link may be up while the
  // browser's socket is down: the client is then as offline as that socket, though its edits still
  // reach the browser's replica
  const status = () => {
    if (!linked()) return 'offline';
    if (!synced) return 'connecting';
    const upstream = connection.upstream;
    if (upstream === undefined || upstream === 'online') return 'online';
    return upstream === 'offline' ? 'offline' : 'connecting';
  };
  const browserPending = () => (typeof connection.pending === 'function' ? connection.pending(storeId) : 0);

  function refreshStatus() {
    const next = status();
    if (next === lastStatus) return;
    lastStatus = next;
    emit('status', next);
  }
  const stopStatus = connection.on('status', refreshStatus);
  const stopSync = typeof connection.pending === 'function' ? connection.on('sync', () => emit('sync')) : () => {};

  function setPresence(users) {
    if (JSON.stringify(users) === JSON.stringify(presence)) return;
    presence = users;
    emit('presence', presence);
  }
  function usersOf() {
    const seen = new Map();
    for (const peer of peerMap.values()) if (peer.key !== undefined && !seen.has(peer.key)) seen.set(peer.key, peer.user);
    return [...seen.values()];
  }
  const isPeer = peer => isObj(peer) && typeof peer.replicaId === 'string';
  const sameJSON = (a, b) => JSON.stringify(a) === JSON.stringify(b);

  function applyPresence(msg) {
    let changed = false;
    let users = false;
    if (Array.isArray(msg.peers)) {
      const before = [...peerMap.values()];
      peerMap = new Map();
      for (const peer of msg.peers) if (isPeer(peer)) peerMap.set(peer.replicaId, peer);
      changed = users = !sameJSON(before, [...peerMap.values()]);
    } else {
      for (const id of Array.isArray(msg.left) ? msg.left : []) if (peerMap.delete(id)) changed = users = true;
      for (const peer of Array.isArray(msg.joined) ? msg.joined : []) {
        if (!isPeer(peer) || sameJSON(peerMap.get(peer.replicaId), peer)) continue;
        peerMap.set(peer.replicaId, peer);
        changed = users = true;
      }
      for (const share of Array.isArray(msg.shared) ? msg.shared : []) {
        const peer = isObj(share) ? peerMap.get(share.replicaId) : undefined;
        if (!peer || sameJSON(peer.data, share.data)) continue;
        const next = { ...peer };
        if (share.data === undefined) delete next.data;
        else next.data = share.data;
        peerMap.set(peer.replicaId, next);
        changed = true;
      }
    }
    if (!changed) return;
    peers = [...peerMap.values()];
    emit('peers', peers);
    if (users) setPresence(usersOf());
  }

  function clearPresence() {
    peerMap = new Map();
    if (peers.length) {
      peers = [];
      emit('peers', peers);
    }
    setPresence([]);
  }

  function setRelayed(next) {
    if (relayed === next) return;
    relayed = next;
    emit('relay', relayed);
  }

  // --- The protocol ------------------------------------------------------------------

  function hello() {
    const ops = outbox.slice(0, HELLO_LIMIT);
    const message = { t: 'hello', proto: PROTOCOL, replica: replicaId, ops, clocks: true };
    const at = performance.now();
    for (const op of ops) sentAt.set(op.seq, at);
    answered = ops.length ? ops[ops.length - 1].seq : 0;
    throttled = false;
    if (known.epoch !== null) Object.assign(message, { since: known.v, epoch: known.epoch });
    if (shared !== undefined) message.share = shared;
    if (!wantsPresence) message.presence = false;
    if (typeof connection.fetch === 'function' && !fetchFailed) message.fetch = true;
    link?.send(message);
  }

  /** Drop acknowledged ops (seq and below); returns the records they touched */
  function acknowledge(upTo) {
    const touched = [];
    const before = outbox.length;
    outbox = outbox.filter(op => {
      if (op.seq > upTo) return true;
      for (const change of op.changes) touched.push(change);
      sentAt.delete(op.seq);
      return false;
    });
    if (outbox.length !== before) persistence.drop(upTo);
    return touched;
  }

  function reportLost(list) {
    for (const entry of list) if (isObj(entry) && Array.isArray(entry.lost) && entry.lost.length) emit('lost', { seq: entry.seq, lost: entry.lost });
  }

  function checkShape(theirs) {
    if (typeof theirs !== 'string' || theirs === shape.hash) return;
    const err = new Error('This client declares the store otherwise than the server does (initial, singletons, registers or parents)');
    err.code = 'shape-mismatch';
    emit('error', err);
  }

  /** A snapshot or a delta landed: the base brought up to date, `state` worked out again, the outbox resent past the hello */
  function caughtUp(msg, applyToBase) {
    clock.adopt(msg.now);
    checkShape(msg.shape);
    const previousEpoch = known.epoch;
    const touched = acknowledge(Number.isInteger(msg.seq) ? msg.seq : 0);
    const changedRecords = applyToBase();
    known = { epoch: typeof msg.epoch === 'string' ? msg.epoch : null, v: Number.isInteger(msg.v) ? msg.v : known.v };
    if (changedRecords === 'all') {
      LazyWatch.overwrite(state, replayedAll(), SNAPSHOT);
      persistence.snapshot();
    } else {
      refresh([...changedRecords, ...touched]);
      persistence.base(changedRecords.map(ch => [ch.c, ch.id]));
      if (!changedRecords.length) persistence.version();
    }
    // A relay counts in versions of its own: moving between it and the server is not the server starting over
    const viaRelay = msg.relay === true;
    if (!viaRelay && !answeredByRelay && previousEpoch !== null && known.epoch !== null && previousEpoch !== known.epoch) emit('reset', { previous: { epoch: previousEpoch } });
    answeredByRelay = viaRelay;
    synced = true;
    refreshStatus();
    reportLost(Array.isArray(msg.lost) ? msg.lost : []);
    if (live()) for (const op of outbox) if (op.seq > answered) sendOp(op);
    emit('sync');
  }

  function snapshotInto(snapshotState, clocks) {
    base = baseFromState(snapshotState, clocks);
    return 'all';
  }

  function fetchSnapshot(msg) {
    const attempt = ++fetching;
    const queue = held = [];
    const current = () => attempt === fetching && held === queue;
    Promise.resolve()
      .then(() => connection.fetch(msg.fetch))
      .then(async response => {
        if (!response.ok) throw new Error(`the server answered ${response.status}`);
        const body = await response.json();
        if (!isObj(body) || !isObj(body.state) || !Number.isInteger(body.v)) throw new Error('the response is not a snapshot');
        return body;
      })
      .then(
        body => {
          if (!current()) return;
          held = null;
          caughtUp({ ...msg, v: body.v, epoch: typeof body.epoch === 'string' ? body.epoch : msg.epoch }, () => snapshotInto(body.state, body.clocks));
          for (const m of queue) if (m.t !== 'patch' || !(Number.isInteger(m.v) && m.v <= body.v)) handle(m);
        },
        err => {
          if (!current()) return;
          held = null;
          fetchFailed = true;
          const error = new Error(`The snapshot could not be fetched: ${err?.message ?? err}`);
          error.code = 'snapshot-fetch';
          emit('error', error);
          for (const m of queue) handle(m);
          hello();
        }
      )
      .catch(err => emit('error', err));
  }

  /** Changes from the server, decided already: onto the base. Returns them */
  function intoBase(changes, ts) {
    clock.observe(ts);
    const list = Array.isArray(changes) ? changes : [];
    for (const change of list) {
      try {
        base.apply(change, ts, 'trust');
      } catch (err) {
        emit('error', err);
      }
    }
    return list;
  }

  function retryHello(after) {
    if (retryTimer) return;
    retryTimer = setTimeout(() => {
      retryTimer = null;
      if (linked()) hello();
    }, after);
    if (typeof retryTimer?.unref === 'function') retryTimer.unref();
  }

  function handle(msg) {
    if (held && msg.t !== 'closed') return void held.push(msg);
    switch (msg.t) {
      case 'snapshot':
        if (typeof msg.fetch === 'string' && msg.state === undefined) return fetchSnapshot(msg);
        return caughtUp(msg, () => snapshotInto(msg.state, msg.clocks));
      case 'delta':
        return caughtUp(msg, () => {
          const all = [];
          for (const entry of Array.isArray(msg.ops) ? msg.ops : []) all.push(...intoBase(entry.changes, entry.ts));
          return all;
        });
      case 'patch': {
        if (!synced) return;
        // Versions count one per patch: one skipped means a patch never arrived, and a catch-up is the way back
        if (Number.isInteger(msg.v) && known.epoch !== null && msg.v > known.v + 1) {
          synced = false;
          refreshStatus();
          hello();
          return;
        }
        if (Number.isInteger(msg.v)) known.v = msg.v;
        const list = intoBase(msg.changes, msg.ts);
        refresh(list);
        persistence.base(list.map(ch => [ch.c, ch.id]));
        return;
      }
      case 'ack': {
        clock.adopt(msg.now);
        const at = sentAt.get(msg.seq);
        if (at !== undefined) ackMs = performance.now() - at;
        if (Number.isInteger(msg.v) && msg.v > known.v) known.v = msg.v;
        refresh(acknowledge(msg.seq));
        reportLost([{ seq: msg.seq, lost: msg.lost }]);
        emit('sync');
        return;
      }
      case 'lost':
      case 'rejected':
      case 'reset': {
        const { t, store: _store, ...payload } = msg;
        emit(t, payload);
        return;
      }
      case 'relay':
        setRelayed(msg.status === 'local');
        return;
      case 'presence':
        if (wantsPresence) applyPresence(msg);
        return;
      case 'closed': {
        if (msg.code === 'unavailable') {
          synced = false;
          fetching++;
          held = null;
          clearPresence();
          refreshStatus();
          retryHello(250 + Math.random() * 750);
          return;
        }
        ended = { code: msg.code, message: msg.message };
        if (link) {
          link.detach();
          link = null;
        }
        synced = false;
        fetching++;
        held = null;
        if (ownsConnection) connection.close();
        clearPresence();
        refreshStatus();
        setRelayed(false);
        emit('closed', ended);
        return;
      }
      case 'error': {
        const err = new Error(msg.message);
        if (msg.code) err.code = msg.code;
        emit('error', err);
        if (msg.code === 'rate-limited') {
          if (Number.isInteger(msg.seq)) throttled = true;
          retryHello(Number.isInteger(msg.retryAfter) ? msg.retryAfter : 1000);
          return;
        }
        if (Number.isInteger(msg.seq)) {
          const refused = outbox.find(op => op.seq === msg.seq);
          if (!refused) return;
          outbox = outbox.filter(op => op !== refused);
          persistence.remove(refused.seq);
          refresh(refused.changes, { origin: 'rejected' });
          emit('rejected', { seq: msg.seq, code: msg.code ?? 'invalid', message: msg.message, changes: refused.changes });
          emit('sync');
        }
        return;
      }
    }
  }

  const handler = {
    onOpen(attached) {
      link = attached;
      synced = false;
      fetching++;
      held = null;
      fetchFailed = false;
      ended = null;
      hello();
      refreshStatus();
    },
    onMessage: handle,
    onClose() {
      synced = false;
      fetching++;
      held = null;
      clearPresence();
      refreshStatus();
      setRelayed(false);
    },
    onClosed(info) {
      ended = { code: info.code, message: info.message };
      emit('closed', ended);
    }
  };

  function connect() {
    ended = null;
    if (!link) link = connection.attach(storeId, handler, { initial, singletons, registers, parents });
    connection.connect();
    refreshStatus();
  }

  function disconnect() {
    if (link) {
      link.detach();
      link = null;
    }
    if (ownsConnection) connection.close();
    synced = false;
    fetching++;
    held = null;
    clearPresence();
    refreshStatus();
  }

  /** Whether an edit not yet acknowledged writes at, beneath or above `path` ('c/id/field' or segments) */
  function isPending(path) {
    const target = Array.isArray(path) ? path.map(String) : registerKey(path);
    for (const op of outbox) {
      for (const change of op.changes) {
        const full = shape.full(change.c, change.id, change.path ?? []);
        if (within(full, target) || within(target, full)) return true;
      }
    }
    return false;
  }

  /** Records of a collection, and what happens to them batch by batch */
  function collection(name) {
    if (!shape.isCollection(name)) throw new TypeError(`"${name}" is not a collection of this store`);
    const ids = () => Object.keys(state[name]);
    return {
      add(record) {
        if (!isObj(record)) throw new TypeError('collection.add expects a plain object');
        const id = record.id != null ? String(record.id) : randomId();
        state[name][id] = { ...record, id };
        return id;
      },
      update(id, fields) {
        const record = state[name][id];
        if (!isObj(record)) return false;
        Object.assign(record, fields);
        return true;
      },
      remove(id) {
        if (!Object.hasOwn(state[name], id)) return false;
        delete state[name][id];
        return true;
      },
      get: id => state[name][id],
      has: id => Object.hasOwn(state[name], id),
      ids,
      all: () => ids().map(id => state[name][id]),
      watch: listener => watchRecords(name, listener)
    };
  }

  /** The records of `state[name]` a batch changed: insert, update (with previous) or remove */
  function watchRecords(name, listener) {
    return LazyWatch.on(state, (diff, inverse, meta) => {
      if (!diff || !Object.hasOwn(diff, name) || !isObj(diff[name])) return;
      const now = state[name];
      const was = inverse?.[name];
      const out = [];
      for (const id of Object.keys(diff[name])) {
        const current = Object.hasOwn(now, id) ? now[id] : undefined;
        const reverse = isObj(was) ? was[id] : undefined;
        let previous;
        if (reverse === null) previous = undefined;
        else if (current === undefined) previous = reverse === undefined ? undefined : clone(reverse);
        else if (isObj(reverse) && isObj(current)) {
          previous = LazyWatch.snapshot(current);
          LazyWatch.patchObject(previous, reverse);
        } else previous = reverse === undefined ? undefined : clone(reverse);
        if (current === undefined) {
          if (previous !== undefined) out.push({ type: 'remove', id, previous });
        } else {
          const record = plain(current);
          out.push(previous === undefined ? { type: 'insert', id, record } : { type: 'update', id, record, previous });
        }
      }
      if (out.length) listener(out, meta);
    });
  }

  function travel(method) {
    if (!undoManager) return false;
    const done = undoManager[method]();
    if (done !== false) emit('history', history());
    return done;
  }

  return {
    /** The state: the server's, with this client's pending edits on top. Read and write it like a plain object */
    state,
    /** The server's state as this client last heard it (plain; read only) */
    get base() { return base.state; },
    /** A record's clock in the base: { live, gone }, the times by path the server decides by */
    clockOf: (c, id) => base.clock(c, id),
    replicaId,
    store: storeId,
    connection,
    shape,
    get status() { return status(); },
    get pending() { return outbox.length + browserPending(); },
    stats() {
      return {
        pending: outbox.length + browserPending(),
        oldestPendingMs: outbox.length ? Math.max(0, clock.time - outbox[0].ts) : null,
        ackMs
      };
    },
    get version() { return known.v; },
    get epoch() { return known.epoch; },
    get relayed() { return relayed; },
    get presence() { return presence; },
    get peers() { return peers; },
    get wantsPresence() { return wantsPresence; },
    wantPresence(on) {
      on = Boolean(on);
      if (on === wantsPresence) return;
      wantsPresence = on;
      if (!on) clearPresence();
      if (linked() && !retryTimer) hello();
    },
    get shared() { return shared; },
    share(data) {
      if (data !== null && data !== undefined && JSON.stringify(data) === undefined) throw new TypeError('share expects a JSON value, or null');
      shared = data === null ? undefined : data;
      link?.send({ t: 'share', data: shared === undefined ? null : shared });
    },
    get closed() { return ended; },
    connect,
    disconnect,
    collection,
    isPending,
    /** An ordered list of records: a collection, or a keyed map at a path (see list.js) */
    list: (path, options) => createList(state, path, { position, ...options }),
    /** Hear every batch on `state`: (diff, inverse, meta), meta.origin 'remote' for the server's */
    watch: (listener, options) => LazyWatch.on(state, listener, options),
    on(event, fn) {
      if (!listeners[event]) throw new TypeError(`Unknown event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    undo: () => travel('undo'),
    redo: () => travel('redo'),
    get canUndo() { return undoManager ? undoManager.canUndo : false; },
    get canRedo() { return undoManager ? undoManager.canRedo : false; },
    checkpoint: () => undoManager?.checkpoint(),
    group: fn => (undoManager ? undoManager.group(fn) : fn()),
    clearHistory: () => { travel('clear'); },
    restored,
    dispose() {
      disconnect();
      clearTimeout(retryTimer);
      persistence.flush();
      persistence.dispose();
      storage.close?.();
      stopStatus();
      stopSync();
      undoManager?.dispose();
      LazyWatch.dispose(state);
    }
  };
}
