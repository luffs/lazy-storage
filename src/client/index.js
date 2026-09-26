// client/index.js - A synced, offline-capable mirror of a store
//
// The client wraps a LazyWatch instance. Local writes are ordinary
// property writes on `state`; each emitted batch becomes an op with a
// hybrid-logical-clock timestamp, is appended to a persisted outbox, and
// is sent when online. A new op also takes over from whatever older
// pending ops wrote at or under its paths, writes that could never win
// again (see supersede below), so typing into one field keeps one op
// pending rather than one per keystroke. Everything from the server is
// applied tagged { origin: 'remote' }, which is how the listener tells
// its own edits from the server's, and how the undo manager keeps remote
// changes out of history.
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
import { registerSet, valueAt } from '../core/paths.js';
import { leaves, expandRegisters, rebuild } from '../core/model.js';
import { randomId } from '../core/ids.js';
import { memoryOutbox } from './storage.js';
import { createConnection } from './connection.js';
import { createPersistence, isRowAdapter } from './persistence.js';
import { createList } from './list.js';
import { createFacade, wireInitial } from './facade.js';

const { Utils } = LazyWatch;
const REMOTE = { origin: 'remote' };
const SNAPSHOT = { origin: 'remote', snapshot: true };
const RESTORE = { origin: 'restore' };
const HISTORY = new Set(['undo', 'redo']);
const EVENTS = ['status', 'error', 'sync', 'closed', 'presence', 'peers', 'history', 'conflict', 'rejected', 'reset', 'relay'];
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
 *   (arrays of anything as one value); must match the server's, which
 *   reports a mismatch as an error with code 'registers-mismatch' on every
 *   snapshot
 * @param {Array<string|string[]>} [options.lists] - list paths (register
 *   syntax) that `state` presents as plain arrays of records, in position
 *   order, while the wire keeps keyed maps with positions (see facade.js).
 *   With lists declared, `db.state` is that view and `db.wire` the synced
 *   state underneath; `initial` may use arrays at those paths
 * @param {string} [options.position='pos'] - the position field on list records
 * @param {string} [options.replicaId] - defaults to the persisted one, else random
 * @param {Object} [options.storage] - outbox and state-cache persistence
 *   (default: memory). An adapter whose load() returns a promise needs
 *   openClient()
 * @param {boolean} [options.mirror=false] - a client that only reads (a
 *   server-owned store, a dashboard): `cache`, `undo` and `presence` default
 *   to false, each still settable on its own
 * @param {boolean} [options.cache=true] - persist the state with the
 *   outbox and start from it on the next load; false keeps only the outbox
 * @param {number} [options.cacheDelay=1000] - with a document adapter, the
 *   state is written once changes have settled this long (ms), at least
 *   every ten of these under steady traffic, and when the page is hidden
 * @param {boolean} [options.undo=true] - attach an undo manager
 * @param {number} [options.undoLimit=100]
 * @param {{min: number, max: number}|false} [options.reconnect] - retry
 *   backoff for an owned connection; false disables automatic reconnects
 * @param {boolean} [options.presence=true] - false asks the server (in
 *   the hello) not to send presence to this client, which then never has
 *   peers; a mirror that never reads them spares every change's message.
 *   It may still share
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
  lists = [],
  position = 'pos',
  replicaId,
  storage,
  // A follower that only reads: no undo manager, no state cache, no
  // presence. Each of the three can still be set on its own
  mirror = false,
  cache = !mirror,
  cacheDelay = 1000,
  undo = !mirror,
  undoLimit = 100,
  reconnect = { min: 500, max: 10_000 },
  presence: wantsPresence = !mirror,   // `presence` below is the list
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
  // An adapter written to an older contract fails here, once, rather than on every op
  if (rows) {
    for (const method of ['load', 'replace', 'saveOp', 'removeOp', 'dropOps']) {
      if (typeof storage[method] !== 'function') throw new TypeError(`A row storage adapter needs ${method} (load, commit, replace, saveOp, removeOp, dropOps)`);
    }
  }
  if (lists.length) {
    const listed = registerSet(lists);
    for (const pattern of listed.patterns) if (regs.matches(pattern)) throw new TypeError(`"${pattern.join('/')}" cannot be both a list and a register`);
    initial = wireInitial(initial, lists, position);
  }

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
      ? (rows ? rebuild(initial, saved.rows, regs) : { ...structuredClone(initial), ...structuredClone(saved.state) })
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
  // For stats(): when each pending op last went out (seq -> performance.now()),
  // the last ack's round trip, and how old the last remote patch was on arrival
  const sentAt = new Map();
  let ackMs = null;
  let remoteAgeMs = null;
  /** Send one op live, noting when */
  const sendOp = op => {
    sentAt.set(op.seq, performance.now());
    link.send({ t: 'op', op });
  };
  const clock = createClock(replicaId, () => wall() + offset);
  const restamped = new Map(); // seq -> how many times the op was re-stamped after a skew refusal
  const listeners = Object.fromEntries(EVENTS.map(e => [e, new Set()]));
  let link = null;        // attachment to the connection while connected
  let synced = false;     // this store's snapshot has landed on this socket
  let fetching = 0;       // snapshot fetches begun; bumped to disown one in flight (see fetchSnapshot)
  let held = null;        // what the socket delivered while a snapshot was being fetched, replayed on top of it
  let fetchFailed = false; // a fetch failed on this socket: the next hello asks for the snapshot inline
  let lastStatus = 'offline';
  let presence = [];      // distinct users with a live session on this store, derived from the peers
  let peerMap = new Map(); // replicaId -> peer: every live session on this store, with what it shares
  let peers = [];         // the same as a list
  let shared;             // what this client shares with them (undefined: nothing)
  let ended = null;       // { code, message } after the server closed this store for us
  let retryTimer = null;  // a hello scheduled after a rate-limit refusal
  let throttled = false;  // refused for the rate: nothing goes live until that hello, which resends in order
  let helloCut = false;   // the last hello left part of the outbox out: the next hello carries it
  let gapped = false;     // a patch skipped a version: a catch-up hello is out, and patches wait for it
  let relayed = false;    // the answers come from a relay's copy while the server is away (see 'relay' below)

  const persistence = createPersistence({
    storage,
    cache,
    regs,
    state: () => state,
    ops: () => outbox,
    meta: () => ({ replicaId, seq, version: known.v, epoch: known.epoch }),
    onError: err => emit('error', err),
    cacheDelay
  });

  // Own batches (meta undefined) and undo/redo replays are history; remote
  // and rejected batches are not
  const undoManager = undo
    ? LazyWatch.createUndoManager(state, { limit: undoLimit, record: meta => !meta || HISTORY.has(meta.origin) })
    : null;

  // With lists declared, the app works on a view with arrays (facade.js)
  // that follows the wire state and feeds its edits back into it
  const facade = lists.length ? createFacade({ wire: state, lists, position }) : null;
  const exposed = facade ? facade.view : state;

  function emit(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        console.error(`Error in lazy-storage "${event}" listener:`, err);
      }
    }
  }

  // Whether this client can send: it has a link and the connection is up.
  // What the app is told is a step further: a shared connection (shared.js)
  // stands in for the browser's socket, so the link may be up while the
  // socket is down, and then this client is offline for the app though it
  // still hands its edits to the browser's replica
  const linked = () => Boolean(link) && connection.status !== 'offline';
  const status = () => {
    if (!linked()) return 'offline';
    if (!synced) return 'connecting';
    // 'online' the moment a snapshot or delta is applied: `state` is
    // current from here, while the batch that carries it to `watch`
    // listeners follows on the microtask (and a snapshot equal to what
    // the client had produces none), so "the store is current" is the
    // status event rather than the first watch
    const upstream = connection.upstream;
    if (upstream === undefined || upstream === 'online') return 'online';
    return upstream === 'offline' ? 'offline' : 'connecting';
  };
  /** The replica's unsent ops behind a shared connection count as this client's pending */
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

  /** The users behind the peers, distinct by the key the server groups them with, in order of arrival */
  function usersOf() {
    const seen = new Map();
    for (const peer of peerMap.values()) if (peer.key !== undefined && !seen.has(peer.key)) seen.set(peer.key, peer.user);
    return [...seen.values()];
  }

  const isPeer = peer => Utils.isPlainObject(peer) && typeof peer.replicaId === 'string';
  const sameJSON = (a, b) => JSON.stringify(a) === JSON.stringify(b);

  /**
   * Presence arrived: the whole list (`peers`, right after our hello), or
   * what changed since (`left`, `joined`, `shared`, applied in that
   * order). What is already known as it is, our own join among it, is
   * no change
   */
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
        const peer = Utils.isPlainObject(share) ? peerMap.get(share.replicaId) : undefined;
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

  /**
   * Whether this store is answered by a relay on its own (lazy-storage/relay,
   * while the server is away) rather than by the server: the relay says so
   * after each answer it makes, and an answer from the server itself (it
   * carries the server's epoch) or a socket gone says otherwise
   */
  function setRelayed(next) {
    if (relayed === next) return;
    relayed = next;
    emit('relay', relayed);
  }

  /** Offline, or the store ended for us: nobody is there to see */
  function clearPresence() {
    peerMap = new Map();
    if (peers.length) {
      peers = [];
      emit('peers', peers);
    }
    setPresence([]);
  }

  /** Drop acknowledged ops (seq and below) from the outbox and its persistence */
  function acknowledge(upTo) {
    const at = sentAt.get(upTo);
    if (at !== undefined) ackMs = performance.now() - at;
    for (const seq of sentAt.keys()) if (seq <= upTo) sentAt.delete(seq);
    outbox = outbox.filter(op => op.seq > upTo);
    for (const s of restamped.keys()) if (s <= upTo) restamped.delete(s);
    persistence.drop(upTo);
  }

  /**
   * Take out of a diff what it holds at `path`, a leaf or a whole
   * subtree, and the containers that leaves empty. A record's `id` stays:
   * it is what makes the write a record write, which alone re-adds a
   * deleted record (see core/merge.js), and the fields beside it count
   * on that. True when something went.
   */
  function prune(node, path, i = 0) {
    const key = path[i];
    if (!Utils.isPlainObject(node) || !Object.hasOwn(node, key)) return false;
    if (i === path.length - 1) {
      if (key === 'id') return false;
      delete node[key];
      return true;
    }
    const child = node[key];
    if (!prune(child, path, i + 1)) return false;
    if (Object.keys(child).length === 0) delete node[key];
    return true;
  }

  /**
   * A new op takes over from what the pending ops before it wrote at or
   * under the paths it writes. Under last-writer-wins per leaf those older
   * writes can never decide a value again, the newer stamp beats them
   * wherever it lands, so they leave the outbox and the server never
   * sees them: typing into one field keeps one op pending instead of one
   * per keystroke, and deleting a record drops its pending edits. Nothing
   * is re-stamped, an older write to another path keeps its own time, so
   * the merge decides exactly as if every op had been sent; a sent op
   * still awaiting its ack is pruned like any other, since the server
   * either merged it whole already or will get the newer op instead.
   * Returns the ops changed and the seqs of those emptied, for persistence.
   */
  function supersede(op, paths) {
    const changed = [];
    const removed = [];
    outbox = outbox.filter(older => {
      if (compareTs(older.ts, op.ts) >= 0) return true;
      let touched = false;
      for (const path of paths) if (prune(older.diff, path)) touched = true;
      if (!touched) return true;
      if (Object.keys(older.diff).length > 0) {
        changed.push(older);
        return true;
      }
      removed.push(older.seq);
      restamped.delete(older.seq);
      return false;
    });
    return { changed, removed };
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
    let paths;
    try {
      expanded = expandRegisters(diff, regs, state);
      paths = leaves(expanded, regs).map(([path]) => path);
    } catch (err) {
      if (inverse) LazyWatch.patch(state, inverse, { origin: 'rejected', error: err });
      emit('error', err);
      emit('rejected', { seq: null, code: err.code ?? 'invalid', message: err.message, diff: LazyWatch.Utils.deepClone(diff) });
      return;
    }
    const op = { replicaId, seq: ++seq, ts: clock.now(), diff: expanded };
    const superseded = supersede(op, paths);
    outbox.push(op);
    persistence.op(op, superseded);
    persistence.batch(expanded, meta);
    if (linked() && synced && !throttled) sendOp(op);
    emit('sync');
    // The undo manager listens ahead of this, so the batch is in history by now
    if (undoManager) emit('history', history());
  });

  /** What undo and redo can do right now, for the 'history' event */
  const history = () => ({ canUndo: undoManager.canUndo, canRedo: undoManager.canRedo });

  /** Run an undo-manager method, then report where history stands: its stacks move after the batch it emits */
  function travel(method) {
    if (!undoManager) return false;
    const done = undoManager[method]();
    if (done !== false) emit('history', history());
    return done;
  }

  /**
   * The (re)connect message: the outbox (its first HELLO_LIMIT ops) and,
   * unless a full snapshot is wanted, where our knowledge of the store ends
   */
  function hello({ full = false } = {}) {
    const message = { t: 'hello', replicaId, ops: outbox.slice(0, HELLO_LIMIT) };
    const at = performance.now();
    for (const op of message.ops) sentAt.set(op.seq, at);
    helloCut = outbox.length > HELLO_LIMIT;
    throttled = false;
    if (!full) Object.assign(message, { since: known.v, epoch: known.epoch });
    if (shared !== undefined) message.share = shared;
    if (!wantsPresence) message.presence = false;
    // A large snapshot can be fetched over HTTP instead, when the connection can fetch (see fetchSnapshot)
    if (typeof connection.fetch === 'function' && !fetchFailed) message.fetch = true;
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
    // A new epoch where this client held a version of the old one: the
    // store's storage started over (a backup restored, a store moved or
    // wiped), and what the client held may be gone. The state it showed
    // is kept for the event, read before the snapshot overwrites it. A
    // version of 0 held nothing (a store that never committed mints an
    // epoch per load), and a relay's epoch is null: its replica reports
    const reset = typeof msg.epoch === 'string' && known.epoch !== null && known.v > 0 && msg.epoch !== known.epoch
      ? { epoch: msg.epoch, previous: { epoch: known.epoch, version: known.v, state: LazyWatch.snapshot(state) } }
      : null;
    // The version first, so what persistence writes for the batches below
    // is stamped with the version the state is about to reflect
    if (Number.isInteger(msg.v)) known = { epoch: typeof msg.epoch === 'string' ? msg.epoch : null, v: msg.v };
    gapped = false;
    const lost = lostOps(msg.lost);
    acknowledge(msg.seq);
    applyServerState();
    // Read what won before the ops still pending are laid back on top
    const conflicts = lost.map(conflictOf);
    // What the hello left out goes in the next hello, not as live ops: a
    // backlog sent live would run into the rate limit, and the store stays
    // 'connecting' until it has everything (live sends wait on `synced`,
    // so nothing overtakes the backlog). Only a server's answer, under its
    // epoch, acknowledges the hello's ops: a relay's (epoch null) leaves
    // them pending, and another hello would carry the same ops again and
    // be answered alike, for ever, so the rest go out live instead
    const more = helloCut && outbox.length > 0 && typeof msg.epoch === 'string';
    for (const op of outbox) {
      LazyWatch.patch(state, op.diff, REMOTE);
      if (!more) sendOp(op);
    }
    persistence.version();
    if (more) {
      hello();
      emit('sync');
      setRelayed(false);
      if (reset) emit('reset', reset);
      for (const conflict of conflicts) emit('conflict', conflict);
      return;
    }
    synced = true;
    refreshStatus();
    emit('sync');
    // The server's own answer: whatever relay passed it on, it answers on its own no longer
    if (typeof msg.epoch === 'string') setRelayed(false);
    if (reset) emit('reset', reset);
    for (const conflict of conflicts) emit('conflict', conflict);
  }

  /**
   * The ops a server says lost leaves, as [op, paths]: `list` is
   * [{ seq, paths }], read before the ops leave the outbox
   */
  function lostOps(list) {
    const found = [];
    for (const entry of Array.isArray(list) ? list : []) {
      if (!Utils.isPlainObject(entry) || !Array.isArray(entry.paths)) continue;
      const op = outbox.find(o => o.seq === entry.seq);
      const paths = entry.paths.filter(path => Array.isArray(path) && path.every(seg => typeof seg === 'string'));
      if (op && paths.length) found.push([op, paths]);
    }
    return found;
  }

  /** A lost op as the app hears it: per path, what this client wrote and what won (null: gone) */
  function conflictOf([op, paths]) {
    const plainAt = (root, path) => {
      const value = valueAt(root, path);
      if (value === undefined) return null;
      return LazyWatch.isProxy(value) ? LazyWatch.snapshot(value) : LazyWatch.Utils.deepClone(value);
    };
    return { seq: op.seq, lost: paths.map(path => ({ path, mine: plainAt(op.diff, path), theirs: plainAt(state, path) })) };
  }

  function reportLost(lost) {
    for (const entry of lost) emit('conflict', conflictOf(entry));
  }

  /**
   * Whether an edit of this client's not yet acknowledged writes at
   * `path`, under it, or over it (a record deleted or replaced whole
   * counts for each of its fields). Paths are the synced state's: under a
   * list declared as an array, a record is addressed by its id
   */
  function isPending(path) {
    const segments = Array.isArray(path) ? path.map(String) : String(path).split('/').filter(Boolean);
    return outbox.some(op => {
      let node = op.diff;
      for (const segment of segments) {
        if (!Utils.isPlainObject(node)) return true;     // written whole above the path
        if (!Object.hasOwn(node, segment)) return false;
        node = node[segment];
      }
      return true;
    });
  }

  /**
   * The server pointed at where to fetch the snapshot rather than sending
   * it (a large one, and this connection can fetch): fetch it, holding
   * back what the socket delivers meanwhile, then land it as the snapshot
   * would have landed and replay what was held, minus the patches the
   * fetched state already includes. A fetch that fails is reported and
   * asks again, for the snapshot inline this time
   */
  function fetchSnapshot(msg) {
    const attempt = ++fetching;
    const queue = held = [];
    const current = () => attempt === fetching && held === queue;
    Promise.resolve()
      .then(() => connection.fetch(msg.fetch))
      .then(async response => {
        if (!response.ok) throw new Error(`the server answered ${response.status}`);
        const body = await response.json();
        if (!Utils.isPlainObject(body) || !Utils.isPlainObject(body.state) || !Number.isInteger(body.v)) throw new Error('the response is not a snapshot');
        return body;
      })
      .then(
        body => {
          if (!current()) return;
          held = null;
          caughtUp({ ...msg, v: body.v, epoch: typeof body.epoch === 'string' ? body.epoch : msg.epoch }, () => LazyWatch.overwrite(state, body.state, SNAPSHOT));
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
      .catch(err => emit('error', err));   // a fault landing the snapshot: reported, not swallowed
  }

  function handle(msg) {
    // While a snapshot is being fetched what arrives waits for it (see fetchSnapshot); a closed ends the wait
    if (held && msg.t !== 'closed') return void held.push(msg);
    switch (msg.t) {
      case 'snapshot':
        if (typeof msg.fetch === 'string' && msg.state === undefined) return fetchSnapshot(msg);
        return caughtUp(msg, () => LazyWatch.overwrite(state, msg.state, SNAPSHOT));
      case 'delta':
        return caughtUp(msg, () => {
          for (const diff of Array.isArray(msg.patches) ? msg.patches : []) LazyWatch.patch(state, diff, REMOTE);
        });
      case 'patch':
        clock.receive(msg.ts);
        // What a catch-up is on its way for, it brings
        if (gapped) return;
        // A server's versions count one per patch: one skipped means a
        // patch never arrived (a socket that dropped it under
        // backpressure), and applying on regardless would leave this
        // replica wrong for good. Catch up from the last version held
        // instead. (A relay's versions, epoch null, do not count so)
        if (Number.isInteger(msg.v) && known.epoch !== null && synced && msg.v > known.v + 1) {
          gapped = true;
          synced = false;
          refreshStatus();
          hello();
          return;
        }
        if (Number.isInteger(msg.v)) known.v = msg.v;
        // How old another replica's write is by the time it arrives, on the
        // clock the server keeps this one to (see 'clock-skew')
        if (Array.isArray(msg.ts) && msg.ts[2] !== replicaId && Number.isFinite(msg.ts[0])) remoteAgeMs = Math.max(0, wall() + offset - msg.ts[0]);
        LazyWatch.patch(state, msg.diff, REMOTE);
        return;
      case 'ack': {
        clock.receive(msg.ts);
        const lost = lostOps([{ seq: msg.seq, paths: msg.lost }]);
        acknowledge(msg.seq);
        if (msg.correction) LazyWatch.patch(state, msg.correction, REMOTE);
        reportLost(lost);
        emit('sync');
        return;
      }
      case 'conflict':
      case 'rejected':
      case 'reset': {
        // A relay passing on the browser replica's (see shared.js); a server sends none of these
        const { t, store: _store, ...payload } = msg;
        emit(t, payload);
        return;
      }
      case 'relay':
        // A relay says whether it answers this store from its own copy
        // ('local', the server away) or not; a server never sends it, and a
        // client that predates it ignores it as it does any unknown message
        setRelayed(msg.status === 'local');
        return;
      case 'presence':
        applyPresence(msg);
        return;
      case 'closed': {
        if (msg.code === 'unavailable') {
          // Not final: the server unloaded the store under us (a registry
          // released it) and loads it again on the next hello. Nothing
          // pending is dropped; the hello resends it
          synced = false;
          fetching++;
          held = null;
          clearPresence();
          refreshStatus();
          if (!retryTimer) {
            retryTimer = setTimeout(() => {
              retryTimer = null;
              if (linked()) hello();
            }, 250 + Math.random() * 750);
            if (typeof retryTimer?.unref === 'function') retryTimer.unref();
          }
          return;
        }
        // Final for this store: evicted, forbidden, or unknown. Detach and
        // stay offline until connect() is called again
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
        if (msg.code === 'clock-skew' && correctClock(msg)) return;
        const err = new Error(msg.message);
        if (msg.code) err.code = msg.code;
        emit('error', err);
        if (msg.code === 'rate-limited') {
          // Nothing is dropped: the op stays in the outbox, nothing more goes
          // live (the server refuses it anyway until the hello), and a hello
          // after the server's retryAfter resends everything still pending,
          // in order. A refused hello (no seq) is retried the same way
          if (Number.isInteger(msg.seq)) throttled = true;
          if (retryTimer) return;
          retryTimer = setTimeout(() => {
            retryTimer = null;
            if (linked()) hello();
          }, Number.isInteger(msg.retryAfter) ? msg.retryAfter : 1000);
          if (typeof retryTimer?.unref === 'function') retryTimer.unref();
          return;
        }
        // The server refused an op we already applied locally: drop it and
        // resync from a snapshot so this replica falls back in line (when a
        // hello is in flight its snapshot is already on the way)
        if (Number.isInteger(msg.seq)) {
          const refused = outbox.find(op => op.seq === msg.seq);
          if (refused) emit('rejected', { seq: msg.seq, code: msg.code ?? 'invalid', message: msg.message, diff: LazyWatch.Utils.deepClone(refused.diff) });
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
    if (synced && !throttled) for (const op of behind) sendOp(op);
    return true;
  }

  const handler = {
    // Receives the link because on an already-open connection this fires
    // inside attach(), before connect() has stored the return value
    onOpen(attached) {
      link = attached;
      synced = false;
      // A new socket: a fetch begun on the last one is disowned, and a fetch that failed there is tried again
      fetching++;
      held = null;
      fetchFailed = false;
      // A socket the server had turned away is back (another client on
      // the connection reconnected, if not this one)
      ended = null;
      hello();
      refreshStatus();
    },
    onMessage: handle,
    onClose() {
      synced = false;
      gapped = false;
      fetching++;
      held = null;
      clearPresence();
      refreshStatus();
      setRelayed(false);
    },
    /** The server turned the whole socket away (not signed in): final for this store too, until connect(); onClose came first */
    onClosed(info) {
      ended = { code: info.code, message: info.message };
      emit('closed', ended);
    }
  };

  function connect() {
    ended = null;
    // What this client declared, for a connection that builds a replica of the store elsewhere (shared.js)
    if (!link) link = connection.attach(storeId, handler, { initial, registers });
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
    fetching++;
    held = null;
    clearPresence();
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
    /** The mirrored state: read and write it like a plain object (with lists declared, the view with arrays) */
    state: exposed,
    /** The synced state underneath: keyed maps with positions; the same object as `state` without lists */
    wire: state,
    replicaId,
    store: storeId,
    connection,
    get status() { return status(); },
    /** Unacknowledged local ops */
    get pending() { return outbox.length + browserPending(); },
    /**
     * How the client is doing, for a "saving…" indicator or a status line:
     * `pending` ops, how long the oldest has waited (`oldestPendingMs`),
     * the last ack's round trip (`ackMs`), and how old the last remote
     * patch was when it arrived (`remoteAgeMs`); null where nothing has
     * happened yet
     */
    stats() {
      return {
        pending: outbox.length + browserPending(),
        oldestPendingMs: outbox.length ? Math.max(0, wall() + offset - outbox[0].ts[0]) : null,
        ackMs,
        remoteAgeMs
      };
    },
    /** The store version this client has seen everything up to */
    get version() { return known.v; },
    /**
     * True while a relay answers this store from its own copy, the server
     * being away (lazy-storage/relay): edits reach the others behind the
     * relay but stay pending until the server has them
     */
    get relayed() { return relayed; },
    /** Distinct users with a live session on this store (empty while offline) */
    get presence() { return presence; },
    /** Every live session on this store, `{ replicaId, user, key, data }`, this client's own included (by `replicaId`); `key` is what presence groups users by */
    get peers() { return peers; },
    /** What this client shares with its peers, or undefined */
    get shared() { return shared; },
    /**
     * Share a small JSON value with everyone on the store: it rides on
     * presence, is never written, and lives as long as the session (a
     * hello carries it, so a reconnect restores it). null clears it
     */
    share(data) {
      if (data !== null && data !== undefined && JSON.stringify(data) === undefined) throw new TypeError('share expects a JSON value, or null');
      shared = data === null ? undefined : data;
      link?.send({ t: 'share', data: shared === undefined ? null : shared });
    },
    /** Why the server closed this store for us ({ code, message }), or null */
    get closed() { return ended; },
    connect,
    disconnect,
    collection,
    isPending,
    /** An ordered list of records under `path` on the wire: a keyed map with a position on each record (see list.js) */
    list: (path, options) => createList(state, path, { position, ...options }),
    /** Subscribe to changes of `state` (a LazyWatch listener; meta.origin tells remote from local) */
    watch: (listener, options) => LazyWatch.on(exposed, listener, options),
    /**
     * Lifecycle events: 'status' (string), 'error' (Error, with `code` when
     * the server gave one), 'sync' (outbox changed), 'presence' (users),
     * 'peers' (every session with what it shares), 'closed' ({ code,
     * message }: the server ended this store, or the socket, for us),
     * 'history' ({ canUndo, canRedo }: after a local batch, an undo, a
     * redo, or clearHistory), 'relay' (boolean: `relayed` changed)
     */
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
    /** True when this client started from a cached state rather than `initial` */
    restored,
    dispose() {
      disconnect();
      clearTimeout(retryTimer);
      persistence.flush();
      persistence.dispose();
      stopStatus();
      stopSync();
      facade?.dispose();
      undoManager?.dispose();
      LazyWatch.dispose(state);
    }
  };
}
