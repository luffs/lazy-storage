// relay/index.js - A relay on the LAN: the server's sockets passed through, and answered from a copy while it is away
//
// A relay sits between clients on a local network (a shop's displays, an
// office's screens) and the server they sync with, somewhere on the
// internet. It accepts their sockets as a server would and, while the
// server answers, is no more than a pipe: each client's socket is dialled
// through to the server with that client's own credentials (the host's
// `upstream` factory holds them; the relay never reads them), and every
// frame goes up and comes down as it is. The server sees each client as it
// would without the relay, judges every op as that client's, and alone
// acknowledges anything. On the way past, the relay builds a COPY of every
// store from what the server sends: the snapshots and deltas that answer
// hellos, and the patches that follow.
//
// When the server has not answered for `grace` (a failed dial, a socket
// that dropped or went silent; a server's deploy is shorter than that), or
// the host says it is away (`upstreamDown()`), the relay goes LOCAL: it
// answers the clients' hellos from its copies, applies their ops to the
// copy under last-writer-wins as the server would, and sends each applied
// op to the store's clients as a patch (its author's too, as a server
// does), so the clients on the LAN stay in step with each other. It
// acknowledges nothing: no `ack`, no
// `error` carrying an op's seq, no `lost`, and every answer's `seq` is 0,
// so every op stays in its author's outbox, persisted there, until the
// server has it. The relay keeps no queue of anyone's ops. When the server
// answers again (the relay dials it every `probeEvery` through a client's
// credentials, or the host says so with `upstreamUp()`) the relay closes
// every local socket, the clients reconnect, their sockets go through, and
// each client's outbox reaches the server in its own hello, under its own
// identity: what the server refuses comes back to that client as
// `rejected` and a snapshot, what lost to a newer write as a conflict.
//
// Epochs and versions. While a copy holds nothing but the server's state
// (nobody has edited it offline), a local answer carries the server's own
// epoch and version, so a client says hello with them and, once the server
// is back, is answered with a delta rather than a snapshot. The first op
// the relay applies makes the copy DIVERGED: its local sessions are told
// `closed` 'unavailable' and say hello again, and from then on every
// answer carries epoch null and versions of the relay's own counting. A
// client told epoch null asks the server for a snapshot when it is back,
// which reverts anything the server refused in whichever client saw it;
// with the server's epoch it would be sent a delta instead, and keep an
// edit the server never took. Epoch null also raises no `reset`, going or
// coming back. A client whose hello says it is ahead of the copy (it saw
// the server further along than the relay did) is not rolled back: it is
// answered with no patches, or with the offline edits alone.
//
// What the relay cannot do: judge an op as the server would (its access
// rules, its read-only paths, its validator) beyond what it can see, and a
// host's `validate`; so an edit the server will refuse shows on the LAN
// until the server is back and refuses it. The merge's clocks are known
// only from the patches seen since the last snapshot, so offline an older
// write may win where the server would have kept a newer one: the server
// decides again when it is back. A store the relay never saw, a client the
// server never answered, a replica id the server never saw under that
// client's credentials, and a client under an epoch the copy never held
// are not answered offline: the client waits, `connecting` on its own
// cache with its edits pending, which is what it does without a relay.
//
//   const relay = createRelay({ storage: fileCopies('./copies') });
//   const session = relay.accept({ send, close, key, upstream });   // per client socket
//   session.receive(message); ... session.close();
import { LazyWatch } from 'lazy-watch';
import { isTimestamp, compareTs } from '../core/hlc.js';
import { registerSet } from '../core/paths.js';
import { leaves, replacingRegisters } from '../core/model.js';
import { mergeOp } from '../core/merge.js';
import { ClockMap } from '../core/clocks.js';
import { isStoreId } from '../server/registry.js';
import { HELLO_OPS } from '../server/store.js';
import { presetJSON, toJSON, utf8Bytes } from '../server/wire.js';
import { memoryCopies } from './storage.js';

export { memoryCopies, fileCopies } from './storage.js';

const { Utils } = LazyWatch;
const { isPlainObject } = Utils;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
// setTimeout fires at once past this
const LONGEST_TIMER = 2 ** 31 - 1;

/** The close code a client's socket gets when it should reconnect: the relay changed modes */
const RESTART = 1012;
/** The server turned the socket away (see server/wire.js): passed down, and the credential forgotten */
const UNAUTHORIZED = 4401;
/** A server's closes that mean something of their own (normal, policy, too far behind, 4000s): passed down, and not taken for the server being away */
const deliberate = code => code === 1000 || code === 1008 || code === 1013 || (code >= 4000 && code < 5000);
/** What a server may send as a close code; the rest (1005, 1006, 1015, none) are reported, not sent */
const sendable = code => Number.isInteger(code) && (code === 1000 || (code >= 1001 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006) || (code >= 3000 && code < 5000));
/** A close reason as a WebSocket takes one: at most 123 bytes (the server's own is passed on, cut short) */
function closeReason(reason) {
  let text = String(reason ?? '');
  while (utf8Bytes(text) > 123) text = text.slice(0, -1);
  return text;
}
/** Closed codes after which the server will not answer this credential's hello on that store again */
const FINAL = new Set(['forbidden', 'evicted', 'unknown-store', 'invalid-store', 'replica-taken']);
/** Messages a client may send before its socket is dialled through, and their size (characters of JSON); more closes it */
const QUEUE_LIMIT = 1000;
const QUEUE_CHARS = 16 * 1024 * 1024;
/** Replica ids kept per credential and store: the clients of one device (its windows, each a client of its own) share a credential */
const REPLICAS_KEPT = 32;
/** Epochs a copy moved on from that it remembers: a client under one of them missed a restore the copy has */
const PAST_EPOCHS = 8;
/** Offline edits a diverged copy remembers, to give a client ahead of it; past this such a client gets a snapshot */
const LOCAL_LOG = 1000;
/** The most a local session may share, in bytes of JSON (the server's default) */
const MAX_SHARE = 4096;

/**
 * @param {Object} [options]
 * @param {Object} [options.storage] - where the copies are kept: memoryCopies()
 *   (the default) or fileCopies(dir), or an adapter of the same shape
 * @param {number} [options.grace=10000] - how long (ms) the server may fail
 *   to answer before the relay answers on its own; a deploy's restart is
 *   shorter than that, and is waited out
 * @param {number} [options.dialTimeout=4000] - a dial not open by then has failed
 * @param {(() => boolean|Promise<boolean>)|false} [options.probe] - while
 *   local, whether the server answers again; by default a client's
 *   `upstream` is dialled and closed again (one the server answered, a
 *   different one each time). false leaves it to upstreamUp()
 * @param {number} [options.probeEvery=5000]
 * @param {number|false} [options.keepalive=15000] - the relay pings every
 *   socket it passes through at this interval, and one the server has
 *   not answered for two of them is taken for the server being away
 * @param {number} [options.jitter=1000] - the most (ms) the relay waits,
 *   at random, before it sends its clients back to a server that is back,
 *   so that many relays do not bring every client back in one instant
 * @param {number} [options.maxSkew=300000] - an offline op stamped further
 *   ahead (ms) of the relay's reference time is not applied (it stays
 *   pending): the server's clock as last heard, carried on by the time
 *   elapsed since, or the relay's own wall clock if that is later
 * @param {number} [options.maxLeaves=10000] - the most leaves an offline op may touch
 * @param {(key: string, storeId: string) => boolean} [options.authorizeOffline]
 *   whether a client whose credential has fingerprint `key` is answered
 *   from the copy of `storeId` while the server is away; by default, when
 *   the server has answered that credential's hello on that store. The
 *   replica id must in any case be one the server saw with that credential
 * @param {(diff: Object, context: { key: string, replicaId: string, storeId: string, state: Object }) => boolean} [options.validate]
 *   an offline op's last gate: false or a throw leaves it unapplied (and
 *   pending, for the server to judge). `state` is the copy: read it only
 * @param {number} [options.saveDelay=1000] - copies are written at most
 *   this often (ms), and on flush() and close()
 * @param {number} [options.forgetAfter=2592000000] - a copy nobody has had
 *   open for this long (ms, default 30 days), and a credential the server
 *   has not answered for as long, are let go; Infinity keeps them
 * @param {(error: any) => void} [options.onError] - faults: storage that
 *   failed, a bug while handling a message; default console
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createRelay({
  storage = memoryCopies(),
  grace = 10_000,
  dialTimeout = 4_000,
  probe,
  probeEvery = 5_000,
  keepalive = 15_000,
  jitter = 1_000,
  maxSkew = 5 * MINUTE,
  maxLeaves = 10_000,
  authorizeOffline,
  validate,
  saveDelay = 1_000,
  forgetAfter = 30 * DAY,
  onError = err => console.error('lazy-storage relay:', err),
  now = Date.now
} = {}) {
  if (!storage || typeof storage.load !== 'function' || typeof storage.write !== 'function' || typeof storage.writeKnown !== 'function') {
    throw new TypeError('The relay\'s storage needs load, write and writeKnown (see memoryCopies)');
  }
  if (authorizeOffline !== undefined && typeof authorizeOffline !== 'function') throw new TypeError('authorizeOffline must be a function');
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('validate must be a function');
  if (probe !== undefined && probe !== false && typeof probe !== 'function') throw new TypeError('probe must be a function, or false');

  const copies = new Map();    // store id -> its copy (see newCopy)
  const known = new Map();     // credential key -> { at, stores: Map<storeId, Set<replicaId>> }: what the server answered
  const sockets = new Set();   // every client socket accepted and not closed
  const listeners = { mode: new Set(), copy: new Set(), error: new Set() };
  let mode = 'through';        // 'through' | 'local'
  let downSince = null;        // when the server first failed to answer, until it answers again
  let graceTimer = null;
  let probeTimer = null;
  let probing = false;
  let probeTurn = 0;           // whose credentials the default probe dials with next
  let switchTimer = null;      // the jittered way back to 'through'
  let saveTimer = null;
  let closed = false;
  const dirty = new Set();     // store ids whose copy changed since it was written
  let knownDirty = false;
  // The server's clock as last heard (its ms), and the monotonic time it was
  // heard at: the reference an offline op's stamp is judged against, which
  // a relay whose own clock is wrong (rebooted without a network) keeps
  let anchorTs = null;
  let anchorMono = 0;
  let anchorSaved = null;

  const report = err => {
    try {
      onError(err);
    } catch { /* a reporter that throws reports nothing more */ }
    for (const fn of listeners.error) {
      try { fn(err); } catch { /* ditto */ }
    }
  };
  const emit = (event, payload) => {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        if (event !== 'error') report(err);
      }
    }
  };
  /** Run a handler; a bug in it is reported, never thrown into the host's socket handler */
  const guard = fn => {
    try {
      return fn();
    } catch (err) {
      report(err);
    }
  };
  const later = (fn, ms) => {
    const timer = setTimeout(() => guard(fn), Math.max(0, Math.min(ms, LONGEST_TIMER)));
    if (typeof timer?.unref === 'function') timer.unref();
    return timer;
  };

  // --- Time ---------------------------------------------------------------------------------------

  /** The server's clock now, as far as the relay can tell: its last stamp carried on by the time since; null before any */
  const anchor = () => (anchorTs === null ? null : anchorTs + (performance.now() - anchorMono));
  /** What an offline op's stamp is judged against: never behind the relay's wall clock, nor behind the server's */
  const reference = () => Math.max(now(), anchor() ?? -Infinity);

  /** A stamp the server made (a snapshot's, a delta's, an ack's): its clock, from here on */
  function heardServer(ts) {
    if (!isTimestamp(ts)) return;
    if (anchorTs === null || ts[0] > anchor()) {
      anchorTs = ts[0];
      anchorMono = performance.now();
    }
  }

  /** The newest stamp a copy has seen: what a local answer tells a client's clock */
  function noteTs(copy, ts) {
    if (isTimestamp(ts) && compareTs(ts, copy.maxTs) > 0) copy.maxTs = ts;
  }

  // --- Copies -------------------------------------------------------------------------------------

  function newCopy(id) {
    return {
      id,
      state: {},
      v: 0,
      epoch: null,
      past: [],               // epochs it moved on from, oldest first (a restore at the server): a client under one is behind it
      registers: [],          // the server's register patterns, as its answers name them
      regs: registerSet([]),
      clocks: new ClockMap(), // per path, the stamp that won it, from the patches seen since the last snapshot
      seen: new Map(),        // replica id -> the last seq its state holds (applied offline, or the server's), so a resent op is applied once
      maxTs: null,
      presenceOn: false,      // the server's presence is on for the store (it sent some)
      users: new Map(),       // replica id -> { user, key }, as the server's presence showed each replica's user
      live: false,            // current, and following the server's patches through some socket
      diverged: false,        // holds offline edits the server has not had
      localV: 0,              // offline edits applied since it diverged: its versions count on from `v`
      localLog: [],           // those edits, for a client ahead of the copy; null past LOCAL_LOG
      usedAt: now(),
      json: null,             // the state as JSON, for local snapshots; null after a change
      followers: new Set(),   // through sockets whose session on the server follows the store
      locals: new Map()       // local socket -> its session on the store
    };
  }

  function setRegisters(copy, registers) {
    if (!Array.isArray(registers) || registers.some(r => typeof r !== 'string')) return;
    if (JSON.stringify(registers) === JSON.stringify(copy.registers)) return;
    copy.regs = registerSet(registers);
    copy.registers = [...registers];
  }

  /** The copy changed: written soon, and the host told */
  function changed(copy) {
    copy.json = null;
    persist(copy);
    emit('copy', copy.id);
  }

  const stateJSON = copy => (copy.json ??= JSON.stringify(copy.state));

  function docOf(copy) {
    return {
      format: 'lazy-storage/relay-copy',
      store: copy.id,
      state: copy.state,
      v: copy.v,
      epoch: copy.epoch,
      pastEpochs: copy.past,
      registers: copy.registers,
      clocks: [...copy.clocks],
      seen: [...copy.seen],
      maxTs: copy.maxTs,
      presenceOn: copy.presenceOn,
      users: [...copy.users],
      diverged: copy.diverged,
      localV: copy.localV,
      localLog: copy.localLog,
      usedAt: copy.usedAt
    };
  }

  /** A copy from its document, or null for one that is not: a copy loaded is never live, until the server's answers make it so */
  function copyOf(doc) {
    if (!isPlainObject(doc) || !isStoreId(doc.store) || !isPlainObject(doc.state) || !Number.isInteger(doc.v) || doc.v < 0) return null;
    if (typeof doc.epoch !== 'string') return null;
    const copy = newCopy(doc.store);
    copy.state = doc.state;
    copy.v = doc.v;
    copy.epoch = doc.epoch;
    if (Array.isArray(doc.pastEpochs)) copy.past = doc.pastEpochs.filter(e => typeof e === 'string' && e !== doc.epoch).slice(-PAST_EPOCHS);
    setRegisters(copy, doc.registers);
    for (const entry of Array.isArray(doc.clocks) ? doc.clocks : []) {
      if (Array.isArray(entry) && typeof entry[0] === 'string' && isPlainObject(entry[1]) && isTimestamp(entry[1].ts)) {
        copy.clocks.set(entry[0], entry[1].deleted ? { ts: entry[1].ts, deleted: true } : { ts: entry[1].ts });
      }
    }
    for (const entry of Array.isArray(doc.seen) ? doc.seen : []) {
      if (Array.isArray(entry) && typeof entry[0] === 'string' && Number.isInteger(entry[1])) copy.seen.set(entry[0], entry[1]);
    }
    copy.maxTs = isTimestamp(doc.maxTs) ? doc.maxTs : null;
    copy.presenceOn = doc.presenceOn === true;
    for (const entry of Array.isArray(doc.users) ? doc.users : []) {
      if (Array.isArray(entry) && typeof entry[0] === 'string' && isPlainObject(entry[1]) && typeof entry[1].key === 'string') copy.users.set(entry[0], { user: entry[1].user, key: entry[1].key });
    }
    copy.diverged = doc.diverged === true;
    copy.localV = Number.isInteger(doc.localV) && doc.localV >= 0 ? doc.localV : 0;
    copy.localLog = Array.isArray(doc.localLog) ? doc.localLog.filter(isPlainObject) : null;
    copy.usedAt = Number.isFinite(doc.usedAt) ? doc.usedAt : now();
    return copy;
  }

  function load() {
    let saved;
    try {
      saved = storage.load();
    } catch (err) {
      report(err);
      return;
    }
    if (!isPlainObject(saved)) return;
    for (const doc of Array.isArray(saved.copies) ? saved.copies : []) {
      const copy = guard(() => copyOf(doc));
      if (copy) copies.set(copy.id, copy);
    }
    const doc = saved.known;
    if (!isPlainObject(doc)) return;
    if (Number.isFinite(doc.centralTs)) {
      anchorTs = anchorSaved = doc.centralTs;
      anchorMono = performance.now();
    }
    for (const entry of Array.isArray(doc.keys) ? doc.keys : []) {
      if (!Array.isArray(entry) || typeof entry[0] !== 'string' || !isPlainObject(entry[1]) || !isPlainObject(entry[1].stores)) continue;
      const stores = new Map();
      for (const [id, record] of Object.entries(entry[1].stores)) {
        if (!isStoreId(id) || !isPlainObject(record) || !Array.isArray(record.replicaIds)) continue;
        const replicas = new Set(record.replicaIds.filter(r => typeof r === 'string' && r).slice(-REPLICAS_KEPT));
        if (replicas.size) stores.set(id, replicas);
      }
      if (stores.size) known.set(entry[0], { at: Number.isFinite(entry[1].at) ? entry[1].at : now(), stores });
    }
  }

  // --- Persistence --------------------------------------------------------------------------------

  function persist(copy) {
    dirty.add(copy.id);
    scheduleSave();
  }

  function persistKnown() {
    knownDirty = true;
    scheduleSave();
  }

  // At most once per saveDelay, never postponed by more changes: a store
  // that changes all the time is still written
  function scheduleSave(delay = saveDelay) {
    if (saveTimer || closed) return;
    saveTimer = later(() => {
      saveTimer = null;
      save();
    }, delay);
  }

  function knownDoc() {
    return {
      format: 'lazy-storage/relay-known',
      centralTs: anchor(),
      keys: [...known].map(([key, entry]) => [key, { at: entry.at, stores: Object.fromEntries([...entry.stores].map(([id, replicas]) => [id, { replicaIds: [...replicas] }])) }])
    };
  }

  /** Write what changed; what fails is reported and tried again a moment later */
  function save() {
    let failed = false;
    for (const id of [...dirty]) {
      dirty.delete(id);
      const copy = copies.get(id);
      try {
        if (copy) storage.write(id, docOf(copy));
        else storage.remove?.(id);
      } catch (err) {
        report(err);
        dirty.add(id);
        failed = true;
      }
    }
    const centralTs = anchor();
    if (knownDirty || (centralTs !== null && centralTs !== anchorSaved)) {
      try {
        storage.writeKnown(knownDoc());
        knownDirty = false;
        anchorSaved = centralTs;
      } catch (err) {
        report(err);
        failed = true;
      }
    }
    if (failed) scheduleSave(Math.max(saveDelay, 1000));
  }

  // --- Credentials the server answered -------------------------------------------------------------

  /**
   * The server answered a hello of this credential on this store, for this
   * replica. A credential may have several (a device's windows, each a
   * client of its own), up to REPLICAS_KEPT a store, the one answered
   * longest ago let go first
   */
  function recordKnown(key, storeId, replicaId) {
    if (typeof key !== 'string' || typeof replicaId !== 'string') return;
    let entry = known.get(key);
    if (!entry) known.set(key, (entry = { at: now(), stores: new Map() }));
    entry.at = now();
    let replicas = entry.stores.get(storeId);
    if (!replicas) entry.stores.set(storeId, (replicas = new Set()));
    const had = replicas.delete(replicaId);
    replicas.add(replicaId);
    if (had) return;   // the order changed, which is worth no write of its own
    if (replicas.size > REPLICAS_KEPT) replicas.delete(replicas.values().next().value);
    persistKnown();
  }

  function forgetStore(key, storeId) {
    const entry = known.get(key);
    if (!entry?.stores.delete(storeId)) return;
    if (entry.stores.size === 0) known.delete(key);
    persistKnown();
  }

  function forgetKey(key) {
    if (known.delete(key)) persistKnown();
  }

  const defaultAuthorize = (key, storeId) => known.get(key)?.stores.has(storeId) === true;

  /**
   * Whether a client may be answered from the copy: its credential is one
   * the server answered, `authorizeOffline` lets it have the store, and its
   * replica id is one the server saw with that credential (so nobody
   * speaks for someone else's replica, which the server would refuse)
   */
  function mayServe(key, storeId, replicaId) {
    const entry = typeof key === 'string' ? known.get(key) : undefined;
    if (!entry) return false;
    let owns = false;
    for (const replicas of entry.stores.values()) if (replicas.has(replicaId)) owns = true;
    if (!owns) return false;
    try {
      return Boolean((authorizeOffline ?? defaultAuthorize)(key, storeId));
    } catch (err) {
      report(err);
      return false;
    }
  }

  // --- Sockets --------------------------------------------------------------------------------------

  /** Send to a client; a transport that throws is reported, and the socket is left to its own close */
  const deliver = (sock, message) => {
    if (sock.state === 'closed') return;
    try {
      sock.send(message);
    } catch (err) {
      report(err);
    }
  };

  /** Close the upstream transport of a socket, disowned first so nothing it still says is heard */
  function closeUp(sock) {
    clearTimeout(sock.dialTimer);
    clearTimeout(sock.redialTimer);
    sock.dialTimer = sock.redialTimer = null;
    const t = sock.up;
    sock.up = null;
    if (!t) return;
    try {
      t.close();
    } catch (err) {
      report(err);
    }
  }

  /** The client's socket is gone: its sessions end here, and its session on the server with the upstream */
  function drop(sock) {
    if (sock.state === 'closed') return;
    for (const id of [...sock.locals.keys()]) leaveLocal(sock, id);
    unfollowAll(sock);
    closeUp(sock);
    sock.state = 'closed';
    sock.queue = [];
    sock.queued = 0;
    sockets.delete(sock);
  }

  /** Close a client's socket with a code: it reconnects on any but 4401 */
  function shut(sock, code, reason) {
    if (sock.state === 'closed') return;
    drop(sock);
    try {
      sock.close(sendable(code) ? code : RESTART, closeReason(reason));
    } catch (err) {
      report(err);
    }
  }

  function dial(sock) {
    if (sock.state !== 'dialing' || closed) return;
    let t;
    try {
      t = sock.upstream();
    } catch (err) {
      report(err);
      return dialFailed(sock);
    }
    if (!t || typeof t.send !== 'function' || typeof t.close !== 'function') {
      report(new TypeError('upstream must return a transport: { send, close, onopen, onmessage, onclose }'));
      return dialFailed(sock);
    }
    sock.up = t;
    sock.dialTimer = later(() => {
      if (sock.up !== t || sock.state !== 'dialing') return;
      closeUp(sock);
      dialFailed(sock);
    }, dialTimeout);
    t.onopen = () => guard(() => {
      if (sock.up !== t || sock.state !== 'dialing') return;
      clearTimeout(sock.dialTimer);
      sock.dialTimer = null;
      opened(sock);
    });
    t.onmessage = message => guard(() => {
      if (sock.up === t && sock.state === 'through') fromServer(sock, message);
    });
    t.onclose = info => guard(() => {
      if (sock.up !== t) return;
      sock.up = null;
      clearTimeout(sock.dialTimer);
      sock.dialTimer = null;
      upstreamClosed(sock, info);
    });
  }

  function dialFailed(sock) {
    if (sock.state !== 'dialing') return;
    failedFor(sock);
    // Answered from the copy from here, if the relay went local just now
    if (sock.state !== 'dialing') return;
    // Tried again after a pause, the client waiting with its hellos queued
    sock.redialDelay = Math.min(sock.redialDelay ? sock.redialDelay * 2 : 250, 2000);
    sock.redialTimer = later(() => {
      sock.redialTimer = null;
      dial(sock);
    }, sock.redialDelay);
  }

  /** Dialled through: what the client said meanwhile goes up, in order */
  function opened(sock) {
    sock.state = 'through';
    sock.redialDelay = 0;
    sock.pings = [];
    sock.heard = true;
    sock.silent = 0;
    serverAnswered();
    const queued = sock.queue;
    sock.queue = [];
    sock.queued = 0;
    for (const message of queued) {
      if (sock.state !== 'through') break;
      up(sock, message);
    }
  }

  /**
   * The server closed a socket (or it dropped). A socket never changes its
   * way to the server in place: it is closed, and the client reconnects,
   * which runs the host's upstream factory again with fresh credentials
   */
  function upstreamClosed(sock, info) {
    const code = info?.code;
    if (sock.state === 'dialing') {
      // Turned away at the door: the client hears it as it would from the server
      if (code === UNAUTHORIZED) {
        forgetKey(sock.key);
        return shut(sock, UNAUTHORIZED, info?.reason || 'Unauthorized');
      }
      return dialFailed(sock);
    }
    if (sock.state !== 'through') return;
    if (code === UNAUTHORIZED) forgetKey(sock.key);
    if (!deliberate(code)) failedFor(sock);
    shut(sock, code, info?.reason);
  }

  /**
   * The server failed to answer this socket. A credential the server never
   * answered says nothing of the server: nobody is answered offline on its
   * account, and a client whose own way there is broken (a request the
   * server's proxy turns away, a frame the server closes the socket for,
   * made so on purpose or not) would otherwise send every client local
   */
  function failedFor(sock) {
    if (sock.key !== null && known.has(sock.key)) serverFailed();
  }

  // --- Modes ----------------------------------------------------------------------------------------

  /**
   * The server failed to answer a socket: after `grace` of that, the relay
   * answers on its own. A failure is one socket's word (a dial, a close, a
   * silence), and one client's way to the server can break alone, so every
   * other socket passed through is asked at once (a ping): a server that
   * answers any of them before `grace` is out is there. With none passed
   * through, `grace` decides alone
   */
  function serverFailed() {
    if (closed || downSince !== null) return;
    downSince = now();
    clearTimeout(graceTimer);
    graceTimer = null;
    if (!(grace > 0)) return goLocal();
    for (const sock of [...sockets]) {
      if (sock.state !== 'through' || !sock.up || sock.pings.includes('relay')) continue;
      sock.pings.push('relay');
      try {
        sock.up.send({ t: 'ping' });
      } catch (err) {
        report(err);
      }
    }
    if (Number.isFinite(grace)) {
      graceTimer = later(() => {
        graceTimer = null;
        if (downSince !== null) goLocal();
      }, grace);
    }
  }

  function serverAnswered() {
    downSince = null;
    clearTimeout(graceTimer);
    graceTimer = null;
  }

  function goLocal() {
    if (closed) return;
    // A way back to 'through' that was waiting out its jitter is called off
    if (switchTimer) {
      clearTimeout(switchTimer);
      switchTimer = null;
      scheduleProbe();
    }
    if (mode === 'local') return;
    mode = 'local';
    downSince ??= now();
    emit('mode', mode);
    for (const sock of [...sockets]) {
      if (sock.state === 'dialing') toLocal(sock);
      // Every client is answered the same way: one still through (the server
      // answered it, not others) reconnects to be answered from the copy
      else if (sock.state === 'through') shut(sock, RESTART, 'The server is away; the relay answers');
    }
    scheduleProbe();
  }

  /** A socket that was waiting to be dialled through is answered from the copy, what it said first */
  function toLocal(sock) {
    closeUp(sock);
    sock.state = 'local';
    const queued = sock.queue;
    sock.queue = [];
    sock.queued = 0;
    for (const message of queued) {
      if (sock.state !== 'local') break;
      local(sock, message);
    }
  }

  /** The server answers again: every local socket is closed (after a moment's jitter), to reconnect through */
  function goThrough() {
    if (closed) return;
    serverAnswered();
    if (mode !== 'local' || switchTimer) return;
    clearTimeout(probeTimer);
    probeTimer = null;
    const run = () => {
      switchTimer = null;
      if (closed || mode !== 'local') return;
      mode = 'through';
      emit('mode', mode);
      for (const sock of [...sockets]) if (sock.state === 'local') shut(sock, RESTART, 'The server is back');
    };
    if (jitter > 0) switchTimer = later(run, Math.random() * jitter);
    else run();
  }

  function scheduleProbe() {
    clearTimeout(probeTimer);
    probeTimer = null;
    if (closed || mode !== 'local' || switchTimer || probe === false || !Number.isFinite(probeEvery)) return;
    probeTimer = later(runProbe, probeEvery);
  }

  async function runProbe() {
    probeTimer = null;
    if (closed || mode !== 'local' || probing) return;
    probing = true;
    let answered = false;
    try {
      answered = await (typeof probe === 'function' ? probe() : dialProbe());
    } catch (err) {
      report(err);
    }
    probing = false;
    if (closed || mode !== 'local') return;
    if (answered) goThrough();
    else scheduleProbe();
  }

  /**
   * The default probe: dial the server with a client's credentials, and
   * hang up once it answers. The credentials are the server's answered
   * ones where there are any, a different client's each time from the
   * newest back, so that one client whose way to the server is broken does
   * not keep the relay local
   */
  function dialProbe() {
    const open = [...sockets].reverse();
    const answered = open.filter(sock => sock.key !== null && known.has(sock.key));
    const pool = answered.length ? answered : open;
    const sock = pool[probeTurn++ % Math.max(pool.length, 1)];
    if (!sock) return false;
    return new Promise(resolve => {
      let t;
      try {
        t = sock.upstream();
      } catch {
        return resolve(false);
      }
      let done = false;
      const finish = answered => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        t.onopen = t.onclose = t.onmessage = null;
        try { t.close(); } catch { /* it is going anyway */ }
        resolve(answered);
      };
      const timer = later(() => finish(false), dialTimeout);
      t.onmessage = () => {};
      t.onopen = () => finish(true);
      t.onclose = () => finish(false);
    });
  }

  // --- Through: the client's frames up, the server's down, and the copy following ----------------

  /** A client message on its way up. Pings are counted, so the relay knows whose pong comes back; a hello may be rewritten */
  function up(sock, message) {
    const t = sock.up;
    if (!t) return;
    const id = message.store;
    if (message.t === 'ping') {
      sock.pings.push('client');
    } else if (message.t === 'hello' && isStoreId(id) && typeof message.replicaId === 'string') {
      const claimed = sock.claims.get(id);
      sock.claims.set(id, claimed === undefined || claimed === message.replicaId ? message.replicaId : null);
      if (!sock.stores.has(id)) sock.stores.set(id, { epoch: null });
      message = helloUp(message, copies.get(id));
    } else if (message.t === 'leave' && isStoreId(id)) {
      unfollow(sock, id);
    }
    t.send(message);
  }

  /**
   * A hello as the server should see it. `fetch` goes, so a large snapshot
   * comes inline and the copy sees it. `since` and `epoch` go too where the
   * copy could not use the delta they would bring: there is no copy, it
   * holds offline edits, or it is not following and the client is past it
   * or under another epoch. The server then answers with a snapshot, which
   * the copy takes. A hello left as it was keeps its frame as it came
   */
  function helloUp(message, copy) {
    const strip = !copy || copy.diverged ||
      (!copy.live && (message.epoch !== copy.epoch || !Number.isInteger(message.since) || message.since > copy.v));
    if (!strip && message.fetch === undefined) return message;
    const { fetch: _fetch, ...rest } = message;
    if (strip) {
      delete rest.since;
      delete rest.epoch;
    }
    return rest;
  }

  /** A server message: down to the client as it came, then read for the copy */
  function fromServer(sock, message) {
    sock.heard = true;
    // The server answers, whatever another client's dial found
    if (downSince !== null) serverAnswered();
    if (!isPlainObject(message)) return;
    if (message.t === 'pong') {
      if (sock.pings.shift() === 'relay') return;   // the relay's own keepalive
      return deliver(sock, message);
    }
    deliver(sock, message);
    const id = message.store;
    if (id === undefined) {
      // The socket turned away (not signed in, or no longer): the credential is not to be answered offline either
      if (message.t === 'closed') forgetKey(sock.key);
      return;
    }
    if (!isStoreId(id)) return;
    switch (message.t) {
      case 'snapshot': return tookSnapshot(sock, id, message);
      case 'delta': return tookDelta(sock, id, message);
      case 'patch': return tookPatch(sock, id, message);
      case 'ack': return heardServer(message.ts);
      case 'presence': return learnPresence(id, message);
      case 'closed':
        unfollow(sock, id);
        // The server's session on the store is over, and with it the replica it spoke for
        sock.claims.delete(id);
        if (FINAL.has(message.code)) forgetStore(sock.key, id);
        return;
    }
  }

  /**
   * The server answered this socket's hello on a store: the socket follows
   * it, and the credential is known to be let in with the replica its
   * hellos there claimed. The server answers only a hello for the replica
   * its session speaks for, and refuses the rest, but its answer does not
   * say which hello it answers: where the hellos named more than one
   * replica (a client does not; a forger would, hoping to have a replica
   * it was refused recorded as its own), none is recorded
   */
  function answered(sock, id, epoch) {
    const entry = sock.stores.get(id) ?? { epoch: null };
    sock.stores.set(id, entry);
    entry.epoch = epoch;
    copies.get(id)?.followers.add(sock);
    const replicaId = sock.claims.get(id);
    if (typeof replicaId === 'string') recordKnown(sock.key, id, replicaId);
  }

  function unfollow(sock, id) {
    if (!sock.stores.delete(id)) return;
    const copy = copies.get(id);
    if (!copy) return;
    copy.usedAt = now();
    if (copy.followers.delete(sock) && copy.followers.size === 0) copy.live = false;
  }

  function unfollowAll(sock) {
    for (const id of [...sock.stores.keys()]) unfollow(sock, id);
  }

  /**
   * A snapshot: the copy takes it unless it is already there. A copy under
   * the same epoch at the same version is current as it is and follows
   * from here; one further along keeps its own (the snapshot left the
   * server before patches the copy already has)
   */
  function tookSnapshot(sock, id, message) {
    heardServer(message.ts);
    if (typeof message.epoch !== 'string' || !Number.isInteger(message.v)) return;
    let copy = copies.get(id);
    if (!isPlainObject(message.state)) {
      // Pointed at the HTTP route: the copy does not see the state (the relay asks for it inline; an old server may not)
      if (copy) copy.live = false;
      return answered(sock, id, message.epoch);
    }
    if (!copy) copies.set(id, (copy = newCopy(id)));
    answered(sock, id, message.epoch);
    if (copy.epoch === message.epoch && !copy.diverged && message.v <= copy.v) {
      if (message.v === copy.v) copy.live = true;
      return;
    }
    copy.state = structuredClone(message.state);
    copy.v = message.v;
    if (copy.epoch !== message.epoch) {
      if (copy.epoch !== null) copy.past = [...copy.past.filter(e => e !== copy.epoch && e !== message.epoch), copy.epoch].slice(-PAST_EPOCHS);
      copy.epoch = message.epoch;
    }
    setRegisters(copy, message.registers);
    // What decided each path before is not known from a snapshot: offline, a
    // write to a path the patches since have not touched is taken as it comes
    copy.clocks = new ClockMap();
    // Nor which replicas' offline ops it holds: one whose author was not yet
    // back through is not in it, and is applied again when it comes again.
    // The answered replica's are known, up to the answer's seq
    copy.seen = new Map();
    const answering = sock.claims.get(id);
    if (typeof answering === 'string' && Number.isInteger(message.seq) && message.seq > 0) copy.seen.set(answering, message.seq);
    copy.diverged = false;
    copy.localV = 0;
    copy.localLog = [];
    copy.live = true;
    copy.usedAt = now();
    noteTs(copy, message.ts);
    changed(copy);
  }

  /**
   * A delta: the patches since the hello's `since`, then at most one
   * correction, there exactly when the answer says what the hello's ops
   * lost (`lost`), so `since` is the delta's `v` less its logged patches.
   * The copy applies what it has not had, from its own version on, and is
   * current at `v`; a copy behind the hello's `since` cannot, and waits for
   * the patches (or its next snapshot)
   */
  function tookDelta(sock, id, message) {
    heardServer(message.ts);
    if (typeof message.epoch !== 'string' || !Number.isInteger(message.v) || !Array.isArray(message.patches)) return;
    answered(sock, id, message.epoch);
    const copy = copies.get(id);
    if (!copy || copy.diverged) return;
    if (copy.epoch !== message.epoch) {
      copy.live = false;
      return;
    }
    const logged = message.patches.length - (Array.isArray(message.lost) && message.lost.length ? 1 : 0);
    const since = message.v - logged;
    if (logged < 0 || since < 0) {
      copy.live = false;
      return;
    }
    if (copy.v >= message.v) {
      if (copy.v === message.v) copy.live = true;
      return;
    }
    if (since > copy.v) return;
    for (const diff of message.patches.slice(copy.v - since)) if (isPlainObject(diff)) LazyWatch.patch(copy.state, diff);
    copy.v = message.v;
    copy.live = true;
    copy.usedAt = now();
    noteTs(copy, message.ts);
    changed(copy);
  }

  /**
   * A patch, which every socket on the store hears: applied once, when it is
   * the next version and the socket's session is under the copy's epoch
   * (a socket's patches are of the epoch its last answer named). Its stamp
   * goes into the clocks, for judging offline ops later. A version skipped
   * means the copy no longer follows
   */
  function tookPatch(sock, id, message) {
    const copy = copies.get(id);
    if (!copy || !copy.live || copy.diverged || sock.stores.get(id)?.epoch !== copy.epoch) return;
    if (!Number.isInteger(message.v) || message.v <= copy.v) return;
    if (message.v !== copy.v + 1 || !isPlainObject(message.diff)) {
      copy.live = false;
      return;
    }
    try {
      if (isTimestamp(message.ts)) mergeOp(copy.clocks, message.ts, message.diff, copy.regs, { authority: true });
      LazyWatch.patch(copy.state, message.diff);
    } catch (err) {
      copy.live = false;
      return report(err);
    }
    copy.v = message.v;
    noteTs(copy, message.ts);
    changed(copy);
  }

  /**
   * Presence: the store has it on, and whose each replica is, as the server
   * shows its users, for the peers the relay lists while the server is away
   */
  function learnPresence(id, message) {
    const copy = copies.get(id);
    if (!copy) return;
    let learned = !copy.presenceOn;
    copy.presenceOn = true;
    for (const peer of [...(Array.isArray(message.peers) ? message.peers : []), ...(Array.isArray(message.joined) ? message.joined : [])]) {
      if (!isPlainObject(peer) || typeof peer.replicaId !== 'string' || typeof peer.key !== 'string') continue;
      const had = copy.users.get(peer.replicaId);
      if (had?.key === peer.key && JSON.stringify(had.user) === JSON.stringify(peer.user)) continue;
      copy.users.set(peer.replicaId, { user: peer.user, key: peer.key });
      learned = true;
    }
    if (learned) persist(copy);
  }

  // --- Local: answered from the copy ------------------------------------------------------------------

  function local(sock, message) {
    if (!isPlainObject(message)) return;
    if (message.t === 'ping') return deliver(sock, { t: 'pong' });
    const id = message.store;
    if (!isStoreId(id)) return;
    switch (message.t) {
      case 'hello': return localHello(sock, id, message);
      case 'op': return localOp(sock, id, message.op);
      case 'share': return localShare(sock, id, message.data);
      case 'leave': return leaveLocal(sock, id);
    }
  }

  /**
   * A hello, answered from the copy (see the header), or not at all: a
   * store without a copy, or a client the server has not let in with that
   * replica, waits for the server
   */
  function localHello(sock, id, message) {
    const copy = copies.get(id);
    const { replicaId } = message;
    if (!copy || typeof replicaId !== 'string' || !replicaId || !mayServe(sock.key, id, replicaId)) return;
    // A client under an epoch the copy never held saw the server where the
    // relay did not (a restore the copy missed, a relay restarted from an
    // older file): it may be the newer of the two, so it is not rolled back
    // to the copy, and waits for the server. One under an epoch the copy
    // moved on from is behind it, and is answered as the server would
    if (typeof message.epoch === 'string' && message.epoch !== copy.epoch && !copy.past.includes(message.epoch)) return;
    let session = sock.locals.get(id);
    if (session && session.replicaId !== replicaId) return;   // a session speaks for one replica
    const joined = !session;
    if (!session) {
      session = { replicaId, presence: true, shared: undefined, sharedJson: undefined, answered: false };
      sock.locals.set(id, session);
      copy.locals.set(sock, session);
    }
    session.presence = message.presence !== false;
    copy.usedAt = now();
    const ops = Array.isArray(message.ops) ? message.ops.slice(0, HELLO_OPS) : [];
    for (const op of ops) {
      const applied = applyOffline(sock, copy, session, op);
      if (!applied) continue;
      // The first edit of the copy: every other client on it says hello again, to answers under epoch null
      if (!copy.diverged) diverge(copy, sock);
      recordLocal(copy, applied);
      broadcast(copy, sock, { t: 'patch', store: id, diff: applied, ts: op.ts, v: copy.v + copy.localV });
    }
    const shared = message.share !== undefined && copy.presenceOn && setShare(session, message.share);
    deliver(sock, answerOf(copy, message, ops.length >= HELLO_OPS));
    deliver(sock, { t: 'relay', store: id, status: 'local' });
    session.answered = true;
    if (!copy.presenceOn) return;
    if (session.presence) deliver(sock, { t: 'presence', store: id, peers: peersOf(copy) });
    if (joined) tellPresence(copy, sock, { joined: [peerOf(copy, session)] });
    else if (shared) tellPresence(copy, null, { shared: [shareOf(session)] });
  }

  /**
   * The answer to a local hello. While the copy is the server's own state,
   * it says so with the server's epoch and version; a hello that carried a
   * full load of ops is answered with epoch null all the same, as a relay's
   * answer, since a client told the server's epoch sends the rest of its
   * outbox in another hello rather than live, and would be answered alike
   * for ever. A client at or past the copy under the same epoch keeps its
   * state: it gets no patches, or the offline edits alone
   */
  function answerOf(copy, message, cut) {
    const own = !copy.diverged && !cut;
    const since = Number.isInteger(message.since) ? message.since : null;
    const base = { store: copy.id, ts: copy.maxTs, seq: 0, registers: copy.registers };
    if (message.epoch === copy.epoch && since !== null && since >= copy.v) {
      if (!copy.diverged) return { t: 'delta', ...base, patches: [], v: since, epoch: own ? copy.epoch : null };
      if (copy.localLog) return { t: 'delta', ...base, patches: copy.localLog, v: copy.v + copy.localV, epoch: null };
    }
    return own ? snapshotOf(copy, copy.v, copy.epoch) : snapshotOf(copy, copy.v + copy.localV, null);
  }

  /** A snapshot of the copy, its JSON assembled around the copy's cached encoding: a burst of hellos pays for one */
  function snapshotOf(copy, v, epoch) {
    const json = stateJSON(copy);
    const message = { t: 'snapshot', store: copy.id, ts: copy.maxTs, seq: 0, registers: copy.registers, v, epoch };
    let decoded;
    Object.defineProperty(message, 'state', { enumerable: true, configurable: true, get: () => (decoded ??= JSON.parse(json)) });
    const head = `{"t":"snapshot","store":${JSON.stringify(copy.id)},"state":`;
    const tail = `,"ts":${JSON.stringify(copy.maxTs)},"seq":0,"registers":${JSON.stringify(copy.registers)},"v":${v},"epoch":${JSON.stringify(epoch)}}`;
    return presetJSON(message, head + json + tail);
  }

  /** A live op: applied, and sent to the store's clients as a patch; never acknowledged */
  function localOp(sock, id, op) {
    const copy = copies.get(id);
    const session = sock.locals.get(id);
    if (!copy || !session?.answered) return;
    const applied = applyOffline(sock, copy, session, op);
    if (!applied) return;
    if (!copy.diverged) {
      // Its author included: the op stays in its outbox, and its hello brings it back, applied once
      diverge(copy, null);
      recordLocal(copy, applied);
      return;
    }
    recordLocal(copy, applied);
    // To its author too, as a server does: an edit of another's that reached
    // it after its own, and lost to it here, is put back to its own
    broadcast(copy, null, { t: 'patch', store: id, diff: applied, ts: op.ts, v: copy.v + copy.localV });
  }

  /**
   * An offline op through the gates, merged into the copy: the diff that
   * changed it, or null. Nothing that fails a gate is applied, and nothing
   * is said: the op stays pending with its author, for the server to judge
   */
  function applyOffline(sock, copy, session, op) {
    if (!isPlainObject(op) || op.replicaId !== session.replicaId || !Number.isInteger(op.seq) || op.seq < 1) return null;
    if (!isTimestamp(op.ts) || op.ts[2] !== op.replicaId || !isPlainObject(op.diff)) return null;
    // Once per seq: a client resends its whole outbox after every answer
    if (op.seq <= (copy.seen.get(op.replicaId) ?? 0)) return null;
    let entries;
    try {
      entries = leaves(op.diff, copy.regs);
    } catch {
      return null;
    }
    if (entries.length > maxLeaves) return null;
    // A clock running ahead would pull every client's along, and the server would refuse them all
    if (op.ts[0] > reference() + maxSkew) return null;
    // The server refuses deleting a container of its skeleton, which the relay does not know: none is deleted offline
    if (entries.some(([path, value]) => value === null && path.length === 1 && Object.hasOwn(copy.state, path[0]) && isPlainObject(copy.state[path[0]]))) return null;
    if (validate) {
      try {
        if (validate(op.diff, { key: sock.key, replicaId: op.replicaId, storeId: copy.id, state: copy.state }) === false) return null;
      } catch {
        return null;
      }
    }
    copy.seen.set(op.replicaId, op.seq);
    noteTs(copy, op.ts);
    const { accepted } = mergeOp(copy.clocks, op.ts, op.diff, copy.regs);
    if (!accepted) {
      persist(copy);
      return null;
    }
    // A register the op writes becomes its new value whole, as on the server
    const applied = replacingRegisters(accepted, copy.regs, copy.state);
    LazyWatch.patch(copy.state, applied);
    return applied;
  }

  /** The copy takes its first offline edit: its local sessions (but `spare`, whose hello is being answered) start over */
  function diverge(copy, spare) {
    copy.diverged = true;
    copy.live = false;
    copy.localV = 0;
    copy.localLog = [];
    for (const [sock, session] of [...copy.locals]) {
      if (sock === spare) continue;
      sock.locals.delete(copy.id);
      copy.locals.delete(sock);
      if (session.answered) deliver(sock, { t: 'closed', store: copy.id, code: 'unavailable', message: 'The relay\'s copy took an edit; say hello again' });
    }
  }

  function recordLocal(copy, applied) {
    copy.localV++;
    if (copy.localLog && copy.localLog.length < LOCAL_LOG) copy.localLog.push(applied);
    else copy.localLog = null;
    changed(copy);
  }

  /** To every answered session on the copy but `except`'s, encoded once */
  function broadcast(copy, except, message) {
    let encoded = false;
    for (const [sock, session] of copy.locals) {
      if (sock === except || !session.answered) continue;
      if (!encoded) {
        toJSON(message);
        encoded = true;
      }
      deliver(sock, message);
    }
  }

  // --- Local presence: the sessions on the relay, as the server's presence showed their users ------

  function peerOf(copy, session) {
    const peer = { replicaId: session.replicaId };
    const who = copy.users.get(session.replicaId);
    if (who) {
      peer.user = who.user;
      peer.key = who.key;
    }
    if (session.shared !== undefined) peer.data = session.shared;
    return peer;
  }

  const peersOf = copy => [...copy.locals.values()].filter(s => s.answered).map(s => peerOf(copy, s));
  const shareOf = session => (session.shared === undefined ? { replicaId: session.replicaId } : { replicaId: session.replicaId, data: session.shared });

  /** A presence change to the store's sessions that want presence, but `except` */
  function tellPresence(copy, except, change) {
    broadcastPresence(copy, except, { t: 'presence', store: copy.id, ...change });
  }

  function broadcastPresence(copy, except, message) {
    for (const [sock, session] of copy.locals) if (sock !== except && session.answered && session.presence) deliver(sock, message);
  }

  /** What a session shares: JSON within the server's default limit, null clearing; true when it changed */
  function setShare(session, data) {
    let json;
    if (data !== null && data !== undefined) {
      try {
        json = JSON.stringify(data);
      } catch {
        return false;
      }
      if (json === undefined || json.length > MAX_SHARE) return false;
    }
    if (json === session.sharedJson) return false;
    session.shared = json === undefined ? undefined : JSON.parse(json);
    session.sharedJson = json;
    return true;
  }

  function localShare(sock, id, data) {
    const copy = copies.get(id);
    const session = sock.locals.get(id);
    if (!copy?.presenceOn || !session?.answered) return;
    if (setShare(session, data)) tellPresence(copy, null, { shared: [shareOf(session)] });
  }

  function leaveLocal(sock, id) {
    const session = sock.locals.get(id);
    if (!session) return;
    sock.locals.delete(id);
    const copy = copies.get(id);
    if (!copy) return;
    copy.locals.delete(sock);
    copy.usedAt = now();
    if (session.answered && copy.presenceOn) tellPresence(copy, sock, { left: [session.replicaId] });
  }

  // --- Upkeep ----------------------------------------------------------------------------------------

  // The relay's own pings on every socket it passes through, sharing the
  // way with the client's: a pong for one of its own is not passed down,
  // and a server silent for two intervals is taken for gone, the socket
  // closed and the client reconnecting
  const keepaliveTimer = keepalive ? setInterval(() => guard(() => {
    for (const sock of [...sockets]) {
      if (sock.state !== 'through' || !sock.up) continue;
      sock.silent = sock.heard ? 0 : sock.silent + 1;
      if (sock.silent >= 2) {
        failedFor(sock);
        shut(sock, RESTART, 'The server stopped answering');
        continue;
      }
      sock.heard = false;
      sock.pings.push('relay');
      sock.up.send({ t: 'ping' });
    }
  }), keepalive) : null;
  if (typeof keepaliveTimer?.unref === 'function') keepaliveTimer.unref();

  /** Let go of the copies nobody has had open, and the credentials not answered, for `forgetAfter` */
  function sweep() {
    if (!Number.isFinite(forgetAfter)) return;
    const horizon = now() - forgetAfter;
    for (const copy of [...copies.values()]) {
      if (copy.followers.size || copy.locals.size || copy.usedAt >= horizon) continue;
      copies.delete(copy.id);
      dirty.add(copy.id);
    }
    // A credential with a socket open is in use, however long ago the server last answered a hello of it
    const inUse = new Set([...sockets].map(sock => sock.key));
    for (const [key, entry] of [...known]) {
      if (inUse.has(key)) entry.at = now();
      if (entry.at >= horizon) continue;
      known.delete(key);
      knownDirty = true;
    }
    save();
  }
  const sweeper = Number.isFinite(forgetAfter) ? setInterval(() => guard(sweep), HOUR) : null;
  if (typeof sweeper?.unref === 'function') sweeper.unref();

  // The server's clock, carried on, is written every minute even when
  // nothing else is: a relay restarted in an outage on a machine with no
  // clock of its own to keep (a Raspberry Pi) starts from where it was a
  // minute before, not from its last edit, maybe an hour back, which would
  // hold back every client's offline edits as running ahead
  const anchorWriter = setInterval(() => guard(() => { if (anchorTs !== null) scheduleSave(); }), MINUTE);
  if (typeof anchorWriter?.unref === 'function') anchorWriter.unref();

  load();

  return {
    /**
     * A client's socket, accepted: `send(message)` delivers to it (encoding
     * or copying the message before it returns), `close(code, reason)`
     * closes it, `key` is a fingerprint of its credential (the relay never
     * reads the credential itself), and `upstream` a transport factory that
     * dials the server with that credential (see client/transport.js).
     * Feed the returned session every message the client sends, parsed,
     * and close it when the socket closes
     * @returns {{ receive(message: Object): void, close(): void, readonly state: string }}
     */
    accept({ send, close, key, upstream } = {}) {
      if (typeof send !== 'function' || typeof close !== 'function') throw new TypeError('accept needs send and close functions');
      if (typeof upstream !== 'function') throw new TypeError('accept needs an upstream transport factory');
      const sock = {
        send,
        close,
        key: typeof key === 'string' && key ? key : null,
        upstream,
        state: mode === 'local' ? 'local' : 'dialing',
        queue: [],            // what the client said while its socket was being dialled through
        queued: 0,            // and its size, in characters of JSON
        up: null,             // the transport to the server, while dialling or through
        dialTimer: null,
        redialTimer: null,
        redialDelay: 0,
        pings: [],            // through: whose each ping on its way up is ('client' | 'relay')
        heard: true,          // through: the server has said something since the last keepalive
        silent: 0,
        stores: new Map(),    // through: store id -> { epoch of its last answer }
        claims: new Map(),    // through: store id -> the replica its hellos there named, or null once they named two (see answered)
        locals: new Map(),    // local: store id -> its session
        openedAt: performance.now()
      };
      sockets.add(sock);
      if (closed) shut(sock, 1001, 'The relay is shutting down');
      else if (sock.state === 'dialing') dial(sock);
      return {
        receive: message => guard(() => {
          if (!isPlainObject(message)) return;
          if (sock.state === 'through') return up(sock, message);
          if (sock.state === 'local') return local(sock, message);
          if (sock.state !== 'dialing') return;
          if (message.t === 'ping') return deliver(sock, { t: 'pong' });
          let size;
          try {
            size = toJSON(message).length;
          } catch {
            return;   // what cannot be encoded cannot go up either
          }
          if (sock.queue.length >= QUEUE_LIMIT || sock.queued + size > QUEUE_CHARS) return shut(sock, 1008, 'Too much said before the server answered');
          sock.queue.push(message);
          sock.queued += size;
        }),
        close: () => guard(() => drop(sock)),
        get state() { return sock.state; }
      };
    },

    /** 'through' while the server answers, 'local' while the relay answers from its copies */
    get mode() { return mode; },

    /** 'mode' (the new mode), 'copy' (the id of a store whose copy changed), 'error' (a fault). Returns an unsubscribe function */
    on(event, fn) {
      if (!listeners[event]) throw new TypeError(`Unknown relay event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },

    /**
     * What the relay holds of a store, or null: the copy's state (a copy of
     * it), the server's version and epoch it is at, whether it follows the
     * server now (`live`), and whether it holds offline edits the server
     * has not had (`diverged`, `local` of them)
     */
    copy(storeId) {
      const copy = copies.get(storeId);
      if (!copy) return null;
      return { state: JSON.parse(stateJSON(copy)), v: copy.v, epoch: copy.epoch, live: copy.live, diverged: copy.diverged, local: copy.localV };
    },

    /** The host's word that the server answers again (its own link is back): the relay goes through without waiting for its probe */
    upstreamUp() {
      guard(() => {
        goThrough();
        // Sockets waiting to be dialled through try now
        for (const sock of [...sockets]) {
          if (sock.state !== 'dialing' || !sock.redialTimer) continue;
          clearTimeout(sock.redialTimer);
          sock.redialTimer = null;
          dial(sock);
        }
      });
    },

    /** The host's word that the server is away: the relay answers on its own now, without waiting out `grace`; its probe still finds the server back */
    upstreamDown() {
      guard(() => {
        downSince ??= now();
        goLocal();
      });
    },

    /** How the relay is doing, for a status line */
    stats() {
      const counts = { dialing: 0, through: 0, local: 0 };
      for (const sock of sockets) counts[sock.state]++;
      let live = 0;
      let diverged = 0;
      for (const copy of copies.values()) {
        if (copy.live) live++;
        if (copy.diverged) diverged++;
      }
      return { mode, downSince, sockets: counts, copies: copies.size, live, diverged, credentials: known.size };
    },

    /** Every client socket: its credential's fingerprint, its state, the stores it has open, and for how long (ms) */
    sockets() {
      const at = performance.now();
      return [...sockets].map(sock => ({
        key: sock.key,
        state: sock.state,
        stores: [...(sock.state === 'local' ? sock.locals.keys() : sock.stores.keys())],
        openMs: at - sock.openedAt
      }));
    },

    /** Write what changed now */
    flush() {
      clearTimeout(saveTimer);
      saveTimer = null;
      save();
    },

    /** Let go of the copies and credentials unused for `forgetAfter`; runs every hour on its own */
    sweep: () => guard(sweep),

    /** Close every socket (code 1001), stop, and write what changed */
    close() {
      if (closed) return;
      closed = true;
      for (const timer of [graceTimer, probeTimer, switchTimer, saveTimer]) clearTimeout(timer);
      clearInterval(keepaliveTimer);
      clearInterval(sweeper);
      clearInterval(anchorWriter);
      saveTimer = null;
      for (const sock of [...sockets]) shut(sock, 1001, 'The relay is shutting down');
      save();
    }
  };
}
