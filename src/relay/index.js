// relay/index.js - A relay between clients and the server: their sockets passed through or their stores fanned out, and answered from a copy while the server is away
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
// Fan-out. Given `link`, a way to the server's relay route under the
// relay's own credential (see src/server/relays.js), the relay reads each
// store ONCE for every client behind it instead of once per client: a
// client's socket (one accepted with a `credential`) is not dialled
// through. For each store a client says hello to, the relay asks the
// server whether that client may read it (`vouch`, with the client's
// credential, judged as the client's own socket would be), opens its own
// session on the store (let in while some client of it is), keeps the copy
// by it, and answers the client from the copy under the server's epoch and
// version: a delta out of the copy's log of the server's last patches
// where it reaches back far enough, else a snapshot. Every patch the
// server sends then goes to each of the store's clients in version order
// (each has a cursor, the version it was brought to), encoded once.
// Writes never go through the relay's session: a client's ops, and a
// hello that carries some, go up a socket of the client's own, dialled
// with its credentials as a pass-through socket is, as a write-only
// session (hello `follow: false`), so the server judges and acknowledges
// each op as its author's, and the relay could not write as anyone if it
// tried. An ack is held until the client has heard every patch up to the
// ack's version, so it lands where it would on a direct socket (and one
// heard after later patches has its correction taken from the copy, as it
// stands then). Presence is the server's: each client is listed there as a
// peer, and the relay passes the store's presence on. When the server
// takes a client's access away, the relay hears `revoked` and ends that
// client's session as the server would. The link failing is the server
// failing (see `grace`); the relay then answers from its copies as it
// always has, and on the way back tells every client that sent an op in
// the meantime, or holds a copy's offline edits, to say hello again. A
// server that answers the clients' own dials but not the link (one without
// relays, a proxy that refuses the route) has the relay pass every socket
// through as before, until the link answers again.
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
import { registerSet, correctionAt } from '../core/paths.js';
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
/** The close code for a client whose session ran out or was ended by the server: it reconnects and is judged afresh (see server/wire.js) */
const REAUTHENTICATE = 4001;
/** Closed codes after which the server will not answer this credential's hello on that store again */
const FINAL = new Set(['forbidden', 'evicted', 'unknown-store', 'invalid-store', 'replica-taken']);
/** Fan-out: the server's patches a copy keeps, to answer a client a little behind with a delta */
const LOG = 1000;
/** Fan-out: vouches out on the link at once; more wait their turn, so a relay's restart does not flood the server */
const VOUCHES_AT_ONCE = 50;
/** Fan-out: how long a credential the server turned away is turned away here, without asking again */
const TURNED_AWAY_MS = 10_000;
/** Fan-out: the replica id the relay's own sessions say hello with (they write nothing, and claim no replica) */
const RELAY_REPLICA = 'relay';
/** Fan-out: a link the server turned away is tried again this much later (or at the host's word, upstreamUp) */
const REFUSED_RETRY = 5 * MINUTE;
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
 * @param {() => Object} [options.link] - fan-out (see the header): a
 *   transport factory that dials the server's relay route (see
 *   server/relays.js) with the relay's own credential. Without it every
 *   socket is passed through
 * @param {number} [options.linger=20000] - fan-out: how long (ms) the
 *   relay's session on a store outlives the store's last client
 * @param {number} [options.writeIdle=30000] - fan-out: a client's socket up
 *   for its edits is closed once it has had nothing to wait for this long
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
  link,
  linger = 20_000,
  writeIdle = 30_000,
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
  if (link !== undefined && typeof link !== 'function') throw new TypeError('link must be a transport factory');
  const fanOut = typeof link === 'function';

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
  let knownReplicas = null;    // every replica id `known` holds (see ownedBy); null after `known` changed
  // The server's clock as last heard (its ms), and the monotonic time it was
  // heard at: the reference an offline op's stamp is judged against, which
  // a relay whose own clock is wrong (rebooted without a network) keeps
  let anchorTs = null;
  let anchorMono = 0;
  let anchorSaved = null;
  let anchorDue = false;       // the minute is out (see anchorWriter): the clock goes with the next save
  // Fan-out (see the header and the section below)
  let linkUp = null;             // the link's transport, while dialling or open
  let linkState = 'down';        // 'down' | 'dialing' | 'open'
  let linkDialTimer = null;
  let linkRedialTimer = null;
  let linkGraceTimer = null;
  let linkDelay = 0;
  let linkHeard = true;          // the server said something on the link since the last keepalive
  let linkSilent = 0;
  let linkBroken = false;        // the server answers clients but not the link: sockets pass through meanwhile
  let linkProbed = false;        // the server answered a probe while the link was down: a link that fails again means the link alone is broken
  const grantsOut = new Map();   // grant id -> { sock, id }: the client session the relay vouched for under that name
  let grantSeq = 0;
  let socketSeq = 0;
  let vouchQueue = [];           // vouches waiting their turn (see VOUCHES_AT_ONCE)
  const vouchesOut = new Set();  // grant ids of vouches the server has not answered
  const vouchTimers = new Map(); // grant id -> the timer that lets its vouch go unanswered
  const turnedAway = new Map();  // credential key -> until when the server's 'unauthorized' holds here

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
      presenceOn: false,      // the server's presence is on for the store (its `admitted` said so, or it sent some)
      users: new Map(),       // replica id -> { user, key }, the relay's own clients' users as the server shows them (see ownedBy)
      live: false,            // current, and following the server's patches through some socket
      diverged: false,        // holds offline edits the server has not had
      localV: 0,              // offline edits applied since it diverged: its versions count on from `v`
      localLog: [],           // those edits, for a client ahead of the copy; null past LOCAL_LOG
      usedAt: now(),
      json: null,             // the state as JSON, for local snapshots; null after a change
      followers: new Set(),   // through sockets whose session on the server follows the store
      locals: new Map(),      // local socket -> its session on the store
      // Fan-out
      shared: null,           // the relay's own session on the store: null, 'hello' (said), 'open' (answered)
      presence: 'off',        // whether that session hears presence: 'off', 'asked' (its hello said so), 'on' (the whole list came)
      log: [],                // the server's last patches, as the messages that carried them: contiguous, ending at `v`
      peers: new Map(),       // replica id -> peer, the server's presence as the relay's session hears it
      lingerTimer: null,
      sharedRetry: null
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
    // The users of replicas that are no longer the relay's go with the write
    const own = ownedBy(copy);
    for (const replicaId of [...copy.users.keys()]) if (!own(replicaId)) copy.users.delete(replicaId);
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
    knownReplicas = null;
  }

  // --- Persistence --------------------------------------------------------------------------------

  function persist(copy) {
    dirty.add(copy.id);
    scheduleSave();
  }

  function persistKnown() {
    knownDirty = true;
    knownReplicas = null;
    scheduleSave();
  }

  /**
   * Whose users a copy keeps: the replicas the relay may answer offline
   * (those `known` holds, under any credential and store, as mayServe
   * reads them) and those on the store through it now. The rest of the
   * store's peers are no use to it: offline it lists its own clients alone
   */
  function ownedBy(copy) {
    if (!knownReplicas) {
      knownReplicas = new Set();
      for (const { stores } of known.values()) for (const replicas of stores.values()) for (const replicaId of replicas) knownReplicas.add(replicaId);
    }
    // Those on the store now, looked for only when `known` does not have the replica (most the relay answered, it has)
    let here = null;
    const onStore = () => {
      here = new Set();
      for (const session of copy.locals.values()) if (session.replicaId) here.add(session.replicaId);
      // A socket passed through claims its replica as its hello goes up, before the answer: the
      // presence that names it may come on another socket, or the link, first
      for (const sock of sockets) {
        const replicaId = sock.claims.get(copy.id);
        if (replicaId) here.add(replicaId);
      }
      return here;
    };
    return replicaId => knownReplicas.has(replicaId) || (here ?? onStore()).has(replicaId);
  }

  /** A client's user as the server shows it, kept for the offline presence of its replica; written when it changed */
  function keepUser(copy, peer) {
    if (!isPlainObject(peer) || typeof peer.replicaId !== 'string' || typeof peer.key !== 'string') return false;
    const had = copy.users.get(peer.replicaId);
    if (had?.key === peer.key && JSON.stringify(had.user) === JSON.stringify(peer.user)) return false;
    copy.users.set(peer.replicaId, { user: peer.user, key: peer.key });
    return true;
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
    // The server's clock carried on moves with every call: written once a
    // minute (anchorWriter), and at once when it jumped further than that
    // (the first stamp heard after a restart), not at every save
    const centralTs = anchor();
    const clockDue = centralTs !== null && centralTs !== anchorSaved && (anchorDue || anchorSaved === null || centralTs - anchorSaved > MINUTE + saveDelay);
    if (knownDirty || clockDue) {
      try {
        storage.writeKnown(knownDoc());
        knownDirty = false;
        anchorDue = false;
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
    closeWrite(sock);
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
    // With fan-out the link says whether the server is there (see linkLost); one client's way failing does not
    if (closed || downSince !== null || (fanOut && !linkBroken && linkState === 'open')) return;
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
      else if (sock.state === 'fan') fanToLocal(sock);
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
      for (const sock of [...sockets]) {
        if (sock.state === 'local') shut(sock, RESTART, 'The server is back');
        else if (sock.state === 'fan') fanToThrough(sock);
      }
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
    if (!answered) return scheduleProbe();
    if (fanOut && !linkBroken && linkState !== 'open') return kickLink();
    goThrough();
  }

  /**
   * The server answers, and the link is not up: it is dialled now, and if
   * that fails too it is the link alone that is broken, and the clients
   * are passed through meanwhile (see linkLost)
   */
  function kickLink() {
    linkProbed = !linkBroken;
    if (linkState === 'refused') linkState = 'down';
    if (linkState !== 'down') return;
    clearTimeout(linkRedialTimer);
    linkRedialTimer = null;
    dialLink();
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
    if (copy.followers.delete(sock) && copy.followers.size === 0 && copy.shared !== 'open') copy.live = false;
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
      if (copy && !copy.shared) copy.live = false;
      return answered(sock, id, message.epoch);
    }
    if (!copy) copies.set(id, (copy = newCopy(id)));
    answered(sock, id, message.epoch);
    // The relay's own session feeds the copy while it is there (said hello to, or answered): an answer to a socket
    // passed through beside it, which may be ahead of the session's patches, would make the copy jump past its clients
    if (copy.shared) return;
    takeSnapshot(copy, message, sock.claims.get(id));
  }

  /** A snapshot of the server's, from whichever way it came; `answering`, the replica whose hello it answers, if one */
  function takeSnapshot(copy, message, answering) {
    if (copy.epoch === message.epoch && !copy.diverged && message.v <= copy.v) {
      if (message.v === copy.v) copy.live = true;
      fanFollowUp(copy);
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
    if (typeof answering === 'string' && Number.isInteger(message.seq) && message.seq > 0) copy.seen.set(answering, message.seq);
    copy.diverged = false;
    copy.localV = 0;
    copy.localLog = [];
    // A jump: the patches before it say nothing of how to get here
    copy.log = [];
    copy.live = true;
    copy.usedAt = now();
    noteTs(copy, message.ts);
    changed(copy);
    fanFollowUp(copy);
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
    if (copy && !copy.shared) takeDelta(copy, message);
  }

  /** A delta of the server's, from whichever way it came (see tookDelta) */
  function takeDelta(copy, message) {
    if (copy.diverged) return;
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
      fanFollowUp(copy);
      return;
    }
    if (since > copy.v) return;
    // Each patch the copy had not, into its log as the patch it was, for the clients fanned out to
    const fresh = message.patches.slice(copy.v - since);
    for (const [i, diff] of fresh.entries()) {
      if (!isPlainObject(diff)) continue;
      LazyWatch.patch(copy.state, diff);
      const v = copy.v + i + 1;
      if (v <= message.v) logPatch(copy, { t: 'patch', store: copy.id, diff, ts: message.ts, v });
    }
    copy.v = message.v;
    copy.live = true;
    copy.usedAt = now();
    noteTs(copy, message.ts);
    changed(copy);
    fanFollowUp(copy);
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
    if (!copy || copy.shared || sock.stores.get(id)?.epoch !== copy.epoch) return;
    takePatch(copy, message);
  }

  /** A patch of the server's, from whichever way it came (see tookPatch): applied, logged, and fanned out */
  function takePatch(copy, message) {
    if (!copy.live || copy.diverged) return;
    if (!Number.isInteger(message.v) || message.v <= copy.v) return;
    if (message.v !== copy.v + 1 || !isPlainObject(message.diff)) {
      copy.live = false;
      return refollow(copy);
    }
    try {
      if (isTimestamp(message.ts)) mergeOp(copy.clocks, message.ts, message.diff, copy.regs, { authority: true });
      LazyWatch.patch(copy.state, message.diff);
    } catch (err) {
      copy.live = false;
      report(err);
      return refollow(copy);
    }
    copy.v = message.v;
    logPatch(copy, message);
    noteTs(copy, message.ts);
    changed(copy);
    fanFollowUp(copy);
  }

  /** A patch into the copy's log, which keeps the last LOG (with fan-out: nothing else reads it) */
  function logPatch(copy, message) {
    if (!fanOut) return;
    copy.log.push(message);
    if (copy.log.length > LOG) copy.log.splice(0, copy.log.length - LOG);
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
    const named = [...(Array.isArray(message.peers) ? message.peers : []), ...(Array.isArray(message.joined) ? message.joined : [])];
    // A leave or a share names no user to learn
    const own = named.length ? ownedBy(copy) : null;
    for (const peer of named) {
      // The relay's own clients' users alone: a store's thousand others are nothing to it offline
      if (isPlainObject(peer) && typeof peer.replicaId === 'string' && own(peer.replicaId) && keepUser(copy, peer)) learned = true;
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
  function localHello(sock, id, message, { fresh = false } = {}) {
    const copy = copies.get(id);
    const { replicaId } = message;
    // A copy the server never filled (one a fan-out client made) has nothing to answer with
    if (!copy || copy.epoch === null || typeof replicaId !== 'string' || !replicaId || !mayServe(sock.key, id, replicaId)) return;
    // A client under an epoch the copy never held saw the server where the
    // relay did not (a restore the copy missed, a relay restarted from an
    // older file): it may be the newer of the two, so it is not rolled back
    // to the copy, and waits for the server. One under an epoch the copy
    // moved on from is behind it, and is answered as the server would
    if (typeof message.epoch === 'string' && message.epoch !== copy.epoch && !copy.past.includes(message.epoch)) return;
    let session = sock.locals.get(id);
    if (session && session.replicaId !== replicaId) return;   // a session speaks for one replica
    // The others hear of a session new here, or of one a fanned-out socket made before its first answer
    const joined = !session || fresh;
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

  /**
   * The copy takes its first offline edit: its local sessions (but
   * `spare`, whose hello is being answered) start over. A fan-out session
   * still waiting for its first answer waits on
   */
  function diverge(copy, spare) {
    copy.diverged = true;
    copy.live = false;
    copy.localV = 0;
    copy.localLog = [];
    copy.log = [];
    for (const [sock, session] of [...copy.locals]) {
      if (sock === spare || (sock.state === 'fan' && !session.answered)) continue;
      sock.locals.delete(copy.id);
      copy.locals.delete(sock);
      forgetGrant(session);
      flushHeld(sock, session, copy);
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

  /**
   * A session on a store ends: the client left, its socket closed, or (a
   * fan-out session) the server ended it, when `tell` is false: otherwise
   * the server hears that the client let go (unvouch)
   */
  function leaveLocal(sock, id, { tell = true } = {}) {
    const session = sock.locals.get(id);
    if (!session) return;
    sock.locals.delete(id);
    forgetGrant(session, tell);
    // Its write-only session up the client's own socket goes too, and that socket closes once nothing else waits on it
    if (session.writing && sock.write && sock.state === 'fan') {
      session.writing = false;
      writeSend(sock, { t: 'leave', store: id });
      idleWrite(sock);
    }
    const copy = copies.get(id);
    if (!copy) return;
    copy.locals.delete(sock);
    copy.usedAt = now();
    // Offline the relay lists its sessions itself; through, a fan-out client's leaving is the server's to tell
    if (session.answered && copy.presenceOn && (mode === 'local' || sock.state !== 'fan')) tellPresence(copy, sock, { left: [session.replicaId] });
    if (sock.state === 'fan') lingerShared(copy);
    // A copy a fan-out client made for a store the server never filled goes with it
    if (copy.epoch === null && !copy.locals.size && !copy.followers.size && !copy.shared) copies.delete(id);
  }

  // --- Fan-out: the stores read once on the relay's link, each client's edits on its own socket -----
  //
  // See the header. A fan-out socket's sessions live in `sock.locals` and
  // `copy.locals`, as local sessions do, so that the relay answers them
  // from the copy the same way when the server is away; through, each also
  // carries what fan-out needs (see newFanSession).

  /** A fan-out session on a store: a local session's fields, and fan-out's */
  function newFanSession(replicaId) {
    return {
      replicaId,
      presence: true,
      shared: undefined,
      sharedJson: undefined,
      answered: false,
      grant: null,        // the relay's name for it on the link (see server/relays.js)
      admitted: false,    // the server let it read the store, on the link as it is now
      vouching: false,    // a vouch for it is out, or waiting its turn
      vouchAfter: 0,      // not vouched for again before (the server's retryAfter)
      peer: null,         // how the server's presence shows it
      waiting: null,      // its hello, until that is answered (held, or gone up its write connection)
      inFlight: false,    // its hello went up its write connection, the answer to come from there
      share: undefined,   // a share of its the server has not had yet
      cursor: null,       // { v, epoch }: what it has been brought to; null for an answer of the relay's own (offline)
      held: [],           // acks waiting for the cursor to reach their version
      sent: 0,            // the highest seq of an op it sent through the relay
      acked: 0,           // the highest the server acknowledged
      awaiting: [],       // answers to come on its write connection: 'own' (the relay's write hello) or 'client' (its hello)
      parked: [],         // live ops waiting for its vouch, before its write-only session is opened (the server opens one only for a client a relay serves)
      writing: false,     // it has a write-only session on its write connection
      toldLocal: false,   // it was told the relay answers on its own
      untilTimer: null
    };
  }

  /** A fan-out socket's session on a store, made (with a copy to fill) when it has none; null when the socket speaks for another replica there */
  function fanSession(sock, id, replicaId) {
    const session = sock.locals.get(id);
    if (session) return session.replicaId === replicaId ? session : null;
    let copy = copies.get(id);
    if (!copy) copies.set(id, (copy = newCopy(id)));
    const made = newFanSession(replicaId);
    sock.locals.set(id, made);
    copy.locals.set(sock, made);
    copy.usedAt = now();
    clearTimeout(copy.lingerTimer);
    copy.lingerTimer = null;
    return made;
  }

  /** A session's name on the link is let go: nothing the server says of it is heard any more */
  /**
   * A session's name on the link is let go: nothing the server says of it
   * is heard any more, and the server hears it let go too (unvouch) while
   * the link is up, unless it was the server that ended it (`tell` false)
   */
  function forgetGrant(session, tell = true) {
    if (session.grant) {
      if (tell && linkState === 'open' && (session.admitted || session.vouching)) linkSend({ t: 'unvouch', grant: session.grant });
      grantsOut.delete(session.grant);
      if (vouchesOut.delete(session.grant)) pumpVouches();
    }
    session.grant = null;
    session.admitted = false;
    session.vouching = false;
    clearTimeout(session.untilTimer);
    session.untilTimer = null;
  }

  /** The session a grant names, while it is the one the grant was made for */
  function fanOf(grant) {
    const entry = typeof grant === 'string' ? grantsOut.get(grant) : undefined;
    if (!entry) return null;
    const session = entry.sock.locals.get(entry.id);
    if (!session || session.grant !== grant || entry.sock.state !== 'fan') {
      grantsOut.delete(grant);
      return null;
    }
    return { sock: entry.sock, id: entry.id, session };
  }

  /** A client message on a fan-out socket */
  function fan(sock, message) {
    if (message.t === 'ping') return deliver(sock, { t: 'pong' });
    const id = message.store;
    if (!isStoreId(id)) return;
    if (mode === 'local') return fanLocal(sock, id, message);
    switch (message.t) {
      case 'hello': return fanHello(sock, id, message);
      case 'op': return fanOp(sock, id, message);
      case 'share': return fanShare(sock, id, message.data);
      case 'leave': return fanLeave(sock, id);
    }
  }

  /** The client leaves a store (see leaveLocal, which lets go of its write-only session there too) */
  function fanLeave(sock, id) {
    leaveLocal(sock, id);
  }

  const highestSeq = ops => (Array.isArray(ops) ? ops : []).reduce((max, op) => (isPlainObject(op) && Number.isInteger(op.seq) && op.seq > max ? op.seq : max), 0);

  /**
   * Offline, a fan-out socket is answered as any local one is. What it
   * sends is noted: an op of its (applied or not) makes it say hello again
   * once the server is back, so its outbox reaches the server in order;
   * and a hello the copy cannot answer waits in its session, to be answered
   * through when the server is back
   */
  function fanLocal(sock, id, message) {
    if (message.t === 'hello') {
      if (typeof message.replicaId !== 'string' || !message.replicaId) return;
      const session = fanSession(sock, id, message.replicaId);
      if (!session) return;
      const fresh = !session.answered;
      session.presence = message.presence !== false;
      session.answered = false;
      session.waiting = message;
      flushHeld(sock, session, copies.get(id));
      if (message.share !== undefined) session.share = message.share;
      localHello(sock, id, message, { fresh });
      if (!session.answered) return;
      session.waiting = null;
      session.cursor = null;
      session.toldLocal = false;
      session.sent = Math.max(session.sent, highestSeq(message.ops));
      return;
    }
    if (message.t === 'op') {
      const session = sock.locals.get(id);
      if (session?.answered) session.sent = Math.max(session.sent, highestSeq([message.op]));
    }
    local(sock, message);
  }

  function fanHello(sock, id, message) {
    if (typeof message.replicaId !== 'string' || !message.replicaId) return;
    const session = fanSession(sock, id, message.replicaId);
    if (!session) return;   // a socket's session on a store speaks for one replica
    session.presence = message.presence !== false;
    if (session.presence) askPresence(copies.get(id));
    // A hello starts the session over: nothing reaches it until it is
    // answered, and its ops come again in it (what waited for the cursor
    // goes to it now, a correction with it, as on a direct socket)
    session.answered = false;
    flushHeld(sock, session, copies.get(id));
    session.parked = [];
    session.waiting = message;
    if (message.share !== undefined) {
      session.share = message.share;
      setShare(session, message.share);
    }
    proceed(sock, id, session);
  }

  /**
   * Move a waiting hello on: the server asked about the client (vouch),
   * its hello up its write connection when it carries ops or knows more
   * than the copy, else answered from the copy once the copy follows
   */
  function proceed(sock, id, session) {
    const message = session.waiting;
    if (!message || session.answered || session.inFlight || mode !== 'through' || sock.state !== 'fan') return;
    if (!session.admitted) return vouchFor(sock, id, session);
    const copy = copies.get(id);
    ensureShared(copy);
    const ahead = message.epoch === copy.epoch && Number.isInteger(message.since) && message.since > copy.v;
    if ((Array.isArray(message.ops) && message.ops.length) || ahead) return helloUpWrite(sock, id, session, message);
    if (!copy.live) return;   // answered once the copy follows (see fanFollowUp)
    answerFromCopy(sock, id, session, copy, message);
  }

  /** The answer of the copy, under the server's epoch and version: a delta from its log when that reaches back to the client, else a snapshot */
  function answerFromCopy(sock, id, session, copy, message) {
    const since = Number.isInteger(message.since) ? message.since : null;
    let answer = null;
    if (message.epoch === copy.epoch && since !== null && since <= copy.v) {
      const start = copy.log.length - (copy.v - since);
      if (since === copy.v) answer = { t: 'delta', store: id, patches: [], ts: copy.maxTs, seq: 0, registers: copy.registers, v: copy.v, epoch: copy.epoch };
      else if (start >= 0 && copy.log[start]?.v === since + 1) answer = { t: 'delta', store: id, patches: copy.log.slice(start).map(patch => patch.diff), ts: copy.maxTs, seq: 0, registers: copy.registers, v: copy.v, epoch: copy.epoch };
    }
    answer ??= snapshotOf(copy, copy.v, copy.epoch);
    session.waiting = null;
    session.answered = true;
    session.cursor = { v: copy.v, epoch: copy.epoch };
    session.toldLocal = false;
    deliver(sock, answer);
    afterAnswer(sock, id, session);
  }

  /**
   * After a fan-out session's answer: the store's presence (the server's, the
   * client's own peer in it), once the relay's session hears it whole (until
   * then the list reaches the client as it comes, see sharedPresence), and a
   * share the server has not had
   */
  function afterAnswer(sock, id, session) {
    const copy = copies.get(id);
    if (copy?.presenceOn && copy.presence === 'on' && session.presence) deliver(sock, { t: 'presence', store: id, peers: peersFor(copy, session) });
    if (session.share !== undefined) afterShare(session);
  }

  /** A client's live op: up its write connection, behind the write hello that opens its write-only session there */
  function fanOp(sock, id, message) {
    const session = sock.locals.get(id);
    const op = message.op;
    // Not answered: the client sends its outbox again once it is (and one
    // told to say hello again sends nothing more until then, so no op
    // overtakes one whose fate on a lost write connection is not known)
    if (!session?.answered || !isPlainObject(op) || !Number.isInteger(op.seq)) return;
    session.sent = Math.max(session.sent, op.seq);
    // No write-only session yet, and the server's word on the client lost
    // with the link: its ops wait for it, in order (see admitted)
    if (!session.writing && (!session.admitted || session.parked.length)) {
      session.parked.push(message);
      return;
    }
    writeOp(sock, id, session, message);
  }

  /** An op up the client's own socket, behind the write hello that opens its write-only session there */
  function writeOp(sock, id, session, message) {
    if (!session.writing) {
      session.writing = true;
      session.awaiting.push('own');
      const copy = copies.get(id);
      const hello = { t: 'hello', store: id, replicaId: session.replicaId, ops: [], follow: false, presence: false };
      if (copy && !copy.diverged && typeof copy.epoch === 'string') Object.assign(hello, { since: copy.v, epoch: copy.epoch });
      writeSend(sock, hello);
    }
    writeSend(sock, message);
  }

  /** A client's hello up its write connection, as a write-only session's, for the server to answer (its ops judged as its own) */
  function helloUpWrite(sock, id, session, message) {
    const { fetch: _fetch, share: _share, ...rest } = message;
    session.inFlight = true;
    session.writing = true;
    session.awaiting.push('client');
    session.sent = Math.max(session.sent, highestSeq(message.ops));
    writeSend(sock, { ...rest, follow: false, presence: false });
  }

  /** What the client shares: kept for the relay's own presence offline, and told the server as the client's */
  function fanShare(sock, id, data) {
    const session = sock.locals.get(id);
    if (!session) return;
    setShare(session, data);
    session.share = data;
    if (session.answered && session.admitted && linkState === 'open') {
      linkSend({ t: 'share', grant: session.grant, data });
      session.share = undefined;
    }
  }

  // --- Fan-out: bringing clients along --------------------------------------------------------------

  /** The copy moved: every client on it that the relay answered is brought along, and every one waiting for it is answered */
  function fanFollowUp(copy) {
    if (mode !== 'through' || copy.diverged) return;
    for (const [sock, session] of [...copy.locals]) {
      if (sock.state !== 'fan' || sock.locals.get(copy.id) !== session) continue;
      if (session.answered) rejoin(sock, copy.id, session, copy);
      else proceed(sock, copy.id, session);
    }
  }

  /**
   * The copy's patches past a client's cursor, in order, each followed by
   * the acks that waited for it. A client the log no longer reaches back to,
   * or whose epoch the copy moved on from, says hello again; one under an
   * epoch the copy has not reached yet waits for it
   */
  function bringUp(sock, copy, session) {
    const cursor = session.cursor;
    // Nothing more of the store for a client the server has not (again) let in: brought along once it is (see admitted)
    if (!cursor || !copy.live || !session.admitted) return;
    if (cursor.epoch !== copy.epoch) {
      if (cursor.epoch === null || copy.past.includes(cursor.epoch)) tellAgain(sock, copy.id, session);
      return;
    }
    if (cursor.v < copy.v) {
      const start = copy.log.length - (copy.v - cursor.v);
      if (start < 0 || copy.log[start]?.v !== cursor.v + 1) return tellAgain(sock, copy.id, session);
      for (let i = start; i < copy.log.length; i++) {
        const patch = copy.log[i];
        deliver(sock, patch);
        cursor.v = patch.v;
        releaseAcks(sock, copy, session);
        if (sock.locals.get(copy.id) !== session || !session.answered) return;
      }
    }
    releaseAcks(sock, copy, session);
  }

  /** The acks whose version the client has reached, in order, and the refusals the server sent after them */
  function releaseAcks(sock, copy, session) {
    while (session.held.length && session.cursor && (session.held[0].t === 'error' || session.held[0].v <= session.cursor.v)) {
      const next = session.held.shift();
      if (next.t === 'error') deliver(sock, next);
      else deliverAck(sock, copy, session, next);
    }
  }

  /**
   * What waited for the client's cursor goes to it now, in order: its
   * session starts over (a hello, the relay going local, being told to say
   * hello again), and an ack's correction is not to be lost with it. The
   * client applies what an ack says whenever it comes, and what it lacks of
   * the store up to there reaches it with its next answer
   */
  function flushHeld(sock, session, copy) {
    const held = session.held;
    if (!held?.length) return;   // a local session of a socket passed through has none
    session.held = [];
    for (const next of held) {
      if (next.t === 'error') deliver(sock, next);
      else deliverAck(sock, copy, session, next);
    }
  }

  /**
   * An ack on to its client. One the client hears after patches past its
   * version (its write connection was slower than the link) says what its
   * op lost as the copy has it now, not as the server had it then, which
   * those patches may have changed
   */
  function deliverAck(sock, copy, session, ack) {
    if (Number.isInteger(ack.seq)) session.acked = Math.max(session.acked, ack.seq);
    const late = Number.isInteger(ack.v) && session.cursor && session.cursor.v > ack.v && Array.isArray(ack.lost) && ack.lost.length &&
      copy && copy.epoch === session.cursor.epoch && copy.v === session.cursor.v;
    if (late) {
      const paths = ack.lost.filter(path => Array.isArray(path) && path.every(segment => typeof segment === 'string'));
      ack = { ...ack, correction: paths.length ? correctionAt(copy.state, paths) : ack.correction };
    }
    deliver(sock, ack);
    idleWrite(sock);
  }

  /**
   * The client says hello again (it hears `closed` 'unavailable', and
   * answers with a hello that carries its whole outbox in order): until
   * then it is not answered, hears nothing, and its live ops are left for
   * that hello to carry. Its vouch stands
   */
  function tellAgain(sock, id, session, message = 'The relay lost its place on the store; say hello again') {
    flushHeld(sock, session, copies.get(id));
    session.answered = false;
    session.waiting = null;
    session.parked = [];
    session.inFlight = false;
    session.cursor = null;
    session.sent = session.acked = 0;
    session.toldLocal = false;
    deliver(sock, { t: 'closed', store: id, code: 'unavailable', message });
  }

  /**
   * A fan-out session the server ended (a vouch refused, access revoked,
   * its write-only session closed for good): the client hears it as from
   * the server, and a final verdict on its credential's store holds
   * offline too (see FINAL). `tell`: whether the server still has to hear
   * that the client let go
   */
  function endFan(sock, id, session, code, message, { tell = false } = {}) {
    if (code === 'unauthorized') {
      forgetKey(sock.key);
      if (sock.key !== null) turnedAway.set(sock.key, Date.now() + TURNED_AWAY_MS);
      return shut(sock, UNAUTHORIZED, message || 'Unauthorized');
    }
    if (code === 'reauthenticate') return shut(sock, REAUTHENTICATE, 'Reauthenticate');
    if (FINAL.has(code)) forgetStore(sock.key, id);
    deliver(sock, { t: 'closed', store: id, code, message });
    leaveLocal(sock, id, { tell });
  }

  // --- Fan-out: the relay's own session on each store -----------------------------------------------

  /** The relay's session on the store, said hello to while some client of it is let in, and the link is up */
  function ensureShared(copy) {
    if (!copy) return;
    clearTimeout(copy.lingerTimer);
    copy.lingerTimer = null;
    if (linkState !== 'open' || copy.shared || mode !== 'through') return;
    let admitted = false;
    for (const session of copy.locals.values()) if (session.admitted) admitted = true;
    if (!admitted) return;
    sharedHello(copy);
  }

  /** Hello on the relay's session: from where the copy stands, or (a copy diverged, or never filled) for a snapshot */
  function sharedHello(copy) {
    copy.shared = 'hello';
    if (hearsPresence(copy)) {
      if (copy.presence === 'off') copy.presence = 'asked';
    } else {
      copy.peers = new Map();
    }
    linkSend(sharedHelloOf(copy, !copy.diverged && typeof copy.epoch === 'string'));
  }

  /**
   * The relay's hello on a store, `from` where the copy stands or (false)
   * for a snapshot. It always says whether the session hears presence: a
   * server takes a hello's word either way
   */
  function sharedHelloOf(copy, from) {
    const hello = { t: 'hello', store: copy.id, replicaId: RELAY_REPLICA, ops: [], presence: copy.presence !== 'off' };
    if (from) Object.assign(hello, { since: copy.v, epoch: copy.epoch });
    return hello;
  }

  /** Whether some client behind the relay wants the store's presence */
  function presenceWanted(copy) {
    for (const [sock, session] of copy.locals) if (sock.state === 'fan' && session.presence) return true;
    return false;
  }

  /**
   * Whether the relay's session on the store is to hear presence: while a
   * client behind it wants it, and a session that does keeps it (turned
   * off with the session, not in place, so a client that comes and goes
   * costs no hello each time)
   */
  function hearsPresence(copy) {
    if (copy.presence !== 'off') return true;
    return copy.presenceOn && presenceWanted(copy);
  }

  /** A client wants presence the relay's session does not hear: the relay says hello again, asking for it (the whole list comes) */
  function askPresence(copy) {
    if (!copy || copy.presence !== 'off' || copy.shared !== 'open' || !copy.presenceOn) return;
    if (linkState !== 'open' || mode !== 'through' || !presenceWanted(copy)) return;
    copy.presence = 'asked';
    linkSend(sharedHelloOf(copy, !copy.diverged && typeof copy.epoch === 'string'));
  }

  /** The relay's session on a store is gone: whether a new one hears presence is decided afresh */
  function sharedGone(copy) {
    copy.shared = null;
    copy.presence = 'off';
    copy.peers = new Map();
  }

  /** The copy lost its place on the relay's session (a patch missed): it asks again from where it stands */
  function refollow(copy) {
    if (copy.shared === 'open' && linkState === 'open') sharedHello(copy);
  }

  /** The server answered the relay's hello on a store: the copy takes it, and its clients are brought along or answered */
  function sharedAnswered(copy, message) {
    heardServer(message.ts);
    if (!copy.shared || typeof message.epoch !== 'string' || !Number.isInteger(message.v)) return;
    copy.shared = 'open';
    if (message.t === 'snapshot') {
      if (isPlainObject(message.state)) takeSnapshot(copy, message, null);
    } else {
      takeDelta(copy, message);
    }
    // A delta the copy could not take (another epoch, or a place it was not at): a snapshot instead
    if (copy.epoch !== message.epoch || copy.v < message.v || copy.diverged) {
      copy.live = false;
      return linkSend(sharedHelloOf(copy, false));
    }
    copy.live = true;
    fanFollowUp(copy);
    // A client that came wanting presence while the hello was out
    askPresence(copy);
  }

  /** The server's presence on a store, as the relay's session hears it: kept, and passed on to its clients through */
  function sharedPresence(copy, message) {
    learnPresence(copy.id, message);
    if (Array.isArray(message.peers)) {
      copy.presence = 'on';
      copy.peers = new Map();
      for (const peer of message.peers) if (isPlainObject(peer) && typeof peer.replicaId === 'string') copy.peers.set(peer.replicaId, peer);
    } else {
      for (const replicaId of Array.isArray(message.left) ? message.left : []) copy.peers.delete(replicaId);
      for (const peer of Array.isArray(message.joined) ? message.joined : []) if (isPlainObject(peer) && typeof peer.replicaId === 'string') copy.peers.set(peer.replicaId, peer);
      for (const change of Array.isArray(message.shared) ? message.shared : []) {
        const peer = isPlainObject(change) ? copy.peers.get(change.replicaId) : undefined;
        if (!peer) continue;
        const next = { ...peer };
        if (change.data === undefined) delete next.data;
        else next.data = change.data;
        copy.peers.set(peer.replicaId, next);
      }
    }
    if (mode !== 'through') return;
    for (const [sock, session] of copy.locals) if (sock.state === 'fan' && session.answered && session.admitted && session.presence) deliver(sock, message);
  }

  /**
   * The server closed the relay's session on a store. 'forbidden' is the
   * relay not being let carry it: its clients there hear so, as from the
   * server. Anything else ('unused', no client of it let in just then;
   * 'unavailable', the store unloaded or served elsewhere for a moment) is
   * the relay's own affair: it says hello again while a client is let in
   */
  function sharedClosed(copy, message) {
    sharedGone(copy);
    if (!copy.followers.size) copy.live = false;
    if (message.code === 'forbidden') {
      for (const [sock, session] of [...copy.locals]) if (sock.state === 'fan') endFan(sock, copy.id, session, 'forbidden', message.message, { tell: true });
      return;
    }
    if (copy.sharedRetry) return;
    copy.sharedRetry = later(() => {
      copy.sharedRetry = null;
      ensureShared(copy);
    }, 250 + Math.random() * 750);
  }

  /** The store's last fan-out client left: the relay's session there goes `linger` later, unless another comes */
  function lingerShared(copy) {
    for (const sock of copy.locals.keys()) if (sock.state === 'fan') return;
    if (!copy.shared || copy.lingerTimer) return;
    copy.lingerTimer = later(() => {
      copy.lingerTimer = null;
      for (const sock of copy.locals.keys()) if (sock.state === 'fan') return;
      if (!copy.shared) return;
      linkSend({ t: 'leave', store: copy.id });
      sharedGone(copy);
      if (!copy.followers.size) copy.live = false;
      copy.usedAt = now();
    }, linger);
  }

  // --- Fan-out: vouches -----------------------------------------------------------------------------

  /** Ask the server whether the client may read the store; asked when the link is up (see resumeFan), and in turn */
  function vouchFor(sock, id, session) {
    // Not before the server's retryAfter (see refused), whatever moves the copy meanwhile
    if (session.vouching || linkState !== 'open' || session.vouchAfter > Date.now()) return;
    session.grant ??= `g${++grantSeq}`;
    grantsOut.set(session.grant, { sock, id });
    session.vouching = true;
    vouchQueue.push({ t: 'vouch', grant: session.grant, socket: sock.name, store: id, replicaId: session.replicaId, credential: sock.credential ?? {} });
    pumpVouches();
  }

  function pumpVouches() {
    while (vouchQueue.length && vouchesOut.size < VOUCHES_AT_ONCE && linkState === 'open') {
      const message = vouchQueue.shift();
      const found = fanOf(message.grant);
      if (!found || !found.session.vouching) continue;   // left, or answered, meanwhile
      vouchesOut.add(message.grant);
      // One the server takes long over (an authentication service that hangs) holds its turn no longer than a dial
      // may take, so it holds nobody else back: its answer is still waited for, and taken when it comes
      const timer = later(() => {
        vouchTimers.delete(message.grant);
        if (vouchesOut.delete(message.grant)) pumpVouches();
      }, dialTimeout);
      vouchTimers.set(message.grant, timer);
      linkSend(message);
    }
  }

  function vouchAnswered(grant) {
    clearTimeout(vouchTimers.get(grant));
    vouchTimers.delete(grant);
    if (vouchesOut.delete(grant)) pumpVouches();
  }

  function admitted(message) {
    vouchAnswered(message.grant);
    const found = fanOf(message.grant);
    // Left meanwhile: the server lets go too
    if (!found) return linkSend({ t: 'unvouch', grant: message.grant });
    const { sock, id, session } = found;
    session.vouching = false;
    session.admitted = true;
    recordKnown(sock.key, id, session.replicaId);
    const copy = copies.get(id);
    // Whether the store has presence at all (the relay's session hears it only while a client behind it wants it)
    if (copy && typeof message.presence === 'boolean' && copy.presenceOn !== message.presence) {
      copy.presenceOn = message.presence;
      persist(copy);
    }
    if (isPlainObject(message.peer) && typeof message.peer.replicaId === 'string') {
      session.peer = message.peer;
      if (copy && copy.presence !== 'off') copy.peers.set(message.peer.replicaId, message.peer);
      // Its user, for its presence offline, whether or not the relay's session hears the store's
      if (copy && keepUser(copy, message.peer)) persist(copy);
    }
    // Its session runs out: it comes back and is judged afresh (the server says so too; this holds if that is lost)
    clearTimeout(session.untilTimer);
    session.untilTimer = Number.isFinite(message.until)
      ? later(() => { if (sock.locals.get(id) === session && mode === 'through') shut(sock, REAUTHENTICATE, 'Reauthenticate'); }, message.until - Date.now())
      : null;
    ensureShared(copy);
    if (session.presence) askPresence(copy);
    if (session.answered) {
      // The server's list, which a link lost and found again may have changed and this client missed (see rejoin)
      if (copy?.presenceOn && copy.presence === 'on' && session.presence) deliver(sock, { t: 'presence', store: id, peers: peersFor(copy, session) });
      afterShare(session);
      // Ops that waited for this go up now, in order, and what the store did meanwhile reaches it
      for (const parked of session.parked.splice(0)) writeOp(sock, id, session, parked);
      if (copy) rejoin(sock, id, session, copy);
    }
    proceed(sock, id, session);
  }

  /**
   * A client answered before, let in again (after a link lost, or the relay
   * local for a while with the link up): brought along from its cursor, told
   * the relay answers through again, and given the server's peers anew
   */
  function rejoin(sock, id, session, copy) {
    bringUp(sock, copy, session);
    if (!session.toldLocal || !session.admitted || !copy.live || !session.answered || sock.locals.get(id) !== session) return;
    session.toldLocal = false;
    deliver(sock, { t: 'relay', store: id, status: 'through' });
    if (copy.presenceOn && copy.presence === 'on' && session.presence) deliver(sock, { t: 'presence', store: id, peers: peersFor(copy, session) });
  }

  /** The server's peers on a store for a client's list, its own among them as the server shows it */
  function peersFor(copy, session) {
    const peers = new Map(copy.peers);
    if (session.peer && !peers.has(session.peer.replicaId)) peers.set(session.peer.replicaId, session.peer);
    return [...peers.values()];
  }

  /**
   * What the client shares, for the server as its own (a peer made anew, after
   * the link was lost, shares nothing until told): one made while its vouch
   * was out, else the last it made
   */
  function afterShare(session) {
    if (!session.admitted || linkState !== 'open') return;
    const data = session.share !== undefined ? session.share : session.sharedJson !== undefined ? session.shared : undefined;
    session.share = undefined;
    if (data !== undefined) linkSend({ t: 'share', grant: session.grant, data });
  }

  function refused(message) {
    vouchAnswered(message.grant);
    const found = fanOf(message.grant);
    if (!found) return;
    const { sock, id, session } = found;
    session.vouching = false;
    // Not a verdict on the client (the server is busy, or a store is being
    // taken over by another process): asked again a little later, the
    // client none the wiser
    if (message.code === 'rate-limited' || message.code === 'unavailable') {
      const wait = Number.isFinite(message.retryAfter) ? message.retryAfter : 1000;
      session.vouchAfter = Date.now() + wait;
      later(() => {
        if (sock.locals.get(id) === session && session.grant === message.grant && !session.admitted) vouchFor(sock, id, session);
      }, wait + Math.random() * wait);
      return;
    }
    grantsOut.delete(message.grant);
    session.grant = null;
    endFan(sock, id, session, message.code, message.message);
  }

  function revoked(message) {
    for (const grant of Array.isArray(message.grants) ? message.grants : []) {
      const found = fanOf(grant);
      if (!found) continue;
      const { sock, id, session } = found;
      grantsOut.delete(grant);
      session.grant = null;
      session.admitted = false;
      endFan(sock, id, session, message.code, message.message);
    }
  }

  // --- Fan-out: the link ----------------------------------------------------------------------------

  const linkSend = message => {
    if (linkState !== 'open' || !linkUp) return;
    try {
      linkUp.send(message);
    } catch (err) {
      report(err);
    }
  };

  function dialLink() {
    if (!fanOut || closed || linkUp) return;
    clearTimeout(linkRedialTimer);
    linkRedialTimer = null;
    linkState = 'dialing';
    let t;
    try {
      t = link();
    } catch (err) {
      report(err);
      return linkLost(null);
    }
    if (!t || typeof t.send !== 'function' || typeof t.close !== 'function') {
      report(new TypeError('link must return a transport: { send, close, onopen, onmessage, onclose }'));
      return linkLost(null);
    }
    linkUp = t;
    linkDialTimer = later(() => {
      if (linkUp !== t || linkState !== 'dialing') return;
      dropLink();
      linkLost(null);
    }, dialTimeout);
    t.onopen = () => guard(() => {
      if (linkUp !== t || linkState !== 'dialing') return;
      clearTimeout(linkDialTimer);
      linkDialTimer = null;
      linkOpened();
    });
    t.onmessage = message => guard(() => {
      if (linkUp === t && linkState === 'open') fromLink(message);
    });
    t.onclose = info => guard(() => {
      if (linkUp !== t) return;
      linkUp = null;
      clearTimeout(linkDialTimer);
      linkDialTimer = null;
      linkLost(info);
    });
  }

  /** Close the link's transport, disowned first so nothing it still says is heard */
  function dropLink() {
    const t = linkUp;
    linkUp = null;
    clearTimeout(linkDialTimer);
    linkDialTimer = null;
    if (!t) return;
    try {
      t.close();
    } catch (err) {
      report(err);
    }
  }

  function linkOpened() {
    linkState = 'open';
    linkProbed = false;
    linkDelay = 0;
    linkHeard = true;
    linkSilent = 0;
    clearTimeout(linkGraceTimer);
    linkGraceTimer = null;
    serverAnswered();
    if (linkBroken) {
      // Its clients passed through while it was down come back to be fanned out
      linkBroken = false;
      for (const sock of [...sockets]) if (sock.credential && sock.state !== 'fan') shut(sock, RESTART, 'The relay fans the stores out again');
    }
    // Back from an outage: through again, after the jitter (see goThrough), which asks after every client
    if (mode === 'local') return goThrough();
    resumeFan();
  }

  /** The link is (back) up: every client on it is vouched for (the server forgot them with the link), and the copies they read follow again */
  function resumeFan() {
    for (const sock of [...sockets]) {
      if (sock.state !== 'fan') continue;
      for (const [id, session] of [...sock.locals]) {
        if (session.answered || session.waiting) vouchFor(sock, id, session);
      }
    }
  }

  /**
   * The link closed or never opened. The server forgot every vouch and the
   * relay's sessions with it; the clients do not hear of it. A link turned
   * away (4401) is final: the clients are passed through from then on. Any
   * other failure is the server being away, as a pass-through socket's is:
   * after `grace` the relay answers on its own, unless the server answers a
   * client's own dial, when it is the link alone that is broken and the
   * clients are passed through meanwhile. The link is dialled again, backing off
   */
  function linkLost(info) {
    linkState = 'down';
    vouchQueue = [];
    vouchesOut.clear();
    for (const timer of vouchTimers.values()) clearTimeout(timer);
    vouchTimers.clear();
    for (const copy of copies.values()) {
      clearTimeout(copy.sharedRetry);
      copy.sharedRetry = null;
      if (!copy.shared) continue;
      sharedGone(copy);
      if (!copy.followers.size) copy.live = false;
    }
    for (const sock of sockets) {
      if (sock.state !== 'fan') continue;
      for (const session of sock.locals.values()) {
        session.admitted = false;
        session.vouching = false;
      }
    }
    if (closed) return;
    if (info?.code === UNAUTHORIZED) {
      report(new Error('The server turned the relay\'s link away (4401): its clients are passed through, and it is tried again in a while'));
      linkState = 'refused';
      clearTimeout(linkGraceTimer);
      linkGraceTimer = null;
      fallBack();
      linkRedialTimer = later(() => {
        linkRedialTimer = null;
        if (linkState === 'refused') {
          linkState = 'down';
          dialLink();
        }
      }, REFUSED_RETRY);
      return goThrough();
    }
    // The server answered a probe, and the link still does not: clients pass through meanwhile
    if (linkProbed) {
      linkProbed = false;
      fallBack();
      goThrough();
    }
    if (!linkGraceTimer && !linkBroken && mode === 'through' && Number.isFinite(grace)) linkGraceTimer = later(linkGraceOver, Math.max(0, grace));
    linkDelay = Math.min(linkDelay ? linkDelay * 2 : 250, linkBroken ? 30_000 : 5_000);
    linkRedialTimer = later(() => {
      linkRedialTimer = null;
      dialLink();
    }, linkDelay * (0.75 + Math.random() * 0.5));
  }

  async function linkGraceOver() {
    linkGraceTimer = null;
    if (closed || linkState === 'open' || mode === 'local' || linkBroken) return;
    let answered = false;
    try {
      answered = probe === false ? false : await (typeof probe === 'function' ? probe() : dialProbe());
    } catch (err) {
      report(err);
    }
    if (closed || linkState === 'open' || mode === 'local' || linkBroken) return;
    // The server answers: the link is dialled at once, and only if that fails too are the clients passed through
    if (answered) return kickLink();
    downSince ??= now();
    goLocal();
  }

  /** The server answers the clients but not the link: every fan-out socket reconnects, to be passed through (see accept) */
  function fallBack() {
    if (linkBroken) return;
    linkBroken = true;
    for (const sock of [...sockets]) if (sock.state === 'fan') shut(sock, RESTART, 'The relay passes the socket through');
  }

  function fromLink(message) {
    linkHeard = true;
    if (downSince !== null) serverAnswered();
    if (!isPlainObject(message)) return;
    switch (message.t) {
      case 'pong': return;
      case 'admitted': return admitted(message);
      case 'refused': return refused(message);
      case 'revoked': return revoked(message);
      case 'error': {
        // A client's share the server refused: the client hears it
        const found = message.grant !== undefined ? fanOf(message.grant) : null;
        if (found) deliver(found.sock, { t: 'error', store: found.id, code: message.code, message: message.message, ...(message.retryAfter === undefined ? {} : { retryAfter: message.retryAfter }) });
        return;
      }
    }
    const id = message.store;
    const copy = isStoreId(id) ? copies.get(id) : undefined;
    if (!copy) return;
    switch (message.t) {
      case 'snapshot':
      case 'delta': return sharedAnswered(copy, message);
      case 'patch': return takePatch(copy, message);
      case 'presence': return sharedPresence(copy, message);
      case 'closed': return sharedClosed(copy, message);
    }
  }

  /** The keepalive's turn on the link: a link silent for two of them is taken for gone */
  function keepLinkAlive() {
    if (linkState !== 'open' || !linkUp) return;
    linkSilent = linkHeard ? 0 : linkSilent + 1;
    if (linkSilent >= 2) {
      dropLink();
      return linkLost({ code: 1006 });
    }
    linkHeard = false;
    linkSend({ t: 'ping' });
  }

  // --- Fan-out: each client's socket up, for its edits ------------------------------------------------

  /** Send up a client's own socket, dialled when it has none (backing off after a dial that failed); what it says meanwhile waits in order */
  function writeSend(sock, message) {
    const w = sock.write ?? openWrite(sock);
    clearTimeout(w.idleTimer);
    w.idleTimer = null;
    if (w.open) {
      try {
        w.t.send(message);
      } catch (err) {
        report(err);
      }
      return;
    }
    if (w.queue.length >= QUEUE_LIMIT) return failWrite(sock, w, null);
    w.queue.push(message);
  }

  function openWrite(sock) {
    const w = { t: null, open: false, queue: [], dialTimer: null, idleTimer: null, heard: true, silent: 0, dial: null };
    sock.write = w;
    const dialNow = w.dial = () => {
      if (sock.write !== w || w.t) return;
      let t;
      try {
        t = sock.upstream();
      } catch (err) {
        report(err);
        return dialFailedWrite(sock, w);
      }
      if (!t || typeof t.send !== 'function' || typeof t.close !== 'function') {
        report(new TypeError('upstream must return a transport: { send, close, onopen, onmessage, onclose }'));
        return dialFailedWrite(sock, w);
      }
      w.t = t;
      w.dialTimer = later(() => {
        if (sock.write !== w || w.open) return;
        dialFailedWrite(sock, w);
      }, dialTimeout);
      t.onopen = () => guard(() => {
        if (sock.write !== w || w.t !== t) return;
        clearTimeout(w.dialTimer);
        w.dialTimer = null;
        w.open = true;
        sock.writeDelay = 0;
        const queued = w.queue;
        w.queue = [];
        for (const message of queued) t.send(message);
      });
      t.onmessage = message => guard(() => {
        if (sock.write === w && w.t === t) fromWrite(sock, w, message);
      });
      t.onclose = info => guard(() => {
        if (sock.write !== w || w.t !== t) return;
        if (!w.open && info?.code !== UNAUTHORIZED) return dialFailedWrite(sock, w);
        failWrite(sock, w, info);
      });
    };
    if (sock.writeDelay > 0) w.dialTimer = later(() => { w.dialTimer = null; dialNow(); }, sock.writeDelay);
    else dialNow();
    return w;
  }

  /** A write dial that failed: dialled again after a pause, the queue kept (the client's hellos and ops wait in order) */
  function dialFailedWrite(sock, w) {
    clearTimeout(w.dialTimer);
    w.dialTimer = null;
    const t = w.t;
    w.t = null;
    if (t) {
      t.onopen = t.onmessage = t.onclose = null;
      try { t.close(); } catch { /* it is going anyway */ }
    }
    if (sock.write !== w) return;
    sock.writeDelay = Math.min(sock.writeDelay ? sock.writeDelay * 2 : 250, 5_000);
    w.dialTimer = later(() => {
      w.dialTimer = null;
      w.dial();
    }, sock.writeDelay);
  }

  /**
   * A client's socket up is gone (or its queue overflowed). The server
   * turning the credential away (4401) is the client's socket turned away;
   * otherwise every session with something up there whose fate is not
   * known says hello again, which carries its outbox in order
   */
  function failWrite(sock, w, info) {
    if (sock.write === w) sock.write = null;
    clearTimeout(w.dialTimer);
    clearTimeout(w.idleTimer);
    const t = w.t;
    w.t = null;
    if (t) {
      t.onopen = t.onmessage = t.onclose = null;
      try { t.close(); } catch { /* it is going anyway */ }
    }
    if (info?.code === UNAUTHORIZED) {
      forgetKey(sock.key);
      return shut(sock, UNAUTHORIZED, info.reason || 'Unauthorized');
    }
    if (info?.code === REAUTHENTICATE) return shut(sock, REAUTHENTICATE, 'Reauthenticate');
    for (const [id, session] of [...sock.locals]) {
      const unsure = session.inFlight || session.awaiting.length || session.sent > session.acked || session.held.length;
      session.writing = false;
      session.awaiting = [];
      if (unsure && (session.answered || session.inFlight)) tellAgain(sock, id, session);
    }
  }

  /** Close a client's socket up, saying nothing to its sessions (the relay goes local, or the socket closes) */
  function closeWrite(sock) {
    const w = sock.write;
    if (!w) return;
    sock.write = null;
    clearTimeout(w.dialTimer);
    clearTimeout(w.idleTimer);
    const t = w.t;
    w.t = null;
    for (const session of sock.locals.values()) {
      session.writing = false;
      session.awaiting = [];
    }
    if (!t) return;
    t.onopen = t.onmessage = t.onclose = null;
    try { t.close(); } catch (err) { report(err); }
  }

  /** Nothing to wait for up a client's socket: closed `writeIdle` later, unless something goes up meanwhile */
  function idleWrite(sock) {
    const w = sock.write;
    if (!w || w.idleTimer) return;
    for (const session of sock.locals.values()) {
      if (session.inFlight || session.awaiting.length || session.sent > session.acked || session.held.length) return;
    }
    w.idleTimer = later(() => {
      w.idleTimer = null;
      if (sock.write !== w) return;
      for (const session of sock.locals.values()) {
        if (session.inFlight || session.awaiting.length || session.sent > session.acked || session.held.length) return;
      }
      closeWrite(sock);
    }, writeIdle);
  }

  /** The keepalive's turn on a client's socket up: one silent for two of them is taken for gone */
  function keepWriteAlive(sock) {
    const w = sock.write;
    if (!w?.open || !w.t) return;
    w.silent = w.heard ? 0 : w.silent + 1;
    if (w.silent >= 2) return failWrite(sock, w, { code: 1006 });
    w.heard = false;
    try {
      w.t.send({ t: 'ping' });
    } catch (err) {
      report(err);
    }
  }

  /** What the server says up a client's own socket: answers to hellos, acks, refusals */
  function fromWrite(sock, w, message) {
    w.heard = true;
    if (!isPlainObject(message)) return;
    const id = message.store;
    if (id === undefined) {
      // The socket turned away (not signed in, or no longer): its close (4401) follows
      if (message.t === 'closed') forgetKey(sock.key);
      return;
    }
    const session = isStoreId(id) ? sock.locals.get(id) : undefined;
    if (!session) return;
    switch (message.t) {
      case 'snapshot':
      case 'delta': {
        heardServer(message.ts);
        if (Number.isInteger(message.seq)) session.acked = Math.max(session.acked, message.seq);
        const whose = session.awaiting.shift();
        if (whose !== 'client' || !session.inFlight) return idleWrite(sock);   // the answer to the relay's own write hello
        // The answer to the client's hello: on to it as it came, and the copy's patches past it after
        flushHeld(sock, session, copies.get(id));
        session.inFlight = false;
        session.waiting = null;
        session.answered = true;
        session.toldLocal = false;
        session.cursor = { v: message.v, epoch: typeof message.epoch === 'string' ? message.epoch : null };
        deliver(sock, message);
        afterAnswer(sock, id, session);
        const copy = copies.get(id);
        if (copy) bringUp(sock, copy, session);
        return idleWrite(sock);
      }
      case 'ack': {
        heardServer(message.ts);
        const copy = copies.get(id);
        // A server from before acks carried a version: passed on as it comes
        if (!Number.isInteger(message.v)) return deliverAck(sock, copy, session, message);
        session.held.push(message);
        if (session.answered) releaseAcks(sock, copy, session);
        return;
      }
      case 'error':
        if (Number.isInteger(message.seq)) {
          session.acked = Math.max(session.acked, message.seq);
          // Behind the acks the server sent before it, as a direct socket hears them
          if (session.held.length) {
            session.held.push(message);
            return;
          }
        } else {
          // A hello refused (too many, say): its answer is not coming. The client's hears it and says hello again
          // after the server's retryAfter; the relay's own opens the write-only session again with the next op
          // (either way the next op says a write hello first: a session the server never had a hello of would follow the store)
          const whose = session.awaiting.shift();
          if (whose === 'client') {
            session.inFlight = false;
            session.waiting = null;
          }
          if (whose) session.writing = session.awaiting.length > 0;
        }
        deliver(sock, message);
        return idleWrite(sock);
      case 'closed':
        session.writing = false;
        session.awaiting = [];
        // Unloaded, or no relay serves the replica on the server just then (a vouch lost with the link): hello again
        if (message.code === 'unavailable') return tellAgain(sock, id, session, message.message);
        return endFan(sock, id, session, message.code, message.message, { tell: true });
    }
  }

  // --- Fan-out: modes ----------------------------------------------------------------------------------

  /**
   * The relay goes local: a fan-out socket's sessions are answered from the
   * copy from here, as any local socket's. Its socket up closes (its ops
   * stay in its outbox, for its hello once the server is back); a client
   * answered already is told so; one waiting for an answer is answered from
   * the copy if it may be, and one whose hello had gone up says hello again
   */
  function fanToLocal(sock) {
    closeWrite(sock);
    for (const [id, session] of [...sock.locals]) {
      if (sock.locals.get(id) !== session) continue;
      // The link still up (the host said the server is away): the server lets go of the client too (forgetGrant),
      // and it is vouched for afresh when the relay goes through again
      forgetGrant(session);
      flushHeld(sock, session, copies.get(id));
      session.parked = [];
      if (session.inFlight) {
        tellAgain(sock, id, session, 'The server is away; say hello again');
        continue;
      }
      if (session.answered) {
        // Behind the copy (patches of a socket passed through, which it was not let in for again while the link was
        // down, or that had not reached it yet): it says hello again, answered from the copy as any client offline is
        const copy = copies.get(id);
        if (copy && session.cursor && (session.cursor.epoch !== copy.epoch || session.cursor.v < copy.v)) {
          tellAgain(sock, id, session, 'The server is away; say hello again');
          continue;
        }
        session.toldLocal = true;
        deliver(sock, { t: 'relay', store: id, status: 'local' });
        if (copy?.presenceOn && session.presence) deliver(sock, { t: 'presence', store: id, peers: peersOf(copy) });
        continue;
      }
      if (session.waiting) fanLocal(sock, id, session.waiting);
    }
  }

  /**
   * The relay goes through again (the link is up). A client that sent an
   * op meanwhile, or whose copy took edits offline, or that the relay
   * answered on its own, says hello again, so that its outbox reaches the
   * server in order and the server's answer replaces the relay's; the rest
   * are vouched for again and brought along from where they are, and told
   * the relay answers through no longer on its own
   */
  function fanToThrough(sock) {
    for (const [id, session] of [...sock.locals]) {
      if (!session.answered) continue;
      const copy = copies.get(id);
      if (copy?.diverged || session.sent > session.acked || session.cursor === null) tellAgain(sock, id, session, 'The server is back; say hello again');
    }
    if (linkState === 'open') resumeFan();
  }

  // --- Upkeep ----------------------------------------------------------------------------------------

  // The relay's own pings on every socket it passes through, sharing the
  // way with the client's: a pong for one of its own is not passed down,
  // and a server silent for two intervals is taken for gone, the socket
  // closed and the client reconnecting
  const keepaliveTimer = keepalive ? setInterval(() => guard(() => {
    keepLinkAlive();
    for (const sock of [...sockets]) {
      keepWriteAlive(sock);
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
    for (const [key, until] of [...turnedAway]) if (until <= Date.now()) turnedAway.delete(key);
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
      knownReplicas = null;
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
  const anchorWriter = setInterval(() => guard(() => {
    if (anchorTs === null) return;
    anchorDue = true;
    scheduleSave();
  }), MINUTE);
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
    accept({ send, close, key, upstream, credential } = {}) {
      if (typeof send !== 'function' || typeof close !== 'function') throw new TypeError('accept needs send and close functions');
      if (typeof upstream !== 'function') throw new TypeError('accept needs an upstream transport factory');
      // Fanned out when the relay has a link that answers and the client a credential to be vouched for by
      const fanned = fanOut && !linkBroken && isPlainObject(credential);
      const sock = {
        send,
        close,
        key: typeof key === 'string' && key ? key : null,
        upstream,
        credential: isPlainObject(credential) ? credential : null,
        name: `s${++socketSeq}`,   // the server's word for it, in its vouches (see server/relays.js)
        write: null,          // fan-out: its own socket up, for its edits (see writeConnection)
        writeDelay: 0,
        state: fanned ? 'fan' : mode === 'local' ? 'local' : 'dialing',
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
      // A credential the server turned away a moment ago is turned away here, without asking again
      else if (sock.state === 'fan' && sock.key !== null && (turnedAway.get(sock.key) ?? 0) > Date.now()) shut(sock, UNAUTHORIZED, 'Unauthorized');
      else if (sock.state === 'fan' && linkState === 'down' && !linkRedialTimer) dialLink();
      return {
        receive: message => guard(() => {
          if (!isPlainObject(message)) return;
          if (sock.state === 'fan') return fan(sock, message);
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
        // The link's opening brings it through (and its failing, the clients through past it); a link broken is tried again at once
        if (fanOut && linkState !== 'open') {
          kickLink();
          if (!linkBroken) return;
        }
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
      const counts = { dialing: 0, through: 0, local: 0, fan: 0 };
      for (const sock of sockets) counts[sock.state]++;
      let live = 0;
      let diverged = 0;
      let shared = 0;
      for (const copy of copies.values()) {
        if (copy.live) live++;
        if (copy.diverged) diverged++;
        if (copy.shared === 'open') shared++;
      }
      const stats = { mode, downSince, sockets: counts, copies: copies.size, live, diverged, credentials: known.size };
      // Fan-out: the link's state ('broken': the server answers the clients, not it), the stores read on it, the clients vouched for
      if (fanOut) stats.link = { state: linkState === 'refused' ? 'refused' : linkBroken ? 'broken' : linkState, shared, clients: grantsOut.size };
      return stats;
    },

    /** Every client socket: its credential's fingerprint, its state, the stores it has open, and for how long (ms) */
    sockets() {
      const at = performance.now();
      return [...sockets].map(sock => ({
        key: sock.key,
        state: sock.state,
        stores: [...(sock.state === 'local' || sock.state === 'fan' ? sock.locals.keys() : sock.stores.keys())],
        openMs: at - sock.openedAt
      }));
    },

    /** Write what changed now, the server's clock with it */
    flush() {
      clearTimeout(saveTimer);
      saveTimer = null;
      anchorDue = true;
      save();
    },

    /** Let go of the copies and credentials unused for `forgetAfter`; runs every hour on its own */
    sweep: () => guard(sweep),

    /** Close every socket (code 1001), stop, and write what changed */
    close() {
      if (closed) return;
      closed = true;
      for (const timer of [graceTimer, probeTimer, switchTimer, saveTimer, linkDialTimer, linkRedialTimer, linkGraceTimer]) clearTimeout(timer);
      for (const copy of copies.values()) {
        clearTimeout(copy.lingerTimer);
        clearTimeout(copy.sharedRetry);
      }
      clearInterval(keepaliveTimer);
      clearInterval(sweeper);
      clearInterval(anchorWriter);
      saveTimer = null;
      for (const sock of [...sockets]) shut(sock, 1001, 'The relay is shutting down');
      const t = linkUp;
      linkUp = null;
      linkState = 'down';
      try { t?.close(); } catch (err) { report(err); }
      anchorDue = true;
      save();
    }
  };
}
