// store.js - The authority: merges ops, holds the state, serves sessions
//
// A store owns one state tree (a LazyWatch instance), the per-path clocks
// the merge decides with, and the last sequence number seen from each
// replica (so a resent op is ignored). It is transport-agnostic: a
// session is created with a `send` function and fed parsed messages.
//
// Persistence is row-oriented (see storage.js): every accepted op is
// committed as the rows it won and the clock entries it dropped. On load
// the state is `initial` with the persisted rows applied on top, so
// `initial` acts as the skeleton (the containers an app expects to exist)
// and rows carry the data.
//
// Client ops pass three gates before the merge sees them, in this order:
// - the clock guard: an op stamped more than `maxSkew` ahead of the
//   server's clock is refused (code 'clock-skew', with the server's time
//   so the client can correct itself), since a fast clock would otherwise
//   win every conflict and drag the server's clock along
// - write authorization: a leaf at or under a `readOnly` path refuses the
//   op (`readOnly: true` locks the whole store), and `validate(diff, {
//   user, replicaId, store })` may refuse it or hand back a trimmed diff
//   to accept instead (code 'forbidden')
// - retention: an op older than `retention` is refused (code 'expired'),
//   because deletions older than that may have been compacted away and
//   the op could resurrect what they removed. Policy is judged first so
//   an op the store would refuse anyway is told so, not that it is old
// The server's own writes (`patch`, and `apply` called without a session)
// skip all three. Compaction of old tombstones and idle replicas runs by
// itself once an hour of store time has passed since the last one.
//
// Sessions may carry a `user` (whatever the transport authenticated). The
// store broadcasts PRESENCE, the distinct users with a live session, when
// sessions with a user open and close, and `closeSessions` evicts sessions
// by predicate with a `closed` message the client treats as final.
//
// Protocol (client -> server):
//   { t: 'hello', replicaId, ops: [op...], since? }   connect or reconnect:
//       the client's whole outbox, and the store version it last saw; the
//       server merges the ops and replies with what the client is missing
//   { t: 'op', op }                           one live batch
//   { t: 'share', data }                      what this session shares with
//       every peer (see presence below); null clears it. A hello may carry
//       it too, as `share`, so a reconnect restores it, and `presence:
//       false` in a hello asks not to be sent presence at all. Refused
//       with 'forbidden' while the store's presence is off, and with
//       'rate-limited' beyond the replica's bucket (the same one ops use)
//   { t: 'ping' }
// where op = { replicaId, seq, ts, diff }.
//
// Server -> client:
//   { t: 'snapshot', state, ts, seq, registers, v }  full state; `seq` is
//       the last op of this replica the server holds, so the client can
//       drop acknowledged outbox entries; `registers` are the server's
//       register patterns, for the client to check against its own; `v` is
//       the store version the state reflects
//   { t: 'delta', patches, ts, seq, registers, v }  instead of a snapshot
//       when the client's `since` is recent enough: the accepted diffs
//       since then, in order, followed by corrections for the hello's own
//       ops. The store keeps the last `deltaLog` accepted diffs for this
//       (in memory: after a restart the first reconnect is a snapshot), and
//       answers with a snapshot when a hello op was refused, since the
//       client then holds an edit the server never will
//   { t: 'patch', diff, ts, v }                 an accepted diff (from any
//       replica, the receiving one included) and the version it made
//   { t: 'ack', seq, ts, correction }           `correction` is a diff with
//       the server's values at the leaves the op lost, or null
//   { t: 'presence', peers }                    every session as a peer
//       { replicaId, user, key, data }: `key` is what presence groups
//       users by, `data` what the session shares. Sent to a session right
//       after its hello is answered; from then on it gets
//   { t: 'presence', left?, joined?, shared? }  what changed, applied in
//       that order: replica ids gone, peers arrived, { replicaId, data }
//       for peers sharing anew. Changes are batched (see presence.every);
//       nothing here is written, and none of it is sent while the store's
//       presence is off (the default)
//   { t: 'closed', code, message }              this session is over
//       (code 'evicted', or 'unavailable' when the store is disposed under
//       it, which the client answers with a new hello; hubs also send
//       'forbidden', 'unknown-store', 'invalid-store')
//   { t: 'error', seq?, code?, message, now?, ts? }  a refused op carries its
//       seq and a code: 'invalid' (breaks the model), 'clock-skew' (with
//       the server's `now` and the op's `ts`), 'expired', 'forbidden'
//   { t: 'pong' }
import { LazyWatch } from 'lazy-watch';
import { createClock, isTimestamp } from '../core/hlc.js';
import { registerSet, pathKey, parsePathKey, setAt, valueAt } from '../core/paths.js';
import { leaves, assertModel, rebuild, expandRegisters, replacingRegisters } from '../core/model.js';
import { mergeOp, compactTombstones } from '../core/merge.js';
import { ClockMap } from '../core/clocks.js';
import { memoryStorage, assertDocument } from './storage.js';
import { toJSON, presetJSON, SNAPSHOT_THRESHOLD } from './wire.js';
import { randomId } from '../core/ids.js';

const { Utils } = LazyWatch;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** The most ops one hello carries; the client sends the rest once it is answered */
export const HELLO_OPS = 1000;

/** A client op the store would not merge; `code` travels to the client */
/** The `presence` option as the store uses it: null when off, else its hooks and limits with the defaults filled in */
function presenceOptions(presence) {
  if (!presence) return null;
  const given = presence === true ? {} : presence;
  if (!Utils.isPlainObject(given)) throw new TypeError('presence must be true, false, or an options object');
  const { key = defaultPresenceKey, user = u => u, validate, every = 0, maxShare = 4096 } = given;
  if (typeof key !== 'function' || typeof user !== 'function') throw new TypeError('presence.key and presence.user must be functions');
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('presence.validate must be a function');
  if (!(every >= 0)) throw new TypeError('presence.every must be a number of milliseconds');
  if (!(maxShare > 0)) throw new TypeError('presence.maxShare must be a positive number of bytes');
  return { key, user, validate, every, maxShare };
}

export class RefusedError extends Error {
  constructor(code, message, extra) {
    super(message);
    this.name = 'RefusedError';
    this.code = code;
    Object.assign(this, extra);
  }
}

/** Replica progress from a saved document: `{ replicas: { id: { seq, seen, owner? } } }` */
function loadReplicas(saved) {
  const replicas = new Map();
  for (const [id, r] of Object.entries(saved?.replicas ?? {})) {
    replicas.set(id, typeof r.owner === 'string' ? { seq: r.seq, seen: r.seen, owner: r.owner } : { seq: r.seq, seen: r.seen });
  }
  return replicas;
}

/**
 * A persisted delta log is usable only where it is contiguous and ends at
 * the current version; the longest such suffix is kept, capped at `limit`.
 * Anything else (a gap, a log that stops short) would answer a reconnect
 * with a delta missing ops, so it is dropped and snapshots serve instead.
 */
function restoreLog(entries, version, limit) {
  if (!Array.isArray(entries) || limit <= 0) return [];
  const sorted = entries
    .filter(e => Utils.isPlainObject(e) && Number.isInteger(e.v) && Utils.isPlainObject(e.diff))
    .sort((a, b) => a.v - b.v);
  if (sorted.length === 0 || sorted[sorted.length - 1].v !== version) return [];
  let start = sorted.length - 1;
  while (start > 0 && sorted[start - 1].v === sorted[start].v - 1) start--;
  return sorted.slice(Math.max(start, sorted.length - limit));
}

/** Users are distinct by `id` when they have one, else by value */
const defaultPresenceKey = user =>
  (user !== null && typeof user === 'object' && user.id != null ? String(user.id) : JSON.stringify(user));

/**
 * @param {Object} [options]
 * @param {Object} [options.initial] - the skeleton: state when nothing is
 *   persisted, and the base persisted rows are applied onto
 * @param {Array<string|string[]>} [options.registers] - paths whose value
 *   is one unit (arrays live only here); `*` matches one segment
 * @param {Array<string|string[]>|true} [options.readOnly] - paths clients may
 *   not write, same syntax as registers; a client op touching a leaf at or
 *   under one is refused whole. `true` locks the whole store: clients only
 *   read. The server's own `patch` is not bound
 * @param {(diff: Object, context: { user: any, replicaId: string, store: Object }) => boolean|Object|void} [options.validate]
 *   - judges every client op after the read-only check: return `false` or
 *   throw to refuse it (the error's message reaches the client), return a
 *   diff to accept that instead (leaves it leaves out are corrected on the
 *   client), or `true` / nothing to accept it as is. Synchronous
 * @param {number} [options.maxSkew=300000] - how far ahead of the server's
 *   clock (ms) a client op may be stamped; further is refused with code
 *   'clock-skew'. `Infinity` disables the guard
 * @param {number} [options.retention=2592000000] - how long (ms, default 30
 *   days) deletions and idle replicas are remembered; an op older than
 *   this is refused with code 'expired'. `Infinity` keeps everything
 * @param {number} [options.compactEvery=3600000] - how often (ms of store
 *   time) compaction runs on its own, checked as ops arrive
 * @param {number} [options.deltaLog=1000] - how many accepted diffs to keep
 *   for answering a reconnect with a delta instead of a snapshot; 0 always
 *   sends snapshots
 * @param {number} [options.maxLeaves=10000] - the most leaves one client op
 *   may touch; a larger one is refused with code 'too-large'
 * @param {{ burst: number, perSecond: number }|false} [options.rateLimit] -
 *   live ops a user may send: a token bucket holding `burst` tokens,
 *   refilled at `perSecond` (default 500 and 100), per user (by presence's
 *   key), or per replica for a session without a user. An op beyond it is
 *   refused with code 'rate-limited' and a `retryAfter` in ms; the client
 *   keeps the op and resends its outbox in a hello after that. A hello and
 *   a share cost one token each; the ops inside a hello are not counted,
 *   and a hello carries at most HELLO_OPS of them (the rest wait for the
 *   next). `false` disables
 * @param {Array<(state: Object, context: { store: Object }) => Object|void>} [options.migrations] -
 *   changes to the shape of the state, in order, each run once per store:
 *   it is handed a copy of the state and returns a diff (applied as the
 *   server's own `patch`, so it is persisted, logged, and sent to every
 *   client) or nothing. How many have run is stored with the store's rows,
 *   in the same commit as the migration's, so a store loaded later picks
 *   up where it stopped; a new store starts with them all done (`initial`
 *   is in the latest shape), and one stored before migrations were given
 *   runs them all. A store whose storage has run more migrations than this
 *   list holds (code rolled back after a newer one migrated it) is refused
 *   with code 'schema-ahead', rather than written in the older shape
 * @param {Object} [options.storage] - a storage adapter (default: memory)
 * @param {boolean|Object} [options.presence=false] - whether sessions
 *   learn of each other. Off, nothing is broadcast, `presence()` and
 *   `peers()` are empty, and a share is refused with 'forbidden'. `true`
 *   turns it on with the defaults below; an object sets any of:
 * @param {(user: any) => string} [options.presence.key] - what presence
 *   groups users by (default: `id`, else the user's JSON)
 * @param {(user: any) => any} [options.presence.user] - what of a user its
 *   peers see (default: all of it); the server's own `presence()` shows
 *   the same
 * @param {(data: any, context: { user: any, replicaId: string, store: Object }) => boolean|any|void} [options.presence.validate]
 *   judges what a session shares, the way `validate` judges an op: return
 *   false or throw to refuse it (code 'forbidden', the error's message
 *   reaching the client), return a value to share that instead, return
 *   true or nothing to let it through. Clearing a share is never judged
 * @param {number} [options.presence.every=0] - at most one presence message
 *   per this many milliseconds (wall clock): changes within the window go
 *   out together, a session's later share replacing its earlier one. 0
 *   still sends what one turn of the event loop brought as one message
 * @param {number} [options.presence.maxShare=4096] - the most a session
 *   may share, in bytes of JSON; more is refused with 'too-large'
 * @param {(error: any) => void} [options.onError] - where faults that are
 *   not a client's (an observer that throws) are reported; default console
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createStore({
  initial = {},
  registers = [],
  readOnly = [],
  validate,
  maxSkew = 5 * MINUTE,
  retention = 30 * DAY,
  compactEvery = HOUR,
  deltaLog = 1000,
  maxLeaves = 10_000,
  rateLimit = { burst: 500, perSecond: 100 },
  storage = memoryStorage(),
  migrations = [],
  presence: presenceOption = false,   // `presence` below is the list
  onError = err => console.error('lazy-storage:', err),
  now
} = {}) {
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('validate must be a function');
  const pres = presenceOptions(presenceOption);
  if (rateLimit && !(rateLimit.burst > 0 && rateLimit.perSecond > 0)) throw new TypeError('rateLimit needs positive burst and perSecond');
  const time = now ?? Date.now;
  const regs = registerSet(registers);
  const locked = readOnly === true ? null : registerSet(readOnly);   // null: the whole store is read-only
  const saved = storage.load();
  const state = new LazyWatch(rebuild(initial, saved ? saved.rows.map(([key, row]) => [key, row.deleted ? null : row.value]) : [], regs));
  const clocks = new ClockMap(saved ? saved.rows.map(([key, row]) => [key, row.deleted ? { ts: row.ts, deleted: true } : { ts: row.ts }]) : []);
  const replicas = loadReplicas(saved);
  let version = saved ? saved.version : 0;
  // Versions count from 0 for the life of a store's storage. The epoch,
  // minted with the storage, tells one life from the next, so a client
  // whose cache remembers a version of storage that has since been wiped
  // gets a snapshot, not a delta computed against a different history
  const epoch = saved ? saved.epoch : randomId();
  if (!Array.isArray(migrations) || migrations.some(m => typeof m !== 'function')) throw new TypeError('migrations must be an array of functions');
  // How many migrations the stored rows have been through; see migrate() below
  let schema = saved ? (Number.isInteger(saved.schema) ? saved.schema : 0) : migrations.length;
  if (schema > migrations.length) {
    storage.close?.();
    const err = new Error(`The store's storage has run ${schema} migrations and this code knows ${migrations.length}: it was migrated by a newer version, and is not served in an older shape`);
    err.code = 'schema-ahead';
    throw err;
  }
  const clock = createClock('server', now);
  const sessions = new Set();
  let serverSeq = replicas.get('server')?.seq ?? 0;
  let lastCompaction = -Infinity;
  // The last `deltaLog` accepted diffs, as { v, diff }; an adapter that
  // persists them hands them back on load, so a restart still answers
  // reconnects with deltas
  const log = restoreLog(saved?.log, version, deltaLog);
  const buckets = new Map();  // replicaId -> { tokens, at }, for the rate limit
  const observers = { op: new Set(), refused: new Set(), session: new Set() };
  // The state as JSON, encoded straight from the proxy's plain target (a
  // deep copy through the proxy costs several times more) and kept until
  // the next accepted op, so a burst of reconnects pays for one encoding
  let stateJSON = null;
  let self;

  function encodedState() {
    if (stateJSON === null) {
      const plain = typeof LazyWatch.resolveIfProxy === 'function' ? LazyWatch.resolveIfProxy(state) : LazyWatch.snapshot(state);
      stateJSON = JSON.stringify(plain);
    }
    return stateJSON;
  }

  function notify(event, payload) {
    for (const fn of observers[event]) {
      try {
        fn(payload);
      } catch (err) {
        onError(err);
      }
    }
  }

  /**
   * Take one token from a replica's bucket. Returns 0 when the op may
   * proceed, else the milliseconds until a token is back.
   */
  function throttle(replicaId) {
    if (!rateLimit) return 0;
    const wall = time();
    let bucket = buckets.get(replicaId);
    if (!bucket) {
      if (buckets.size >= 10_000) {
        for (const [id, b] of buckets) if (wall - b.at > MINUTE) buckets.delete(id);
      }
      bucket = { tokens: rateLimit.burst, at: wall };
      buckets.set(replicaId, bucket);
    }
    bucket.tokens = Math.min(rateLimit.burst, bucket.tokens + ((wall - bucket.at) / 1000) * rateLimit.perSecond);
    bucket.at = wall;
    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      return 0;
    }
    return Math.ceil(((1 - bucket.tokens) / rateLimit.perSecond) * 1000);
  }

  function broadcast(message) {
    toJSON(message);  // encoded once, however many sessions there are
    // A session whose transport can fan out (Bun's topic publish, which
    // compresses once) hears it through one publish, which reaches every
    // such session at once; the rest are sent to one by one
    let published = false;
    for (const s of sessions) {
      if (!s.publish) s.send(message);
      else if (!published) {
        s.publish(message);
        published = true;
      }
    }
  }

  /**
   * Every commit carries the version and epoch alongside its rows. A
   * commit that fails (a full disk, a database locked past its timeout)
   * leaves memory ahead of disk, with the change never broadcast: the
   * store unloads itself rather than serve a state it cannot keep. Its
   * sessions hear `unavailable` and say hello again, their unacknowledged
   * ops with them, to a store a registry loads afresh from what is on disk
   */
  function commit(change) {
    try {
      storage.commit({ ...change, version, epoch, schema });
    } catch (err) {
      onError(err);
      self.dispose();
      throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
    }
  }

  // A session counts from its hello: that is when it has a replica id to
  // be listed as a peer by, and with a hub, the message it was created for.
  // Users are shown as `presence.user` renders them, here and to peers
  function presence() {
    const seen = new Map();
    if (!pres) return [];
    for (const s of sessions) {
      if (s.user === undefined || !s.replicaId) continue;
      const key = pres.key(s.user);
      if (!seen.has(key)) seen.set(key, pres.user(s.user));
    }
    return [...seen.values()];
  }

  /** A session as its peers see it: replica id, user with the key presence groups by, and what it shares */
  function peerOf(s) {
    const peer = { replicaId: s.replicaId };
    if (s.user !== undefined) {
      peer.user = pres.user(s.user);
      peer.key = pres.key(s.user);
    }
    if (s.shared !== undefined) peer.data = s.shared;
    return peer;
  }

  /** Every session that has said hello (none while presence is off) */
  function peers() {
    const list = [];
    if (!pres) return list;
    for (const s of sessions) if (s.replicaId) list.push(peerOf(s));
    return list;
  }

  // Presence travels as deltas. A session joining, leaving, or sharing
  // anew is noted here and goes out with the next flush, at most once per
  // `presence.every`, so a burst (a deploy's reconnect storm, a busy room's
  // shares) costs every socket one small message per window rather than
  // the whole list per change; within a window a session's later share
  // replaces its earlier one, and a join followed by a leave is just the
  // leave (harmless where the join was never heard of). Only a newcomer
  // gets the whole list, right after its hello is answered.
  let pending = new Map();   // session -> { joined, left, shared } since the last flush
  let cancelFlush = null;    // cancels the scheduled flush; null when none is
  let flushedAt = -Infinity;

  function noteChange(s, what) {
    if (!pres) return;
    let entry = pending.get(s);
    if (!entry) pending.set(s, (entry = { joined: false, left: false, shared: false }));
    if (what === 'leave') Object.assign(entry, { joined: false, shared: false, left: true });
    else entry[what === 'join' ? 'joined' : 'shared'] = true;
    if (cancelFlush) return;
    const wait = pres.every > 0 ? Math.max(0, flushedAt + pres.every - time()) : 0;
    let timer;
    if (wait > 0) {
      timer = setTimeout(flushPresence, wait);
      cancelFlush = () => clearTimeout(timer);
    } else if (typeof setImmediate === 'function') {
      // After this turn of the event loop, so what one poll of the sockets brought goes out together
      timer = setImmediate(flushPresence);
      cancelFlush = () => clearImmediate(timer);
    } else {
      timer = setTimeout(flushPresence, 0);
      cancelFlush = () => clearTimeout(timer);
    }
    if (typeof timer?.unref === 'function') timer.unref();
  }

  function flushPresence() {
    cancelFlush = null;
    flushedAt = time();
    const changes = pending;
    pending = new Map();
    const joined = [];
    const left = [];
    const shared = [];
    for (const [s, entry] of changes) {
      if (entry.left) left.push(s.replicaId);
      else if (entry.joined) joined.push(peerOf(s));
      else if (entry.shared) shared.push(s.shared === undefined ? { replicaId: s.replicaId } : { replicaId: s.replicaId, data: s.shared });
    }
    if (!joined.length && !left.length && !shared.length) return;
    const message = { t: 'presence' };
    if (left.length) message.left = left;
    if (joined.length) message.joined = joined;
    if (shared.length) message.shared = shared;
    // Encoded once; a session that opted out of presence in its hello is skipped
    toJSON(message);
    for (const s of sessions) if (s.presence) s.send(message);
  }

  /**
   * Set what a session shares (JSON within maxShare, as `presence.validate`
   * lets it through or replaces it; null clears); true when it changed
   */
  function setShared(s, data) {
    if (!pres) throw new RefusedError('forbidden', 'Presence is off on this store');
    let json;
    if (data !== null && data !== undefined) {
      json = JSON.stringify(data);
      if (json === undefined) throw new RefusedError('invalid', 'Shared data must be JSON');
      if (json.length > pres.maxShare) throw new RefusedError('too-large', `Shared data over ${pres.maxShare} bytes`);
      if (pres.validate) {
        let verdict;
        try {
          verdict = pres.validate(data, { user: s.user, replicaId: s.replicaId, store: self });
        } catch (err) {
          throw new RefusedError('forbidden', err?.message || 'Share refused');
        }
        if (verdict === false) throw new RefusedError('forbidden', 'Share refused');
        if (verdict !== true && verdict !== undefined) {
          data = verdict;
          json = JSON.stringify(data);
          if (json === undefined) {
            onError(new TypeError('presence.validate must return true, false, a JSON value, or nothing'));
            throw new RefusedError('forbidden', 'Share refused');
          }
        }
      }
    }
    if (json === s.sharedJson) return false;
    s.shared = json === undefined ? undefined : data;
    s.sharedJson = json;
    return true;
  }

  /**
   * The server's current values at rejected paths, as a diff the losing
   * client applies to fall back in line. A rejected path whose record no
   * longer exists is corrected at the shallowest missing ancestor with a
   * deletion, so the client drops the whole record rather than keeping an
   * empty shell.
   */
  function correction(paths) {
    // Read where the paths lead, and copy only that: a copy of the whole
    // state per op that lost a leaf cost the state's size per conflict
    const raw = LazyWatch.resolveIfProxy(state);
    const entries = new Map();
    for (const path of paths) {
      let corrected = false;
      for (let i = 1; i <= path.length; i++) {
        const sub = path.slice(0, i);
        const key = pathKey(sub);
        if (entries.has(key)) { corrected = entries.get(key) === null; if (corrected) break; continue; }
        const value = valueAt(raw, sub);
        if (value === undefined) { entries.set(key, null); corrected = true; break; }
      }
      if (!corrected) entries.set(pathKey(path), structuredClone(valueAt(raw, path)));
    }
    const diff = {};
    // Shallow entries first so a deletion is not overwritten by a deeper value
    for (const [key, value] of [...entries].sort((a, b) => a[0].length - b[0].length)) {
      const path = parsePathKey(key);
      if (path.slice(0, -1).some((_, i) => entries.get(pathKey(path.slice(0, i + 1))) === null)) continue;
      setAt(diff, path, value);
    }
    return diff;
  }

  function assertOp(op) {
    if (!Utils.isPlainObject(op)) throw new TypeError('An op must be an object');
    if (typeof op.replicaId !== 'string' || !op.replicaId) throw new TypeError('op.replicaId must be a string');
    if (!Number.isInteger(op.seq) || op.seq < 1) throw new TypeError('op.seq must be a positive integer');
    if (!isTimestamp(op.ts)) throw new TypeError('op.ts must be a timestamp [ms, count, replicaId]');
    if (!Utils.isPlainObject(op.diff)) throw new TypeError('op.diff must be a plain object');
  }

  /** True when some read-only pattern matches the path or one of its ancestors; always, for a store locked whole */
  function underReadOnly(path) {
    if (!locked) return true;
    for (let i = 1; i <= path.length; i++) if (locked.matches(path.slice(0, i))) return true;
    return false;
  }

  /**
   * The gates a client op passes before the merge (see the header). Returns
   * the diff to merge and the leaves of the original the validator left
   * out, which the client is corrected on.
   */
  function admit(op, session) {
    const entries = leaves(op.diff, regs);
    if (entries.length > maxLeaves) {
      throw new RefusedError('too-large', `The op touches ${entries.length} leaves; the store accepts at most ${maxLeaves} in one op`);
    }
    const wall = time();
    if (op.ts[0] > wall + maxSkew) {
      throw new RefusedError('clock-skew',
        `The op is stamped ${Math.round((op.ts[0] - wall) / 1000)} s ahead of the server's clock`, { now: wall, ts: op.ts });
    }
    // A top-level container of `initial` is the skeleton every client
    // expects: deleted, its tombstone would refuse every write under it
    // (none of them a record write that lifts it) for the whole retention
    const skeleton = entries.find(([path, value]) => value === null && path.length === 1 && Object.hasOwn(initial, path[0]) && Utils.isPlainObject(initial[path[0]]));
    if (skeleton) {
      throw new RefusedError('forbidden', `"${skeleton[0][0]}" is one of the store's top-level containers: clear what is in it rather than delete it`);
    }
    const lockedLeaf = entries.find(([path]) => underReadOnly(path));
    if (lockedLeaf) throw new RefusedError('forbidden', locked ? `"${lockedLeaf[0].join('/')}" is read-only` : 'The store is read-only');
    const admitted = validated(op, session, entries);
    // Policy first, age last: an op the store would refuse anyway hears that
    if (op.ts[0] < wall - retention) {
      throw new RefusedError('expired',
        `The op is ${Math.round((wall - op.ts[0]) / DAY)} days old, older than the store keeps history for`);
    }
    return admitted;
  }

  /** The validator's verdict on an op: the diff to merge and the leaves it left out */
  function validated(op, session, entries) {
    if (!validate) return { diff: op.diff, stripped: [] };
    let verdict;
    try {
      verdict = validate(op.diff, { user: session.user, replicaId: op.replicaId, store: self });
    } catch (err) {
      throw new RefusedError('forbidden', err?.message || 'The op was refused');
    }
    if (verdict === false) throw new RefusedError('forbidden', 'The op was refused');
    if (verdict === true || verdict === undefined || verdict === op.diff) return { diff: op.diff, stripped: [] };
    if (!Utils.isPlainObject(verdict)) throw new TypeError('validate must return true, false, or a diff');
    const kept = new Set(leaves(verdict, regs).map(([path]) => pathKey(path)));
    return { diff: verdict, stripped: entries.filter(([path]) => !kept.has(pathKey(path))).map(([path]) => path) };
  }

  /**
   * Merge one op. Idempotent per (replicaId, seq): a resent op is ignored.
   * With a `session` the op is a client's and passes the gates first; the
   * server's own ops are trusted. `authority` marks the store's own patch,
   * which a tombstone never holds back (see merge.js).
   * @returns {{ duplicate: boolean, accepted: Object|null, rejected: string[][], correction: Object|null }}
   */
  function apply(op, session, { authority = false } = {}) {
    assertOp(op);
    if (session) {
      // A session speaks for the replica its hello named and no other: an
      // op under another id could claim a seq far ahead and turn that
      // replica's (or the server's own) later writes into duplicates
      const claim = claimReplica(session, op.replicaId);
      if (claim) throw new RefusedError(claim.code === 'replica-taken' ? 'forbidden' : claim.code, claim.message);
      session.speaksFor = op.replicaId;
      if (op.ts[2] !== op.replicaId) throw new RefusedError('invalid', 'op.ts must carry the op\'s own replica id');
    }
    maybeCompact();
    const last = replicas.get(op.replicaId)?.seq ?? 0;
    if (op.seq <= last) return { duplicate: true, accepted: null, rejected: [], correction: null };
    let diff = op.diff;
    let stripped = [];
    if (session) ({ diff, stripped } = admit(op, session));
    else assertModel(diff, regs);

    const seen = time();
    const owner = replicas.get(op.replicaId)?.owner ?? ownerKey(session?.user);
    replicas.set(op.replicaId, owner === undefined ? { seq: op.seq, seen } : { seq: op.seq, seen, owner });
    clock.receive(op.ts);

    const { accepted, rejected, won, dropped } = mergeOp(clocks, op.ts, diff, regs, { authority });
    // A register the op writes becomes its new value whole: what the value had and the
    // new one lacks goes, here and, through the patch sent on, in every client
    const applied = accepted ? replacingRegisters(accepted, regs, state) : null;
    if (applied) {
      LazyWatch.patch(state, applied);
      stateJSON = null;
      version++;
      if (deltaLog > 0) {
        log.push({ v: version, diff: applied });
        if (log.length > deltaLog) log.splice(0, log.length - deltaLog);
      }
    }
    commit({
      upserts: won.map(([path, value]) => [pathKey(path), value === null ? { ts: op.ts, deleted: true } : { value, ts: op.ts }]),
      deletes: dropped,
      replica: replicas.get(op.replicaId) && { id: op.replicaId, ...replicas.get(op.replicaId) },
      // The log entry this op made, and the oldest version the store still
      // keeps, so an adapter persisting the log can prune to match
      log: applied && deltaLog > 0 ? { v: version, diff: applied } : undefined,
      logFloor: log.length ? log[0].v : version + 1
    });
    if (applied) broadcast({ t: 'patch', diff: applied, ts: op.ts, v: version });
    const lost = [...rejected, ...stripped];
    if (observers.op.size) {
      notify('op', { replicaId: op.replicaId, seq: op.seq, user: session?.user, accepted: accepted !== null, rejected: lost.length, version });
    }
    return { duplicate: false, accepted, rejected: lost, correction: lost.length ? correction(lost) : null };
  }

  /**
   * Forget tombstones older than the retention window and replicas not
   * heard from within it. Safe because an op older than the window is
   * refused, so nothing can arrive that the forgotten entries would have
   * had to judge. Returns how many of each were removed.
   */
  function compact() {
    lastCompaction = time();
    if (!Number.isFinite(retention)) return { tombstones: 0, replicas: 0 };
    const horizon = lastCompaction - retention;
    const removed = compactTombstones(clocks, [horizon, 0, '']);
    const forgotten = [];
    for (const [id, r] of replicas) {
      if (id !== 'server' && r.seen < horizon) {
        replicas.delete(id);
        forgotten.push(id);
      }
    }
    if (removed.length || forgotten.length) commit({ upserts: [], deletes: removed, forgetReplicas: forgotten });
    return { tombstones: removed.length, replicas: forgotten.length };
  }

  function maybeCompact() {
    if (Number.isFinite(retention) && time() - lastCompaction >= compactEvery) compact();
  }

  // The snapshot names the server's register patterns so a client can
  // detect a declaration that differs from its own
  const registerPatterns = regs.patterns.map(p => p.join('/'));

  /**
   * A snapshot message whose JSON is assembled around the cached encoding
   * of the state, so sending it to a socket never decodes or re-encodes
   * the state; `state` is decoded lazily for a consumer that reads the
   * object (a test, an in-process transport)
   */
  function snapshotMessage(replicaId, session, lost = []) {
    const json = encodedState();
    const ts = clock.peek();
    const seq = replicas.get(replicaId)?.seq ?? 0;
    const message = { t: 'snapshot', ts, seq, registers: registerPatterns, v: version, epoch };
    if (lost.length) message.lost = lost;
    // A client that can fetch, on a transport that serves snapshots, is
    // pointed at the route when the state is large: gzipped once there
    // rather than compressed and buffered per socket here (see snapshot.js)
    const route = session?.fetch ? session.httpSnapshot : null;
    if (route && json.length >= route.threshold) return { ...message, fetch: route.url };
    let decoded;
    Object.defineProperty(message, 'state', { enumerable: true, configurable: true, get: () => (decoded ??= JSON.parse(json)) });
    return presetJSON(message,
      `{"t":"snapshot","state":${json},"ts":${JSON.stringify(ts)},"seq":${seq},"registers":${JSON.stringify(registerPatterns)},"v":${version},"epoch":${JSON.stringify(epoch)}${lost.length ? `,"lost":${JSON.stringify(lost)}` : ''}}`);
  }

  /**
   * The accepted diffs after version `since`, in order, or null when the
   * log no longer reaches back that far (or `since` is not one of ours).
   */
  function deltaSince(since) {
    if (!Number.isInteger(since) || since < 0 || since > version) return null;
    if (since === version) return [];
    const start = log.findIndex(entry => entry.v > since);
    if (start === -1 || log[start].v !== since + 1) return null;
    return log.slice(start).map(entry => entry.diff);
  }

  /**
   * The answer to a hello: a delta when the client's `since` is recent and
   * every op it sent was merged (accepted or rejected leaf by leaf, both
   * of which the delta and the corrections express), else a snapshot.
   */
  function catchUp(session, since, sinceEpoch, refused, corrections, lost = []) {
    const patches = refused || sinceEpoch !== epoch ? null : deltaSince(since);
    if (patches === null) return snapshotMessage(session.replicaId, session, lost);
    const message = { t: 'delta', patches: [...patches, ...corrections], ts: clock.peek(), seq: replicas.get(session.replicaId)?.seq ?? 0, registers: registerPatterns, v: version, epoch };
    if (lost.length) message.lost = lost;
    return message;
  }

  /** The error message for an op the store did not merge */
  function refusal(op, err, session) {
    const message = { t: 'error', seq: Utils.isPlainObject(op) ? op.seq : undefined, code: err.code ?? 'invalid', message: err.message };
    if (err.code === 'clock-skew') Object.assign(message, { now: err.now, ts: err.ts });
    if (err.code === 'rate-limited') message.retryAfter = err.retryAfter;
    if (observers.refused.size) {
      notify('refused', { replicaId: Utils.isPlainObject(op) ? op.replicaId : undefined, seq: message.seq, user: session?.user, code: message.code, message: err.message });
    }
    return message;
  }

  /**
   * Whether a session may speak for `replicaId`: null when it may, else the
   * error to send. The server's own id is reserved; a session keeps the id
   * it first spoke for (its hello's, or its first op's); and a replica
   * belongs to the user who first spoke for it, recorded with its progress,
   * so another user can take it over neither while it is connected nor
   * while it is away. A session without a user owns nothing and is not
   * held back. See takeReplica for where ownership is recorded
   */
  function claimReplica(s, replicaId) {
    if (replicaId === 'server') return { code: 'forbidden', message: 'The replica id "server" is reserved for the store' };
    if (s.speaksFor) {
      return s.speaksFor === replicaId ? null
        : { code: 'forbidden', message: `This session speaks for replica "${s.speaksFor}", not "${replicaId}"` };
    }
    const owner = replicas.get(replicaId)?.owner;
    if (owner !== undefined && owner !== ownerKey(s.user)) {
      return { code: 'replica-taken', message: 'That replica id belongs to another user' };
    }
    return null;
  }

  /**
   * Record the session's user as the owner of a replica nobody owns yet, at
   * its hello, so a replica that only reads is held too, and persist it with
   * the replica's progress
   */
  function takeReplica(s, replicaId) {
    const key = ownerKey(s.user);
    const known = replicas.get(replicaId);
    if (key === undefined || known?.owner !== undefined) return;
    const entry = { seq: known?.seq ?? 0, seen: known?.seen ?? time(), owner: key };
    replicas.set(replicaId, entry);
    commit({ upserts: [], deletes: [], replica: { id: replicaId, ...entry } });
  }

  /**
   * The rate-limit bucket a session draws on: its user's, so a client
   * cannot shed its limit by minting replica ids, or the replica's for a
   * session without a user
   */
  const bucketOf = (s, replicaId) => (s.user === undefined ? `r:${replicaId}` : `u:${ownerKey(s.user)}`);

  /** What a replica's user is told apart by, as a string: presence's key when set, else the default */
  const ownerKey = user => (user === undefined ? undefined : String((pres?.key ?? defaultPresenceKey)(user)));

  /**
   * Attach a session. `send` receives message objects; feed the session
   * parsed client messages with `receive`, and `close` it when the
   * connection ends. `user` is whatever the transport authenticated
   * (counted in presence when present, and handed to `validate`). A
   * transport that can reach every session on the store at once (see the
   * hub and the Bun adapter) hands in `broadcast`: this session then hears
   * the patch fan-out through one call of it, made for every session that
   * offered one, so any of them must reach all of them; a session without
   * one is sent to on its own. `onEvict` is called after `closeSessions`
   * closed this session, so the transport can drop the socket or the hub
   * its entry. A transport that serves snapshots over HTTP (see
   * snapshot.js) hands in `httpSnapshot: { url, threshold }`: a client
   * that says in its hello it can fetch is then pointed at `url` in place
   * of a snapshot of `threshold` bytes or more.
   */
  function session({ send, user, onEvict, broadcast: publish, httpSnapshot } = {}) {
    if (typeof send !== 'function') throw new TypeError('A session needs a send function');
    const s = {
      send,
      user,
      onEvict,
      publish: typeof publish === 'function' ? publish : null,   // hears broadcasts through the transport's fan-out
      httpSnapshot: typeof httpSnapshot?.url === 'string' ? { url: httpSnapshot.url, threshold: httpSnapshot.threshold ?? SNAPSHOT_THRESHOLD } : null,
      fetch: false,          // whether its hello said it can fetch a snapshot over HTTP
      replicaId: null,       // set by the hello: the replica presence knows it as
      speaksFor: null,       // the replica its ops must come from, fixed by its hello or its first op
      heldUntil: null,       // after a rate-limit refusal: live ops are refused until the next hello
      shared: undefined,     // what this session shares with its peers, and its JSON
      sharedJson: undefined,
      presence: true,        // whether it wants to hear of its peers (its hello may say no)
      receive(msg) {
        // A closed session (evicted, refused its replica, or its store
        // unloaded) hears nothing more, whatever still reaches it
        if (!sessions.has(s)) return;
        if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
        switch (msg.t) {
          case 'hello': {
            if (typeof msg.replicaId !== 'string' || !msg.replicaId) return send({ t: 'error', message: 'hello requires a replicaId' });
            const claim = claimReplica(s, msg.replicaId);
            if (claim?.code === 'replica-taken') {
              // Final for this store on this connection: the client's replica
              // is someone else's (a browser that signed in as another user
              // with the last one's storage, or a forgery); the app decides
              send({ t: 'closed', code: claim.code, message: claim.message });
              s.close();
              s.onEvict?.();
              return;
            }
            if (claim) return send({ t: 'error', ...claim });
            // A hello costs a token: its ops ride free, so a reconnect after
            // a long offline spell is not throttled, but a client cannot
            // replay hellos in a loop
            const wait = throttle(bucketOf(s, msg.replicaId));
            if (wait > 0) return send({ t: 'error', code: 'rate-limited', message: `Too many hellos; try again in ${wait} ms`, retryAfter: wait });
            s.speaksFor = msg.replicaId;
            s.heldUntil = null;
            try {
              takeReplica(s, msg.replicaId);
            } catch (err) {
              if (disposed) return;   // the commit failed and the store unloaded (see commit)
              throw err;
            }
            const joined = !s.replicaId;
            s.replicaId = msg.replicaId;
            if (msg.presence === false) s.presence = false;
            s.fetch = msg.fetch === true;
            let shared = false;
            if (msg.share !== undefined) {
              try {
                shared = setShared(s, msg.share);
              } catch (err) {
                send({ t: 'error', code: err.code, message: err.message });
              }
            }
            let refused = false;
            const lost = [];
            const lostByOp = [];   // { seq, paths }: what each of the hello's ops lost, for the client to tell its app
            // The client sends its first HELLO_OPS and the rest once this is answered
            for (const op of Array.isArray(msg.ops) ? msg.ops.slice(0, HELLO_OPS) : []) {
              try {
                const { rejected } = apply(op, s);
                lost.push(...rejected);
                if (rejected.length) lostByOp.push({ seq: op.seq, paths: rejected });
              } catch (err) {
                // Unloaded under us: the client resends everything to the next store
                if (disposed) return;
                refused = true;
                send(refusal(op, err, s));
              }
            }
            // One correction for every leaf the hello's ops lost, taken
            // after the last of them: a value read mid-hello could be
            // overtaken by a later op of the same hello, and the client
            // applies corrections last
            const corrections = lost.length ? [correction(lost)] : [];
            send(catchUp(s, msg.since, msg.epoch, refused, corrections, lostByOp));
            // The newcomer gets the whole picture now; everyone else hears
            // of it with the next flush. A re-hello on the same session
            // changes nothing unless it shares anew
            if (joined && pres) {
              if (s.presence) send({ t: 'presence', peers: peers() });
              noteChange(s, 'join');
            } else if (shared) {
              noteChange(s, 'share');
            }
            return;
          }
          case 'share': {
            try {
              // A share draws on the same bucket as an op, so a client
              // cannot flood the room through presence; the refused one is
              // not lost, since the client's next hello carries its latest
              const wait = s.replicaId ? throttle(bucketOf(s, s.replicaId)) : 0;
              if (wait > 0) throw new RefusedError('rate-limited', `Too many shares; try again in ${wait} ms`, { retryAfter: wait });
              if (setShared(s, msg.data) && s.replicaId) noteChange(s, 'share');
            } catch (err) {
              const message = { t: 'error', code: err.code, message: err.message };
              if (err.code === 'rate-limited') message.retryAfter = err.retryAfter;
              send(message);
            }
            return;
          }
          case 'op': {
            try {
              const replica = Utils.isPlainObject(msg.op) ? msg.op.replicaId : undefined;
              // Once an op is refused for the rate, every later live op is
              // too, until the hello that resends them in order: a later op
              // accepted first would make the refused one a duplicate, lost
              if (s.heldUntil !== null) {
                const wait = Math.max(1, s.heldUntil - time());
                throw new RefusedError('rate-limited', `Waiting for a hello after a refusal; try again in ${wait} ms`, { retryAfter: wait });
              }
              const wait = typeof replica === 'string' ? throttle(bucketOf(s, replica)) : 0;
              if (wait > 0) {
                s.heldUntil = time() + wait;
                throw new RefusedError('rate-limited', `Too many ops; try again in ${wait} ms`, { retryAfter: wait });
              }
              const result = apply(msg.op, s);
              const ack = { t: 'ack', seq: msg.op.seq, ts: clock.peek(), correction: result.correction };
              // Which of the op's leaves lost (to a newer write, a tombstone, or the validator), so the client can say so
              if (result.rejected.length) ack.lost = result.rejected;
              return send(ack);
            } catch (err) {
              // Unloaded under us (see commit): the op stays pending on the client, which says hello again
              if (disposed) return;
              return send(refusal(msg.op, err, s));
            }
          }
          case 'ping':
            return send({ t: 'pong' });
          default:
            return send({ t: 'error', message: `Unknown message type: ${msg.t}` });
        }
      },
      close() {
        if (!sessions.delete(s)) return;
        if (s.replicaId) noteChange(s, 'leave');
        if (observers.session.size) notify('session', { event: 'close', user, replicaId: s.replicaId, sessions: sessions.size });
      }
    };
    sessions.add(s);
    // Presence goes out once the session has said hello (see presence above)
    if (observers.session.size) notify('session', { event: 'open', user, replicaId: null, sessions: sessions.size });
    return s;
  }

  /**
   * Evict every session the predicate selects: it receives a `closed`
   * message (code 'evicted') and is closed; its transport is told through
   * `onEvict`. Returns how many were closed.
   * @param {(session: {user: any, replicaId: string|null}) => boolean} predicate
   * @param {string} [message]
   */
  function closeSessions(predicate, message = 'Your session was closed by the server') {
    let closed = 0;
    for (const s of [...sessions]) {
      if (!predicate(s)) continue;
      try {
        s.send({ t: 'closed', code: 'evicted', message });
      } catch { /* the transport may already be gone */ }
      s.close();
      s.onEvict?.();
      closed++;
    }
    return closed;
  }

  // A disposed store is dead: a registry's idle sweep released it, or
  // dispose() was called. A writer still holding it is told so, and what
  // to do, rather than hearing the proxy underneath complain
  let disposed = false;
  function alive(what) {
    if (disposed) {
      throw new Error(`Cannot ${what} a disposed store. A registry with \`idle\` releases a store that has had no session for that long, ` +
        'and a server that writes to a store is not a session: resolve it again with stores.get(id) before each write instead of holding a reference');
    }
  }

  /**
   * Apply a change from the server itself, timestamped now. The authority:
   * a tombstone on its way is lifted, so what it writes at a deleted path
   * re-adds it, id or not.
   */
  function exportDocument() {
    const raw = LazyWatch.resolveIfProxy(state);
    const rows = [];
    for (const [key, entry] of clocks) {
      if (entry.deleted) {
        rows.push([key, { ts: entry.ts, deleted: true }]);
        continue;
      }
      const path = parsePathKey(key);
      const value = valueAt(raw, path);
      // A container written as a leaf (an empty object) keeps its fields as
      // rows of their own: the row is the container, not what fills it
      const leaf = Utils.isPlainObject(value) && !regs.matches(path) ? {} : structuredClone(value);
      rows.push([key, { value: leaf, ts: entry.ts }]);
    }
    return {
      format: 'lazy-storage/store',
      rows,
      replicas: Object.fromEntries([...replicas].map(([id, progress]) => [id, { ...progress }])),
      version,
      epoch,
      schema
    };
  }

  function patch(diff) {
    alive('patch');
    return apply({ replicaId: 'server', seq: ++serverSeq, ts: clock.now(), diff }, undefined, { authority: true });
  }

  /**
   * Publish a batch from state the server keeps elsewhere. lazy-watch emits
   * array changes as fragments ({ 2: 'c', $length: 3 }, a $splice) and
   * arrays travel as whole values, so every array the diff touches is
   * replaced with a copy of its current value read from `state` (the
   * LazyWatch the diff came from, or a plain object shaped like it), then
   * patched as the server's own change. The way to serve a LazyWatch that
   * other code already writes:
   *
   *   LazyWatch.on(live, diff => store.patchFrom(diff, live));
   */
  function patchFrom(diff, state) {
    return patch(expandRegisters(diff, regs, state));
  }

  self = {
    /** The live state; read freely, write through `patch` so clocks stay right */
    state,
    get version() { return version; },
    /** Identifies this life of the store's storage; changes when storage is wiped */
    epoch,
    get sessions() { return sessions.size; },
    /** Replica ids the store remembers progress for (pruned by compaction) */
    get replicas() { return [...replicas.keys()]; },
    /** A trusted op, gates skipped — judged by the merge like any replica's */
    apply: (op, session) => {
      alive('apply an op to');
      return apply(op, session);
    },
    patch,
    patchFrom,
    session: options => {
      alive('open a session on');
      return session(options);
    },
    /** True once dispose() ran (a registry's idle sweep does): the store takes no more writes or sessions */
    get disposed() { return disposed; },
    closeSessions,
    /** The state as JSON, encoded once per change: what a snapshot carries, inline or over HTTP */
    snapshotJSON: encodedState,
    /** Distinct users with a live session */
    presence,
    /** Every live session: `{ replicaId, user, data }`, `data` being what it shares */
    peers,
    snapshot: () => LazyWatch.snapshot(state),
    /** Subscribe to accepted changes (a LazyWatch listener on the state) */
    on: (listener, options) => LazyWatch.on(state, listener, options),
    /**
     * Watch what happens to the store, for logs, audits, and metrics:
     * 'op' ({ replicaId, seq, user, accepted, rejected, version }) for every
     * op merged, the server's own included; 'refused' ({ replicaId, seq,
     * user, code, message }) for every client op turned away; 'session'
     * ({ event: 'open' | 'close', user, replicaId, sessions }). Returns an
     * unsubscribe function
     */
    observe(event, fn) {
      if (!observers[event]) throw new TypeError(`Unknown store event "${event}"`);
      if (typeof fn !== 'function') throw new TypeError('observe needs a function');
      observers[event].add(fn);
      return () => observers[event].delete(fn);
    },
    /** A few numbers about the store's size and activity */
    stats() {
      let tombstones = 0;
      for (const entry of clocks.values()) if (entry.deleted) tombstones++;
      return { version, epoch, schema, sessions: sessions.size, replicas: replicas.size, rows: clocks.size, tombstones, log: log.length };
    },
    /** Forget what the retention window no longer needs; returns { tombstones, replicas } removed */
    compact,
    /**
     * The store as a storage document, JSON all through: every row with the
     * timestamp that won it (tombstones included), each replica's progress
     * and owner, and the version, epoch and schema. An adapter's
     * `replace(doc)` makes a store of it that refuses what this one would:
     * a stale write, a write under a tombstone, another user's replica.
     * For backups, moving a store between adapters or servers, and looking
     * at one
     */
    export: exportDocument,
    /**
     * Put `doc` (store.export(), or an adapter's load()) in this store's
     * storage in place of what it holds, while the server runs, and end
     * this store: its sessions are told `unavailable` and say hello again,
     * and the next load (a registry's get) serves the document, under a
     * new epoch. A document that is not one, or storage without
     * `replace`, is refused before anything ends. With a registry, use
     * stores.restore(id, doc), which also drops this instance
     */
    restore(doc) {
      alive('restore');
      if (typeof storage.replace !== 'function') throw new TypeError("This store's storage cannot take a document (it has no replace(doc))");
      assertDocument(doc);
      self.dispose();
      storage.replace(doc);
    },
    flush: () => storage.flush(),
    dispose() {
      if (disposed) return;
      disposed = true;
      try {
        storage.flush();
      } catch (err) {
        // The sessions below are still told
        onError(err);
      }
      // Storage that holds the store for this process (a SQLite lease) lets it go
      try {
        storage.close?.();
      } catch (err) {
        onError(err);
      }
      // Sessions still open are told the store went away but is not gone
      // for good: the client says hello again, and a registry loads the
      // store afresh, where an op sent to this one would be refused and lost
      for (const s of [...sessions]) {
        try {
          s.send({ t: 'closed', code: 'unavailable', message: 'The store was unloaded; say hello again' });
        } catch { /* the transport may already be gone */ }
        s.close();
        s.onEvict?.();
      }
      // Nobody is left to tell
      cancelFlush?.();
      cancelFlush = null;
      pending = new Map();
      LazyWatch.dispose(state);
    }
  };

  // A write on `state` that bypassed `patch` still drops the cached
  // encoding, on the batch's microtask
  LazyWatch.on(state, () => { stateJSON = null; });

  // Rows loaded from disk may hold deletions and replicas the window has
  // outlived; the first op after that runs compaction again on schedule
  compact();
  migrate();
  return self;

  /**
   * Run the migrations the stored rows have not been through, in order,
   * before anyone is served. Each one's diff is the server's own patch,
   * committed together with the new count, so a crash between two leaves
   * the store at the one before, never half through one. A migration that
   * throws stops the store from loading, the storage let go
   */
  function migrate() {
    for (let i = schema; i < migrations.length; i++) {
      try {
        const diff = migrations[i](LazyWatch.snapshot(state), { store: self });
        schema = i + 1;
        if (Utils.isPlainObject(diff) && Object.keys(diff).length) patch(diff);
        else commit({ upserts: [], deletes: [] });
      } catch (err) {
        schema = i;
        if (!disposed) self.dispose();
        throw new Error(`Migration ${i} failed: ${err?.message ?? err}`, { cause: err });
      }
    }
  }
}
