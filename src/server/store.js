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
// - retention: an op older than `retention` is refused (code 'expired'),
//   because deletions older than that may have been compacted away and
//   the op could resurrect what they removed
// - write authorization: a leaf at or under a `readOnly` path refuses the
//   op, and `validate(diff, { user, replicaId, store })` may refuse it or
//   hand back a trimmed diff to accept instead (code 'forbidden')
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
//   { t: 'presence', users }                    distinct users with a live session
//   { t: 'closed', code, message }              this session is over
//       (code 'evicted'; hubs also send 'forbidden', 'unknown-store', 'invalid-store')
//   { t: 'error', seq?, code?, message, now?, ts? }  a refused op carries its
//       seq and a code: 'invalid' (breaks the model), 'clock-skew' (with
//       the server's `now` and the op's `ts`), 'expired', 'forbidden'
//   { t: 'pong' }
import { LazyWatch } from 'lazy-watch';
import { createClock, isTimestamp } from '../core/hlc.js';
import { registerSet, pathKey, parsePathKey, setAt, valueAt } from '../core/paths.js';
import { leaves, assertModel, rebuild } from '../core/model.js';
import { mergeOp, compactTombstones } from '../core/merge.js';
import { memoryStorage } from './storage.js';
import { toJSON } from './wire.js';
import { randomId } from '../core/ids.js';

const { Utils } = LazyWatch;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** A client op the store would not merge; `code` travels to the client */
export class RefusedError extends Error {
  constructor(code, message, extra) {
    super(message);
    this.name = 'RefusedError';
    this.code = code;
    Object.assign(this, extra);
  }
}

/**
 * Replica progress from a saved document: `{ replicas: { id: { seq, seen } } }`,
 * or the older `{ seqs: { id: seq } }` shape. A replica with no `seen` is
 * treated as seen now, so it gets a full retention window before pruning.
 */
function loadReplicas(saved, now) {
  const replicas = new Map();
  if (!saved) return replicas;
  if (saved.replicas) {
    for (const [id, r] of Object.entries(saved.replicas)) replicas.set(id, { seq: r.seq, seen: r.seen ?? now });
  } else {
    for (const [id, seq] of Object.entries(saved.seqs ?? {})) replicas.set(id, { seq, seen: now });
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
 * @param {Array<string|string[]>} [options.readOnly] - paths clients may
 *   not write, same syntax as registers; a client op touching a leaf at or
 *   under one is refused whole. The server's own `patch` is not bound
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
 *   live ops a replica may send: a token bucket holding `burst` tokens,
 *   refilled at `perSecond` (default 500 and 100). An op beyond it is
 *   refused with code 'rate-limited' and a `retryAfter` in ms; the client
 *   keeps the op and resends its outbox in a hello after that. Ops in a
 *   hello are not counted (they are bounded by the payload limit). `false`
 *   disables
 * @param {Object} [options.storage] - a storage adapter (default: memory)
 * @param {(user: any) => string} [options.presenceKey] - how presence
 *   dedupes users (default: by `id`)
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
  presenceKey = defaultPresenceKey,
  onError = err => console.error('lazy-storage:', err),
  now
} = {}) {
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('validate must be a function');
  if (rateLimit && !(rateLimit.burst > 0 && rateLimit.perSecond > 0)) throw new TypeError('rateLimit needs positive burst and perSecond');
  const time = now ?? Date.now;
  const regs = registerSet(registers);
  const locked = registerSet(readOnly);
  const saved = storage.load();
  const state = new LazyWatch(rebuild(initial, saved ? saved.rows.map(([key, row]) => [key, row.deleted ? null : row.value]) : []));
  const clocks = new Map(saved ? saved.rows.map(([key, row]) => [key, row.deleted ? { ts: row.ts, deleted: true } : { ts: row.ts }]) : []);
  const replicas = loadReplicas(saved, time());
  let version = saved ? saved.version : 0;
  // Versions count from 0 for the life of a store's storage. The epoch
  // tells one life from the next, so a client whose cache remembers a
  // version of storage that has since been wiped gets a snapshot, not a
  // delta computed against a different history
  const epoch = typeof saved?.epoch === 'string' && saved.epoch ? saved.epoch : randomId();
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
  let self;

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
    for (const s of sessions) s.send(message);
  }

  /** Every commit carries the version and epoch alongside its rows */
  function commit(change) {
    storage.commit({ ...change, version, epoch });
  }

  function presence() {
    const seen = new Map();
    for (const s of sessions) {
      if (s.user === undefined) continue;
      const key = presenceKey(s.user);
      if (!seen.has(key)) seen.set(key, s.user);
    }
    return [...seen.values()];
  }

  function broadcastPresence() {
    broadcast({ t: 'presence', users: presence() });
  }

  /**
   * The server's current values at rejected paths, as a diff the losing
   * client applies to fall back in line. A rejected path whose record no
   * longer exists is corrected at the shallowest missing ancestor with a
   * deletion, so the client drops the whole record rather than keeping an
   * empty shell.
   */
  function correction(paths) {
    const raw = LazyWatch.snapshot(state);
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

  /** True when some read-only pattern matches the path or one of its ancestors */
  function underReadOnly(path) {
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
    if (op.ts[0] < wall - retention) {
      throw new RefusedError('expired',
        `The op is ${Math.round((wall - op.ts[0]) / DAY)} days old, older than the store keeps history for`);
    }
    const lockedLeaf = entries.find(([path]) => underReadOnly(path));
    if (lockedLeaf) throw new RefusedError('forbidden', `"${lockedLeaf[0].join('/')}" is read-only`);
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
   * server's own ops are trusted.
   * @returns {{ duplicate: boolean, accepted: Object|null, rejected: string[][], correction: Object|null }}
   */
  function apply(op, session) {
    assertOp(op);
    maybeCompact();
    const last = replicas.get(op.replicaId)?.seq ?? 0;
    if (op.seq <= last) return { duplicate: true, accepted: null, rejected: [], correction: null };
    let diff = op.diff;
    let stripped = [];
    if (session) ({ diff, stripped } = admit(op, session));
    else assertModel(diff, regs);

    const seen = time();
    replicas.set(op.replicaId, { seq: op.seq, seen });
    clock.receive(op.ts);

    const { accepted, rejected, won, dropped } = mergeOp(clocks, op.ts, diff, regs);
    if (accepted) {
      LazyWatch.patch(state, accepted);
      version++;
      if (deltaLog > 0) {
        log.push({ v: version, diff: accepted });
        if (log.length > deltaLog) log.splice(0, log.length - deltaLog);
      }
    }
    commit({
      upserts: won.map(([path, value]) => [pathKey(path), value === null ? { ts: op.ts, deleted: true } : { value, ts: op.ts }]),
      deletes: dropped,
      replica: { id: op.replicaId, seq: op.seq, seen },
      // The log entry this op made, and the oldest version the store still
      // keeps, so an adapter persisting the log can prune to match
      log: accepted && deltaLog > 0 ? { v: version, diff: accepted } : undefined,
      logFloor: log.length ? log[0].v : version + 1
    });
    if (accepted) broadcast({ t: 'patch', diff: accepted, ts: op.ts, v: version });
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

  function snapshotMessage(replicaId) {
    return { t: 'snapshot', state: LazyWatch.snapshot(state), ts: clock.peek(), seq: replicas.get(replicaId)?.seq ?? 0, registers: registerPatterns, v: version, epoch };
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
  function catchUp(replicaId, since, sinceEpoch, refused, corrections) {
    const patches = refused || sinceEpoch !== epoch ? null : deltaSince(since);
    if (patches === null) return snapshotMessage(replicaId);
    return { t: 'delta', patches: [...patches, ...corrections], ts: clock.peek(), seq: replicas.get(replicaId)?.seq ?? 0, registers: registerPatterns, v: version, epoch };
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
   * Attach a session. `send` receives message objects; feed the session
   * parsed client messages with `receive`, and `close` it when the
   * connection ends. `user` is whatever the transport authenticated
   * (counted in presence when present, and handed to `validate`);
   * `onEvict` is called after `closeSessions` closed this session, so the
   * transport can drop the socket or the hub its entry.
   */
  function session({ send, user, onEvict } = {}) {
    if (typeof send !== 'function') throw new TypeError('A session needs a send function');
    const s = {
      send,
      user,
      onEvict,
      replicaId: null,
      receive(msg) {
        if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
        switch (msg.t) {
          case 'hello': {
            if (typeof msg.replicaId !== 'string' || !msg.replicaId) return send({ t: 'error', message: 'hello requires a replicaId' });
            s.replicaId = msg.replicaId;
            let refused = false;
            const lost = [];
            for (const op of Array.isArray(msg.ops) ? msg.ops : []) {
              try {
                lost.push(...apply(op, s).rejected);
              } catch (err) {
                refused = true;
                send(refusal(op, err, s));
              }
            }
            // One correction for every leaf the hello's ops lost, taken
            // after the last of them: a value read mid-hello could be
            // overtaken by a later op of the same hello, and the client
            // applies corrections last
            const corrections = lost.length ? [correction(lost)] : [];
            return send(catchUp(msg.replicaId, msg.since, msg.epoch, refused, corrections));
          }
          case 'op': {
            try {
              const replica = Utils.isPlainObject(msg.op) ? msg.op.replicaId : undefined;
              const wait = typeof replica === 'string' ? throttle(replica) : 0;
              if (wait > 0) {
                throw new RefusedError('rate-limited', `Too many ops; try again in ${wait} ms`, { retryAfter: wait });
              }
              const result = apply(msg.op, s);
              return send({ t: 'ack', seq: msg.op.seq, ts: clock.peek(), correction: result.correction });
            } catch (err) {
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
        if (user !== undefined) broadcastPresence();
        if (observers.session.size) notify('session', { event: 'close', user, replicaId: s.replicaId, sessions: sessions.size });
      }
    };
    sessions.add(s);
    // A user joining changes the list for everyone; an anonymous session
    // still needs to see who is here
    if (user !== undefined) broadcastPresence();
    else send({ t: 'presence', users: presence() });
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

  /** Apply a change from the server itself, timestamped now */
  function patch(diff) {
    return apply({ replicaId: 'server', seq: ++serverSeq, ts: clock.now(), diff });
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
    apply,
    patch,
    session,
    closeSessions,
    /** Distinct users with a live session */
    presence,
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
      return { version, epoch, sessions: sessions.size, replicas: replicas.size, rows: clocks.size, tombstones, log: log.length };
    },
    /** Forget what the retention window no longer needs; returns { tombstones, replicas } removed */
    compact,
    /** Forget tombstones older than a timestamp; returns how many */
    compactTombstones(olderThan) {
      const removed = compactTombstones(clocks, olderThan);
      if (removed.length) commit({ upserts: [], deletes: removed });
      return removed.length;
    },
    flush: () => storage.flush(),
    dispose() {
      storage.flush();
      for (const s of [...sessions]) s.close();
      LazyWatch.dispose(state);
    }
  };

  // Rows loaded from disk may hold deletions and replicas the window has
  // outlived; the first op after that runs compaction again on schedule
  compact();
  return self;
}
