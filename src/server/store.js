// store.js - The authority: decides every change, holds the state, serves sessions
//
// A store owns one state tree (a plain object, doc.js), each record's
// clock, the last op seen from each replica (so a resent op is ignored),
// and a log of the last accepted changes for deltas. It is
// transport-agnostic: a session is created with a `send` function and fed
// parsed messages.
//
// An op is { replica, seq, ts, changes }. It passes these gates, in order,
// before the merge sees it: its shape (shape.js), the store's `rules` (a
// verdict per record on the record before and after the op), the size
// limit, the rate limit, and retention: an op stamped before the retention
// window is refused as 'expired'. Its timestamp is then clamped to the
// store's time (clock.js), and each change is decided on its own (doc.js).
// What landed is broadcast as a patch, to the sender too; what lost is
// listed in the ack with why.
//
// Retention is three rules with one window: the log keeps the last
// `deltaLog` changes, deletions inside records older than the window are
// forgotten, and so are replicas not heard from within it. That is safe
// because timestamps are clamped: an op the store applied is stamped no
// later than it arrived, so every op of a forgotten replica that may have
// been applied is older than the window and refused if resent, and a
// forgotten deletion can only have shadowed an op older than itself.
//
// Commits are grouped. What one turn of the event loop merged goes to
// storage as one commit at the end of the turn, and what the store would
// have sent meanwhile (acks, patches, answers, presence) is held back until
// that commit is done, then sent in the order it was made, so a client
// never hears of a change the storage does not have. flush(), creating a
// session, evictions, export() and dispose() commit what is pending first.
//
// A FOLLOWER (a relay, see follower.js) speaks for clients of its own on
// one session: its hello says `follower: true`, it is sent the records'
// clocks with its snapshots, and for each client the transport has vouched
// for it holds a GRANT (`session.grant(as, user, replica)`). An op, share
// or leave that carries `as` is judged as that client's, its ack and
// errors come back with the same `as`, and presence lists the client as a
// peer of its own.
import { LazyWatch } from 'lazy-watch';
import { createClock, isTimestamp, clamp } from '../core/clock.js';
import { PROTOCOL } from '../core/protocol.js';
import { createShape, assertChange, ModelError } from '../core/shape.js';
import { createDoc, docFromRecords, changeFor, diffOf } from '../core/doc.js';
import { createRules, RuleError } from '../core/rules.js';
import { memoryStorage, assertDocument } from './storage.js';
import { toJSON, presetJSON, encodedBytes, utf8Bytes, SNAPSHOT_THRESHOLD, TALLY, LAZY } from './wire.js';
import { randomId } from '../core/ids.js';

const { Utils } = LazyWatch;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** The most ops one hello carries; the client sends the rest once it is answered */
export const HELLO_OPS = 1000;

/** The most changes one commit groups: a turn that merges more is stored this many at a time */
const GROUP_MAX = 256;

/** A client op the store would not merge; `code` travels to the client */
export class RefusedError extends Error {
  constructor(code, message, extra) {
    super(message);
    this.name = 'RefusedError';
    this.code = code;
    Object.assign(this, extra);
  }
}

const defaultPresenceKey = user =>
  (user !== null && typeof user === 'object' && user.id != null ? String(user.id) : JSON.stringify(user));

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

/**
 * @param {Object} [options]
 * @param {Object} [options.initial] - the top level: collections as `{}`, singletons with their values (shape.js)
 * @param {string[]} [options.singletons]
 * @param {Array<string|string[]>} [options.registers]
 * @param {Object} [options.parents]
 * @param {Object} [options.rules] - per collection or singleton, `'*'` for the rest: true, false, or
 *   `(user, before, after, { c, id, state, replicaId, changes, store }) => boolean` (throw to say why)
 * @param {boolean|string[]} [options.readOnly=false] - refuse every client op (true), or every op that writes
 *   one of these collections or singletons
 * @param {(diff: Object, context: { user: any, replicaId: string, store: Object, changes: Object[] }) => boolean|void} [options.validate]
 *   a check of the whole op, for what spans records: its changes as one diff in the tree's shape. false,
 *   or a throw (its message the refusal's), refuses it with 'forbidden'; anything but true, false or nothing refuses it too (a diff to accept in its place is not taken)
 * @param {number} [options.retention=30 days]
 * @param {number} [options.compactEvery=1 hour]
 * @param {number} [options.deltaLog=1000]
 * @param {number} [options.maxChanges=10000] - changes in one op, each field a put writes counted
 * @param {{burst: number, perSecond: number}|false} [options.rateLimit]
 * @param {Object} [options.storage]
 * @param {boolean} [options.groupCommit=true]
 * @param {Function[]} [options.migrations] - `(state, { store }) => diff` each, run once, in order
 * @param {boolean|Object} [options.presence=false]
 * @param {(error: any) => void} [options.onError]
 * @param {() => number} [options.now]
 */
export function createStore({
  initial = {},
  singletons = [],
  registers = [],
  parents = {},
  rules = {},
  readOnly = false,
  validate,
  retention = 30 * DAY,
  compactEvery = HOUR,
  deltaLog = 1000,
  maxChanges = 10_000,
  rateLimit = { burst: 500, perSecond: 100 },
  storage = memoryStorage(),
  groupCommit = true,
  migrations = [],
  presence: presenceOption = false,
  onError = err => console.error('lazy-storage:', err),
  now
} = {}) {
  const shape = createShape({ initial, singletons, registers, parents });
  const pres = presenceOptions(presenceOption);
  if (rateLimit && !(rateLimit.burst > 0 && rateLimit.perSecond > 0)) throw new TypeError('rateLimit needs positive burst and perSecond');
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('validate must be a function');
  // readOnly as a list: those collections and singletons are the server's alone
  const locked = Array.isArray(readOnly) ? Object.fromEntries(readOnly.map(key => [key, false])) : {};
  for (const key of Object.keys(locked)) if (!shape.has(key)) throw new TypeError(`readOnly: "${key}" is not a collection or singleton`);
  const judgeRules = createRules(shape, { ...rules, ...locked }, readOnly === true);
  if (!Array.isArray(migrations) || migrations.some(m => typeof m !== 'function')) throw new TypeError('migrations must be an array of functions');
  const time = now ?? Date.now;
  const saved = storage.load();
  const doc = saved ? docFromRecords(shape, saved.records) : createDoc(shape);
  const replicas = new Map(Object.entries(saved?.replicas ?? {}).map(([id, r]) => [id, { ...r }]));
  let version = saved ? saved.version : 0;
  const epoch = saved?.epoch ?? randomId();
  let schema = saved ? (Number.isInteger(saved.schema) ? saved.schema : 0) : migrations.length;
  if (schema > migrations.length) {
    storage.close?.();
    const err = new Error(`The store's storage has run ${schema} migrations and this code knows ${migrations.length}: it was migrated by a newer version, and is not served in an older shape`);
    err.code = 'schema-ahead';
    throw err;
  }
  const log = (Array.isArray(saved?.log) ? saved.log : []).filter(e => e.v <= version).slice(-deltaLog);
  const clock = createClock({ now: time });
  for (const entry of log) clock.observe(entry.ts);
  const sessions = new Set();
  let madeSessions = 0;
  let lastCompaction = -Infinity;
  const buckets = new Map();
  const observers = { op: new Set(), refused: new Set(), session: new Set() };
  const listeners = new Set();
  const sent = {};
  let stateJSON = null;
  let stateBytes;
  let clocksJSON = null;
  let clocksBytes;
  let disposed = false;

  function tally(type, count, bytes) {
    const entry = sent[type] ??= { messages: 0, bytes: 0 };
    entry.messages += count;
    entry.bytes += bytes;
  }

  function encodedState() {
    if (stateJSON === null) {
      stateJSON = JSON.stringify(doc.state);
      stateBytes = undefined;
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

  function throttle(bucketKey) {
    if (!rateLimit) return 0;
    const wall = time();
    let bucket = buckets.get(bucketKey);
    if (!bucket) {
      if (buckets.size >= 10_000) for (const [id, b] of buckets) if (wall - b.at > MINUTE) buckets.delete(id);
      bucket = { tokens: rateLimit.burst, at: wall };
      buckets.set(bucketKey, bucket);
    }
    bucket.tokens = Math.min(rateLimit.burst, bucket.tokens + ((wall - bucket.at) / 1000) * rateLimit.perSecond);
    bucket.at = wall;
    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      return 0;
    }
    return Math.ceil(((1 - bucket.tokens) / rateLimit.perSecond) * 1000);
  }

  // --- Group commit ---------------------------------------------------------------------

  let changes = [];
  let effects = [];
  let draining = false;
  let cancelCommit = null;

  /** Run `fn` now, or once what is pending is stored */
  function later(fn) {
    if (changes.length || effects.length || draining) effects.push(fn);
    else fn();
  }

  function commit(change) {
    changes.push({ ...change, version, epoch, schema });
    if (groupCommit && changes.length < GROUP_MAX) return scheduleCommit();
    if (!commitNow()) throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
  }

  function scheduleCommit() {
    if (cancelCommit) return;
    if (typeof setImmediate === 'function') {
      const timer = setImmediate(commitNow);
      cancelCommit = () => clearImmediate(timer);
    } else {
      const timer = setTimeout(commitNow, 0);
      cancelCommit = () => clearTimeout(timer);
    }
  }

  /**
   * Store what is pending, then send what waited for it. A commit that
   * fails leaves memory ahead of disk: what it would have told is dropped,
   * and the store unloads itself; its sessions say hello again to a store
   * loaded afresh from what is stored. Returns false then
   */
  function commitNow() {
    cancelCommit?.();
    cancelCommit = null;
    if (changes.length) {
      const batch = changes;
      changes = [];
      try {
        if (typeof storage.commitMany === 'function') storage.commitMany(batch);
        else for (const change of batch) storage.commit(change);
      } catch (err) {
        effects = [];
        onError(err);
        self.dispose();
        return false;
      }
    }
    if (draining) return true;
    while (effects.length && !changes.length) {
      const run = effects;
      effects = [];
      draining = true;
      try {
        for (const fn of run) {
          try {
            fn();
          } catch (err) {
            onError(err);
          }
        }
      } finally {
        draining = false;
      }
    }
    if (changes.length) scheduleCommit();
    return true;
  }

  function broadcast(message) {
    let followers = 0;
    for (const s of sessions) if (s.follows) followers++;
    if (followers === 0) return;
    toJSON(message);
    tally(String(message.t), followers, followers * encodedBytes(message));
    const upTo = madeSessions;
    later(() => {
      let published = false;
      for (const s of sessions) {
        if (s.number > upTo) break;
        if (!s.follows) continue;
        if (!s.publish) s.transportSend(message);
        else if (!published) {
          s.publish(message);
          published = true;
        }
      }
    });
  }

  // --- Presence ---------------------------------------------------------------------------

  /** Every peer: listed sessions that said hello, and the clients followers speak for */
  function* listedPeers() {
    for (const s of sessions) {
      if (s.replicaId && s.listed) yield s;
      if (s.grants) for (const g of s.grants.values()) if (g.replicaId) yield g;
    }
  }

  function presence() {
    const seen = new Map();
    if (!pres) return [];
    for (const p of listedPeers()) {
      if (p.user === undefined) continue;
      const key = pres.key(p.user);
      if (!seen.has(key)) seen.set(key, pres.user(p.user));
    }
    return [...seen.values()];
  }

  function peerOf(p) {
    const peer = { replicaId: p.replicaId };
    if (p.user !== undefined) {
      peer.user = pres.user(p.user);
      peer.key = pres.key(p.user);
    }
    if (p.shared !== undefined) peer.data = p.shared;
    return peer;
  }

  function peers() {
    return pres ? [...listedPeers()].map(peerOf) : [];
  }

  let pendingPresence = new Map();
  let cancelFlush = null;
  let flushedAt = -Infinity;

  function noteChange(p, what) {
    if (!pres) return;
    let entry = pendingPresence.get(p);
    if (!entry) pendingPresence.set(p, (entry = { joined: false, left: false, shared: false }));
    if (what === 'leave') Object.assign(entry, { joined: false, shared: false, left: true });
    else entry[what === 'join' ? 'joined' : 'shared'] = true;
    if (cancelFlush) return;
    const wait = pres.every > 0 ? Math.max(0, flushedAt + pres.every - time()) : 0;
    let timer;
    if (wait > 0) {
      timer = setTimeout(flushPresence, wait);
      cancelFlush = () => clearTimeout(timer);
    } else if (typeof setImmediate === 'function') {
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
    const noted = pendingPresence;
    pendingPresence = new Map();
    const joined = [];
    const left = [];
    const shared = [];
    for (const [p, entry] of noted) {
      if (entry.left) left.push(p.replicaId);
      else if (entry.joined) joined.push(peerOf(p));
      else if (entry.shared) shared.push(p.shared === undefined ? { replicaId: p.replicaId } : { replicaId: p.replicaId, data: p.shared });
    }
    if (!joined.length && !left.length && !shared.length) return;
    const message = { t: 'presence' };
    if (left.length) message.left = left;
    if (joined.length) message.joined = joined;
    if (shared.length) message.shared = shared;
    toJSON(message);
    for (const s of sessions) if (s.presence) s.send(message);
  }

  /** Set what a peer shares; true when it changed */
  function setShared(p, data) {
    if (!pres) throw new RefusedError('forbidden', 'Presence is off on this store');
    let json;
    if (data !== null && data !== undefined) {
      json = JSON.stringify(data);
      if (json === undefined) throw new RefusedError('invalid', 'Shared data must be JSON');
      if (json.length > pres.maxShare) throw new RefusedError('too-large', `Shared data over ${pres.maxShare} bytes`);
      if (pres.validate) {
        let verdict;
        try {
          verdict = pres.validate(data, { user: p.user, replicaId: p.replicaId, store: self });
        } catch (err) {
          throw new RefusedError('forbidden', err?.message || 'Share refused');
        }
        if (verdict === false) throw new RefusedError('forbidden', 'Share refused');
        if (verdict !== true && verdict !== undefined) {
          data = verdict;
          json = JSON.stringify(data);
          if (json === undefined) throw new RefusedError('forbidden', 'Share refused');
        }
      }
    }
    if (json === p.sharedJson) return false;
    p.shared = json === undefined ? undefined : data;
    p.sharedJson = json;
    return true;
  }

  // --- Replicas ---------------------------------------------------------------------------

  const keyOf = pres?.key ?? defaultPresenceKey;
  let unkeyedReported = false;
  const ownerKey = user => {
    if (user === undefined) return undefined;
    // The default tells a user without an `id` apart by its whole value, and anything in it that changes between
    // sign-ins (a session token, an expiry) makes the same person another user: said once, to the app, which is the one to fix it
    if (keyOf === defaultPresenceKey && !unkeyedReported && Utils.isPlainObject(user) && user.id == null) {
      unkeyedReported = true;
      onError(new TypeError('A user without an "id" is told apart by its whole value: give the user authenticate returns a stable "id", or set presence.key. Anything in it that changes between sign-ins (a session token, an expiry) makes the same person another user, whose replicas are refused with "replica-taken"'));
    }
    return String(keyOf(user));
  };
  const bucketOf = (user, replicaId) => (user === undefined ? `r:${replicaId}` : `u:${ownerKey(user)}`);

  /** Whether `user` may speak for `replicaId`: null when it may, else the refusal */
  function claimReplica(speaker, user, replicaId) {
    if (replicaId === 'server') return { code: 'forbidden', message: 'The replica id "server" is reserved for the store' };
    if (speaker.speaksFor && speaker.speaksFor !== replicaId) {
      return { code: 'forbidden', message: `This session speaks for replica "${speaker.speaksFor}", not "${replicaId}"` };
    }
    const owner = replicas.get(replicaId)?.owner;
    if (owner !== undefined && owner !== ownerKey(user)) return { code: 'replica-taken', message: 'That replica id belongs to another user' };
    return null;
  }

  /** Record the user as the owner of a replica nobody owns yet */
  function takeReplica(user, replicaId) {
    const key = ownerKey(user);
    const known = replicas.get(replicaId);
    if (key === undefined || known?.owner !== undefined) return;
    const entry = { seq: known?.seq ?? 0, seen: known?.seen ?? time(), owner: key };
    replicas.set(replicaId, entry);
    commit({ upserts: [], deletes: [], replica: { id: replicaId, ...entry } });
  }

  // --- Deciding ops -----------------------------------------------------------------------

  /** The records' rows for a commit, from the changes that landed */
  function rowsOf(applied) {
    const touched = new Map();
    for (const change of applied) touched.set(shape.recordKey(change.c, change.id), [change.c, change.id]);
    const upserts = [];
    const deletes = [];
    for (const [c, id] of touched.values()) {
      const record = doc.get(c, id);
      if (record === undefined) deletes.push([c, id ?? null]);
      else upserts.push([c, id ?? null, record, doc.clock(c, id)]);
    }
    return { upserts, deletes };
  }

  function assertOp(op) {
    if (!Utils.isPlainObject(op)) throw new RefusedError('invalid', 'An op must be an object');
    if (typeof op.replica !== 'string' || !op.replica) throw new RefusedError('invalid', 'op.replica must be a string');
    if (!Number.isInteger(op.seq) || op.seq < 1) throw new RefusedError('invalid', 'op.seq must be a positive integer');
    if (!isTimestamp(op.ts)) throw new RefusedError('invalid', 'op.ts must be a whole number of milliseconds');
    if (!Array.isArray(op.changes)) throw new RefusedError('invalid', 'op.changes must be an array');
  }

  /** Changes that landed: into the log, to storage, to every session, to listeners */
  function landed(applied, ts, replicaId, replicaEntry, user, seq) {
    if (applied.length) {
      version++;
      stateJSON = null;
      clocksJSON = null;
      const entry = { v: version, ts, changes: applied };
      log.push(entry);
      if (log.length > deltaLog) log.splice(0, log.length - deltaLog);
      commit({ ...rowsOf(applied), replica: replicaEntry ? { id: replicaId, ...replicaEntry } : undefined, log: entry, logFloor: log[0]?.v ?? version });
      broadcast({ t: 'patch', changes: applied, v: version, ts });
      // Heard on the next microtask, as a batch is: a listener that writes (closes a session, patches) does so
      // after the write that set it off has finished, not in the middle of it
      if (listeners.size) {
        const meta = { origin: replicaId === 'server' ? 'server' : 'remote', replicaId, v: version, changes: applied };
        const diff = diffOf(shape, applied);
        queueMicrotask(() => {
          for (const fn of listeners) {
            try {
              fn(diff, meta);
            } catch (err) {
              onError(err);
            }
          }
        });
      }
    } else if (replicaEntry) {
      commit({ upserts: [], deletes: [], replica: { id: replicaId, ...replicaEntry } });
    }
    if (observers.op.size) { const v = version; later(() => notify('op', { replicaId, seq, user, changes: applied, version: v })); }
  }

  /**
   * How much an op writes: a change each, and each field of what a put
   * writes, since a put is merged into a record field by field. Counted only
   * as far as `cap`
   */
  function sizeOf(changes, cap) {
    let n = changes.length;
    const stack = [];
    for (const change of changes) {
      const value = Object.hasOwn(change, 'put') ? change.put : change.set;
      if (Utils.isPlainObject(value)) stack.push(value);
    }
    while (stack.length && n <= cap) {
      const value = stack.pop();
      for (const key in value) {
        n++;
        if (Utils.isPlainObject(value[key])) stack.push(value[key]);
      }
    }
    return n;
  }

  /**
   * Decide a replica's op. `speaker` is the session or grant it came on.
   * Returns { lost, duplicate }; throws a RefusedError for an op refused whole
   */
  function apply(op, speaker) {
    assertOp(op);
    if (speaker.writes === false) throw new RefusedError('forbidden', 'This session writes only as the clients vouched for on it');
    const claim = claimReplica(speaker, speaker.user, op.replica);
    if (claim) throw new RefusedError(claim.code, claim.message);
    speaker.speaksFor = op.replica;
    const known = replicas.get(op.replica);
    if (known && op.seq <= known.seq) return { lost: [], duplicate: true };
    const size = sizeOf(op.changes, maxChanges);
    if (size > maxChanges) throw new RefusedError('too-large', `An op of more than ${maxChanges} changes (each field a put writes counted)`);
    for (const change of op.changes) {
      try {
        assertChange(shape, change);
      } catch (err) {
        if (err instanceof ModelError) throw new RefusedError('invalid', err.message);
        throw err;
      }
    }
    const wall = time();
    if (Number.isFinite(retention) && op.ts < wall - retention) throw new RefusedError('expired', 'The op is older than the store keeps track of; resync and make it again');
    const ts = clamp(op.ts, wall);
    const applied = [];
    const lost = [];
    const validated = () => {
      if (!validate) return;
      let verdict;
      try {
        verdict = validate(diffOf(shape, op.changes), { user: speaker.user, replicaId: op.replica, store: self, changes: op.changes });
      } catch (err) {
        throw new RuleError(err?.message || 'Refused');
      }
      if (verdict === false) throw new RuleError('Refused');
      // A diff returned to accept in the op's place was taken before 1.0; letting the op through whole would quietly undo what it stripped
      if (verdict !== true && verdict !== undefined) throw new RuleError('validate returns true, false or nothing; a diff to accept in place of the op is no longer taken');
    };
    // Validated before anything is applied, after what the rules refuse outright (judge, rules.js)
    const applyAll = () => {
      validated();
      for (const change of op.changes) {
        const result = doc.apply(change, ts, 'judge');
        for (const a of result.applied) applied.push(a);
        for (const l of result.lost) lost.push(l);
      }
    };
    try {
      judgeRules(doc, op.changes, speaker.user, op.replica, { store: self }, applyAll);
    } catch (err) {
      if (err instanceof RuleError) throw new RefusedError('forbidden', err.message);
      throw err;
    }
    clock.observe(ts);
    const entry = { seq: op.seq, seen: wall, ...(known?.owner !== undefined ? { owner: known.owner } : {}) };
    if (known?.owner === undefined && ownerKey(speaker.user) !== undefined) entry.owner = ownerKey(speaker.user);
    replicas.set(op.replica, entry);
    landed(applied, ts, op.replica, entry, speaker.user, op.seq);
    maybeCompact();
    return { lost, duplicate: false };
  }

  /** The server's own write: a diff in the tree's shape, decided as the authority, now */
  function patch(diff) {
    alive('write to');
    if (!Utils.isPlainObject(diff)) throw new TypeError('store.patch takes a diff: { collection: { id: value } } or { singleton: value }');
    const list = [];
    for (const c of Object.keys(diff)) {
      if (!shape.has(c)) throw new TypeError(`"${c}" is not a collection or singleton of this store`);
      const value = diff[c];
      if (shape.isSingleton(c)) {
        if (value === null) throw new TypeError(`"${c}" is a singleton, and cannot be deleted`);
        list.push(changeFor({ c, path: [] }, value));
        continue;
      }
      if (!Utils.isPlainObject(value)) throw new TypeError(`"${c}" is a collection: patch its records by id`);
      for (const id of Object.keys(value)) list.push(changeFor({ c, id, path: [] }, value[id]));
    }
    return run(list);
  }

  function run(list) {
    for (const change of list) {
      try {
        assertChange(shape, change);
      } catch (err) {
        throw new TypeError(err.message);
      }
    }
    const ts = clock.stamp();
    const applied = [];
    for (const change of list) applied.push(...doc.apply(change, ts, 'authority').applied);
    landed(applied, ts, 'server', null, undefined, null);
    return applied;
  }

  function compact() {
    lastCompaction = time();
    if (!Number.isFinite(retention)) return { deletions: 0, replicas: 0 };
    const cutoff = lastCompaction - retention;
    const touched = doc.prune(cutoff);
    if (touched.length) clocksJSON = null;
    const forgotten = [];
    for (const [id, r] of replicas) {
      if (id !== 'server' && (r.seen ?? 0) < cutoff) {
        replicas.delete(id);
        forgotten.push(id);
      }
    }
    if (touched.length || forgotten.length) {
      const upserts = touched.map(([c, id]) => [c, id ?? null, doc.get(c, id), doc.clock(c, id)]);
      commit({ upserts, deletes: [], forgetReplicas: forgotten });
    }
    return { deletions: touched.length, replicas: forgotten.length };
  }

  function maybeCompact() {
    if (Number.isFinite(retention) && time() - lastCompaction >= compactEvery) compact();
  }

  // --- Answers ----------------------------------------------------------------------------

  /** Every record's clock as JSON, encoded once per change like the state (a snapshot carries both) */
  function encodedClocks() {
    if (clocksJSON === null) {
      clocksJSON = JSON.stringify(clocksOf());
      clocksBytes = undefined;
    }
    return clocksJSON;
  }

  function clocksOf() {
    const out = [];
    for (const [c, id, , clockJSON] of doc.records()) out.push([c, id ?? null, clockJSON]);
    return out;
  }

  /**
   * A snapshot whose JSON is assembled around the cached encodings of the
   * state and, for a session that asks (a client judging its pending edits,
   * a follower), the records' clocks: sending it never encodes either
   * again, and the object decodes them only when something reads it. A
   * client that can fetch is pointed at the HTTP route for a large one,
   * which carries the clocks too (snapshot.js)
   */
  function snapshotMessage(replicaId, session, lost = []) {
    const json = encodedState();
    const seq = replicas.get(replicaId)?.seq ?? 0;
    const message = { t: 'snapshot', seq, v: version, epoch, now: time(), shape: shape.hash };
    if (lost.length) message.lost = lost;
    const route = session.fetch ? session.httpSnapshot : null;
    if (route && json.length >= route.threshold) return { ...message, fetch: route.url };
    let decoded;
    Object.defineProperty(message, 'state', { enumerable: true, configurable: true, get: () => (decoded ??= JSON.parse(json)) });
    let clocksPart = '';
    let clocksSize = 0;
    if (session.clocks) {
      const cj = encodedClocks();
      let decodedClocks;
      Object.defineProperty(message, 'clocks', { enumerable: true, configurable: true, get: () => (decodedClocks ??= JSON.parse(cj)) });
      clocksPart = `,"clocks":${cj}`;
      clocksBytes ??= utf8Bytes(cj);
      clocksSize = 10 + clocksBytes;
    }
    Object.defineProperty(message, LAZY, { value: true });
    const head = '{"t":"snapshot","state":';
    const tail = `,"seq":${seq},"v":${version},"epoch":${JSON.stringify(epoch)},"now":${message.now},"shape":${JSON.stringify(shape.hash)}${lost.length ? `,"lost":${JSON.stringify(lost)}` : ''}}`;
    stateBytes ??= utf8Bytes(json);
    return presetJSON(message, head + json + clocksPart + tail, head.length + stateBytes + clocksSize + utf8Bytes(tail));
  }


  /** The log after version `since`, or null when it does not reach back that far */
  function deltaSince(since) {
    if (!Number.isInteger(since) || since < 0 || since > version) return null;
    if (since === version) return [];
    const start = log.findIndex(entry => entry.v > since);
    if (start === -1 || log[start].v !== since + 1) return null;
    return log.slice(start);
  }

  function catchUp(session, since, sinceEpoch, lost) {
    const ops = sinceEpoch === epoch ? deltaSince(since) : null;
    if (ops === null) return snapshotMessage(session.replicaId, session, lost);
    const message = { t: 'delta', ops, seq: replicas.get(session.replicaId)?.seq ?? 0, v: version, epoch, now: time(), shape: shape.hash };
    if (lost.length) message.lost = lost;
    return message;
  }

  function refusal(op, err, user, as) {
    const message = { t: 'error', seq: Utils.isPlainObject(op) ? op.seq : undefined, code: err.code ?? 'invalid', message: err.message };
    if (as !== undefined) {
      message.as = as;
      if (Utils.isPlainObject(op) && typeof op.replica === 'string') message.replica = op.replica;
    }
    if (err.code === 'rate-limited') message.retryAfter = err.retryAfter;
    if (!(err instanceof RefusedError)) onError(err);
    if (observers.refused.size) notify('refused', { replicaId: Utils.isPlainObject(op) ? op.replica : undefined, seq: message.seq, user, code: message.code, message: err.message });
    return message;
  }

  // --- Sessions ---------------------------------------------------------------------------

  /**
   * Attach a session. `send` receives message objects; feed the session
   * parsed client messages with `receive`, and `close` it when the
   * connection ends. `user` is whatever the transport authenticated.
   * `broadcast`, when given, fans a patch out to every session that offered
   * one at once; `onEvict` is called after `closeSessions` closed it;
   * `httpSnapshot: { url, threshold }` points a client that can fetch at
   * the HTTP route for a large snapshot. `follower: true` lets the session
   * be a follower's (a relay's link: its hub vouches); on any other a hello
   * that asks to be one is refused. `writes: false` has it write nothing as
   * itself, only as the clients vouched for on it
   */
  function session({ send: transportSend, user, onEvict, broadcast: publish, httpSnapshot, follower: mayFollow = false, writes = true } = {}) {
    alive('open a session on');
    if (typeof transportSend !== 'function') throw new TypeError('A session needs a send function');
    const deliver = message => {
      transportSend(message);
      tally(String(message.t), 1, encodedBytes(message));
    };
    const send = message => {
      if (!changes.length && !effects.length && !draining) return deliver(message);
      effects.push(() => { if (sessions.has(s)) deliver(message); });
    };
    const closeWith = message => {
      if (!commitNow()) return;
      s.close();
      later(() => deliver(message));
      later(() => s.onEvict?.());
    };
    const errorOf = (err, extra) => {
      const message = { t: 'error', code: err.code, message: err.message, ...extra };
      if (err.code === 'rate-limited') message.retryAfter = err.retryAfter;
      return message;
    };

    /** A live op, from the session itself or a client it speaks for */
    function liveOp(op, speaker, as) {
      const tag = as === undefined ? {} : { as };
      try {
        const replica = Utils.isPlainObject(op) ? op.replica : undefined;
        // A session waits for its next hello; a follower's client says none of its own, and waits out its time
        if (speaker.heldUntil !== null && speaker.kind === 'peer' && time() >= speaker.heldUntil) speaker.heldUntil = null;
        if (speaker.heldUntil !== null) {
          const wait = Math.max(1, speaker.heldUntil - time());
          throw new RefusedError('rate-limited', `Waiting for a hello after a refusal; try again in ${wait} ms`, { retryAfter: wait });
        }
        const wait = typeof replica === 'string' ? throttle(bucketOf(speaker.user, replica)) : 0;
        if (wait > 0) {
          speaker.heldUntil = time() + wait;
          throw new RefusedError('rate-limited', `Too many ops; try again in ${wait} ms`, { retryAfter: wait });
        }
        const { lost } = apply(op, speaker);
        // For a follower's client, which replica it was: the follower routes the ack by it
        const ack = { t: 'ack', ...tag, ...(as === undefined ? {} : { replica: op.replica }), seq: op.seq, v: version, now: time() };
        if (lost.length) ack.lost = lost;
        return send(ack);
      } catch (err) {
        if (disposed) return;
        return send(refusal(op, err, speaker.user, as));
      }
    }

    const s = {
      send,
      transportSend,
      user,
      onEvict,
      kind: 'client',
      publish: typeof publish === 'function' ? publish : null,
      httpSnapshot: typeof httpSnapshot?.url === 'string' ? { url: httpSnapshot.url, threshold: httpSnapshot.threshold ?? SNAPSHOT_THRESHOLD } : null,
      fetch: false,
      clocks: false,
      replicaId: null,
      speaksFor: null,
      heldUntil: null,
      shared: undefined,
      sharedJson: undefined,
      presence: true,
      writes,
      follows: true,
      listed: true,
      grants: null,
      held: null,
      receive(msg) {
        if (!sessions.has(s)) return;
        if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
        const as = msg.as;
        if (as !== undefined && msg.t !== 'hello') {
          const grant = s.grants?.get(as);
          // A client that left before it was let in has nothing to leave
          if (!grant && msg.t === 'leave') return;
          if (!grant) return send({ t: 'error', as, code: 'forbidden', message: 'No client is vouched for under that name' });
          if (msg.t === 'op') return liveOp(msg.op, grant, as);
          if (msg.t === 'share') {
            try {
              if (setShared(grant, msg.data) && grant.replicaId) noteChange(grant, 'share');
            } catch (err) {
              send(errorOf(err, { as }));
            }
            return;
          }
          if (msg.t === 'leave') return s.ungrant(as);
          return send({ t: 'error', as, message: `Unknown message type: ${msg.t}` });
        }
        switch (msg.t) {
          case 'hello': {
            if (msg.proto !== PROTOCOL) return closeWith({ t: 'closed', code: 'outdated', message: `This server speaks protocol ${PROTOCOL}; the client speaks ${msg.proto ?? 'an older one'}` });
            if (typeof msg.replica !== 'string' || !msg.replica) return send({ t: 'error', message: 'hello requires a replica' });
            // Unlisted and told every clock: a relay's link alone, whose clients are judged one by one
            if (msg.follower === true && !mayFollow && !s.grants) return send({ t: 'error', code: 'forbidden', message: "Only a relay's link opens a follower session" });
            const claim = claimReplica(s, user, msg.replica);
            if (claim?.code === 'replica-taken') return closeWith({ t: 'closed', code: claim.code, message: claim.message });
            if (claim) return send({ t: 'error', ...claim });
            const wait = throttle(bucketOf(user, msg.replica));
            if (wait > 0) return send({ t: 'error', code: 'rate-limited', message: `Too many hellos; try again in ${wait} ms`, retryAfter: wait });
            s.heldUntil = null;
            s.speaksFor = msg.replica;
            takeReplica(user, msg.replica);
            if (disposed) return;
            const joined = !s.replicaId;
            s.replicaId = msg.replica;
            if (joined && msg.follower === true) {
              // A follower: the clients it speaks for are peers, not it
              s.kind = 'follower';
              s.listed = false;
              s.grants ??= new Map();
            }
            s.clocks = msg.clocks === true;
            const hadPresence = s.presence;
            s.presence = msg.presence !== false;
            s.fetch = msg.fetch === true;
            let shared = false;
            if (msg.share !== undefined && s.listed) {
              try {
                shared = setShared(s, msg.share);
              } catch (err) {
                send(errorOf(err));
              }
            }
            const lost = [];
            const acked = [];
            for (const op of Array.isArray(msg.ops) ? msg.ops.slice(0, HELLO_OPS) : []) {
              try {
                const result = apply(op, s);
                acked.push(op.seq);
                if (result.lost.length) lost.push({ seq: op.seq, lost: result.lost });
              } catch (err) {
                if (disposed) return;
                send(refusal(op, err, user));
              }
            }
            // A follower reads nothing until a client of its own is let in: what its hello brought is acknowledged,
            // and the answer waits for the first grant (see grant), as does every broadcast
            if (s.grants && s.grants.size === 0) {
              s.follows = false;
              s.held = { since: msg.since, epoch: msg.epoch, lost };
              for (const seq of acked) send({ t: 'ack', seq, v: version, now: time() });
              return;
            }
            send(catchUp(s, msg.since, msg.epoch, lost));
            if (joined && pres) {
              if (s.presence) send({ t: 'presence', peers: peers() });
              if (s.listed) noteChange(s, 'join');
            } else {
              if (pres && s.presence && !hadPresence) send({ t: 'presence', peers: peers() });
              if (shared) noteChange(s, 'share');
            }
            return;
          }
          case 'share': {
            try {
              if (!s.listed) throw new RefusedError('forbidden', 'A follower shares as the clients it speaks for');
              const wait = s.replicaId ? throttle(bucketOf(user, s.replicaId)) : 0;
              if (wait > 0) throw new RefusedError('rate-limited', `Too many shares; try again in ${wait} ms`, { retryAfter: wait });
              if (setShared(s, msg.data) && s.replicaId) noteChange(s, 'share');
            } catch (err) {
              send(errorOf(err));
            }
            return;
          }
          case 'op':
            return liveOp(msg.op, s);
          case 'ping':
            return send({ t: 'pong' });
          default:
            return send({ t: 'error', message: `Unknown message type: ${msg.t}` });
        }
      },
      /**
       * A follower's client, vouched for by the transport: `as` its name on
       * this session, `user` who it is, `replica` the replica it speaks for.
       * Returns null, or the refusal ({ code, message }) when the replica is
       * another user's
       */
      grant(as, grantUser, replica) {
        // A follower's vouch may come before its hello (one sent on a session the server had just closed opens the next)
        if (!s.grants && mayFollow) s.grants = new Map();
        if (!s.grants) return { code: 'forbidden', message: 'Only a follower speaks for others' };
        if (typeof replica !== 'string' || !replica) return { code: 'invalid', message: 'A vouched client names its replica' };
        const previous = s.grants.get(as);
        const g = previous ?? { kind: 'peer', link: s, as, user: grantUser, replicaId: null, speaksFor: null, heldUntil: null, shared: undefined, sharedJson: undefined };
        const claim = claimReplica(g, grantUser, replica);
        if (claim) return claim;
        g.user = grantUser;
        g.speaksFor = replica;
        takeReplica(grantUser, replica);
        if (disposed) return { code: 'unavailable', message: 'The store was unloaded' };
        if (!previous) {
          s.grants.set(as, g);
          g.replicaId = replica;
          noteChange(g, 'join');
        }
        // The first client let in: the follower's held answer goes now, and it hears the store from here on
        if (s.held) {
          const { since, epoch, lost } = s.held;
          s.held = null;
          s.follows = true;
          send(catchUp(s, since, epoch, lost));
          if (pres && s.presence) send({ t: 'presence', peers: peers() });
        }
        return null;
      },
      /** Take back the follower's clients `filter(user)` picks, telling the follower why; returns how many */
      revoke(filter, code, message) {
        if (!s.grants) return 0;
        let count = 0;
        for (const g of [...s.grants.values()]) {
          if (!filter(g.user)) continue;
          s.ungrant(g.as);
          s.send({ t: 'revoked', as: g.as, code, message });
          count++;
        }
        return count;
      },
      /** The follower's client left */
      ungrant(as) {
        const g = s.grants?.get(as);
        if (!g) return;
        s.grants.delete(as);
        noteChange(g, 'leave');
      },
      close() {
        if (!sessions.delete(s)) return;
        if (s.replicaId && s.listed) noteChange(s, 'leave');
        if (s.grants) for (const g of s.grants.values()) noteChange(g, 'leave');
        if (observers.session.size) notify('session', { event: 'close', kind: s.kind, user, replicaId: s.replicaId, sessions: sessions.size });
      }
    };
    commitNow();
    s.number = ++madeSessions;
    sessions.add(s);
    if (observers.session.size) notify('session', { event: 'open', kind: s.kind, user, replicaId: null, sessions: sessions.size });
    return s;
  }

  /**
   * End the sessions (and the clients followers speak for) that
   * `predicate(session)` picks, with a `closed` message the client treats
   * as final. Returns how many it ended
   */
  function closeSessions(predicate, message = 'Your session was closed by the server') {
    commitNow();
    let closed = 0;
    for (const s of [...sessions]) {
      if (s.grants) {
        for (const g of [...s.grants.values()]) {
          if (!predicate(g)) continue;
          s.ungrant(g.as);
          s.send({ t: 'revoked', as: g.as, code: 'evicted', message });
          closed++;
        }
      }
      if (s.kind === 'follower' || !predicate(s)) continue;
      try {
        s.send({ t: 'closed', code: 'evicted', message });
      } catch { /* the transport may already be gone */ }
      s.close();
      s.onEvict?.();
      closed++;
    }
    return closed;
  }

  function alive(what) {
    if (disposed) {
      throw new Error(`Cannot ${what} a disposed store. A registry with \`idle\` releases a store that has had no session for that long: ` +
        'resolve it again with stores.get(id) before each write instead of holding a reference');
    }
  }

  function exportDocument() {
    const records = [];
    for (const [c, id, data, clockJSON] of doc.records()) records.push([c, id ?? null, structuredClone(data), clockJSON]);
    const reps = {};
    for (const [id, r] of replicas) reps[id] = { ...r };
    return { records, replicas: reps, version, epoch, schema, log: structuredClone(log) };
  }

  function migrate() {
    for (let i = schema; i < migrations.length; i++) {
      try {
        const diff = migrations[i](structuredClone(doc.state), { store: self });
        schema = i + 1;
        if (Utils.isPlainObject(diff) && Object.keys(diff).length) patch(diff);
        else commit({ upserts: [], deletes: [] });
        if (!commitNow()) throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
      } catch (err) {
        schema = i;
        if (!disposed) self.dispose();
        throw new Error(`Migration ${i} failed: ${err?.message ?? err}`, { cause: err });
      }
    }
  }

  const self = {
    /** The state: read it, never write it (store.patch writes) */
    get state() { return doc.state; },
    get shape() { return shape; },
    get version() { return version; },
    get epoch() { return epoch; },
    get sessions() { return sessions.size; },
    get disposed() { return disposed; },
    /** The current time on the store's clock */
    now: () => time(),
    session,
    patch,
    /** Delete a record, or a path in one, as the server */
    del(c, id, path = []) {
      alive('write to');
      return run([shape.isSingleton(c) ? { c, path: Array.isArray(id) ? id : path, del: true } : { c, id, path, del: true }]);
    },
    /** Decide a replica's op on trust, as if it came on a session of `user` */
    apply(op, { user } = {}) {
      alive('write to');
      return apply(op, { user, speaksFor: null, heldUntil: null });
    },
    closeSessions,
    snapshotJSON: () => {
      commitNow();
      return encodedState();
    },
    /** Every record's clock as JSON, as a snapshot carries them (stored first) */
    clocksJSON: () => {
      commitNow();
      return encodedClocks();
    },
    [TALLY]: tally,
    presence,
    peers,
    snapshot: () => structuredClone(doc.state),
    /** A record's clock: { live, gone }, timestamps by path key */
    clock: (c, id) => doc.clock(c, id),
    /**
     * Hear every batch that lands, before it is stored: `listener(diff, { origin, replicaId, v, changes })`,
     * the diff in the tree's shape (null for a deletion), the changes as they landed
     */
    on(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    /** 'op' (once merged, stored with the turn), 'refused', 'session' */
    observe(event, fn) {
      if (!observers[event]) throw new TypeError(`Unknown store event "${event}"`);
      if (typeof fn !== 'function') throw new TypeError('observe needs a function');
      observers[event].add(fn);
      return () => observers[event].delete(fn);
    },
    stats() {
      let records = 0;
      for (const c of shape.collections) records += Object.keys(doc.state[c]).length;
      const out = {};
      for (const [type, entry] of Object.entries(sent)) out[type] = { ...entry };
      return { version, epoch, schema, sessions: sessions.size, replicas: replicas.size, records, log: log.length, sent: out };
    },
    compact,
    export: () => {
      commitNow();
      return exportDocument();
    },
    restore(doc) {
      alive('restore');
      if (typeof storage.replace !== 'function') throw new TypeError("This store's storage cannot take a document (it has no replace(doc))");
      assertDocument(doc);
      self.dispose();
      storage.replace(doc);
    },
    flush() {
      if (!commitNow()) throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
      storage.flush?.();
    },
    dispose() {
      if (disposed) return;
      commitNow();
      if (disposed) return;
      disposed = true;
      try {
        storage.flush?.();
      } catch (err) {
        onError(err);
      }
      try {
        storage.close?.();
      } catch (err) {
        onError(err);
      }
      for (const s of [...sessions]) {
        try {
          s.send({ t: 'closed', code: 'unavailable', message: 'The store was unloaded; say hello again' });
        } catch { /* the transport may already be gone */ }
        s.close();
        s.onEvict?.();
      }
      cancelFlush?.();
      cancelFlush = null;
      pendingPresence = new Map();
    }
  };

  // A new store writes its seed records at once: a record is in storage or it is not there, so one deleted later stays deleted
  if (!saved) {
    const seeds = [];
    for (const c of shape.collections) for (const id of Object.keys(doc.state[c])) seeds.push([c, id, structuredClone(doc.state[c][id]), { live: {}, gone: {} }]);
    if (seeds.length) commit({ upserts: seeds, deletes: [] });
  }
  compact();
  migrate();
  if (!commitNow()) throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
  return self;
}
