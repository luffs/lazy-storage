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
import { createSchemas, SchemaError } from '../core/schema.js';
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
 * @param {Object} [options.schemas] - per collection or singleton, what a record may hold (schema.js): a write that
 *   leaves a record otherwise is refused whole, a replica's op with 'invalid' and the server's own with a throw
 * @param {boolean|string[]} [options.readOnly=false] - refuse every client op (true), or every op that writes
 *   one of these collections or singletons
 * @param {(diff: Object, context: { user: any, replicaId: string, store: Object, changes: Object[] }) => boolean|void} [options.validate]
 *   a check of the whole op, for what spans records: its changes as one diff in the tree's shape. false,
 *   or a throw (its message the refusal's), refuses it with 'forbidden'; anything but true, false or nothing refuses it too (a diff to accept in its place is not taken)
 * @param {number} [options.retention=30 days]
 * @param {number} [options.compactEvery=1 hour]
 * @param {number} [options.deltaLog=1000]
 * @param {number} [options.maxChanges=10000] - changes in one op, each field a put writes counted
 * @param {number} [options.maxReplicas=1000] - replicas remembered per user (the replicas of sessions without
 *   a user share one allowance): past it, the user's replicas not seen for longest, and not live, are
 *   forgotten, as the retention window forgets them, so a client minting replica ids fills nothing
 * @param {{burst: number, perSecond: number}|false} [options.rateLimit]
 * @param {Object} [options.storage]
 * @param {boolean} [options.groupCommit=true]
 * @param {Function[]} [options.migrations] - `(state, { store }) => diff` each, run once, in order
 * @param {boolean|{ keep?: number }} [options.history=false] - keep every record as each write left it, with when
 *   and who, for `store.history`, `store.recordAt` and `store.revert`; `keep` (ms) forgets what is older, but for
 *   the last form of each record before it. The storage has to keep history (the memory and SQLite adapters do)
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
  schemas = {},
  readOnly = false,
  validate,
  retention = 30 * DAY,
  compactEvery = HOUR,
  deltaLog = 1000,
  maxChanges = 10_000,
  maxReplicas = 1000,
  rateLimit = { burst: 500, perSecond: 100 },
  storage = memoryStorage(),
  groupCommit = true,
  migrations = [],
  history: historyOption = false,
  presence: presenceOption = false,
  onError = err => console.error('lazy-storage:', err),
  now
} = {}) {
  const shape = createShape({ initial, singletons, registers, parents });
  const pres = presenceOptions(presenceOption);
  if (rateLimit && !(rateLimit.burst > 0 && rateLimit.perSecond > 0)) throw new TypeError('rateLimit needs positive burst and perSecond');
  if (!(maxReplicas >= 1)) throw new TypeError('maxReplicas must be a positive number (Infinity for no limit)');
  if (validate !== undefined && typeof validate !== 'function') throw new TypeError('validate must be a function');
  // readOnly as a list: those collections and singletons are the server's alone
  const locked = Array.isArray(readOnly) ? Object.fromEntries(readOnly.map(key => [key, false])) : {};
  for (const key of Object.keys(locked)) if (!shape.has(key)) throw new TypeError(`readOnly: "${key}" is not a collection or singleton`);
  const checkRecord = createSchemas(shape, schemas);
  // What the store starts from is held to the same schemas as what is written to it
  for (const c of [...shape.collections, ...shape.singletons]) {
    if (!checkRecord.has(c)) continue;
    const broken = shape.isSingleton(c)
      ? checkRecord.check(c, undefined, shape.initial[c])
      : Object.keys(shape.initial[c]).map(id => checkRecord.check(c, id, shape.initial[c][id])).find(Boolean);
    if (broken) throw new TypeError(`initial does not fit schemas: ${broken.message}`);
  }
  const judgeRules = createRules(shape, { ...rules, ...locked }, readOnly === true, checkRecord);
  const judgeSchemas = createRules(shape, {}, false, checkRecord);
  if (!Array.isArray(migrations) || migrations.some(m => typeof m !== 'function')) throw new TypeError('migrations must be an array of functions');
  // History (see landed, and the history section below): null when off, else how long it is kept
  if (historyOption !== false && historyOption !== true && !Utils.isPlainObject(historyOption)) throw new TypeError('history must be true, false, or { keep }');
  const keeps = historyOption === false ? null : { keep: historyOption === true ? Infinity : historyOption.keep ?? Infinity };
  if (keeps && !(keeps.keep > 0)) throw new TypeError('history.keep must be a positive number of milliseconds');
  if (keeps && ['history', 'historyOfVersion', 'historyCount', 'pruneHistory'].some(method => typeof storage[method] !== 'function')) {
    throw new TypeError("This store's storage keeps no history (it has no history(query), historyOfVersion(v), historyCount() and pruneHistory(before))");
  }
  const time = now ?? Date.now;
  const saved = storage.load();
  const doc = saved ? docFromRecords(shape, saved.records) : createDoc(shape);
  const replicas = new Map(Object.entries(saved?.replicas ?? {}).map(([id, r]) => [id, { ...r }]));
  // The replicas each user owns (null: those of sessions without a user), to keep them within maxReplicas
  const replicasOf = new Map();
  const ownerSet = owner => {
    const key = owner ?? null;
    if (!replicasOf.has(key)) replicasOf.set(key, new Set());
    return replicasOf.get(key);
  };
  for (const [id, r] of replicas) if (id !== 'server') ownerSet(r.owner).add(id);
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
  // The clock starts after every stamp the store holds: its records' as well as its log's, since a short
  // log may end with a replica's op stamped before the server's last writes (an edit made offline)
  const clock = createClock({ now: time });
  for (const entry of log) clock.observe(entry.ts);
  clock.observe(doc.latest());
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

  /**
   * Take a token from a bucket: 0 when there was one, else how long (ms)
   * until the bucket holds `want` of them (never more than it can hold)
   */
  function throttle(bucketKey, want = 1) {
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
    return Math.ceil(((Math.max(1, Math.min(want, rateLimit.burst)) - bucket.tokens) / rateLimit.perSecond) * 1000);
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

  /** The replicas some session speaks for now: never forgotten for the limit */
  function liveReplicas() {
    const live = new Set();
    for (const s of sessions) {
      if (s.replicaId) live.add(s.replicaId);
      if (s.grants) for (const g of s.grants.values()) if (g.replicaId) live.add(g.replicaId);
    }
    return live;
  }

  /**
   * A replica's entry set, and the owner's allowance kept: past maxReplicas,
   * the owner's replicas not seen for longest go, as the retention window
   * takes them, but for those a session speaks for now
   */
  function rememberReplica(id, entry) {
    const before = replicas.get(id);
    if (before && (before.owner ?? null) !== (entry.owner ?? null)) ownerSet(before.owner).delete(id);
    replicas.set(id, entry);
    const mine = ownerSet(entry.owner);
    mine.add(id);
    if (mine.size <= maxReplicas) return;
    const live = liveReplicas();
    const stale = [...mine].filter(other => other !== id && !live.has(other)).sort((a, b) => (replicas.get(a).seen ?? 0) - (replicas.get(b).seen ?? 0));
    const forgotten = stale.slice(0, mine.size - maxReplicas);
    if (!forgotten.length) return;
    for (const other of forgotten) {
      replicas.delete(other);
      mine.delete(other);
    }
    commit({ upserts: [], deletes: [], forgetReplicas: forgotten });
  }

  /** Record the user as the owner of a replica nobody owns yet */
  function takeReplica(user, replicaId) {
    const key = ownerKey(user);
    const known = replicas.get(replicaId);
    if (key === undefined || known?.owner !== undefined) return;
    const entry = { seq: known?.seq ?? 0, seen: known?.seen ?? time(), owner: key };
    rememberReplica(replicaId, entry);
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

  /**
   * A history row per record the changes wrote: the record as they left it
   * (null: deleted) and the changes to it, both as JSON, encoded now. A
   * commit is stored at the end of the turn, and by then a later write of
   * the same turn may have changed the record again: its row is its own
   */
  function historyOf(applied, { upserts, deletes }, ts, replicaId, user) {
    const changesOf = new Map();
    for (const change of applied) {
      const key = shape.recordKey(change.c, change.id);
      if (!changesOf.has(key)) changesOf.set(key, []);
      changesOf.get(key).push(change);
    }
    const by = replicaId === 'server' ? null : ownerKey(user) ?? null;
    const row = (c, id, data) => ({ c, id: id ?? null, v: version, ts, user: by, replica: replicaId, data, changes: JSON.stringify(changesOf.get(shape.recordKey(c, id ?? undefined)) ?? []) });
    return [...upserts.map(([c, id, record]) => row(c, id, JSON.stringify(record))), ...deletes.map(([c, id]) => row(c, id, null))];
  }

  function assertOp(op) {
    if (!Utils.isPlainObject(op)) throw new RefusedError('invalid', 'An op must be an object');
    if (typeof op.replica !== 'string' || !op.replica) throw new RefusedError('invalid', 'op.replica must be a string');
    if (!Number.isInteger(op.seq) || op.seq < 1) throw new RefusedError('invalid', 'op.seq must be a positive integer');
    if (!isTimestamp(op.ts)) throw new RefusedError('invalid', 'op.ts must be a whole number of milliseconds');
    if (!Array.isArray(op.changes)) throw new RefusedError('invalid', 'op.changes must be an array');
  }

  /** Changes that landed: into the log, to storage, to every session, to listeners */
  function landed(applied, ts, replicaId, replicaEntry, user, seq, lost = []) {
    if (applied.length) {
      version++;
      stateJSON = null;
      clocksJSON = null;
      const entry = { v: version, ts, changes: applied };
      log.push(entry);
      if (log.length > deltaLog) log.splice(0, log.length - deltaLog);
      const rows = rowsOf(applied);
      commit({
        ...rows,
        replica: replicaEntry ? { id: replicaId, ...replicaEntry } : undefined,
        log: entry,
        logFloor: log[0]?.v ?? version,
        ...(keeps ? { history: historyOf(applied, rows, ts, replicaId, user) } : {})
      });
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
    // Told of the op once it is stored, what landed and what lost (an op that lost whole included): what to count or forward
    if (observers.op.size) { const v = version; later(() => notify('op', { replicaId, seq, user, changes: applied, lost, version: v })); }
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
    const ts = clamp(op.ts, clock.time);
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
      if (err instanceof SchemaError) throw new RefusedError('invalid', err.message);
      throw err;
    }
    clock.observe(ts);
    const entry = { seq: op.seq, seen: wall, ...(known?.owner !== undefined ? { owner: known.owner } : {}) };
    if (known?.owner === undefined && ownerKey(speaker.user) !== undefined) entry.owner = ownerKey(speaker.user);
    rememberReplica(op.replica, entry);
    landed(applied, ts, op.replica, entry, speaker.user, op.seq, lost);
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
    const applyAll = () => { for (const change of list) applied.push(...doc.apply(change, ts, 'authority').applied); };
    // The server's writes are held to the schemas too, by the same judge as a replica's op but with no rules:
    // applied as a trial, each record they wrote checked once, and put back whole when one does not fit
    try {
      judgeSchemas(doc, list, null, 'server', { store: self }, applyAll);
    } catch (err) {
      if (err instanceof SchemaError) throw new TypeError(err.message);
      throw err;
    }
    landed(applied, ts, 'server', null, undefined, null);
    // As a replica's op does: a store only the server writes would otherwise forget nothing until it was loaded again
    maybeCompact();
    return applied;
  }

  function compact() {
    lastCompaction = time();
    if (keeps && Number.isFinite(keeps.keep)) {
      // What is pending is stored first: a row it holds may be the one a record's history is pruned back to
      if (commitNow()) storage.pruneHistory(lastCompaction - keeps.keep);
    }
    if (!Number.isFinite(retention)) return { deletions: 0, replicas: 0 };
    const cutoff = lastCompaction - retention;
    const touched = doc.prune(cutoff);
    if (touched.length) clocksJSON = null;
    const forgotten = [];
    for (const [id, r] of replicas) {
      if (id !== 'server' && (r.seen ?? 0) < cutoff) {
        replicas.delete(id);
        ownerSet(r.owner).delete(id);
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
    if ((Number.isFinite(retention) || (keeps && Number.isFinite(keeps.keep))) && time() - lastCompaction >= compactEvery) compact();
  }

  // --- History ----------------------------------------------------------------------------
  //
  // With `history` on, every write leaves a row per record it wrote: the
  // record as the write left it, the changes to it, when (the store's
  // clock and version) and who (the replica, and its user's key). The
  // storage keeps the rows; the store asks it for a record's, newest
  // first. A store that already holds records when history is first
  // turned on writes a row for each as it stands (`baseline`), so a record
  // nobody has written since still has a form to be asked for; before that
  // row nothing is known.

  /** `when` as the storage is asked: { v } a version, { ts } or a number or a Date a time on the store's clock; nothing: now */
  function upTo(when) {
    if (when === undefined) return {};
    const ts = Utils.isPlainObject(when) ? when.ts : when;
    if (Utils.isPlainObject(when) && Number.isSafeInteger(when.v)) return { upToV: when.v };
    if ((typeof ts === 'number' || ts instanceof Date) && Number.isFinite(Number(ts))) return { upToTs: Number(ts) };
    throw new TypeError('When is a time (a number of milliseconds, a Date, { ts }) or a version ({ v })');
  }

  function historyRows(c, id, query) {
    if (!keeps) throw new TypeError('History is off on this store: make it with history: true');
    if (!shape.has(c)) throw new TypeError(`"${c}" is not a collection or singleton of this store`);
    if (shape.isCollection(c) && (typeof id !== 'string' || !id)) throw new TypeError(`A record of "${c}" is named by its id`);
    commitNow();
    return storage.history({ c, id: shape.isSingleton(c) ? null : id, ...query });
  }

  const entryOf = row => ({
    v: row.v,
    ts: row.ts,
    replicaId: row.replica,
    ...(row.user === null || row.user === undefined ? {} : { user: row.user }),
    changes: JSON.parse(row.changes),
    record: row.data === null ? undefined : JSON.parse(row.data)
  });

  /** Whether a change is at, above or beneath a path in its record */
  const touches = (change, path) => {
    const at = change.path ?? [];
    for (let i = 0; i < Math.min(at.length, path.length); i++) if (at[i] !== path[i]) return false;
    return true;
  };

  /**
   * The records a record's deletion took with it, as changes that make
   * them again: its children, theirs, and so on, each as it was just before
   * (they are not asked to go back in time, only to come back). A deletion
   * and what it cascaded to are one write, so they are the deletions of
   * that version whose record named, as its parent, one being made again:
   * another record deleted by the same write, but no child of it, stays
   * deleted. One made again since is left as it is
   */
  function takenWith(c, id) {
    const [last] = storage.history({ c, id, limit: 1 });
    if (!last || last.data !== null || !shape.hasParents) return [];
    const back = new Set([shape.recordKey(c, id)]);
    const gone = [];
    for (const row of storage.historyOfVersion(last.v)) {
      if (row.data !== null || row.id === null || !shape.parentsOf(row.c).length || doc.get(row.c, row.id) !== undefined) continue;
      const [before] = storage.history({ c: row.c, id: row.id, upToV: last.v - 1, limit: 1 });
      if (before && before.data !== null) gone.push({ c: row.c, id: row.id, was: JSON.parse(before.data) });
    }
    const changes = [];
    // A child's child comes back once the child is among those coming back: round by round until none is added
    for (let added = true; added;) {
      added = false;
      for (const record of gone) {
        const key = shape.recordKey(record.c, record.id);
        if (back.has(key) || !Utils.isPlainObject(record.was)) continue;
        const parent = shape.parentsOf(record.c).some(({ field, parent: pc }) => typeof record.was[field] === 'string' && back.has(shape.recordKey(pc, record.was[field])));
        if (!parent) continue;
        back.add(key);
        changes.push(changeFor({ c: record.c, id: record.id, path: [] }, record.was));
        added = true;
      }
    }
    return changes;
  }

  /**
   * What takes a record from `current` to `target`, as a diff a put merges
   * in: a field `target` lacks is null (deleted), an object both have is
   * gone into, anything else is written whole (a register and a list
   * always are)
   */
  function diffTo(c, id, current, target, path = []) {
    const diff = {};
    for (const key of Object.keys(current)) if (!Object.hasOwn(target, key)) diff[key] = null;
    for (const [key, value] of Object.entries(target)) {
      const here = [...path, key];
      const both = Utils.isPlainObject(value) && Utils.isPlainObject(current[key]) && !shape.isRegister(c, id, here);
      if (both) {
        const inner = diffTo(c, id, current[key], value, here);
        if (Object.keys(inner).length) diff[key] = inner;
      } else if (JSON.stringify(current[key]) !== JSON.stringify(value)) {
        diff[key] = value;
      }
    }
    return diff;
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
   * at its state-only document when it asked for no clocks (snapshot.js)
   */
  function snapshotMessage(replicaId, session, lost = []) {
    const json = encodedState();
    const seq = replicas.get(replicaId)?.seq ?? 0;
    const message = { t: 'snapshot', seq, v: version, epoch, now: clock.time, shape: shape.hash };
    if (lost.length) message.lost = lost;
    const route = session.fetch ? session.httpSnapshot : null;
    if (route && json.length >= route.threshold) return { ...message, fetch: session.clocks ? route.url : `${route.url}${route.url.includes('?') ? '&' : '?'}clocks=0` };
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
    const message = { t: 'delta', ops, seq: replicas.get(session.replicaId)?.seq ?? 0, v: version, epoch, now: clock.time, shape: shape.hash };
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
        const ack = { t: 'ack', ...tag, ...(as === undefined ? {} : { replica: op.replica }), seq: op.seq, v: version, now: clock.time };
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
            const ops = Array.isArray(msg.ops) ? msg.ops.slice(0, HELLO_OPS) : [];
            const bucket = bucketOf(user, msg.replica);
            // Its token pays for its first op (see below). Refused, it is asked back once the bucket holds enough for the ops it carries
            const wait = throttle(bucket, Math.max(1, ops.length));
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
            // Each op draws on the bucket as a live one does, or a hello would be a thousand ops for one token: the
            // hello's own token pays for the first, so n ops cost n tokens however they come, and a bucket of one
            // still lands one op a hello. One the store has merged already (sent live, its ack lost with the socket)
            // costs nothing again. At the first op beyond the bucket the rest wait, in order: the client says hello
            // again after `retryAfter`, long enough for what is left (as much of it as the bucket holds)
            let paid = true;
            for (let i = 0; i < ops.length; i++) {
              const op = ops[i];
              const merged = Utils.isPlainObject(op) && op.replica === msg.replica && op.seq <= (replicas.get(msg.replica)?.seq ?? 0);
              let wait = 0;
              if (!merged && paid) paid = false;
              else if (!merged) wait = throttle(bucket, ops.length - i);
              if (wait > 0) {
                send(refusal(op, new RefusedError('rate-limited', `Too many ops; try again in ${wait} ms`, { retryAfter: wait }), user));
                break;
              }
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
              for (const seq of acked) send({ t: 'ack', seq, v: version, now: clock.time });
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
    now: () => clock.time,
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
    /**
     * A record's history, newest first: `[{ v, ts, replicaId, user?, changes, record }]`, a write each, `record`
     * as that write left it (undefined: deleted) and `changes` what it did to it. `limit` (default 100) entries,
     * older than version `before` when given, and with `path` only the writes at, above or beneath that path in
     * the record. A singleton takes the options in the id's place
     */
    history(c, id, options) {
      if (Utils.isPlainObject(id) && options === undefined) [id, options] = [undefined, id];
      const { limit = 100, before, path } = options ?? {};
      if (!(Number.isInteger(limit) && limit >= 1)) throw new TypeError('history takes a limit of one or more');
      if (before !== undefined && !Number.isSafeInteger(before)) throw new TypeError('before is a version: a whole number');
      if (path !== undefined && !(Array.isArray(path) && path.every(seg => typeof seg === 'string'))) throw new TypeError('A path is an array of keys');
      const out = [];
      let cursor = before;
      // With a path, read on until enough of the writes that touch it are found
      for (;;) {
        const rows = historyRows(c, id, { before: cursor, limit: path ? 200 : limit });
        for (const row of rows) {
          const entry = entryOf(row);
          if (!path || entry.changes.some(change => touches(change, path)) || entry.changes.length === 0) out.push(entry);
          if (out.length === limit) return out;
        }
        if (!path || rows.length < 200) return out;
        cursor = rows[rows.length - 1].v;
      }
    },
    /**
     * A record as it was: at a time on the store's clock (a number of ms, a Date, `{ ts }`) or at a version
     * (`{ v }`). undefined when it was not there then, or when that is before the store kept history
     */
    recordAt(c, id, when) {
      if (shape.isSingleton(c) && when === undefined) [id, when] = [undefined, id];
      const [row] = historyRows(c, id, { ...upTo(when), limit: 1 });
      return row && row.data !== null ? JSON.parse(row.data) : undefined;
    },
    /**
     * Put a record back as it was then (see recordAt), as a write of the server's: the fields it had are
     * written, the ones it has since gained deleted, and a record that was not there is deleted. Throws when
     * nothing is known of the record then, and what a patch throws (a form its schema no longer takes).
     * Returns the changes that landed
     */
    revert(c, id, when, { children = true } = {}) {
      alive('write to');
      if (shape.isSingleton(c) && when === undefined) [id, when] = [undefined, id];
      const [row] = historyRows(c, id, { ...upTo(when), limit: 1 });
      if (!row) throw new Error(`Nothing is known of ${shape.full(c, id).join('/')} then: it is before the store kept its history`);
      const current = doc.get(c, id);
      if (row.data === null) return current === undefined ? [] : run([{ c, id, del: true }]);
      const target = JSON.parse(row.data);
      // A record made again brings back the records its deletion took with it, in the same write
      if (current === undefined) return run([changeFor({ c, id, path: [] }, target), ...(children ? takenWith(c, id) : [])]);
      const whole = !Utils.isPlainObject(target) || !Utils.isPlainObject(current) || shape.isRegister(c, id);
      const value = whole ? target : diffTo(c, id, current, target);
      if (!whole && Object.keys(value).length === 0) return [];
      return run([changeFor(shape.isSingleton(c) ? { c, path: [] } : { c, id, path: [] }, value)]);
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
  // History turned on over records already there: each gets a row as it stands, the first thing known of it
  if (keeps && commitNow() && storage.historyCount() === 0) {
    const baseline = [];
    const ts = clock.time;
    for (const [c, id, data] of doc.records()) {
      baseline.push({ c, id: id ?? null, v: version, ts, user: null, replica: 'server', data: JSON.stringify(data), changes: '[]' });
    }
    if (baseline.length) commit({ upserts: [], deletes: [], history: baseline });
  }
  migrate();
  if (!commitNow()) throw new RefusedError('unavailable', 'The store could not save the change and was unloaded');
  return self;
}
