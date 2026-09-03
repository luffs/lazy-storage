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
// Protocol (client -> server):
//   { t: 'hello', replicaId, ops: [op...] }   connect or reconnect: the
//       client's whole outbox; the server merges it and replies with a
//       snapshot that already contains the merged result
//   { t: 'op', op }                           one live batch
//   { t: 'ping' }
// where op = { replicaId, seq, ts, diff }.
//
// Server -> client:
//   { t: 'snapshot', state, ts, version, seq }  full state; `seq` is the
//       last op of this replica the server holds, so the client can drop
//       acknowledged outbox entries
//   { t: 'patch', diff, ts, version }           an accepted diff (from any
//       replica, the receiving one included)
//   { t: 'ack', seq, ts, correction }           `correction` is a diff with
//       the server's values at the leaves the op lost, or null
//   { t: 'error', seq?, message }
//   { t: 'pong' }
import { LazyWatch } from 'lazy-watch';
import { createClock, isTimestamp } from '../core/hlc.js';
import { registerSet, pathKey, parsePathKey, setAt, valueAt, deleteAt } from '../core/paths.js';
import { assertModel } from '../core/model.js';
import { mergeOp, compactTombstones } from '../core/merge.js';
import { memoryStorage } from './storage.js';

const { Utils } = LazyWatch;

/** `initial` with the persisted rows applied on top, shallow paths first */
function rebuild(initial, rows) {
  const state = structuredClone(initial);
  const ordered = rows.map(([key, row]) => [parsePathKey(key), row]).sort((a, b) => a[0].length - b[0].length);
  for (const [path, row] of ordered) {
    if (row.deleted) deleteAt(state, path);
    else setAt(state, path, structuredClone(row.value));
  }
  return state;
}

/**
 * @param {Object} [options]
 * @param {Object} [options.initial] - the skeleton: state when nothing is
 *   persisted, and the base persisted rows are applied onto
 * @param {Array<string|string[]>} [options.registers] - paths whose value
 *   is one unit (arrays live only here)
 * @param {Object} [options.storage] - a storage adapter (default: memory)
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createStore({ initial = {}, registers = [], storage = memoryStorage(), now } = {}) {
  const regs = registerSet(registers);
  const saved = storage.load();
  const state = new LazyWatch(rebuild(initial, saved ? saved.rows : []));
  const clocks = new Map(saved ? saved.rows.map(([key, row]) => [key, row.deleted ? { ts: row.ts, deleted: true } : { ts: row.ts }]) : []);
  const seqs = new Map(saved ? Object.entries(saved.seqs) : []);
  let version = saved ? saved.version : 0;
  const clock = createClock('server', now);
  const sessions = new Set();
  let serverSeq = seqs.get('server') ?? 0;

  function broadcast(message) {
    for (const s of sessions) s.send(message);
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

  /**
   * Merge one op. Idempotent per (replicaId, seq): a resent op is ignored.
   * @returns {{ duplicate: boolean, accepted: Object|null, rejected: string[][], correction: Object|null }}
   */
  function apply(op) {
    assertOp(op);
    assertModel(op.diff, regs);
    const last = seqs.get(op.replicaId) ?? 0;
    if (op.seq <= last) return { duplicate: true, accepted: null, rejected: [], correction: null };
    seqs.set(op.replicaId, op.seq);
    clock.receive(op.ts);

    const { accepted, rejected, won, dropped } = mergeOp(clocks, op.ts, op.diff, regs);
    if (accepted) {
      LazyWatch.patch(state, accepted);
      version++;
    }
    storage.commit({
      upserts: won.map(([path, value]) => [pathKey(path), value === null ? { ts: op.ts, deleted: true } : { value, ts: op.ts }]),
      deletes: dropped,
      replica: { id: op.replicaId, seq: op.seq },
      version
    });
    if (accepted) broadcast({ t: 'patch', diff: accepted, ts: op.ts, version });
    return { duplicate: false, accepted, rejected, correction: rejected.length ? correction(rejected) : null };
  }

  function snapshotMessage(replicaId) {
    return { t: 'snapshot', state: LazyWatch.snapshot(state), ts: clock.peek(), version, seq: seqs.get(replicaId) ?? 0 };
  }

  /**
   * Attach a session. `send` receives message objects; feed the session
   * parsed client messages with `receive`, and `close` it when the
   * connection ends.
   */
  function session({ send }) {
    const s = {
      send,
      replicaId: null,
      receive(msg) {
        if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
        switch (msg.t) {
          case 'hello': {
            if (typeof msg.replicaId !== 'string' || !msg.replicaId) return send({ t: 'error', message: 'hello requires a replicaId' });
            s.replicaId = msg.replicaId;
            for (const op of Array.isArray(msg.ops) ? msg.ops : []) {
              try {
                apply(op);
              } catch (err) {
                send({ t: 'error', seq: Utils.isPlainObject(op) ? op.seq : undefined, message: err.message });
              }
            }
            return send(snapshotMessage(msg.replicaId));
          }
          case 'op': {
            try {
              const result = apply(msg.op);
              return send({ t: 'ack', seq: msg.op.seq, ts: clock.peek(), correction: result.correction });
            } catch (err) {
              return send({ t: 'error', seq: Utils.isPlainObject(msg.op) ? msg.op.seq : undefined, message: err.message });
            }
          }
          case 'ping':
            return send({ t: 'pong' });
          default:
            return send({ t: 'error', message: `Unknown message type: ${msg.t}` });
        }
      },
      close() {
        sessions.delete(s);
      }
    };
    sessions.add(s);
    return s;
  }

  /** Apply a change from the server itself, timestamped now */
  function patch(diff) {
    return apply({ replicaId: 'server', seq: ++serverSeq, ts: clock.now(), diff });
  }

  return {
    /** The live state; read freely, write through `patch` so clocks stay right */
    state,
    get version() { return version; },
    get sessions() { return sessions.size; },
    apply,
    patch,
    session,
    snapshot: () => LazyWatch.snapshot(state),
    /** Subscribe to accepted changes (a LazyWatch listener on the state) */
    on: (listener, options) => LazyWatch.on(state, listener, options),
    /** Forget tombstones older than a timestamp; returns how many */
    compactTombstones(olderThan) {
      const removed = compactTombstones(clocks, olderThan);
      if (removed.length) storage.commit({ upserts: [], deletes: removed, version });
      return removed.length;
    },
    flush: () => storage.flush(),
    dispose() {
      storage.flush();
      for (const s of [...sessions]) s.close();
      LazyWatch.dispose(state);
    }
  };
}
