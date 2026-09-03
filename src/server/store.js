// store.js - The authority: merges ops, holds the state, serves sessions
//
// A store owns one state tree (a LazyWatch instance), the per-path clocks
// the merge decides with, and the last sequence number seen from each
// replica (so a resent op is ignored). It is transport-agnostic: a
// session is created with a `send` function and fed parsed messages.
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
import { registerSet, pathKey, setAt, valueAt } from '../core/paths.js';
import { assertModel } from '../core/model.js';
import { mergeOp, compactTombstones } from '../core/merge.js';
import { memoryStorage } from './storage.js';

const { Utils } = LazyWatch;

/**
 * @param {Object} [options]
 * @param {Object} [options.initial] - state when nothing is persisted
 * @param {Array<string|string[]>} [options.registers] - paths whose value
 *   is one unit (arrays live only here)
 * @param {Object} [options.storage] - a storage adapter (default: memory)
 * @param {() => number} [options.now] - wall clock (injectable for tests)
 */
export function createStore({ initial = {}, registers = [], storage = memoryStorage(), now } = {}) {
  const regs = registerSet(registers);
  const saved = storage.load();
  const state = new LazyWatch(saved ? saved.state : structuredClone(initial));
  const clocks = new Map(saved ? saved.clocks : []);
  const seqs = new Map(saved ? Object.entries(saved.seqs) : []);
  let version = saved ? saved.version : 0;
  const clock = createClock('server', now);
  const sessions = new Set();
  let serverSeq = seqs.get('server') ?? 0;

  function persist() {
    storage.save({
      state: LazyWatch.snapshot(state),
      clocks: [...clocks],
      seqs: Object.fromEntries(seqs),
      version
    });
  }

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
      const path = JSON.parse(key);
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

    const { accepted, rejected } = mergeOp(clocks, op.ts, op.diff, regs);
    if (accepted) {
      LazyWatch.patch(state, accepted);
      version++;
      broadcast({ t: 'patch', diff: accepted, ts: op.ts, version });
    }
    persist();
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
    compactTombstones: olderThan => { const n = compactTombstones(clocks, olderThan); if (n) persist(); return n; },
    flush: () => storage.flush(),
    dispose() {
      storage.flush();
      for (const s of [...sessions]) s.close();
      LazyWatch.dispose(state);
    }
  };
}
