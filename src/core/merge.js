// merge.js - Per-path last-writer-wins with tombstones
//
// The authority (the server) keeps one entry per leaf path it has
// accepted: { ts } for a live value, { ts, deleted: true } for a
// tombstone. An incoming op carries one timestamp for all its leaves.
//
// Rules, all decided by timestamp so every replica agrees:
// - A write at P wins if nothing at P is newer, and no ancestor of P is
//   tombstoned. An older ancestor tombstone still blocks a plain field
//   write (a deleted record's field must not come back as a partial
//   record); it is lifted only by a newer RECORD write, an object carrying
//   an `id`, which re-adds the record wholesale.
// - A deletion at P wins if nothing at or below P is newer; it then drops
//   every descendant entry and leaves a tombstone at P.
// - A write that changes a container into a leaf (or a register) drops the
//   descendant entries the container had.
//
// Besides the accepted diff, the merge reports exactly which entries it
// set and dropped, so a storage adapter can persist the op as row upserts
// and deletes rather than rewriting the whole state.
//
// Tombstones are the only entries that grow without bound. The authority
// may compact them (see `compactTombstones`) once every replica that
// could still hold an older write has synced.
import { compareTs } from './hlc.js';
import { pathKey, descendantPrefix } from './paths.js';
import { leaves, fromLeaves } from './model.js';
import { LazyWatch } from 'lazy-watch';

const { Utils } = LazyWatch;

/**
 * Merge one op into the clocks, deciding per leaf.
 * @param {Map<string, {ts: any[], deleted?: boolean}>} clocks - mutated; a
 *   ClockMap (see clocks.js) finds descendants through its index, a plain
 *   Map by scanning every key
 * @param {any[]} ts - the op's timestamp
 * @param {Object} diff - the op's diff (validated against the model here)
 * @param {Set<string>} registers
 * @returns {{
 *   accepted: Object|null,   rebuilt diff of the winning leaves (null when none)
 *   rejected: string[][],    paths of the losing leaves
 *   won: Array<[string[], any]>,  the winning leaves themselves
 *   dropped: string[]        clock keys removed (descendants of a winning
 *                            write or deletion, lifted tombstones)
 * }}
 */
export function mergeOp(clocks, ts, diff, registers) {
  const entries = leaves(diff, registers);
  const dropped = new Set();
  liftTombstones(diff, clocks, ts, [], dropped);

  const won = [];
  const rejected = [];
  for (const [path, value] of entries) {
    const ok = value === null
      ? acceptDelete(clocks, path, ts, dropped)
      : acceptWrite(clocks, path, ts, dropped);
    if (ok) {
      won.push([path, value]);
      dropped.delete(pathKey(path));
    } else {
      rejected.push(path);
    }
  }
  return { accepted: won.length ? fromLeaves(won) : null, rejected, won, dropped: [...dropped] };
}

/**
 * A newer object carrying an `id` at a tombstoned path re-adds the record:
 * lift the tombstone so its leaves can be judged on their own timestamps.
 */
function liftTombstones(node, clocks, ts, path, dropped) {
  if (!Utils.isPlainObject(node)) return;
  for (const key of Object.keys(node)) {
    const value = node[key];
    if (!Utils.isPlainObject(value)) continue;
    const p = [...path, key];
    const k = pathKey(p);
    const entry = clocks.get(k);
    if (entry && entry.deleted && Object.hasOwn(value, 'id') && compareTs(ts, entry.ts) > 0) {
      clocks.delete(k);
      dropped.add(k);
    }
    liftTombstones(value, clocks, ts, p, dropped);
  }
}

function acceptWrite(clocks, path, ts, dropped) {
  for (let i = 1; i < path.length; i++) {
    const ancestor = clocks.get(pathKey(path.slice(0, i)));
    if (ancestor && ancestor.deleted) return false;
  }
  const key = pathKey(path);
  const own = clocks.get(key);
  if (own && compareTs(own.ts, ts) >= 0) return false;
  dropDescendants(clocks, path, dropped);
  clocks.set(key, { ts });
  return true;
}

function acceptDelete(clocks, path, ts, dropped) {
  const key = pathKey(path);
  const own = clocks.get(key);
  if (own && compareTs(own.ts, ts) >= 0) return false;
  for (const k of descendants(clocks, path)) {
    if (compareTs(clocks.get(k).ts, ts) >= 0) return false;
  }
  dropDescendants(clocks, path, dropped);
  clocks.set(key, { ts, deleted: true });
  return true;
}

function dropDescendants(clocks, path, dropped) {
  for (const k of [...descendants(clocks, path)]) {
    clocks.delete(k);
    dropped.add(k);
  }
}

/**
 * The keys strictly under a path: through the index when the clocks are
 * a ClockMap (the store's), by scanning when they are a plain Map
 */
function descendants(clocks, path) {
  if (typeof clocks.descendants === 'function') return clocks.descendants(path);
  const prefix = descendantPrefix(path);
  return [...clocks.keys()].filter(k => k.startsWith(prefix));
}

/**
 * Forget tombstones older than `olderThan` (a timestamp). Safe once every
 * replica that might still send a write older than that has synced, since
 * such a write would then be rejected on timestamp by nothing and could
 * resurrect a field. Live entries are never compacted: they are the
 * last-writer-wins state itself.
 * @returns {string[]} the removed keys
 */
export function compactTombstones(clocks, olderThan) {
  const removed = [];
  for (const [k, entry] of [...clocks]) {
    if (entry.deleted && compareTs(entry.ts, olderThan) < 0) {
      clocks.delete(k);
      removed.push(k);
    }
  }
  return removed;
}
