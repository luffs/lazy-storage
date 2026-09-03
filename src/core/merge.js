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
 * @param {Map<string, {ts: any[], deleted?: boolean}>} clocks - mutated
 * @param {any[]} ts - the op's timestamp
 * @param {Object} diff - the op's diff (validated against the model here)
 * @param {Set<string>} registers
 * @returns {{ accepted: Object|null, rejected: string[][] }} accepted diff
 *   (null when nothing won) and the paths of rejected leaves
 */
export function mergeOp(clocks, ts, diff, registers) {
  const entries = leaves(diff, registers);
  liftTombstones(diff, clocks, ts, []);

  const won = [];
  const rejected = [];
  for (const [path, value] of entries) {
    const ok = value === null
      ? acceptDelete(clocks, path, ts)
      : acceptWrite(clocks, path, ts);
    if (ok) won.push([path, value]);
    else rejected.push(path);
  }
  return { accepted: won.length ? fromLeaves(won) : null, rejected };
}

/**
 * A newer object carrying an `id` at a tombstoned path re-adds the record:
 * lift the tombstone so its leaves can be judged on their own timestamps.
 */
function liftTombstones(node, clocks, ts, path) {
  if (!Utils.isPlainObject(node)) return;
  for (const key of Object.keys(node)) {
    const value = node[key];
    if (!Utils.isPlainObject(value)) continue;
    const p = [...path, key];
    const entry = clocks.get(pathKey(p));
    if (entry && entry.deleted && Object.hasOwn(value, 'id') && compareTs(ts, entry.ts) > 0) {
      clocks.delete(pathKey(p));
    }
    liftTombstones(value, clocks, ts, p);
  }
}

function acceptWrite(clocks, path, ts) {
  for (let i = 1; i < path.length; i++) {
    const ancestor = clocks.get(pathKey(path.slice(0, i)));
    if (ancestor && ancestor.deleted) return false;
  }
  const key = pathKey(path);
  const own = clocks.get(key);
  if (own && compareTs(own.ts, ts) >= 0) return false;
  dropDescendants(clocks, path);
  clocks.set(key, { ts });
  return true;
}

function acceptDelete(clocks, path, ts) {
  const key = pathKey(path);
  const own = clocks.get(key);
  if (own && compareTs(own.ts, ts) >= 0) return false;
  const prefix = descendantPrefix(path);
  for (const [k, entry] of clocks) {
    if (k.startsWith(prefix) && compareTs(entry.ts, ts) >= 0) return false;
  }
  dropDescendants(clocks, path);
  clocks.set(key, { ts, deleted: true });
  return true;
}

function dropDescendants(clocks, path) {
  const prefix = descendantPrefix(path);
  for (const k of [...clocks.keys()]) {
    if (k.startsWith(prefix)) clocks.delete(k);
  }
}

/**
 * Forget tombstones older than `olderThan` (a timestamp). Safe once every
 * replica that might still send a write older than that has synced, since
 * such a write would then be rejected on timestamp by nothing and could
 * resurrect a field. Live entries are never compacted: they are the
 * last-writer-wins state itself.
 */
export function compactTombstones(clocks, olderThan) {
  let removed = 0;
  for (const [k, entry] of [...clocks]) {
    if (entry.deleted && compareTs(entry.ts, olderThan) < 0) {
      clocks.delete(k);
      removed++;
    }
  }
  return removed;
}
