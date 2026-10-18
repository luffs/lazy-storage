// merge.js - Per-path last-writer-wins with tombstones
//
// The authority (the server) keeps one entry per leaf path it has
// accepted: { ts } for a live value, { ts, deleted: true } for a
// tombstone, { ts, made: true, since, v } for a barrier (below). An
// incoming op carries one timestamp for all its leaves, and the store
// version its replica had applied when it made them (`seen`).
//
// Rules, all decided by timestamp so every replica agrees:
// - A write at P wins if nothing at P is newer, and no ancestor of P is
//   tombstoned. An older ancestor tombstone still blocks a plain field
//   write (a deleted record's field must not come back as a partial
//   record); it is lifted only by a newer write that re-adds the
//   container wholesale: a RECORD write, an object carrying an `id`, or a
//   container the op's author MADE (`made`: the paths the replica had
//   nothing at before the batch, so what it writes there is new, not a
//   stale edit of what was deleted). The authority's own op (`authority`:
//   the server's patch) lifts a newer tombstone on its way whatever it
//   writes there: it is not a stale replica but the source of truth, and
//   a record it recreates under a deleted key carries no id more often
//   than not. Nothing is lifted beneath a tombstone that stays, nor
//   beneath a barrier the op neither passes nor may write under. Beneath a
//   tombstone that is lifted nothing stands (the deletion dropped it all),
//   so every leaf the op writes there wins, and the barrier stands for
//   something.
// - A lifted tombstone becomes a BARRIER: `since` the deletion's
//   timestamp, `ts` the write that re-added the container, `v` the store
//   version that write became, `by` the replicas that re-added it. Whether
//   an edit beneath it is stale is not a matter of time but of what its
//   author had seen: a plain write or deletion under a barrier lands only
//   from a replica that had applied version `v`, the re-add, or that made
//   a re-add itself (it has the container it wrote, whatever it has heard
//   back), so an edit of the deleted container, however late it was made,
//   is refused, while an edit of the re-added one goes by timestamp as
//   anywhere. A write that re-adds the container itself passes the barrier
//   when newer than the deletion, so two replicas re-adding the same
//   container after one deletion both land, whichever arrives first, and
//   both are among `by`. A barrier says the container is there: an empty object
//   written at its path (an ensure) leaves it as it is, and a barrier
//   persisted is `{}` at its path on load. A deletion refused under a
//   barrier of what is not there is MOOT: nothing to do, and nothing lost.
// - A deletion at P wins if nothing at or below P is newer; it then drops
//   every descendant entry and leaves a tombstone at P.
// - A write that changes a container into a leaf (or a register) drops the
//   descendant entries the container had. An empty object is the one
//   exception: it ensures a container and keeps what is under it. A
//   register is one leaf whatever its value: a tombstone at its path is an
//   older entry a newer write out-writes, no lifting and no barrier.
//
// Besides the accepted diff, the merge reports exactly which entries it
// set and dropped, so a storage adapter can persist the op as row upserts
// and deletes rather than rewriting the whole state.
//
// Tombstones and barriers are the only entries that grow without bound.
// The authority may compact them (see `compactTombstones` and
// `settleBarriers`) once every replica that could still hold an older
// write has synced.
import { compareTs } from './hlc.js';
import { pathKey, descendantPrefix } from './paths.js';
import { leaves, fromLeaves } from './model.js';
import { LazyWatch } from 'lazy-watch';

const { Utils } = LazyWatch;

/**
 * Merge one op into the clocks, deciding per leaf.
 * @param {Map<string, {ts: any[], deleted?: boolean, made?: boolean, since?: any[], v?: number, by?: string[]}>} clocks - mutated; a
 *   ClockMap (see clocks.js) finds descendants through its index, a plain
 *   Map by scanning every key
 * @param {any[]} ts - the op's timestamp
 * @param {Object} diff - the op's diff (validated against the model here)
 * @param {Set<string>} registers
 * @param {{ authority?: boolean, made?: string[][], seen?: number, v?: number, author?: string }} [options] -
 *   `authority`: the op is the server's own; a newer tombstone at any
 *   object it writes is lifted, id or not, and no barrier holds it back.
 *   `made`: paths of containers the op's replica made in this batch (it had
 *   nothing at them before); a newer tombstone at one of them, or at any
 *   container the op writes under one, is lifted. Anything else there is
 *   ignored. `seen`: the store version the op's replica had applied when
 *   it made the op (0 when unknown: it had seen no re-add). `v`: the
 *   version the op's patch becomes, stamped on the barriers it leaves.
 *   `author`: the op's replica (its timestamp's, when absent), named by
 *   the barriers it leaves or passes, which let its later ops through
 * @returns {{
 *   accepted: Object|null,   rebuilt diff of the winning leaves (null when none)
 *   rejected: string[][],    paths of the losing leaves (a moot deletion is
 *                            in neither list)
 *   won: Array<[string[], any]>,  the winning leaves themselves
 *   dropped: string[],       clock keys removed (descendants of a winning
 *                            write or deletion)
 *   barriers: string[]       clock keys whose entry is a barrier this op set,
 *                            passed (its author added) or stood on with an
 *                            ensure: the row at each is the barrier's,
 *                            whatever the op won there
 * }}
 */
export function mergeOp(clocks, ts, diff, registers, { authority = false, made = [], seen = 0, v = 0, author = ts[2] } = {}) {
  const entries = leaves(diff, registers);
  const dropped = new Set();
  const barriers = new Set();
  const passed = new Set();
  const madeKeys = new Set(madePaths(made).map(pathKey));
  const sees = authority ? Infinity : Number.isInteger(seen) && seen >= 0 ? seen : 0;
  const under = { passed, sees, author: typeof author === 'string' ? author : ts[2] };
  liftTombstones(diff, clocks, ts, [], { authority, madeKeys, barriers, v, under, registers });

  const won = [];
  const rejected = [];
  for (const [path, value] of entries) {
    const verdict = value === null
      ? acceptDelete(clocks, path, ts, dropped, under)
      : acceptWrite(clocks, path, ts, dropped, barriers, under, isEnsure(value, path, registers));
    if (verdict === true) {
      won.push([path, value]);
      dropped.delete(pathKey(path));
    } else if (verdict === false) {
      rejected.push(path);
    }
  }
  return { accepted: won.length ? fromLeaves(won) : null, rejected, won, dropped: [...dropped], barriers: [...barriers] };
}

/** A deletion of what is not there, under a barrier its author had not seen: nothing to do, and nothing lost */
const MOOT = 'moot';

/** Whether something names a path a replica made: an array of keys (an op from anywhere names them) */
export const isMadePath = path => Array.isArray(path) && path.length > 0 && path.every(segment => typeof segment === 'string');
const madePaths = made => (Array.isArray(made) ? made : []).filter(isMadePath);

/**
 * A newer object at a deleted path re-adds what was there, when it
 * carries an `id` (a record written whole), when its author made it
 * (`madeKeys`, and so every container under one: the author had nothing
 * there either), or when the author is the authority. The tombstone
 * becomes a barrier stamped with this op and its author, which passes it
 * (`under.passed`), so the leaves beneath are judged on their own
 * timestamps, against nothing: the deletion dropped every entry there,
 * and no tombstone under a tombstone can have been set since. At a
 * barrier, such a write newer than the deletion re-adds the container
 * too: both re-adds land, and the author joins the barrier's. Beneath a
 * tombstone that stays, or a barrier the op may not write under (see
 * mayWriteUnder), nothing is lifted (`blocked`): what is under it is
 * refused whole. A register is one leaf, written whole: a tombstone at its
 * path is an older entry, out-written on timestamp as any, not lifted
 */
function liftTombstones(node, clocks, ts, path, lifting, made = false, blocked = false) {
  if (!Utils.isPlainObject(node)) return;
  const { under } = lifting;
  for (const key of Object.keys(node)) {
    const value = node[key];
    if (!Utils.isPlainObject(value)) continue;
    const p = [...path, key];
    if (lifting.registers.matches(p)) continue;
    const k = pathKey(p);
    const here = made || lifting.madeKeys.has(k);
    const lifts = !blocked && (lifting.authority || here || Object.hasOwn(value, 'id'));
    const entry = clocks.get(k);
    let stays = blocked;
    if (entry?.deleted) {
      if (lifts && compareTs(ts, entry.ts) > 0) {
        clocks.set(k, { ts, made: true, since: entry.ts, v: lifting.v, by: [under.author] });
        lifting.barriers.add(k);
        under.passed.add(k);   // its own barrier: the leaves beneath are the re-add's
      } else {
        stays = true;
      }
    } else if (entry?.made) {
      if (lifts && compareTs(ts, entry.since) > 0) {
        under.passed.add(k);
        if (!entry.by.includes(under.author)) {
          entry.by.push(under.author);
          lifting.barriers.add(k);   // the row says who re-added it: written again
        }
      } else if (!mayWriteUnder(entry, k, under)) {
        stays = true;
      }
    }
    liftTombstones(value, clocks, ts, p, lifting, here, stays);
  }
}

/**
 * Whether the op may write beneath a barrier: it passed it with a re-add
 * of its own, its replica had applied the re-add (`sees` at the barrier's
 * version or past it), or its replica is one that re-added the container
 * (what it writes there is of the container it wrote, whatever it has
 * heard back since)
 */
const mayWriteUnder = (entry, key, under) => under.passed.has(key) || under.sees >= entry.v || entry.by.includes(under.author);

/**
 * Whether a write or deletion at `path` is held back by what stands above
 * it: a tombstone (`tombstones`), or a barrier the op may not write under:
 * what was there was deleted and re-added, and an edit made without
 * knowing is an edit of what was deleted
 */
function staleUnder(clocks, path, tombstones, under) {
  for (let i = 1; i < path.length; i++) {
    const key = pathKey(path.slice(0, i));
    const ancestor = clocks.get(key);
    if (!ancestor) continue;
    if (ancestor.deleted) { if (tombstones) return true; } else if (ancestor.made && !mayWriteUnder(ancestor, key, under)) return true;
  }
  return false;
}

/**
 * An empty object outside a register says "a container is here" (lazy-watch
 * emits one for a container created empty, `settings ??= {}`), not "empty
 * it": a container replaced by an empty one arrives with its keys as
 * nulls. It lands without dropping what is under the path, as a patch
 * merges it into the state, so the rows persisted match the state held
 */
const isEnsure = (value, path, registers) =>
  Utils.isPlainObject(value) && Object.keys(value).length === 0 && !registers.matches(path);

function acceptWrite(clocks, path, ts, dropped, barriers, under, ensure = false) {
  if (staleUnder(clocks, path, true, under)) return false;
  const key = pathKey(path);
  const own = clocks.get(key);
  if (own?.made && ensure) {
    // The container is there, as the barrier says: the ensure lands (nothing
    // to apply) and the barrier stays the row at that path
    barriers.add(key);
    return true;
  }
  if (own && compareTs(own.ts, ts) >= 0) return false;
  if (!ensure) dropDescendants(clocks, path, dropped);
  clocks.set(key, { ts });
  return true;
}

function acceptDelete(clocks, path, ts, dropped, under) {
  const key = pathKey(path);
  if (staleUnder(clocks, path, false, under)) return clocks.has(key) || anyUnder(clocks, path) ? false : MOOT;
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

function anyUnder(clocks, path) {
  for (const _ of descendants(clocks, path)) return true;
  return false;
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

/**
 * Settle barriers older than `olderThan`: each becomes the live entry of a
 * container written empty, which is what it has stood for since the lift,
 * so the row at its path is `{}` from then on and the container survives a
 * load as before. Safe only with a HORIZON: the caller keeps the highest
 * version among the barriers it settled and refuses every op made in sight
 * of less (the server's `horizon`), since a settled barrier can no longer
 * tell an edit of the deleted container from one of the re-added, and
 * timestamps never could.
 * @returns {Array<[string, object]>} the settled keys, each with the barrier it was
 */
export function settleBarriers(clocks, olderThan) {
  const settled = [];
  for (const [k, entry] of [...clocks]) {
    if (entry.made && compareTs(entry.ts, olderThan) < 0) {
      clocks.set(k, { ts: entry.ts });
      settled.push([k, entry]);
    }
  }
  return settled;
}
