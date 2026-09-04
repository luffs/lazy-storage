// list.js - An ordered list of records: a keyed map with a position on each
//
// `db.list('tasks')` is the ordered view of `state.tasks`, an object keyed
// by id. Every record carries a position key (positions.js); the list's
// order is the keys' string order, ties by id, records without a key
// last. Adding or moving a record writes one field on that record:
// nothing else in the list changes, so two people inserting at the same
// spot both keep theirs, and a move never loses to an unrelated edit.
// Deleting is deleting the record. The server needs no declaration.
//
// `reconcile(ids)` takes the order the user sees, as ids, and writes the
// fewest position changes that make the sort agree: the longest run of
// records already in increasing order keeps its keys, the rest are
// re-slotted between them. That is what an app keeping its own array
// (a drag-and-drop list, a Vue store) calls after a change.
import { LazyWatch } from 'lazy-watch';
import { keyBetween, comparePositions } from '../core/positions.js';
import { randomId } from '../core/ids.js';

const { Utils } = LazyWatch;

/**
 * @param {Object} state - the client's state (a LazyWatch proxy)
 * @param {string|string[]} path - the list's container: 'tasks', 'tasks/x/subtasks', or segments
 * @param {{ position?: string }} [options] - the field holding the key (default 'pos')
 */
export function createList(state, path, { position = 'pos' } = {}) {
  const segments = Array.isArray(path) ? path.map(String) : String(path).split('/');
  if (segments.length === 0 || segments.some(s => s.length === 0)) throw new TypeError(`Invalid list path: ${JSON.stringify(path)}`);
  const name = segments.join('/');

  /** The container object, created along the path when `create` is set */
  function container(create) {
    let node = state;
    for (const segment of segments) {
      if (!Utils.isPlainObject(node[segment])) {
        if (!create) return undefined;
        node[segment] = {};
      }
      node = node[segment];
    }
    return node;
  }

  const keyOf = record => (typeof record[position] === 'string' ? record[position] : null);

  /** [id, record] pairs in list order */
  function sorted(exclude) {
    const map = container(false);
    if (!map) return [];
    return Object.entries(map)
      .filter(([id, record]) => id !== exclude && Utils.isPlainObject(record))
      .sort(([ia, a], [ib, b]) => comparePositions(a[position], ia, b[position], ib));
  }

  /**
   * The keys a record goes between for `where` ({ before, after, at }; the
   * end by default), leaving `exclude` (the record being moved) out. A
   * neighbour without a key, or tied with the lower one, is skipped over,
   * so the new key sorts after any tied group rather than inside it.
   */
  function bounds(where = {}, exclude) {
    const entries = sorted(exclude);
    let index;
    if (where.before != null) {
      index = entries.findIndex(([id]) => id === String(where.before));
      if (index < 0) throw new Error(`No record "${where.before}" in ${name}`);
    } else if (where.after != null) {
      index = entries.findIndex(([id]) => id === String(where.after));
      if (index < 0) throw new Error(`No record "${where.after}" in ${name}`);
      index++;
    } else if (where.at != null) {
      index = Math.max(0, Math.min(entries.length, Math.trunc(where.at)));
    } else {
      index = entries.length;
    }
    let lower = null;
    for (let i = index - 1; i >= 0; i--) {
      const key = keyOf(entries[i][1]);
      if (key !== null) { lower = key; break; }
    }
    let upper = null;
    for (let i = index; i < entries.length; i++) {
      const key = keyOf(entries[i][1]);
      if (key !== null && (lower === null || key > lower)) { upper = key; break; }
    }
    return { lower, upper };
  }

  return {
    /** The container's path segments */
    path: segments,
    /** Records in list order */
    all: () => sorted().map(([, record]) => record),
    /** Ids in list order */
    ids: () => sorted().map(([id]) => id),
    get(id) {
      const map = container(false);
      return map && Utils.isPlainObject(map[id]) ? map[id] : undefined;
    },
    has(id) {
      const map = container(false);
      return Boolean(map && Utils.isPlainObject(map[id]));
    },
    /**
     * Add a record at the end, or `{ before: id }`, `{ after: id }`,
     * `{ at: index }`. Its id is minted unless provided. Returns the id
     */
    add(record, where = {}) {
      if (!Utils.isPlainObject(record)) throw new TypeError('list.add expects a plain object');
      const id = record.id != null ? String(record.id) : randomId();
      const { lower, upper } = bounds(where, id);
      container(true)[id] = { ...record, id, [position]: keyBetween(lower, upper) };
      return id;
    },
    /** Move a record to the end, or before, after, or at; false when it does not exist */
    move(id, where = {}) {
      const record = this.get(id);
      if (!record) return false;
      const { lower, upper } = bounds(where, String(id));
      const current = keyOf(record);
      // Already there: nothing to write
      if (current !== null && (lower === null || current > lower) && (upper === null || current < upper)) return true;
      record[position] = keyBetween(lower, upper);
      return true;
    },
    remove(id) {
      const map = container(false);
      if (!map || !Object.hasOwn(map, id)) return false;
      delete map[id];
      return true;
    },
    /**
     * Make the list sort in the order of `ids` (ids not in the list are
     * ignored; records not in `ids` follow, in their current order),
     * writing as few positions as possible. Returns how many it wrote
     */
    reconcile(ids) {
      const map = container(false);
      if (!map) return 0;
      const listed = [];
      const seen = new Set();
      for (const raw of ids) {
        const id = String(raw);
        if (!seen.has(id) && Utils.isPlainObject(map[id])) { seen.add(id); listed.push(id); }
      }
      const order = [...listed, ...sorted().map(([id]) => id).filter(id => !seen.has(id))];
      const keys = order.map(id => keyOf(map[id]));
      const keep = longestIncreasing(keys);
      let written = 0;
      let lower = null;
      for (let i = 0; i < order.length; i++) {
        if (keep.has(i)) { lower = keys[i]; continue; }
        let upper = null;
        for (let j = i + 1; j < order.length; j++) if (keep.has(j)) { upper = keys[j]; break; }
        const key = keyBetween(lower, upper);
        map[order[i]][position] = key;
        lower = key;
        written++;
      }
      return written;
    },
    /** The key a record would get for `where`, for callers that write positions themselves */
    keyFor: (where = {}) => {
      const { lower, upper } = bounds(where);
      return keyBetween(lower, upper);
    }
  };
}

/** Indices of a longest strictly increasing run of keys (nulls never count); patience sorting */
function longestIncreasing(keys) {
  const tails = [];      // index of the smallest tail of a run of each length
  const previous = new Array(keys.length).fill(-1);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if (key === null) continue;
    let low = 0;
    let high = tails.length;
    while (low < high) {
      const mid = (low + high) >> 1;
      if (keys[tails[mid]] < key) low = mid + 1;
      else high = mid;
    }
    previous[i] = low > 0 ? tails[low - 1] : -1;
    tails[low] = i;
  }
  const keep = new Set();
  for (let i = tails.length ? tails[tails.length - 1] : -1; i >= 0; i = previous[i]) keep.add(i);
  return keep;
}
