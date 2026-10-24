// doc.js - A store's state, its records' clocks, and the rule that decides every change
//
// The state is a plain tree: `state[c][id]` for a collection's record,
// `state[c]` for a singleton. Each record has a CLOCK: a timestamp per
// path inside it, in two maps. `live` holds, for a leaf, when it was
// written, and for a container (the record itself at `[]` included), when
// it was made: its BIRTH. `gone` holds, for a path deleted inside a living
// record, when it was deleted. A record that is deleted leaves nothing:
// its row and its clock go.
//
// A change is { c, id?, path?, put | set | del } (shape.js checks it). Each
// is decided on its own, against the record's clock, by the op's
// timestamp `ts`:
//
// - put P: lands when the record and P's parent are there (or P is the
//   record itself), and nothing deleted at P is newer. Where P is already a
//   container its content is merged in, path by path.
// - set P: lands when the record and P's parent are there, and nothing at
//   P is newer; a set that replaces a container also needs nothing beneath
//   it to be newer.
// - del P: lands when nothing at or beneath P is newer; a record's children
//   in other collections count as beneath it. Where P is not there, nothing
//   happens.
// - every change: the op is not older than the birth of any container
//   above P (the record included). An edit made before a container was made
//   again is an edit of what was deleted.
//
// Newer means a strictly larger timestamp: on a tie the change that came
// later wins. A change that loses is reported with why: 'missing' (its
// record, its parent, or the parent record a child names, is not there),
// 'newer', or 'stale'. Without tombstones outside a living record that is
// enough: a write to a deleted record finds no parent, a record made again
// has a new birth, and a deleted path keeps its time in the record's clock.
//
// Modes: 'judge' (a replica's op: every rule), 'authority' (the server's own
// write: no checks, missing containers and records made, children deleted
// with their parent), 'trust' (a patch from upstream, already decided:
// applied as it is, with its time), 'replay' (a client's pending ops over
// its base: judged as the server would, but for a child's parent record,
// which the server checks) and 'optimistic' (what has no record or parent
// is skipped, and nothing else is checked; no clocks).
import { LazyWatch } from 'lazy-watch';
import { pathKey, valueAt } from './paths.js';
import { ModelError } from './shape.js';

const { Utils } = LazyWatch;
const isObj = Utils.isPlainObject;
const ROOT = pathKey([]);
const clone = value => (value === null || typeof value !== 'object' ? value : structuredClone(value));

/** The kind of change a value makes at a path: null deletes, an object is put, anything else set */
export const changeFor = (base, value) => (value === null ? { ...base, del: true } : isObj(value) ? { ...base, put: value } : { ...base, set: value });

/** Whether clock keys are at or beneath `path` (strictly beneath with `inclusive` false): the path's key worked out once */
function beneath(path, inclusive) {
  if (path.length === 0) return key => inclusive || key !== ROOT;
  const own = pathKey(path);
  const prefix = own.slice(0, -1) + ',';
  return key => (inclusive && key === own) || key.startsWith(prefix);
}

const maxOf = clock => {
  let max = -1;
  if (!clock) return max;
  for (const ts of clock.live.values()) if (ts > max) max = ts;
  for (const ts of clock.gone.values()) if (ts > max) max = ts;
  return max;
};

/** A clock as it is stored: { live: { pathKey: ts }, gone: { pathKey: ts } } */
export const clockToJSON = clock => ({ live: Object.fromEntries(clock?.live ?? []), gone: Object.fromEntries(clock?.gone ?? []) });
export const clockFromJSON = json => ({ live: new Map(Object.entries(json?.live ?? {})), gone: new Map(Object.entries(json?.gone ?? {})) });

/**
 * @param {Object} shape - from createShape
 * @param {Object} [options]
 * @param {Object} [options.state] - the state to start from (default: `initial`)
 * @param {Map<string, {live: Map, gone: Map}>} [options.clocks] - by record key
 * @param {boolean} [options.track=true] - keep clocks (false for a client's base and view)
 */
export function createDoc(shape, { state, clocks, track = true } = {}) {
  state = state ?? structuredClone(shape.initial);
  for (const c of shape.collections) if (!isObj(state[c])) state[c] = {};
  for (const c of shape.singletons) if (!Object.hasOwn(state, c)) state[c] = clone(shape.initial[c]);
  clocks = track ? (clocks ?? new Map()) : null;
  // Children by parent, for a deletion's cascade and its check: parent record key -> Set of child record keys
  const children = new Map();
  const parentsOfChild = new Map(); // child record key -> Set of parent record keys
  const recordAt = new Map(); // child record key -> [c, id]

  const get = (c, id) => (shape.isSingleton(c) ? state[c] : (Object.hasOwn(state[c], id) ? state[c][id] : undefined));
  const clockOf = (key, make) => {
    if (!clocks) return null;
    let clock = clocks.get(key);
    if (!clock && make) clocks.set(key, clock = { live: new Map(), gone: new Map() });
    return clock;
  };

  /** Bring the child index up to date for one record of a child collection */
  function relink(c, id) {
    const links = shape.parentsOf(c);
    if (!links.length) return;
    const key = shape.recordKey(c, id);
    for (const parent of parentsOfChild.get(key) ?? []) children.get(parent)?.delete(key);
    parentsOfChild.delete(key);
    recordAt.delete(key);
    const record = get(c, id);
    if (!isObj(record)) return;
    const now = new Set();
    for (const { field, parent } of links) {
      if (typeof record[field] === 'string') now.add(shape.recordKey(parent, record[field]));
    }
    if (!now.size) return;
    parentsOfChild.set(key, now);
    recordAt.set(key, [c, id]);
    for (const parent of now) {
      if (!children.has(parent)) children.set(parent, new Set());
      children.get(parent).add(key);
    }
  }
  for (const c of shape.collections) if (shape.parentsOf(c).length) for (const id of Object.keys(state[c])) relink(c, id);

  const childrenOf = (c, id) => [...(children.get(shape.recordKey(c, id)) ?? [])].map(key => recordAt.get(key)).filter(Boolean);

  // While a trial runs: each record as it was before the trial first touched it, by record key
  let journal = null;
  function note(c, id) {
    const key = shape.recordKey(c, id);
    if (!journal || journal.has(key)) return;
    const clock = clockOf(key);
    journal.set(key, [c, id, clone(get(c, id)), clock ? clockToJSON(clock) : null]);
  }

  /** The newest time in a record and every record beneath it */
  function subtreeMax(c, id, seen = new Set()) {
    const key = shape.recordKey(c, id);
    if (seen.has(key)) return -1;
    seen.add(key);
    let max = maxOf(clockOf(key));
    for (const [cc, cid] of childrenOf(c, id)) max = Math.max(max, subtreeMax(cc, cid, seen));
    return max;
  }

  function stamp(clock, c, id, path, value, ts) {
    clock.live.set(pathKey(path), ts);
    if (isObj(value) && !shape.isRegister(c, id, path)) {
      for (const key of Object.keys(value)) stamp(clock, c, id, [...path, key], value[key], ts);
    }
  }

  // A clock keeps nothing beneath a path that is not a container in the
  // state (what was beneath went when it stopped being one), so a write to a
  // leaf, or where nothing is, costs its own key and not a scan of the
  // record's clock: `container` says which it is
  function dropBeneath(clock, path, inclusive, container = true) {
    if (!clock) return;
    if (!container) {
      if (inclusive) {
        const own = pathKey(path);
        clock.live.delete(own);
        clock.gone.delete(own);
      }
      return;
    }
    const test = beneath(path, inclusive);
    for (const map of [clock.live, clock.gone]) for (const key of [...map.keys()]) if (test(key)) map.delete(key);
  }

  function newerBeneath(clock, path, ts) {
    if (!clock) return false;
    const test = beneath(path, false);
    for (const map of [clock.live, clock.gone]) for (const [key, at] of map) if (at > ts && test(key)) return true;
    return false;
  }

  const entry = (c, id, path, why) => (shape.isSingleton(c) ? { c, path, why } : { c, id, path, why });

  /** A record and, with `cascade`, every record beneath it: deleted, each said as a change */
  function removeRecord(c, id, out, cascade, seen = new Set()) {
    const key = shape.recordKey(c, id);
    if (seen.has(key) || get(c, id) === undefined) return;
    seen.add(key);
    note(c, id);
    const kids = cascade ? childrenOf(c, id) : [];
    delete state[c][id];
    clocks?.delete(key);
    relink(c, id);
    out.applied.push({ c, id, del: true });
    for (const [cc, cid] of kids) removeRecord(cc, cid, out, cascade, seen);
  }

  /** Whether a record of a child collection names a parent that is not there */
  function orphaned(c, fields) {
    for (const { field, parent } of shape.parentsOf(c)) {
      if (Object.hasOwn(fields, field) && typeof fields[field] === 'string' && get(parent, fields[field]) === undefined) return true;
    }
    return false;
  }

  function run(change, ts, mode, out) {
    const { c, id } = change;
    const path = change.path ?? [];
    let kind = change.del === true ? 'del' : Object.hasOwn(change, 'put') ? 'put' : 'set';
    let value = kind === 'del' ? undefined : change[kind];
    const register = shape.isRegister(c, id, path);
    if (kind === 'set' && isObj(value) && !register) kind = 'put';
    if (kind === 'put' && (register || !isObj(value))) kind = 'set';
    // 'replay' judges as 'judge' does, but a client replaying its own ops sees no records beyond the one at hand
    const judging = mode === 'judge' || mode === 'replay';
    const checks = mode === 'judge';
    const makes = mode === 'authority' || mode === 'trust';
    const lose = why => { out.lost.push(entry(c, id, path, why)); };
    const key = shape.recordKey(c, id);
    note(c, id);
    let record = get(c, id);

    if (path.length === 0) {
      if (kind === 'del') {
        if (shape.isSingleton(c)) throw new ModelError(`"${c}" is a singleton, and cannot be deleted`, [c]);
        if (record === undefined) return;
        if (judging && subtreeMax(c, id) > ts) return lose('newer');
        return removeRecord(c, id, out, mode === 'judge' || mode === 'authority');
      }
      if (kind === 'put' && isObj(record)) {
        for (const k of Object.keys(value)) run(changeFor({ c, id, path: [k] }, value[k]), ts, mode, out);
        return;
      }
      // Made, written whole, or (an object on a singleton that held a plain value) replaced
      if (record === undefined || kind === 'set' || !isObj(record)) {
        if (judging && record !== undefined && maxOf(clockOf(key)) > ts) return lose('newer');
        if (checks && record === undefined && isObj(value) && orphaned(c, value)) return lose('missing');
        if (shape.isSingleton(c)) state[c] = clone(value);
        else state[c][id] = clone(value);
        if (clocks) {
          const clock = { live: new Map(), gone: new Map() };
          clocks.set(key, clock);
          stamp(clock, c, id, [], value, ts);
        }
        relink(c, id);
        out.applied.push(shape.isSingleton(c) ? { c, [kind]: clone(value) } : { c, id, [kind]: clone(value) });
        return;
      }
    }

    if (record === undefined) {
      if (!makes) return lose('missing');
      state[c][id] = record = {};
      if (clocks) clockOf(key, true).live.set(ROOT, ts);
      out.applied.push({ c, id, put: {} });
    }
    const clock = clockOf(key, true);
    if (judging) {
      for (let i = 0; i < path.length; i++) {
        const birth = clock.live.get(pathKey(path.slice(0, i)));
        if (birth !== undefined && ts < birth) return lose('stale');
      }
    }
    const parentPath = path.slice(0, -1);
    let parent = valueAt(record, parentPath);
    if (!isObj(parent) || (parentPath.length && shape.isRegister(c, id, parentPath))) {
      if (!makes) return lose('missing');
      // The server's own write, or a patch already decided: make what is missing on the way
      if (!isObj(record)) {
        record = {};
        if (shape.isSingleton(c)) state[c] = record; else state[c][id] = record;
        dropBeneath(clock, [], false);
        clock?.live.set(ROOT, ts);
      }
      let node = record;
      for (let i = 0; i < parentPath.length; i++) {
        const seg = parentPath[i];
        if (!isObj(node[seg])) {
          node[seg] = {};
          dropBeneath(clock, parentPath.slice(0, i + 1), true, false);
          clock?.live.set(pathKey(parentPath.slice(0, i + 1)), ts);
        }
        node = node[seg];
      }
      parent = node;
    }
    const last = path[path.length - 1];
    const current = Object.hasOwn(parent, last) ? parent[last] : undefined;
    const own = pathKey(path);
    if (checks && kind !== 'del' && path.length === 1 && orphaned(c, { [last]: value })) return lose('missing');

    if (kind === 'put') {
      if (current === undefined) {
        if (judging && (clock.gone.get(own) ?? -1) > ts) return lose('newer');
        parent[last] = clone(value);
        if (clocks) {
          dropBeneath(clock, path, true, false);
          stamp(clock, c, id, path, value, ts);
        }
        out.applied.push({ ...entry(c, id, path), why: undefined, put: clone(value) });
      } else if (isObj(current)) {
        for (const k of Object.keys(value)) run(changeFor({ c, id, path: [...path, k] }, value[k]), ts, mode, out);
        return;
      } else {
        kind = 'set';
      }
    }
    if (kind === 'set') {
      if (judging) {
        if ((clock.live.get(own) ?? -1) > ts || (clock.gone.get(own) ?? -1) > ts) return lose('newer');
        if (isObj(current) && newerBeneath(clock, path, ts)) return lose('newer');
      }
      parent[last] = clone(value);
      if (clocks) {
        dropBeneath(clock, path, true, isObj(current));
        clock.live.set(own, ts);
      }
      out.applied.push({ ...entry(c, id, path), why: undefined, set: clone(value) });
    }
    if (kind === 'del') {
      if (current === undefined) return;
      if (judging && ((clock.live.get(own) ?? -1) > ts || (isObj(current) && newerBeneath(clock, path, ts)))) return lose('newer');
      delete parent[last];
      if (clocks) {
        dropBeneath(clock, path, true, isObj(current));
        clock.gone.set(own, ts);
      }
      out.applied.push({ ...entry(c, id, path), why: undefined, del: true });
    }
    if (path.length === 1 && shape.parentsOf(c).length) relink(c, id);
  }

  const tidy = change => {
    delete change.why;
    if (change.path && change.path.length === 0) delete change.path;
    return change;
  };

  return {
    state,
    clocks,
    get,
    /** The records beneath one, through `parents`: [[c, id], ...] */
    childrenOf,
    /**
     * Decide one change at time `ts`
     * @returns {{ applied: Object[], lost: Object[] }} the changes as they landed, and the losers with why
     */
    apply(change, ts, mode = 'judge') {
      const out = { applied: [], lost: [] };
      run(change, ts, mode, out);
      out.applied = out.applied.map(tidy);
      return out;
    },
    /** Put a record back as given, its data and clock (undefined data: no record), deciding nothing */
    setRecord(c, id, data, clockJSON) {
      const key = shape.recordKey(c, id);
      if (shape.isSingleton(c)) state[c] = data === undefined ? clone(shape.initial[c]) : data;
      else if (data === undefined) delete state[c][id];
      else state[c][id] = data;
      if (clocks) {
        if (data === undefined || !clockJSON) clocks.delete(key);
        else clocks.set(key, clockFromJSON(clockJSON));
      }
      relink(c, id);
    },
    /**
     * Run `fn` (which applies changes) and say what every record it touched
     * was before: { result, before: Map(record key -> [c, id, data, clock]) },
     * for `undo` to put back. If `fn` throws, what it did is put back first
     */
    trial(fn) {
      if (journal) throw new Error('A trial is already running');
      const before = journal = new Map();
      try {
        const result = fn();
        journal = null;
        return { result, before };
      } catch (err) {
        journal = null;
        this.undo(before);
        throw err;
      }
    },
    /** Put back what a trial touched */
    undo(before) {
      for (const [c, id, data, clockJSON] of before.values()) this.setRecord(c, id, data, clockJSON);
    },
    /** A record's clock, as stored */
    clock(c, id) {
      return clockToJSON(clockOf(shape.recordKey(c, id)));
    },
    /** The newest time in a record */
    newest(c, id) {
      return maxOf(clockOf(shape.recordKey(c, id)));
    },
    /** Forget deletions inside records older than `cutoff`; returns the records it touched as [c, id] */
    prune(cutoff) {
      const touched = [];
      if (!clocks) return touched;
      for (const c of [...shape.collections, ...shape.singletons]) {
        const ids = shape.isSingleton(c) ? [undefined] : Object.keys(state[c]);
        for (const id of ids) {
          const clock = clocks.get(shape.recordKey(c, id));
          if (!clock) continue;
          let dropped = false;
          for (const [key, ts] of [...clock.gone]) if (ts < cutoff) { clock.gone.delete(key); dropped = true; }
          if (dropped) touched.push([c, id]);
        }
      }
      return touched;
    },
    /** Every record, as [c, id, data, clock] (id undefined for a singleton) */
    *records() {
      for (const c of shape.collections) for (const id of Object.keys(state[c])) yield [c, id, state[c][id], clockToJSON(clockOf(shape.recordKey(c, id)))];
      for (const c of shape.singletons) yield [c, undefined, state[c], clockToJSON(clockOf(shape.recordKey(c)))];
    }
  };
}

/**
 * Changes as one diff in the tree's shape: { c: { id: ... } } or { c: ... }
 * for a singleton, null for a deletion. For code that reads a batch as a
 * lazy-watch diff (a listener, a validator spanning records)
 */
export function diffOf(shape, changes) {
  const diff = {};
  for (const change of changes) {
    const value = change.del === true ? null : Object.hasOwn(change, 'put') ? change.put : change.set;
    const path = shape.full(change.c, change.id, change.path ?? []);
    let node = diff;
    for (let i = 0; i < path.length - 1; i++) {
      if (!isObj(node[path[i]])) node[path[i]] = {};
      node = node[path[i]];
    }
    const last = path[path.length - 1];
    if (isObj(value) && isObj(node[last])) Object.assign(node[last], structuredClone(value));
    else node[last] = clone(value);
  }
  return diff;
}

/**
 * A doc from stored records: [[c, id, data, clock], ...]; anything the shape
 * does not know is left out. The records are all there is: a collection's
 * seed records in `initial` are not added, since a deleted record leaves no
 * row to say it went (a store writes its seeds when it starts, and a doc
 * that should start from them is createDoc's). A singleton with no record
 * is its initial value
 */
export function docFromRecords(shape, records = []) {
  const state = {};
  for (const c of shape.collections) state[c] = {};
  for (const c of shape.singletons) state[c] = clone(shape.initial[c]);
  const clocks = new Map();
  for (const [c, id, data, clock] of records) {
    if (shape.isCollection(c) && typeof id === 'string') state[c][id] = data;
    else if (shape.isSingleton(c)) state[c] = data;
    else continue;
    clocks.set(shape.recordKey(c, id), clockFromJSON(clock));
  }
  return createDoc(shape, { state, clocks });
}
