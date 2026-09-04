// model.js - The data model lazy-storage imposes on top of lazy-watch
//
// State is a tree of plain objects and JSON leaves. Lists of records are
// objects keyed by id (see lazy-watch's "Lists as Keyed Maps" recipe) with
// a position on each record for their order (client/list.js), so an edit
// or deletion addresses a record by identity and two writers never
// collide on a slot. Arrays are whole values: an array of primitives (a
// set of tags, a list of ids, a pair of coordinates) may live anywhere
// and is written and merged as one leaf, last-writer-wins as a unit. An
// array holding objects is refused, since giving a list of records unit
// semantics loses concurrent edits, unless its path is a declared
// REGISTER, which stores any array as one value on purpose.
//
// A diff is flattened into LEAVES, [path, value], for the merge: objects
// recurse, `null` is a deletion leaf, primitives and arrays are leaves.
// The merge decides per leaf and rebuilds the accepted diff.
//
// `registers` everywhere below is the matcher from registerSet(): patterns
// may contain `*` segments, so 'tasks/*/subtaskOrder' declares one
// register per task.
import { LazyWatch } from 'lazy-watch';
import { setAt, valueAt, deleteAt, parsePathKey } from './paths.js';

const { Utils } = LazyWatch;

export class ModelError extends TypeError {
  constructor(message, path) {
    super(message);
    this.name = 'ModelError';
    this.path = path;
  }
}

/** A diff value that stands for an array: a real one or a marked fragment */
export const isArrayish = value =>
  Array.isArray(value) || (Utils.isPlainObject(value) && Utils.hasArrayMarker(value));

/** An array whose every element is a primitive (or null): a whole value anywhere */
export const isPrimitiveArray = value =>
  Array.isArray(value) && value.every(item => item === null || (typeof item !== 'object' && typeof item !== 'function'));

/**
 * Flatten a diff into leaves. Throws a ModelError for an array fragment
 * (arrays travel whole), or for an array holding objects outside a
 * register (a list of records belongs in a keyed map).
 * @param {Object} diff
 * @param {{ matches: (path: string[]) => boolean }} registers - from registerSet()
 * @param {string[]} [path]
 * @param {Array} [out]
 * @returns {Array<[string[], any]>}
 */
export function leaves(diff, registers, path = [], out = []) {
  if (!Utils.isPlainObject(diff)) {
    throw new ModelError('A diff must be a plain object', path);
  }
  for (const key of Object.keys(diff)) {
    const value = diff[key];
    const p = [...path, key];
    if (Utils.isPlainObject(value) && Utils.hasArrayMarker(value)) {
      throw new ModelError(`The array at "${p.join('/')}" must be written as a whole value, not a fragment`, p);
    }
    if (Array.isArray(value)) {
      if (!registers.matches(p) && !isPrimitiveArray(value)) {
        throw new ModelError(
          `The array at "${p.join('/')}" holds objects: keep a list of records as an object keyed by id with positions (db.list), or declare the path a register to store the array as one value`, p);
      }
      out.push([p, value]);
      continue;
    }
    if (registers.matches(p)) {
      out.push([p, value]);
      continue;
    }
    if (Utils.isPlainObject(value) && Object.keys(value).length > 0) {
      leaves(value, registers, p, out);
    } else {
      out.push([p, value]);
    }
  }
  return out;
}

/** Validate a diff against the model without producing leaves */
export function assertModel(diff, registers) {
  leaves(diff, registers);
}

/**
 * lazy-watch emits array changes as fragments ({ 2: 'c', $length: 3 }).
 * Arrays travel as whole values, so replace every array the diff touches
 * (a fragment, a whole array, or a register node) with a deep copy of its
 * current value from the live state (`null` when it was deleted).
 * @param {Object} diff
 * @param {{ matches: (path: string[]) => boolean, size: number }} registers
 * @param {Object} state - live state (a LazyWatch proxy or plain object)
 */
export function expandRegisters(diff, registers, state) {
  // Only the arrays the diff touches are read, and each is copied on its
  // own: snapshotting the whole state here would make every local batch
  // cost as much as the state is large
  const copy = value => {
    if (value === undefined) return null;
    if (value === null || typeof value !== 'object') return value;
    return LazyWatch.isProxy(value) ? LazyWatch.snapshot(value) : structuredClone(value);
  };
  const walk = (node, path) => {
    if (!Utils.isPlainObject(node)) return node;
    const out = {};
    for (const key of Object.keys(node)) {
      const p = [...path, key];
      const value = node[key];
      out[key] = registers.matches(p) || isArrayish(value) ? copy(valueAt(state, p)) : walk(value, p);
    }
    return out;
  };
  return walk(diff, []);
}

/** Rebuild a nested diff from accepted leaves */
export function fromLeaves(entries) {
  const diff = {};
  for (const [path, value] of entries) setAt(diff, path, value);
  return diff;
}

/**
 * `initial` with rows applied on top, shallow paths first, so a
 * container's row lands before its children's and a leaf that later
 * became a container is overridden by the deeper rows. A row is
 * [pathKey, value]; a `null` value deletes the path (a tombstone on the
 * server). Values are cloned, so the rows may be kept.
 * @param {Object} initial
 * @param {Array<[string, any]>} rows
 */
export function rebuild(initial, rows) {
  const state = structuredClone(initial);
  const ordered = rows.map(([key, value]) => [parsePathKey(key), value]).sort((a, b) => a[0].length - b[0].length);
  for (const [path, value] of ordered) {
    if (value === null) deleteAt(state, path);
    else setAt(state, path, structuredClone(value));
  }
  return state;
}
