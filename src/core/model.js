// model.js - The data model lazy-storage imposes on top of lazy-watch
//
// State is a tree of plain objects and JSON leaves. Lists are objects
// keyed by id (see lazy-watch's "Lists as Keyed Maps" recipe), so an edit
// or deletion addresses a record by identity and two writers never
// collide on a position. Arrays are allowed only at declared REGISTER
// paths, where the whole value is one unit: written wholesale, resolved
// last-writer-wins as a unit (an ordering of ids, a set of tags).
//
// A diff is flattened into LEAVES, [path, value], for the merge: objects
// recurse, `null` is a deletion leaf, primitives and register values are
// leaves. The merge decides per leaf and rebuilds the accepted diff.
//
// `registers` everywhere below is the matcher from registerSet(): patterns
// may contain `*` segments, so 'tasks/*/subtaskOrder' declares one
// register per task.
import { LazyWatch } from 'lazy-watch';
import { setAt, valueAt } from './paths.js';

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

/**
 * Flatten a diff into leaves. Throws a ModelError for an array outside a
 * register, or an array fragment at a register (registers are whole values).
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
    if (registers.matches(p)) {
      if (Utils.isPlainObject(value) && Utils.hasArrayMarker(value)) {
        throw new ModelError(`Register "${p.join('/')}" must be written as a whole value, not an array fragment`, p);
      }
      out.push([p, value]);
      continue;
    }
    if (isArrayish(value)) {
      throw new ModelError(
        `Arrays are not allowed at "${p.join('/')}": store lists as objects keyed by id, or declare the path a register`, p);
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
 * Registers travel as whole values, so replace every register node the
 * diff touches with a deep copy of the register's current value from the
 * live state (`null` when it was deleted).
 * @param {Object} diff
 * @param {{ matches: (path: string[]) => boolean, size: number }} registers
 * @param {Object} state - live state (a LazyWatch proxy or plain object)
 */
export function expandRegisters(diff, registers, state) {
  if (registers.size === 0) return diff;
  const live = LazyWatch.isProxy(state) ? LazyWatch.snapshot(state) : state;
  const walk = (node, path) => {
    if (!Utils.isPlainObject(node)) return node;
    const out = {};
    for (const key of Object.keys(node)) {
      const p = [...path, key];
      if (registers.matches(p)) {
        const value = valueAt(live, p);
        out[key] = value === undefined ? null : structuredClone(value);
      } else {
        out[key] = walk(node[key], p);
      }
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
