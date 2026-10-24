// compile.js - A lazy-watch batch as the changes it makes to records
//
// A client's state is a LazyWatch proxy with `inverse: true`, and each
// batch arrives as a diff and its inverse. The diff says what was written
// (nested objects, null for a deletion, arrays as fragments); the inverse
// says what was there before, null where nothing was. Walking the two
// together gives the changes (doc.js):
//
// - a value deleted: `del`
// - a container that was not there before (the inverse says null), or that
//   replaced a leaf: `put`, with its whole current value
// - a container that was there: walked key by key
// - anything else (a leaf, an array, a register): `set`, with its whole
//   current value read from the state, so an array that lazy-watch sent as
//   a fragment travels whole
//
// What breaks the shape throws a ModelError, and the client reverts the batch.
import { LazyWatch } from 'lazy-watch';
import { valueAt } from './paths.js';
import { ModelError, isForbiddenKey } from './shape.js';

const { Utils } = LazyWatch;
const isObj = Utils.isPlainObject;

/** A value of the state as plain data: a proxy snapshotted, anything else cloned */
const plain = value => (value === null || typeof value !== 'object' ? value : LazyWatch.isProxy(value) ? LazyWatch.snapshot(value) : structuredClone(value));

/** Whether a diff node stands for an array: a whole one, or lazy-watch's fragment of one */
const arrayish = node => Array.isArray(node) || (isObj(node) && Utils.hasArrayMarker(node));

/**
 * @param {Object} shape - from createShape
 * @param {Object} diff - the batch's diff
 * @param {Object} inverse - the batch's inverse diff
 * @param {Object} state - the state the batch was made on (a proxy or plain), read for whole values
 * @returns {Object[]} changes
 */
export function compile(shape, diff, inverse, state) {
  const changes = [];
  if (!isObj(diff)) return changes;
  for (const c of Object.keys(diff)) {
    if (!shape.has(c)) throw new ModelError(`"${c}" is not a collection or singleton of this store`, [c]);
    const node = diff[c];
    const inv = isObj(inverse) ? inverse[c] : undefined;
    if (shape.isSingleton(c)) {
      if (node === null) throw new ModelError(`"${c}" is a singleton, and cannot be deleted`, [c]);
      walk(shape, changes, c, undefined, [], node, inv, state);
      continue;
    }
    if (!isObj(node) || arrayish(node)) throw new ModelError(`"${c}" is a collection: write its records, not the collection`, [c]);
    for (const id of Object.keys(node)) {
      if (isForbiddenKey(id)) throw new ModelError(`"${c}/${id}" uses a reserved name`, [c, id]);
      const value = node[id];
      if (value !== null && !isObj(value) && !shape.isRegister(c, id)) throw new ModelError(`A record of "${c}" is an object`, [c, id]);
      walk(shape, changes, c, id, [], value, isObj(inv) ? inv[id] : undefined, state);
    }
  }
  return changes;
}

function walk(shape, changes, c, id, path, node, inv, state) {
  const base = shape.isSingleton(c) ? { c, path } : { c, id, path };
  const full = shape.full(c, id, path);
  if (node === null) {
    changes.push({ ...base, del: true });
    return;
  }
  const leaf = shape.isRegister(c, id, path) || arrayish(node) || !isObj(node);
  if (leaf || !isObj(inv)) {
    const current = valueAt(state, full);
    if (current === undefined) {
      // Written and gone again within the batch
      if (inv !== null && inv !== undefined) changes.push({ ...base, del: true });
      return;
    }
    const value = plain(current);
    changes.push(isObj(value) && !leaf ? { ...base, put: value } : { ...base, set: value });
    return;
  }
  for (const key of Object.keys(node)) {
    if (isForbiddenKey(key)) throw new ModelError(`"${[...full, key].join('/')}" uses a reserved name`, [...full, key]);
    walk(shape, changes, c, id, [...path, key], node[key], inv[key], state);
  }
}

/** Whether `a` is `b` or beneath it (paths as arrays) */
export const within = (a, b) => b.length <= a.length && b.every((seg, i) => a[i] === seg);
