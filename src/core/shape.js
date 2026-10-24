// shape.js - What a store holds, as the server and every client declare it
//
// A store's top level is fixed by `initial`: each key is a COLLECTION (an
// empty object in `initial`, records keyed by id beneath it) or a
// SINGLETON (a key listed in `singletons`, or one whose initial value is
// not an object: one record without an id). A record is a JSON value of
// any depth, merged path by path (doc.js). Two declarations refine that:
//
// - `registers`: paths, with `*` for any one segment, whose value is one
//   leaf, written and merged whole ('tasks/*/geometry', or 'events/*' for
//   a record that is a value of its own). A full path starts with the
//   collection and, for a collection, the record's id.
// - `parents`: `{ child: { field: 'parentCollection' } }`. A record of the
//   child collection whose field names a record of the parent is that
//   record's child: it is not let in while the parent is missing, and it is
//   deleted with the parent (doc.js).
//
// The shape has a hash, which the server sends in its answers so that a
// client declared otherwise hears so.
import { LazyWatch } from 'lazy-watch';
import { registerSet, pathKey } from './paths.js';

const { Utils } = LazyWatch;

export class ModelError extends TypeError {
  constructor(message, path) {
    super(message);
    this.name = 'ModelError';
    this.path = path;
  }
}

/** A key no path may hold: the prototype machinery, and lazy-watch's wire markers */
export const isForbiddenKey = key => Utils.isUnsafeKey(key) || Utils.isReservedDiffKey(key);

/** FNV-1a over a string, as 8 hex digits: a hash to compare, not to protect */
function hash(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}

/**
 * @param {Object} options
 * @param {Object} [options.initial] - the top level: collections as `{}`, singletons with their starting value
 * @param {string[]} [options.singletons] - top-level keys that are one record, not a collection
 * @param {Array<string|string[]>} [options.registers]
 * @param {Object<string, Object<string, string>>} [options.parents]
 */
export function createShape({ initial = {}, singletons = [], registers = [], parents = {} } = {}) {
  if (!Utils.isPlainObject(initial)) throw new TypeError('initial must be a plain object');
  const single = new Set(singletons);
  for (const key of single) if (!Object.hasOwn(initial, key)) throw new TypeError(`Singleton "${key}" is not in initial`);
  const collections = new Set();
  for (const [key, value] of Object.entries(initial)) {
    if (isForbiddenKey(key)) throw new TypeError(`"${key}" is a reserved name`);
    if (!single.has(key) && Utils.isPlainObject(value)) collections.add(key);
    else single.add(key);
  }
  const regs = registerSet(registers);
  for (const c of collections) {
    for (const [id, record] of Object.entries(initial[c])) {
      if (!Utils.isPlainObject(record) && !regs.matches([c, id])) {
        throw new TypeError(`initial.${c} reads as a collection, but "${id}" is not a record: list "${c}" in singletons to make it one record`);
      }
    }
  }
  // child collection -> [{ field, parent }], parent collection -> [{ child, field }]
  const parentFields = new Map();
  const childLinks = new Map();
  for (const [child, fields] of Object.entries(parents ?? {})) {
    if (!collections.has(child)) throw new TypeError(`parents: "${child}" is not a collection`);
    for (const [field, parent] of Object.entries(fields)) {
      if (!collections.has(parent)) throw new TypeError(`parents: "${child}.${field}" points at "${parent}", which is not a collection`);
      if (!parentFields.has(child)) parentFields.set(child, []);
      parentFields.get(child).push({ field, parent });
      if (!childLinks.has(parent)) childLinks.set(parent, []);
      childLinks.get(parent).push({ child, field });
    }
  }
  const described = {
    collections: [...collections].sort(),
    singletons: [...single].sort(),
    registers: regs.patterns.map(p => p.join('/')).sort(),
    parents: [...parentFields].map(([child, links]) => [child, links.map(l => `${l.field}:${l.parent}`).sort()]).sort()
  };

  const shape = {
    initial,
    hash: hash(JSON.stringify(described)),
    described,
    has: key => collections.has(key) || single.has(key),
    isCollection: key => collections.has(key),
    isSingleton: key => single.has(key),
    collections: [...collections],
    singletons: [...single],
    /** The path from the store's root: [c, id, ...path], or [c, ...path] for a singleton */
    full(c, id, path = []) {
      return single.has(c) ? [c, ...path] : [c, id, ...path];
    },
    /** Whether the path in a record is a register (or the record itself is one) */
    isRegister(c, id, path = []) {
      return regs.size > 0 && regs.matches(shape.full(c, id, path));
    },
    /** Whether anything above the path is a register, which makes the path part of a leaf */
    underRegister(c, id, path = []) {
      if (regs.size === 0) return false;
      for (let i = 0; i < path.length; i++) if (regs.matches(shape.full(c, id, path.slice(0, i)))) return true;
      return false;
    },
    /** One key per record: the collection and id, or the singleton's name */
    recordKey(c, id) {
      return single.has(c) ? pathKey([c]) : pathKey([c, id]);
    },
    /** The fields of a child collection that name a parent: [{ field, parent }] */
    parentsOf: c => parentFields.get(c) ?? [],
    /** The collections whose records name a record of this one: [{ child, field }] */
    childrenOf: c => childLinks.get(c) ?? [],
    get hasParents() { return parentFields.size > 0; }
  };
  return shape;
}

/** Throw a ModelError for a forbidden key, or a value JSON cannot carry, anywhere inside a value */
export function assertValue(value, path) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new ModelError(`"${path.join('/')}" holds a number JSON cannot carry`, path);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, i) => assertValue(item, [...path, String(i)]));
    return;
  }
  if (!Utils.isPlainObject(value)) throw new ModelError(`"${path.join('/')}" holds a value that is not JSON`, path);
  for (const key of Object.keys(value)) {
    const p = [...path, key];
    if (isForbiddenKey(key)) throw new ModelError(`"${p.join('/')}" uses the reserved name "${key}"`, p);
    assertValue(value[key], p);
  }
}

/**
 * Check one change against the shape: a known top-level key, an id for a
 * collection's record and none for a singleton's, a path of safe segments,
 * one of put, set or del, and a JSON value. Throws a ModelError
 */
export function assertChange(shape, change) {
  if (!Utils.isPlainObject(change)) throw new ModelError('A change must be an object', []);
  const { c, id, path = [] } = change;
  if (typeof c !== 'string' || !shape.has(c)) throw new ModelError(`"${c}" is not a collection or singleton of this store`, [String(c)]);
  if (shape.isCollection(c)) {
    if (typeof id !== 'string' || id.length === 0 || isForbiddenKey(id)) throw new ModelError(`A record of "${c}" needs an id`, [c]);
  } else if (id !== undefined) {
    throw new ModelError(`"${c}" is a singleton: a change to it has no id`, [c]);
  }
  if (!Array.isArray(path) || path.some(seg => typeof seg !== 'string' || isForbiddenKey(seg))) {
    throw new ModelError('A change\'s path is an array of keys', [c]);
  }
  const kinds = ['put', 'set', 'del'].filter(kind => Object.hasOwn(change, kind));
  const full = shape.full(c, id, path);
  if (kinds.length !== 1) throw new ModelError('A change is one of put, set or del', full);
  if (shape.underRegister(c, id, path)) throw new ModelError(`"${full.join('/')}" is inside a register, which is written whole`, full);
  if (kinds[0] === 'del') {
    if (change.del !== true) throw new ModelError('A deletion is del: true', full);
    if (path.length === 0 && shape.isSingleton(c)) throw new ModelError(`"${c}" is a singleton, and cannot be deleted`, full);
    return;
  }
  const value = change[kinds[0]];
  if (value === undefined) throw new ModelError(`A change at "${full.join('/')}" needs a value`, full);
  assertValue(value, full);
  if (kinds[0] === 'put' && !Utils.isPlainObject(value)) throw new ModelError(`A put writes an object at "${full.join('/')}"`, full);
  if (path.length === 0 && shape.isCollection(c) && !Utils.isPlainObject(value) && !shape.isRegister(c, id)) {
    throw new ModelError(`A record of "${c}" is an object`, full);
  }
}
