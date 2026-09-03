// paths.js - Path keys and helpers shared by the merge and the clients
//
// Paths are arrays of string segments. Their map key is the JSON encoding,
// which is unambiguous for any segment content ("a/b" as one key stays
// distinct from two segments). A descendant's key starts with the
// ancestor's key minus its closing bracket, plus a comma.

export const pathKey = path => JSON.stringify(path);

export const parsePathKey = key => JSON.parse(key);

/** Prefix shared by the keys of every strict descendant of `path` */
export const descendantPrefix = path => pathKey(path).slice(0, -1) + ',';

/** Normalize a register spec ('order', 'a/b', or ['a', 'b']) to a key */
export function registerKey(spec) {
  if (Array.isArray(spec)) return pathKey(spec.map(String));
  if (typeof spec === 'string' && spec.length > 0) return pathKey(spec.split('/'));
  throw new TypeError(`Invalid register path: ${JSON.stringify(spec)}`);
}

export function registerSet(specs = []) {
  return new Set(specs.map(registerKey));
}

/** Set `value` at `path` inside a nested plain-object diff, creating levels */
export function setAt(target, path, value) {
  let node = target;
  for (let i = 0; i < path.length - 1; i++) {
    const seg = path[i];
    if (node[seg] === null || typeof node[seg] !== 'object' || Array.isArray(node[seg])) node[seg] = {};
    node = node[seg];
  }
  node[path[path.length - 1]] = value;
  return target;
}

/** Own-property walk; `undefined` when the path does not resolve */
export function valueAt(root, path) {
  let node = root;
  for (const seg of path) {
    if (node === null || typeof node !== 'object' || !Object.hasOwn(node, seg)) return undefined;
    node = node[seg];
  }
  return node;
}
