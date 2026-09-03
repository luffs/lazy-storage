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

/**
 * Normalize a register spec to segments: 'order', 'tasks/*\/subtaskOrder',
 * or ['tasks', '*', 'subtaskOrder']. A `*` segment matches any one segment.
 */
export function registerKey(spec) {
  const segments = Array.isArray(spec) ? spec.map(String) : typeof spec === 'string' ? spec.split('/') : null;
  if (!segments || segments.length === 0 || segments.some(s => s.length === 0)) {
    throw new TypeError(`Invalid register path: ${JSON.stringify(spec)}`);
  }
  return segments;
}

/**
 * The set of register paths, as a matcher: `matches(path)` is true when
 * some pattern has the path's length and every segment equals the path's
 * or is `*`.
 */
export function registerSet(specs = []) {
  const patterns = specs.map(registerKey);
  return {
    patterns,
    size: patterns.length,
    matches(path) {
      return patterns.some(p => p.length === path.length && p.every((seg, i) => seg === '*' || seg === path[i]));
    }
  };
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

/** Remove the property at `path` if it exists; ancestors are left alone */
export function deleteAt(target, path) {
  const parent = valueAt(target, path.slice(0, -1));
  if (parent !== null && typeof parent === 'object' && !Array.isArray(parent)) {
    delete parent[path[path.length - 1]];
  }
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
