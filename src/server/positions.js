// positions.js - Turn an order register into positions on the records
//
// Before lists carried positions, an ordering was a register: an array of
// ids beside the keyed map. `orderToPositions` gives every record in such
// a list a position key in the register's order (records the register
// does not mention follow, by id), deletes the register, and does it in
// one server patch. Run it once, then drop the register from both sides'
// declarations. Patterns take the register syntax, so
// `{ list: 'tasks/*/subtasks', order: 'tasks/*/subtaskOrder' }` migrates
// every task's subtask list.
import { LazyWatch } from 'lazy-watch';
import { keysBetween } from '../core/positions.js';
import { registerKey, setAt, valueAt } from '../core/paths.js';

const { Utils } = LazyWatch;

/** Every path in `state` matching `pattern`, with the segments its `*`s bound */
function matches(state, pattern, path = [], bound = [], out = []) {
  if (pattern.length === path.length) {
    out.push({ path, bound });
    return out;
  }
  const node = valueAt(state, path);
  if (!Utils.isPlainObject(node)) return out;
  const segment = pattern[path.length];
  if (segment === '*') {
    for (const key of Object.keys(node)) matches(state, pattern, [...path, key], [...bound, key], out);
  } else if (Object.hasOwn(node, segment)) {
    matches(state, pattern, [...path, segment], bound, out);
  }
  return out;
}

/**
 * @param {Object} store - the server store
 * @param {Object} mapping
 * @param {string|string[]} mapping.list - the keyed map's path pattern
 * @param {string|string[]} mapping.order - the order register's path pattern, with `*`s matching the list's in turn
 * @param {string} [mapping.position='pos'] - the field to write
 * @returns {{ lists: number, records: number }} what was migrated
 */
export function orderToPositions(store, { list, order, position = 'pos' }) {
  const listPattern = registerKey(list);
  const orderPattern = registerKey(order);
  const stars = orderPattern.filter(s => s === '*').length;
  if (stars !== listPattern.filter(s => s === '*').length) throw new TypeError('list and order patterns must have the same number of * segments');
  const state = store.snapshot();
  const diff = {};
  let lists = 0;
  let records = 0;
  for (const { path, bound } of matches(state, listPattern)) {
    const map = valueAt(state, path);
    if (!Utils.isPlainObject(map)) continue;
    let next = 0;
    const orderPath = orderPattern.map(s => (s === '*' ? bound[next++] : s));
    const ids = valueAt(state, orderPath);
    const listed = (Array.isArray(ids) ? ids : []).map(String).filter(id => Utils.isPlainObject(map[id]));
    const seen = new Set(listed);
    const rest = Object.keys(map).filter(id => Utils.isPlainObject(map[id]) && !seen.has(id)).sort();
    const ordered = [...listed.filter((id, i) => listed.indexOf(id) === i), ...rest];
    if (ordered.length) {
      const keys = keysBetween(null, null, ordered.length);
      ordered.forEach((id, i) => setAt(diff, [...path, id, position], keys[i]));
      records += ordered.length;
    }
    if (ids !== undefined) setAt(diff, orderPath, null);
    lists++;
  }
  if (Object.keys(diff).length) store.patch(diff);
  return { lists, records };
}
