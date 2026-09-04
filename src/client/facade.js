// facade.js - Plain arrays for the app, keyed maps with positions on the wire
//
// With `lists` declared, the client keeps two states. The WIRE state is
// what syncs: lists are keyed maps whose records carry a position (see
// list.js), and persistence, ops, deltas, and undo all work on it as they
// always have. The VIEW is what the app reads and writes: the same tree
// with every list as a real array, in position order, records carrying
// their id and no position. This module keeps the two in step.
//
// View -> wire. Every batch the app makes on the view is translated into
// plain writes on the wire state, which the client turns into ops. A
// change inside a record ({ 3: { title } } in lazy-watch's array
// fragment) maps its index to the record's id. Anything structural (a
// splice, a length change, a whole record at an index, which is what a
// reorder or a replacement emits) resyncs that list by id: records gone
// from the array are deleted, new ones added (minting an id when the app
// gave none, written back into the view), and `reconcile` writes the
// fewest positions that make the wire order match the array.
//
// Wire -> view. Every batch on the wire state (remote, undo, the app's
// own echoed back, a correction) is applied to the view by id: a deleted
// record is spliced out, a new one spliced in at its sorted place, a
// changed position moves it, changed fields patch the element. Each
// application is idempotent, so the echo of the view's own edits changes
// nothing. A snapshot rebuilds the view wholesale. Applications carry the
// wire batch's meta, so the app's listeners on the view still see
// `origin: 'remote'` (or 'undo'), and the view's own listener knows to
// leave them alone.
import { LazyWatch } from 'lazy-watch';
import { registerSet, valueAt } from '../core/paths.js';
import { keysBetween, comparePositions } from '../core/positions.js';
import { isArrayish } from '../core/model.js';
import { randomId } from '../core/ids.js';
import { createList } from './list.js';

const { Utils } = LazyWatch;
// Batches the facade itself makes on the view, which its listener must not translate back
const FACADE_ORIGINS = new Set(['wire', 'remote', 'undo', 'redo', 'rejected', 'restore', 'ids']);
const WIRE = { origin: 'wire' };
const IDS = { origin: 'ids' };

const isIndex = key => /^(0|[1-9]\d*)$/.test(key);
const plain = value => (LazyWatch.isProxy(value) ? LazyWatch.snapshot(value) : structuredClone(value));
const same = (a, b) => JSON.stringify(plain(a)) === JSON.stringify(plain(b));

/**
 * @param {Object} options
 * @param {Object} options.wire - the client's wire state (a LazyWatch proxy)
 * @param {Array<string|string[]>} options.lists - list paths, register syntax (`*` matches one segment)
 * @param {string} [options.position='pos'] - the position field on wire records
 */
export function createFacade({ wire, lists, position = 'pos' }) {
  const matcher = registerSet(lists);

  // --- Conversions --------------------------------------------------------------------

  /** A wire subtree as the view sees it: lists become sorted arrays, records carry id and no position */
  function fromWire(node, path) {
    if (Array.isArray(node)) return plain(node);
    if (!Utils.isPlainObject(node)) return node;
    if (matcher.matches(path)) {
      return Object.entries(node)
        .filter(([, record]) => Utils.isPlainObject(record))
        .sort(([ia, a], [ib, b]) => comparePositions(a[position], ia, b[position], ib))
        .map(([id, record]) => fromWireRecord(record, [...path, id], id));
    }
    const out = {};
    for (const key of Object.keys(node)) out[key] = fromWire(node[key], [...path, key]);
    return out;
  }

  function fromWireRecord(record, path, id) {
    const out = {};
    for (const key of Object.keys(record)) {
      if (key === position) continue;
      out[key] = fromWire(record[key], [...path, key]);
    }
    out.id = id;
    return out;
  }

  const view = new LazyWatch(fromWire(LazyWatch.snapshot(wire), []));

  // --- Wire writes ------------------------------------------------------------------------

  /** Write `value` at a wire path, creating objects on the way; null deletes */
  function setWire(path, value) {
    let node = wire;
    for (let i = 0; i < path.length - 1; i++) {
      const segment = path[i];
      if (!Utils.isPlainObject(node[segment])) {
        if (value === null) return;
        node[segment] = {};
      }
      node = node[segment];
    }
    const last = path[path.length - 1];
    if (value === null) {
      if (Object.hasOwn(node, last)) delete node[last];
    } else if (!same(node[last], value)) {
      node[last] = value;
    }
  }

  /** The wire map for a list, created when missing */
  function wireMapFor(path) {
    let node = wire;
    for (const segment of path) {
      if (!Utils.isPlainObject(node[segment])) node[segment] = {};
      node = node[segment];
    }
    return node;
  }

  // --- View -> wire ------------------------------------------------------------------------

  /** Translate a view batch (diff) into wire writes; `viewPath` and `wirePath` differ below a list, where the view has an index and the wire an id */
  function translateView(diff, viewPath, wirePath) {
    for (const key of Object.keys(diff)) {
      const value = diff[key];
      const vp = [...viewPath, key];
      const wp = [...wirePath, key];
      if (matcher.matches(wp)) {
        translateList(vp, wp, value);
      } else if (isArrayish(value)) {
        // An array of primitives, whole or as a fragment: the live value travels
        setWire(wp, plain(valueAt(view, vp)));
      } else if (Utils.isPlainObject(value) && Object.keys(value).length > 0) {
        if (Utils.isPlainObject(valueAt(wire, wp))) {
          translateView(value, vp, wp);
        } else {
          // A subtree new to the wire: built field by field so a list in it mints ids into the view
          setWire(wp, {});
          syncObject(valueAt(view, vp), valueAt(wire, wp), vp, wp);
        }
      } else {
        setWire(wp, value === undefined ? null : value);
      }
    }
  }

  /** A change to a list array: field edits map index to id; anything structural resyncs the list by id */
  function translateList(viewPath, wirePath, fragment) {
    const array = valueAt(view, viewPath);
    if (!Array.isArray(array)) {
      // The list was replaced by something else (null, an object): every record goes
      const map = wireMapFor(wirePath);
      for (const id of Object.keys(map)) delete map[id];
      return;
    }
    const map = wireMapFor(wirePath);
    const structural = Array.isArray(fragment)
      || !Utils.isPlainObject(fragment)
      || Object.hasOwn(fragment, '$splice')
      || (Number.isInteger(fragment.$length) && fragment.$length !== Object.keys(map).length)
      || Object.keys(fragment).some(k => isIndex(k) && (!Utils.isPlainObject(fragment[k]) || Object.hasOwn(fragment[k], 'id') || Object.hasOwn(fragment[k], position)));
    if (structural) return syncList(viewPath, wirePath);
    for (const key of Object.keys(fragment)) {
      if (!isIndex(key)) continue;
      const element = array[Number(key)];
      const id = Utils.isPlainObject(element) ? element.id : undefined;
      if (typeof id !== 'string' || !Utils.isPlainObject(map[id])) return syncList(viewPath, wirePath);
      translateView(fragment[key], [...viewPath, key], [...wirePath, id]);
    }
  }

  /** Make the wire map and positions of a list match its view array */
  function syncList(viewPath, wirePath) {
    const array = valueAt(view, viewPath);
    const map = wireMapFor(wirePath);
    const ids = [];
    const minted = [];
    for (let i = 0; i < array.length; i++) {
      const element = array[i];
      if (!Utils.isPlainObject(element)) continue;
      let id = element.id;
      if (typeof id !== 'string' || !id || ids.includes(id)) {
        id = randomId();
        minted.push([i, id]);
      }
      ids.push(id);
    }
    if (minted.length) {
      const fragment = {};
      for (const [index, id] of minted) fragment[index] = { id };
      patchView(viewPath, fragment, IDS);
    }
    const keep = new Set(ids);
    for (const id of Object.keys(map)) if (!keep.has(id)) delete map[id];
    let n = 0;
    for (let i = 0; i < array.length; i++) {
      const element = array[i];
      if (!Utils.isPlainObject(element)) continue;
      const id = ids[n++];
      // A new record starts as a shell and is filled by the same sync as an
      // existing one, so a nested list inside it mints its ids into the view
      if (!Utils.isPlainObject(map[id])) map[id] = { id };
      syncObject(element, map[id], [...viewPath, String(i)], [...wirePath, id]);
    }
    createList(wire, wirePath, { position }).reconcile(ids);
  }

  /** Make a wire object's fields match a view object's, nested lists included; extra wire fields go */
  function syncObject(source, target, viewPath, wirePath) {
    for (const key of Object.keys(source)) {
      if (key === position) continue;
      const vp = [...viewPath, key];
      const wp = [...wirePath, key];
      const value = source[key];
      if (matcher.matches(wp)) {
        if (Array.isArray(value)) syncList(vp, wp);
        else setWire(wp, null);
      } else if (Utils.isPlainObject(value)) {
        if (!Utils.isPlainObject(target[key])) target[key] = {};
        syncObject(value, target[key], vp, wp);
      } else if (!same(value, target[key])) {
        target[key] = plain(value);
      }
    }
    for (const key of Object.keys(target)) {
      if (key === position || Object.hasOwn(source, key)) continue;
      delete target[key];
    }
  }

  // --- Wire -> view --------------------------------------------------------------------------

  /**
   * Patch the view at a path. Where the path crosses an array, that level
   * of the diff carries `$length`, the marker that tells lazy-watch's patch
   * the object is an array fragment (an index-keyed object without one
   * would replace the array with an object). A `$splice` value is its own
   * marker; an index-keyed value gets `$length` too.
   */
  function patchView(viewPath, value, meta) {
    const root = {};
    let diffNode = root;
    let viewNode = view;
    for (let i = 0; i < viewPath.length; i++) {
      const segment = viewPath[i];
      const last = i === viewPath.length - 1;
      if (Array.isArray(viewNode)) diffNode.$length = viewNode.length;
      const target = viewNode?.[segment];
      if (last && Array.isArray(target) && Utils.isPlainObject(value) && !Utils.hasArrayMarker(value)) value.$length = target.length;
      diffNode[segment] = last ? value : {};
      if (!last) {
        diffNode = diffNode[segment];
        viewNode = target;
      }
    }
    LazyWatch.patch(view, root, meta);
  }

  /** Apply a wire batch (diff) to the view */
  function applyWire(diff, viewPath, wirePath, meta) {
    for (const key of Object.keys(diff)) {
      const value = diff[key];
      const vp = [...viewPath, key];
      const wp = [...wirePath, key];
      if (matcher.matches(wp)) {
        applyList(vp, wp, value, meta);
      } else if (Utils.isPlainObject(value) && Object.keys(value).length > 0) {
        if (!Utils.isPlainObject(valueAt(view, vp))) patchView(vp, fromWire(plain(valueAt(wire, wp)), wp), meta);
        else applyWire(value, vp, wp, meta);
      } else if (value === undefined) {
        patchView(vp, null, meta);
      } else if (!same(valueAt(view, vp), value)) {
        patchView(vp, plain(value), meta);
      }
    }
  }

  const indexOf = (array, id) => {
    for (let i = 0; i < array.length; i++) if (Utils.isPlainObject(array[i]) && array[i].id === id) return i;
    return -1;
  };

  /**
   * A wire change under a list. Anything structural (a record gone, new,
   * or repositioned) is settled in one pass that brings the array to the
   * wire's sorted order: deletions first, then a walk down the target
   * order, moving a present record into place or inserting a new one; all
   * as one list of splices, applied in sequence. Moves are decided
   * against the whole target order rather than one record at a time,
   * since a batch that repositions several records (another client's
   * reconcile) leaves no single record's neighbours trustworthy. Field
   * changes on surviving records follow.
   */
  function applyList(viewPath, wirePath, mapDiff, meta) {
    const map = valueAt(wire, wirePath);
    const array = valueAt(view, viewPath);
    if (mapDiff === null || !Utils.isPlainObject(map)) {
      if (Array.isArray(array) && array.length) patchView(viewPath, [], meta);
      return;
    }
    if (!Utils.isPlainObject(mapDiff)) return;
    if (!Array.isArray(array)) {
      patchView(viewPath, fromWire(plain(map), wirePath), meta);
      return;
    }
    const inserted = new Set();
    const structural = Object.keys(mapDiff).some(id =>
      mapDiff[id] === null || !Utils.isPlainObject(map[id]) || indexOf(array, id) < 0 || (Utils.isPlainObject(mapDiff[id]) && Object.hasOwn(mapDiff[id], position)));
    if (structural) {
      const ops = [];
      // The array as it will be, as original indices (-1: a record inserted below)
      const idAt = original => (Utils.isPlainObject(array[original]) ? array[original].id : undefined);
      const current = array.map((_, i) => i);
      const present = new Set(Object.keys(map).filter(id => Utils.isPlainObject(map[id])));
      for (let i = current.length - 1; i >= 0; i--) {
        if (!present.has(idAt(current[i]))) {
          ops.push([i, 1, []]);
          current.splice(i, 1);
        }
      }
      const target = [...present].sort((a, b) => comparePositions(map[a][position], a, map[b][position], b));
      for (let t = 0; t < target.length; t++) {
        const id = target[t];
        if (current[t] !== undefined && current[t] !== -1 && idAt(current[t]) === id) continue;
        const j = current.findIndex((original, k) => k > t && original !== -1 && idAt(original) === id);
        if (j >= 0) {
          ops.push([j, 1, []], [t, 0, [plain(array[current[j]])]]);
          const [original] = current.splice(j, 1);
          current.splice(t, 0, original);
        } else {
          ops.push([t, 0, [fromWireRecord(plain(map[id]), [...wirePath, id], id)]]);
          current.splice(t, 0, -1);
          inserted.add(id);
        }
      }
      if (ops.length) patchView(viewPath, { $splice: ops }, meta);
    }
    for (const id of Object.keys(mapDiff)) {
      const change = mapDiff[id];
      if (change === null || !Utils.isPlainObject(change) || !Utils.isPlainObject(map[id]) || inserted.has(id)) continue;
      const fields = { ...change };
      delete fields[position];
      if (!Object.keys(fields).length) continue;
      const index = indexOf(valueAt(view, viewPath), id);
      if (index >= 0) applyWire(fields, [...viewPath, String(index)], [...wirePath, id], meta);
    }
  }

  // --- Wiring ---------------------------------------------------------------------------------

  const stopWire = LazyWatch.on(wire, (diff, inverse, meta) => {
    // The app's pending writes on the view go first, as their own batch,
    // so they are not swept into the one tagged below
    LazyWatch.flush(view);
    if (meta?.snapshot) {
      LazyWatch.overwrite(view, fromWire(LazyWatch.snapshot(wire), []), meta);
      return;
    }
    applyWire(diff, [], [], meta ?? WIRE);
  });

  const stopView = LazyWatch.on(view, (diff, inverse, meta) => {
    if (meta && FACADE_ORIGINS.has(meta.origin)) return;
    translateView(diff, [], []);
  });

  return {
    view,
    dispose() {
      stopWire();
      stopView();
      LazyWatch.dispose(view);
    }
  };
}

/** `initial` with arrays at list paths turned into keyed maps, for a client that declares lists */
export function wireInitial(initial, lists, position = 'pos') {
  const matcher = registerSet(lists);
  const walk = (node, path) => {
    if (Array.isArray(node)) {
      if (!matcher.matches(path)) return structuredClone(node);
      const map = {};
      const records = node.filter(Utils.isPlainObject);
      const keys = keysBetween(null, null, records.length);
      records.forEach((record, i) => {
        const id = typeof record.id === 'string' && record.id ? record.id : randomId();
        const { [position]: _dropped, ...rest } = record;
        map[id] = { ...walk(rest, [...path, id]), id, [position]: keys[i] };
      });
      return map;
    }
    if (!Utils.isPlainObject(node)) return node;
    const out = {};
    for (const key of Object.keys(node)) out[key] = walk(node[key], [...path, key]);
    return out;
  };
  return walk(initial, []);
}
