// vue/index.js - lazy-storage in a Vue component
//
// Vue cannot follow db.state: it is lazy-watch's proxy, and a remote
// batch lands underneath anything Vue wraps around it, so nothing would
// re-render. What Vue follows is a plain copy patched in place, which is
// what useClient keeps: a reactive mirror of the client's state that
// every batch the client sees (local, remote, undo, redo, a snapshot) is
// applied to, so a teammate's edit renders exactly like our own. Reads
// come from the mirror; writes go to db.state and reach the mirror when
// the batch closes. The client's status, presence, outbox size, closed
// reason, and undo state come along as refs.
//
//   import { useClient } from 'lazy-storage/vue';
//
//   // Composition API
//   const { state, status, presence } = useClient(db);
//   db.state.tasks.push({ title: 'Ship it' });   // a write goes to the client
//
//   // Options API: the same as data, stopped when the component unmounts
//   data() { return { ...useClient(this.db), title: '' }; }
//
// One mirror per client serves every component on it: the first call
// makes it and starts following the client, later calls share it, and
// the last to stop ends it. So a list of a hundred rows, each calling
// useClient, holds one copy of the state and patches it once per batch.
// The mirror is read-only: a write to it would go nowhere but the mirror
// (Vue warns); writes go to db.state.
//
// A component that edits what it shows asks for a writable state instead,
// useClient(db, { writable: true }), and binds it like any data:
//
//   <input v-model="state.title">        // the write goes to the client
//   state.tasks.push({ title: 'Ship it' })
//
// That state is a view over two things the client already has. Reads come
// from db.state, so a write is read back at once, before its batch has
// closed. Vue cannot track db.state, so each read also touches the same
// path in the shared mirror, and that is what Vue tracks: the component
// renders again when a batch lands, its own, a teammate's or an undo.
// Writes go to db.state; a list's methods (push, splice, sort, ...) run on
// the client's array; and a view or a Vue object written in is copied out
// as plain data. The views are cursors by path, one per path and client,
// so the same path is the same object (indexOf finds it), and Vue leaves
// them as they are in data() and reactive(). A computed over the view
// catches up when the batch lands, a direct read at once.
//
// Each call's listeners stop with the current effect scope (a
// component's, or one made with effectScope()); outside any scope, call
// stop().
import { reactive, readonly, shallowRef, getCurrentScope, onScopeDispose, isProxy, toRaw } from 'vue';
import { LazyWatch } from 'lazy-watch';

const EVENTS = ['status', 'presence', 'peers', 'sync', 'closed', 'history'];

const mirrors = new WeakMap();   // client -> the mirror its components share, while any does

/** The shared mirror of a client, made on first use */
function mirrorOf(db) {
  let mirror = mirrors.get(db);
  if (mirror) return mirror;
  const state = reactive(LazyWatch.snapshot(db.state));
  const status = shallowRef(db.status);
  const presence = shallowRef(db.presence);
  const peers = shallowRef(db.peers);
  const pending = shallowRef(db.pending);
  const closed = shallowRef(db.closed);
  const canUndo = shallowRef(db.canUndo);
  const canRedo = shallowRef(db.canRedo);
  const refresh = () => {
    status.value = db.status;
    presence.value = db.presence;
    peers.value = db.peers;
    pending.value = db.pending;
    closed.value = db.closed;
    canUndo.value = db.canUndo;
    canRedo.value = db.canRedo;
  };
  // The mirror follows every batch; the refs follow the client's events
  // (an outbox change is 'sync', undo and redo standing is 'history')
  const stops = [
    db.watch(diff => LazyWatch.patch(state, diff)),
    ...EVENTS.map(event => db.on(event, refresh))
  ];
  let writable = null;
  mirror = {
    users: 0,
    view: { state: readonly(state), status, presence, peers, pending, closed, canUndo, canRedo },
    /** The writable view of the state, made on first use and shared like the mirror */
    writable() {
      return writable || (writable = writableView(db, state));
    },
    release() {
      if (--mirror.users > 0) return;
      for (const off of stops.splice(0)) off();
      mirrors.delete(db);
    }
  };
  mirrors.set(db, mirror);
  return mirror;
}

// A list's methods that change it: run on the client's array. The others
// (map, find, indexOf, iteration) run over the view, so what they read is
// tracked and comes back as views
const MUTATORS = new Set(['push', 'pop', 'shift', 'unshift', 'splice', 'sort', 'reverse', 'fill', 'copyWithin']);
const isObject = value => value !== null && typeof value === 'object';

/** The node at `path` under `root`, or undefined where the path leads nowhere */
function nodeAt(root, path) {
  let node = root;
  for (const key of path) {
    if (!isObject(node)) return undefined;
    node = node[key];
  }
  return node;
}

/**
 * A writable view of a client's state for Vue: reads from db.state, each
 * one tracked through the same path in `mirror` (the reactive mirror that
 * follows every batch), writes to db.state
 */
function writableView(db, mirror) {
  const views = new Map();       // kind and path, as JSON -> its view
  const paths = new WeakMap();   // view -> its path

  // Reading the mirror along the path registers each step with Vue
  const track = (path, key) => {
    const node = nodeAt(mirror, path);
    if (isObject(node)) void node[key];
  };
  const trackKeys = path => {
    const node = nodeAt(mirror, path);
    if (isObject(node)) Reflect.ownKeys(node);
  };
  const live = path => {
    const node = nodeAt(db.state, path);
    if (!isObject(node)) throw new Error(`lazy-storage/vue: nothing to write to at ${path.join('.') || 'the root'}: it was deleted or replaced by a value`);
    return node;
  };

  // What is written: plain data, with a view or a Vue proxy copied out of what it stands for
  const plain = value => {
    if (!isObject(value)) return value;
    if (paths.has(value)) return LazyWatch.snapshot(live(paths.get(value)));
    const raw = isProxy(value) ? toRaw(value) : value;
    if (Array.isArray(raw)) return raw.map(plain);
    return Object.fromEntries(Object.entries(raw).map(([key, item]) => [key, plain(item)]));
  };

  function view(path) {
    // The target only says what kind it is (Array.isArray); everything is read from db.state.
    // A path whose object became a list (or the other way) gets a view of the new kind
    const isList = Array.isArray(nodeAt(db.state, path));
    const id = JSON.stringify([isList, path]);
    const known = views.get(id);
    if (known) return known;
    const target = isList ? [] : {};
    const handler = {
      get(_, key) {
        if (key === '__v_skip') return true;   // Vue leaves it as it is (data(), reactive(), props)
        if (typeof key === 'string' && key.startsWith('__v_')) return undefined;
        const node = nodeAt(db.state, path);
        if (!isObject(node)) return undefined;
        if (Array.isArray(node) && typeof Array.prototype[key] === 'function') {
          if (MUTATORS.has(key)) {
            return (...args) => {
              const result = node[key](...args.map(plain));
              return result === node ? made : result;
            };
          }
          trackKeys(path);
          return Array.prototype[key];
        }
        if (typeof key === 'symbol') return Reflect.get(node, key);
        track(path, key);
        const value = node[key];
        return isObject(value) ? view([...path, key]) : value;
      },
      set(_, key, value) {
        live(path)[key] = plain(value);
        return true;
      },
      deleteProperty(_, key) {
        delete live(path)[key];
        return true;
      },
      has(_, key) {
        track(path, key);
        const node = nodeAt(db.state, path);
        return isObject(node) && key in node;
      },
      ownKeys(t) {
        trackKeys(path);
        const node = nodeAt(db.state, path);
        const keys = isObject(node) ? Reflect.ownKeys(node) : [];
        // An array target has a length of its own, which a proxy must list
        return Array.isArray(t) && !keys.includes('length') ? [...keys, 'length'] : keys;
      },
      getOwnPropertyDescriptor(t, key) {
        if (Array.isArray(t) && key === 'length') return { value: handler.get(t, 'length'), writable: true, enumerable: false, configurable: false };
        const node = nodeAt(db.state, path);
        const found = isObject(node) ? Reflect.getOwnPropertyDescriptor(node, key) : undefined;
        return found && { value: handler.get(t, key), writable: true, enumerable: found.enumerable, configurable: true };
      }
    };
    const made = new Proxy(target, handler);
    views.set(id, made);
    paths.set(made, path);
    return made;
  }

  return view([]);
}

/**
 * @param {Object} db - a client from createClient or openClient
 * @param {Object} [options]
 * @param {boolean} [options.writable=false] - `state` is a writable view:
 *   reads from db.state, tracked through the mirror, and writes to it
 * @returns {{ state: Object, status: Object, presence: Object, peers: Object, pending: Object, closed: Object, canUndo: Object, canRedo: Object, restored: boolean, stop: () => void }}
 *   `state` is a read-only reactive mirror (or, `writable`, the writable
 *   view), the same for every call on this client; the rest of what changes
 *   are shallow refs, shared likewise
 */
export function useClient(db, { writable = false } = {}) {
  const mirror = mirrorOf(db);
  mirror.users++;
  let stopped = false;
  const stop = () => {
    if (stopped) return;
    stopped = true;
    mirror.release();
  };
  if (getCurrentScope()) onScopeDispose(stop);
  const view = { ...mirror.view, restored: db.restored, stop };
  if (writable) view.state = mirror.writable();
  return view;
}
