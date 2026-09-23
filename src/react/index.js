// react/index.js - lazy-storage in a React component
//
// React needs no mirror: a component reads db.state while rendering and
// re-renders whenever the client changes, which useClient arranges
// through useSyncExternalStore. The snapshot it hands React is a new
// object for every batch the client sees (local, remote, undo, redo, a
// snapshot), every change of the outbox, and every status, presence, or
// closed event, so a teammate's edit renders exactly like our own. Writes
// go straight to db.state.
//
//   import { useClient } from 'lazy-storage/react';
//
//   function List({ db }) {
//     const { state, status } = useClient(db);
//     return <ul>{state.tasks.map(task => <li key={task.id}>{task.title}</li>)}</ul>;
//   }
//
// One subscription per client serves every component on it, opened when
// the first mounts and closed when the last unmounts.
//
// useClient re-renders its component for every change, which is right
// for a component that reads much of the state. A component that reads a
// little (a row of a long list, a badge) picks it with useClientSelector
// and re-renders only when that changes:
//
//   function Row({ db, id }) {
//     const task = useClientSelector(db, state => state.tasks.find(t => t.id === id));
//     return <li>{task?.title}</li>;
//   }
//
// What the selector returns is copied out of the client's state as plain
// data (the state's records keep their identity as they change, so they
// could never compare unequal) and compared with the last selection,
// deeply unless given an `isEqual`; the component gets the plain copy.
import { useRef, useSyncExternalStore } from 'react';
import { LazyWatch } from 'lazy-watch';

const EVENTS = ['status', 'presence', 'peers', 'sync', 'closed', 'history'];
const trackers = new WeakMap();

/** What React reads: the state (the same proxy every time) and the client's other facts */
const read = db => ({
  state: db.state,
  status: db.status,
  presence: db.presence,
  peers: db.peers,
  pending: db.pending,
  closed: db.closed,
  canUndo: db.canUndo,
  canRedo: db.canRedo,
  restored: db.restored
});

/**
 * The subscribe/getSnapshot pair for a client, one per client, shaped
 * the way useSyncExternalStore wants: the snapshot keeps its identity
 * until something changed, and a change is a batch, an outbox change, or
 * an event. Between subscriptions (no component mounted) the facts are
 * re-read on demand, so a component mounting later starts current.
 */
export function trackClient(db) {
  let tracker = trackers.get(db);
  if (tracker) return tracker;
  const listeners = new Set();
  let stops = [];
  let changes = 0;    // counted so a batch makes a new snapshot even when every fact reads the same
  // For selectors: every change, and the subscription opening, since a
  // change before it (between a render and its subscribe) went uncounted
  let version = 0;
  let readAt = 0;
  let snapshot = read(db);
  const getSnapshot = () => {
    const next = read(db);
    if (readAt === changes && Object.keys(next).every(key => Object.is(next[key], snapshot[key]))) return snapshot;
    readAt = changes;
    snapshot = next;
    return snapshot;
  };
  const changed = () => {
    changes++;
    version++;
    getSnapshot();
    for (const fn of listeners) fn();
  };
  tracker = {
    subscribe(fn) {
      if (listeners.size === 0) {
        stops = [db.watch(changed), ...EVENTS.map(event => db.on(event, changed))];
        version++;
      }
      listeners.add(fn);
      return () => {
        listeners.delete(fn);
        if (listeners.size === 0) for (const off of stops.splice(0)) off();
      };
    },
    getSnapshot,
    /** Bumped by every change while subscribed (and when the subscription opens), so a selector can tell nothing changed without selecting */
    get version() { return version; }
  };
  trackers.set(db, tracker);
  return tracker;
}

/** A selection as plain data: proxies of the client's state become copies, anywhere in it */
function plainOf(value) {
  if (value === null || typeof value !== 'object') return value;
  if (LazyWatch.isProxy(value)) return LazyWatch.snapshot(value);
  if (Array.isArray(value)) return value.map(plainOf);
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return value;
  const out = {};
  for (const key of Object.keys(value)) out[key] = plainOf(value[key]);
  return out;
}

/**
 * What `select(state, db)` picks from the client, re-rendering the
 * component only when that changes (by `isEqual`, deep by default). The
 * selection is handed out as plain data, the same object until it changes
 */
export function useClientSelector(db, select, isEqual = LazyWatch.Utils.deepEqual.bind(LazyWatch.Utils)) {
  const tracker = trackClient(db);
  const last = useRef(null);   // { version, select, value }
  const getSelection = () => {
    const seen = last.current;
    // Nothing changed and the same selector: the same selection, without selecting again
    if (seen && seen.version === tracker.version && seen.select === select) return seen.value;
    const next = plainOf(select(db.state, db));
    const value = seen && isEqual(seen.value, next) ? seen.value : next;
    last.current = { version: tracker.version, select, value };
    return value;
  };
  return useSyncExternalStore(tracker.subscribe, getSelection, getSelection);
}

/** The client's state and facts, read again by the component on every change */
export function useClient(db) {
  const { subscribe, getSnapshot } = trackClient(db);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
