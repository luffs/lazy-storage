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
import { useSyncExternalStore } from 'react';

const EVENTS = ['status', 'presence', 'sync', 'closed', 'history'];
const trackers = new WeakMap();

/** What React reads: the state (the same proxy every time) and the client's other facts */
const read = db => ({
  state: db.state,
  status: db.status,
  presence: db.presence,
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
    getSnapshot();
    for (const fn of listeners) fn();
  };
  tracker = {
    subscribe(fn) {
      if (listeners.size === 0) stops = [db.watch(changed), ...EVENTS.map(event => db.on(event, changed))];
      listeners.add(fn);
      return () => {
        listeners.delete(fn);
        if (listeners.size === 0) for (const off of stops.splice(0)) off();
      };
    },
    getSnapshot
  };
  trackers.set(db, tracker);
  return tracker;
}

/** The client's state and facts, read again by the component on every change */
export function useClient(db) {
  const { subscribe, getSnapshot } = trackClient(db);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
