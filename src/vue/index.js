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
// The listeners stop with the current effect scope (a component's, or one
// made with effectScope()); outside any scope, call stop().
import { reactive, shallowRef, getCurrentScope, onScopeDispose } from 'vue';
import { LazyWatch } from 'lazy-watch';

const EVENTS = ['status', 'presence', 'sync', 'closed', 'history'];

/**
 * @param {Object} db - a client from createClient or openClient
 * @returns {{ state: Object, status: Object, presence: Object, pending: Object, closed: Object, canUndo: Object, canRedo: Object, restored: boolean, stop: () => void }}
 *   `state` is reactive; the rest of what changes are shallow refs
 */
export function useClient(db) {
  const state = reactive(LazyWatch.snapshot(db.state));
  const status = shallowRef(db.status);
  const presence = shallowRef(db.presence);
  const pending = shallowRef(db.pending);
  const closed = shallowRef(db.closed);
  const canUndo = shallowRef(db.canUndo);
  const canRedo = shallowRef(db.canRedo);
  const refresh = () => {
    status.value = db.status;
    presence.value = db.presence;
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
  const stop = () => {
    for (const off of stops.splice(0)) off();
  };
  if (getCurrentScope()) onScopeDispose(stop);
  return { state, status, presence, pending, closed, canUndo, canRedo, restored: db.restored, stop };
}
