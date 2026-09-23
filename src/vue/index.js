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
// Each call's listeners stop with the current effect scope (a
// component's, or one made with effectScope()); outside any scope, call
// stop().
import { reactive, readonly, shallowRef, getCurrentScope, onScopeDispose } from 'vue';
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
  mirror = {
    users: 0,
    view: { state: readonly(state), status, presence, peers, pending, closed, canUndo, canRedo },
    release() {
      if (--mirror.users > 0) return;
      for (const off of stops.splice(0)) off();
      mirrors.delete(db);
    }
  };
  mirrors.set(db, mirror);
  return mirror;
}

/**
 * @param {Object} db - a client from createClient or openClient
 * @returns {{ state: Object, status: Object, presence: Object, peers: Object, pending: Object, closed: Object, canUndo: Object, canRedo: Object, restored: boolean, stop: () => void }}
 *   `state` is a read-only reactive mirror, the same for every call on this
 *   client; the rest of what changes are shallow refs, shared likewise
 */
export function useClient(db) {
  const mirror = mirrorOf(db);
  mirror.users++;
  let stopped = false;
  const stop = () => {
    if (stopped) return;
    stopped = true;
    mirror.release();
  };
  if (getCurrentScope()) onScopeDispose(stop);
  return { ...mirror.view, restored: db.restored, stop };
}
