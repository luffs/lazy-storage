// hub.js - Many stores over one connection
//
// A hub is the server side of a multiplexed connection: it looks like a
// session (receive/close) but every incoming message names a `store`, and
// the hub keeps one real session per store on this socket, tagging what
// the sessions send back with the store id. Store sessions are created
// lazily on the first message for a store, after `authorizeId(user, id)`
// (before the store is resolved) and `authorize(user, id, store)` (after)
// allowed it (synchronously or through a promise; messages for that store
// queue meanwhile), and closed by `leave`, by eviction, or when
// the connection closes. The per-store protocol is untouched, so a store
// cannot tell a hub session from a direct one.
//
// Terminal conditions for one store are reported with a `closed` message
// carrying a code — 'invalid-store', 'unknown-store', 'forbidden' — which
// the client treats as final for that store.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';
import { tagStore } from './wire.js';

const { Utils } = LazyWatch;

/**
 * @param {(id: string) => Object|null} resolveStore - store for an id, or null
 * @param {Object} options
 * @param {(message: Object) => void} options.send
 * @param {any} [options.user] - the authenticated user, attached to every
 *   store session opened on this connection
 * @param {(user: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeId]
 *   runs before the store is resolved, so a refusal loads nothing and
 *   reads the same whether the store exists or not; the check to use when
 *   it needs only the user and the id
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 *   runs once the store is resolved, for a check that needs the store
 * @param {{ subscribe(id: string): void, unsubscribe(id: string): void, publish(id: string, message: Object): void }} [options.channel]
 *   a transport that can fan a message out to every socket on a store at
 *   once (Bun's topic publish). Given one, a store's patch goes out as a
 *   single `publish` per store rather than a send per socket; each socket
 *   subscribes when it opens a store and unsubscribes on leave, eviction,
 *   or close
 * @param {{ url(storeId: string): string, threshold: number }} [options.httpSnapshots]
 *   where a client that can fetch gets a snapshot of `threshold` bytes or
 *   more, when the transport serves the route (see snapshot.js)
 * @param {(error: any) => void} [options.onError] - server faults (a store
 *   factory that threw); default console
 */
export function createHub(resolveStore, { send, user, authorizeId, authorize, channel, httpSnapshots, onError = err => console.error('lazy-storage:', err) } = {}) {
  if (typeof send !== 'function') throw new TypeError('A hub needs a send function');
  const sessions = new Map();
  const pending = new Map(); // store id -> messages queued while authorization is in flight
  let closed = false;

  const refuse = (id, code, message) => send({ t: 'closed', store: id, code, message });

  function drop(id) {
    sessions.delete(id);
    // Stop this socket hearing the store's broadcasts (the socket stays for its other stores)
    channel?.unsubscribe(id);
  }

  function open(id, store) {
    // Subscribe before the session exists, so no broadcast can slip past on its way in
    channel?.subscribe(id);
    const session = store.session({
      send: message => send(tagStore(message, id)),
      user,
      onEvict: () => drop(id),
      // The transport fans a store's patch out to every subscribed socket at once, tagged and compressed once
      broadcast: channel ? message => channel.publish(id, tagStore(message, id)) : undefined,
      // Where a client that can fetch gets a large snapshot, when the transport serves the route
      httpSnapshot: httpSnapshots ? { url: httpSnapshots.url(id), threshold: httpSnapshots.threshold } : undefined
    });
    sessions.set(id, session);
    return session;
  }

  function deliver(id, msg) {
    const { store: _store, ...inner } = msg;
    sessions.get(id)?.receive(inner);
  }

  /**
   * Open a store for this connection: `authorizeId`, then the store, then
   * `authorize`, each of which may answer through a promise. Messages for
   * the store queue meanwhile. The queue is this attempt's token: a `leave`
   * (and perhaps a new attempt) disowns it, and whatever it was waiting for
   * is then ignored rather than opening a second session
   */
  function admit(id, msg) {
    const queued = [msg];
    pending.set(id, queued);
    const current = () => !closed && pending.get(id) === queued;
    const deny = message => {
      pending.delete(id);
      refuse(id, 'forbidden', message || `Not allowed to access store "${id}"`);
    };
    // Run a step, then `next` with its verdict, now or when its promise settles
    const step = (run, next) => {
      let verdict;
      try {
        verdict = run();
      } catch (err) {
        return deny(err?.message);
      }
      if (!verdict || typeof verdict.then !== 'function') return next(verdict);
      verdict.then(v => { if (current()) next(v); }, err => { if (current()) deny(err?.message); });
    };
    // Before the store is touched: a refusal here loads nothing, and reads
    // the same whether the store exists or not
    step(() => (authorizeId ? authorizeId(user, id) : true), allowed => {
      if (!allowed) return deny();
      // A store that cannot be opened (a factory or migration that throws)
      // is refused like an unknown one and logged as the server fault it
      // is; the connection and its other stores are unaffected
      let store;
      try {
        store = resolveStore(id);
      } catch (err) {
        onError(err);
        pending.delete(id);
        return refuse(id, 'unknown-store', `Store "${id}" could not be opened: ${err?.message ?? err}`);
      }
      if (!store) {
        pending.delete(id);
        return refuse(id, 'unknown-store', `Unknown store "${id}"`);
      }
      step(() => (authorize ? authorize(user, id, store) : true), ok => {
        if (!ok) return deny();
        pending.delete(id);
        open(id, store);
        for (const m of queued) deliver(id, m);
      });
    });
  }

  return {
    receive(msg) {
      if (closed) return;
      if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
      if (msg.t === 'ping') return send({ t: 'pong' });
      const id = msg.store;
      if (!isStoreId(id)) {
        return refuse(typeof id === 'string' ? id : undefined, 'invalid-store', 'A message on a multiplexed connection needs a valid store id');
      }
      if (msg.t === 'leave') {
        sessions.get(id)?.close();
        drop(id);
        pending.delete(id);
        return;
      }
      if (sessions.has(id)) return deliver(id, msg);
      if (pending.has(id)) return void pending.get(id).push(msg);

      admit(id, msg);
    },
    /** Store ids with a live session on this connection */
    get stores() { return [...sessions.keys()]; },
    get user() { return user; },
    close() {
      closed = true;
      for (const [id, session] of sessions) {
        session.close();
        channel?.unsubscribe(id);
      }
      sessions.clear();
      pending.clear();
    }
  };
}
