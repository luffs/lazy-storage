// hub.js - Many stores over one connection
//
// A hub is the server side of a multiplexed connection: it looks like a
// session (receive/close) but every incoming message names a `store`, and
// the hub keeps one real session per store on this socket, tagging what
// the sessions send back with the store id. Store sessions are created
// lazily on the first message for a store, after `authorize(user, id,
// store)` allowed it (synchronously or through a promise; messages for
// that store queue meanwhile), and closed by `leave`, by eviction, or when
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
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 */
export function createHub(resolveStore, { send, user, authorize } = {}) {
  if (typeof send !== 'function') throw new TypeError('A hub needs a send function');
  const sessions = new Map();
  const pending = new Map(); // store id -> messages queued while authorization is in flight
  let closed = false;

  const refuse = (id, code, message) => send({ t: 'closed', store: id, code, message });

  function open(id, store) {
    const session = store.session({
      send: message => send(tagStore(message, id)),
      user,
      onEvict: () => sessions.delete(id)
    });
    sessions.set(id, session);
    return session;
  }

  function deliver(id, msg) {
    const { store: _store, ...inner } = msg;
    sessions.get(id)?.receive(inner);
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
        sessions.delete(id);
        pending.delete(id);
        return;
      }
      if (sessions.has(id)) return deliver(id, msg);
      if (pending.has(id)) return void pending.get(id).push(msg);

      // A store that cannot be opened (a factory or migration that throws)
      // is refused like an unknown one and logged as the server fault it
      // is; the connection and its other stores are unaffected
      let store;
      try {
        store = resolveStore(id);
      } catch (err) {
        console.error(`lazy-storage: opening store "${id}" failed:`, err);
        return refuse(id, 'unknown-store', `Store "${id}" could not be opened: ${err?.message ?? err}`);
      }
      if (!store) return refuse(id, 'unknown-store', `Unknown store "${id}"`);
      const forbidden = () => refuse(id, 'forbidden', `Not allowed to access store "${id}"`);
      let verdict;
      try {
        verdict = authorize ? authorize(user, id, store) : true;
      } catch (err) {
        return refuse(id, 'forbidden', err?.message || `Not allowed to access store "${id}"`);
      }
      if (verdict && typeof verdict.then === 'function') {
        pending.set(id, [msg]);
        verdict.then(
          ok => {
            const queued = pending.get(id) ?? [];
            pending.delete(id);
            if (closed) return;
            if (!ok) return forbidden();
            open(id, store);
            for (const m of queued) deliver(id, m);
          },
          err => {
            pending.delete(id);
            if (!closed) refuse(id, 'forbidden', err?.message || `Not allowed to access store "${id}"`);
          }
        );
        return;
      }
      if (!verdict) return forbidden();
      open(id, store);
      deliver(id, msg);
    },
    /** Store ids with a live session on this connection */
    get stores() { return [...sessions.keys()]; },
    get user() { return user; },
    close() {
      closed = true;
      for (const session of sessions.values()) session.close();
      sessions.clear();
      pending.clear();
    }
  };
}
