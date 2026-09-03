// hub.js - Many stores over one connection
//
// A hub is the server side of a multiplexed connection: it looks like a
// session (receive/close) but every incoming message names a `store`, and
// the hub keeps one real session per store on this socket, tagging what
// the sessions send back with the store id. Store sessions are created
// lazily on the first message for a store and closed by `leave` or when
// the connection closes. The per-store protocol is untouched, so a store
// cannot tell a hub session from a direct one.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';

const { Utils } = LazyWatch;

/**
 * @param {(id: string) => Object|null} resolveStore - store for an id, or null
 * @param {{ send: (message: Object) => void }} options
 */
export function createHub(resolveStore, { send }) {
  const sessions = new Map();

  return {
    receive(msg) {
      if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
      if (msg.t === 'ping') return send({ t: 'pong' });
      const id = msg.store;
      if (!isStoreId(id)) return send({ t: 'error', store: typeof id === 'string' ? id : undefined, message: 'A message on a multiplexed connection needs a valid store id' });
      if (msg.t === 'leave') {
        sessions.get(id)?.close();
        sessions.delete(id);
        return;
      }
      let session = sessions.get(id);
      if (!session) {
        const store = resolveStore(id);
        if (!store) return send({ t: 'error', store: id, message: `Unknown store "${id}"` });
        session = store.session({ send: message => send({ ...message, store: id }) });
        sessions.set(id, session);
      }
      const { store: _store, ...inner } = msg;
      session.receive(inner);
    },
    /** Store ids with a live session on this connection */
    get stores() { return [...sessions.keys()]; },
    close() {
      for (const session of sessions.values()) session.close();
      sessions.clear();
    }
  };
}
