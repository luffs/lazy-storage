// follow.js - The server's side of a follower's link
//
// A follower (a relay, see src/relay) dials the server at its own route,
// authenticated as itself, and opens each store on one session of its own
// (`follower: true` in its hello). That socket is an ordinary hub whose
// user is the relay, with two differences:
//
// - Whether the relay may carry a store at all is `authorizeRelay(relay,
//   storeId)` (anything, without one), not the clients' hooks.
// - For each client behind it, the relay asks `vouch` with the client's
//   credential: the request the client's own socket would have made
//   (`credentialRequest`), judged by the adapter's `authenticate`, then
//   `authorizeId` and `authorize` as that client's, each handed `{ relay }`
//   as its last argument so the app can tell. A client let in is a grant on
//   the relay's session (store.js): its ops are judged as its own, and
//   presence lists it.
import { LazyWatch } from 'lazy-watch';
import { createHub } from './hub.js';

const { Utils } = LazyWatch;

/** The headers a credential may carry: what says who the client is, never what a proxy vouches for */
const CREDENTIAL_HEADERS = ['authorization', 'cookie', 'user-agent'];

/**
 * The request a client's own socket would have made, from what a relay
 * says of it (`{ headers, query }`): the same path, and of the headers only
 * those that say who the client is (`authorization`, `cookie`,
 * `user-agent`, and the adapter's `allowed`), never an origin or a
 * forwarded address, which the relay could make up
 */
export function credentialRequest(credential, path, allowed = []) {
  const names = new Set([...CREDENTIAL_HEADERS, ...allowed.map(name => String(name).toLowerCase())]);
  const headers = new Headers();
  const given = Utils.isPlainObject(credential?.headers) ? credential.headers : {};
  for (const [name, value] of Object.entries(given)) {
    if (typeof value !== 'string' || !names.has(name.toLowerCase())) continue;
    try {
      headers.set(name, value);
    } catch { /* a value no request may carry is left out */ }
  }
  const query = typeof credential?.query === 'string' && credential.query.startsWith('?') ? credential.query : '';
  return new Request(`http://relay.invalid${path}${query}`, { headers });
}

/**
 * A hub for a relay's socket
 * @param {(id: string) => Object|null} resolveStore
 * @param {Object} options
 * @param {(message: Object) => void} options.send
 * @param {any} options.relay - who the relay is (its route's authenticate)
 * @param {(credential: Object, relay: any) => Promise<{ user: any }|null>} options.admit - a client's credential judged
 * @param {(relay: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeRelay]
 * @param {Function} [options.authorizeId]
 * @param {Function} [options.authorize]
 * @param {{ burst: number, perSecond: number }|false} [options.rate] - the vouches turned away (unauthorized or forbidden) it may ask for before every vouch waits (hub.js, vouchRate)
 * @param {Object} [options.channel]
 * @param {(error: any) => void} [options.onError]
 */
export function followerHub(resolveStore, { send, relay, admit, authorizeRelay, authorizeId, authorize, rate, channel, onError }) {
  const hub = createHub(resolveStore, {
    send,
    user: relay,
    ...(rate === undefined ? {} : { vouchRate: rate }),
    channel,
    onError,
    authorizeId: (user, id) => (user === relay
      ? (authorizeRelay ? authorizeRelay(relay, id) : true)
      : (authorizeId ? authorizeId(user, id, { relay }) : true)),
    authorize: authorize ? (user, id, store) => (user === relay ? true : authorize(user, id, store, { relay })) : undefined,
    vouch: async credential => {
      const admitted = await admit(credential, relay);
      if (!admitted) return null;
      return admitted.user === undefined ? {} : admitted.user;
    }
  });
  hub.relay = relay;
  return hub;
}
