// wire.js - Serialize a message once, however many sockets it goes to
//
// A store broadcasts one message object to every session; each hub tags
// it with its store id and each socket turns it into JSON. Encoding the
// payload once per broadcast instead of once per socket is the difference
// between O(sessions) and O(1) serialization work per patch. The JSON is
// remembered on the message under a symbol (non-enumerable, so a spread
// or structuredClone of the message does not carry it), and tagging with
// a store id splices the id into the remembered JSON rather than encoding
// the payload again.
const JSON_CACHE = Symbol('lazy-storage.json');

/** The message's JSON, encoded once and remembered on the object */
export function toJSON(message) {
  let json = message[JSON_CACHE];
  if (json === undefined) {
    json = JSON.stringify(message);
    Object.defineProperty(message, JSON_CACHE, { value: json });
  }
  return json;
}

/**
 * Remember `json` as the message's encoding, for a message assembled from
 * parts already encoded (a snapshot splices in the cached state) so that
 * toJSON never re-encodes it.
 */
export function presetJSON(message, json) {
  Object.defineProperty(message, JSON_CACHE, { value: json, configurable: true });
  return message;
}

/**
 * `{ ...message, store }`, keeping the payload's remembered JSON: the id
 * is spliced in as the first key instead of re-encoding everything. A
 * lazy property of the message (a snapshot's `state`, decoded only when
 * something reads it) is copied as the getter it is, not invoked.
 */
/**
 * Turn away a socket whose request did not authenticate. A browser cannot
 * read the status of a refused handshake, so the handshake is completed
 * only to say why: a `closed` message without a store (it is the socket
 * that ends, not one store on it), then a close with code 4401. The
 * client stops reconnecting and reports it on every store attached (see
 * client/connection.js, which knows the code too).
 */
export function closeUnauthorized(ws) {
  ws.send(JSON.stringify({ t: 'closed', code: 'unauthorized', message: 'Unauthorized' }));
  ws.close(4401, 'Unauthorized');
}

export function tagStore(message, store) {
  const tagged = {};
  for (const key of Object.keys(message)) Object.defineProperty(tagged, key, Object.getOwnPropertyDescriptor(message, key));
  tagged.store = store;
  const inner = message[JSON_CACHE];
  if (inner !== undefined && inner.length > 2 && !Object.hasOwn(message, 'store')) {
    Object.defineProperty(tagged, JSON_CACHE, { value: `{"store":${JSON.stringify(store)},${inner.slice(1)}` });
  }
  return tagged;
}
