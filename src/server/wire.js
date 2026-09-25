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

/**
 * Below this many bytes of state JSON a snapshot goes inline on the socket;
 * at or above, a client that can fetch is pointed at the HTTP route (see
 * snapshot.js), where it is gzipped once rather than per socket
 */
export const SNAPSHOT_THRESHOLD = 64 * 1024;

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
 * The adapters' `perMessageDeflate` option, sorted: null when off, else
 * the `threshold` below which a message goes plain (default 1024 bytes,
 * where compressing costs more than it saves) and `runtime`, the rest of
 * the object for the runtime's own knobs (true when there is none)
 */
export function deflateOptions(perMessageDeflate) {
  if (!perMessageDeflate) return null;
  const { threshold = 1024, ...runtime } = perMessageDeflate === true ? {} : perMessageDeflate;
  if (!(threshold >= 0)) throw new TypeError('perMessageDeflate.threshold must be a number of bytes');
  return { threshold, runtime: Object.keys(runtime).length ? runtime : true };
}

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

/**
 * A store's own counter of what it sends, by message type (see
 * store.stats().sent), under a symbol so that only the modules that send
 * for it (the snapshot route) reach it: `store[TALLY](type, count, bytes)`
 */
export const TALLY = Symbol('lazy-storage.tally');

/** The close code and reason for a socket cut off for falling behind: 1013, try again later */
export const TOO_FAR_BEHIND = [1013, 'Too far behind'];

/**
 * The adapters' `socketStats()`: how many sockets are open, the bytes
 * they hold unsent in total and at most, and how many were cut off for
 * falling behind since the server started. `sockets` is their
 * `sockets()` list
 */
export function rollUpSockets(sockets, cutOff) {
  let buffered = 0;
  let largest = 0;
  for (const s of sockets) {
    buffered += s.buffered;
    if (s.buffered > largest) largest = s.buffered;
  }
  return { sockets: sockets.length, buffered, largest, cutOff };
}

/**
 * `{ ...message, store }`, keeping the payload's remembered JSON: the id
 * is spliced in as the first key instead of re-encoding everything. A
 * lazy property of the message (a snapshot's `state`, decoded only when
 * something reads it) is copied as the getter it is, not invoked.
 */
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
