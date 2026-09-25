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
const BYTES_CACHE = Symbol('lazy-storage.bytes');

/** The UTF-8 size of a string: its length for ASCII, more for å, ä, ö and emoji */
export const utf8Bytes = typeof Buffer === 'function'
  ? text => Buffer.byteLength(text, 'utf8')
  : (encoder => text => encoder.encode(text).length)(new TextEncoder());

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
 * The UTF-8 size of the message's JSON if something has encoded it
 * (toJSON), else 0: measured without encoding, once, and remembered
 */
export function encodedBytes(message) {
  const json = message[JSON_CACHE];
  if (json === undefined) return 0;
  let bytes = message[BYTES_CACHE];
  if (bytes === undefined) {
    bytes = utf8Bytes(json);
    Object.defineProperty(message, BYTES_CACHE, { value: bytes });
  }
  return bytes;
}

/**
 * Remember `json` as the message's encoding, for a message assembled from
 * parts already encoded (a snapshot splices in the cached state) so that
 * toJSON never re-encodes it, and `bytes`, its UTF-8 size, when known.
 */
export function presetJSON(message, json, bytes) {
  Object.defineProperty(message, JSON_CACHE, { value: json, configurable: true });
  // Its UTF-8 size too, when the parts' sizes are known (a snapshot's
  // state, measured once per change), so counting it scans nothing
  if (bytes !== undefined) Object.defineProperty(message, BYTES_CACHE, { value: bytes, configurable: true });
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
 * The close code and reason for a socket the app disconnected, or whose
 * session expired: not final, unlike 4401, so the client reconnects (with
 * its usual backoff) and the upgrade authenticates it afresh. Whether it gets back in is then
 * `authenticate`'s call, and every store it asks for is authorized again
 */
export const REAUTHENTICATE = [4001, 'Reauthenticate'];

/**
 * The shortest session an expiry may leave, by default (the adapters' `minSession`):
 * a socket whose credentials run out sooner is turned away as
 * unauthorized, rather than let in to be closed again in a moment, over
 * and over while an app keeps handing over a token that is about to lapse
 */
export const MIN_SESSION_MS = 30_000;

/**
 * The time the adapters' `expiresAt` hook gave, in ms since the epoch:
 * it may answer with a number or a Date, and null or undefined for a
 * session that does not expire
 */
export function expiryOf(value) {
  if (value === null || value === undefined) return null;
  const at = value instanceof Date ? value.getTime() : value;
  if (typeof at !== 'number' || Number.isNaN(at)) throw new TypeError('expiresAt must give a time in ms, a Date, or nothing');
  return at;
}

// setTimeout fires at once past this (some 24.8 days)
const LONGEST_TIMER = 2 ** 31 - 1;

/**
 * Run `fn` at `at` (ms since the epoch), however far off: a longer wait
 * than one timer can hold is taken in steps. Returns a cancel function
 */
export function runAt(at, fn) {
  let timer = null;
  const step = () => {
    const wait = at - Date.now();
    if (wait <= 0) return fn();
    timer = setTimeout(step, Math.min(wait, LONGEST_TIMER));
    if (typeof timer.unref === 'function') timer.unref();
  };
  step();
  return () => clearTimeout(timer);
}

/**
 * The adapters' `socketStats()`: how many sockets are open, the bytes
 * they hold unsent in total and at most, and since the server started,
 * how many were cut off for falling behind, disconnected by the app,
 * closed as their session expired, and store sessions closed by
 * `revalidate`. `sockets` is their `sockets()` list
 */
export function rollUpSockets(sockets, { cutOff, disconnected, expired, revoked }) {
  let buffered = 0;
  let largest = 0;
  for (const s of sockets) {
    buffered += s.buffered;
    if (s.buffered > largest) largest = s.buffered;
  }
  return { sockets: sockets.length, buffered, largest, cutOff, disconnected, expired, revoked };
}

/**
 * `{ ...message, store }`, keeping the payload's JSON: the payload is
 * encoded (once, and remembered on it, where the store counts its size)
 * and the id spliced in as the first key. A lazy property of the message
 * (a snapshot's `state`, decoded only when something reads it) is copied
 * as the getter it is, not invoked.
 */
export function tagStore(message, store) {
  const tagged = {};
  for (const key of Object.keys(message)) Object.defineProperty(tagged, key, Object.getOwnPropertyDescriptor(message, key));
  tagged.store = store;
  if (Object.hasOwn(message, 'store')) return tagged;
  const inner = toJSON(message);
  if (inner.length > 2) {
    Object.defineProperty(tagged, JSON_CACHE, { value: `{"store":${JSON.stringify(store)},${inner.slice(1)}` });
  }
  return tagged;
}
