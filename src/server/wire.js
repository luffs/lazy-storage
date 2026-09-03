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
 * `{ ...message, store }`, keeping the payload's remembered JSON: the id
 * is spliced in as the first key instead of re-encoding everything.
 */
export function tagStore(message, store) {
  const tagged = { ...message, store };
  const inner = message[JSON_CACHE];
  if (inner !== undefined && inner.length > 2 && !Object.hasOwn(message, 'store')) {
    Object.defineProperty(tagged, JSON_CACHE, { value: `{"store":${JSON.stringify(store)},${inner.slice(1)}` });
  }
  return tagged;
}
