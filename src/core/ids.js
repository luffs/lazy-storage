// ids.js - Record and replica ids

/** A 16-character id from the platform's CSPRNG */
export function randomId() {
  return globalThis.crypto.randomUUID().replace(/-/g, '').slice(0, 16);
}

/** The ids a store may have: safe in a URL, a file name and a table key, without escaping */
const STORE_ID = /^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$/;
export const isStoreId = id => typeof id === 'string' && STORE_ID.test(id);
