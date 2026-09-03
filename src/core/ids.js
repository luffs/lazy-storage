// ids.js - Record and replica ids

/** A 16-character id from the platform's CSPRNG */
export function randomId() {
  return globalThis.crypto.randomUUID().replace(/-/g, '').slice(0, 16);
}
