// relay/copies.js - A relay's copies in memory (see storage.js for the contract)

/** Copies in memory: a relay restarted starts empty */
export function memoryCopies() {
  let meta = null;
  const copies = new Map();
  return {
    loadMeta: () => (meta ? { ...meta } : null),
    saveMeta: next => { meta = { ...next }; },
    load: id => (copies.has(id) ? structuredClone(copies.get(id)) : null),
    save: (id, copy) => { copies.set(id, structuredClone(copy)); },
    flush() {}
  };
}
