// relay/storage.js - Where a relay keeps its copies
//
// A relay keeps, per store, the server's records with their clocks, the
// version and epoch they are at, its own ops not yet acknowledged, and who
// the server let in through it (so it can answer them while the server is
// away), plus, once, its own replica id:
//
//   loadMeta() -> null | { replicaId }       saveMeta({ replicaId })
//   load(storeId) -> null | copy             save(storeId, copy)
//   flush()                                  write out what is buffered
//
// where copy = { records: [[c, id, data, clock]], v, epoch, own: { seq,
// ops }, admitted: { key: [replica, ...] }, fepoch, fv }.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { join, resolve } from 'node:path';

export { memoryCopies } from './copies.js';

/**
 * One JSON file per store under `dir`, and `relay.json` for the relay's
 * own id; each written whole through a rename, at most every `saveDelay`
 * @param {string} dir
 * @param {{ saveDelay?: number, onError?: (error: any) => void }} [options]
 */
export function fileCopies(dir, { saveDelay = 1000, onError = err => console.error('lazy-storage relay:', err) } = {}) {
  const root = resolve(dir);
  const pending = new Map();
  let timer = null;
  const fileOf = id => join(root, `${id}.json`);
  const read = file => {
    try {
      return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : null;
    } catch (err) {
      onError(err);
      return null;
    }
  };
  const write = (file, data) => {
    mkdirSync(root, { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(data));
    renameSync(tmp, file);
  };
  function flush() {
    clearTimeout(timer);
    timer = null;
    for (const [id, copy] of pending) {
      try {
        write(fileOf(id), copy);
        pending.delete(id);
      } catch (err) {
        onError(err);
      }
    }
  }
  return {
    loadMeta: () => read(join(root, 'relay.json')),
    saveMeta: meta => write(join(root, 'relay.json'), meta),
    load: id => (pending.has(id) ? structuredClone(pending.get(id)) : read(fileOf(id))),
    // Kept as given until it is written: a relay's copy builds its records when they are read, here once a write
    save(id, copy) {
      pending.set(id, copy);
      if (timer) return;
      timer = setTimeout(flush, saveDelay);
      if (typeof timer?.unref === 'function') timer.unref();
    },
    flush
  };
}
