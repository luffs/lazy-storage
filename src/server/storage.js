// storage.js - Server persistence adapters
//
// A storage adapter loads the store's persisted form once at startup and
// saves it after every accepted op. The persisted form is
// { state, clocks, seqs, version } — plain JSON.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

/** Keeps nothing; the store starts from `initial` every time */
export function memoryStorage() {
  let data = null;
  return {
    load: () => data,
    save: next => { data = next; },
    flush: () => {}
  };
}

/**
 * One JSON file, written atomically (temp file + rename) and debounced so
 * a burst of ops costs one write. Call `flush()` before exit.
 * @param {string} file - path of the JSON file
 * @param {{ debounce?: number }} [options]
 */
export function jsonFileStorage(file, { debounce = 200 } = {}) {
  const path = resolve(file);
  let pending = null;
  let timer = null;

  const write = () => {
    if (pending === null) return;
    mkdirSync(dirname(path), { recursive: true });
    const tmp = `${path}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(pending));
    renameSync(tmp, path);
    pending = null;
  };

  return {
    load() {
      if (!existsSync(path)) return null;
      return JSON.parse(readFileSync(path, 'utf8'));
    },
    save(data) {
      pending = data;
      clearTimeout(timer);
      timer = setTimeout(write, debounce);
    },
    flush() {
      clearTimeout(timer);
      write();
    }
  };
}
