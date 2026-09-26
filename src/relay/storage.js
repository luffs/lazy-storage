// relay/storage.js - Where a relay keeps its copies
//
// A relay (see index.js) keeps a copy of every store it has passed through,
// and the credentials the server has answered, so that a relay restarted
// while the server is away still answers from what it had. Two kinds of
// document, both plain JSON:
//
//   one per store:  { format: 'lazy-storage/relay-copy', store, state, v,
//                     epoch, pastEpochs, registers, clocks, seen, maxTs,
//                     presenceOn, users, diverged, localV, localLog, usedAt }
//   one for the rest: { format: 'lazy-storage/relay-known', centralTs, keys }
//
// A copy's state, its clock table and the replicas' progress (`seen`) are
// one document, written whole: a restart never finds a state that holds an
// op without the progress that says so, and applies it again. The relay
// decides when to write (debounced, see `saveDelay`); an adapter only
// reads and writes, synchronously, and copies what it is handed before it
// returns, since the relay goes on changing it:
//
//   load()             -> { copies: CopyDocument[], known: KnownDocument | null } | null
//   write(storeId, doc)   one store's copy, in place of what was there
//   remove(storeId)       a copy the relay let go
//   writeKnown(doc)       the credentials and the server's clock
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

/** Keeps the documents in memory: a relay made again on the same one (a test, a restart in process) finds them */
export function memoryCopies() {
  const copies = new Map();
  let known = null;
  return {
    load: () => ({ copies: [...copies.values()].map(doc => structuredClone(doc)), known: known && structuredClone(known) }),
    write(storeId, doc) { copies.set(storeId, structuredClone(doc)); },
    remove(storeId) { copies.delete(storeId); },
    writeKnown(doc) { known = structuredClone(doc); }
  };
}

/**
 * One JSON file per store in `dir`, `<storeId>.json`, and `_known.json`
 * for the credentials (a store id cannot start with `_`, so the two never
 * meet), each written through a temp file and a rename, so a crash leaves
 * the last whole document. A file that cannot be read is reported to
 * `onError` and skipped: that store starts again from the server's next
 * snapshot. Store ids that differ only by case share a file on a
 * filesystem that ignores case
 * @param {string} dir
 * @param {{ onError?: (error: any) => void }} [options]
 */
export function fileCopies(dir, { onError = err => console.error('lazy-storage relay:', err) } = {}) {
  const root = resolve(dir);
  const KNOWN = '_known.json';
  const fileOf = storeId => join(root, `${storeId}.json`);
  const write = (file, doc) => {
    mkdirSync(root, { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(doc));
    renameSync(tmp, file);
  };
  return {
    load() {
      if (!existsSync(root)) return null;
      const copies = [];
      let known = null;
      for (const name of readdirSync(root)) {
        if (!name.endsWith('.json')) continue;   // a temp file a crash left behind
        let doc;
        try {
          doc = JSON.parse(readFileSync(join(root, name), 'utf8'));
        } catch (err) {
          onError(new Error(`The relay's copy ${name} could not be read: ${err?.message ?? err}`, { cause: err }));
          continue;
        }
        if (name === KNOWN) known = doc;
        else copies.push(doc);
      }
      return { copies, known };
    },
    write(storeId, doc) { write(fileOf(storeId), doc); },
    remove(storeId) { rmSync(fileOf(storeId), { force: true }); },
    writeKnown(doc) { write(join(root, KNOWN), doc); }
  };
}
