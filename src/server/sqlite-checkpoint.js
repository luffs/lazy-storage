// sqlite-checkpoint.js - A worker thread that copies a SQLite file's write-ahead log back into it
//
// In WAL mode a commit appends pages to the -wal file, and a checkpoint
// copies them into the database file and waits for the disk to have them.
// SQLite runs one itself at the end of the commit that takes the log past
// a thousand pages, on the thread that committed: a server's event loop,
// which then answers nobody until the disk is done. A busy store's slowest
// answers were mostly those waits. So the SQLite adapters (see
// sqlite-shared.js) have this worker checkpoint instead, on a connection of
// its own: every `every` ms a PASSIVE checkpoint, which copies what no
// reader still needs and never makes a writer wait. The main connection
// keeps a checkpoint of its own for a log far longer (a worker that falls
// behind, or is gone).
//
// It closes its connection when told (`close`), and says so through
// `done`, a shared flag the adapter waits on: a file still open here would
// keep its WAL, and on Windows could not be deleted.
//   workerData: { file, driver: 'bun' | 'node', every, done: SharedArrayBuffer }
import { parentPort, workerData } from 'node:worker_threads';

const { file, driver, every, done } = workerData;
const closed = new Int32Array(done);
const db = driver === 'bun'
  ? new (await import('bun:sqlite')).Database(file)
  : new (await import('node:sqlite')).DatabaseSync(file);
db.exec('PRAGMA synchronous = NORMAL;');

function checkpoint() {
  try {
    db.exec('PRAGMA wal_checkpoint(PASSIVE);');
  } catch { /* busy (another checkpoint, a backup): the next one */ }
}

const timer = setInterval(checkpoint, every);
parentPort.on('message', message => {
  if (message !== 'close') return;
  clearInterval(timer);
  try {
    db.close();
  } finally {
    Atomics.store(closed, 0, 1);
    Atomics.notify(closed, 0);
    parentPort.close();
  }
});
