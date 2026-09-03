// A client on the server side: follows the `shared` store of a running
// example server, prints every change, and (under Bun) keeps its own copy
// in SQLite so a restart starts from it and reconnects with a delta.
//   bun examples/mirror.js        (or: node examples/mirror.js)
import { createClient, createConnection, webSocketTransport, memoryOutbox } from '../src/index.js';

const url = process.env.URL ?? 'ws://localhost:3200/ws?name=mirror';
let storage = memoryOutbox();
if (typeof Bun !== 'undefined') {
  const { sqliteClientStorage } = await import('../src/client/sqlite-bun.js');
  storage = sqliteClientStorage(new URL('./mirror.sqlite', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
}

const connection = createConnection({ transport: webSocketTransport(url) });
const db = createClient({ connection, store: 'shared', initial: { tasks: {}, order: [] }, registers: ['order'], storage });
if (db.restored) console.log(`starting from the copy on disk (version ${db.version}, ${db.collection('tasks').ids().length} tasks)`);

db.on('status', status => console.log(`[${status}]`));
db.on('presence', users => console.log(`here now: ${users.join(', ') || 'nobody'}`));
db.watch((changes, inverse, meta) => {
  if (meta?.origin !== 'remote') return;
  for (const [id, task] of Object.entries(changes.tasks ?? {})) {
    if (task === null) console.log(`- ${id} removed`);
    else if (task.title !== undefined) console.log(`+ ${id}: ${task.title}`);
    else console.log(`~ ${id}: ${JSON.stringify(task)}`);
  }
  if (changes.order) console.log(`order: ${changes.order.length} tasks`);
});
db.connect();

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    db.dispose();
    storage.close?.();
    process.exit(0);
  });
}
