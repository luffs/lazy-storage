// examples/inspector/server.js - A server with something to look at, and the inspector to look at it with
//
//   node examples/inspector/server.js      then open http://127.0.0.1:3210/ws/inspect
//   STORES=300 node examples/inspector/server.js      as many stores, to see All stores with many
//
// Stores with schemas and history (two unless STORES says), seeded with a
// few records, and a writer that goes on changing them, some much more than
// others and now and then with a write a schema refuses, so that the page has
// records, a history to scrub through, an activity that moves, and stores
// that differ side by side.
//
// The inspector shows every record of every store, so a server says who
// may see it: `authorize(request, { write })`. Here that is anyone, and
// the server listens on this machine alone. A real server checks the
// request as it checks its own admin pages: a session cookie, a header
// its proxy sets, and for `write` (a revert, a disconnect) perhaps a
// role. What it answers is who is looking: a revert is told in the
// history as theirs.
import { createStore, createStores, listItem, oneOf, optional } from '../../src/server/index.js';
import { serve } from '../../src/server/node.js';
import { createClient, webSocketTransport } from '../../src/index.js';

const PORT = Number(process.env.PORT) || 3210;
const COUNT = Math.max(1, Math.min(Number(process.env.STORES) || 2, 5000));
const ids = Array.from({ length: COUNT }, (_, i) => `team-${i + 1}`);

const shape = {
  initial: { tasks: {}, comments: {}, settings: { theme: 'light', title: 'Groceries' } },
  singletons: ['settings'],
  parents: { comments: { taskId: 'tasks' } },
  schemas: {
    tasks: listItem({ title: 'string', done: 'boolean?', owner: 'string?', tags: optional(['string']) }),
    comments: { taskId: 'string', text: 'string' },
    settings: { theme: oneOf('light', 'dark'), title: 'string' }
  }
};

const known = new Set(ids);
const stores = createStores(id => {
  if (!known.has(id)) return null;
  const store = createStore({ ...shape, history: true, presence: true });
  store.patch({
    tasks: {
      milk: { title: 'Milk', owner: 'alice', tags: ['dairy'] },
      eggs: { title: 'Eggs', done: true, owner: 'bob' },
      bread: { title: 'Bread', owner: 'alice' }
    },
    comments: { c1: { taskId: 'milk', text: 'Oat, if they have it' } }
  });
  return store;
});
for (const id of ids) stores.get(id);

// Someone at work: an edit every few seconds in team-1, as two users, and now and then a record made and
// deleted; and, with more stores, edits in the others too, the first ones far more often than the last (team-k
// about 1/k as often), and every so often a theme its schema does not take, which is refused
const users = ['alice', 'bob'];
let n = 0;
const seqs = new Map();
function edit(id) {
  const store = stores.get(id);
  const user = { id: users[n % 2] };
  const replica = `demo-${id}-${user.id}`;
  const op = changes => {
    const seq = (seqs.get(replica) ?? 0) + 1;
    seqs.set(replica, seq);
    try {
      store.apply({ replica, seq, ts: store.now(), changes }, { user });
    } catch (err) {
      if (err.code !== 'invalid') throw err;
    }
  };
  n++;
  const step = n % 6;
  if (step === 0) op([{ c: 'tasks', id: 'milk', path: ['title'], set: n % 12 ? 'Oat milk' : 'Milk' }]);
  else if (step === 1) op([{ c: 'tasks', id: 'eggs', path: ['done'], set: n % 4 === 1 }]);
  else if (step === 2) op([{ c: 'tasks', id: `extra-${n}`, put: { title: `Something else ${n}`, owner: user.id } }, { c: 'comments', id: `c-${n}`, put: { taskId: `extra-${n}`, text: 'Added by the demo' } }]);
  else if (step === 3) op([{ c: 'settings', path: ['theme'], set: n % 12 < 6 ? 'dark' : 'light' }]);
  else if (step === 4) op([{ c: 'tasks', id: 'bread', path: ['tags'], set: ['bakery', `n${n}`] }]);
  else op([{ c: 'tasks', id: `extra-${n - 3}`, del: true }]);
  if (n % 25 === 0) op([{ c: 'settings', path: ['theme'], set: 'purple' }]);
}
const writer = setInterval(() => {
  edit('team-1');
  for (let k = 2; k <= Math.min(COUNT, 400); k++) if (Math.random() < 1 / k) edit(`team-${k}`);
}, 3000);
writer.unref();

const server = serve({
  stores,
  port: PORT,
  host: '127.0.0.1',
  authenticate: request => ({ id: new URL(request.url).searchParams.get('user') ?? 'guest' }),
  inspector: { authorize: () => ({ id: 'you' }) }
});
server.on('listening', () => {
  console.log(`inspector: http://127.0.0.1:${PORT}/ws/inspect`);
  // Two clients on sockets of their own, for the page's sessions to have someone in them
  for (const user of ['carol', 'dave']) {
    const db = createClient({ transport: webSocketTransport(`ws://127.0.0.1:${PORT}/ws?user=${user}`), store: 'team-1', initial: shape.initial, singletons: shape.singletons });
    db.share({ looking: 'at the list' });
    db.connect();
  }
});
