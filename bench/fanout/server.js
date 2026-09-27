// server.js - The fan-out bench's server: one store, `feed`, that every client
// follows, published to by the server itself (store.patch), as a server's own
// sensor readings or an admin's change would be. Each patch carries the time it
// was due (`at`) and the time it went out (`sent`): a server behind its
// schedule shows as latency from `at`, what delivery takes as latency from `sent`.
//   env: STORAGE ('sqlite' | 'memory'), DB (the sqlite file), KEYS (100),
//        PRESENCE_EVERY (ms; 0 a flush a turn; 'off' no presence), DEFLATE ('off')
// Control: /stats, /run (the paced run, cheaply), /publish?rate&seconds&size
// (paced; refused while one is going), /burst?count&size (in one turn)
import { createStore, memoryStorage } from '../../src/server/index.js';
import { createHandlers } from '../../src/server/bun.js';
import { sqliteStorage } from '../../src/server/sqlite-bun.js';
import { clock, cpuSeconds, control } from './common.js';

const STORE = 'feed';
const KEYS = Number(process.env.KEYS || 100);
const every = process.env.PRESENCE_EVERY ?? '250';
const storage = process.env.STORAGE === 'memory' ? memoryStorage() : sqliteStorage(process.env.DB || ':memory:').store(STORE);
const store = createStore({ initial: { feed: {} }, presence: every === 'off' ? false : { every: Number(every) }, rateLimit: false, storage });
if (!store.snapshot().feed?.k0) {
  const feed = {};
  for (let k = 0; k < KEYS; k++) feed[`k${k}`] = { i: -1, at: 0, sent: 0, pad: '' };
  store.patch({ feed });
}

const handlers = createHandlers({
  stores: id => (id === STORE ? store : null),
  path: '/sync',
  ...(process.env.DEFLATE === 'off' ? { perMessageDeflate: false } : {}),
  // Every client its own user, as a display is its own device: a hello costs a token of its own bucket
  authenticate: req => {
    const id = new URL(req.url).searchParams.get('id');
    return id ? { id } : null;
  },
  relays: {
    authenticate: req => {
      const relay = new URL(req.url).searchParams.get('relay');
      return relay ? { id: `relay:${relay}` } : null;
    }
  },
  onError: err => console.error('server:', err)
});
const sync = Bun.serve({
  hostname: '127.0.0.1',
  port: 0,
  async fetch(req, server) {
    return (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 });
  },
  websocket: handlers.websocket
});

// A pad that compresses as readings do (numbers, about 4 to 6 times under deflate), in a string, so the
// clients' search for "at", "sent" and "v" never lands in it
const padOf = size => {
  let pad = '';
  while (pad.length < size) pad += `${(Math.random() * 1000).toFixed(2)},`;
  return pad.slice(0, size);
};

let published = 0;
let run = null;   // the paced run: { count, done, lagMax, started, finished }
const publishOne = (i, at, pad) => {
  store.patch({ feed: { [`k${i % KEYS}`]: { i, at, sent: clock(), pad } } });
  published++;
};

/**
 * `rate` patches a second for `seconds`, each due at start + i/rate; what is
 * overdue goes at once. After each go the loop yields a turn (Bun.sleep(0), a
 * timer: the topic's write-out runs in the loop's prepare phase, after the
 * immediates), so the next wait is armed after the writes, not before them
 */
async function paced(rate, seconds, size) {
  const count = Math.round(rate * seconds);
  const pad = padOf(size);
  const state = { count, done: 0, lagMax: 0, started: clock(), first: null, last: null, finished: null };
  run = state;
  const start = clock() + 20;
  while (state.done < count) {
    const wait = start + (state.done / rate) * 1000 - clock();
    if (wait > 0) await Bun.sleep(Math.ceil(wait));
    const now = clock();
    for (let at = start + (state.done / rate) * 1000; state.done < count && at <= now; at = start + (state.done / rate) * 1000) {
      state.lagMax = Math.max(state.lagMax, now - at);
      publishOne(state.done, at, pad);
      state.first ??= now;
      state.last = now;
      state.done++;
    }
    await Bun.sleep(0);
  }
  state.finished = clock();
}

control({
  '/stats': () => ({
    cpu: cpuSeconds(),
    rss: process.memoryUsage().rss,
    v: store.version,
    published,
    sessions: store.sessions,
    listed: store.presence().length,
    sockets: handlers.socketStats(),
    run
  }),
  '/run': () => ({ v: store.version, finished: !run || run.finished !== null, largest: handlers.socketStats().largest }),
  '/publish': url => {
    if (run && run.finished === null) return { refused: 'a paced run is still going' };
    const q = name => Number(url.searchParams.get(name));
    const from = store.version;
    const count = Math.round(q('rate') * q('seconds'));
    paced(q('rate'), q('seconds'), q('size') || 0).catch(err => console.error('publish:', err));
    return { from, count };
  },
  '/burst': url => {
    const count = Number(url.searchParams.get('count'));
    const pad = padOf(Number(url.searchParams.get('size')) || 0);
    const from = store.version;
    const at = clock();
    for (let i = 0; i < count; i++) publishOne(i, at, pad);
    return { from, count, ms: clock() - at };
  }
}, { role: 'server', sync: sync.port });
