// clients.js - N thin clients of a store (`feed`, or STORES between them):
// raw sockets that say hello, take the answer's version, and then only read
// each patch's version and the times it carries, without applying it, so a
// few processes hold thousands and what is measured is the server and the
// relays, not the clients. A client's hello says presence: false unless it
// is an admin's (ADMIN=1)
//
// Writes: `writers` clients of the process each write a key of their own
// (`w-<id>` in the feed), `count` ops between them at `rate` a second, round
// robin, as live ops on their sockets, stamped as a replica stamps them. Each
// op carries the time it was due and the time it went out as the server's
// patches do, so every client hears the patch it makes as it hears the
// server's, and its author times its ack (sent → ack) besides. Against a
// deployment (run.js --url) no server of the bench's own says which versions
// a phase is, so each op carries the phase's number too (`ph`), and a client
// counts the patches that carry it. The writers start at the `first` store
// of those the process holds (their share of all the processes' writers),
// so however few there are, the stores take turns
//
// The storm (/storm): every answered client drops its socket at once and
// dials again as a client does after a drop (250-500 ms later: the first
// step of its backoff), its hello asking for what it missed since its
// version; each one's time from dialling to its answer is recorded
//   env: TARGETS (comma-separated ws(s)://host:port/path), N, OFFSET, ADMIN, INFLIGHT (8),
//        STORES (comma-separated, client i's the i-th round robin; `feed`), AUTH (the query
//        parameter naming a client: `id`), QUERY (more of the query, for every socket),
//        RUN (in every replica's id, so a store kept from an earlier run takes this one's
//        ops as new), INSECURE (1: any TLS certificate is taken)
// Control: /stats, /count (cheap: counts and the versions held),
// /reset?from&count (the phase's window: only patches from+1..from+count count),
// /reset?phase (only patches carrying that phase's number count),
// /write?rate&count&writers&size&first (paced; refused while one is going), /storm
import { clock, cpuSeconds, control, newHistogram, record, padOf } from './common.js';

const TARGETS = process.env.TARGETS.split(',');
const N = Number(process.env.N);
const OFFSET = Number(process.env.OFFSET || 0);
const ADMIN = process.env.ADMIN === '1';
const INFLIGHT = Number(process.env.INFLIGHT || 8);
const STORES = (process.env.STORES || 'feed').split(',');
const AUTH = process.env.AUTH || 'id';
const QUERY = new URLSearchParams(process.env.QUERY || '');
const RUN = process.env.RUN ? `${process.env.RUN}-` : '';
const SOCKET_OPTIONS = process.env.INSECURE === '1' ? { tls: { rejectUnauthorized: false } } : null;

// `histogram` from when a patch was due, `delivery` from when the server sent it; `stale`: patches outside the window.
// A writer's: `acks` from when its op went out to its ack, `refused` its ops the server turned away (an error
// carrying the op's seq), `skipped` the ops due while it had no answered socket to send them on, `written` the
// ops it sent, by store. The storm's: `back` the clients answered again (`deltas` of them with a delta, not a
// snapshot), `storms` from dialling to the answer
const fresh = () => ({
  received: 0, stale: 0, gaps: 0, retold: 0, errors: 0, presence: 0, histogram: newHistogram(), delivery: newHistogram(),
  acked: 0, refused: 0, skipped: 0, acks: newHistogram(), written: {}, back: 0, deltas: 0, storms: newHistogram()
});
// `failed`: sockets that closed before they opened (refused, TLS, no port left), the first one's error kept
const stats = { answered: 0, closes: 0, failed: 0, firstError: null, ...fresh() };
let window = { phase: null, from: -1, to: Infinity };
const clients = [];
const members = {};  // how many of this process's clients each store has
let writes = null;   // the paced write run: { count, done, lagMax, first, last, finished }

const numberAfter = (text, key, from = 0) => {
  const at = text.indexOf(key, from);
  if (at < 0) return NaN;
  let end = at + key.length;
  while (end < text.length && '0123456789.-e'.includes(text[end])) end++;
  return Number(text.slice(at + key.length, end));
};

function open(c) {
  const ws = SOCKET_OPTIONS ? new WebSocket(c.url, SOCKET_OPTIONS) : new WebSocket(c.url);
  c.ws = ws;
  c.v = null;
  let opened = false;
  ws.onopen = () => {
    opened = true;
    c.opened?.();
    // After a storm's drop, as a client comes back: asking for what it missed since the version it had
    ws.send(c.since ? JSON.stringify({ ...c.helloMessage, ...c.since }) : c.hello);
    c.since = null;
  };
  ws.onerror = event => {
    stats.firstError ??= String(event?.message || 'error');
  };
  ws.onmessage = ({ data }) => {
    const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
    // A patch is read without parsing it: its version is its last field, the times are in its diff
    const t = text.indexOf('"t":"patch"');
    if (t >= 0 && t < 40) {
      if (c.v === null) return;   // before the answer: the answer's version covers it
      const v = numberAfter(text, '"v":', text.lastIndexOf('"v":'));
      if (v !== c.v + 1) stats.gaps++;
      c.v = v;
      const counted = window.phase !== null ? numberAfter(text, '"ph":') === window.phase : v > window.from && v <= window.to;
      if (!counted) {
        stats.stale++;
        return;
      }
      stats.received++;
      const now = clock();
      const at = numberAfter(text, '"at":');
      if (at > 0) record(stats.histogram, now - at);
      const sent = numberAfter(text, '"sent":');
      if (sent > 0) record(stats.delivery, now - sent);
      return;
    }
    const m = JSON.parse(text);
    if (m.t === 'ack') {
      const sentAt = c.pending.get(m.seq);
      if (sentAt === undefined) return;
      c.pending.delete(m.seq);
      stats.acked++;
      record(stats.acks, clock() - sentAt);
    } else if (m.t === 'snapshot' || m.t === 'delta') {
      if (!c.answered) {
        c.answered = true;
        stats.answered++;
      }
      c.v = m.v;
      c.epoch = m.epoch;
      if (c.dialled !== null) {
        record(stats.storms, clock() - c.dialled);
        stats.back++;
        if (m.t === 'delta') stats.deltas++;
        c.dialled = null;
      }
    } else if (m.t === 'presence') stats.presence++;
    else if (m.t === 'closed' && m.code === 'unavailable') {
      // The relay lost its place (or the store went for a moment): hello again
      stats.retold++;
      c.v = null;
      setTimeout(() => ws.readyState === 1 && ws.send(c.hello), 250 + Math.random() * 750);
    } else if (m.t === 'error') {
      stats.errors++;
      if (Number.isInteger(m.seq) && c.pending.delete(m.seq)) stats.refused++;
      if (m.code === 'rate-limited') setTimeout(() => ws.readyState === 1 && ws.send(c.hello), (m.retryAfter || 100) + Math.random() * 500);
    }
  };
  ws.onclose = () => {
    c.opened?.();
    if (c.answered) {
      c.answered = false;
      stats.answered--;
    }
    c.v = null;
    // Its ops not acknowledged are gone with the socket (the bench keeps no outbox)
    c.pending.clear();
    if (c.leaving) {
      c.leaving = false;
      setTimeout(() => {
        c.dialled = clock();
        open(c);
      }, 250 + Math.random() * 250);
      return;
    }
    stats.closes++;
    if (!opened) stats.failed++;
    // (A storm's client that failed to get back keeps its time from its first dial, retries and all)
    setTimeout(() => open(c), 1000 + Math.random() * 1000);
  };
}

/** The versions held, all told (null for none: JSON has no Infinity) and by store ([min, max]) */
const versions = () => {
  let minV = Infinity;
  let maxV = -Infinity;
  const stores = {};
  for (const c of clients) {
    if (c.v === null) continue;
    if (c.v < minV) minV = c.v;
    if (c.v > maxV) maxV = c.v;
    const held = stores[c.store] ??= [c.v, c.v];
    if (c.v < held[0]) held[0] = c.v;
    if (c.v > held[1]) held[1] = c.v;
  }
  return minV === Infinity ? { minV: null, maxV: null, stores } : { minV, maxV, stores };
};
const pending = () => clients.reduce((a, c) => a + c.pending.size, 0);

/** A live op from a writer, on its own key, stamped as a replica's hybrid logical clock stamps it */
function writeOp(c, i, at, pad) {
  const ms = Math.max(Date.now(), c.ms);
  c.tick = ms === c.ms ? c.tick + 1 : 0;
  c.ms = ms;
  const seq = ++c.seq;
  const sent = clock();
  c.pending.set(seq, sent);
  const ph = window.phase;
  const value = ph === null ? { i, at, sent, pad } : { i, at, sent, ph, pad };
  c.ws.send(JSON.stringify({ t: 'op', store: c.store, op: { replicaId: c.replicaId, seq, ts: [ms, c.tick, c.replicaId], diff: { feed: { [`w-${c.id}`]: value } } } }));
  stats.written[c.store] = (stats.written[c.store] || 0) + 1;
}

/**
 * The process's `writers`, from the store the `first` of them falls on
 * (client j of the process is in store (OFFSET + j) % STORES.length), so the
 * processes' writers between them go round the stores
 */
function writersFrom(writers, first) {
  const n = Math.max(1, Math.min(writers, clients.length));
  const k = STORES.length;
  const start = (((first - OFFSET) % k) + k) % k;
  return Array.from({ length: n }, (_, j) => clients[(start + j) % clients.length]);
}

/**
 * `count` ops at `rate` a second, round robin over the `writers`, each due
 * at start + i/rate; what is overdue goes at once. A writer without an
 * answered socket (reconnecting) skips its turn, counted as skipped: a relay
 * passes on no op of a client it has not answered
 */
async function writing(rate, count, writers, size, first) {
  const pad = padOf(size);
  const own = writersFrom(writers, first);
  const state = { count, done: 0, lagMax: 0, first: null, last: null, finished: null };
  writes = state;
  const start = clock() + 20;
  while (state.done < count) {
    const wait = start + (state.done / rate) * 1000 - clock();
    if (wait > 0) await Bun.sleep(Math.ceil(wait));
    const now = clock();
    for (let at = start + (state.done / rate) * 1000; state.done < count && at <= now; at = start + (state.done / rate) * 1000) {
      state.lagMax = Math.max(state.lagMax, now - at);
      const c = own[state.done % own.length];
      if (c.v !== null && c.ws?.readyState === 1) writeOp(c, state.done, at, pad);
      else stats.skipped++;
      state.first ??= now;
      state.last = now;
      state.done++;
    }
    await Bun.sleep(0);
  }
  state.finished = clock();
}

control({
  '/stats': () => ({ cpu: cpuSeconds(), rss: process.memoryUsage().rss, clients: clients.length, ...stats, members, ...versions(), writes }),
  '/count': () => ({
    answered: stats.answered, received: stats.received, stale: stats.stale, closes: stats.closes, failed: stats.failed,
    firstError: stats.firstError, acked: stats.acked, refused: stats.refused, pending: pending(), written: stats.written,
    members, back: stats.back, writing: writes !== null && writes.finished === null, ...versions()
  }),
  '/reset': url => {
    const phase = url.searchParams.get('phase');
    const from = Number(url.searchParams.get('from'));
    const count = Number(url.searchParams.get('count'));
    window = phase !== null ? { phase: Number(phase), from: -1, to: Infinity } : { phase: null, from, to: from + count };
    Object.assign(stats, fresh());
    return {};
  },
  '/write': url => {
    if (writes && writes.finished === null) return { refused: 'a paced write run is still going' };
    const q = name => Number(url.searchParams.get(name));
    writing(q('rate'), q('count'), q('writers'), q('size') || 0, q('first') || 0).catch(err => console.error('write:', err));
    return { count: q('count') };
  },
  // Every answered client off its socket at once, back 250-500 ms later (see open's onclose)
  '/storm': () => {
    let dropped = 0;
    for (const c of clients) {
      if (!c.answered || c.ws?.readyState !== 1) continue;
      c.since = c.v !== null ? { since: c.v, epoch: c.epoch } : null;
      c.leaving = true;
      c.answered = false;
      stats.answered--;
      c.v = null;
      c.ws.close();
      dropped++;
    }
    return { dropped };
  }
}, { role: 'clients', n: N, admin: ADMIN });

// Connect at most INFLIGHT at a time: a Windows listener turns away connects past a couple of hundred pending
for (let j = 0; j < N; j++) {
  const i = OFFSET + j;
  const id = ADMIN ? `admin${i}` : `c${i}`;
  const store = ADMIN ? STORES[0] : STORES[i % STORES.length];
  const url = new URL(TARGETS[i % TARGETS.length]);
  for (const [key, value] of QUERY) url.searchParams.set(key, value);
  url.searchParams.set(AUTH, id);
  const helloMessage = { t: 'hello', store, replicaId: `r-${RUN}${id}`, ops: [], ...(ADMIN ? {} : { presence: false }) };
  clients.push({
    id, store, replicaId: helloMessage.replicaId, url: url.href, answered: false, v: null, epoch: null,
    helloMessage, hello: JSON.stringify(helloMessage),
    // A writer's: its last seq, its clock (ms and the count within it), and its ops not yet acknowledged (seq -> when sent)
    seq: 0, ms: 0, tick: 0, pending: new Map(),
    // The storm's: dropped on purpose, what the next hello asks for, when it dialled again
    leaving: false, since: null, dialled: null
  });
  members[store] = (members[store] || 0) + 1;
}
let next = 0;
const starter = () => {
  if (next >= clients.length) return;
  const c = clients[next++];
  let done = false;
  c.opened = () => {
    if (done) return;
    done = true;
    starter();
  };
  open(c);
  // Each its own keepalive, spread over the 30 s
  setTimeout(() => setInterval(() => c.ws?.readyState === 1 && c.ws.send('{"t":"ping"}'), 30_000), Math.random() * 30_000);
};
for (let k = 0; k < INFLIGHT; k++) starter();
