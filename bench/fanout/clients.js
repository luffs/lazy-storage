// clients.js - N thin clients of the store `feed`: raw sockets that say hello,
// take the answer's version, and then only read each patch's version and the
// times it carries, without applying it, so a few processes hold thousands and
// what is measured is the server and the relays, not the clients. A client's
// hello says presence: false unless it is an admin's (ADMIN=1)
//
// Writes: the first `writers` clients of the process each write a key of
// their own (`w-<id>` in the feed), `count` ops between them at `rate` a
// second, round robin, as live ops on their sockets, stamped as a replica
// stamps them. Each op carries the time it was due and the time it went out
// as the server's patches do, so every client hears the patch it makes as
// it hears the server's, and its author times its ack (sent → ack) besides
//   env: TARGETS (comma-separated ws://host:port/sync), N, OFFSET, ADMIN, INFLIGHT (8)
// Control: /stats, /count (cheap: counts and the versions held),
// /reset?from&count (the phase's window: only patches from+1..from+count count),
// /write?rate&count&writers&size (paced; refused while one is going)
import { clock, cpuSeconds, control, newHistogram, record, padOf } from './common.js';

const TARGETS = process.env.TARGETS.split(',');
const N = Number(process.env.N);
const OFFSET = Number(process.env.OFFSET || 0);
const ADMIN = process.env.ADMIN === '1';
const INFLIGHT = Number(process.env.INFLIGHT || 8);
const STORE = 'feed';

// `histogram` from when a patch was due, `delivery` from when the server sent it; `stale`: patches outside the window.
// A writer's: `acks` from when its op went out to its ack, `refused` its ops the server turned away (an error
// carrying the op's seq), `skipped` the ops due while it had no answered socket to send them on
const fresh = () => ({
  received: 0, stale: 0, gaps: 0, retold: 0, errors: 0, presence: 0, histogram: newHistogram(), delivery: newHistogram(),
  acked: 0, refused: 0, skipped: 0, acks: newHistogram()
});
const stats = { answered: 0, closes: 0, ...fresh() };
let window = { from: -1, to: Infinity };
const clients = [];
let writes = null;   // the paced write run: { count, done, lagMax, first, last, finished }

const numberAfter = (text, key, from = 0) => {
  const at = text.indexOf(key, from);
  if (at < 0) return NaN;
  let end = at + key.length;
  while (end < text.length && '0123456789.-e'.includes(text[end])) end++;
  return Number(text.slice(at + key.length, end));
};

function open(c) {
  const ws = new WebSocket(`${c.target}?id=${c.id}`);
  c.ws = ws;
  c.v = null;
  ws.onopen = () => {
    c.opened?.();
    ws.send(c.hello);
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
      if (v <= window.from || v > window.to) {
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
    stats.closes++;
    c.opened?.();
    if (c.answered) {
      c.answered = false;
      stats.answered--;
    }
    c.v = null;
    setTimeout(() => open(c), 1000 + Math.random() * 1000);
  };
}

const versions = () => {
  let minV = Infinity;
  let maxV = -Infinity;
  for (const c of clients) {
    if (c.v === null) continue;
    if (c.v < minV) minV = c.v;
    if (c.v > maxV) maxV = c.v;
  }
  // null for none (JSON has no Infinity)
  return minV === Infinity ? { minV: null, maxV: null } : { minV, maxV };
};

/** A live op from a writer, on its own key, stamped as a replica's hybrid logical clock stamps it */
function writeOp(c, i, at, pad) {
  const ms = Math.max(Date.now(), c.ms);
  c.tick = ms === c.ms ? c.tick + 1 : 0;
  c.ms = ms;
  const seq = ++c.seq;
  const sent = clock();
  c.pending.set(seq, sent);
  c.ws.send(JSON.stringify({ t: 'op', store: STORE, op: { replicaId: c.replicaId, seq, ts: [ms, c.tick, c.replicaId], diff: { feed: { [`w-${c.id}`]: { i, at, sent, pad } } } } }));
}

/**
 * `count` ops at `rate` a second, round robin over the first `writers`
 * clients, each due at start + i/rate; what is overdue goes at once. A
 * writer without an answered socket (reconnecting) skips its turn, counted
 * as skipped: a relay passes on no op of a client it has not answered
 */
async function writing(rate, count, writers, size) {
  const pad = padOf(size);
  const own = clients.slice(0, Math.max(1, Math.min(writers, clients.length)));
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
  '/stats': () => ({ cpu: cpuSeconds(), rss: process.memoryUsage().rss, clients: clients.length, ...stats, ...versions(), writes }),
  '/count': () => ({
    answered: stats.answered, received: stats.received, stale: stats.stale, closes: stats.closes, acked: stats.acked,
    writing: writes !== null && writes.finished === null, ...versions()
  }),
  '/reset': url => {
    const from = Number(url.searchParams.get('from'));
    const count = Number(url.searchParams.get('count'));
    window = { from, to: from + count };
    Object.assign(stats, fresh());
    return {};
  },
  '/write': url => {
    if (writes && writes.finished === null) return { refused: 'a paced write run is still going' };
    const q = name => Number(url.searchParams.get(name));
    writing(q('rate'), q('count'), q('writers'), q('size') || 0).catch(err => console.error('write:', err));
    return { count: q('count') };
  }
}, { role: 'clients', n: N, admin: ADMIN });

// Connect at most INFLIGHT at a time: a Windows listener turns away connects past a couple of hundred pending
for (let j = 0; j < N; j++) {
  const i = OFFSET + j;
  const id = ADMIN ? `admin${i}` : `c${i}`;
  clients.push({
    id, replicaId: `r-${id}`, target: TARGETS[i % TARGETS.length], answered: false, v: null,
    hello: JSON.stringify({ t: 'hello', store: STORE, replicaId: `r-${id}`, ops: [], ...(ADMIN ? {} : { presence: false }) }),
    // A writer's: its last seq, its clock (ms and the count within it), and its ops not yet acknowledged (seq -> when sent)
    seq: 0, ms: 0, tick: 0, pending: new Map()
  });
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
