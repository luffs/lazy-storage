// clients.js - N thin clients of the store `feed`: raw sockets that say hello,
// take the answer's version, and then only read each patch's version and the
// times it carries, without applying it, so a few processes hold thousands and
// what is measured is the server and the relays, not the clients. A client's
// hello says presence: false unless it is an admin's (ADMIN=1)
//   env: TARGETS (comma-separated ws://host:port/sync), N, OFFSET, ADMIN, INFLIGHT (8)
// Control: /stats, /count (cheap: counts and the versions held),
// /reset?from&count (the phase's window: only patches from+1..from+count count)
import { clock, cpuSeconds, control, newHistogram, record } from './common.js';

const TARGETS = process.env.TARGETS.split(',');
const N = Number(process.env.N);
const OFFSET = Number(process.env.OFFSET || 0);
const ADMIN = process.env.ADMIN === '1';
const INFLIGHT = Number(process.env.INFLIGHT || 8);
const STORE = 'feed';

// `histogram` from when a patch was due, `delivery` from when the server sent it; `stale`: patches outside the window
const fresh = () => ({ received: 0, stale: 0, gaps: 0, retold: 0, errors: 0, presence: 0, histogram: newHistogram(), delivery: newHistogram() });
const stats = { answered: 0, closes: 0, ...fresh() };
let window = { from: -1, to: Infinity };
const clients = [];

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
    if (m.t === 'snapshot' || m.t === 'delta') {
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

control({
  '/stats': () => ({ cpu: cpuSeconds(), rss: process.memoryUsage().rss, clients: clients.length, ...stats, ...versions() }),
  '/count': () => ({ answered: stats.answered, received: stats.received, stale: stats.stale, closes: stats.closes, ...versions() }),
  '/reset': url => {
    const from = Number(url.searchParams.get('from'));
    const count = Number(url.searchParams.get('count'));
    window = { from, to: from + count };
    Object.assign(stats, fresh());
    return {};
  }
}, { role: 'clients', n: N });

// Connect at most INFLIGHT at a time: a Windows listener turns away connects past a couple of hundred pending
for (let j = 0; j < N; j++) {
  const i = OFFSET + j;
  const id = ADMIN ? `admin${i}` : `c${i}`;
  clients.push({ id, target: TARGETS[i % TARGETS.length], answered: false, v: null, hello: JSON.stringify({ t: 'hello', store: STORE, replicaId: `r-${id}`, ops: [], ...(ADMIN ? {} : { presence: false }) }) });
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
