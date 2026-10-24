// relay/follower.js - A follower: stores served from a copy of the server's, kept by a link of its own
//
// A relay sits between clients and the server. It speaks the server's
// protocol to its clients and keeps, per store, a COPY: the server's
// records with their clocks (its BASE, read on one session of the relay's
// own, `follower: true`) and a QUEUE of ops the server has not acknowledged.
// What its clients see is the base with the queue replayed on top, under
// the same rule the server decides by (core/doc.js), so an op is shown to
// every client the moment the relay has it, and when the server answers,
// what it decided falls into place record by record.
//
// Two ways to acknowledge (`acknowledge`):
//
// - 'upstream' (a site's hub): every client is its own replica. The relay
//   asks the server whether the client may open the store (`vouch`, with
//   the client's credential, judged as its own socket would be), applies
//   the client's ops to the copy at once, and sends each up on the link
//   under the client's name (`as`), judged and acknowledged as the
//   client's. The client's ack is the server's, passed on, so its outbox
//   holds every op until the server has it, and a refusal reaches it.
// - 'local' (a browser's tabs): the clients are one replica, the relay's.
//   A client's op is acknowledged when it is in the relay's queue, which
//   the storage keeps, and goes up as the relay's own.
//
// Time is the server's: the relay learns it over the link, clamps what its
// clients stamp to it, and stamps its own writes (`relay.patch`) with it.
//
// While the server is away the relay goes on answering from its copy:
// hellos from clients the server let in through it before (by `key`, a
// fingerprint of the credential), and their ops, which wait in the
// clients' outboxes, and in the queue, until the server is back. After
// `grace` without the server, it says so (`mode` 'local', and a `relay`
// message to every client: `db.relayed`).
//
// The relay's own answers count in its own versions and epoch, and say
// `relay: true`, so a client that later talks to the server directly asks
// it for a snapshot and hears no reset.
//
// A store no client has open any more is let go of `linger` later (unless
// one comes back meanwhile): its copy saved, its session at the server
// left, so the server may unload it, and the relay holds nothing of it but
// what storage keeps. One with ops of the relay's own the server has not
// acknowledged waits for them first.
import { LazyWatch } from 'lazy-watch';
import { createClock, clamp, isTimestamp } from '../core/clock.js';
import { PROTOCOL } from '../core/protocol.js';
import { createShape, assertChange } from '../core/shape.js';
import { createDoc, docFromRecords, changeFor } from '../core/doc.js';
import { createRules, RuleError } from '../core/rules.js';
import { fingerprintOf } from './fingerprint.js';
import { randomId, isStoreId } from '../core/ids.js';
import { createConnection } from '../client/connection.js';
import { memoryCopies } from './copies.js';
import { tagStore } from '../server/wire.js';

const { Utils } = LazyWatch;
const isObj = Utils.isPlainObject;
const DELTA_LOG = 1000;
/** How long a store the server would not let the relay carry is answered with the server's word before it asks again */
const REASK = 10 * 60_000;
const clone = value => (value === null || value === undefined || typeof value !== 'object' ? value : structuredClone(value));

/**
 * @param {Object} options
 * @param {(storeId: string) => Object|null} options.shapeOf - a store's shape ({ initial, singletons, registers, parents, rules?, readOnly? }), or null for one it does not carry
 * @param {() => Object} options.link - a transport factory to the server, under the relay's own credential
 * @param {Object} [options.storage] - memoryCopies() (default) or fileCopies(dir)
 * @param {'upstream'|'local'} [options.acknowledge='upstream']
 * @param {number} [options.grace=10000] - how long the server may be away before the relay says it answers alone
 * @param {(key: string, storeId: string) => boolean} [options.authorizeOffline] - whether to answer a client the server let in before, while it is away
 * @param {number|false} [options.linger=30000] - how long (ms) a store is kept after its last client left; false keeps every store
 * @param {{min: number, max: number}|false} [options.reconnect]
 * @param {number|false} [options.keepalive]
 * @param {(error: any) => void} [options.onError]
 * @param {() => number} [options.now]
 * @param {string} [options.replicaId] - the relay's own replica (default: the one storage keeps, else new);
 *   it must stay the same across restarts, for the ops it keeps go up under it
 */
export function createRelay({
  shapeOf,
  link,
  storage = memoryCopies(),
  acknowledge = 'upstream',
  grace = 10_000,
  authorizeOffline = () => true,
  linger = 30_000,
  reconnect = { min: 500, max: 10_000 },
  keepalive = 30_000,
  onError = err => console.error('lazy-storage relay:', err),
  now = Date.now,
  replicaId: givenReplica
} = {}) {
  if (typeof shapeOf !== 'function') throw new TypeError('createRelay needs shapeOf(storeId)');
  if (typeof link !== 'function') throw new TypeError('createRelay needs a link: a transport factory to the server');
  if (acknowledge !== 'upstream' && acknowledge !== 'local') throw new TypeError("acknowledge is 'upstream' or 'local'");
  if (linger !== false && !(Number.isFinite(linger) && linger >= 0)) throw new TypeError('linger is a number of ms, or false');
  const local = acknowledge === 'local';
  const meta = storage.loadMeta?.() ?? null;
  const replicaId = givenReplica ?? meta?.replicaId ?? randomId();
  if (meta?.replicaId !== replicaId) storage.saveMeta?.({ replicaId });
  const clock = createClock({ now });
  const connection = createConnection({ transport: link, reconnect, keepalive });
  const edges = new Map();
  const listeners = { mode: new Set(), error: new Set(), sync: new Set(), status: new Set() };
  let mode = 'through';
  let downSince = null;
  let graceTimer = null;
  let forcedDown = false;
  let closed = false;

  function emit(event, payload) {
    for (const fn of listeners[event]) {
      try {
        fn(payload);
      } catch (err) {
        onError(err);
      }
    }
  }
  const fault = err => {
    onError(err);
    emit('error', err);
  };

  // --- Mode ----------------------------------------------------------------------------

  const linkUp = () => !forcedDown && connection.status === 'online';
  function setMode(next) {
    if (mode === next) return;
    mode = next;
    emit('mode', mode);
    for (const edge of edges.values()) for (const ss of edge.sessions) if (ss.synced) ss.send({ t: 'relay', status: mode === 'local' ? 'local' : 'through' });
    emit('status', linkUp() ? 'online' : connection.status);
  }
  function noteLink() {
    if (linkUp()) {
      downSince = null;
      clearTimeout(graceTimer);
      graceTimer = null;
      setMode('through');
      return;
    }
    if (downSince === null) downSince = now();
    if (graceTimer || mode === 'local') return;
    if (!(grace > 0)) return setMode('local');
    graceTimer = setTimeout(() => {
      graceTimer = null;
      if (!linkUp()) setMode('local');
    }, grace);
    if (typeof graceTimer?.unref === 'function') graceTimer.unref();
  }
  connection.on('status', status => {
    noteLink();
    emit('status', status);
  });

  // --- A store's copy --------------------------------------------------------------------

  /**
   * A store's copy, made on first use from what storage keeps. Storage that
   * loads asynchronously (a browser's IndexedDB) has the copy wait for it:
   * meanwhile it is `loading`, and what comes for the store waits too
   */
  function edgeOf(storeId) {
    if (edges.has(storeId)) return edges.get(storeId);
    let declared;
    try {
      declared = shapeOf(storeId);
    } catch (err) {
      fault(err);
      declared = null;
    }
    if (!declared) return null;
    const shape = createShape(declared);
    let saved;
    try {
      saved = storage.load(storeId);
    } catch (err) {
      fault(err);
      saved = null;
    }
    if (saved && typeof saved.then === 'function') {
      const waiting = { id: storeId, loading: true, waiting: [], sessions: new Set() };
      edges.set(storeId, waiting);
      saved.then(copy => copy, err => { fault(err); return null; }).then(copy => {
        if (closed) return;
        // A copy that cannot be built from is reported, and the store starts from the server: its clients would wait on it for ever
        let edge;
        try {
          edge = makeEdge(storeId, declared, shape, copy);
        } catch (err) {
          fault(err);
          edges.delete(storeId);
          edge = makeEdge(storeId, declared, shape, null);
        }
        for (const ss of waiting.sessions) {
          ss.edge = edge;
          edge.sessions.add(ss);
        }
        for (const run of waiting.waiting) run();
        idle(edge);
      });
      return waiting;
    }
    try {
      return makeEdge(storeId, declared, shape, saved);
    } catch (err) {
      fault(err);
      return makeEdge(storeId, declared, shape, null);
    }
  }

  function makeEdge(storeId, declared, shape, saved) {
    const edge = {
      id: storeId,
      shape,
      judge: createRules(shape, declared.rules ?? {}, declared.readOnly === true),
      // Records of their own: what storage handed over is not written in place
      base: Array.isArray(saved?.records) ? docFromRecords(shape, structuredClone(saved.records)) : createDoc(shape),
      view: null,
      have: Boolean(saved?.records),
      v: saved?.v ?? 0,
      epoch: saved?.epoch ?? null,
      fepoch: saved?.fepoch ?? randomId(),
      fv: saved?.fv ?? 0,
      log: [],
      queue: [],
      ownSeq: saved?.own?.seq ?? 0,
      seen: new Map(),
      acked: new Map(),
      admitted: new Map(Object.entries(saved?.admitted ?? {}).map(([key, list]) => [key, new Set(list)])),
      sessions: new Set(),
      grants: new Map(),
      peers: new Map(),
      link: null,
      synced: false,
      retry: null,
      lingering: null
    };
    // An op of another shape (a browser's storage from before 1.0, whose ops were diffs) is let go of, not left to stop the store
    const ops = (saved?.own?.ops ?? []).filter(op => isObj(op) && Number.isInteger(op.seq) && Array.isArray(op.changes));
    for (const op of ops) edge.queue.push({ own: true, replica: replicaId, op, sent: false });
    // The view's records are copies: a change from the server applied to the base must show as a change of the view
    edge.view = docFromRecords(shape, structuredClone([...edge.base.records()]));
    for (const entry of edge.queue) for (const change of entry.op.changes) safeApply(edge.view, change, entry.op.ts);
    edges.set(storeId, edge);
    edge.link = connection.attach(storeId, upstreamHandler(edge));
    connection.connect();
    return edge;
  }

  /** A store with no client: let go of `linger` from now, unless one comes (`inUse`) */
  function idle(edge) {
    if (linger === false || edge.loading || edge.sessions.size || edge.lingering || closed) return;
    edge.lingering = setTimeout(() => {
      edge.lingering = null;
      release(edge);
    }, linger);
    if (typeof edge.lingering?.unref === 'function') edge.lingering.unref();
  }
  function inUse(edge) {
    clearTimeout(edge.lingering);
    edge.lingering = null;
  }

  /** Let go of a store: its copy saved, its session at the server left; one with ops still waiting for the server waits too */
  function release(edge) {
    if (edges.get(edge.id) !== edge || edge.sessions.size || closed) return;
    // Still waiting for the server, or still answering with the server's word (closedWith): kept a while longer
    if (edge.queue.length || (edge.closedWith && now() < edge.closedWith.until)) return idle(edge);
    clearTimeout(edge.retry);
    persist(edge);
    edges.delete(edge.id);
    edge.link?.detach();
  }


  function safeApply(doc, change, ts, mode = 'judge') {
    try {
      return doc.apply(change, ts, mode);
    } catch (err) {
      fault(err);
      return { applied: [], lost: [] };
    }
  }

  function persist(edge) {
    const own = edge.queue.filter(e => e.own).length;
    if (own !== edge.ownPending) {
      edge.ownPending = own;
      emit('sync', { store: edge.id, pending: own });
    }
    // The records are read when storage writes them, not with every change: building them is the costly part,
    // and a storage that writes once a second (fileCopies) reads them once a second. They are the base as it is
    // then, which may be past `v`: a delta from `v` brings it to the same place
    const copy = {
      v: edge.v,
      epoch: edge.epoch,
      fepoch: edge.fepoch,
      fv: edge.fv,
      own: { seq: edge.ownSeq, ops: edge.queue.filter(e => e.own).map(e => e.op) }
    };
    // The records, and who was admitted (an entry per client ever let in), are read when storage writes them too
    const base = edge.base;
    const grants = edge.admitted;
    let records = null;
    let admitted = null;
    Object.defineProperty(copy, 'records', {
      enumerable: true,
      get: () => (records ??= [...base.records()].map(([c, id, data, clockJSON]) => [c, id ?? null, data, clockJSON]))
    });
    Object.defineProperty(copy, 'admitted', {
      enumerable: true,
      get: () => {
        if (!admitted) {
          admitted = {};
          for (const [key, set] of grants) admitted[key] = [...set];
        }
        return admitted;
      }
    });
    try {
      storage.save(edge.id, copy);
    } catch (err) {
      fault(err);
    }
  }

  /** Changes to every client of the store, in the relay's own versions */
  function emitPatch(edge, changes, ts) {
    if (!changes.length) return;
    edge.fv++;
    edge.log.push({ v: edge.fv, ts, changes });
    if (edge.log.length > DELTA_LOG) edge.log.splice(0, edge.log.length - DELTA_LOG);
    const message = { t: 'patch', changes, v: edge.fv, ts };
    for (const ss of edge.sessions) if (ss.synced) ss.send(message);
  }

  /**
   * The view worked out again for these records: their base, with the
   * whole queue replayed (idempotent for what was already in the view), and
   * what changed sent to the clients
   */
  function recompute(edge, records, ts) {
    const touched = new Map();
    const reset = new Map();
    const add = (c, id) => touched.set(edge.shape.recordKey(c, id), [c, id]);
    // A record put back from the base takes its children with it: a queued
    // deletion of the parent took them out of the view, and may have lost
    const putBack = (c, id) => {
      const key = edge.shape.recordKey(c, id);
      if (reset.has(key)) return;
      reset.set(key, [c, id]);
      add(c, id);
      for (const [cc, cid] of edge.base.childrenOf(c, id)) putBack(cc, cid);
    };
    for (const [c, id] of records) putBack(c, id);
    for (const entry of edge.queue) for (const change of entry.op.changes) add(change.c, change.id);
    const before = new Map();
    for (const [key, [c, id]] of touched) before.set(key, JSON.stringify(edge.view.get(c, id)));
    for (const [c, id] of reset.values()) {
      const data = edge.base.get(c, id);
      edge.view.setRecord(c, id, clone(data), data === undefined ? null : edge.base.clock(c, id));
    }
    for (const entry of edge.queue) for (const change of entry.op.changes) safeApply(edge.view, change, entry.op.ts);
    const out = [];
    for (const [key, [c, id]] of touched) {
      const after = edge.view.get(c, id);
      if (JSON.stringify(after) === before.get(key)) continue;
      if (edge.shape.isSingleton(c)) out.push({ c, set: clone(after) });
      else if (after === undefined) out.push({ c, id, del: true });
      else {
        if (before.get(key) !== undefined) out.push({ c, id, del: true });
        out.push({ c, id, put: clone(after) });
      }
    }
    emitPatch(edge, out, ts);
  }

  /** Changes the server decided: into the base, and on to the clients */
  function fromServer(edge, changes, ts) {
    clock.observe(ts);
    const list = Array.isArray(changes) ? changes : [];
    for (const change of list) safeApply(edge.base, change, ts, 'trust');
    if (!edge.queue.length) {
      for (const change of list) safeApply(edge.view, change, ts, 'trust');
      emitPatch(edge, list, ts);
    } else {
      recompute(edge, list.map(ch => [ch.c, ch.id]), ts);
    }
  }

  /**
   * A client's ops sent up under a session of its that has left: once the
   * link drops, nothing can send them again, and the client keeps them in its
   * outbox and sends them when it is back (take hands one over to its new
   * session meanwhile)
   */
  function forgetOrphans(edge) {
    const orphans = edge.queue.filter(e => !e.own && !edge.grants.has(e.as));
    if (!orphans.length) return;
    edge.queue = edge.queue.filter(e => !orphans.includes(e));
    recompute(edge, orphans.flatMap(e => e.op.changes.map(ch => [ch.c, ch.id])), clock.highest);
    idle(edge);
  }

  function sendUp(edge, entry) {
    if (!edge.synced || !linkUp() || entry.sent) return;
    if (entry.own) {
      edge.link.send({ t: 'op', op: entry.op });
      entry.sent = true;
      return;
    }
    const grant = edge.grants.get(entry.as);
    if (grant?.state !== 'admitted') return;
    edge.link.send({ t: 'op', as: entry.as, op: entry.op });
    entry.sent = true;
  }

  // Asked as soon as the link is up, before the store's answer: the server answers a follower only once a client of it is let in
  function vouch(edge, grant) {
    if (!edge.link || !linkUp() || local) return;
    grant.state = 'asked';
    edge.link.send({ t: 'vouch', as: grant.as, credential: grant.credential, replica: grant.replica });
  }

  // --- The link to the server --------------------------------------------------------

  function upstreamHandler(edge) {
    return {
      onOpen(attached) {
        edge.link = attached;
        edge.synced = false;
        const own = edge.queue.filter(e => e.own).map(e => e.op);
        for (const entry of edge.queue) entry.sent = false;
        // A site's relay speaks for clients of its own; a browser's is one replica, listed and sharing as itself
        const hello = { t: 'hello', proto: PROTOCOL, replica: replicaId, ops: own, clocks: true };
        if (!local) hello.follower = true;
        if (edge.have && edge.epoch) Object.assign(hello, { since: edge.v, epoch: edge.epoch });
        attached.send(hello);
        for (const entry of own.length ? edge.queue.filter(e => e.own) : []) entry.sent = true;
        for (const grant of edge.grants.values()) if (grant.state === 'pending') vouch(edge, grant);
      },
      onMessage: msg => {
        try {
          fromUpstream(edge, msg);
        } catch (err) {
          fault(err);
        }
      },
      onClose() {
        edge.synced = false;
        for (const grant of edge.grants.values()) if (grant.state !== 'refused') grant.state = 'pending';
        forgetOrphans(edge);
        for (const entry of edge.queue) entry.sent = false;
        edge.peers = new Map();
        noteLink();
      },
      onClosed() {}
    };
  }

  function caughtUp(edge, msg, applyToBase) {
    clock.adopt(msg.now);
    const previous = edge.epoch;
    if (Number.isInteger(msg.seq)) edge.queue = edge.queue.filter(e => !(e.own && e.op.seq <= msg.seq));
    const records = applyToBase();
    edge.v = Number.isInteger(msg.v) ? msg.v : edge.v;
    edge.epoch = typeof msg.epoch === 'string' ? msg.epoch : null;
    edge.have = true;
    edge.synced = true;
    if (previous !== null && edge.epoch !== previous) {
      // The server's storage started over (a backup put back): the clients hear it as from the server
      edge.fepoch = randomId();
      for (const ss of edge.sessions) ss.send({ t: 'reset', previous: { epoch: previous } });
    }
    recompute(edge, records, clock.highest);
    persist(edge);
    for (const p of Array.isArray(msg.lost) ? msg.lost : []) if (isObj(p) && p.lost?.length) fault(new Error(`The server took only part of the relay's op ${p.seq}`));
    noteLink();
    // Who was waiting, and what was waiting to go up
    for (const grant of edge.grants.values()) if (grant.state === 'pending') vouch(edge, grant);
    for (const entry of edge.queue) sendUp(edge, entry);
    for (const ss of edge.sessions) if (!ss.synced && ss.hello) tryHello(edge, ss);
  }

  function fromUpstream(edge, msg) {
    switch (msg.t) {
      case 'snapshot': {
        if (msg.state === undefined) return fault(new Error('A relay needs snapshots inline'));
        return caughtUp(edge, msg, () => {
          const records = [];
          const clocks = new Map((Array.isArray(msg.clocks) ? msg.clocks : []).map(([c, id, clockJSON]) => [edge.shape.recordKey(c, id ?? undefined), clockJSON]));
          const touched = new Set();
          for (const [c, id] of edge.base.records()) touched.add(JSON.stringify([c, id ?? null]));
          for (const c of edge.shape.collections) {
            for (const id of Object.keys(msg.state?.[c] ?? {})) {
              records.push([c, id, msg.state[c][id], clocks.get(edge.shape.recordKey(c, id))]);
              touched.add(JSON.stringify([c, id]));
            }
          }
          for (const c of edge.shape.singletons) if (msg.state && Object.hasOwn(msg.state, c)) records.push([c, null, msg.state[c], clocks.get(edge.shape.recordKey(c))]);
          edge.base = docFromRecords(edge.shape, records);
          return [...touched].map(k => JSON.parse(k)).map(([c, id]) => [c, id ?? undefined]);
        });
      }
      case 'delta':
        return caughtUp(edge, msg, () => {
          const records = [];
          for (const entry of Array.isArray(msg.ops) ? msg.ops : []) {
            clock.observe(entry.ts);
            for (const change of entry.changes ?? []) {
              safeApply(edge.base, change, entry.ts, 'trust');
              records.push([change.c, change.id]);
            }
          }
          return records;
        });
      case 'patch':
        if (!edge.synced) return;
        if (Number.isInteger(msg.v) && msg.v > edge.v + 1) {
          // A patch never arrived: catch up
          edge.synced = false;
          upstreamHandler(edge).onOpen(edge.link);
          return;
        }
        if (Number.isInteger(msg.v)) edge.v = msg.v;
        fromServer(edge, msg.changes, msg.ts);
        persist(edge);
        return;
      case 'ack': {
        clock.adopt(msg.now);
        if (Number.isInteger(msg.v) && msg.v > edge.v) edge.v = msg.v;
        if (msg.as !== undefined) {
          // A client's op: acknowledged to every session of its replica, a reconnected one included
          const replica = msg.replica ?? edge.grants.get(msg.as)?.replica;
          if (typeof replica !== 'string') return;
          edge.acked.set(replica, Math.max(edge.acked.get(replica) ?? 0, msg.seq));
          const ack = { t: 'ack', seq: msg.seq, v: edge.fv, now: clock.time };
          if (Array.isArray(msg.lost) && msg.lost.length) ack.lost = msg.lost;
          for (const ss of edge.sessions) if (ss.replica === replica) ss.send(ack);
          const taken = takeClientEntry(edge, replica, msg.seq);
          if (taken) recompute(edge, taken.op.changes.map(ch => [ch.c, ch.id]), taken.op.ts);
          persist(edge);
          return;
        }
        const entry = takeEntry(edge, undefined, msg.seq);
        if (!entry) return;
        if (local && entry.from) {
          // The browser's replica: what lost is told to every tab
          if (Array.isArray(msg.lost) && msg.lost.length) for (const ss of edge.sessions) if (ss.synced) ss.send({ t: 'lost', seq: entry.from.seq, lost: msg.lost });
        }
        recompute(edge, entry.op.changes.map(ch => [ch.c, ch.id]), entry.op.ts);
        persist(edge);
        return;
      }
      case 'error': {
        if (!Number.isInteger(msg.seq)) {
          fault(new Error(`The server${msg.as === undefined ? '' : ` (about ${msg.as})`}: ${msg.message}`));
          return;
        }
        if (msg.code === 'rate-limited') {
          for (const entry of edge.queue) if (entry.as === msg.as) entry.sent = false;
          clearTimeout(edge.retry);
          edge.retry = setTimeout(() => { for (const entry of edge.queue) sendUp(edge, entry); }, Number.isInteger(msg.retryAfter) ? msg.retryAfter : 1000);
          if (typeof edge.retry?.unref === 'function') edge.retry.unref();
          return;
        }
        if (msg.as !== undefined) {
          const replica = msg.replica ?? edge.grants.get(msg.as)?.replica;
          if (typeof replica !== 'string') return;
          const taken = takeClientEntry(edge, replica, msg.seq);
          if (taken) recompute(edge, taken.op.changes.map(ch => [ch.c, ch.id]), taken.op.ts);
          for (const ss of edge.sessions) if (ss.replica === replica) ss.send({ t: 'error', seq: msg.seq, code: msg.code, message: msg.message });
          persist(edge);
          return;
        }
        const entry = takeEntry(edge, undefined, msg.seq);
        if (!entry) return;
        recompute(edge, entry.op.changes.map(ch => [ch.c, ch.id]), entry.op.ts);
        if (local) {
          for (const ss of edge.sessions) if (ss.synced) ss.send({ t: 'rejected', seq: entry.from?.seq ?? null, code: msg.code, message: msg.message, changes: entry.op.changes });
        } else {
          fault(new Error(`The server refused the relay's own op: ${msg.message}`));
        }
        persist(edge);
        return;
      }
      case 'presence':
        applyPresence(edge, msg);
        for (const ss of edge.sessions) if (ss.synced && ss.presence) ss.send(msg);
        return;
      case 'admitted': {
        const grant = edge.grants.get(msg.as);
        if (!grant) return;
        grant.state = 'admitted';
        grant.user = msg.user;
        if (!edge.admitted.has(grant.key)) edge.admitted.set(grant.key, new Set());
        edge.admitted.get(grant.key).add(grant.replica);
        persist(edge);
        for (const ss of sessionsOf(edge, msg.as)) if (!ss.synced) tryHello(edge, ss);
        for (const entry of edge.queue) if (entry.as === msg.as) sendUp(edge, entry);
        return;
      }
      case 'refused':
      case 'revoked': {
        const grant = edge.grants.get(msg.as);
        if (!grant) return;
        if (msg.t === 'refused' && msg.code === 'rate-limited') {
          // Not a verdict: asked again once the server says it may be, the client waiting meanwhile
          grant.state = 'pending';
          const timer = setTimeout(() => { if (edge.grants.get(msg.as) === grant && grant.state === 'pending') vouch(edge, grant); }, Number.isInteger(msg.retryAfter) ? msg.retryAfter : 1000);
          if (typeof timer?.unref === 'function') timer.unref();
          return;
        }
        grant.state = 'refused';
        edge.admitted.get(grant.key)?.delete(grant.replica);
        edge.queue = edge.queue.filter(e => e.as !== msg.as);
        persist(edge);
        const code = msg.t === 'revoked' ? (msg.code ?? 'evicted') : (msg.code ?? 'forbidden');
        for (const ss of sessionsOf(edge, msg.as)) {
          // Taken back to be judged afresh: the client's socket closes, and it comes back with what it has by then
          if (code === 'reauthenticate') ss.evictSocket();
          else ss.end(code, msg.message ?? 'Not allowed to open this store');
        }
        return;
      }
      case 'closed': {
        edge.synced = false;
        if (msg.code === 'unavailable') {
          // The server's session is gone with every grant on it: they are asked for again on the next one, and what was sent goes again
          for (const grant of edge.grants.values()) if (grant.state !== 'refused') grant.state = 'pending';
          forgetOrphans(edge);
          for (const entry of edge.queue) entry.sent = false;
          edge.peers = new Map();
          setTimeout(() => { if (edge.link && connection.status === 'online') upstreamHandler(edge).onOpen(edge.link); }, 250 + Math.random() * 750);
          return;
        }
        // The server will not let the relay carry the store (or ended its session): what it said goes to every client
        // there, and to every one that opens the store for a while; then the relay asks again
        edge.closedWith = { code: msg.code, message: msg.message, until: now() + REASK };
        for (const ss of [...edge.sessions]) ss.end(msg.code, msg.message);
        return;
      }
      default:
    }
  }

  function takeEntry(edge, as, seq) {
    const i = edge.queue.findIndex(e => (as === undefined ? e.own : e.as === as) && e.op.seq === seq);
    if (i < 0) return null;
    return edge.queue.splice(i, 1)[0];
  }

  /** A client's op in the queue, by its replica and seq, taken out */
  function takeClientEntry(edge, replica, seq) {
    const i = edge.queue.findIndex(e => !e.own && e.replica === replica && e.op.seq === seq);
    return i < 0 ? null : edge.queue.splice(i, 1)[0];
  }

  const sessionsOf = (edge, as) => [...edge.sessions].filter(ss => ss.as === as);

  function applyPresence(edge, msg) {
    if (Array.isArray(msg.peers)) {
      edge.peers = new Map();
      for (const peer of msg.peers) if (isObj(peer)) edge.peers.set(peer.replicaId, peer);
      return;
    }
    for (const id of msg.left ?? []) edge.peers.delete(id);
    for (const peer of msg.joined ?? []) if (isObj(peer)) edge.peers.set(peer.replicaId, peer);
    for (const share of msg.shared ?? []) {
      const peer = edge.peers.get(share?.replicaId);
      if (peer) edge.peers.set(peer.replicaId, share.data === undefined ? (({ data: _d, ...rest }) => rest)(peer) : { ...peer, data: share.data });
    }
  }

  // --- The clients -------------------------------------------------------------------

  /** Whether the client may be answered now: the server let it in, or (away) let it in before */
  function admitted(edge, ss) {
    if (local) return true;
    const grant = edge.grants.get(ss.as);
    if (grant?.state === 'admitted') return true;
    if (grant?.state === 'refused') return false;
    if (linkUp() && edge.synced) return false;
    return Boolean(edge.admitted.get(ss.key)?.has(ss.replica)) && authorizeOffline(ss.key, edge.id) !== false;
  }

  function tryHello(edge, ss) {
    const msg = ss.hello;
    if (!msg || ss.synced) return;
    if (!local) {
      let grant = edge.grants.get(ss.as);
      if (!grant) {
        grant = { as: ss.as, key: ss.key, credential: ss.credential, replica: ss.replica, state: 'pending', user: undefined };
        edge.grants.set(ss.as, grant);
      }
      if (grant.state === 'pending') vouch(edge, grant);
    }
    if (!edge.have || !admitted(edge, ss)) return;
    ss.hello = null;
    const lost = [];
    for (const op of Array.isArray(msg.ops) ? msg.ops : []) take(edge, ss, op, { answer: false });
    const seq = local ? (edge.seen.get(ss.replica) ?? 0) : (edge.acked.get(ss.replica) ?? 0);
    const answer = { seq, v: edge.fv, epoch: edge.fepoch, now: clock.time, shape: edge.shape.hash, relay: true };
    if (lost.length) answer.lost = lost;
    const since = msg.epoch === edge.fepoch && Number.isInteger(msg.since) ? msg.since : null;
    const start = since === null ? -1 : since === edge.fv ? edge.log.length : edge.log.findIndex(e => e.v === since + 1);
    if (since !== null && start >= 0) ss.send({ t: 'delta', ops: edge.log.slice(start), ...answer });
    else ss.send({ t: 'snapshot', state: clone(edge.view.state), ...answer });
    ss.synced = true;
    if (mode === 'local') ss.send({ t: 'relay', status: 'local' });
    if (ss.presence && edge.peers.size) ss.send({ t: 'presence', peers: [...edge.peers.values()] });
    if (!local) for (const entry of edge.queue) if (entry.as === ss.as) sendUp(edge, entry);
  }

  /** A client's op: judged as the server would where it can, applied to the view at once, queued, sent up */
  function take(edge, ss, op, { answer = true } = {}) {
    const refuse = (code, message) => ss.send({ t: 'error', seq: isObj(op) ? op.seq : undefined, code, message });
    if (!isObj(op) || typeof op.replica !== 'string' || !Number.isInteger(op.seq) || op.seq < 1 || !isTimestamp(op.ts) || !Array.isArray(op.changes)) return refuse('invalid', 'A malformed op');
    if (op.replica !== ss.replica) return refuse('forbidden', `This session speaks for replica "${ss.replica}"`);
    const queued = local ? null : edge.queue.find(e => !e.own && e.replica === op.replica && e.op.seq === op.seq);
    if (queued && queued.as !== ss.as && !edge.grants.has(queued.as)) {
      // Sent under a session of the client's that has left: this one takes it over, to send again if the server never had it
      queued.as = ss.as;
      queued.key = ss.key;
      sendUp(edge, queued);
      return;
    }
    if (local ? (edge.seen.get(op.replica) ?? 0) >= op.seq : (edge.acked.get(op.replica) ?? 0) >= op.seq || queued) {
      if (local && answer) ss.send({ t: 'ack', seq: op.seq, v: edge.fv, now: clock.time });
      return;
    }
    const ts = clamp(op.ts, clock.time);
    const applied = [];
    try {
      for (const change of op.changes) assertChange(edge.shape, change);
      edge.judge(edge.view, op.changes, edge.grants.get(ss.as)?.user, op.replica, { relay: true }, () => {
        for (const change of op.changes) for (const a of safeApply(edge.view, change, ts).applied) applied.push(a);
      });
    } catch (err) {
      return refuse(err instanceof RuleError ? 'forbidden' : 'invalid', err.message);
    }
    clock.observe(ts);
    edge.seen.set(op.replica, op.seq);
    const entry = local
      ? { own: true, replica: replicaId, op: { replica: replicaId, seq: ++edge.ownSeq, ts, changes: op.changes }, from: { replica: op.replica, seq: op.seq }, sent: false }
      : { own: false, as: ss.as, key: ss.key, replica: op.replica, op: { ...op, ts }, sent: false };
    edge.queue.push(entry);
    emitPatch(edge, applied, ts);
    if (local) {
      persist(edge);
      if (answer) ss.send({ t: 'ack', seq: op.seq, v: edge.fv, now: clock.time });
    }
    sendUp(edge, entry);
  }

  /**
   * A client's socket. `credential` is what the server judges it by (the
   * headers and query its own socket would carry; the relay never reads
   * it), `key` a fingerprint of it, for answering it while the server is
   * away (default: the credential's SHA-256, fingerprint.js). Returns { receive(message), close() }
   */
  function session({ send, credential, key, user, onEvict } = {}) {
    if (typeof send !== 'function') throw new TypeError('A relay session needs a send function');
    credential = credential ?? user;
    // What the relay keeps on disk and shows: a fingerprint, never the credential itself
    key = key ?? fingerprintOf(credential);
    const stores = new Map();
    let gone = false;

    function storeSession(id, edge) {
      const ss = {
        id,
        edge,
        as: randomId(),
        key,
        credential,
        replica: null,
        presence: true,
        synced: false,
        hello: null,
        // Tagged as the hub tags: the message encoded once, however many sessions hear it, the store id spliced in
        send: message => { if (!gone) send(tagStore(message, id)); },
        evictSocket: () => socket.evict(),
        end(code, message) {
          leave(id);
          ss.send({ t: 'closed', code, message });
        }
      };
      return ss;
    }

    function leave(id) {
      const ss = stores.get(id);
      if (!ss) return;
      stores.delete(id);
      ss.edge.sessions.delete(ss);
      const grant = local || ss.edge.loading ? null : ss.edge.grants.get(ss.as);
      if (grant) {
        ss.edge.grants.delete(ss.as);
        if (grant.state === 'admitted' && ss.edge.synced && linkUp()) ss.edge.link.send({ t: 'leave', as: ss.as });
        // What it had queued and the server has not taken: the client keeps it in its outbox and sends it again
        const mine = ss.edge.queue.filter(e => e.as === ss.as && !e.sent);
        if (mine.length) {
          ss.edge.queue = ss.edge.queue.filter(e => !mine.includes(e));
          recompute(ss.edge, mine.flatMap(e => e.op.changes.map(ch => [ch.c, ch.id])), clock.highest);
        }
      }
      idle(ss.edge);
    }

    const socket = {
      receive(msg) {
        if (gone || !isObj(msg)) return;
        if (msg.t === 'ping') return send({ t: 'pong' });
        const id = msg.store;
        if (!isStoreId(id)) return send({ t: 'closed', store: typeof id === 'string' ? id : undefined, code: 'invalid-store', message: 'A message needs a valid store id' });
        if (msg.t === 'leave') return leave(id);
        let ss = stores.get(id);
        if (!ss) {
          const edge = edgeOf(id);
          if (!edge) return send({ t: 'closed', store: id, code: 'unknown-store', message: `This relay does not carry store "${id}"` });
          ss = storeSession(id, edge);
          stores.set(id, ss);
          edge.sessions.add(ss);
          if (!edge.loading) inUse(edge);
        }
        const edge = ss.edge;
        // Its copy is still being read from storage: this waits for it, in order
        if (edge.loading) return void edge.waiting.push(() => socket.receive(msg));
        switch (msg.t) {
          case 'hello': {
            if (edge.closedWith) {
              if (now() < edge.closedWith.until) return ss.end(edge.closedWith.code, edge.closedWith.message);
              edge.closedWith = null;
              if (edge.link && connection.status === 'online') upstreamHandler(edge).onOpen(edge.link);
            }
            if (msg.proto !== PROTOCOL) return ss.end('outdated', `This relay speaks protocol ${PROTOCOL}; the client speaks ${msg.proto ?? 'an older one'}`);
            if (typeof msg.replica !== 'string' || !msg.replica) return ss.send({ t: 'error', message: 'hello requires a replica' });
            if (ss.replica && ss.replica !== msg.replica) return ss.send({ t: 'error', code: 'forbidden', message: `This session speaks for replica "${ss.replica}"` });
            ss.replica = msg.replica;
            ss.presence = msg.presence !== false;
            ss.synced = false;
            ss.hello = msg;
            if (msg.share !== undefined) ss.share = msg.share;
            tryHello(edge, ss);
            return;
          }
          case 'op':
            if (!ss.synced) return ss.send({ t: 'error', seq: msg.op?.seq, code: 'rate-limited', message: 'Say hello first', retryAfter: 500 });
            return take(edge, ss, msg.op);
          case 'share':
            if (!linkUp() || !edge.synced) return;
            if (local) edge.link.send({ t: 'share', data: msg.data });
            else if (edge.grants.get(ss.as)?.state === 'admitted') edge.link.send({ t: 'share', as: ss.as, data: msg.data });
            return;
          default:
            return ss.send({ t: 'error', message: `Unknown message type: ${msg.t}` });
        }
      },
      close() {
        if (gone) return;
        for (const id of [...stores.keys()]) leave(id);
        gone = true;
      },
      /** End this socket, as the server's eviction would */
      evict() {
        socket.close();
        onEvict?.();
      }
    };
    return socket;
  }

  /** The relay's own write to a store: a diff in the tree's shape, as an op of its own replica */
  function patch(storeId, diff) {
    const edge = edgeOf(storeId);
    if (!edge) throw new TypeError(`This relay does not carry store "${storeId}"`);
    if (!isObj(diff)) throw new TypeError('relay.patch takes a diff in the tree\'s shape');
    const changes = [];
    for (const c of Object.keys(diff)) {
      if (!edge.shape.has(c)) throw new TypeError(`"${c}" is not a collection or singleton of store "${storeId}"`);
      if (edge.shape.isSingleton(c)) changes.push(changeFor({ c }, diff[c]));
      else for (const id of Object.keys(diff[c] ?? {})) changes.push(changeFor({ c, id }, diff[c][id]));
    }
    for (const change of changes) assertChange(edge.shape, change);
    const ts = clock.stamp();
    const entry = { own: true, replica: replicaId, op: { replica: replicaId, seq: ++edge.ownSeq, ts, changes }, sent: false };
    edge.queue.push(entry);
    const applied = [];
    for (const change of changes) applied.push(...safeApply(edge.view, change, ts).applied);
    emitPatch(edge, applied, ts);
    persist(edge);
    sendUp(edge, entry);
    idle(edge);
    return applied;
  }

  return {
    replicaId,
    session,
    patch,
    /** What the relay shows of a store: its copy with the queue on top (plain; read only) */
    copy: storeId => edges.get(storeId)?.view?.state,
    /** The relay's own ops on a store the server has not acknowledged: in 'local' mode, every client's */
    pending: storeId => edges.get(storeId)?.queue?.filter(e => e.own).length ?? 0,
    /** 'through' while the server answers, 'local' once it has been away for `grace` */
    get mode() { return mode; },
    /** The link to the server: 'offline' | 'connecting' | 'online' */
    get status() { return connection.status; },
    on(event, fn) {
      if (!listeners[event]) throw new TypeError(`Unknown relay event "${event}"`);
      listeners[event].add(fn);
      return () => listeners[event].delete(fn);
    },
    /** The host's word that the server is away, or back */
    upstreamDown() {
      forcedDown = true;
      clearTimeout(graceTimer);
      graceTimer = null;
      setMode('local');
    },
    upstreamUp() {
      const held = forcedDown && connection.status === 'online';
      forcedDown = false;
      connection.connect();
      noteLink();
      // The link stayed up while the host held it down: nothing reopens it, so each store says hello again,
      // and what waited meanwhile (its queue, its clients to vouch for) goes up once the server has answered
      if (held) for (const edge of edges.values()) if (!edge.loading && edge.link) upstreamHandler(edge).onOpen(edge.link);
    },
    stats() {
      const stores = {};
      for (const [id, edge] of edges) {
        if (edge.loading) continue;
        stores[id] = {
          sessions: edge.sessions.size,
          queue: edge.queue.length,
          v: edge.v,
          synced: edge.synced,
          // What waits to go up, and why: its replica and seq, whether it went, and how its client stands with the server
          waiting: edge.queue.map(e => ({ replica: e.replica, seq: e.op.seq, sent: e.sent, ...(e.own ? { own: true } : { grant: edge.grants.get(e.as)?.state ?? 'gone' }) }))
        };
      }
      return { mode, link: connection.status, stores };
    },
    close() {
      if (closed) return;
      closed = true;
      clearTimeout(graceTimer);
      for (const edge of edges.values()) {
        if (edge.loading) continue;
        clearTimeout(edge.retry);
        clearTimeout(edge.lingering);
        persist(edge);
      }
      storage.flush?.();
      connection.close();
    }
  };
}
