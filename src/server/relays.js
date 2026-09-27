// relays.js - A relay's link: many clients' reads on one socket, each client judged by the server
//
// A relay on a LAN (src/relay) that carries many clients reads each store
// once, on a session of its own, and passes the store on to every client
// behind it. Who may read what is still the server's to say, one client
// at a time: the relay asks (`vouch`) with the client's credential, and the
// server judges it as it would the client's own socket, with the same
// `authenticate`, `expiresAt`, `authorizeId` and `authorize`. A client let
// in is a peer of the store (store.peer): listed in presence as itself,
// its replica claimed, sent nothing. Its edits never pass this link: the
// relay sends them up on a socket dialled with the client's own
// credential, as a write-only session (see store.js), so the relay reads
// what its clients may read and writes nothing at all.
//
// The relay's own session on a store is let in only while some client
// behind it is (a relay reads no store for nobody), and `authorizeRelay`
// may narrow that further. When the last client of a store goes, the
// relay's session there is closed `linger` later (code 'unused', which the
// relay takes for "say hello again once a client is let in") unless
// another comes; at once when the last one's access was taken away.
//
// The server takes access away the ways it does from a socket: the
// adapter's `disconnect(filter)` and a client's expiry revoke with
// 'reauthenticate' (the relay closes the client's socket with 4001, and it
// comes back with whatever credential it has by then), `revalidate` with
// 'forbidden', and a store's own eviction (closeSessions) with 'evicted'. A
// store unloaded under its clients (a restore, a failed commit) revokes
// them with 'unavailable': the relay tells each to say hello again, and each
// is judged afresh on the store as it is loaded now. The relay itself is
// judged again by `revalidate` (`authorizeRelay` for each store it
// carries) and cut off by the adapter's `disconnect`.
//
// Protocol, besides the store-tagged messages of the relay's own sessions
// (hello, leave, and what the store sends them, as on a hub):
//   relay -> server
//     { t: 'vouch', grant, socket?, store, replicaId, credential }
//         may this client read this store? `grant` is the relay's name for
//         the client on the store, `socket` its name for the client's
//         socket (for sockets()); `credential` is { headers?, query? }, what
//         the client's own socket would have been judged by
//     { t: 'unvouch', grant }                     the client left
//     { t: 'share', grant, data }                 what the client shares
//     { t: 'ping' }
//   server -> relay
//     { t: 'admitted', grant, store, peer, presence, until } `peer`: how
//         presence shows the client; `presence`: whether the store has
//         presence on (a relay's own session hears it only while a client
//         behind it wants it: its hellos say so, either way); `until`: when
//         its session runs out, or null
//     { t: 'refused', grant, store, code, message, retryAfter? }
//     { t: 'revoked', grants, code, message }
//     { t: 'error', grant, store, code, message, retryAfter? }   a share refused
//     { t: 'pong' }
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';
import { tagStore, runAt } from './wire.js';

const { Utils } = LazyWatch;

/** A grant id or socket name the relay makes: a short string */
const isName = value => typeof value === 'string' && value.length > 0 && value.length <= 200;

/** Headers of a vouch's credential a request is made with, besides the adapter's `headers`: what says who a client is, and what it runs */
const CREDENTIAL_HEADERS = ['authorization', 'cookie', 'user-agent'];

/**
 * @param {(id: string) => Object|null} resolveStore
 * @param {Object} options
 * @param {(message: Object) => void} options.send
 * @param {any} options.relay - the relay, as the adapter's `relays.authenticate` gave it
 * @param {(credential: Object, relay: any) => { user: any, expires: number|null }|null|Promise<{ user: any, expires: number|null }|null>} options.admit
 *   who a credential is, as the server's own socket would have it (the
 *   adapter runs `authenticate` and `expiresAt` over a request made of it);
 *   null turns it away
 * @param {(relay: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeRelay]
 * @param {(user: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeId]
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 * @param {Object} [options.channel] - as the hub's
 * @param {number} [options.linger=30000] - how long (ms) the relay's session
 *   on a store outlives its last client
 * @param {{ burst: number, perSecond: number }|false} [options.rate] - vouches
 *   turned away as unauthorized the relay may send before every vouch is
 *   refused for a while (a flood of made-up credentials costs the server an
 *   `authenticate` each); vouches that pass cost their user's bucket instead
 * @param {(error: any) => void} [options.onError]
 */
export function createRelayLink(resolveStore, {
  send,
  relay,
  admit,
  authorizeRelay,
  authorizeId,
  authorize,
  channel,
  linger = 30_000,
  rate = { burst: 100, perSecond: 10 },
  onError = err => console.error('lazy-storage:', err)
} = {}) {
  if (typeof send !== 'function') throw new TypeError('A relay link needs a send function');
  if (typeof admit !== 'function') throw new TypeError('A relay link needs admit(credential)');
  const shared = new Map();    // store id -> { session, store }: the relay's own session on it
  const pending = new Map();   // store id -> messages queued while the relay's session is being let in
  const grants = new Map();    // grant id -> { id, socket, storeId, replicaId, user, store, peer, expires, cancelExpiry, openedAt }
  const vouching = new Map();  // grant id -> { msg }: the vouch being judged (a later one, or an unvouch, disowns it)
  const lingering = new Map(); // store id -> the timer that closes the relay's session there
  const bucket = rate ? { tokens: rate.burst, at: Date.now() } : null;
  let closed = false;

  const refuseStore = (id, code, message) => send({ t: 'closed', store: id, code, message });

  /** The bucket of vouches turned away, refilled; 0 when a vouch may be judged, else the ms until one may */
  function waitForBucket() {
    if (!bucket) return 0;
    const wall = Date.now();
    bucket.tokens = Math.min(rate.burst, bucket.tokens + ((wall - bucket.at) / 1000) * rate.perSecond);
    bucket.at = wall;
    return bucket.tokens >= 1 ? 0 : Math.ceil(((1 - bucket.tokens) / rate.perSecond) * 1000);
  }

  // --- The relay's own sessions --------------------------------------------------------------------

  const grantsOn = storeId => [...grants.values()].filter(g => g.storeId === storeId);

  /** Stop hearing a store: the session gone, the topic left, no linger pending */
  function dropShared(id) {
    shared.delete(id);
    channel?.unsubscribe(id);
    clearTimeout(lingering.get(id));
    lingering.delete(id);
  }

  function closeShared(id, code, message) {
    const entry = shared.get(id);
    const waiting = pending.delete(id);
    if (entry) {
      entry.session.close();
      dropShared(id);
    }
    if (entry || waiting) refuseStore(id, code, message);
  }

  /** The store's last client went: the relay's session there goes too, `linger` later (at once when `now`) unless another comes */
  function lastGone(storeId, now = false) {
    if (grantsOn(storeId).length || (!shared.has(storeId) && !pending.has(storeId))) return;
    clearTimeout(lingering.get(storeId));
    lingering.delete(storeId);
    if (now || !(linger > 0)) return closeShared(storeId, 'unused', 'No client behind this relay reads the store');
    const timer = setTimeout(() => {
      lingering.delete(storeId);
      if (!closed && !grantsOn(storeId).length) closeShared(storeId, 'unused', 'No client behind this relay reads the store');
    }, linger);
    if (typeof timer.unref === 'function') timer.unref();
    lingering.set(storeId, timer);
  }

  /** Run a check that may answer through a promise, then `next` with its verdict (a throw is a no, its message the reason) */
  function step(run, next, deny, current) {
    let verdict;
    try {
      verdict = run();
    } catch (err) {
      return deny(err?.message);
    }
    if (!verdict || typeof verdict.then !== 'function') return next(verdict);
    verdict.then(v => { if (current()) next(v); }, err => { if (current()) deny(err?.message); });
  }

  /** A store the adapter's resolver gives, or the refusal to send: [store] or [null, code, message] */
  function resolve(id) {
    let store;
    try {
      store = resolveStore(id);
    } catch (err) {
      // Served by another process for now (a deploy's overlap): not a fault, and not final
      if (err?.code === 'store-locked') return [null, 'unavailable', err.message];
      onError(err);
      return [null, 'unknown-store', `Store "${id}" could not be opened: ${err?.message ?? err}`];
    }
    return store ? [store] : [null, 'unknown-store', `Unknown store "${id}"`];
  }

  /** The relay says hello to a store: let in while a client of it is, on the store its clients are peers of, and the relay may carry it */
  function openShared(id, queued) {
    pending.set(id, queued);
    const current = () => !closed && pending.get(id) === queued;
    const unused = () => {
      pending.delete(id);
      refuseStore(id, 'unused', 'No client behind this relay reads the store');
    };
    const deny = message => {
      pending.delete(id);
      refuseStore(id, 'forbidden', message || `This relay may not carry store "${id}"`);
    };
    if (!grantsOn(id).length) return unused();
    step(() => (authorizeRelay ? authorizeRelay(relay, id) : true), allowed => {
      if (!allowed) return deny();
      // The store its clients were judged on: one reloaded since revoked them (see evicted)
      const store = grantsOn(id)[0]?.store;
      if (!store || store.disposed) return unused();
      pending.delete(id);
      channel?.subscribe(id);
      const session = store.session({
        send: m => send(tagStore(m, id)),
        user: relay,
        relay: true,
        onEvict: () => dropShared(id),
        broadcast: channel ? m => channel.publish(id, tagStore(m, id)) : undefined
      });
      shared.set(id, { session, store });
      for (const m of queued) session.receive(m);
    }, deny, current);
  }

  function toShared(msg) {
    const id = msg.store;
    if (!isStoreId(id)) return refuseStore(typeof id === 'string' ? id : undefined, 'invalid-store', 'A message on a relay link needs a valid store id');
    const { store: _store, ...inner } = msg;
    if (msg.t === 'leave') {
      const entry = shared.get(id);
      entry?.session.close();
      if (entry) dropShared(id);
      pending.delete(id);
      return;
    }
    if (shared.has(id)) return shared.get(id).session.receive(inner);
    if (pending.has(id)) return void pending.get(id).push(inner);
    if (inner.t !== 'hello') return refuseStore(id, 'unused', 'Say hello to the store first');
    openShared(id, [inner]);
  }

  // --- Clients ----------------------------------------------------------------------------------------

  /** The store closed a client's peer: evicted, or unloaded under it (judged afresh when it says hello again) */
  function evicted(grant, code) {
    if (grants.get(grant.id) !== grant) return;
    grant.peer = null;   // closed already
    revoke([grant.id], code === 'unavailable' ? 'unavailable' : code || 'evicted',
      code === 'unavailable' ? 'The store was unloaded; say hello again' : 'Your session was closed by the server');
  }

  function forget(grant) {
    grants.delete(grant.id);
    grant.cancelExpiry?.();
    grant.peer?.close();
  }

  /** End these clients' access (see the header); the relay is told once */
  function revoke(ids, code, message = 'Your access to the store was taken away') {
    const gone = [];
    const stores = new Set();
    for (const id of ids) {
      const grant = grants.get(id);
      if (!grant) continue;
      forget(grant);
      gone.push(id);
      stores.add(grant.storeId);
    }
    if (!gone.length) return 0;
    send({ t: 'revoked', grants: gone, code, message });
    for (const storeId of stores) lastGone(storeId, code === 'forbidden' || code === 'unavailable');
    return gone.length;
  }

  function refuse(msg, code, message, extra) {
    send({ t: 'refused', grant: msg.grant, store: msg.store, code, message, ...extra });
  }

  /**
   * Judge a client for a store, as its own socket would be judged: who its
   * credential is, whether it may open the store, and its replica
   */
  function vouch(msg) {
    const { grant: id, store: storeId, replicaId } = msg;
    if (!isName(id)) return send({ t: 'error', message: 'A vouch needs a grant id' });
    if (!isStoreId(storeId)) return refuse(msg, 'invalid-store', 'A vouch needs a valid store id');
    if (!isName(replicaId)) return refuse(msg, 'invalid', 'A vouch needs the client\'s replicaId');
    // The same name again: the last client under it is replaced, its store let go of unless this one is let in there
    const old = grants.get(id);
    if (old) {
      forget(old);
      lastGone(old.storeId);
    }
    // This attempt's token: a later vouch of the same name, or an unvouch, disowns it (an earlier one's held token going back)
    vouching.get(id)?.settle?.();
    const attempt = { msg };
    vouching.set(id, attempt);
    const current = () => !closed && vouching.get(id) === attempt;
    // A token of the bucket is held while the vouch is judged, so vouches judged at once cannot all pass on one;
    // it is kept by a credential turned away, and given back otherwise
    let held = false;
    const settle = turnedAway => {
      if (held && !turnedAway) bucket.tokens = Math.min(rate.burst, bucket.tokens + 1);
      held = false;
    };
    const deny = (code, message, extra) => {
      vouching.delete(id);
      settle(code === 'unauthorized');
      refuse(msg, code, message, extra);
    };
    const wait = waitForBucket();
    if (wait > 0) return deny('rate-limited', `Too many vouches turned away; try again in ${wait} ms`, { retryAfter: wait });
    if (bucket) {
      bucket.tokens -= 1;
      held = true;
    }
    let who;
    step(() => admit(Utils.isPlainObject(msg.credential) ? msg.credential : {}, relay), answer => {
      if (!answer) return deny('unauthorized', 'Unauthorized');
      who = answer;
      step(() => (authorizeId ? authorizeId(who.user, storeId) : true), allowed => {
        if (!allowed) return deny('forbidden', `Not allowed to access store "${storeId}"`);
        const [store, code, message] = resolve(storeId);
        if (!store) return deny(code, message);
        step(() => (authorize ? authorize(who.user, storeId, store) : true), ok => {
          if (!ok) return deny('forbidden', `Not allowed to access store "${storeId}"`);
          if (store.disposed) return deny('unavailable', 'The store was unloaded; say hello again');
          const grant = { id, socket: isName(msg.socket) ? msg.socket : null, storeId, replicaId, user: who.user, store, peer: null, expires: who.expires ?? null, cancelExpiry: null, openedAt: Date.now() };
          try {
            grant.peer = store.peer({ user: grant.user, replicaId, via: relay, onEvict: code => evicted(grant, code) });
          } catch (err) {
            return deny(err?.code ?? 'forbidden', err?.message ?? 'Refused', err?.retryAfter === undefined ? undefined : { retryAfter: err.retryAfter });
          }
          vouching.delete(id);
          settle(false);
          grants.set(id, grant);
          if (grant.expires !== null) grant.cancelExpiry = runAt(grant.expires, () => revoke([id], 'reauthenticate', 'Your session ran out'));
          clearTimeout(lingering.get(storeId));
          lingering.delete(storeId);
          send({ t: 'admitted', grant: id, store: storeId, peer: grant.peer.peer, presence: grant.peer.presence, until: grant.expires });
        }, reason => deny('forbidden', reason || `Not allowed to access store "${storeId}"`), current);
      }, reason => deny('forbidden', reason || `Not allowed to access store "${storeId}"`), current);
    }, reason => deny('unauthorized', reason || 'Unauthorized'), current);
    // Disowned before its verdict (an unvouch, another vouch of the name): the token goes back with it
    attempt.settle = () => settle(false);
  }

  function unvouch(id) {
    const attempt = vouching.get(id);
    vouching.delete(id);
    attempt?.settle?.();
    const grant = grants.get(id);
    if (grant) forget(grant);
    // Its store let go of, unless another client there is let in (the relay's session lingers, see lastGone)
    const storeId = grant?.storeId ?? attempt?.msg.store;
    if (isStoreId(storeId)) lastGone(storeId);
  }

  function share(msg) {
    const grant = grants.get(msg.grant);
    const refused = (code, message, retryAfter) => send({ t: 'error', grant: msg.grant, store: grant?.storeId ?? msg.store, code, message, ...(retryAfter === undefined ? {} : { retryAfter }) });
    if (!grant?.peer || grant.peer.closed) return refused('unavailable', 'The client is not on the store');
    try {
      grant.peer.share(msg.data);
    } catch (err) {
      refused(err?.code ?? 'forbidden', err?.message ?? 'Share refused', err?.retryAfter);
    }
  }

  return {
    receive(msg) {
      if (closed) return;
      if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
      switch (msg.t) {
        case 'ping': return send({ t: 'pong' });
        case 'vouch': return vouch(msg);
        case 'unvouch': return unvouch(msg.grant);
        case 'share': if (msg.grant !== undefined) return share(msg); break;
      }
      toShared(msg);
    },

    /**
     * Judge again, those `filter(user, storeId)` picks (see the hub's
     * revalidate): each client let in, whose store no longer being theirs
     * revokes it ('forbidden'), and the relay itself on each store it
     * carries (`authorizeRelay`, the filter given the relay), whose store
     * refused closes its session there and revokes its clients there. A
     * vouch still being judged starts over. Resolves to how many clients
     * were revoked
     */
    async revalidate(filter) {
      if (closed) return 0;
      for (const { msg } of [...vouching.values()]) vouch(msg);
      // The relay's hellos still waiting on its own verdict start over too (a fresh queue disowns the old)
      for (const [id, queued] of [...pending]) openShared(id, [...queued]);
      let count = 0;
      if (authorizeRelay) {
        const verdicts = [...shared.keys()].filter(id => !filter || filter(relay, id)).map(async id => {
          let allowed;
          try {
            allowed = await authorizeRelay(relay, id);
          } catch {
            allowed = false;
          }
          return allowed ? null : id;
        });
        for (const id of await Promise.all(verdicts)) {
          if (id === null || closed) continue;
          closeShared(id, 'forbidden', `This relay may no longer carry store "${id}"`);
          count += revoke(grantsOn(id).map(g => g.id), 'forbidden', `This relay may no longer carry store "${id}"`);
        }
      }
      if (!authorizeId && !authorize) return count;
      const verdicts = [...grants.values()].filter(g => !filter || filter(g.user, g.storeId)).map(async grant => {
        let allowed;
        try {
          allowed = (!authorizeId || await authorizeId(grant.user, grant.storeId)) && (!authorize || await authorize(grant.user, grant.storeId, grant.store));
        } catch {
          allowed = false;
        }
        return allowed || grants.get(grant.id) !== grant ? 0 : revoke([grant.id], 'forbidden');
      });
      for (const n of await Promise.all(verdicts)) count += n;
      return count;
    },

    /** Revoke the clients whose user `filter(user)` picks ('reauthenticate'): they come back with their credential judged afresh. Returns how many */
    disconnect(filter) {
      if (closed) return 0;
      // A vouch being judged is judged again, with the credential as it stands by then
      for (const { msg } of [...vouching.values()]) vouch(msg);
      return revoke([...grants.values()].filter(g => !filter || filter(g.user)).map(g => g.id), 'reauthenticate', 'Sign in again');
    },

    get relay() { return relay; },
    /** Store ids the relay has a session on */
    get stores() { return [...shared.keys()]; },
    /** The clients let in: `{ user, store, socket, replicaId, openedAt, expiresAt }` */
    grants: () => [...grants.values()].map(g => ({ user: g.user, store: g.storeId, socket: g.socket, replicaId: g.replicaId, openedAt: g.openedAt, expiresAt: g.expires })),

    close() {
      if (closed) return;
      closed = true;
      for (const timer of lingering.values()) clearTimeout(timer);
      lingering.clear();
      for (const grant of grants.values()) {
        grant.cancelExpiry?.();
        grant.peer?.close();
      }
      grants.clear();
      vouching.clear();
      for (const [id, { session }] of shared) {
        session.close();
        channel?.unsubscribe(id);
      }
      shared.clear();
      pending.clear();
    }
  };
}

/**
 * A relay's socket as the adapters' `sockets()` list it, and its clients
 * after it, one entry per client socket (grants grouped by the relay's name
 * for it), each with `via`, the relay: `info` is the relay socket's own
 * numbers ({ buffered, idleMs, openMs }), which its clients share but for
 * how long each has been let in
 */
export function relaySockets(link, info, now = Date.now()) {
  const grants = link.grants();
  const clients = new Map();
  for (const [i, g] of grants.entries()) {
    const key = g.socket ?? `#${i}`;
    const entry = clients.get(key);
    if (entry) {
      entry.stores.push(g.store);
      entry.openMs = Math.max(entry.openMs, now - g.openedAt);
      continue;
    }
    clients.set(key, { user: g.user, stores: [g.store], via: link.relay, buffered: 0, idleMs: info.idleMs, openMs: now - g.openedAt, expiresAt: g.expiresAt });
  }
  return [{ relay: link.relay, stores: link.stores, grants: grants.length, ...info, expiresAt: null }, ...clients.values()];
}

/**
 * The request a client's own socket would have made, from what a relay
 * says of it (`{ headers, query }`), for the adapters' `authenticate` and
 * `expiresAt`: the same path, and of the relay's headers only those that
 * say who the client is (`authorization`, `cookie`, `user-agent`, and the
 * adapter's `headers`), never what a proxy or a browser would vouch for
 * (an origin, a forwarded address), which the relay could make up
 */
export function credentialRequest(credential, path, allowed = []) {
  const names = new Set([...CREDENTIAL_HEADERS, ...allowed.map(name => String(name).toLowerCase())]);
  const headers = new Headers();
  const given = Utils.isPlainObject(credential?.headers) ? credential.headers : {};
  for (const [name, value] of Object.entries(given)) {
    if (typeof value !== 'string' || !names.has(name.toLowerCase())) continue;
    try {
      headers.set(name, value);
    } catch { /* a value no request may carry is left out */ }
  }
  const query = typeof credential?.query === 'string' && credential.query.startsWith('?') ? credential.query : '';
  return new Request(`http://relay.invalid${path}${query}`, { headers });
}
