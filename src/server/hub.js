// hub.js - Many stores over one connection
//
// A hub is the server side of a multiplexed connection: it looks like a
// session (receive/close) but every incoming message names a `store`, and
// the hub keeps one real session per store on this socket, tagging what
// the sessions send back with the store id. Store sessions are created
// lazily on the first message for a store, after `authorizeId(user, id)`
// (before the store is resolved) and `authorize(user, id, store)` (after)
// allowed it (synchronously or through a promise; messages for that store
// queue meanwhile), and closed by `leave`, by eviction, by `revalidate`
// judging them again and refusing, or when the connection closes. The
// per-store protocol is untouched, so a store cannot tell a hub session
// from a direct one.
//
// Terminal conditions for one store are reported with a `closed` message
// carrying a code — 'invalid-store', 'unknown-store', 'forbidden' — which
// the client treats as final for that store.
import { LazyWatch } from 'lazy-watch';
import { isStoreId } from './registry.js';
import { tagStore } from './wire.js';

const { Utils } = LazyWatch;

/**
 * @param {(id: string) => Object|null} resolveStore - store for an id, or null
 * @param {Object} options
 * @param {(message: Object) => void} options.send
 * @param {any} [options.user] - the authenticated user, attached to every
 *   store session opened on this connection
 * @param {(user: any, storeId: string) => boolean|Promise<boolean>} [options.authorizeId]
 *   runs before the store is resolved, so a refusal loads nothing and
 *   reads the same whether the store exists or not; the check to use when
 *   it needs only the user and the id
 * @param {(user: any, storeId: string, store: Object) => boolean|Promise<boolean>} [options.authorize]
 *   runs once the store is resolved, for a check that needs the store
 * @param {(credential: Object, storeId: string) => any|Promise<any>} [options.vouch]
 *   on a follower's socket: a client's credential judged as its own socket
 *   would be; its user, or null to refuse it as unauthorized
 * @param {{ burst: number, perSecond: number }|false} [options.vouchRate]
 *   the vouches turned away (unauthorized, forbidden, or refused) this
 *   socket may ask for before every vouch is refused for a while
 *   ('rate-limited', with retryAfter): each cost the server its hooks, and a
 *   follower that floods it with credentials or stores it may not have is
 *   held back without holding back one whose clients are let in. Default
 *   { burst: 100, perSecond: 10 }
 * @param {{ subscribe(id: string): void, unsubscribe(id: string): void, publish(id: string, message: Object): void }} [options.channel]
 *   a transport that can fan a message out to every socket on a store at
 *   once (Bun's topic publish). Given one, a store's patch goes out as a
 *   single `publish` per store rather than a send per socket; each socket
 *   subscribes when it opens a store and unsubscribes on leave, eviction,
 *   or close
 * @param {{ url(storeId: string): string, threshold: number }} [options.httpSnapshots]
 *   where a client that can fetch gets a snapshot of `threshold` bytes or
 *   more, when the transport serves the route (see snapshot.js)
 * @param {(error: any) => void} [options.onError] - server faults (a store
 *   factory that threw); default console
 */
export function createHub(resolveStore, { send, user, authorizeId, authorize, vouch, vouchRate = { burst: 100, perSecond: 10 }, channel, httpSnapshots, onError = err => console.error('lazy-storage:', err), now = Date.now } = {}) {
  if (typeof send !== 'function') throw new TypeError('A hub needs a send function');
  if (vouchRate && !(vouchRate.burst > 0 && vouchRate.perSecond > 0)) throw new TypeError('vouchRate needs positive burst and perSecond, or false');
  // The vouches turned away: a token is held while a vouch is judged, and kept unless the client is let in
  const bucket = vouchRate ? { tokens: vouchRate.burst, at: now() } : null;
  const sessions = new Map();
  const storeOf = new Map(); // store id -> the store its session is on, judged again by revalidate
  const pending = new Map(); // store id -> messages queued while authorization is in flight
  let closed = false;

  const refuse = (id, code, message) => send({ t: 'closed', store: id, code, message });

  function drop(id) {
    sessions.delete(id);
    storeOf.delete(id);
    // Stop this socket hearing the store's broadcasts (the socket stays for its other stores)
    channel?.unsubscribe(id);
  }

  function open(id, store) {
    const session = store.session({
      send: message => send(tagStore(message, id)),
      user,
      onEvict: () => drop(id),
      // The transport fans a store's patch out to every subscribed socket at once, tagged and encoded once
      broadcast: channel ? message => channel.publish(id, tagStore(message, id)) : undefined,
      // Where a client that can fetch gets a large snapshot, when the transport serves the route
      httpSnapshot: httpSnapshots ? { url: httpSnapshots.url(id), threshold: httpSnapshots.threshold } : undefined,
      // A socket that vouches is a relay's link: its sessions may be followers', no other's
      follower: typeof vouch === 'function'
    });
    // Subscribed once the session exists, before its first message: the
    // store sent what it held back (see groupCommit) before making it, so the
    // socket hears no patch from before its hello, and every one after
    channel?.subscribe(id);
    sessions.set(id, session);
    storeOf.set(id, store);
    return session;
  }

  /**
   * A follower asks whether one of its clients may open the store: the
   * client's credential judged by `vouch` as its own socket would be, then
   * `authorizeId` and `authorize` as that client's. A yes is a grant on the
   * follower's session; the answer goes back as `admitted` or `refused`
   */
  function vouchFor(id, session, { as, credential, replica }) {
    if (typeof as !== 'string' || !as) return;
    const refused = (code, message, extra) => send({ t: 'refused', store: id, as, code, message, ...extra });
    if (typeof vouch !== 'function') return refused('forbidden', 'This server takes no clients vouched for by a follower');
    if (bucket) {
      const wall = now();
      bucket.tokens = Math.min(vouchRate.burst, bucket.tokens + ((wall - bucket.at) / 1000) * vouchRate.perSecond);
      bucket.at = wall;
      if (bucket.tokens < 1) {
        const retryAfter = Math.ceil(((1 - bucket.tokens) / vouchRate.perSecond) * 1000);
        return refused('rate-limited', `Too many vouches turned away; try again in ${retryAfter} ms`, { retryAfter });
      }
      bucket.tokens -= 1;
    }
    // The token comes back only with a client let in: every vouch turned away cost the hooks it ran
    const giveBack = () => {
      if (bucket) bucket.tokens = Math.min(vouchRate.burst, bucket.tokens + 1);
    };
    const store = storeOf.get(id);
    Promise.resolve()
      .then(() => vouch(credential, id))
      .then(async client => {
        if (client === null || client === undefined) return refused('unauthorized', "The client's credential was not accepted");
        if (authorizeId && !(await authorizeId(client, id))) return refused('forbidden', `Not allowed to access store "${id}"`);
        if (authorize && !(await authorize(client, id, store))) return refused('forbidden', `Not allowed to access store "${id}"`);
        if (closed || sessions.get(id) !== session) return;
        const refusal = session.grant(as, client, replica);
        if (refusal) return refused(refusal.code, refusal.message);
        giveBack();
        send({ t: 'admitted', store: id, as });
      })
      .catch(err => refused('forbidden', err?.message || 'Not allowed'));
  }

  function deliver(id, msg) {
    const { store: _store, ...inner } = msg;
    const session = sessions.get(id);
    if (!session) return;
    if (inner.t === 'vouch') return vouchFor(id, session, inner);
    session.receive(inner);
  }

  /**
   * Open a store for this connection: `authorizeId`, then the store, then
   * `authorize`, each of which may answer through a promise. Messages for
   * the store queue meanwhile. The queue is this attempt's token: a `leave`
   * (and perhaps a new attempt) disowns it, and whatever it was waiting for
   * is then ignored rather than opening a second session
   */
  function admit(id, queued) {
    pending.set(id, queued);
    const current = () => !closed && pending.get(id) === queued;
    const deny = message => {
      pending.delete(id);
      refuse(id, 'forbidden', message || `Not allowed to access store "${id}"`);
    };
    // Run a step, then `next` with its verdict, now or when its promise settles
    const step = (run, next) => {
      let verdict;
      try {
        verdict = run();
      } catch (err) {
        return deny(err?.message);
      }
      if (!verdict || typeof verdict.then !== 'function') return next(verdict);
      verdict.then(v => { if (current()) next(v); }, err => { if (current()) deny(err?.message); });
    };
    // Before the store is touched: a refusal here loads nothing, and reads
    // the same whether the store exists or not
    step(() => (authorizeId ? authorizeId(user, id) : true), allowed => {
      if (!allowed) return deny();
      // A store that cannot be opened (a factory or migration that throws)
      // is refused like an unknown one and logged as the server fault it
      // is; the connection and its other stores are unaffected
      let store;
      try {
        store = resolveStore(id);
      } catch (err) {
        pending.delete(id);
        // Served by another process for now (a deploy's overlap): not a
        // fault, and not final; the client says hello again in a moment
        if (err?.code === 'store-locked') return refuse(id, 'unavailable', err.message);
        onError(err);
        return refuse(id, 'unknown-store', `Store "${id}" could not be opened: ${err?.message ?? err}`);
      }
      if (!store) {
        pending.delete(id);
        return refuse(id, 'unknown-store', `Unknown store "${id}"`);
      }
      step(() => (authorize ? authorize(user, id, store) : true), ok => {
        if (!ok) return deny();
        pending.delete(id);
        open(id, store);
        for (const m of queued) deliver(id, m);
      });
    });
  }

  /**
   * Judge this connection's stores again, as the user it authenticated as:
   * `authorizeId`, then `authorize` on the store already open. A store
   * refused now is closed with 'forbidden', as a refusal at open is, and
   * the socket stays up for the rest. A store still waiting for its first
   * verdict starts over, so a check that began before whatever changed
   * cannot let it in. `filter(user, storeId)` picks the stores to judge.
   * Resolves to how many were closed
   */
  async function revalidate(filter) {
    if (closed || (!authorizeId && !authorize)) return 0;
    const judged = ([id]) => !filter || filter(user, id);
    // A fresh queue disowns the attempt in flight (see admit)
    for (const [id, queued] of [...pending].filter(judged)) admit(id, [...queued]);
    const verdicts = [...sessions].filter(judged).map(async ([id, session]) => {
      const store = storeOf.get(id);
      let allowed;
      let reason;
      try {
        allowed = (!authorizeId || await authorizeId(user, id)) && (!authorize || await authorize(user, id, store));
      } catch (err) {
        reason = err?.message;
      }
      // Left, closed, or reopened meanwhile: not this verdict's to close
      if (allowed || closed || sessions.get(id) !== session) return 0;
      session.close();
      drop(id);
      refuse(id, 'forbidden', reason || `Not allowed to access store "${id}"`);
      return 1;
    });
    let count = 0;
    for (const closedOne of await Promise.all(verdicts)) count += closedOne;
    return count;
  }

  return {
    receive(msg) {
      if (closed) return;
      if (!Utils.isPlainObject(msg)) return send({ t: 'error', message: 'Expected a message object' });
      if (msg.t === 'ping') return send({ t: 'pong' });
      const id = msg.store;
      if (!isStoreId(id)) {
        return refuse(typeof id === 'string' ? id : undefined, 'invalid-store', 'A message on a multiplexed connection needs a valid store id');
      }
      // A follower's client leaving (`as`) is the store session's to hear; a plain leave closes it
      if (msg.t === 'leave' && msg.as === undefined) {
        sessions.get(id)?.close();
        drop(id);
        pending.delete(id);
        return;
      }
      if (sessions.has(id)) return deliver(id, msg);
      if (pending.has(id)) return void pending.get(id).push(msg);

      admit(id, [msg]);
    },
    revalidate,
    /**
     * On a follower's socket: take back the clients behind it that
     * `filter(user)` picks, on every store, with `code` ('reauthenticate':
     * the follower closes the client's socket, which comes back judged
     * afresh). Returns how many
     */
    revoke(filter, code = 'reauthenticate', message = 'Your session was closed by the server') {
      let count = 0;
      for (const session of sessions.values()) if (typeof session.revoke === 'function') count += session.revoke(filter, code, message);
      return count;
    },
    /** Store ids with a live session on this connection */
    get stores() { return [...sessions.keys()]; },
    get user() { return user; },
    close() {
      closed = true;
      for (const [id, session] of sessions) {
        session.close();
        channel?.unsubscribe(id);
      }
      sessions.clear();
      storeOf.clear();
      pending.clear();
    }
  };
}
