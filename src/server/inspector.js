// inspector.js - A page to look into a running server: its stores, records, sessions, history and activity
//
// What a store holds is JSON in a process, and what it did is rows in its
// storage: to see either took a script. The inspector is one page, served
// by the server itself, that reads them through the store's own calls:
// the stores that are live, a store's collections and records, a record's
// clock and its history, the store's activity as it happens, who is
// connected, and a button that puts a record back as it was (store.revert).
//
// It is an administrator's view: it reads every record of every store, as
// the server does, by no rule of the stores' own. So there is no inspector
// unless the app gives one an `authorize(request, { write })`, which says
// who the request is from and whether they may look, and with `write`
// whether they may change (the revert). Nothing is open by default.
//
// It is transport-agnostic, as the snapshot route is: `handle(request,
// base)` takes a Web Request and answers a Response, or null for a path
// that is not under `base`. The adapters mount it at `<path>/inspect`.
//
//   GET  <base>                    the page
//   GET  <base>/api/stores         the live stores, each with its counts
//   GET  <base>/api/store          ?store=      a store: collections, sessions, counts
//   GET  <base>/api/records        ?store=&c=   a collection's records, a page at a time (&after=, &limit=, &q=)
//   GET  <base>/api/record         ?store=&c=&id=   a record and its clock
//   GET  <base>/api/history        ?store=&c=&id=   a record's history (&cursor=v.n, &limit=)
//   GET  <base>/api/activity       ?store=      the store's activity (&after=, &user=, &collection=, &deleted=1, &oldestFirst=1, &cursor=v.n, &limit=)
//   GET  <base>/api/peers          ?store=      who is connected to a store, a hundred at most (&q=)
//   GET  <base>/api/sockets        the server's open sockets, a hundred at most, the furthest behind first (&store=, &q=)
//   POST <base>/api/revert         { store, c, id, v, children? }   put a record back as it was at a version
//   POST <base>/api/disconnect     { key }      close a user's sockets, to authenticate afresh
//
// What `authorize` answers is who is looking: when it is a user (an object,
// or a string for its key) and not just `true`, a revert is told in the
// store's history as that user's, made through the server.
//
// The page is one document with its script and style inline, under a
// Content-Security-Policy that lets nothing else load or run, and it
// writes what it shows as text, never as markup: a record is whatever a
// client wrote.
import { randomId } from '../core/ids.js';
import { isStoreId } from './registry.js';

const PAGE_MOST = 200;
/** The most sockets, or peers, one answer lists: the page narrows them to find one among more */
const LIST_MOST = 100;
/** The most records one request looks into for what is searched for; it says where it stopped, to be asked on from */
const SEARCH_MOST = 5000;
const BODY_MOST = 64 * 1024;

const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json', 'cache-control': 'no-store' } });
const refusal = (status, message) => json({ error: message }, status);

/** `v.n` from a query, as a cursor; undefined for none */
function cursorOf(text) {
  if (text === null || text === '') return undefined;
  const [v, n] = text.split('.').map(Number);
  if (!Number.isSafeInteger(v) || (n !== undefined && !Number.isSafeInteger(n))) throw new TypeError('cursor is v.n: a version and a place in it');
  return { v, n };
}

/** A whole number from a query; undefined for none */
function numberOf(text, name) {
  if (text === null || text === '') return undefined;
  const n = Number(text);
  if (!Number.isSafeInteger(n)) throw new TypeError(`${name} is a whole number`);
  return n;
}

/**
 * @param {Object} options
 * @param {Object|((id: string) => Object|null)} options.stores - the registry, or the resolver, the server serves from
 * @param {(request: Request, context: { write: boolean }) => any} options.authorize - who may look (and, with
 *   `write`, change): a falsy answer, or a throw, turns the request away. It may answer through a promise
 * @param {() => Array<Object>} [options.sockets] - the server's open sockets, as an adapter's `sockets()` lists them
 * @param {(filter: (user: any) => boolean) => number} [options.disconnect] - close the sockets of the users a
 *   filter picks, as an adapter's `disconnect` does
 * @param {(error: any) => void} [options.onError]
 * @returns {{ handle: (request: Request, base: string) => Promise<Response|null> }}
 */
export function createInspector({ stores, authorize, sockets, disconnect, onError = err => console.error('lazy-storage:', err) } = {}) {
  if (!stores) throw new TypeError('An inspector needs stores (a registry or a resolver function)');
  if (typeof authorize !== 'function') throw new TypeError('An inspector needs authorize(request, { write }): it shows every record of every store, and nothing lets a request in by default');
  const resolve = typeof stores === 'function' ? stores : id => stores.get(id);
  const liveIds = () => (typeof stores.ids === 'function' ? stores.ids() : null);
  // A collection's ids in order, kept for as long as the store's version stands: a page of records is then a
  // place in a list, and not every id of the collection sorted again
  const sorted = new WeakMap();
  function idsOf(store, c) {
    let kept = sorted.get(store);
    if (!kept || kept.version !== store.version) sorted.set(store, kept = { version: store.version, by: new Map() });
    if (!kept.by.has(c)) kept.by.set(c, Object.keys(store.state[c]).sort());
    return kept.by.get(c);
  }
  /** Who a user is, as a store keys users unless it is told otherwise (`presence.key`): its id, or the whole of it as JSON */
  const keyOf = user => (user !== null && typeof user === 'object' && user.id != null ? String(user.id) : JSON.stringify(user));
  /** Each entry with the record as it was before the write (`was`), so that a change can be shown beside what it replaced */
  const withBefore = (store, entries) => entries.map(e => {
    let was;
    try {
      was = e.v > 0 ? store.recordAt(e.c, e.id, { v: e.v - 1 }) : undefined;
    } catch { /* nothing known of it then */ }
    return was === undefined ? e : { ...e, was };
  });

  /**
   * The store a request names. To look is not to load: a registry's store that no client has open is not made
   * for a look (its factory may make a file for it, or take its lease), and says so; `load` is the change that
   * loads it. A resolver function has no such list, and is asked
   */
  function storeOf(params, { load = false } = {}) {
    const id = params.get('store');
    if (!isStoreId(id)) throw new TypeError('store is a store id');
    if (!load && typeof stores.has === 'function' && !stores.has(id)) {
      throw Object.assign(new Error(`Store "${id}" is not loaded: no client has it open`), { status: 404, notLoaded: true });
    }
    const store = resolve(id);
    if (!store) throw Object.assign(new Error(`No store "${id}"`), { status: 404 });
    return store;
  }

  // Whether a store keeps history: it says so only by refusing, and is asked once
  const keeps = new WeakMap();
  function keepsHistory(store) {
    if (!keeps.has(store)) {
      try {
        store.activity({ limit: 1 });
        keeps.set(store, true);
      } catch {
        keeps.set(store, false);
      }
    }
    return keeps.get(store);
  }

  // A store's collections with how many records each has: counted once for a version of the store, since to
  // count them is to go through every record's key
  const counted = new WeakMap();
  function collectionsOf(store) {
    let kept = counted.get(store);
    if (!kept || kept.version !== store.version) {
      const { shape } = store;
      kept = {
        version: store.version,
        collections: [...shape.collections.map(c => ({ c, records: Object.keys(store.state[c]).length })), ...shape.singletons.map(c => ({ c, singleton: true }))]
      };
      counted.set(store, kept);
    }
    return kept.collections;
  }

  function overview(id, store) {
    const collections = collectionsOf(store);
    return {
      id,
      version: store.version,
      epoch: store.epoch,
      now: store.now(),
      history: keepsHistory(store),
      sessions: store.sessions,
      records: collections.reduce((sum, { records = 0 }) => sum + records, 0),
      collections
    };
  }

  /** How many of a list are asked for: a hundred or so unless it says, one at the least, `most` at the most */
  function limitOf(params, fallback, most = PAGE_MOST) {
    const limit = numberOf(params.get('limit'), 'limit');
    if (limit !== undefined && limit < 1) throw new TypeError('limit is a whole number of one or more');
    return Math.min(limit ?? fallback, most);
  }

  const api = {
    stores() {
      const ids = liveIds();
      return {
        // A resolver has no list of what it serves: the page asks for a store by its id
        listed: ids !== null,
        stores: (ids ?? []).map(id => {
          const store = resolve(id);
          return { id, version: store.version, sessions: store.sessions };
        })
      };
    },
    store: params => overview(params.get('store'), storeOf(params)),
    // What the page asks every two seconds, to know whether anything has changed: nothing that is counted
    glance(params) {
      const store = storeOf(params);
      return { version: store.version, sessions: store.sessions };
    },
    records(params) {
      const store = storeOf(params);
      const c = params.get('c');
      if (!store.shape.has(c)) throw new TypeError(`"${c}" is not a collection or singleton of this store`);
      if (store.shape.isSingleton(c)) return { singleton: true, record: store.state[c] };
      const limit = limitOf(params, 100);
      const after = params.get('after');
      const q = (params.get('q') ?? '').toLowerCase();
      const all = store.state[c];
      const ids = idsOf(store, c);
      // Where the page before stopped: the first id after it, found by halving
      let from = 0;
      if (after !== null) {
        let [low, high] = [0, ids.length];
        while (low < high) {
          const mid = (low + high) >> 1;
          if (ids[mid] <= after) low = mid + 1; else high = mid;
        }
        from = low;
      }
      const records = [];
      let more;
      let looked = 0;
      for (let i = from; i < ids.length; i++) {
        const id = ids[i];
        if (records.length === limit) {
          more = ids[i - 1];
          break;
        }
        // What is looked for is in the id, or anywhere in the record as it is written. So many records are
        // looked into for one request and no more: `searching` says it stopped for that, with more to look in
        if (q) {
          if (looked === SEARCH_MOST) return { records, more: ids[i - 1], searching: true, total: ids.length };
          looked++;
          if (!id.toLowerCase().includes(q) && !JSON.stringify(all[id]).toLowerCase().includes(q)) continue;
        }
        records.push({ id, record: all[id] });
      }
      return { records, more, total: ids.length };
    },
    record(params) {
      const store = storeOf(params);
      const c = params.get('c');
      if (!store.shape.has(c)) throw new TypeError(`"${c}" is not a collection or singleton of this store`);
      const id = store.shape.isSingleton(c) ? undefined : params.get('id');
      const record = store.shape.isSingleton(c) ? store.state[c] : typeof id === 'string' && Object.hasOwn(store.state[c], id) ? store.state[c][id] : undefined;
      return { c, id, there: record !== undefined, record, clock: record === undefined ? undefined : store.clock(c, id) };
    },
    history(params) {
      const store = storeOf(params);
      const c = params.get('c');
      const id = store.shape?.isSingleton(c) ? undefined : params.get('id');
      const limit = limitOf(params, 50);
      return { entries: withBefore(store, store.historyOf(c, id, { limit, cursor: cursorOf(params.get('cursor')) })) };
    },
    activity(params) {
      const store = storeOf(params);
      const limit = limitOf(params, 50);
      return {
        version: store.version,
        entries: withBefore(store, store.activity({
          limit,
          after: numberOf(params.get('after'), 'after'),
          before: numberOf(params.get('before'), 'before'),
          user: params.get('user') || undefined,
          collection: params.get('collection') || undefined,
          deleted: params.get('deleted') === '1',
          oldestFirst: params.get('oldestFirst') === '1',
          cursor: cursorOf(params.get('cursor'))
        }))
      };
    },
    // Who is connected to a store, a page of them: a store may have thousands, and the page asks every time it looks
    peers(params) {
      const store = storeOf(params);
      const q = (params.get('q') ?? '').toLowerCase();
      const all = store.peers();
      const found = q ? all.filter(peer => `${peer.replicaId} ${peer.key ?? ''} ${JSON.stringify(peer.user) ?? ''}`.toLowerCase().includes(q)) : all;
      const stats = store.stats();
      return { total: all.length, found: found.length, peers: found.slice(0, LIST_MOST), sessions: stats.sessions, replicas: stats.replicas, sent: stats.sent, epoch: store.epoch };
    },
    // The server's sockets, a page of them: the ones furthest behind first, since those are the ones to look at.
    // `store` narrows them to the sockets that have that store open, `q` to the users whose key has it
    sockets(params) {
      // A server of one's own may have none to tell of
      if (typeof sockets !== 'function') return { listed: false, total: 0, found: 0, sockets: [] };
      const store = params.get('store');
      const q = (params.get('q') ?? '').toLowerCase();
      const all = sockets().map(socket => ({
        ...(Object.hasOwn(socket, 'relay') ? { relay: true, key: keyOf(socket.relay) } : { key: socket.user === undefined ? null : keyOf(socket.user) }),
        stores: socket.stores,
        buffered: socket.buffered,
        idleMs: socket.idleMs,
        openMs: socket.openMs,
        expiresAt: socket.expiresAt ?? null
      }));
      const found = all.filter(socket => (!store || socket.stores.includes(store)) && (!q || (socket.key ?? '').toLowerCase().includes(q)));
      found.sort((x, y) => y.buffered - x.buffered || y.idleMs - x.idleMs);
      return {
        listed: true,
        disconnects: typeof disconnect === 'function',
        total: all.length,
        found: found.length,
        // Never more than its first few stores each: a socket may have hundreds open
        sockets: found.slice(0, LIST_MOST).map(socket => (socket.stores.length > 5 ? { ...socket, stores: socket.stores.slice(0, 5), moreStores: socket.stores.length - 5 } : socket))
      };
    }
  };

  /** The JSON a change is asked with, or the refusal: a change is taken from the inspector's own page */
  async function bodyOf(request) {
    // A page of another origin can post a form, but not this: JSON, and a header a form cannot set (for a
    // browser too old to say where its requests come from)
    if (request.headers.get('x-lazy-inspect') !== '1' || !(request.headers.get('content-type') ?? '').startsWith('application/json')) {
      return [null, refusal(400, 'A change is asked for by the inspector\'s own page')];
    }
    // So much of a body is read and no more: one that says it is larger is not read at all, and one that turns
    // out to be is let go of where it passes the mark
    if (Number(request.headers.get('content-length')) > BODY_MOST) return [null, refusal(413, 'Too large')];
    const chunks = [];
    let size = 0;
    const reader = request.body?.getReader();
    for (let read = await reader?.read(); read && !read.done; read = await reader.read()) {
      size += read.value.length;
      if (size > BODY_MOST) {
        await reader.cancel().catch(() => {});
        return [null, refusal(413, 'Too large')];
      }
      chunks.push(read.value);
    }
    const bytes = new Uint8Array(size);
    chunks.reduce((at, chunk) => (bytes.set(chunk, at), at + chunk.length), 0);
    const text = new TextDecoder().decode(bytes);
    try {
      const body = JSON.parse(text);
      return body !== null && typeof body === 'object' ? [body, null] : [null, refusal(400, 'Expected a JSON object')];
    } catch {
      return [null, refusal(400, 'Expected JSON')];
    }
  }

  const changes = {
    /** Put a record back as it was at a version: told in the history as whoever is looking, when `authorize` says who */
    revert(body, who) {
      if (!Number.isSafeInteger(body.v)) throw new TypeError('A revert names a store, a record and the version to put it back to');
      const store = storeOf(new URLSearchParams({ store: String(body.store) }));
      // Who is looking, as a key: a string is one, and an object's id. Anything else (a session with no id to
      // it) is nobody the history should be handed whole
      const id = who !== null && typeof who === 'object' ? who.id : who;
      const by = typeof id === 'string' && id ? id : typeof id === 'number' ? String(id) : undefined;
      const landed = store.revert(body.c, store.shape.isSingleton(body.c) ? undefined : body.id, { v: body.v }, { children: body.children !== false, by });
      return { changes: landed, version: store.version };
    },
    /** Load a store no client has open, to look at it: what a look alone does not do (see storeOf) */
    load(body) {
      const store = storeOf(new URLSearchParams({ store: String(body.store) }), { load: true });
      return { version: store.version };
    },
    /** Close the sockets of the user with this key: its clients come back and authenticate afresh */
    disconnect(body) {
      if (typeof disconnect !== 'function') throw new TypeError('This server has no sockets for the inspector to close');
      if (typeof body.key !== 'string') throw new TypeError('A disconnect names a user by its key');
      return { closed: disconnect(user => user !== undefined && keyOf(user) === body.key) };
    }
  };

  /** The Response for a request under `base`; null for one that is not */
  async function handle(request, base) {
    const url = new URL(request.url);
    if (url.pathname !== base && !url.pathname.startsWith(`${base}/`)) return null;
    const rest = url.pathname.slice(base.length).replace(/^\/+|\/+$/g, '');
    const write = request.method === 'POST';
    if (request.method !== 'GET' && request.method !== 'POST') return new Response('Method not allowed', { status: 405, headers: { allow: 'GET, POST' } });
    let allowed;
    try {
      allowed = await authorize(request, { write });
    } catch (err) {
      onError(err);
      allowed = false;
    }
    // What turns a request away may be the app's own answer: a 401 that asks for credentials, a redirect to
    // where one signs in
    if (allowed instanceof Response) return allowed;
    if (!allowed) return new Response('Not allowed', { status: 401, headers: { 'cache-control': 'no-store' } });
    try {
      if (rest === '' && !write) return page(base);
      const name = rest.startsWith('api/') ? rest.slice(4) : null;
      // What is asked of the inspector is asked by its own page, or by no page at all: a browser says where a
      // request comes from (Sec-Fetch-Site, which a page cannot set), and one from a page of another site is
      // not to be had with the credentials the browser adds to it. A program says nothing, and is let by
      const site = request.headers.get('sec-fetch-site');
      if (name !== null && site !== null && site !== 'same-origin' && site !== 'none') return refusal(403, 'Asked for from another site');
      if (write) {
        if (name === null || !Object.hasOwn(changes, name)) return refusal(404, 'Not found');
        const [body, refused] = await bodyOf(request);
        return refused ?? json(changes[name](body, allowed));
      }
      if (name === null || !Object.hasOwn(api, name)) return refusal(404, 'Not found');
      return json(api[name](url.searchParams));
    } catch (err) {
      // What was asked could not be answered as asked (a store that is not, a collection it has not, history
      // that is off) is the asker's to hear; anything else is a fault
      if (err instanceof TypeError) return refusal(400, err.message);
      if (err?.status === 404) return json({ error: err.message, ...(err.notLoaded ? { notLoaded: true } : {}) }, 404);
      if (/^Nothing is known/.test(err?.message ?? '')) return refusal(409, err.message);
      onError(err);
      return refusal(500, 'Something went wrong');
    }
  }

  return { handle };
}

/** The page: one document, its style and script inline under a nonce, and nothing else allowed to load or run */
function page(base) {
  const nonce = randomId();
  const html = PAGE.replaceAll('%NONCE%', nonce).replace('%BASE%', () => JSON.stringify(base).replaceAll('<', '\\u003c'));
  return new Response(html, {
    headers: {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'content-security-policy': `default-src 'none'; script-src 'nonce-${nonce}'; style-src 'nonce-${nonce}'; connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
      'x-content-type-options': 'nosniff',
      'referrer-policy': 'no-referrer'
    }
  });
}

const PAGE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>lazy-storage inspector</title>
<link rel="icon" href="data:,">
<style nonce="%NONCE%">
  :root { color-scheme: light dark; --bg: #fff; --fg: #1b1b1f; --dim: #6b6b76; --line: #dcdce3; --soft: #f3f3f7; --accent: #2f5fd0; --bad: #b3261e; --good: #1a7f4b; --num: #9a5b00; --minus: #fde7e5; --plus: #e2f5e9; }
  @media (prefers-color-scheme: dark) { :root { --bg: #16161a; --fg: #e7e7ec; --dim: #9a9aa6; --line: #2e2e36; --soft: #1f1f25; --accent: #8fb0ff; --bad: #ff8a80; --good: #6fd39b; --num: #e0b062; --minus: #4a2523; --plus: #1e3d2c; } }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; background: var(--bg); color: var(--fg); }
  header { display: flex; flex-wrap: wrap; gap: 8px 16px; align-items: center; padding: 10px 16px; border-bottom: 1px solid var(--line); }
  header h1 { font-size: 15px; margin: 0; }
  nav { display: flex; gap: 4px; }
  button, select, input { font: inherit; color: inherit; background: var(--soft); border: 1px solid var(--line); border-radius: 6px; padding: 4px 9px; }
  button { cursor: pointer; }
  button[aria-pressed="true"] { background: var(--accent); border-color: var(--accent); color: var(--bg); }
  button.danger { color: var(--bad); }
  main { display: grid; grid-template-columns: minmax(150px, 190px) minmax(180px, 260px) 1fr; min-height: calc(100vh - 52px); }
  main > section { border-right: 1px solid var(--line); padding: 10px 12px; overflow: auto; min-width: 0; }
  main > section:last-child { border-right: 0; }
  main.wide { display: block; }
  main.wide > section { border: 0; }
  @media (max-width: 760px) { main { display: block; } main > section { border-right: 0; border-bottom: 1px solid var(--line); } }
  h2:not(:first-child) { margin-top: 18px; }
  h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .05em; color: var(--dim); margin: 0 0 8px; }
  ul { list-style: none; margin: 0; padding: 0; }
  li.pick { padding: 4px 8px; border-radius: 6px; cursor: pointer; display: flex; justify-content: space-between; gap: 8px; overflow: hidden; }
  li.pick:hover { background: var(--soft); }
  li.pick[aria-current="true"] { background: var(--accent); color: var(--bg); }
  li.pick[aria-current="true"] .dim { color: inherit; }
  li.pick span:first-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .dim { color: var(--dim); }
  .bad { color: var(--bad); }
  .good { color: var(--good); }
  pre { margin: 0; padding: 10px; background: var(--soft); border-radius: 8px; overflow: auto; font: 12.5px/1.45 ui-monospace, monospace; white-space: pre-wrap; word-break: break-word; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 5px 10px 5px 0; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { font-size: 12px; color: var(--dim); font-weight: 600; }
  tr.pick { cursor: pointer; }
  tr.pick:hover td { background: var(--soft); }
  td.mono, .mono { font: 12.5px ui-monospace, monospace; word-break: break-word; }
  .row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: 0 0 10px; }
  .entry { border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px; margin: 0 0 8px; }
  .entry header { padding: 0 0 6px; border: 0; gap: 4px 12px; }
  details { margin: 10px 0; }
  summary { cursor: pointer; color: var(--dim); }
  #note, #offline { padding: 6px 16px; }
  #offline { background: var(--minus); }
  /* A panel that is being filled: dimmed once it has taken a moment, so that a quick answer does not flicker */
  [aria-busy="true"] { opacity: .55; cursor: progress; transition: opacity .15s linear .2s; }
  #ask { position: fixed; inset: 0; z-index: 10; background: rgba(0, 0, 0, .45); display: flex; align-items: flex-start; justify-content: center; padding: 8vh 16px; overflow: auto; }
  #ask[hidden] { display: none; }
  #ask .box { background: var(--bg); border: 1px solid var(--line); border-radius: 10px; padding: 16px; width: min(640px, 100%); box-shadow: 0 10px 40px rgba(0, 0, 0, .35); }
  #ask .row.end { justify-content: flex-end; margin: 14px 0 0; }
  .json { position: relative; padding: 10px; background: var(--soft); border-radius: 8px; overflow: auto; font: 12.5px/1.5 ui-monospace, monospace; word-break: break-word; }
  .json details { margin: 0; }
  .json summary { color: inherit; }
  .json details[open] > summary .count { display: none; }
  .json .in { padding-left: 18px; }
  .json .leaf { padding-left: 14px; }
  .json .k { color: var(--accent); }
  .json .s { color: var(--good); }
  .json .n, .json .b { color: var(--num); }
  .json .z { color: var(--dim); }
  .json > button.copy { position: absolute; top: 6px; right: 6px; padding: 1px 7px; font: 12px system-ui, sans-serif; }
  .change { margin: 0 0 6px; font: 12.5px/1.5 ui-monospace, monospace; }
  .change .at { color: var(--dim); }
  .change pre { padding: 2px 8px; border-radius: 4px; background: none; }
  .change pre.minus { background: var(--minus); }
  .change pre.plus { background: var(--plus); }
  /* Numbers that change as they are watched keep their width: digits of one width, and room for the longest */
  .ago { color: var(--dim); font-weight: normal; display: inline-block; min-width: 5.5em; }
  .ago, .entry header strong, #meta, td, li.pick span:last-child { font-variant-numeric: tabular-nums; }
  @keyframes changed { from { box-shadow: 0 0 0 2px var(--accent); } to { box-shadow: 0 0 0 2px transparent; } }
  .changed { animation: changed 1.6s ease-out; }
</style>
</head>
<body>
<header>
  <h1>lazy-storage inspector</h1>
  <label>Store <select id="stores"></select></label>
  <input id="storeId" placeholder="store id" size="14" hidden>
  <nav id="tabs"></nav>
  <span id="meta" class="dim"></span>
</header>
<div id="offline" class="bad" hidden></div>
<div id="note" class="bad" hidden></div>
<div id="ask" hidden></div>
<main id="main"></main>
<script nonce="%NONCE%">
'use strict';
const BASE = %BASE%;
const $ = id => document.getElementById(id);
// Everything that is shown is set as text: a record is whatever a client wrote
const el = (tag, props = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (key === 'on') for (const [event, fn] of Object.entries(value)) node.addEventListener(event, fn);
    else if (key === 'class') node.className = value;
    else if (value !== undefined && value !== false) node.setAttribute(key, value === true ? '' : String(value));
  }
  for (const child of children.flat(Infinity)) if (child !== undefined && child !== null && child !== false) node.append(child);
  return node;
};
const pretty = value => (value === undefined ? '(not there)' : JSON.stringify(value, null, 2));
// A time as 2026-11-12 10:16:12, in the reader's own zone: the same everywhere, and sorted as it reads
const two = n => String(n).padStart(2, '0');
const when = ms => { const d = new Date(ms); return d.getFullYear() + '-' + two(d.getMonth() + 1) + '-' + two(d.getDate()) + ' ' + two(d.getHours()) + ':' + two(d.getMinutes()) + ':' + two(d.getSeconds()); };
// How long ago, said roughly: what a time is for, most of the time
const ago = ms => {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return s + ' s ago';
  if (s < 3600) return Math.round(s / 60) + ' min ago';
  if (s < 86400) return Math.round(s / 3600) + ' h ago';
  return Math.round(s / 86400) + ' d ago';
};
// A time, and beside it how long ago, which the page keeps current (see glance)
const timed = ms => [el('strong', {}, when(ms)), el('span', { class: 'ago', 'data-at': ms }, ago(ms))];
const brief = (value, most = 80) => { const text = JSON.stringify(value); return text === undefined ? '' : text.length > most ? text.slice(0, most) + '…' : text; };

// A value as a tree: coloured by kind, an object or a list folded and unfolded, and the whole copied as JSON.
// A value too large to lay out so is shown as its text
const kind = value => (value === null ? 'z' : typeof value === 'string' ? 's' : typeof value === 'number' ? 'n' : typeof value === 'boolean' ? 'b' : '');
function branch(value, key, last) {
  const label = key === undefined ? [] : [el('span', { class: 'k' }, JSON.stringify(key)), ': '];
  const comma = last ? '' : ',';
  if (value === null || typeof value !== 'object') return el('div', { class: 'leaf' }, label, el('span', { class: kind(value) }, JSON.stringify(value)), comma);
  const list = Array.isArray(value);
  const keys = list ? value.map((_, i) => i) : Object.keys(value);
  const [open, close] = list ? ['[', ']'] : ['{', '}'];
  if (!keys.length) return el('div', { class: 'leaf' }, label, open + close, comma);
  return el('details', { open: true },
    el('summary', {}, label, open, el('span', { class: 'count z' }, ' ' + keys.length + (list ? ' items ' : ' fields ') + close + comma)),
    el('div', { class: 'in' }, keys.map((k, i) => branch(value[k], list ? undefined : k, i === keys.length - 1))),
    el('div', { class: 'leaf' }, close + comma));
}
function shown(value) {
  if (value === undefined) return el('div', { class: 'json z' }, '(not there)');
  const text = JSON.stringify(value, null, 2);
  const copy = el('button', { class: 'copy', on: { click: async () => {
    try {
      await navigator.clipboard.writeText(text);
      copy.textContent = 'Copied';
    } catch {
      copy.textContent = 'Not copied';
    }
    setTimeout(() => { copy.textContent = 'Copy'; }, 1500);
  } } }, 'Copy');
  return el('div', { class: 'json', 'data-json': JSON.stringify(value) }, copy, text.length > 200000 ? el('pre', {}, text) : branch(value, undefined, true));
}
// A value folded away until it is asked for: its tree is made when it is first opened
const folded = (summary, value) => {
  const details = el('details', {}, el('summary', {}, summary));
  details.addEventListener('toggle', () => { if (details.open && details.children.length === 1) details.append(shown(value)); });
  return details;
};

// A change as what was there and what is there now: was is the record before the write, where it is known
const at = (record, path) => path.reduce((node, key) => (node !== null && typeof node === 'object' && Object.hasOwn(node, key) ? node[key] : undefined), record);
const side = (sign, value) => el('pre', { class: sign === '-' ? 'minus' : 'plus' }, pretty(value).split('\\n').map(line => sign + ' ' + line).join('\\n'));
function changed(change, was) {
  const path = change.path ?? [];
  const where = path.join(' / ') || 'the record';
  const old = was === undefined ? undefined : at(was, path);
  const gone = change.del === true;
  const now = gone ? undefined : Object.hasOwn(change, 'put') ? change.put : change.set;
  const verb = gone ? 'delete' : path.length === 0 ? (was === undefined ? 'make' : 'put') : Object.hasOwn(change, 'put') ? 'put' : 'set';
  // Written with the value it had already: said once, as no change
  if (!gone && old !== undefined && JSON.stringify(old) === JSON.stringify(now)) return el('div', { class: 'change' }, el('div', { class: 'at' }, verb + ' ' + where + ' (as it was)'), el('pre', {}, pretty(now).split('\\n').map(line => '  ' + line).join('\\n')));
  return el('div', { class: 'change' }, el('div', { class: 'at' }, verb + ' ' + where), old !== undefined && side('-', old), !gone && side('+', now));
}

let feedBody = null;
const state = { store: null, tab: 'records', c: null, id: null, q: '', feed: [], feedAt: null, filters: { user: '', collection: '', deleted: false }, live: true, overview: null };

async function ask(name, params = {}) {
  const query = new URLSearchParams(Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== '' && value !== false).map(([key, value]) => [key, value === true ? '1' : String(value)]));
  let response;
  try {
    response = await fetch(BASE + '/api/' + name + (query.size ? '?' + query : ''), { headers: { accept: 'application/json' } });
  } catch (err) {
    // Not refused: not reached. The page waits and asks again (see the timer at the end)
    throw Object.assign(new Error('The server is not answering'), { unreached: true });
  }
  const body = await response.json().catch(() => ({ error: response.statusText }));
  if (!response.ok) throw Object.assign(new Error(body.error || response.statusText), { notLoaded: body.notLoaded === true });
  return body;
}
function note(message) {
  $('note').hidden = !message;
  $('note').textContent = message || '';
}
const tried = fn => async (...args) => {
  try {
    note('');
    return await fn(...args);
  } catch (err) {
    note(err.message);
  }
};

// A part of the page that is filled from the server: dimmed while it waits, and where it could not be filled
// it says why, in its own place, with a way to try again. What a button was asked to do and could not is said
// in the line at the top (tried)
async function panel(node, fn, again) {
  node.setAttribute('aria-busy', 'true');
  try {
    return await fn();
  } catch (err) {
    const load = err.notLoaded && el('button', { on: { click: tried(async () => { await post('load', { store: state.store }); await again(); }) } }, 'Load it');
    node.replaceChildren(el('section', {}, el('p', { class: 'bad' }, err.message), load || (again && el('button', { on: { click: again } }, 'Try again'))));
  } finally {
    node.removeAttribute('aria-busy');
  }
}

// A question asked in the page, with what the answer will do laid out beneath it: resolves to yes or no
let unasked = () => {};
function sure(title, body, yes) {
  return new Promise(resolve => {
    const done = answer => {
      $('ask').hidden = true;
      $('ask').replaceChildren();
      removeEventListener('keydown', key);
      unasked = () => {};
      resolve(answer);
    };
    // The page going elsewhere (back, a link) is a no: what was asked about is no longer what is shown
    unasked = () => done(false);
    const key = event => { if (event.key === 'Escape') done(false); };
    const no = el('button', { class: 'no', on: { click: () => done(false) } }, 'Cancel');
    $('ask').replaceChildren(el('div', { class: 'box', role: 'dialog', 'aria-modal': 'true', 'aria-label': title },
      el('h2', {}, title), body, el('div', { class: 'row end' }, no, el('button', { class: 'danger yes', on: { click: () => done(true) } }, yes))));
    $('ask').hidden = false;
    addEventListener('keydown', key);
    no.focus();
  });
}

// What differs between a record as it is and as it would be: [{ path, old, now }], a field each, an object both
// have gone into
function differences(old, now, path = []) {
  const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  if (plain(old) && plain(now)) return [...new Set([...Object.keys(old), ...Object.keys(now)])].flatMap(key => differences(old[key], now[key], [...path, key]));
  return JSON.stringify(old) === JSON.stringify(now) ? [] : [{ path, old, now }];
}

const TABS = [['records', 'Records'], ['activity', 'Activity'], ['sessions', 'Sessions']];

// Where the page is, kept in its address: a record can be linked to, and the way back is the browser's
const placed = () => '#' + new URLSearchParams(Object.entries({ store: state.store, tab: state.tab, c: state.tab === 'records' ? state.c : null, id: state.tab === 'records' ? state.id : null }).filter(([, value]) => value !== null && value !== undefined));
function place() {
  if (location.hash === placed() || !state.store) return;
  // The address the page was opened at (none, or half of one) is not a place to go back to: the first place
  // takes its place, and each after it is a step that back undoes
  if (placedOnce) history.pushState(null, '', placed());
  else history.replaceState(null, '', placed());
  placedOnce = true;
}
let placedOnce = false;
function placeFrom() {
  const params = new URLSearchParams(location.hash.slice(1));
  const tab = params.get('tab');
  const changed = params.get('store') !== state.store;
  Object.assign(state, { store: params.get('store') ?? state.store, tab: TABS.some(([name]) => name === tab) ? tab : 'records', c: params.get('c'), id: params.get('id') });
  if (changed) Object.assign(state, { feed: [], feedAt: null });
  return params.has('store');
}
function tabs() {
  $('tabs').replaceChildren(...TABS.map(([name, label]) => el('button', { 'aria-pressed': String(state.tab === name), on: { click: () => { state.tab = name; show(); } } }, label)));
}

const loadStores = tried(async () => {
  const { stores, listed } = await ask('stores');
  $('storeId').hidden = listed;
  $('stores').replaceChildren(...stores.map(s => el('option', { value: s.id, selected: s.id === state.store }, s.id)));
  if (state.store) await show();
  else if (stores.length) await pick(stores[0].id);
  else if (!stores.length && listed) $('main').replaceChildren(el('section', {}, el('p', { class: 'dim' }, 'No store is live. One appears here when a client opens it.')));
});

const pick = tried(async id => {
  Object.assign(state, { store: id, c: null, id: null, feed: [], feedAt: null });
  await show();
});

// The line at the top: what the store is at, from the last look at it
function top() {
  const o = state.overview;
  $('meta').textContent = o ? 'version ' + o.version + ' · ' + o.records + ' records · ' + o.sessions + ' sessions · history ' + (o.history ? 'on' : 'off') : '';
}

const show = () => panel($('main'), async () => {
  note('');
  tabs();
  if (!state.store) return;
  place();
  if ($('stores').value !== state.store) $('stores').value = state.store;
  state.overview = await ask('store', { store: state.store });
  counted = Date.now();
  uncounted = false;
  top();
  $('main').className = state.tab === 'records' ? '' : 'wide';
  if (state.tab === 'records') await records();
  else if (state.tab === 'activity') await activity(true);
  else await sessions();
}, () => show());

// --- Records -------------------------------------------------------------------------------

async function records() {
  const o = state.overview;
  if (!state.c && o.collections.length) state.c = o.collections[0].c;
  const collections = el('section', {}, el('h2', {}, 'Collections'), el('ul', {}, o.collections.map(({ c, records: count, singleton }) =>
    el('li', { class: 'pick', 'data-c': c, 'aria-current': String(c === state.c), on: { click: () => { state.c = c; state.id = null; state.q = ''; show(); } } }, el('span', {}, c), el('span', { class: 'dim' }, singleton ? 'one' : String(count))))));
  const list = el('section', {});
  const detail = el('section', {});
  $('main').replaceChildren(collections, list, detail);
  if (!state.c) return;
  const single = o.collections.find(x => x.c === state.c)?.singleton;
  if (single) {
    list.append(el('h2', {}, state.c), el('p', { class: 'dim' }, 'A singleton: one record, with no id.'));
    return record(detail);
  }
  const ids = el('ul', {});
  const more = el('button', { hidden: true }, 'More');
  const fill = tried(async after => {
    const page = await ask('records', { store: state.store, c: state.c, q: state.q, after });
    if (after === undefined) ids.replaceChildren();
    ids.append(...page.records.map(({ id, record: value }) => el('li', { class: 'pick', title: brief(value, 300), 'aria-current': String(id === state.id), on: { click: () => { state.id = id; place(); for (const li of ids.children) li.setAttribute('aria-current', String(li.dataset.id === id)); record(detail); } }, 'data-id': id }, el('span', {}, id))));
    more.hidden = page.more === undefined;
    // So many records are looked into for one request: the search goes on from where it stopped
    more.textContent = page.searching ? 'Search further' : 'More';
    more.onclick = () => fill(page.more);
    if (!page.records.length && after === undefined) ids.append(el('li', { class: 'dim' }, !state.q ? 'No records.' : page.searching ? 'Nothing matches so far.' : 'Nothing matches.'));
  });
  const search = el('input', { type: 'search', placeholder: 'find in id or record', value: state.q, on: { change: () => { state.q = search.value; fill(); } } });
  list.append(el('h2', {}, state.c), el('div', { class: 'row' }, search), ids, more);
  await fill();
  if (state.id) await record(detail);
  else detail.append(el('p', { class: 'dim' }, 'Pick a record.'));
}

// The record that is shown, to tell when it has changed under the reader (see glance)
let looking = null;
const record = (into, flash) => panel(into, async () => {
  const r = await ask('record', { store: state.store, c: state.c, id: state.id });
  looking = { into, store: state.store, c: state.c, id: state.id, json: JSON.stringify(r.record) };
  const title = state.id === null ? state.c : state.c + ' / ' + state.id;
  const history = el('div', {});
  into.replaceChildren(
    el('h2', {}, title),
    shown(r.record),
    r.clock && folded('Clock: when each path was written', r.clock),
    el('h2', {}, 'History'),
    history
  );
  if (!state.overview.history) return history.append(el('p', { class: 'dim' }, 'This store keeps no history. Make it with history: true to see what a record was.'));
  const fill = tried(async cursor => {
    const { entries } = await ask('history', { store: state.store, c: state.c, id: state.id, cursor: cursor && cursor.v + '.' + cursor.n });
    history.querySelector('button.more')?.remove();
    history.append(...entries.map((e, i) => entry(e, { revert: !(cursor === undefined && i === 0) })));
    if (entries.length === 50) history.append(el('button', { class: 'more', on: { click: () => fill(entries[entries.length - 1]) } }, 'Older'));
    if (!entries.length && cursor === undefined) history.append(el('p', { class: 'dim' }, 'Nothing is known of this record.'));
  });
  await fill();
  if (flash) into.querySelector('.json').classList.add('changed');
}, () => record(into));

function entry(e, { revert = false, where = false } = {}) {
  const who = e.replicaId === 'server' ? (e.user === undefined ? 'the server' : e.user + ', through the server') : (e.user ?? 'someone') + ' (' + e.replicaId + ')';
  const late = e.at - e.ts > 1000 ? ' · made ' + when(e.ts) : '';
  return el('div', { class: 'entry' },
    el('header', {},
      timed(e.at),
      el('span', { class: 'dim' }, 'v' + e.v + ' · ' + who + late),
      where && el('button', { on: { click: () => { state.tab = 'records'; state.c = e.c; state.id = e.id ?? null; show(); } } }, e.c + (e.id === undefined ? '' : ' / ' + e.id)),
      e.record === undefined && el('span', { class: 'bad' }, 'deleted'),
      // A deletion is put back from: to the record as it was just before. Any other write is put back to
      revert && el('button', { class: 'danger', on: { click: () => putBack(e, e.record === undefined ? e.v - 1 : e.v) } }, e.record === undefined ? 'Restore' : 'Revert to this')),
    e.changes.length ? e.changes.map(change => changed(change, e.was)) : el('span', { class: 'dim' }, 'as it stood when the history began'),
    folded('The record as this write left it', e.record));
}

const putBack = tried(async (e, v) => {
  const what = e.c + (e.id === undefined ? '' : ' / ' + e.id);
  // What the record was then is in the entry: as the write left it, or for a deletion as it was just before
  const restoring = e.record === undefined;
  const target = restoring ? e.was : e.record;
  // Of the store the entry was shown in: the page may be elsewhere by the time the answer is given
  const store = state.store;
  const now = (await ask('record', { store, c: e.c, id: e.id ?? undefined })).record;
  const differ = differences(now, target);
  const body = [
    el('p', {}, 'Every client of the store sees the change, and it is told in the history as yours.'),
    differ.length
      ? differ.map(({ path, old, now: next }) => el('div', { class: 'change' },
        el('div', { class: 'at' }, (next === undefined ? 'delete ' : old === undefined ? 'make ' : 'set ') + (path.join(' / ') || 'the record')),
        old !== undefined && side('-', old), next !== undefined && side('+', next)))
      : el('p', { class: 'dim' }, 'The record is as it was then already: nothing will be written.'),
    restoring && now === undefined && el('p', { class: 'dim' }, 'The records its deletion took with it come back too.')
  ];
  if (!await sure((restoring ? 'Restore ' : 'Revert ') + what + ' to version ' + v + '?', body, restoring ? 'Restore' : 'Revert')) return;
  await post('revert', { store, c: e.c, id: e.id, v });
  if (state.store !== store) return;
  state.c = e.c;
  state.id = e.id ?? null;
  state.tab = 'records';
  await show();
});

// --- Activity ------------------------------------------------------------------------------

async function activity(fresh) {
  const o = state.overview;
  const body = el('div', {});
  const f = state.filters;
  const user = el('input', { placeholder: 'user key', value: f.user, size: 12, on: { change: () => { f.user = user.value; activity(true); } } });
  const collection = el('select', { on: { change: () => { f.collection = collection.value; activity(true); } } }, el('option', { value: '' }, 'every collection'), o.collections.map(({ c }) => el('option', { value: c, selected: c === f.collection }, c)));
  const deleted = el('input', { type: 'checkbox', checked: f.deleted, on: { change: () => { f.deleted = deleted.checked; activity(true); } } });
  const live = el('input', { type: 'checkbox', checked: state.live, on: { change: () => { state.live = live.checked; } } });
  $('main').replaceChildren(el('section', {},
    el('h2', {}, 'Activity'),
    el('div', { class: 'row' }, user, collection, el('label', {}, deleted, ' deletions only'), el('label', {}, live, ' live')),
    body));
  if (!o.history) return body.append(el('p', { class: 'dim' }, 'This store keeps no history. Make it with history: true to see what is written to it.'));
  if (fresh) {
    const { entries, version } = await ask('activity', { store: state.store, user: f.user, collection: f.collection, deleted: f.deleted });
    state.feed = entries;
    // Where the feed has read to: its newest entry, or the end of the store's last write where there is none
    state.feedAt = entries.length ? { v: entries[0].v, n: entries.filter(e => e.v === entries[0].v).reduce((most, e) => Math.max(most, e.n), 0) } : { v: version, n: Number.MAX_SAFE_INTEGER };
  }
  const older = el('button', { on: { click: tried(async () => {
    const last = state.feed[state.feed.length - 1];
    const { entries } = await ask('activity', { store: state.store, user: f.user, collection: f.collection, deleted: f.deleted, cursor: last && last.v + '.' + last.n });
    state.feed.push(...entries);
    activity(false);
  }) } }, 'Older');
  body.append(...state.feed.map(e => entry(e, { where: true, revert: true })), state.feed.length ? older : el('p', { class: 'dim' }, 'Nothing has been written that matches.'));
  feedBody = body;
}

// What has landed since the page last looked, oldest first, put at the top
// How many entries the feed shows: older ones go as newer ones come, from the page as from its memory
const FEED_MOST = 500;
const follow = tried(async () => {
  if (!state.store || state.tab !== 'activity' || !state.live || !state.overview?.history || !state.feedAt || document.hidden) return;
  const f = state.filters;
  const asked = { store: state.store, user: f.user, collection: f.collection, deleted: f.deleted };
  const { entries } = await ask('activity', { ...asked, cursor: state.feedAt.v + '.' + state.feedAt.n, oldestFirst: true, limit: 200 });
  // The page has gone elsewhere, or is narrowed otherwise, since this was asked: it is not what is shown
  if (state.tab !== 'activity' || asked.store !== state.store || asked.user !== f.user || asked.collection !== f.collection || asked.deleted !== f.deleted) return;
  if (!entries.length) return;
  // Read on from the last row, not the last version: a write of several records may be half in this page
  const last = entries[entries.length - 1];
  const before = state.feedAt.v;
  state.feedAt = { v: last.v, n: last.n };
  // Newest first, as the rest: by version, and within a write in the order it wrote
  // The rest of a write the last page ended inside goes beneath what is shown of it, the newer writes above
  const rest = entries.filter(e => e.v === before).sort((a, b) => a.n - b.n);
  const fresh = entries.filter(e => e.v !== before).sort((a, b) => b.v - a.v || a.n - b.n);
  const empty = state.feed.length === 0;
  let begun = 0;
  while (begun < state.feed.length && state.feed[begun].v === before) begun++;
  state.feed.splice(begun, 0, ...rest);
  state.feed.unshift(...fresh);
  state.feed.length = Math.min(state.feed.length, FEED_MOST);
  // Put above what is shown, which stays as it is: an entry someone has open is not shut under them
  if (empty || !feedBody?.isConnected) return activity(false);
  const made = list => list.map(e => entry(e, { where: true, revert: true }));
  const after = feedBody.querySelectorAll(':scope > .entry')[begun - 1];
  if (after) after.after(...made(rest));
  else feedBody.prepend(...made(rest));
  feedBody.prepend(...made(fresh));
  const shown = feedBody.querySelectorAll(':scope > .entry');
  for (let i = FEED_MOST; i < shown.length; i++) shown[i].remove();
});

// The line at the top, whatever is shown beneath it
// A server that is not answering is asked again after a while, a longer while each time, and the page says so;
// when it answers, the page is shown afresh
const down = { failures: 0, until: 0 };
function unreached() {
  down.failures++;
  const wait = Math.min(30000, 2000 * 2 ** down.failures);
  down.until = Date.now() + wait;
  $('offline').hidden = false;
  $('offline').textContent = 'The server is not answering. What is shown is as it was; asking again in ' + Math.round(wait / 1000) + ' s.';
}
async function reached() {
  if (!down.failures) return;
  down.failures = 0;
  down.until = 0;
  $('offline').hidden = true;
  await show();
}

// When the collections were last counted: on a store that is written all the time, not at every look
let counted = 0;
let uncounted = false;
const glance = async () => {
  if (!state.store || document.hidden) return false;
  const store = state.store;
  try {
    const g = await ask('glance', { store });
    await reached();
    if (store !== state.store || !state.overview) return true;
    const written = g.version !== state.overview.version;
    Object.assign(state.overview, g);
    top();
    for (const node of document.querySelectorAll('.ago[data-at]')) node.textContent = ago(Number(node.dataset.at));
    // The store has been written since the page last looked. The record that is shown, when it is not what it
    // was; and how many records there are, which costs the server a look at every record's key, and is asked
    // for no more often than every ten seconds: until then the count is of a moment ago
    if (written) uncounted = true;
    const l = looking;
    if (written && state.tab === 'records' && l?.into.isConnected && l.store === store && l.c === state.c && l.id === state.id) {
      const r = await ask('record', { store, c: l.c, id: l.id });
      if (JSON.stringify(r.record) !== l.json && looking === l && store === state.store) await record(l.into, true);
    }
    if (!uncounted || Date.now() - counted < 10000 || store !== state.store) return true;
    counted = Date.now();
    uncounted = false;
    const o = await ask('store', { store });
    if (store !== state.store) return true;
    state.overview = o;
    top();
    for (const node of document.querySelectorAll('li.pick[data-c]')) {
      const found = o.collections.find(({ c }) => c === node.dataset.c);
      if (found && !found.singleton) node.lastElementChild.textContent = String(found.records);
    }
    return true;
  } catch (err) {
    if (err.unreached) unreached();
    return false;
  }
};

// --- Sessions ------------------------------------------------------------------------------

const post = async (name, body) => {
  const response = await fetch(BASE + '/api/' + name, { method: 'POST', headers: { 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: JSON.stringify(body) });
  const answer = await response.json().catch(() => ({ error: response.statusText }));
  if (!response.ok) throw new Error(answer.error || response.statusText);
  return answer;
};
const lasted = ms => (ms < 1000 ? ms + ' ms' : ms < 60000 ? Math.round(ms / 1000) + ' s' : ms < 3600000 ? Math.round(ms / 60000) + ' min' : Math.round(ms / 3600000) + ' h');

const hangUp = tried(async key => {
  if (!await sure('Close the sockets of ' + key + '?', el('p', {}, 'Its clients reconnect at once, and are let in again only if the server still authenticates them.'), 'Disconnect')) return;
  await post('disconnect', { key });
  await show();
});

// Who is connected and the server's sockets may be thousands: each is asked for a hundred at a time, narrowed
// by what is typed, and says how many there are in all
const seen = { q: '', everyStore: false };
async function sessions() {
  const o = state.overview;
  const [people, wires] = await Promise.all([
    ask('peers', { store: state.store, q: seen.q }),
    ask('sockets', { store: seen.everyStore ? undefined : state.store, q: seen.q })
  ]);
  const of = (shown, found, total, what) => (shown === total ? total + ' ' + what : shown + ' of ' + (found === total ? total : found + ' that match, of ' + total) + ' ' + what);
  const find = el('input', { type: 'search', placeholder: 'find a user', value: seen.q, on: { change: () => { seen.q = find.value; show(); } } });
  const every = el('input', { type: 'checkbox', checked: seen.everyStore, on: { change: () => { seen.everyStore = every.checked; show(); } } });
  const socketRows = wires.sockets.map(w => el('tr', {},
    el('td', { class: 'mono' }, w.relay ? 'relay ' + w.key : w.key ?? '(no user)'),
    el('td', { class: 'mono' }, w.stores.join(', ') + (w.moreStores ? ' and ' + w.moreStores + ' more' : '')),
    el('td', {}, lasted(w.openMs)),
    el('td', {}, lasted(w.idleMs)),
    el('td', { class: w.buffered > 65536 ? 'bad' : '' }, w.buffered + ' B'),
    el('td', {}, w.expiresAt === null ? '' : when(w.expiresAt)),
    el('td', {}, wires.disconnects && !w.relay && w.key !== null && el('button', { class: 'danger', on: { click: () => hangUp(w.key) } }, 'Disconnect'))));
  const rows = people.peers.map(p => el('tr', {}, el('td', { class: 'mono' }, p.replicaId), el('td', { class: 'mono' }, p.key ?? ''), el('td', { class: 'mono' }, brief(p.user, 120)), el('td', { class: 'mono' }, brief(p.data, 120))));
  $('main').replaceChildren(el('section', {},
    el('h2', {}, 'Sessions'),
    el('p', {}, people.sessions + ' sessions, ' + people.replicas + ' replicas remembered. Epoch ' + people.epoch + '.'),
    el('div', { class: 'row' }, find, el('label', {}, every, ' sockets of every store')),
    people.total
      ? el('div', {}, el('p', { class: 'dim' }, of(people.peers.length, people.found, people.total, 'connected')), el('table', {}, el('thead', {}, el('tr', {}, ['Replica', 'User key', 'User', 'Shares'].map(h => el('th', {}, h)))), el('tbody', {}, rows)))
      : el('p', { class: 'dim' }, people.sessions ? 'Presence is off on this store: who is connected is not told. Make it with presence: true.' : 'Nobody is connected.'),
    el('h2', {}, seen.everyStore ? 'Sockets, of every store' : 'Sockets with this store open'),
    !wires.listed
      ? el('p', { class: 'dim' }, 'This server does not tell the inspector of its sockets.')
      : wires.total
        ? el('div', {}, el('p', { class: 'dim' }, of(wires.sockets.length, wires.found, wires.total, 'sockets') + (wires.sockets.length < wires.found ? ', the furthest behind first' : '')), wires.sockets.length ? el('table', {}, el('thead', {}, el('tr', {}, ['User key', 'Stores', 'Open', 'Quiet', 'Unsent', 'Session ends', ''].map(h => el('th', {}, h)))), el('tbody', {}, socketRows)) : null)
        : el('p', { class: 'dim' }, 'No socket is open.'),
    el('h2', {}, 'Sent, by message'),
    el('table', {}, el('thead', {}, el('tr', {}, ['Message', 'Count', 'Bytes'].map(h => el('th', {}, h)))), el('tbody', {}, Object.entries(people.sent).map(([type, { messages, bytes }]) => el('tr', {}, el('td', { class: 'mono' }, type), el('td', {}, String(messages)), el('td', {}, String(bytes))))))));
}

// The address changed under the page (back, forward, a link, a hash typed in): it goes where the address says.
// A browser may say so twice, and says so of an address the page itself has just set: only one that is not where
// the page is already is acted on
const moved = () => {
  if (location.hash === placed()) return;
  unasked();
  placeFrom();
  show();
};
addEventListener('hashchange', moved);
addEventListener('popstate', moved);
placeFrom();
$('stores').addEventListener('change', () => pick($('stores').value));
$('storeId').addEventListener('change', () => pick($('storeId').value.trim()));
// One look at a time: a server that takes longer than the two seconds to answer is not asked again meanwhile
let lookingNow = false;
setInterval(async () => {
  if (lookingNow || Date.now() < down.until) return;
  lookingNow = true;
  try {
    if (await glance()) await follow();
  } finally {
    lookingNow = false;
  }
}, 2000);
loadStores();
</script>
</body>
</html>
`;
