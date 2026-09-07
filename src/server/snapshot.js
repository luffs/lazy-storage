// snapshot.js - A store's snapshot over HTTP
//
// A hello answered with a large snapshot costs the wire a compression of
// the whole state per socket (some 5 ms per megabyte) and a copy of it
// into each socket's send buffer. A client that can fetch says so in its
// hello, and a transport that serves this route answers such a hello with
// where to fetch the snapshot instead of the state (see the store's
// snapshotMessage): `GET <path>/snapshot/<store id>` serves the state with
// the version and epoch it is at, compressed once per change (brotli, or
// gzip for a client without it) and kept until the next, with an ETag so
// a reload that finds the store unchanged is answered 304. The rest of
// the protocol is untouched: patches keep flowing on the socket, and the
// client lays the ones newer than the snapshot it fetched on top of it.
import { gzipSync, brotliCompressSync, constants } from 'node:zlib';
import { isStoreId } from './registry.js';
import { SNAPSHOT_THRESHOLD } from './wire.js';

// Brotli at quality 5 packs this JSON some 15% tighter than gzip in the
// same time (a 1 MB snapshot in about 10 ms); the higher qualities buy a
// little more for much longer. Paid once per change, so the ratio is what
// counts, but not at the price of blocking the loop for a busy store
const BROTLI_QUALITY = 5;

/** Which of brotli and gzip an Accept-Encoding header takes, brotli first; null for neither */
function encodingFor(header) {
  if (typeof header !== 'string') return null;
  const accepted = new Set();
  for (const part of header.split(',')) {
    const [name, ...params] = part.trim().split(';');
    const q = params.map(p => p.trim()).find(p => p.startsWith('q='));
    if (name && (q === undefined || Number(q.slice(2)) > 0)) accepted.add(name.toLowerCase());
  }
  if (accepted.has('br')) return 'br';
  if (accepted.has('gzip')) return 'gzip';
  return null;
}

const compress = {
  br: plain => brotliCompressSync(plain, {
    params: {
      [constants.BROTLI_PARAM_QUALITY]: BROTLI_QUALITY,
      [constants.BROTLI_PARAM_MODE]: constants.BROTLI_MODE_TEXT,
      [constants.BROTLI_PARAM_SIZE_HINT]: plain.length
    }
  }),
  gzip: plain => gzipSync(plain)
};

/** The adapters' `httpSnapshots` option normalized: `{ threshold, origins }`, or null when off */
export function snapshotOptions(httpSnapshots) {
  if (!httpSnapshots) return null;
  const { threshold = SNAPSHOT_THRESHOLD, origins = '*' } = httpSnapshots === true ? {} : httpSnapshots;
  if (!(threshold >= 0)) throw new TypeError('httpSnapshots.threshold must be a number of bytes');
  if (origins !== '*' && origins !== false && !(Array.isArray(origins) && origins.every(o => typeof o === 'string'))) {
    throw new TypeError("httpSnapshots.origins must be '*', false, or an array of origins");
  }
  return { threshold, origins };
}

/**
 * The CORS header for a snapshot response, by the `origins` option: '*'
 * lets any origin ask (the default: credentials travel in the URL as they
 * do for the socket, and a browser withholds cookies from a cross-origin
 * request under '*', which keeps a cookie session same-origin); an array
 * echoes a listed origin and no other; false sends no header, so only the
 * page's own origin may fetch
 */
function corsHeaders(request, origins) {
  if (origins === '*') return { 'access-control-allow-origin': '*' };
  if (!origins) return {};
  const origin = request.headers.get('origin');
  return origin && origins.includes(origin) ? { 'access-control-allow-origin': origin, vary: 'accept-encoding, origin' } : { vary: 'accept-encoding, origin' };
}

/** The store id a request path names under `<path>/snapshot/`, or null when the path is not that route */
export function snapshotId(pathname, path) {
  const prefix = `${path}/snapshot/`;
  if (!pathname.startsWith(prefix)) return null;
  try {
    return decodeURIComponent(pathname.slice(prefix.length));
  } catch {
    return '';
  }
}

// The document per store, kept as long as the state's JSON is the same
// string (the store encodes it once per change): plain, and each encoding
// on first demand
const bodies = new WeakMap();

function snapshotBody(store) {
  const json = store.snapshotJSON();
  let body = bodies.get(store);
  if (!body || body.json !== json || body.v !== store.version) {
    body = { json, v: store.version, epoch: store.epoch, plain: `{"v":${store.version},"epoch":${JSON.stringify(store.epoch)},"state":${json}}`, br: null, gzip: null };
    bodies.set(store, body);
  }
  return body;
}

function matchesETag(header, etag) {
  return typeof header === 'string' && header.split(',').some(tag => tag.trim().replace(/^W\//, '') === etag);
}

/**
 * The response to a request for a store's snapshot: `{ v, epoch, state }`
 * as JSON, brotli or gzip as the request accepts (brotli first), with an
 * ETag of the epoch and version and a 304 when If-None-Match carries it.
 * Authentication and authorization are the caller's (the adapters do
 * both; see serveSnapshot).
 * @param {Object} store
 * @param {Request} request
 * @param {{ origins?: '*' | false | string[] }} [options] - which origins
 *   may fetch cross-origin (see corsHeaders); default '*'
 * @returns {Response}
 */
export function snapshotResponse(store, request, { origins = '*' } = {}) {
  const body = snapshotBody(store);
  const etag = `"${body.epoch}:${body.v}"`;
  const headers = {
    etag,
    'cache-control': 'private, no-cache',   // kept, and revalidated: unchanged is a 304
    vary: 'accept-encoding',
    ...corsHeaders(request, origins)
  };
  if (matchesETag(request.headers.get('if-none-match'), etag)) return new Response(null, { status: 304, headers });
  headers['content-type'] = 'application/json';
  let payload = body.plain;
  const encoding = encodingFor(request.headers.get('accept-encoding'));
  if (encoding) {
    headers['content-encoding'] = encoding;
    payload = body[encoding] ??= compress[encoding](body.plain);
  }
  return new Response(request.method === 'HEAD' ? null : payload, { status: 200, headers });
}

/**
 * Serve the snapshot route as the adapters do: the request is authenticated
 * and authorized like an upgrade (401, 403), the store resolved (404), and
 * the snapshot answered (see snapshotResponse). GET and HEAD only (405).
 * @param {Request} request
 * @param {string} id - the store id the path named
 * @param {{ resolveStore: (id: string) => Object|null, authenticate?: Function, authorize?: Function, onError?: Function, origins?: '*' | false | string[] }} options
 * @returns {Promise<Response>}
 */
export async function serveSnapshot(request, id, { resolveStore, authenticate, authorize, onError, origins = '*' }) {
  if (request.method !== 'GET' && request.method !== 'HEAD') return new Response('Method not allowed', { status: 405, headers: { allow: 'GET, HEAD' } });
  let user;
  if (authenticate) {
    user = await authenticate(request);
    if (user === null || user === undefined) return new Response('Unauthorized', { status: 401 });
  }
  if (!isStoreId(id)) return new Response('Not found', { status: 404 });
  let store;
  try {
    store = resolveStore(id);
  } catch (err) {
    onError?.(err);
    return new Response('Not found', { status: 404 });
  }
  if (!store) return new Response('Not found', { status: 404 });
  if (authorize) {
    let allowed;
    try {
      allowed = await authorize(user, id, store);
    } catch {
      allowed = false;
    }
    if (!allowed) return new Response('Forbidden', { status: 403 });
  }
  return snapshotResponse(store, request, { origins });
}
