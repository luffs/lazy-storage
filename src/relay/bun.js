// relay/bun.js - A relay on the LAN, served by Bun
//
// `createRelayHandlers` mounts a relay (see index.js) in a Bun.serve of
// your own, as the server's `createHandlers` mounts a hub: call `upgrade`
// from your fetch and pass `websocket` through. For every socket it asks
// your `key(req)` for a fingerprint of the request's credential (a hash of
// its Authorization header, say; the relay never reads the credential) and
// your `upstream(req)` for a transport factory that dials the server with
// that credential, and the same path and query. `upstreamSocket(url,
// { headers })` is such a factory, on a WebSocket that takes headers
// (Bun's does). Frames are kept as they came both ways, so what the relay
// passes through goes on byte for byte, never decoded and encoded again:
//
//   const relay = createRelay({ storage: fileCopies('./copies') });
//   const handlers = createRelayHandlers({
//     relay,
//     path: '/sync',
//     key: req => sha256(req.headers.get('authorization') ?? ''),
//     upstream: req => upstreamSocket(`wss://central.example${'/sync' + new URL(req.url).search}`, {
//       headers: { authorization: req.headers.get('authorization') ?? '' }
//     })
//   });
//   Bun.serve({ port: 36610, fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }), websocket: handlers.websocket });
import { LazyWatch } from 'lazy-watch';
import { toJSON, presetJSON, deflateOptions, TOO_FAR_BEHIND } from '../server/wire.js';

const { isPlainObject } = LazyWatch.Utils;
const decoder = new TextDecoder();
const textOf = data => (typeof data === 'string' ? data : decoder.decode(data));

/**
 * A frame parsed, and the text it came as remembered on it (see wire.js),
 * so that sending the object on sends that text; undefined for one that is
 * not JSON
 */
function parseFrame(data) {
  const text = textOf(data);
  let message;
  try {
    message = JSON.parse(text);
  } catch {
    return undefined;
  }
  return isPlainObject(message) ? presetJSON(message, text) : message;
}

/**
 * @param {Object} options
 * @param {Object} options.relay - from createRelay
 * @param {(req: Request) => (() => Object)} options.upstream - a transport
 *   factory that dials the server for this request's client, with its
 *   credential (see upstreamSocket)
 * @param {(req: Request) => string|null|Promise<string|null>} [options.key] -
 *   a fingerprint of the request's credential: what the relay knows the
 *   client by while the server is away. Without one (or null) the client
 *   is passed through and never answered offline
 * @param {string} [options.path='/ws'] - the WebSocket path
 * @param {number} [options.maxPayload=4194304] - the largest message (bytes)
 *   a client may send; a hello carries at most 1000 ops
 * @param {boolean|Object} [options.perMessageDeflate=true] - as the server's
 *   (see server/bun.js): messages of `threshold` bytes or more (default
 *   1024) go compressed to a client that takes it
 * @param {number|false} [options.maxBuffered=16777216] - close a socket
 *   whose unsent output passes this many bytes (code 1013); it reconnects
 * @param {(error: any) => void} [options.onError] - default console
 * @returns {{ upgrade(req: Request, server: any): Promise<Response|undefined|null>, websocket: Object, close(options?: { reason?: string }): Promise<void>, sockets(): Array<Object> }}
 *   `upgrade` resolves to null when the URL is not the path, undefined
 *   after an upgrade, or a Response; `close` closes this listener's
 *   sockets (code 1001), and the relay stays for the host to close;
 *   `sockets()` lists them: `{ key, state, buffered, openMs }`
 */
export function createRelayHandlers({
  relay,
  upstream,
  key,
  path = '/ws',
  maxPayload = 4 * 1024 * 1024,
  perMessageDeflate = true,
  maxBuffered = 16 * 1024 * 1024,
  onError = err => console.error('lazy-storage relay:', err)
} = {}) {
  if (!relay || typeof relay.accept !== 'function') throw new TypeError('createRelayHandlers needs a relay (from createRelay)');
  if (typeof upstream !== 'function') throw new TypeError('createRelayHandlers needs upstream: a function of the request giving a transport factory');
  if (key !== undefined && typeof key !== 'function') throw new TypeError('key must be a function of the request');
  const deflate = deflateOptions(perMessageDeflate);
  const open = new Map();   // socket -> { session, opened }
  let closing = false;

  // Every message is encoded once (a frame passed through, never): sent
  // compressed when it is large enough, and a socket too far behind is closed
  const send = (ws, message) => {
    if (maxBuffered && ws.getBufferedAmount() > maxBuffered) return void ws.close(...TOO_FAR_BEHIND);
    const json = toJSON(message);
    ws.send(json, deflate !== null && json.length >= deflate.threshold);
  };

  async function upgrade(req, server) {
    if (new URL(req.url).pathname !== path) return null;
    if (closing) return new Response('Relay shutting down', { status: 503, headers: { 'retry-after': '1' } });
    let fingerprint = null;
    let dial;
    try {
      fingerprint = key ? await key(req) : null;
      dial = upstream(req);
      if (typeof dial !== 'function') throw new TypeError('upstream(req) must give a transport factory');
    } catch (err) {
      onError(err);
      return new Response('Something went wrong', { status: 500 });
    }
    return server.upgrade(req, { data: { key: typeof fingerprint === 'string' ? fingerprint : null, upstream: dial } })
      ? undefined
      : new Response('WebSocket upgrade failed', { status: 400 });
  }

  const websocket = {
    maxPayloadLength: maxPayload,
    perMessageDeflate: deflate === null ? false : deflate.runtime,
    ...(maxBuffered ? { backpressureLimit: maxBuffered * 2 } : {}),
    open(ws) {
      try {
        const session = relay.accept({
          send: message => send(ws, message),
          close: (code, reason) => ws.close(code, reason),
          key: ws.data.key,
          upstream: ws.data.upstream
        });
        open.set(ws, { session, opened: Date.now() });
      } catch (err) {
        onError(err);
        ws.close(1011, 'Something went wrong');
      }
    },
    message(ws, raw) {
      const message = parseFrame(raw);
      if (isPlainObject(message)) open.get(ws)?.session.receive(message);
    },
    close(ws) {
      open.get(ws)?.session.close();
      open.delete(ws);
    }
  };

  /** Close this listener's sockets (code 1001) and take no more; resolves once they have closed, a second at most */
  async function close({ reason = 'Relay shutting down' } = {}) {
    closing = true;
    const sockets = [...open.keys()];
    const gone = Promise.all(sockets.map(ws => new Promise(resolve => {
      const entry = open.get(ws);
      open.set(ws, { ...entry, session: { receive() {}, close() { entry.session.close(); resolve(); } } });
    })));
    for (const ws of sockets) {
      try {
        ws.close(1001, reason);
      } catch (err) {
        onError(err);
      }
    }
    await Promise.race([gone, new Promise(resolve => setTimeout(resolve, 1000))]);
    for (const { session } of open.values()) session.close();
    open.clear();
  }

  function sockets() {
    const now = Date.now();
    return [...open].map(([ws, { session, opened }]) => ({ key: ws.data.key, state: session.state, buffered: ws.getBufferedAmount(), openMs: now - opened }));
  }

  return { upgrade, websocket, close, sockets };
}

/**
 * A transport factory (see client/transport.js) on a WebSocket that takes
 * headers, as Bun's and the `ws` package's do: what a relay dials the
 * server with, the client's credentials in the headers. A frame that
 * comes is parsed with its text remembered, and a message sent that
 * carries one goes as that text, so a relay passes frames on unchanged.
 * Snapshots always come inline (the relay asks for them so); there is no
 * `fetch`
 * @param {string|(() => string)} url - the server's socket URL, or a function giving it per dial
 * @param {{ headers?: Record<string, string>|(() => Record<string, string>), WebSocket?: any }} [options]
 */
export function upstreamSocket(url, { headers, WebSocket: WS = globalThis.WebSocket } = {}) {
  if (typeof WS !== 'function') throw new TypeError('upstreamSocket: no WebSocket implementation available');
  return () => {
    const target = typeof url === 'function' ? url() : url;
    const extra = typeof headers === 'function' ? headers() : headers;
    const socket = extra ? new WS(target, { headers: extra }) : new WS(target);
    const t = {
      onopen: null,
      onmessage: null,
      onclose: null,
      send(message) {
        if (socket.readyState === 1) socket.send(toJSON(message));
      },
      close() {
        try {
          socket.close();
        } catch { /* closing a socket that never opened can throw; it is gone either way */ }
      }
    };
    socket.onopen = () => t.onopen?.();
    socket.onmessage = event => {
      const message = parseFrame(event.data);
      if (message !== undefined) t.onmessage?.(message);
    };
    let done = false;
    const closed = (code, reason) => {
      if (done) return;
      done = true;
      t.onclose?.({ code, reason });
    };
    socket.onclose = event => closed(event?.code, event?.reason);
    // A handshake that failed may report an error and no close (see client/transport.js)
    socket.onerror = () => { if (socket.readyState === 0) closed(1006, ''); };
    return t;
  };
}
