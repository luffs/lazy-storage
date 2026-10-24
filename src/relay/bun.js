// relay/bun.js - A relay (see index.js), served by Bun
//
// `createRelayHandlers` mounts a relay in a Bun.serve of your own, as the
// server's `createHandlers` mounts a hub: call `upgrade` from your fetch
// and pass `websocket` through. For every socket it asks your
// `credential(req)` what the server should judge the client by (the
// headers and query its own socket would carry; the relay never reads
// them) and your `key(req)` for a fingerprint of it, by which the relay
// knows the client while the server is away. `upstreamSocket(url, {
// headers })` is a transport factory on a WebSocket that takes headers
// (Bun's does): what the relay's `link` dials the server's relay route
// with, under the relay's own credential.
//
//   const relay = createRelay({
//     shapeOf: storeId => shapes[storeId] ?? null,
//     storage: fileCopies('./copies'),
//     link: upstreamSocket('wss://central.example/sync/relay', { headers: { authorization: `Bearer ${relayToken}` } })
//   });
//   const handlers = createRelayHandlers({
//     relay,
//     path: '/sync',
//     key: req => sha256(req.headers.get('authorization') ?? ''),
//     credential: req => ({ headers: { authorization: req.headers.get('authorization') ?? '' }, query: new URL(req.url).search })
//   });
//   Bun.serve({ port: 36610, fetch: async (req, server) => (await handlers.upgrade(req, server)) ?? new Response('Not found', { status: 404 }), websocket: handlers.websocket });
import { LazyWatch } from 'lazy-watch';
import { toJSON, deflateOptions, TOO_FAR_BEHIND } from '../server/wire.js';

const { isPlainObject } = LazyWatch.Utils;
const decoder = new TextDecoder();
const textOf = data => (typeof data === 'string' ? data : decoder.decode(data));

/**
 * @param {Object} options
 * @param {Object} options.relay - from createRelay
 * @param {(req: Request) => ({ headers?: Record<string, string>, query?: string }|null|Promise<Object|null>)} [options.credential]
 *   what the server judges the client by; without one, the client is
 *   vouched for by nothing and the server turns it away (unless it takes
 *   any credential)
 * @param {(req: Request) => string|null|Promise<string|null>} [options.key] - a fingerprint of the credential
 * @param {string} [options.path='/ws']
 * @param {number} [options.maxPayload=4194304]
 * @param {boolean|Object} [options.perMessageDeflate=true] - as the server's; the default threshold is 64 KB here
 * @param {number|false} [options.maxBuffered=16777216]
 * @param {(error: any) => void} [options.onError]
 */
export function createRelayHandlers({
  relay,
  credential,
  key,
  path = '/ws',
  maxPayload = 4 * 1024 * 1024,
  perMessageDeflate = true,
  maxBuffered = 16 * 1024 * 1024,
  onError = err => console.error('lazy-storage relay:', err)
} = {}) {
  if (!relay || typeof relay.session !== 'function') throw new TypeError('createRelayHandlers needs a relay (from createRelay)');
  if (key !== undefined && typeof key !== 'function') throw new TypeError('key must be a function of the request');
  if (credential !== undefined && typeof credential !== 'function') throw new TypeError('credential must be a function of the request');
  const deflate = deflateOptions(perMessageDeflate, 64 * 1024);
  const open = new Map();
  let closing = false;
  const queued = new Map();
  const gone = new WeakSet();
  let flushing = null;

  function write(ws, messages) {
    if (maxBuffered && ws.getBufferedAmount() > maxBuffered) return void ws.close(...TOO_FAR_BEHIND);
    try {
      if (messages.length === 1) ws.send(messages[0][0], messages[0][1]);
      else ws.cork(() => { for (const [json, compress] of messages) ws.send(json, compress); });
    } catch (err) {
      onError(err);
    }
  }
  function flush() {
    if (flushing !== null) clearImmediate(flushing);
    flushing = null;
    const all = [...queued];
    queued.clear();
    for (const [ws, messages] of all) write(ws, messages);
  }
  const send = (ws, message) => {
    if (gone.has(ws)) return;
    const json = toJSON(message);
    const entry = [json, deflate !== null && json.length >= deflate.threshold];
    const waiting = queued.get(ws);
    if (waiting) waiting.push(entry);
    else queued.set(ws, [entry]);
    flushing ??= setImmediate(flush);
  };

  async function upgrade(req, server) {
    if (new URL(req.url).pathname !== path) return null;
    if (closing) return new Response('Relay shutting down', { status: 503, headers: { 'retry-after': '1' } });
    let fingerprint = null;
    let vouchBy = null;
    try {
      vouchBy = credential ? await credential(req) : null;
      fingerprint = key ? await key(req) : null;
    } catch (err) {
      onError(err);
      return new Response('Something went wrong', { status: 500 });
    }
    const data = { key: typeof fingerprint === 'string' ? fingerprint : JSON.stringify(vouchBy ?? null), credential: isPlainObject(vouchBy) ? vouchBy : {} };
    return server.upgrade(req, { data }) ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
  }

  const websocket = {
    maxPayloadLength: maxPayload,
    perMessageDeflate: deflate === null ? false : deflate.runtime,
    ...(maxBuffered ? { backpressureLimit: maxBuffered * 2 } : {}),
    open(ws) {
      try {
        const session = relay.session({
          send: message => send(ws, message),
          key: ws.data.key,
          credential: ws.data.credential,
          // The server took the client's access back: its socket closes, and it comes back judged afresh
          onEvict: () => {
            const waiting = queued.get(ws);
            if (waiting) {
              queued.delete(ws);
              write(ws, waiting);
            }
            ws.close(4001, 'Reauthenticate');
          }
        });
        open.set(ws, { session, opened: Date.now() });
      } catch (err) {
        onError(err);
        ws.close(1011, 'Something went wrong');
      }
    },
    message(ws, raw) {
      let message;
      try {
        message = JSON.parse(textOf(raw));
      } catch {
        return;
      }
      if (isPlainObject(message)) open.get(ws)?.session.receive(message);
    },
    close(ws) {
      gone.add(ws);
      queued.delete(ws);
      open.get(ws)?.session.close();
      open.delete(ws);
    }
  };

  /** Close this listener's sockets (code 1001) and take no more; the relay stays for the host to close */
  async function close({ reason = 'Relay shutting down' } = {}) {
    closing = true;
    flush();
    const sockets = [...open.keys()];
    const closed = Promise.all(sockets.map(ws => new Promise(resolve => {
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
    await Promise.race([closed, new Promise(resolve => setTimeout(resolve, 1000))]);
    for (const { session } of open.values()) session.close();
    open.clear();
  }

  function sockets() {
    const now = Date.now();
    return [...open].map(([ws, { opened }]) => ({ key: ws.data.key, buffered: ws.getBufferedAmount(), openMs: now - opened }));
  }

  return { upgrade, websocket, close, sockets };
}

/**
 * A transport factory (see client/transport.js) on a WebSocket that takes
 * headers, as Bun's and the `ws` package's do: what a relay's link dials
 * the server with, under the relay's own credential
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
      let message;
      try {
        message = JSON.parse(textOf(event.data));
      } catch {
        return;
      }
      t.onmessage?.(message);
    };
    let done = false;
    const closed = (code, reason) => {
      if (done) return;
      done = true;
      t.onclose?.({ code, reason });
    };
    socket.onclose = event => closed(event?.code, event?.reason);
    socket.onerror = () => { if (socket.readyState === 0) closed(1006, ''); };
    return t;
  };
}
