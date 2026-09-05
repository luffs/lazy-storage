// transport.js - Transport factories
//
// A transport factory returns a fresh connection object each time the
// client (re)connects: { send(message), close(), onopen, onmessage(message),
// onclose(info) }. Messages are objects on both sides; encoding is the
// transport's business. `info` is the WebSocket close code and reason
// where known ({ code, reason }); the connection reads the code that
// means the server turned the socket away. The in-memory transport used
// by the tests implements the same shape.

/**
 * A WebSocket transport with JSON messages.
 * @param {string|() => string} url - the socket URL, or a function producing
 *   it per connection (for tokens in the query string)
 * @param {{ WebSocket?: typeof WebSocket, fetch?: false | typeof fetch }} [options]
 *   `fetch` fetches a snapshot from the socket's server when the server
 *   points there instead of sending it (a large one; see the server's
 *   snapshot route). The default is the global fetch, called with the
 *   route resolved against the socket URL (ws to http) and carrying the
 *   socket URL's query, so a token there applies to both. Pass your own to
 *   add headers, or false to take every snapshot over the socket
 */
export function webSocketTransport(url, { WebSocket: WS = globalThis.WebSocket, fetch: fetchImpl = globalThis.fetch } = {}) {
  if (typeof WS !== 'function') throw new TypeError('webSocketTransport: no WebSocket implementation available');
  const factory = () => {
    const socket = new WS(typeof url === 'function' ? url() : url);
    const t = {
      onopen: null,
      onmessage: null,
      onclose: null,
      send(message) {
        if (socket.readyState === 1) socket.send(JSON.stringify(message));
      },
      close() {
        socket.close();
      }
    };
    socket.onopen = () => t.onopen?.();
    socket.onmessage = event => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      t.onmessage?.(msg);
    };
    let done = false;
    const closed = (code, reason) => {
      if (done) return;
      done = true;
      t.onclose?.({ code, reason });
    };
    socket.onclose = event => closed(event?.code, event?.reason);
    // Node 22's WebSocket (undici 6) fires 'error' for a failed handshake
    // with no 'close' after it, and the socket stays CONNECTING; the close
    // is reported here so the retry goes on. Where 'close' does follow the
    // error, as in browsers and Node 24+, it is deduplicated
    socket.onerror = () => { if (socket.readyState === 0) closed(1006, ''); };
    return t;
  };
  if (typeof fetchImpl === 'function') {
    /** Fetch a path from the socket's server: same host and credentials, over http(s) */
    factory.fetch = path => {
      const socket = new URL(typeof url === 'function' ? url() : url, globalThis.location?.href);
      const target = new URL(path, socket);
      target.protocol = socket.protocol === 'wss:' ? 'https:' : socket.protocol === 'ws:' ? 'http:' : socket.protocol;
      if (!target.search) target.search = socket.search;
      return fetchImpl(target.href);
    };
  }
  return factory;
}
