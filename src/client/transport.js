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
 * @param {{ WebSocket?: typeof WebSocket }} [options]
 */
export function webSocketTransport(url, { WebSocket: WS = globalThis.WebSocket } = {}) {
  if (typeof WS !== 'function') throw new TypeError('webSocketTransport: no WebSocket implementation available');
  return () => {
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
}
