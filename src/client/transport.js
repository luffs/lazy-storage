// transport.js - Transport factories
//
// A transport factory returns a fresh connection object each time the
// client (re)connects: { send(message), close(), onopen, onmessage(message),
// onclose }. Messages are objects on both sides; encoding is the
// transport's business. The in-memory transport used by the tests
// implements the same shape.

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
    socket.onclose = () => t.onclose?.();
    socket.onerror = () => { /* 'close' follows and drives the retry */ };
    return t;
  };
}
