// transport.test.js - The WebSocket transport against the runtimes' differing close semantics
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { webSocketTransport } from '../src/client/transport.js';

// A WebSocket whose script decides which events the runtime fires, and in
// which order
function fakeSocket(script) {
  return class {
    readyState = 0;
    constructor() { queueMicrotask(() => script(this)); }
    send() {}
    close() {}
  };
}

test('a failed handshake reports one close whether or not the runtime fires "close" after "error"', async () => {
  const runs = [
    { label: 'error only (Node 22, undici 6)', script: ws => { ws.onerror(new Event('error')); } },
    { label: 'error then close (browsers, Node 24+)', script: ws => { ws.onerror(new Event('error')); ws.readyState = 3; ws.onclose({ code: 1006 }); } },
    { label: 'close only', script: ws => { ws.readyState = 3; ws.onclose({ code: 1006 }); } }
  ];
  for (const { label, script } of runs) {
    const t = webSocketTransport('ws://nowhere/ws', { WebSocket: fakeSocket(script) })();
    let closes = 0;
    t.onclose = () => closes++;
    await new Promise(resolve => setTimeout(resolve, 0));
    assert.equal(closes, 1, label);
  }
});

test('an error on an open socket is left to the runtime, whose close follows', async () => {
  let socket;
  const WS = fakeSocket(ws => { socket = ws; ws.readyState = 1; ws.onopen(); ws.onerror(new Event('error')); });
  const t = webSocketTransport('ws://nowhere/ws', { WebSocket: WS })();
  const events = [];
  t.onopen = () => events.push('open');
  t.onclose = () => events.push('close');
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.deepEqual(events, ['open'], 'no close synthesized while the socket is open');
  socket.readyState = 3;
  socket.onclose({ code: 1009 });
  assert.deepEqual(events, ['open', 'close']);
});
