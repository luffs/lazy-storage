// relay-writes.test.js - The relay's Bun handlers write what goes to a socket in one turn as one write
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRelayHandlers } from '../src/relay/bun.js';
import { deflateOptions } from '../src/server/wire.js';

const turn = () => new Promise(resolve => setImmediate(resolve));

/** Handlers over a relay that hands each socket's send and close to the test, and a socket that records its writes */
function setUp(options = {}) {
  const accepted = [];
  const relay = {
    accept(socket) {
      accepted.push(socket);
      return { receive() {}, close() {}, state: 'fan' };
    }
  };
  const errors = [];
  const handlers = createRelayHandlers({ relay, upstream: () => () => ({}), perMessageDeflate: { threshold: 100 }, onError: err => errors.push(err), ...options });
  const socket = () => {
    const ws = {
      data: { key: null, upstream: () => ({}) },
      writes: [],
      buffered: 0,
      closed: null,
      getBufferedAmount: () => ws.buffered,
      send(json, compress) {
        if (ws.corked) ws.corked.push([json, compress]);
        else ws.writes.push([[json, compress]]);
      },
      cork(fn) {
        ws.corked = [];
        fn();
        ws.writes.push(ws.corked);
        ws.corked = null;
      },
      close(code, reason) {
        ws.closed = [code, reason];
      }
    };
    handlers.websocket.open(ws);
    return { ws, relay: accepted.at(-1) };
  };
  return { handlers, socket, errors };
}

test('what goes to a socket in one turn goes out at its end as one write, in order; a lone message as a plain send', async () => {
  const { socket, errors } = setUp();
  const a = socket();
  const b = socket();
  a.relay.send({ t: 'patch', v: 1 });
  a.relay.send({ t: 'patch', v: 2, diff: { text: 'x'.repeat(200) } });
  a.relay.send({ t: 'ack', seq: 1 });
  b.relay.send({ t: 'patch', v: 1 });
  assert.deepEqual([a.ws.writes, b.ws.writes], [[], []], 'nothing before the turn ends');
  await turn();
  assert.equal(a.ws.writes.length, 1, 'one write for the three');
  assert.deepEqual(a.ws.writes[0].map(([json, compress]) => [JSON.parse(json).t, JSON.parse(json).v ?? null, compress]), [['patch', 1, false], ['patch', 2, true], ['ack', null, false]], 'in order, the large one compressed');
  assert.deepEqual(b.ws.writes, [[[JSON.stringify({ t: 'patch', v: 1 }), false]]]);
  a.relay.send({ t: 'patch', v: 3 });
  await turn();
  assert.equal(a.ws.writes.length, 2, 'the next turn, a write of its own');
  assert.deepEqual(errors, []);
});

test('what waits for a socket goes before the relay closes it; a socket gone takes nothing more', async () => {
  const { handlers, socket } = setUp();
  const a = socket();
  a.relay.send({ t: 'closed', code: 'unauthorized' });
  a.relay.close(4401, 'Unauthorized');
  assert.deepEqual(a.ws.writes.map(write => JSON.parse(write[0][0]).t), ['closed'], 'the message first');
  assert.deepEqual(a.ws.closed, [4401, 'Unauthorized']);

  const b = socket();
  b.relay.send({ t: 'patch', v: 1 });
  handlers.websocket.close(b.ws);
  b.relay.send({ t: 'patch', v: 2 });
  await turn();
  assert.deepEqual(b.ws.writes, [], 'queued for a socket that closed: dropped');
});

test('a socket too far behind is closed at its write, and the handlers\' close sends what waits first', async () => {
  const { handlers, socket } = setUp({ maxBuffered: 1000 });
  const a = socket();
  a.ws.buffered = 5000;
  a.relay.send({ t: 'patch', v: 1 });
  await turn();
  assert.deepEqual(a.ws.writes, []);
  assert.deepEqual(a.ws.closed, [1013, 'Too far behind']);

  const b = socket();
  b.relay.send({ t: 'patch', v: 1 });
  const closing = handlers.close();
  assert.equal(b.ws.writes.length, 1, 'sent as the handlers close');
  assert.equal(b.ws.closed[0], 1001);
  handlers.websocket.close(b.ws);
  handlers.websocket.close(a.ws);
  await closing;
});

/** A patch whose JSON is `chars` long */
function sized(chars) {
  const bare = JSON.stringify({ t: 'patch', v: 1, text: '' }).length;
  const message = { t: 'patch', v: 1, text: 'x'.repeat(chars - bare) };
  assert.equal(JSON.stringify(message).length, chars);
  return message;
}

/** Whether each message went compressed, sent in turns of their own */
async function compressed(socket, ...messages) {
  const a = socket();
  for (const message of messages) {
    a.relay.send(message);
    await turn();
  }
  return a.ws.writes.map(([[, compress]]) => compress);
}

test('a relay compresses a message of 64 KB or more by default, not the server\'s 1 KB: a patch goes plain, a snapshot compressed', async () => {
  for (const perMessageDeflate of [undefined, true, { compress: 'shared' }]) {
    const { handlers, socket, errors } = setUp({ perMessageDeflate });
    assert.deepEqual(handlers.websocket.perMessageDeflate, perMessageDeflate === undefined || perMessageDeflate === true ? true : { compress: 'shared' }, 'the runtime\'s own knobs are passed on, without the threshold');
    assert.deepEqual(await compressed(socket, sized(2 * 1024), sized(70 * 1024), sized(64 * 1024 - 1), sized(64 * 1024)), [false, true, false, true],
      `${JSON.stringify(perMessageDeflate)}: 2 KB plain, 70 KB compressed, the line at 64 KB`);
    assert.deepEqual(errors, []);
  }
  assert.equal(deflateOptions(true).threshold, 1024, 'the server\'s own default is as it was');
});

test('a relay\'s threshold given is honoured, and perMessageDeflate false sends everything plain', async () => {
  const given = setUp({ perMessageDeflate: { threshold: 100 } });
  assert.equal(given.handlers.websocket.perMessageDeflate, true);
  assert.deepEqual(await compressed(given.socket, sized(99), sized(100), sized(2 * 1024)), [false, true, true]);
  const zero = setUp({ perMessageDeflate: { threshold: 0 } });
  assert.deepEqual(await compressed(zero.socket, sized(40)), [true], 'a threshold of 0 compresses everything');
  const off = setUp({ perMessageDeflate: false });
  assert.equal(off.handlers.websocket.perMessageDeflate, false, 'the runtime is told not to take it');
  assert.deepEqual(await compressed(off.socket, sized(2 * 1024), sized(70 * 1024), sized(1024 * 1024)), [false, false, false]);
  assert.throws(() => setUp({ perMessageDeflate: { threshold: 'large' } }), /threshold must be a number of bytes/);
});
