// connection.test.js - A connection the server turned away: no retry, the reason on the connection and every client, until connect()
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createConnection } from '../src/client/connection.js';
import { createClient } from '../src/client/index.js';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** A transport whose sockets the test drives by hand */
function fakeTransport() {
  const sockets = [];
  const factory = () => {
    const t = {
      onopen: null,
      onmessage: null,
      onclose: null,
      sent: [],
      send(message) { t.sent.push(message); },
      close() { queueMicrotask(() => t.onclose?.({ code: 1000, reason: '' })); }
    };
    sockets.push(t);
    return t;
  };
  return { factory, sockets, last: () => sockets[sockets.length - 1] };
}

test('a closed message without a store ends the socket: no retry, the reason on the connection and every client, until connect()', async () => {
  const { factory, sockets, last } = fakeTransport();
  const connection = createConnection({ transport: factory, reconnect: { min: 1, max: 2 }, keepalive: false });
  const a = createClient({ connection, store: 'a', initial: {} });
  const b = createClient({ connection, store: 'b', initial: {} });
  const events = [];
  connection.on('closed', c => events.push(['connection', c.code]));
  a.on('closed', c => events.push(['a', c.code]));
  b.on('closed', c => events.push(['b', c.code]));
  const statuses = [];
  a.on('status', s => statuses.push(s));
  a.connect();
  b.connect();
  last().onopen();
  assert.equal(connection.status, 'online');
  assert.deepEqual(last().sent.map(m => [m.t, m.store]), [['hello', 'a'], ['hello', 'b']]);

  last().onmessage({ t: 'closed', code: 'unauthorized', message: 'Not signed in' });
  last().onclose({ code: 4401, reason: 'Unauthorized' });
  assert.deepEqual(events, [['a', 'unauthorized'], ['b', 'unauthorized'], ['connection', 'unauthorized']]);
  assert.deepEqual(connection.closed, { code: 'unauthorized', message: 'Not signed in' });
  assert.deepEqual(a.closed, connection.closed);
  assert.deepEqual(b.closed, connection.closed);
  assert.equal(connection.status, 'offline');
  assert.deepEqual(statuses, ['connecting', 'offline']);
  await sleep(10);
  assert.equal(sockets.length, 1, 'no retry');

  a.connect();
  assert.equal(sockets.length, 2, 'connect() opens a fresh socket');
  assert.equal(connection.closed, null);
  assert.equal(a.closed, null, 'cleared by its connect()');
  assert.deepEqual(b.closed, { code: 'unauthorized', message: 'Not signed in' }, 'the other client keeps the reason until the socket is back');
  last().onopen();
  assert.equal(b.closed, null);
  assert.deepEqual(last().sent.map(m => m.store), ['a', 'b'], 'both say hello again');
  a.dispose();
  b.dispose();
});

test('a client may connect() from inside its own closed event: the clients after it still hear the reason, and the socket comes back once', async () => {
  const { factory, sockets, last } = fakeTransport();
  const connection = createConnection({ transport: factory, reconnect: { min: 1, max: 2 }, keepalive: false });
  const a = createClient({ connection, store: 'a', initial: {} });
  const b = createClient({ connection, store: 'b', initial: {} });
  const heard = [];
  // An app that has new credentials by the time the refusal comes: back in at once
  a.on('closed', c => {
    heard.push(['a', c.code]);
    a.connect();
  });
  b.on('closed', c => heard.push(['b', c?.code ?? null]));
  connection.on('closed', c => heard.push(['connection', c?.code ?? null]));
  a.connect();
  b.connect();
  last().onopen();

  last().onmessage({ t: 'closed', code: 'unauthorized', message: 'Not signed in' });
  assert.deepEqual(heard, [['a', 'unauthorized'], ['b', 'unauthorized'], ['connection', 'unauthorized']], 'everyone hears why, with the reason that was');
  assert.equal(sockets.length, 2, 'the connect() from inside the event opened a fresh socket');
  assert.equal(connection.closed, null, 'cleared by that connect()');
  assert.equal(a.closed, null);
  assert.deepEqual(b.closed, { code: 'unauthorized', message: 'Not signed in' }, 'b keeps the reason until the socket is back');
  last().onopen();
  assert.equal(b.closed, null);
  assert.deepEqual(last().sent.map(m => [m.t, m.store]), [['hello', 'a'], ['hello', 'b']], 'both say hello on the new socket');
  await sleep(10);
  assert.equal(sockets.length, 2, 'and no retry on top of it');
  a.dispose();
  b.dispose();
});

test('the close code alone ends the socket the same way, in case the message did not make it; an ordinary close still retries', async () => {
  const { factory, sockets, last } = fakeTransport();
  const connection = createConnection({ transport: factory, reconnect: { min: 1, max: 2 }, keepalive: false });
  connection.connect();
  last().onopen();
  last().onclose({ code: 1006 });
  await sleep(10);
  assert.equal(sockets.length, 2, 'an ordinary close is retried');
  last().onopen();
  last().onclose({ code: 4401, reason: 'Unauthorized' });
  assert.deepEqual(connection.closed, { code: 'unauthorized', message: 'Unauthorized' });
  await sleep(10);
  assert.equal(sockets.length, 2, 'not this one');
  assert.throws(() => connection.on('open', () => {}), /Unknown connection event/);
  connection.close();
});

test('connect() from a status listener, on the way offline, is the same: everyone hears the reason that was, offline, and the socket comes back once', async () => {
  const { factory, sockets, last } = fakeTransport();
  const connection = createConnection({ transport: factory, reconnect: { min: 1, max: 2 }, keepalive: false });
  const a = createClient({ connection, store: 'a', initial: {} });
  const b = createClient({ connection, store: 'b', initial: {} });
  const heard = [];
  // An app that reconnects whenever it finds itself offline
  a.on('status', s => { if (s === 'offline') a.connect(); });
  a.on('closed', c => heard.push(['a', c.code, a.status]));
  b.on('closed', c => heard.push(['b', c.code, b.status]));
  connection.on('closed', c => heard.push(['connection', c.code, connection.status, connection.closed?.code]));
  a.connect();
  b.connect();
  last().onopen();

  last().onmessage({ t: 'closed', code: 'unauthorized', message: 'Not signed in' });
  assert.deepEqual(heard, [['a', 'unauthorized', 'offline'], ['b', 'unauthorized', 'offline'], ['connection', 'unauthorized', 'offline', 'unauthorized']], 'everyone hears why, offline, with the reason still on the connection');
  assert.equal(sockets.length, 2, 'then the connect() took effect');
  assert.equal(connection.status, 'connecting');
  assert.equal(connection.closed, null);
  await sleep(10);
  assert.equal(sockets.length, 2, 'and no retry on top of it');

  // An ordinary drop with the same listener: its connect() brings the socket back, and the retry does not double it
  last().onopen();
  last().onclose({ code: 1006 });
  assert.equal(sockets.length, 3, 'back at once');
  await sleep(10);
  assert.equal(sockets.length, 3, 'once');
  a.dispose();
  b.dispose();
});

test('a handler that throws does not keep the others from hearing, and a transport that reports its close at once drops the socket once', async t => {
  const sockets = [];
  const factory = () => {
    const t = { onopen: null, onmessage: null, onclose: null, send() {}, close() { t.onclose?.({ code: 1000, reason: '' }); } };   // synchronously
    sockets.push(t);
    return t;
  };
  const connection = createConnection({ transport: factory, reconnect: { min: 1, max: 2 }, keepalive: false });
  const heard = [];
  connection.attach('x', { onOpen() {}, onMessage() {}, onClose() { heard.push('x offline'); }, onClosed() { throw new Error('boom'); } });
  connection.attach('y', { onOpen() {}, onMessage() {}, onClose() { heard.push('y offline'); }, onClosed(info) { heard.push(['y', info.code]); } });
  connection.on('closed', c => heard.push(['connection', c.code]));
  const errors = t.mock.method(console, 'error', () => {});
  connection.connect();
  sockets[0].onopen();
  sockets[0].onmessage({ t: 'closed', code: 'unauthorized', message: 'Not signed in' });
  assert.deepEqual(heard, ['x offline', 'y offline', ['y', 'unauthorized'], ['connection', 'unauthorized']], 'once each, whatever x threw');
  assert.equal(errors.mock.calls.length, 1, 'the throw is reported');
  assert.match(errors.mock.calls[0].arguments[0], /handler/);
  await sleep(10);
  assert.equal(sockets.length, 1, 'no retry');
  connection.close();
});
