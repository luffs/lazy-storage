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
  assert.equal(connection.status, 'open');
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
