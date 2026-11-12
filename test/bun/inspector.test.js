// inspector.test.js - Runs under Bun only: the inspector served by the Bun adapter, as by Node's (test/inspector.test.js)
import assert from 'node:assert/strict';
import { test } from 'bun:test';
import { createStore, createStores } from '../../src/server/index.js';
import { serve } from '../../src/server/bun.js';

test('the Bun server serves the inspector at <path>/inspect when it is given one: the page, what it asks, and a revert', async () => {
  const stores = createStores(() => createStore({ initial: { tasks: {} }, history: true }));
  stores.get('main').patch({ tasks: { k: { title: 'Milk' } } });
  stores.get('main').patch({ tasks: { k: { title: 'Oat milk' } } });
  const server = serve({ stores, port: 0, inspector: { authorize: request => request.headers.get('x-key') === 'let-me-in' } });
  const url = `http://localhost:${server.port}/ws/inspect`;
  const headers = { 'x-key': 'let-me-in' };
  try {
    assert.equal((await fetch(url)).status, 401);
    const page = await fetch(url, { headers });
    assert.equal(page.status, 200);
    assert.match(await page.text(), /lazy-storage inspector/);
    assert.deepEqual((await (await fetch(`${url}/api/stores`, { headers })).json()).stores.map(s => s.id), ['main']);
    assert.deepEqual((await (await fetch(`${url}/api/activity?store=main`, { headers })).json()).entries.map(e => e.record.title), ['Oat milk', 'Milk']);
    const reverted = await fetch(`${url}/api/revert`, { method: 'POST', headers: { ...headers, 'content-type': 'application/json', 'x-lazy-inspect': '1' }, body: JSON.stringify({ store: 'main', c: 'tasks', id: 'k', v: 1 }) });
    assert.equal(reverted.status, 200);
    assert.equal(stores.get('main').state.tasks.k.title, 'Milk');
  } finally {
    await server.shutdown();
  }
});
