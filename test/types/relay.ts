// relay.ts - Exercises the relay's typings the way a hub would; `npm run test:types`
// compiles it with check.ts (never runs it). Wrong usages carry an expect-error.
import { createRelay, memoryCopies, fileCopies, type Relay, type RelayMode, type CopyStorage, type RelayStats } from 'lazy-storage/relay';
import { createRelayHandlers, upstreamSocket } from 'lazy-storage/relay/bun';
import { createClient, createConnection, webSocketTransport, type ServerMessage } from 'lazy-storage';
import { createNetwork } from 'lazy-storage/testing';
import { createStore } from 'lazy-storage/server';

const SHAPE = { initial: { tasks: {} } };

// --- The relay ------------------------------------------------------------------

const relay: Relay = createRelay({
  shapeOf: storeId => (storeId.startsWith('site-') ? SHAPE : null),
  link: upstreamSocket('wss://central.example/sync/relay', { headers: { authorization: 'Bearer relay' } }),
  storage: fileCopies('./copies', { onError: err => void err }),
  acknowledge: 'upstream',
  grace: 10_000,
  authorizeOffline: (key, storeId) => key.length > 0 && storeId !== 'secret',
  keepalive: false,
  onError: err => void err
});
createRelay({ shapeOf: () => SHAPE, link: webSocketTransport('ws://x'), storage: memoryCopies(), acknowledge: 'local' });
// @ts-expect-error a relay needs the shapes it carries and its link
createRelay({ storage: memoryCopies() });
// @ts-expect-error acknowledge is 'upstream' or 'local'
createRelay({ shapeOf: () => SHAPE, link: webSocketTransport('ws://x'), acknowledge: 'never' });

const mode: RelayMode = relay.mode;
relay.on('mode', next => { const _m: 'through' | 'local' = next; });
relay.on('sync', ({ store, pending }) => void [store, pending]);
const copy = relay.copy('site-1');
const stats: RelayStats = relay.stats();
for (const [id, s] of Object.entries(stats.stores)) void [id, s.queue, s.synced];
relay.patch('site-1', { tasks: { a: { title: 'from the hub' } } });
const ownPending: number = relay.pending('site-1');
relay.upstreamDown();
relay.upstreamUp();
void [mode, copy, ownPending];

// A socket by hand, as a host on another server would
const session = relay.session({ send: message => void message, credential: { headers: { authorization: 'Bearer display' } }, key: 'fingerprint' });
session.receive({ t: 'ping' });
session.close();

// A storage of one's own
const mine: CopyStorage = {
  loadMeta: () => null,
  saveMeta: meta => void meta.replicaId,
  load: () => null,
  save: (storeId, copied) => void [storeId, copied.v, copied.own.ops],
  flush: () => {}
};
void mine;

// In tests: the server on one network, the relay's clients on another
const store = createStore(SHAPE);
const central = createNetwork(store, { vouch: credential => credential });
const onNet = createRelay({ shapeOf: () => SHAPE, link: central.link({ user: { id: 'relay' } }).factory });
const lan = createNetwork(onNet);
lan.link({ user: { id: 'u' } }).stall();

// --- Served by Bun --------------------------------------------------------------

const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  key: req => req.headers.get('authorization'),
  credential: req => ({ headers: { authorization: req.headers.get('authorization') ?? '' }, query: new URL(req.url).search }),
  maxPayload: 16 * 1024 * 1024,
  perMessageDeflate: { threshold: 2048 },
  maxBuffered: false
});
const upgraded: Promise<Response | undefined | null> = handlers.upgrade(new Request('http://hub/sync'), {});
for (const s of handlers.sockets()) void [s.key, s.buffered, s.openMs];
void [upgraded, handlers.close({ reason: 'bye' })];
// @ts-expect-error a credential is headers and a query
createRelayHandlers({ relay, credential: () => 'Bearer display' });

// --- A client behind a relay ----------------------------------------------------

const db = createClient({ connection: createConnection({ transport: webSocketTransport('ws://hub:36610/sync') }), store: 'site-1', ...SHAPE });
const relayed: boolean = db.relayed;
// @ts-expect-error relayed is read-only
db.relayed = true;
const answer = null as unknown as ServerMessage;
if (answer.t === 'snapshot' && answer.relay) void 'answered by a relay';
if (answer.t === 'relay') { const _status: 'local' | 'through' = answer.status; }
void relayed;
