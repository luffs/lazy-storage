// relay.ts - Exercises the relay's typings the way a hub would; `npm run test:types`
// compiles it with check.ts (never runs it). Wrong usages carry an expect-error.
import { createRelay, memoryCopies, fileCopies, type Relay, type RelayMode, type CopyInfo, type CopyStorage, type RelayStats } from 'lazy-storage/relay';
import { createRelayHandlers, upstreamSocket } from 'lazy-storage/relay/bun';
import { createClient, createConnection, webSocketTransport, type ServerMessage } from 'lazy-storage';
import { createNetwork } from 'lazy-storage/testing';
import { createStore } from 'lazy-storage/server';

// --- The relay ------------------------------------------------------------------

const relay: Relay = createRelay({
  storage: fileCopies('./copies', { onError: err => void err }),
  grace: 10_000,
  dialTimeout: 4000,
  probe: async () => true,
  probeEvery: 5000,
  keepalive: false,
  jitter: 1000,
  maxSkew: 300_000,
  authorizeOffline: (key, storeId) => key.length > 0 && storeId !== 'secret',
  validate: (diff, { key, replicaId, storeId, state }) => { void [diff, key, replicaId, storeId, state]; return true; },
  saveDelay: 1000,
  forgetAfter: Infinity,
  onError: err => void err
});
createRelay();
createRelay({ storage: memoryCopies(), probe: false });
// @ts-expect-error probe is a function or false
createRelay({ probe: true });

const mode: RelayMode = relay.mode;
relay.on('mode', next => { const _m: 'through' | 'local' = next; });
relay.on('copy', storeId => { const _s: string = storeId; });
relay.on('error', err => void err);
// @ts-expect-error no such event
relay.on('patch', () => {});
const copy: CopyInfo | null = relay.copy('main');
if (copy) {
  const v: number = copy.v;
  const behind: boolean = !copy.live || copy.diverged || copy.local > 0;
  void [v, behind, copy.state, copy.epoch];
}
relay.upstreamDown();
relay.upstreamUp();
const stats: RelayStats = relay.stats();
const through: number = stats.sockets.through;
const since: number | null = stats.downSince;
for (const s of relay.sockets()) { const _state: 'dialing' | 'through' | 'local' | 'closed' = s.state; void s.stores; }
relay.flush();
relay.sweep();
void [mode, through, since];

// A socket accepted by hand, as a host on another server would
const session = relay.accept({
  send: message => void message,
  close: (code, reason) => void [code, reason],
  key: 'fingerprint',
  upstream: webSocketTransport('wss://central.example/sync', { fetch: false })
});
session.receive({ t: 'hello', store: 'main', replicaId: 'r', ops: [] });
const state: 'dialing' | 'through' | 'local' | 'closed' = session.state;
session.close();
void state;
// @ts-expect-error a socket needs its way to the server
relay.accept({ send: () => {}, close: () => {}, key: 'k' });

// A storage of one's own
const mine: CopyStorage = {
  load: () => ({ copies: [], known: null }),
  write: (storeId, doc) => void [storeId, doc.state, doc.v],
  writeKnown: doc => void doc.keys
};
createRelay({ storage: mine });

// In tests, on the in-memory network: the server on one, the relay's clients on another
const centralNet = createNetwork(createStore({ initial: { tasks: {} } }));
const lan = createNetwork({
  session: ({ send, user, onEvict }) => relay.accept({ send, close: (code, reason) => onEvict(code, reason), key: (user as { key: string }).key, upstream: centralNet.link({ user }).factory })
});
lan.link({ user: { id: 'u', key: 'k' } }).stall();

// --- Served by Bun --------------------------------------------------------------

const handlers = createRelayHandlers({
  relay,
  path: '/sync',
  key: req => req.headers.get('authorization'),
  upstream: req => upstreamSocket(`wss://central.example/sync${new URL(req.url).search}`, {
    headers: { authorization: req.headers.get('authorization') ?? '', 'user-agent': req.headers.get('user-agent') ?? '' }
  }),
  maxPayload: 16 * 1024 * 1024,
  perMessageDeflate: { threshold: 2048 },
  maxBuffered: false
});
const upgraded: Promise<Response | undefined | null> = handlers.upgrade(new Request('http://hub/sync'), {});
void upgraded;
for (const s of handlers.sockets()) void [s.key, s.state, s.buffered, s.openMs];
void handlers.close({ reason: 'bye' });
upstreamSocket(() => 'wss://central.example/sync', { headers: () => ({ authorization: 'Bearer x' }) });
// @ts-expect-error the relay's handlers need the way to the server
createRelayHandlers({ relay });

// --- A client behind a relay ----------------------------------------------------

const db = createClient({ connection: createConnection({ transport: webSocketTransport('ws://hub:36610/sync') }), store: 'main', initial: { tasks: {} } });
const relayed: boolean = db.relayed;
db.on('relay', isRelayed => { const _b: boolean = isRelayed; });
// @ts-expect-error relayed is read-only
db.relayed = true;
const answer = null as unknown as ServerMessage;
if (answer.t === 'snapshot' && answer.epoch === null) void 'answered by a relay';
if (answer.t === 'relay') { const _status: 'local' | 'through' = answer.status; }
void relayed;
