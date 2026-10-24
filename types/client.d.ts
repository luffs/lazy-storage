// Type declarations for `lazy-storage/client` (createClient, openClient)
// and the client-side shapes the main entry builds on. Hand-written.
import type { ChangeListener, ListenerOptions, Unsubscribe } from 'lazy-watch';
import type { Change, ClosedCode, ErrorCode, Lost, Op, Peer, RecordClock, ServerMessage, Shape, ShapeOptions } from './core.js';

/** The socket's: `online` once it is open */
export type ConnectionStatus = 'offline' | 'connecting' | 'online';
/** A client's: `online` once its socket is open and its store is synced (a snapshot or delta applied) */
export type ClientStatus = 'offline' | 'connecting' | 'online';

// --- Transports and connections -----------------------------------------------

/** The WebSocket close code and reason, where a transport knows them */
export interface CloseInfo {
  code?: number;
  reason?: string;
}

/** What a transport factory returns per (re)connect; messages are objects on both sides */
export interface Transport {
  send(message: object): void;
  close(): void;
  onopen: (() => void) | null;
  onmessage: ((message: unknown) => void) | null;
  onclose: ((info?: CloseInfo) => void) | null;
}

export type TransportFactory = (() => Transport) & {
  /** Fetch a path from the server the transport connects to; a client that has this fetches a large snapshot over HTTP (see the server's snapshot route) */
  fetch?(path: string): Promise<Response>;
};

export interface ReconnectOptions {
  min: number;
  max: number;
}

export interface ConnectionOptions {
  transport: TransportFactory;
  /** Retry backoff after an unexpected close; false disables automatic reconnects */
  reconnect?: ReconnectOptions | false;
  /** Ping interval in ms while open (default 30 000); false disables */
  keepalive?: number | false;
  /** In a browser, retry at once on `online` or when the page is looked at again, while the socket is down (default true) */
  wake?: boolean;
}

/** A client's attachment to a connection under its store id */
export interface Link {
  send(message: object): void;
  detach(): void;
}

export interface LinkHandler {
  onOpen(link: Link): void;
  onMessage(message: ServerMessage): void;
  onClose(): void;
  /** The server turned the socket away; final for every store on it until connect() */
  onClosed?(closed: Closed): void;
}

/** One socket, shared by any number of clients */
export interface Connection {
  readonly status: ConnectionStatus;
  /** Number of attached clients */
  readonly attached: number;
  /** Why the server turned the socket away, or null; cleared by connect() */
  readonly closed: Closed | null;
  /** Fetch a path from the server the socket connects to, when the transport can (webSocketTransport can); a client then fetches a large snapshot over HTTP */
  fetch?(path: string): Promise<Response>;
  on(event: 'status', fn: (status: ConnectionStatus) => void): Unsubscribe;
  on(event: 'closed', fn: (closed: Closed) => void): Unsubscribe;
  /** Open the socket (idempotent); after the server turned it away, the way back in with fresh credentials */
  connect(): void;
  /** Close the socket for every attached client; no automatic reconnect */
  close(): void;
  /** `info` is what the attaching client declared, for a connection that builds a replica of the store elsewhere */
  attach(storeId: string, handler: LinkHandler, shape?: ShapeOptions): Link;
}

/** What `sharedConnection` needs of a channel between tabs: BroadcastChannel's shape */
export interface TabChannel {
  postMessage(message: unknown): void;
  onmessage: ((event: { data: unknown }) => void) | null;
  close(): void;
}

/** What `sharedConnection` needs of a lock manager: the Web Locks API's `request`, and `query` to find tabs that closed */
export interface TabLocks {
  request(name: string, options: { signal?: AbortSignal }, callback: () => Promise<unknown>): Promise<unknown>;
  query?(): Promise<{ held: { name: string }[]; pending: { name: string }[] }>;
}

export interface SharedConnectionOptions {
  /** One per app: names the channel and the lock */
  name: string;
  /** The browser's socket; opened by the leader tab only */
  transport: TransportFactory;
  /** The browser replica's persistence per store: any adapter, IndexedDB included (default: memory) */
  storage?(storeId: string): ClientStorage;
  /** A replica's storage that failed to open; default console */
  onError?(error: unknown): void;
  /** Retry backoff for the socket; false disables automatic reconnects */
  reconnect?: ReconnectOptions | false;
  /** Ping interval for the socket (default 30 000); false disables */
  keepalive?: number | false;
  /** Makes the channel between tabs (default: BroadcastChannel); null for none */
  channel?: ((name: string) => TabChannel) | null;
  /** The lock manager tabs elect a leader with (default: navigator.locks); null for none, and this tab leads on its own */
  locks?: TabLocks | null;
  tabId?: string;
  /** A store's shape, for one no tab of the leader has declared yet */
  shapeOf?(storeId: string): ShapeOptions | null;
  /** The browser's replica (default: kept in localStorage) */
  replicaId?: string;
  /** How often (ms) the leader looks for tabs that closed without a word, by the lock each tab holds; default 10 000 */
  sweepEvery?: number;
}

/**
 * One socket per browser: the tabs elect a leader, which runs the browser's
 * replica on the real connection; every tab's clients follow it over a
 * channel, in the leader's tab directly
 */
export interface SharedConnection extends Connection {
  /** Whether this tab runs the browser's replica */
  readonly leader: boolean;
  readonly tabId: string;
  /** The browser's socket, which this tab's clients report as their status */
  readonly upstream: ConnectionStatus;
  /** The replica's unsent ops for a store, counted into this tab's clients' `pending` */
  pending(storeId: string): number;
  on(event: 'status', fn: (status: ConnectionStatus) => void): Unsubscribe;
  on(event: 'closed', fn: (closed: Closed) => void): Unsubscribe;
  /** The replica's outbox changed */
  on(event: 'sync', fn: () => void): Unsubscribe;
  /**
   * Let the page on the other end of a MessagePort (an iframe, a worker) have clients on the
   * browser's replica, as this tab's are, for the stores named and no other; the other end runs
   * a client on `messagePortTransport(port)`. Returns what ends it
   */
  follow(port: MessagePortLike, stores: Array<{ store: string } & ShapeOptions>): () => void;
  /** This tab is done: its clients disconnect and, if it led, the replica closes and another tab takes over */
  dispose(): void;
}

/** What `follow` and `messagePortTransport` need of a port: a MessagePort's shape */
export interface MessagePortLike {
  postMessage(message: unknown): void;
  onmessage: ((event: { data: unknown }) => void) | null;
  start?(): void;
}

/** The other end of `follow`: a connection over the port that also knows the browser's socket and outbox, as the host says */
export interface PortConnection extends Connection {
  /** The browser's socket, which this page's clients report as their status */
  readonly upstream: ConnectionStatus;
  /** The replica's unsent ops for a store, counted into this page's clients' `pending` */
  pending(storeId: string): number;
  on(event: 'status', fn: (status: ConnectionStatus) => void): Unsubscribe;
  on(event: 'closed', fn: (closed: Closed) => void): Unsubscribe;
  /** The replica's outbox changed */
  on(event: 'sync', fn: () => void): Unsubscribe;
}

// --- Storage adapters -------------------------------------------------------------

/** What the client tells an adapter about itself with every write */
export interface ClientMeta {
  replicaId: string;
  seq: number;
  version: number;
  epoch: string | null;
}

export interface OutboxDocument {
  replicaId: string;
  seq: number;
  ops: Op[];
}

/** The base: the state as the server last told it, with the version and epoch it is at */
export interface StateCache {
  state: object;
  /** Each record's clock: [collection, id or null, clock] */
  clocks?: Array<[string, string | null, RecordClock]>;
  version: number;
  epoch: string | null;
}

/**
 * A document adapter keeps the outbox as one document, written with every
 * op, and the base as another, written once changes settle.
 */
export interface DocumentStorage {
  load(): (OutboxDocument & Partial<StateCache>) | null;
  save(outbox: OutboxDocument): void;
  saveState(cache: StateCache): void;
  /** Forget the outbox and the cache, for a store that is gone for good; the built-in adapters have it */
  clear?(): void;
  /** Take the outbox op by op instead of whole (localStorageOutbox does): an op new or rewritten */
  saveOp?(op: Op, meta: ClientMeta): void;
  /** An op a newer one emptied, or the server refused */
  removeOp?(seq: number, meta: ClientMeta): void;
  /** The ops up to an acknowledged seq */
  dropOps?(seq: number, meta: ClientMeta): void;
}

/** A record of the base as a row adapter keeps it: [collection, id or null, data, clock] */
export type ClientRecord = [collection: string, id: string | null, data: unknown, clock: RecordClock];

export interface RowDocument extends OutboxDocument {
  records: ClientRecord[];
  version?: number;
  epoch?: string | null;
}

/**
 * A row adapter keeps one row per record of the base and one per pending
 * op, so a patch costs the records it touched. `load` may return a
 * promise, in which case the client is opened with `openClient`.
 */
export interface RowStorage {
  load(): RowDocument | null | Promise<RowDocument | null>;
  commit(change: { puts: ClientRecord[]; deletes: Array<[string, string | null]>; meta: ClientMeta }): void;
  replace(change: { records: ClientRecord[]; meta: ClientMeta }): void;
  saveOp(op: Op, meta: ClientMeta): void;
  removeOp(seq: number, meta: ClientMeta): void;
  dropOps(seq: number, meta: ClientMeta): void;
  /** Optional: call `listener` when what the adapter held was lost (the client then writes everything again) */
  onReset?(listener: () => void): () => void;
}

export type ClientStorage = DocumentStorage | RowStorage;

// --- The client -------------------------------------------------------------------

/** Either a shared connection or a transport for one the client owns, never both */
export type ClientLink =
  | { connection: Connection; transport?: undefined }
  | { transport: TransportFactory; connection?: undefined };

export type ClientOptions<S extends object = any> = ClientOptionsBase<S> & ClientLink;

export interface ClientOptionsBase<S extends object = any> extends Omit<ShapeOptions, 'initial'> {
  /** The store id */
  store: string;
  /** The store's top level, as the server declares it: collections as {}, singletons with their values */
  initial?: S;
  /** The position field on list records (default 'pos') */
  position?: string;
  /** Defaults to the persisted one, else random */
  replicaId?: string;
  /** Outbox and state-cache persistence (default: memory) */
  storage?: ClientStorage;
  /** A client that only reads: `cache`, `undo` and `presence` default to false, each still settable on its own (default false) */
  mirror?: boolean;
  /** Persist the state and start from it on the next load (default true; false under `mirror`) */
  cache?: boolean;
  /** With a document adapter: write the state once changes have settled this long (ms, default 1000), at least every ten of these, and when the page is hidden */
  cacheDelay?: number;
  /** Attach an undo manager (default true) */
  undo?: boolean;
  undoLimit?: number;
  /** Retry backoff for an owned connection; false disables automatic reconnects */
  reconnect?: ReconnectOptions | false;
  /** false asks the server not to send presence to this client, which then never has peers; it may still share (default true) */
  presence?: boolean;
  /** Wall clock (injectable for tests) */
  now?: () => number;
}

export interface Closed {
  code: ClosedCode;
  message: string;
}

/** An error from the server carries its code; a register mismatch and a failed snapshot fetch are detected on the client */
export interface ClientError extends Error {
  code?: ErrorCode | 'shape-mismatch' | 'snapshot-fetch';
}

/** Records keyed by id under `state[name]` */
export interface Collection<T extends { id?: string } = any> {
  /** Add a record; its `id` is minted unless provided. Returns the id */
  add(record: Omit<T, 'id'> & { id?: string }): string;
  /** Merge fields into an existing record; false when it does not exist */
  update(id: string, fields: Partial<T>): boolean;
  remove(id: string): boolean;
  get(id: string): T | undefined;
  has(id: string): boolean;
  ids(): string[];
  all(): T[];
  /**
   * The records a batch changed, once per batch: plain copies of each as it
   * is (`record`) and as it was (`previous`), and the batch's meta
   */
  watch(listener: (changes: RecordChange<T>[], meta?: { origin?: string } & Record<string, unknown>) => void): Unsubscribe;
}

/** One record a batch changed: added, changed, or removed */
export type RecordChange<T> =
  | { type: 'insert'; id: string; record: T }
  | { type: 'update'; id: string; record: T; previous: T }
  | { type: 'remove'; id: string; previous: T };

/** Where a record goes: before or after another, at an index, or (nothing) at the end */
export interface ListSlot {
  before?: string;
  after?: string;
  at?: number;
}

/**
 * An ordered list of records: a keyed map under `path` with a position
 * key on every record. Adds and moves write one field on one record.
 */
export interface List<T extends { id?: string } = any> {
  readonly path: string[];
  /** Records in list order: by position, ties by id, unpositioned last */
  all(): T[];
  ids(): string[];
  get(id: string): T | undefined;
  has(id: string): boolean;
  /** Add a record; its id is minted unless provided. Returns the id */
  add(record: Omit<T, 'id'> & { id?: string }, where?: ListSlot): string;
  /** Move a record; false when it does not exist. Writes nothing when it is already there */
  move(id: string, where?: ListSlot): boolean;
  remove(id: string): boolean;
  /** Make the list sort as `ids`, writing the fewest positions; returns how many. Unlisted records follow */
  reconcile(ids: Iterable<string>): number;
  /** The key a record would get for `where` */
  keyFor(where?: ListSlot): string;
}

export interface ListOptions {
  /** The field holding the position key (default 'pos') */
  position?: string;
}

export interface ClientEvents {
  status: ClientStatus;
  error: ClientError;
  /** The outbox changed */
  sync: undefined;
  presence: unknown[];
  /** Every live session on the store, with what it shares */
  peers: Peer[];
  closed: Closed;
  /** What undo and redo can do changed */
  history: { canUndo: boolean; canRedo: boolean };
  /** Changes of an op of this client's that did not land, and why: 'newer' (a later write), 'missing' (the record or its parent was gone), 'stale' (made before the container was made again) */
  lost: { seq: number; lost: Lost[] };
  /** An op the server refused whole (or a batch the shape refused here, seq null): taken back */
  rejected: Rejected;
  /** The server's storage started over (a backup put back): `previous` is what this client showed */
  reset: Reset;
  /** `relayed` changed: a relay answers on its own (true), or the server does again (false) */
  relay: boolean;
}

export interface Reset {
  epoch: string;
  previous: { epoch: string; version: number; state: unknown };
}

export interface Rejected {
  seq: number | null;
  code: string;
  message: string;
  changes: Change[] | null;
}

/** What db.stats() reports */
export interface ClientStats {
  /** Unacknowledged local ops */
  pending: number;
  /** How long the oldest of them has waited, or null when none is */
  oldestPendingMs: number | null;
  /** The last acknowledgement's round trip: op sent to ack received */
  ackMs: number | null;
}

export interface Client<S extends object = any> {
  /** The state: the server's, with this client's pending edits on top. Read and write it like a plain object */
  readonly state: S;
  /** The server's state as this client last heard it (plain; read only) */
  readonly base: S;
  /** A record's clock in the base: the times by path the server decides by */
  clockOf(c: string, id?: string): RecordClock;
  readonly shape: Shape;
  readonly replicaId: string;
  readonly store: string;
  readonly connection: Connection;
  readonly status: ClientStatus;
  /** Unacknowledged local ops */
  readonly pending: number;
  /** How the client is doing, for a "saving…" indicator or a status line; null where nothing has happened yet */
  stats(): ClientStats;
  /** The store version this client has seen everything up to */
  readonly version: number;
  /** The storage life `version` counts in: the server's epoch, null for a relay's own counting, or before any answer */
  readonly epoch: string | null;
  /** True while a relay (lazy-storage/relay) answers this store from its own copy, the server being away: edits reach the others behind the relay but stay pending until the server has them */
  readonly relayed: boolean;
  /** Distinct users with a live session on this store (empty while offline) */
  readonly presence: unknown[];
  /** Every live session on this store, this client's own included (by `replicaId`), with what each shares */
  readonly peers: Peer[];
  /** Whether this client asks to hear presence (the `presence` option, or what `wantPresence` last said) */
  readonly wantsPresence: boolean;
  /**
   * Hear presence, or stop, while the client runs: the server hears it in a
   * hello said again, and one that turns presence on is sent the whole list.
   * Off, `presence` and `peers` are empty at once
   */
  wantPresence(on: boolean): void;
  /** What this client shares with its peers, or undefined */
  readonly shared: unknown;
  /**
   * Share a small JSON value with everyone on the store: it rides on
   * presence, is never written, and lives as long as the session (a
   * hello carries it, so a reconnect restores it). null clears it
   */
  share(data: unknown): void;
  /** Why the server closed this store for us, or null */
  readonly closed: Closed | null;
  /** True when this client started from a cached state rather than `initial` */
  readonly restored: boolean;
  readonly canUndo: boolean;
  readonly canRedo: boolean;
  connect(): void;
  /** Detach this store; an owned connection closes, a shared one stays up for the others */
  disconnect(): void;
  collection<T extends { id?: string } = any>(name: string): Collection<T>;
  /** Whether an unacknowledged edit writes at, under, or over `path` ('tasks/x/title' or segments; a list's records by id) */
  isPending(path: string | string[]): boolean;
  /** An ordered list of records: a collection ('tasks'), or a keyed map at a path (['state', 'lists', 'items']) */
  list<T extends { id?: string } = any>(path: string | string[], options?: ListOptions): List<T>;
  /** Subscribe to state changes; `meta?.origin === 'remote'` marks the server's */
  watch(listener: ChangeListener<S>, options?: ListenerOptions): Unsubscribe;
  on<E extends keyof ClientEvents>(event: E, fn: (payload: ClientEvents[E]) => void): Unsubscribe;
  undo(): boolean;
  redo(): boolean;
  checkpoint(): void;
  group<R>(fn: () => R): R;
  clearHistory(): void;
  dispose(): void;
}

/**
 * The state type a client takes from `initial`: an empty array there is
 * inferred as `never[]`, which nothing can be pushed onto, so it widens to
 * `any[]` (give the type argument, `createClient<State>(...)`, for records
 * typed throughout). A type given, or `any`, passes as it is
 */
export type FromInitial<T> = 0 extends (1 & T) ? T
  : [T] extends [never[]] ? any[]
  : T extends object ? { [K in keyof T]: FromInitial<T[K]> }
  : T;

/** A client on an adapter that loads synchronously; throws for one whose load() returns a promise */
export function createClient<S extends object = any>(options: ClientOptions<S>): Client<FromInitial<S>>;
/** createClient for an adapter whose load() returns a promise (IndexedDB) */
export function openClient<S extends object = any>(options: ClientOptions<S>): Promise<Client<FromInitial<S>>>;

/** The records of a keyed map in list order: by position, ties by id, records without a position last */
export function inOrder<T = any>(map: Record<string, T> | null | undefined, options?: { position?: string }): T[];
