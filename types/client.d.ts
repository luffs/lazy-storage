// Type declarations for `lazy-storage/client` (createClient, openClient)
// and the client-side shapes the main entry builds on. Hand-written.
import type { ChangeListener, ListenerOptions, Unsubscribe } from 'lazy-watch';
import type { ClosedCode, ErrorCode, Op, Peer, RegisterSpec, Row, ServerMessage } from './core.js';

export type ConnectionStatus = 'offline' | 'connecting' | 'open';
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

export type TransportFactory = () => Transport;

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
  on(event: 'status', fn: (status: ConnectionStatus) => void): Unsubscribe;
  on(event: 'closed', fn: (closed: Closed) => void): Unsubscribe;
  /** Open the socket (idempotent); after the server turned it away, the way back in with fresh credentials */
  connect(): void;
  /** Close the socket for every attached client; no automatic reconnect */
  close(): void;
  /** `info` is what the attaching client declared, for a connection that builds a replica of the store elsewhere */
  attach(storeId: string, handler: LinkHandler, info?: { initial?: object; registers?: RegisterSpec[] }): Link;
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
  /** How long (ms) the replica keeps a store no tab has open anymore, for a reload to come back; default 5000 */
  linger?: number;
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
  /** This tab is done: its clients disconnect and, if it led, the replica closes and another tab takes over */
  dispose(): void;
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

export interface StateCache {
  state: object;
  version: number;
  epoch: string | null;
}

/**
 * A document adapter keeps the outbox as one document, written with every
 * op, and the state as another, written debounced.
 */
export interface DocumentStorage {
  load(): (OutboxDocument & Partial<StateCache>) | null;
  save(outbox: OutboxDocument): void;
  saveState(cache: StateCache): void;
  /** Forget the outbox and the cache, for a store that is gone for good; the built-in adapters have it */
  clear?(): void;
}

export interface RowDocument extends OutboxDocument {
  rows: Row[];
  version?: number;
  epoch?: string | null;
}

/**
 * A row adapter keeps one row per leaf and one per pending op, so a batch
 * costs the leaves it touched. A delete removes the path and everything
 * under it. `load` may return a promise, in which case the client is
 * opened with `openClient`.
 */
export interface RowStorage {
  load(): RowDocument | null | Promise<RowDocument | null>;
  commit(change: { puts: Row[]; deletes: string[]; meta: ClientMeta }): void;
  replace(change: { rows: Row[]; meta: ClientMeta }): void;
  /** Write a pending op: a new one, or an older one a newer op pruned */
  saveOp(op: Op, meta: ClientMeta): void;
  /** Remove one pending op a newer op emptied */
  removeOp(seq: number, meta: ClientMeta): void;
  /** Remove every pending op up to and including `seq`, once acknowledged */
  dropOps(seq: number, meta: ClientMeta): void;
}

export type ClientStorage = DocumentStorage | RowStorage;

// --- The client -------------------------------------------------------------------

/** Either a shared connection or a transport for one the client owns, never both */
export type ClientLink =
  | { connection: Connection; transport?: undefined }
  | { transport: TransportFactory; connection?: undefined };

export type ClientOptions<S extends object = any> = ClientOptionsBase<S> & ClientLink;

export interface ClientOptionsBase<S extends object = any> {
  /** The store id */
  store: string;
  /** State before the first snapshot (and the skeleton under a cached state) */
  initial?: S;
  /** Whole-value paths (arrays of anything as one value); must match the server's */
  registers?: RegisterSpec[];
  /**
   * List paths that `state` presents as plain arrays of records in
   * position order, while the wire keeps keyed maps with positions. With
   * lists declared, `state` is that view and `wire` the synced state
   * underneath; `initial` may use arrays at those paths
   */
  lists?: RegisterSpec[];
  /** The position field on list records (default 'pos') */
  position?: string;
  /** Defaults to the persisted one, else random */
  replicaId?: string;
  /** Outbox and state-cache persistence (default: memory) */
  storage?: ClientStorage;
  /** Persist the state and start from it on the next load (default true) */
  cache?: boolean;
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

/** An error from the server carries its code; a register mismatch is detected on the client */
export interface ClientError extends Error {
  code?: ErrorCode | 'registers-mismatch';
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
}

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
  /** What undo and redo can do changed: after a local batch, an undo, a redo, or clearHistory() */
  history: { canUndo: boolean; canRedo: boolean };
}

export interface Client<S extends object = any> {
  /** The mirrored state: read and write it like a plain object (with lists declared, the view with arrays) */
  readonly state: S;
  /** The synced state underneath: keyed maps with positions; the same object as `state` without lists */
  readonly wire: object;
  readonly replicaId: string;
  readonly store: string;
  readonly connection: Connection;
  readonly status: ClientStatus;
  /** Unacknowledged local ops */
  readonly pending: number;
  /** The store version this client has seen everything up to */
  readonly version: number;
  /** Distinct users with a live session on this store (empty while offline) */
  readonly presence: unknown[];
  /** Every live session on this store, this client's own included (by `replicaId`), with what each shares */
  readonly peers: Peer[];
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
  /** An ordered list of records under `path` ('tasks', 'tasks/x/subtasks', or segments) */
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

/** A client on an adapter that loads synchronously; throws for one whose load() returns a promise */
export function createClient<S extends object = any>(options: ClientOptions<S>): Client<S>;
/** createClient for an adapter whose load() returns a promise (IndexedDB) */
export function openClient<S extends object = any>(options: ClientOptions<S>): Promise<Client<S>>;
