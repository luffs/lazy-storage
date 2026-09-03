// Type declarations for `lazy-storage/client` (createClient, openClient)
// and the client-side shapes the main entry builds on. Hand-written.
import type { ChangeListener, ListenerOptions, Unsubscribe } from 'lazy-watch';
import type { ClosedCode, ErrorCode, Op, RegisterSpec, Row, ServerMessage } from './core.js';

export type ConnectionStatus = 'offline' | 'connecting' | 'open';
export type ClientStatus = 'offline' | 'connecting' | 'online';

// --- Transports and connections -----------------------------------------------

/** What a transport factory returns per (re)connect; messages are objects on both sides */
export interface Transport {
  send(message: object): void;
  close(): void;
  onopen: (() => void) | null;
  onmessage: ((message: unknown) => void) | null;
  onclose: (() => void) | null;
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
}

/** One socket, shared by any number of clients */
export interface Connection {
  readonly status: ConnectionStatus;
  /** Number of attached clients */
  readonly attached: number;
  on(event: 'status', fn: (status: ConnectionStatus) => void): Unsubscribe;
  /** Open the socket (idempotent) */
  connect(): void;
  /** Close the socket for every attached client; no automatic reconnect */
  close(): void;
  attach(storeId: string, handler: LinkHandler): Link;
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
 * op, and the state as another, written debounced. Without `saveState`
 * the state rides inside `save`.
 */
export interface DocumentStorage {
  load(): (OutboxDocument & Partial<StateCache>) | null;
  save(document: OutboxDocument & Partial<StateCache>): void;
  saveState?(cache: StateCache): void;
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
  saveOp(op: Op, meta: ClientMeta): void;
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
  /** Whole-value paths; must match the server's */
  registers?: RegisterSpec[];
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

export interface ClientEvents {
  status: ClientStatus;
  error: ClientError;
  /** The outbox changed */
  sync: undefined;
  presence: unknown[];
  closed: Closed;
}

export interface Client<S extends object = any> {
  /** The mirrored state: read and write it like a plain object */
  readonly state: S;
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
