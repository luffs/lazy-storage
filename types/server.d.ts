// Type declarations for `lazy-storage/server`: stores, storage adapters,
// the registry, and hubs. Hand-written; the runtime is plain JS.
import type { Unsubscribe } from 'lazy-watch';
import type { Change, ClientMessage, Lost, Op, Path, Peer, RecordClock, ServerMessage, Shape, ShapeOptions, Tagged, Timestamp } from './core.js';

export type { ClientMessage, ServerMessage, Tagged } from './core.js';

// --- Storage adapters ---------------------------------------------------------

/** A record as stored: its collection, its id (null for a singleton), its data and its clock */
export type StoredRecord = [collection: string, id: string | null, data: unknown, clock: RecordClock];

export interface ReplicaProgress {
  seq: number;
  /** The store's clock when the replica's last op arrived */
  seen: number;
  /** The key of the user who first spoke for it; another user may not take it over */
  owner?: string;
}

export interface LogEntry {
  v: number;
  ts: Timestamp;
  changes: Change[];
}

export interface StorageDocument {
  records: StoredRecord[];
  replicas: Record<string, ReplicaProgress>;
  version: number;
  /** A random id minted once per storage life */
  epoch: string;
  /** How many of the store's migrations the records have been through */
  schema?: number;
  /** The last changes, for deltas after a restart */
  log?: LogEntry[];
}

export interface StorageCommit {
  upserts: StoredRecord[];
  deletes: Array<[collection: string, id: string | null]>;
  replica?: { id: string; seq: number; seen: number; owner?: string };
  forgetReplicas?: string[];
  version: number;
  epoch: string;
  schema: number;
  log?: LogEntry;
  logFloor?: number;
}

export interface ServerStorage {
  /** null: never seen, the store starts from `initial` */
  load(): StorageDocument | null;
  commit(change: StorageCommit): void;
  /** Several commits as one, all or none: what a turn of the event loop merged */
  commitMany?(changes: StorageCommit[]): void;
  flush(): void;
  /** The store let go of it: another process may serve it now */
  close?(): void;
  /** Take a document as the whole storage, under a new epoch and no log; never under a live store */
  replace?(doc: StorageDocument): void;
}

export function memoryStorage(): ServerStorage & { replace(doc: StorageDocument): void };
export function jsonFileStorage(file: string, options?: { debounce?: number; onError?(error: unknown): void }): ServerStorage & { replace(doc: StorageDocument): void };
export function assertDocument(doc: unknown): asserts doc is StorageDocument;

// --- Stores ---------------------------------------------------------------------

export interface Session {
  replicaId: string | null;
  readonly kind: 'client' | 'follower';
  receive(message: ClientMessage): void;
  close(): void;
  /** On a follower's session: a client it vouched for, judged as itself; null, or the refusal */
  grant(as: string, user: unknown, replica: string): null | { code: string; message: string };
  ungrant(as: string): void;
  /** Take back the follower's clients `filter(user)` picks; returns how many */
  revoke(filter: (user: unknown) => boolean, code: string, message: string): number;
}

export interface SessionOptions {
  send(message: ServerMessage): void;
  /** Whatever the transport authenticated */
  user?: unknown;
  /** Called after closeSessions or dispose closed it */
  onEvict?(): void;
  /** Fan a broadcast out to every session that offered one at once */
  broadcast?(message: ServerMessage): void;
  /** Where a client that can fetch gets a large snapshot */
  httpSnapshot?: { url: string; threshold?: number };
  /** May say follower: true in its hello: a relay's link, whose clients a hub vouches for */
  follower?: boolean;
}

/** What a rule is handed besides the user and the record */
export interface RuleContext<S extends object = any> {
  c: string;
  id: string | undefined;
  /** The store's state, for a rule that looks at other records */
  state: S;
  replica: string;
  /** The op's changes to this record */
  changes: Change[];
  store: Store<S>;
}

/** May the user write this record? `before` as it is, `after` as the op would leave it; null where there is none. false or a throw refuses */
export type Rule<S extends object = any> =
  | boolean
  | ((user: unknown, before: any, after: any, context: RuleContext<S>) => boolean | void)
  | { write: false | ((user: unknown, before: any, after: any, context: RuleContext<S>) => boolean | void) };

export interface ValidateContext<S extends object = any> {
  user: unknown;
  replicaId: string;
  store: Store<S>;
  changes: Change[];
}

export interface RateLimit {
  burst: number;
  perSecond: number;
}

export interface ShareContext<S extends object = any> {
  user: unknown;
  replicaId: string;
  store: Store<S>;
}

export interface PresenceOptions<S extends object = any> {
  /** What presence groups users by (default: user.id, else the whole user) */
  key?(user: unknown): string;
  /** How a user is shown to the others */
  user?(user: unknown): unknown;
  /** Judge what a session shares: false refuses, a value replaces it */
  validate?(data: unknown, context: ShareContext<S>): boolean | unknown | void;
  /** Batch presence changes, at most one message per this many ms */
  every?: number;
  /** The largest share, in bytes of JSON (default 4096) */
  maxShare?: number;
}

export interface StoreOptions<S extends object = any> extends Omit<ShapeOptions, 'initial'> {
  initial?: S;
  /** Per collection or singleton, '*' for the rest */
  rules?: Record<string, Rule<S>>;
  /** true refuses every client op; a list names the collections and singletons that are the server's alone */
  readOnly?: boolean | string[];
  /** A check of the whole op, its changes as one diff in the tree's shape: false or a throw refuses it */
  validate?(diff: Record<string, any>, context: ValidateContext<S>): boolean | void;
  /** Ops older than this (ms) are refused as expired; deletions and replicas older are forgotten (default 30 days) */
  retention?: number;
  compactEvery?: number;
  /** How many changes back a reconnect is answered with a delta (default 1000) */
  deltaLog?: number;
  /** The most changes in one op (default 10 000) */
  maxChanges?: number;
  rateLimit?: RateLimit | false;
  storage?: ServerStorage;
  /** Commit what a turn of the event loop merged together (default true) */
  groupCommit?: boolean;
  migrations?: Array<(state: S, context: { store: Store<S> }) => object | void>;
  presence?: boolean | PresenceOptions<S>;
  onError?(error: unknown): void;
  now?: () => number;
}

export interface OpEvent {
  replicaId: string;
  seq: number | null;
  user: unknown;
  /** The changes as they landed */
  changes: Change[];
  version: number;
}

export interface RefusedEvent {
  replicaId: string | undefined;
  seq: number | undefined;
  user: unknown;
  code: string;
  message: string;
}

export interface SessionEvent {
  event: 'open' | 'close';
  kind: 'client' | 'follower';
  user: unknown;
  replicaId: string | null;
  sessions: number;
}

export interface StoreEvents {
  op: OpEvent;
  refused: RefusedEvent;
  session: SessionEvent;
}

export interface StoreStats {
  version: number;
  epoch: string;
  schema: number;
  sessions: number;
  replicas: number;
  records: number;
  log: number;
  sent: Record<string, { messages: number; bytes: number }>;
}

export interface StoreDocument extends StorageDocument {
  schema: number;
}

export interface Store<S extends object = any> {
  /** The state: read it, never write it (patch, del and apply write) */
  readonly state: S;
  readonly shape: Shape;
  readonly version: number;
  readonly epoch: string;
  readonly sessions: number;
  readonly disposed: boolean;
  now(): number;
  /** The server's own write, in the tree's shape: merged, missing containers and records made, stamped now */
  patch(diff: Record<string, any>): Change[];
  /** Delete a record, or a path in one, as the server */
  del(c: string, id?: string | Path, path?: Path): Change[];
  /** Decide a replica's op as if it came on a session of `user`; throws a RefusedError for one refused whole */
  apply(op: Op, options?: { user?: unknown }): { lost: Lost[]; duplicate: boolean };
  session(options: SessionOptions): Session;
  closeSessions(predicate: (session: Session | { user: unknown; replicaId: string | null }) => boolean, message?: string): number;
  /** Distinct users with a live session */
  presence(): unknown[];
  peers(): Peer[];
  snapshot(): S;
  snapshotJSON(): string;
  clocksJSON(): string;
  /** A record's clock: times by path of what lives and what was deleted inside it */
  clock(c: string, id?: string): RecordClock;
  /** Hear every batch that lands, as a diff in the tree's shape, on the next microtask, before it is stored */
  on(listener: (diff: Record<string, any>, meta: { origin: 'remote' | 'server'; replicaId: string; v: number; changes: Change[] }) => void): Unsubscribe;
  /** Watch ops (once stored), refusals and sessions */
  observe<E extends keyof StoreEvents>(event: E, fn: (payload: StoreEvents[E]) => void): Unsubscribe;
  stats(): StoreStats;
  /** Forget what the retention window no longer needs */
  compact(): { deletions: number; replicas: number };
  export(): StoreDocument;
  restore(doc: StorageDocument): void;
  flush(): void;
  dispose(): void;
}

export function createStore<S extends object = any>(options?: StoreOptions<S>): Store<S>;

export class RefusedError extends Error {
  code: string;
  retryAfter?: number;
}
/** The most ops one hello carries */
export const HELLO_OPS: number;

// --- Many stores ---------------------------------------------------------------

export interface RegistryOptions {
  /** Release a store after this many ms without a session; default never */
  idle?: number;
  sweepEvery?: number;
  now?: () => number;
}

export interface StoreRegistry<S extends object = any> {
  get(id: string): Store<S> | null;
  has(id: string): boolean;
  ids(): string[];
  stats(): { stores: number; idle: number; sessions: number; replicas: number; records: number; log: number; sent: Record<string, { messages: number; bytes: number }> };
  restore(id: string, doc: StorageDocument): void;
  release(id: string): boolean;
  sweep(): string[];
  dispose(): void;
}

export function createStores<S extends object = any>(factory: (id: string) => Store<S> | null, options?: RegistryOptions): StoreRegistry<S>;
export function isStoreId(id: unknown): id is string;

// --- Hubs ------------------------------------------------------------------------

export type StoreResolver = (id: string) => Store | null;
export type Authorize = (user: unknown, storeId: string, store: Store, context?: { relay: unknown }) => boolean | Promise<boolean>;
export type AuthorizeId = (user: unknown, storeId: string, context?: { relay: unknown }) => boolean | Promise<boolean>;

export interface HubChannel {
  subscribe(storeId: string): void;
  unsubscribe(storeId: string): void;
  publish(storeId: string, message: Tagged<ServerMessage>): void;
}

export interface HubOptions {
  send(message: Tagged<ServerMessage> | { t: 'pong' } | { t: 'error'; message: string }): void;
  user?: unknown;
  authorizeId?: AuthorizeId;
  authorize?: Authorize;
  /** A follower's client: its credential judged as its own socket would be; the user, or null to refuse */
  vouch?(credential: unknown, storeId: string): unknown | Promise<unknown>;
  /** The vouches turned away (unauthorized, forbidden or refused) this socket may ask for before every vouch is refused as rate-limited (default { burst: 100, perSecond: 10 }); false for none */
  vouchRate?: { burst: number; perSecond: number } | false;
  now?: () => number;
  channel?: HubChannel;
  httpSnapshots?: { url(storeId: string): string; threshold: number };
  onError?(error: unknown): void;
}

export interface Hub {
  receive(message: unknown): void;
  close(): void;
  readonly stores: string[];
  readonly user: unknown;
  revalidate(filter?: (user: unknown, storeId: string) => boolean): Promise<number>;
  /** On a follower's socket: take back the clients `filter` picks */
  revoke(filter: (user: unknown) => boolean, code?: string, message?: string): number;
}

export function createHub(resolveStore: StoreResolver, options: HubOptions): Hub;

/** A client's credential as a relay vouches with it: the headers and query its own socket would carry */
export interface RelayCredential {
  headers?: Record<string, string>;
  query?: string;
}

export function toJSON(message: object): string;
export function tagStore<M extends object>(message: M, store: string): M & { store: string };
export function snapshotResponse(store: Store, request: Request, options?: { origins?: '*' | false | string[] }): Response;
