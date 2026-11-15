// Type declarations for `lazy-storage/server`: stores, storage adapters,
// the registry, and hubs. Hand-written; the runtime is plain JS.
import type { Unsubscribe } from 'lazy-watch';
import type { ActivityOptions, Change, ClientMessage, HistoryCursor, HistoryEntry, HistoryOptions, Lost, Op, Path, Peer, RecordClock, Schema, When, ServerMessage, Shape, ShapeOptions, Tagged, Timestamp } from './core.js';

export type { ClientMessage, ServerMessage, Tagged, Schema, SchemaCheck } from './core.js';
export { optional, oneOf, listItem, defineSchemas } from './core.js';
export type { Infer, InferState } from './core.js';

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
  /** A row per record the write wrote, for a store made with `history` */
  history?: Array<HistoryRow & { c: string; id: string | null }>;
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
  /** A record's history rows, newest first: older than version `before`, no later than `upToV` or landed by `upToAt`, not before `fromAt`, of one user's writes (for a store made with `history`) */
  history?(query: { c: string; id: string | null; before?: number; upToV?: number; upToAt?: number; fromAt?: number; user?: string; limit?: number }): HistoryRow[];
  /** The rows one write left, each with its `c` and `id` */
  historyOfVersion?(v: number): Array<HistoryRow & { c: string; id: string | null }>;
  /** Every record's rows, each with its `c` and `id`, in order of version (newest first, or oldest with `ascending`) and of their place in it; `cursor` a row to read on after */
  activity?(query: { before?: number; after?: number; fromAt?: number; toAt?: number; user?: string; c?: string; deleted?: boolean; ascending?: boolean; cursor?: HistoryCursor; limit?: number }): Array<HistoryRow & { c: string; id: string | null }>;
  /** How many history rows it holds */
  historyCount?(): number;
  /** Optional: what the history holds, for store.historySize (rows of records' writes, their bytes, the oldest `at`) */
  historySize?(): HistorySize;
  /** Forget the rows that landed before that time, but for each record's last one before it (not kept when that is a deletion) */
  pruneHistory?(before: number): void;
}

/** A record as one write left it, as storage keeps it: `data` and `changes` are JSON, `data` null for a record deleted */
export interface HistoryRow {
  c?: string;
  id?: string | null;
  /** The version the write made, and the row's place among that version's rows */
  v: number;
  n: number;
  /** The write's own stamp, and the store's time when it landed (which never goes back from one version to the next) */
  ts: Timestamp;
  at: Timestamp;
  user: string | null;
  replica: string | null;
  data: string | null;
  changes: string;
}

export type { HistoryEntry, When, HistoryOptions, ActivityOptions, HistoryCursor } from './core.js';

export function memoryStorage(): ServerStorage & { replace(doc: StorageDocument): void };
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
  /** false: writes nothing as itself, only as the clients vouched for on it (default true) */
  writes?: boolean;
}

/** What a rule is handed besides the user and the record */
export interface RuleContext<S extends object = any> {
  c: string;
  id: string | undefined;
  /** The store's state as the op leaves it, for a rule that looks at other records */
  state: S;
  /** The replica the op came from */
  replicaId: string;
  /** The op's changes to this record */
  changes: Change[];
  store: Store<S>;
}

/** May the user write this record? `before` as it is, `after` as the merge leaves it; null where there is none. false or a throw refuses */
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
  /**
   * Per collection or singleton, what a record may hold: a write that leaves a record otherwise is refused
   * whole, a client's with `invalid` and the server's own with a throw
   */
  schemas?: { [K in keyof S]?: Schema };
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
  /** Replicas remembered per user (default 1000; those of sessions without a user share one allowance): past it, the user's replicas not seen for longest and not live are forgotten */
  maxReplicas?: number;
  rateLimit?: RateLimit | false;
  storage?: ServerStorage;
  /** Commit what a turn of the event loop merged together (default true) */
  groupCommit?: boolean;
  migrations?: Array<(state: S, context: { store: Store<S> }) => object | void>;
  /**
   * Keep every record as each write left it, with when and who, for `store.history`, `recordAt` and `revert`;
   * `keep` (ms) forgets what is older, but for the last form of each record before it. Off by default
   */
  history?: boolean | {
    keep?: number;
    /** Who of the clients may read a record's history (`db.historyOf`): true, or a function; nobody by default */
    clients?: boolean | ((user: unknown, context: { c: string; id: string | undefined; store: Store<S> }) => boolean);
  };
  /**
   * Who a write of the server's is made for when it says no `by` of its own (patch, del, revert): asked at each
   * such write, and answered as `by` is given. For an app that knows whom it serves from a request's context
   */
  by?: () => WriteOptions['by'] | undefined;
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
  /** The changes that lost, with why (an op whose every change lost is told too, with none landed) */
  lost: Lost[];
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
  /** Replicas' ops heard since the store was loaded */
  ops: number;
  /** The server's own writes (patch, del, revert) since the store was loaded */
  writes: number;
  /** Changes that lost since it was loaded, by why: newer, stale, missing */
  lost: Record<string, number>;
  /** Ops and requests refused since it was loaded, by code: forbidden, invalid, expired, rate-limited… */
  refused: Record<string, number>;
}

/** An op or request a store turned away: when (its clock), whose (a user's key), which op, and why */
export interface Refusal {
  at: number;
  replicaId: string | null;
  seq: number | null;
  user: string | null;
  code: string;
  message: string;
}

/** What a store's history holds in its storage; `bytes` and `oldest` null from an adapter that tells only its rows */
export interface HistorySize {
  rows: number;
  bytes: number | null;
  oldest: number | null;
}

export interface StoreDocument extends StorageDocument {
  schema: number;
}

/** A write of the server's made for someone: `by` is told in the history as that user's, and to listeners */
export interface WriteOptions {
  /** The user's key as it is (a string), or a user as the store keys its users (presence.key); null or absent: nobody */
  by?: string | Record<string, unknown> | null;
}

export interface Store<S extends object = any> {
  /** The state: read it, never write it (patch, del and apply write) */
  readonly state: S;
  readonly shape: Shape;
  readonly version: number;
  readonly epoch: string;
  readonly sessions: number;
  readonly disposed: boolean;
  /** The time on the store's clock: the wall clock's, never before a timestamp the store has made or accepted */
  now(): number;
  /**
   * The server's own write, in the tree's shape: merged, missing containers and records made, stamped now.
   * `by`: the user it is made for, told in the history and to listeners (a string as the user's key, an object
   * keyed as the store keys its users)
   */
  patch(diff: Record<string, any>, options?: WriteOptions): Change[];
  /** Delete a record, or a path in one, as the server; the options last (a singleton's path in the id's place) */
  del(c: string, id?: string, path?: Path, options?: WriteOptions): Change[];
  del(c: string, id: string, options: WriteOptions): Change[];
  del(singleton: string, path?: Path, options?: WriteOptions): Change[];
  del(singleton: string, options: WriteOptions): Change[];
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
  /** `meta.user`: who wrote, as a key: a replica's user, or the `by` of a write of the server's; null for nobody */
  on(listener: (diff: Record<string, any>, meta: { origin: 'remote' | 'server'; replicaId: string; user: string | null; v: number; changes: Change[] }) => void): Unsubscribe;
  /** Watch ops (once stored), refusals and sessions */
  observe<E extends keyof StoreEvents>(event: E, fn: (payload: StoreEvents[E]) => void): Unsubscribe;
  stats(): StoreStats;
  /** What its history holds in storage, null with history off; it reads every row in SQLite, so not for every write */
  historySize(): HistorySize | null;
  /** The last 50 ops and requests it turned away, newest first, since it was loaded */
  refusals(): Refusal[];
  /** Forget what the retention window no longer needs */
  compact(): { deletions: number; replicas: number };
  /** A record's history, newest first: `limit` entries (default 50); `since`, `until`, `user` and `path` narrow it, and `cursor`, the last entry of an answer, reads on after it */
  historyOf(c: string, id: string, options?: HistoryOptions): HistoryEntry[];
  historyOf(singleton: string, options?: HistoryOptions): HistoryEntry[];
  /** A record as it was then (now, without `when`); undefined when it was not there, or that is before the store kept history */
  /**
   * The writes to every record, newest first, an entry per record a write wrote: narrowed by time, user,
   * collection or to the deletions; `oldestFirst` and `after` follow the store from a version on; `cursor`, the
   * last entry of one answer, reads on after it
   */
  activity(options?: ActivityOptions): HistoryEntry[];
  recordAt(c: string, id: string, when?: When): any;
  recordAt(singleton: string, when?: When): any;
  /**
   * Put a record back as it was then, as a write of the server's; returns the changes that landed. A record
   * made again brings back the children its deletion took with it, as they were just before, unless `children: false`.
   * `by` is the user the server makes the write for (an administrator), told with it in the history
   */
  revert(c: string, id: string, when: When, options?: WriteOptions & { children?: boolean }): Change[];
  revert(singleton: string, when: When, options?: WriteOptions): Change[];
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
/** `context` is `{ relay }` from an adapter for a client a relay vouches for, `{ vouched: true }` from a hub of one's own */
export type Authorize = (user: unknown, storeId: string, store: Store, context?: { relay: unknown } | { vouched: true }) => boolean | Promise<boolean>;
export type AuthorizeId = (user: unknown, storeId: string, context?: { relay: unknown } | { vouched: true }) => boolean | Promise<boolean>;

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
  /** On a follower's socket, whether the follower may write as itself (default true; the adapters' relay route: only with relays.authorize) */
  followerWrites?: boolean;
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

/**
 * Who may look into the server with the inspector, and with `write` change (a revert, a disconnect): a falsy answer,
 * or a throw, turns the request away. An answer that is a user (an object, or a string for its key) is who a revert
 * is told as in the store's history
 */
export type InspectorAuthorize = (request: Request, context: { write: boolean }) => unknown | Response | Promise<unknown | Response>;

export interface InspectorOptions {
  stores: StoreRegistry | StoreResolver;
  /** Required: the inspector shows every record of every store, and lets nobody in by default */
  authorize: InspectorAuthorize;
  /** The server's open sockets, as an adapter's `sockets()` lists them, and its `disconnect`: the adapters give their own */
  sockets?: () => Array<{ user?: unknown; relay?: unknown; stores: string[]; buffered: number; idleMs: number; openMs: number; expiresAt?: number | null }>;
  disconnect?: (filter: (user: unknown) => boolean) => number;
  /** A user's name by its key, shown beside the key in history, activity, sessions and sockets, and found by; nothing shows the key alone */
  nameOf?: (key: string) => string | null | undefined | Promise<string | null | undefined>;
  onError?: (error: any) => void;
}

/**
 * The inspector: a page of the server's stores, records, history, activity and sessions, and what the page asks.
 * `handle` answers a request under `base` (the page at it, its requests beneath it), or null for another path
 */
export function createInspector(options: InspectorOptions): { handle(request: Request, base: string): Promise<Response | null> };
