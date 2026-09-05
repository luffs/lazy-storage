// Type declarations for `lazy-storage/server`: stores, storage adapters,
// the registry, and hubs. Hand-written; the runtime is plain JS.
import type { ChangeListener, ListenerOptions, Unsubscribe } from 'lazy-watch';
import type { ClientMessage, Diff, Op, Path, Peer, RegisterSpec, ServerMessage, Tagged, Timestamp } from './core.js';

export type { ClientMessage, ServerMessage, Tagged } from './core.js';

// --- Storage adapters ---------------------------------------------------------

/** A persisted leaf: a live value with the timestamp that won it, or a tombstone */
export type StoredRow =
  | { value: unknown; ts: Timestamp; deleted?: false }
  | { ts: Timestamp; deleted: true };

export interface ReplicaProgress {
  seq: number;
  /** The store's clock when the replica's last op arrived */
  seen: number;
}

export interface LogEntry {
  v: number;
  diff: Diff;
}

export interface StorageDocument {
  rows: Array<[key: string, row: StoredRow]>;
  replicas: Record<string, ReplicaProgress>;
  version: number;
  /** Minted by the store with its first commit */
  epoch: string;
  /** The persisted delta log, ascending by version, when the adapter keeps it */
  log?: LogEntry[];
}

export interface StorageCommit {
  upserts: Array<[key: string, row: StoredRow]>;
  deletes: string[];
  replica?: { id: string; seq: number; seen: number };
  forgetReplicas?: string[];
  version: number;
  epoch: string;
  /** The accepted diff this op made, for adapters that keep the delta log */
  log?: LogEntry;
  /** Entries below this version are no longer needed */
  logFloor?: number;
}

export interface ServerStorage {
  /** null means never seen: the store starts from `initial` */
  load(): StorageDocument | null;
  commit(change: StorageCommit): void;
  /** Write out anything buffered; called on dispose */
  flush(): void;
}

/** Keeps the document in memory only (delta log included); for tests */
export function memoryStorage(): ServerStorage;
/** One JSON document per store, written atomically and debounced, without the delta log */
export function jsonFileStorage(file: string, options?: { debounce?: number }): ServerStorage;

// --- Stores -----------------------------------------------------------------------

export interface Session {
  readonly user: unknown;
  replicaId: string | null;
  receive(message: ClientMessage): void;
  close(): void;
}

export interface SessionOptions {
  send(message: ServerMessage): void;
  /** Whatever the transport authenticated; counted in presence and handed to `validate` */
  user?: unknown;
  /** Called after `closeSessions` closed this session */
  onEvict?(): void;
}

export interface ApplyResult {
  duplicate: boolean;
  accepted: Diff | null;
  rejected: Path[];
  correction: Diff | null;
}

export interface ValidateContext<S extends object = any> {
  user: unknown;
  replicaId: string;
  store: Store<S>;
}

export interface RateLimit {
  burst: number;
  perSecond: number;
}

/** What `presence.validate` sees with a share */
export interface ShareContext<S extends object = any> {
  user: unknown;
  replicaId: string;
  store: Store<S>;
}

export interface PresenceOptions<S extends object = any> {
  /** What presence groups users by; default `id`, else the user's JSON */
  key?(user: unknown): string;
  /** What of a user its peers see, and `presence()` shows; default all of it */
  user?(user: unknown): unknown;
  /**
   * Judges what a session shares, as `validate` judges an op: false or a
   * throw refuses it (code `forbidden`), a value is shared instead, true
   * or nothing lets it through. Clearing a share is never judged
   */
  validate?(data: unknown, context: ShareContext<S>): boolean | unknown | void;
  /** At most one presence message per this many milliseconds, changes within the window going out together; default 0 (one per turn of the event loop) */
  every?: number;
  /** The most a session may share, in bytes of JSON; default 4096 */
  maxShare?: number;
}

export interface StoreOptions<S extends object = any> {
  /** The skeleton: state when nothing is persisted, and the base rows are applied onto */
  initial?: S;
  /** Paths whose value is one unit (arrays live only here) */
  registers?: RegisterSpec[];
  /** Paths clients may not write; an op touching a leaf at or under one is refused whole */
  readOnly?: RegisterSpec[];
  /**
   * Judges every client op after the read-only check: return false or
   * throw to refuse, a diff to accept that instead, true or nothing to
   * accept as is. Synchronous
   */
  validate?(diff: Diff, context: ValidateContext<S>): boolean | Diff | void;
  /** How far ahead of the server's clock (ms) a client op may be stamped; default 5 minutes */
  maxSkew?: number;
  /** How long (ms) deletions and idle replicas are remembered; default 30 days */
  retention?: number;
  /** How often (ms of store time) compaction runs on its own; default one hour */
  compactEvery?: number;
  /** Accepted diffs kept for answering reconnects with deltas; default 1000, 0 disables */
  deltaLog?: number;
  /** The most leaves one client op may touch; default 10 000 */
  maxLeaves?: number;
  /** Live ops a replica may send; default `{ burst: 500, perSecond: 100 }`, false disables */
  rateLimit?: RateLimit | false;
  storage?: ServerStorage;
  /**
   * Whether sessions learn of each other; default false (nothing broadcast,
   * `presence()` and `peers()` empty, a share refused with `forbidden`).
   * true turns it on with the defaults; an object sets any of its options
   */
  presence?: boolean | PresenceOptions<S>;
  /** Server faults that are nobody's request; default console */
  onError?(error: unknown): void;
  /** Wall clock (injectable for tests) */
  now?: () => number;
}

export interface OpEvent {
  replicaId: string;
  seq: number;
  user: unknown;
  accepted: boolean;
  /** Leaves the op lost */
  rejected: number;
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
  user: unknown;
  replicaId: string | null;
  /** Live sessions after this one opened or closed */
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
  sessions: number;
  replicas: number;
  rows: number;
  tombstones: number;
  log: number;
}

export interface Store<S extends object = any> {
  /** The live state; read freely, write through `patch` */
  readonly state: S;
  readonly version: number;
  /** Identifies this life of the store's storage */
  readonly epoch: string;
  readonly sessions: number;
  /** Replica ids the store remembers progress for */
  readonly replicas: string[];
  /** Merge one op; with a session it is a client's and passes the gates, without one it is trusted */
  apply(op: Op, session?: Session): ApplyResult;
  /** A change from the server itself, timestamped now */
  patch(diff: Diff): ApplyResult;
  session(options: SessionOptions): Session;
  /** Evict every session the predicate selects; returns how many */
  closeSessions(predicate: (session: Session) => boolean, message?: string): number;
  /** Distinct users with a live session */
  presence(): unknown[];
  /** Every live session: its replica id, its user, and what it shares */
  peers(): Peer[];
  snapshot(): S;
  /** Subscribe to accepted changes (a lazy-watch listener on the state) */
  on(listener: ChangeListener<S>, options?: ListenerOptions): Unsubscribe;
  /** Watch ops, refusals, and sessions, for logs, audits, and metrics */
  observe<E extends keyof StoreEvents>(event: E, fn: (payload: StoreEvents[E]) => void): Unsubscribe;
  stats(): StoreStats;
  /** Forget what the retention window no longer needs */
  compact(): { tombstones: number; replicas: number };
  flush(): void;
  dispose(): void;
}

export function createStore<S extends object = any>(options?: StoreOptions<S>): Store<S>;

// --- Many stores ---------------------------------------------------------------

export interface RegistryOptions {
  /** Release a store after this many ms without a session; default never */
  idle?: number;
  /** How often to look (ms); default one minute */
  sweepEvery?: number;
  now?: () => number;
}

export interface StoreRegistry<S extends object = any> {
  /** The live store for `id`, created on first use; null for an invalid or refused id */
  get(id: string): Store<S> | null;
  has(id: string): boolean;
  ids(): string[];
  /** The live stores' stats rolled up: how many are live and idle (no session), and the sums across them */
  stats(): { stores: number; idle: number; sessions: number; replicas: number; rows: number; tombstones: number; log: number };
  /** Dispose a live store (its sessions close; persisted data stays) */
  release(id: string): boolean;
  /** Release every store idle for `idle` or longer; returns their ids */
  sweep(): string[];
  dispose(): void;
}

export function createStores<S extends object = any>(factory: (id: string) => Store<S> | null, options?: RegistryOptions): StoreRegistry<S>;
/** Store ids are restricted to a URL- and filename-safe alphabet */
export function isStoreId(id: unknown): id is string;

// --- Hubs ------------------------------------------------------------------------

export type StoreResolver = (id: string) => Store | null;
export type Authorize = (user: unknown, storeId: string, store: Store) => boolean | Promise<boolean>;

export interface HubOptions {
  send(message: Tagged<ServerMessage> | { t: 'pong' } | { t: 'error'; message: string }): void;
  user?: unknown;
  authorize?: Authorize;
  onError?(error: unknown): void;
}

/** The server side of a multiplexed connection: one store session per store on this socket */
export interface Hub {
  receive(message: unknown): void;
  close(): void;
  /** Store ids with a live session on this connection */
  readonly stores: string[];
  readonly user: unknown;
}

export function createHub(resolveStore: StoreResolver, options: HubOptions): Hub;

/** A message's JSON, encoded once and remembered on the object, however many sockets it goes to */
export function toJSON(message: object): string;
/** `{ ...message, store }`, keeping the payload's remembered JSON */
export function tagStore<M extends object>(message: M, store: string): M & { store: string };
