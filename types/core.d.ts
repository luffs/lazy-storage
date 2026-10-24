// Type declarations for `lazy-storage/core`: the pieces both sides share,
// and the wire protocol they speak. Hand-written; the runtime is plain JS.

/** Milliseconds: a replica stamps causally, the server clamps (see clock.js) */
export type Timestamp = number;

/** A path inside a record, one segment per level */
export type Path = string[];

/** One change to one record: put (make, or merge into), set (write whole), or del */
export type Change =
  | { c: string; id?: string; path?: Path; put: Record<string, unknown> }
  | { c: string; id?: string; path?: Path; set: unknown }
  | { c: string; id?: string; path?: Path; del: true };

/** One op: a replica's batch, numbered and timestamped */
export interface Op {
  replica: string;
  seq: number;
  ts: Timestamp;
  changes: Change[];
}

/** Why a change did not land */
export type LostReason = 'missing' | 'newer' | 'stale';
export interface Lost { c: string; id?: string; path: Path; why: LostReason }

// --- Time ---------------------------------------------------------------------

export interface Clock {
  /** A timestamp for a local event: never below one seen, never repeated */
  stamp(): Timestamp;
  /** A timestamp received: later stamps sort after it */
  observe(ts: unknown): void;
  /** The server's time as it said it: adopted as an offset */
  adopt(serverNow: unknown): void;
  readonly time: number;
  readonly offset: number;
  readonly highest: number;
}
export function createClock(options?: { now?: () => number }): Clock;
export function isTimestamp(value: unknown): value is Timestamp;
export function clamp(ts: number, limit: number): number;

// --- Shape --------------------------------------------------------------------

/** A register path: `'tasks/*\/geo'`, or segments; `*` matches one segment */
export type RegisterSpec = string | string[];

/** What a store holds, declared alike on the server and every client */
export interface ShapeOptions {
  /** The top level: collections as `{}`, singletons with their starting value */
  initial?: Record<string, unknown>;
  /** Top-level keys that are one record rather than a collection */
  singletons?: string[];
  /** Paths whose value is one leaf, written and merged whole */
  registers?: RegisterSpec[];
  /** `{ child: { field: 'parentCollection' } }`: children not let in without their parent, and deleted with it */
  parents?: Record<string, Record<string, string>>;
}

export interface Shape {
  readonly initial: Record<string, unknown>;
  readonly hash: string;
  readonly collections: string[];
  readonly singletons: string[];
  has(key: string): boolean;
  isCollection(key: string): boolean;
  isSingleton(key: string): boolean;
  full(c: string, id: string | undefined, path?: Path): Path;
  isRegister(c: string, id: string | undefined, path?: Path): boolean;
  recordKey(c: string, id?: string): string;
}
export function createShape(options?: ShapeOptions): Shape;

export class ModelError extends TypeError {
  path: Path;
}
export function assertChange(shape: Shape, change: unknown): asserts change is Change;
export function assertValue(value: unknown, path: Path): void;
export function isForbiddenKey(key: string): boolean;

// --- The rule -------------------------------------------------------------------

/** A record's clock: times by path key, of what lives and what was deleted inside it */
export interface RecordClock {
  live: Record<string, Timestamp>;
  gone: Record<string, Timestamp>;
}

export type Mode = 'judge' | 'authority' | 'trust' | 'replay' | 'optimistic';

export interface Doc {
  readonly state: Record<string, any>;
  get(c: string, id?: string): unknown;
  childrenOf(c: string, id: string): Array<[string, string]>;
  /** Decide one change at `ts`: what landed, and what lost with why */
  apply(change: Change, ts: Timestamp, mode?: Mode): { applied: Change[]; lost: Lost[] };
  setRecord(c: string, id: string | undefined, data: unknown, clock?: RecordClock | null): void;
  clock(c: string, id?: string): RecordClock;
  newest(c: string, id?: string): number;
  prune(cutoff: number): Array<[string, string | undefined]>;
  records(): Iterable<[string, string | undefined, unknown, RecordClock]>;
}
export function createDoc(shape: Shape, options?: { state?: Record<string, any>; clocks?: Map<string, unknown>; track?: boolean }): Doc;
export function docFromRecords(shape: Shape, records?: Array<[string, string | null | undefined, unknown, RecordClock?]>): Doc;
export function changeFor(base: { c: string; id?: string; path?: Path }, value: unknown): Change;
/** Changes as one diff in the tree's shape, null for a deletion */
export function diffOf(shape: Shape, changes: Change[]): Record<string, any>;
export function clockToJSON(clock: unknown): RecordClock;
export function clockFromJSON(json: RecordClock): unknown;

/** A lazy-watch batch as the changes it makes */
export function compile(shape: Shape, diff: unknown, inverse: unknown, state: unknown): Change[];

// --- Paths --------------------------------------------------------------------------

export function pathKey(path: Path): string;
export function parsePathKey(key: string): Path;
export function registerKey(spec: RegisterSpec): Path;
export function registerSet(specs?: RegisterSpec[]): { readonly patterns: Path[]; readonly size: number; matches(path: Path): boolean };
export function setAt<T extends object>(target: T, path: Path, value: unknown): T;
export function valueAt(root: unknown, path: Path): unknown;
export function deleteAt(target: object, path: Path): void;

// --- Positions ----------------------------------------------------------------------

export function keyBetween(a: string | null, b: string | null): string;
export function keysBetween(a: string | null, b: string | null, n: number): string[];
export function comparePositions(keyA: unknown, idA: string, keyB: unknown, idB: string): number;
export function isPositionKey(key: unknown): key is string;

export function randomId(): string;
/** The wire protocol's version, said in every hello */
export const PROTOCOL: number;

// --- The wire protocol ------------------------------------------------------------

export type ClientMessage =
  | { t: 'hello'; store?: string; proto: number; replica: string; ops: Op[]; since?: number; epoch?: string | null; share?: unknown; presence?: boolean; fetch?: boolean; clocks?: boolean; follower?: boolean }
  | { t: 'op'; store?: string; op: Op; as?: string }
  | { t: 'leave'; store?: string; as?: string }
  | { t: 'share'; store?: string; data: unknown; as?: string }
  | { t: 'vouch'; store?: string; as: string; credential: unknown; replica: string }
  | { t: 'ping' };

export interface Peer { replicaId: string; user?: unknown; key?: string; data?: unknown }

export type ServerMessage =
  | { t: 'snapshot'; store?: string; state?: Record<string, any>; fetch?: string; clocks?: Array<[string, string | null, RecordClock]>; v: number; epoch: string | null; seq: number; now: number; shape: string; lost?: Array<{ seq: number; lost: Lost[] }>; relay?: true }
  | { t: 'delta'; store?: string; ops: Array<{ v: number; ts: Timestamp; changes: Change[] }>; v: number; epoch: string | null; seq: number; now: number; shape: string; lost?: Array<{ seq: number; lost: Lost[] }>; relay?: true }
  | { t: 'patch'; store?: string; changes: Change[]; v: number; ts: Timestamp }
  | { t: 'ack'; store?: string; seq: number; v: number; now: number; lost?: Lost[]; as?: string; replica?: string }
  | { t: 'error'; store?: string; seq?: number; code?: string; message: string; retryAfter?: number; as?: string; replica?: string }
  | { t: 'presence'; store?: string; peers?: Peer[]; left?: string[]; joined?: Peer[]; shared?: Array<{ replicaId: string; data?: unknown }> }
  | { t: 'relay'; store?: string; status: 'local' | 'through' }
  | { t: 'closed'; store?: string; code: string; message: string }
  | { t: 'admitted'; store?: string; as: string }
  | { t: 'refused' | 'revoked'; store?: string; as: string; code: string; message?: string; retryAfter?: number }
  | { t: 'reset'; store?: string; previous: { epoch: string | null } }
  | { t: 'pong' };

/** A message as it travels on a socket that carries many stores */
export type Tagged<M> = M & { store: string };

/** Why the server ended a store (or, without one, the socket) for a client */
export type ClosedCode = 'evicted' | 'forbidden' | 'unknown-store' | 'invalid-store' | 'unauthorized' | 'replica-taken' | 'outdated' | 'unavailable' | 'reauthenticate';

/** Why the server refused an op */
export type ErrorCode = 'invalid' | 'forbidden' | 'expired' | 'too-large' | 'rate-limited';
