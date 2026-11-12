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

/** A record's clock: when it was made (`at`, the time of every path with no entry of its own; left out once older than retention), and times by path key of what was written since and what was deleted inside it */
export interface RecordClock {
  at?: Timestamp;
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

// --- Schemas ------------------------------------------------------------------------

type TypeName = 'string' | 'number' | 'integer' | 'boolean' | 'any';

/** A check of a value: false refuses, a string refuses with that message, a throw with its own */
export type SchemaCheck = (value: any, context: { c: string; id: string | undefined }) => boolean | string | void;

/** A check that says, to TypeScript alone, what type the values it lets through have (`oneOf` returns one) */
export type TypedCheck<T> = SchemaCheck & { readonly '~type'?: T };

/** A field that may be absent: what `optional(schema)` returns */
export interface OptionalSchema<S = unknown> { readonly '~optional': S }

/** Another library's validator, asked whether a value is valid and no more: Standard Schema (Zod, Valibot, ArkType), or an instance of a library's class with `safeParse` or `parse` (a plain object is read as an object of fields) */
export type ForeignSchema =
  | { readonly '~standard': { validate(value: unknown): unknown } }
  | { safeParse(value: unknown): unknown }
  | { parse(value: unknown): unknown };

/**
 * What a value may hold: a type's name (with `?` for a field that may be
 * absent), an object of fields (`'*'` for every field not named), a list
 * written `[schema]`, a check, another library's validator, or `optional(schema)`
 */
export type Schema =
  | TypeName
  | `${TypeName}?`
  | SchemaCheck
  | readonly [Schema]
  | { readonly [field: string]: Schema }
  | ForeignSchema
  | OptionalSchema;

type TypeOf<N> = N extends 'string' ? string : N extends 'number' | 'integer' ? number : N extends 'boolean' ? boolean : any;
type MayBeAbsent<S> = S extends `${string}?` ? true : S extends OptionalSchema ? true : false;
type Simplify<T> = { [K in keyof T]: T[K] } & {};
type FieldsOf<S> = Simplify<
  & { -readonly [K in keyof S as K extends '*' ? never : MayBeAbsent<S[K]> extends true ? never : K]: Infer<S[K]> }
  & { -readonly [K in keyof S as K extends '*' ? never : MayBeAbsent<S[K]> extends true ? K : never]?: Infer<S[K]> }
> & ('*' extends keyof S ? { [field: string]: Infer<S['*']> } : unknown);

/**
 * The type of the values a schema lets through, so a record's form is
 * declared once: `type Task = Infer<typeof schemas.tasks>`. The schema
 * has to keep its literal types, so declare it `as const` (or inline, in
 * `defineSchemas`). A check written by hand is `any`; `oneOf` is the
 * union of its values; another library's validator is its own output type
 */
export type Infer<S> =
  S extends `${infer N}?` ? TypeOf<N>
  : S extends string ? TypeOf<S>
  : S extends OptionalSchema<infer I> ? Infer<I>
  : S extends { readonly '~standard': { readonly types?: { readonly output: infer O } } } ? (unknown extends O ? any : O)
  : S extends { parse(value: unknown): infer O } ? O
  : S extends { safeParse(value: unknown): unknown } ? any
  : S extends (...args: any[]) => unknown ? (S extends { readonly '~type'?: infer T } ? (unknown extends T ? any : T) : any)
  : S extends readonly [infer I] ? Infer<I>[]
  : S extends object ? FieldsOf<S>
  : any;

/**
 * A store's state from its schemas: a collection is its records by id,
 * a singleton (named in `Singletons`) one record.
 * `type State = InferState<typeof schemas, 'settings'>`
 */
export type InferState<S, Singletons extends keyof S = never> = {
  -readonly [K in keyof S]: K extends Singletons ? Infer<S[K]> : Record<string, Infer<S[K]>>;
};

/** A store's schemas with their literal types kept, for `Infer` and `InferState`: returns what it is given */
export function defineSchemas<const S extends Record<string, Schema>>(schemas: S): S;

/** Why a record broke its schema: `path` from the store's root */
export class SchemaError extends Error {
  code: 'invalid';
  path: Path;
}
/** The fields a record's schema names, each by its path, and whether it lets in others (`'*'`); null for a schema that is not an object of fields */
export function fieldsOf(schema: Schema): { fields: Path[]; open: boolean } | null;
/** A field that may be absent, for a schema that is not a type's name */
export function optional<const S extends Schema>(schema: S): OptionalSchema<S>;
/** One of these values and no other */
export function oneOf<const V extends readonly unknown[]>(...values: V): TypedCheck<V[number]>;
/** The record of a list: its fields, and the `id` and position key (`position`, default 'pos') a list writes on it */
// The fields given are spread last, so one of them named `id` or the position key is the one that holds
export function listItem<const F extends Record<string, Schema>>(fields?: F): Omit<{ readonly id: 'string?'; readonly pos: 'string?' }, keyof F> & F;
export function listItem<const F extends Record<string, Schema>, const P extends string>(fields: F, options: { position: P }): Omit<{ readonly id: 'string?' } & { readonly [K in P]: 'string?' }, keyof F> & F;
/** The schemas of a store as they are judged: `check` says what is wrong with a record, or null */
export function createSchemas(shape: Shape, schemas?: Record<string, Schema>): {
  readonly size: number;
  has(c: string): boolean;
  check(c: string, id: string | undefined, record: unknown): SchemaError | null;
};

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
  | { t: 'history'; store?: string; req: number; c: string; id?: string; limit?: number; before?: number; cursor?: HistoryCursor; path?: Path; since?: number; until?: number; user?: string; when?: number | { ts: number } | { v: number }; as?: string }
  | { t: 'history'; store?: string; req: number; all: true; limit?: number; before?: number; after?: number; since?: number; until?: number; user?: string; collection?: string; deleted?: boolean; oldestFirst?: boolean; cursor?: HistoryCursor; as?: string }
  | { t: 'vouch'; store?: string; as: string; credential: unknown; replica: string }
  | { t: 'ping' };

/** One write to a record, as `historyOf` and `activity` tell it, the store's and a client's alike */
export interface HistoryEntry {
  /** The record it is of: its collection (or singleton), and its id, which a singleton has none of */
  c: string;
  id?: string;
  /** The store's version the write made, and its time on the store's clock */
  v: number;
  ts: Timestamp;
  /** The replica that wrote ('server' for the store's own write), and the key of its user, as presence keys users */
  replicaId: string;
  user?: string;
  /** What the write did to this record; none for the row a record has from when history was turned on */
  changes: Change[];
  /** The record as the write left it; undefined: deleted */
  record: any;
}

/** A time on the store's clock (ms, a Date, `{ ts }`) or a version (`{ v }`) */
export type When = number | Date | { ts: number | Date } | { v: number };

/** Where an answer stopped, to read on after: an entry's version and record. The last entry of an answer will do */
export interface HistoryCursor { v: number; c: string; id?: string | null }

/** What `historyOf` and `activity` both take: how many, from when to when, whose, and where to read on from */
export interface HistoryRange {
  /** How many entries (default 50) */
  limit?: number;
  /** Times on the store's clock */
  since?: number | Date;
  until?: number | Date;
  /** Older than this version */
  before?: number;
  /** A user's key, as presence keys users */
  user?: string;
  /** The last entry of an answer, to read on after it */
  cursor?: HistoryCursor;
}

/** Which of one record's writes */
export interface HistoryOptions extends HistoryRange {
  /** Only the writes at, above or beneath this path in the record */
  path?: Path;
}

/** Which of the store's writes */
export interface ActivityOptions extends HistoryRange {
  /** Newer than this version */
  after?: number;
  collection?: string;
  /** The deletions alone */
  deleted?: boolean;
  /** Oldest first: with `after`, the store followed from a version on */
  oldestFirst?: boolean;
}

export interface Peer { replicaId: string; user?: unknown; key?: string; data?: unknown }

export type ServerMessage =
  | { t: 'snapshot'; store?: string; state?: Record<string, any>; fetch?: string; clocks?: Array<[string, string | null, RecordClock]>; v: number; epoch: string | null; seq: number; now: number; shape: string; lost?: Array<{ seq: number; lost: Lost[] }>; relay?: true }
  | { t: 'delta'; store?: string; ops: Array<{ v: number; ts: Timestamp; changes: Change[] }>; v: number; epoch: string | null; seq: number; now: number; shape: string; lost?: Array<{ seq: number; lost: Lost[] }>; relay?: true }
  | { t: 'patch'; store?: string; changes: Change[]; v: number; ts: Timestamp }
  | { t: 'ack'; store?: string; seq: number; v: number; now: number; lost?: Lost[]; as?: string; replica?: string }
  | { t: 'history'; store?: string; req: number; entries?: HistoryEntry[]; more?: HistoryCursor; record?: unknown; as?: string }
  | { t: 'error'; store?: string; seq?: number; req?: number; code?: string; message: string; retryAfter?: number; as?: string; replica?: string }
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
