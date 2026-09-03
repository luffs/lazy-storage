// Type declarations for `lazy-storage/core`: the pieces both sides share,
// and the wire protocol they speak. Hand-written; the runtime is plain JS.
import type { ChangeSet } from 'lazy-watch';

/** A hybrid-logical-clock timestamp: wall ms, a counter within the ms, the replica as tie-break */
export type Timestamp = [ms: number, count: number, replicaId: string];

/** A path into the state, one segment per level */
export type Path = string[];

/**
 * A plain lazy-watch diff: nested objects for containers, `null` for a
 * deletion, and arrays only at register paths, where they are whole
 * values.
 */
export type Diff = ChangeSet;

/** One op: a replica's batch, numbered and timestamped */
export interface Op {
  replicaId: string;
  seq: number;
  ts: Timestamp;
  diff: Diff;
}

// --- Clocks -----------------------------------------------------------------

export interface Clock {
  readonly replicaId: string;
  /** A timestamp for a local event; strictly greater than every earlier one */
  now(): Timestamp;
  /** Observe a remote timestamp so later local events sort after it */
  receive(ts: unknown): void;
  /** Fall back to the wall clock after it was corrected, never behind what was received */
  rewind(): void;
  /** The current value without advancing */
  peek(): Timestamp;
}

export function createClock(replicaId: string, now?: () => number): Clock;
/** Total order over timestamps; a missing one sorts first */
export function compareTs(a: Timestamp | undefined, b: Timestamp | undefined): number;
export function isTimestamp(value: unknown): value is Timestamp;

// --- Paths and registers ------------------------------------------------------

/** A register path: `'order'`, `'tasks/*\/subtaskOrder'`, or segments; `*` matches one segment */
export type RegisterSpec = string | string[];

export interface RegisterSet {
  readonly patterns: string[][];
  readonly size: number;
  /** True when some pattern has the path's length and matches every segment */
  matches(path: Path): boolean;
}

export function registerKey(spec: RegisterSpec): string[];
export function registerSet(specs?: RegisterSpec[]): RegisterSet;
/** The map key of a path: its JSON encoding */
export function pathKey(path: Path): string;
export function parsePathKey(key: string): Path;
/** The prefix shared by the keys of every strict descendant of a path */
export function descendantPrefix(path: Path): string;
export function setAt<T extends object>(target: T, path: Path, value: unknown): T;
export function valueAt(root: unknown, path: Path): unknown;
export function deleteAt(target: unknown, path: Path): void;

// --- The model ------------------------------------------------------------------

export class ModelError extends TypeError {
  readonly name: 'ModelError';
  readonly path: Path;
  constructor(message: string, path: Path);
}

/** A leaf of a diff: its path and value (`null` for a deletion) */
export type Leaf = [path: Path, value: unknown];

/** A persisted row on the client, or a row handed to `rebuild`: a path key and the leaf's value */
export type Row = [key: string, value: unknown];

/** Flatten a diff into leaves; throws a ModelError for an array outside a register */
export function leaves(diff: Diff, registers: RegisterSet, path?: Path, out?: Leaf[]): Leaf[];
export function assertModel(diff: Diff, registers: RegisterSet): void;
/** Replace every register fragment in a diff with the register's whole value from the live state */
export function expandRegisters(diff: Diff, registers: RegisterSet, state: object): Diff;
export function fromLeaves(entries: Leaf[]): Diff;
/** `initial` with rows applied on top, shallow paths first; a `null` value deletes the path */
export function rebuild<S extends object>(initial: S, rows: Row[]): S;
export function isArrayish(value: unknown): boolean;

// --- The merge --------------------------------------------------------------------

export interface ClockEntry {
  ts: Timestamp;
  deleted?: boolean;
}

export interface MergeResult {
  /** The winning leaves as a diff, or null when none won */
  accepted: Diff | null;
  /** Paths of the losing leaves */
  rejected: Path[];
  won: Leaf[];
  /** Clock keys removed: descendants of a winning write or deletion, lifted tombstones */
  dropped: string[];
}

/** The clock table with a children index, so a write finds its descendants without scanning every key */
export class ClockMap extends Map<string, ClockEntry> {
  constructor(entries?: Iterable<[string, ClockEntry]>);
  /** Keys of every entry strictly under `path` */
  descendants(path: Path): IterableIterator<string>;
}

/** With a ClockMap descendants come from its index; with a plain Map every key is scanned */
export function mergeOp(clocks: Map<string, ClockEntry>, ts: Timestamp, diff: Diff, registers: RegisterSet): MergeResult;
/** Forget tombstones older than a timestamp; returns the removed keys */
export function compactTombstones(clocks: Map<string, ClockEntry>, olderThan: Timestamp): string[];

export function randomId(): string;

// --- The wire protocol --------------------------------------------------------------

/** Why the server refused an op */
export type ErrorCode = 'invalid' | 'forbidden' | 'expired' | 'too-large' | 'rate-limited' | 'clock-skew';

/** Why the server ended a store for a client */
export type ClosedCode = 'evicted' | 'forbidden' | 'unknown-store' | 'invalid-store';

export type ClientMessage =
  | { t: 'hello'; replicaId: string; ops: Op[]; since?: number; epoch?: string | null }
  | { t: 'op'; op: Op }
  | { t: 'ping' }
  | { t: 'leave' };

export type ServerMessage =
  | { t: 'snapshot'; state: object; ts: Timestamp; seq: number; registers: string[]; v: number; epoch: string }
  | { t: 'delta'; patches: Diff[]; ts: Timestamp; seq: number; registers: string[]; v: number; epoch: string }
  | { t: 'patch'; diff: Diff; ts: Timestamp; v: number }
  | { t: 'ack'; seq: number; ts: Timestamp; correction: Diff | null }
  | { t: 'presence'; users: unknown[] }
  | { t: 'closed'; code: ClosedCode; message: string }
  | { t: 'error'; seq?: number; code?: ErrorCode; message: string; now?: number; ts?: Timestamp; retryAfter?: number }
  | { t: 'pong' };

/** On a multiplexed connection every message but ping and pong carries its store id */
export type Tagged<M> = M & { store: string };
