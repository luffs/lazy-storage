// Type declarations for `lazy-storage/relay`: a follower of the server. Hand-written.
import type { ShapeOptions, RecordClock, Op } from './core.js';
import type { TransportFactory } from './client.js';
import type { Rule } from './server.js';

/** 'through' while the server answers, 'local' once it has been away for the relay's grace */
export type RelayMode = 'through' | 'local';

/** A client's credential as a relay vouches with it: the headers and query its own socket would carry */
export interface RelayCredential {
  headers?: Record<string, string>;
  query?: string;
}

/** What a relay keeps of a store */
export interface RelayCopy {
  records: Array<[string, string | null, unknown, RecordClock]> | null;
  v: number;
  epoch: string | null;
  fepoch?: string;
  fv?: number;
  own: { seq: number; ops: Op[] };
  admitted: Record<string, string[]>;
}

/** Where a relay keeps its copies; `load` may return a promise (a browser's IndexedDB) */
export interface CopyStorage {
  loadMeta(): { replicaId: string } | null;
  saveMeta(meta: { replicaId: string }): void;
  load(storeId: string): RelayCopy | null | Promise<RelayCopy | null>;
  save(storeId: string, copy: RelayCopy): void;
  flush(): void;
}

/** A store's shape as the relay judges it, with the rules it can judge offline */
export interface RelayShape extends ShapeOptions {
  rules?: Record<string, Rule>;
  readOnly?: boolean;
}

export interface RelayOptions {
  /** A store's shape, or null for one the relay does not carry */
  shapeOf(storeId: string): RelayShape | null;
  /** A transport factory to the server's relay route, under the relay's own credential */
  link: TransportFactory;
  storage?: CopyStorage;
  /** 'upstream' (each client its own replica, acknowledged by the server) or 'local' (the clients are the relay's one replica; a browser's tabs) */
  acknowledge?: 'upstream' | 'local';
  /** How long (ms) the server may be away before the relay says it answers alone (default 10 000) */
  grace?: number;
  /** Whether to answer a client the server let in before, while it is away (default: yes) */
  authorizeOffline?(key: string, storeId: string): boolean;
  reconnect?: { min: number; max: number } | false;
  keepalive?: number | false;
  /** The relay's own replica (default: the one storage keeps, else new) */
  replicaId?: string;
  onError?(error: unknown): void;
  now?(): number;
}

export interface RelaySession {
  receive(message: unknown): void;
  close(): void;
  evict(): void;
}

export interface RelayStats {
  mode: RelayMode;
  link: 'offline' | 'connecting' | 'online';
  stores: Record<string, { sessions: number; queue: number; v: number; synced: boolean; waiting: Array<{ replica: string; seq: number; sent: boolean; own?: true; grant?: string }> }>;
}

export interface Relay {
  readonly replicaId: string;
  readonly mode: RelayMode;
  /** The link to the server */
  readonly status: 'offline' | 'connecting' | 'online';
  /** A client's socket: `credential` what the server judges it by, `key` a fingerprint of it */
  session(options: { send(message: object): void; credential?: unknown; key?: string; user?: unknown; onEvict?(): void }): RelaySession;
  /** The relay's own write to a store, in the tree's shape, as an op of its own replica */
  patch(storeId: string, diff: Record<string, any>): unknown[];
  /** What the relay shows of a store: its copy with the queue on top */
  copy(storeId: string): Record<string, any> | undefined;
  /** The relay's own ops on a store the server has not acknowledged */
  pending(storeId: string): number;
  on(event: 'mode', fn: (mode: RelayMode) => void): () => void;
  on(event: 'status', fn: (status: string) => void): () => void;
  on(event: 'sync', fn: (info: { store: string; pending: number }) => void): () => void;
  on(event: 'error', fn: (error: unknown) => void): () => void;
  /** The host's word that the server is away, or back */
  upstreamDown(): void;
  upstreamUp(): void;
  stats(): RelayStats;
  close(): void;
}

export function createRelay(options: RelayOptions): Relay;
export function memoryCopies(): CopyStorage;
/** One JSON file per store under `dir`, written whole through a rename at most every `saveDelay` */
export function fileCopies(dir: string, options?: { saveDelay?: number; onError?(error: unknown): void }): CopyStorage;
