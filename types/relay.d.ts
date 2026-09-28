// Type declarations for `lazy-storage/relay`. Hand-written.
import type { TransportFactory } from './client.js';
import type { Unsubscribe } from 'lazy-watch';

/** 'through' while the server answers, 'local' while the relay answers from its copies */
export type RelayMode = 'through' | 'local';

/** A client socket's state at the relay: waiting to be dialled through, passed through, answered from the copies, or fanned out (read on the relay's link) */
export type RelaySocketState = 'dialing' | 'through' | 'local' | 'fan' | 'closed';

/** Fan-out: what the server's relay route judges a client by, the headers and query its own socket would carry */
export interface RelayCredential {
  headers?: Record<string, string>;
  query?: string;
}

/** One store's copy as a document: what `write` is handed and `load` gives back */
export interface CopyDocument {
  format: 'lazy-storage/relay-copy';
  store: string;
  state: object;
  v: number;
  epoch: string;
  /** Epochs it moved on from (a restore at the server), oldest first */
  pastEpochs: string[];
  registers: string[];
  clocks: Array<[string, { ts: [number, number, string]; deleted?: true }]>;
  seen: Array<[string, number]>;
  maxTs: [number, number, string] | null;
  presenceOn: boolean;
  users: Array<[string, { user: unknown; key: string }]>;
  diverged: boolean;
  localV: number;
  localLog: object[] | null;
  usedAt: number;
}

/** The credentials the server answered, and its clock as last heard */
export interface KnownDocument {
  format: 'lazy-storage/relay-known';
  centralTs: number | null;
  /** Per credential, the replicas the server answered it for on each store, the most recently answered last */
  keys: Array<[string, { at: number; stores: Record<string, { replicaIds: string[] }> }]>;
}

/**
 * Where a relay keeps its copies. The relay decides when to write; the
 * adapter reads and writes synchronously, and copies what it is handed
 * before it returns
 */
export interface CopyStorage {
  load(): { copies: CopyDocument[]; known: KnownDocument | null } | null;
  write(storeId: string, doc: CopyDocument): void;
  remove?(storeId: string): void;
  writeKnown(doc: KnownDocument): void;
}

export interface RelayOptions {
  /** memoryCopies() (the default) or fileCopies(dir) */
  storage?: CopyStorage;
  /** How long (ms) the server may fail to answer before the relay answers on its own (default 10 000) */
  grace?: number;
  /** A dial not open by then has failed (ms, default 4000) */
  dialTimeout?: number;
  /** While local, whether the server answers again; by default a client's upstream is dialled. false leaves it to upstreamUp() */
  probe?: (() => boolean | Promise<boolean>) | false;
  /** Default 5000 */
  probeEvery?: number;
  /** The relay's own pings on sockets it passes through (ms, default 15 000); a server silent for two is taken for gone. false disables */
  keepalive?: number | false;
  /** The most (ms) the relay waits, at random, before sending its clients back to a server that is back (default 1000) */
  jitter?: number;
  /** An offline op stamped further ahead (ms) of the relay's reference time stays unapplied (default 300 000) */
  maxSkew?: number;
  /** The most leaves an offline op may touch (default 10 000) */
  maxLeaves?: number;
  /** Whether a client with this credential is answered from the copy of this store while the server is away; by default, when the server answered its hello on it before */
  authorizeOffline?(key: string, storeId: string): boolean;
  /** An offline op's last gate: false or a throw leaves it unapplied, and pending. `state` is the copy: read it only */
  validate?(diff: object, context: { key: string | null; replicaId: string; storeId: string; state: object }): boolean | void;
  /** Fan-out: dials the server's relay route with the relay's own credential; the relay then reads each store once for every client that has a credential */
  link?: TransportFactory;
  /** Fan-out: how long (ms) the relay's session on a store outlives the store's last client (default 20 000) */
  linger?: number;
  /** Fan-out: a client's socket up for its edits closes once it has had nothing to wait for this long (ms, default 30 000) */
  writeIdle?: number;
  /** Copies are written at most this often (ms, default 1000), and on flush() and close() */
  saveDelay?: number;
  /** A copy nobody had open, and a credential the server did not answer, for this long (ms, default 30 days) are let go; Infinity keeps them */
  forgetAfter?: number;
  /** Faults: storage that failed, a bug while handling a message; default console */
  onError?(error: unknown): void;
  /** Wall clock (injectable for tests) */
  now?(): number;
}

/** What the host hands the relay for a client's socket */
export interface RelaySocketOptions {
  /** Deliver a message to the client: encode or copy it before returning */
  send(message: object): void;
  /** Close the client's socket with a WebSocket close code */
  close(code: number, reason: string): void;
  /** A fingerprint of the client's credential (a hash of its Authorization header, say); without one it is never answered offline */
  key?: string | null;
  /** Dials the server with the client's credential, and the same path and query */
  upstream: TransportFactory;
  /** Fan-out: what the server judges the client by; a client without one is passed through */
  credential?: RelayCredential;
}

/** A client's socket at the relay: feed it what the client sends, parsed, and close it when the socket closes */
export interface RelaySession {
  receive(message: object): void;
  close(): void;
  readonly state: RelaySocketState;
}

/** What the relay holds of a store */
export interface CopyInfo {
  /** A copy of the copy's state */
  state: object;
  /** The server's version the copy is at (its offline edits, `local`, on top) */
  v: number;
  /** The server's epoch */
  epoch: string;
  /** Following the server's patches now */
  live: boolean;
  /** Holds offline edits the server has not had */
  diverged: boolean;
  /** How many */
  local: number;
}

export interface RelayStats {
  mode: RelayMode;
  /** When the server first failed to answer (the relay's clock), or null */
  downSince: number | null;
  sockets: { dialing: number; through: number; local: number; fan: number };
  copies: number;
  live: number;
  diverged: number;
  /** Credentials the server has answered */
  credentials: number;
  /**
   * Fan-out: the link's state ('broken': the server answers the clients
   * and not the link, which are passed through meanwhile; 'refused': the
   * server turned the relay away), how many stores it reads on it, how
   * many clients it has vouched for, and how many clients' sessions it
   * passes through on stores the server will not let it carry
   */
  link?: { state: 'down' | 'dialing' | 'open' | 'broken' | 'refused'; shared: number; clients: number; passed: number };
}

export interface RelaySocketInfo {
  key: string | null;
  state: RelaySocketState;
  stores: string[];
  openMs: number;
}

export interface Relay {
  /** A client's socket, accepted */
  accept(options: RelaySocketOptions): RelaySession;
  readonly mode: RelayMode;
  on(event: 'mode', fn: (mode: RelayMode) => void): Unsubscribe;
  /** A store's copy changed */
  on(event: 'copy', fn: (storeId: string) => void): Unsubscribe;
  on(event: 'error', fn: (error: unknown) => void): Unsubscribe;
  /** What the relay holds of a store, or null */
  copy(storeId: string): CopyInfo | null;
  /** The host's word that the server answers again: the relay goes through without waiting for its probe */
  upstreamUp(): void;
  /** The host's word that the server is away: the relay answers on its own now; its probe still finds the server back */
  upstreamDown(): void;
  stats(): RelayStats;
  sockets(): RelaySocketInfo[];
  /** Write what changed now */
  flush(): void;
  /** Let go of what `forgetAfter` says; runs every hour on its own */
  sweep(): void;
  /** Close every socket (code 1001), stop, and write what changed */
  close(): void;
}

export function createRelay(options?: RelayOptions): Relay;

/** Copies kept in memory: a relay made again on the same one finds them */
export function memoryCopies(): CopyStorage;

/** One JSON file per store in `dir` and `_known.json`, each written through a temp file and a rename; a file that cannot be read goes to `onError` */
export function fileCopies(dir: string, options?: { onError?(error: unknown): void }): CopyStorage;
