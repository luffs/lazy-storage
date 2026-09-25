// Type declarations for `lazy-storage/server/bun`
import type { Authorize, AuthorizeId, Store, StoreRegistry, StoreResolver } from './server.js';

export interface HandlerOptions {
  /** A registry from createStores, or a resolver; for a single store, `() => store` */
  stores: StoreRegistry | StoreResolver;
  /** WebSocket path (default '/ws') */
  path?: string;
  /** The user for a request, or null/undefined to refuse (401); may return a promise */
  authenticate?(req: Request): unknown;
  /** Whether the user may open a store, before it is loaded: false closes it 'forbidden' and loads nothing. Prefer it for any check that needs only the user and the id */
  authorizeId?: AuthorizeId;
  /** The same once the store is loaded, for a check that needs it */
  authorize?: Authorize;
  /** The largest message (bytes) a socket may send; default 4 MB */
  maxPayload?: number;
  /**
   * Offer the permessage-deflate extension: a client that takes it receives
   * messages of `threshold` bytes or more (default 1024) compressed, smaller
   * ones plain. The object form sets `threshold` and the runtime's own
   * options; false turns it off. Default true
   */
  perMessageDeflate?: boolean | ({ threshold?: number } & Record<string, unknown>);
  /**
   * Serve snapshots over HTTP at `<path>/snapshot/<store id>`, compressed
   * once per change (brotli or gzip, as the client accepts): a client
   * that can fetch (webSocketTransport can) is
   * pointed there in place of a snapshot of `threshold` bytes or more
   * (default 64 KB) instead of having the state compressed and buffered
   * for its socket alone. The route authenticates and authorizes like an
   * upgrade. false turns it off. Default true
   */
  httpSnapshots?: boolean | { threshold?: number; origins?: '*' | false | string[] };
  /**
   * Close a socket whose unsent output passes this many bytes, with code
   * 1013; the client reconnects and catches up with a delta. Default 16 MB.
   * false lets it grow (on Bun, until Bun's own limit, past which it drops
   * messages)
   */
  maxBuffered?: number | false;
  /** Server faults; default console */
  onError?(error: unknown): void;
}

/** An open socket, as `sockets()` lists it */
export interface SocketInfo {
  /** What `authenticate` returned for it */
  user: unknown;
  /** Store ids it has a live session on */
  stores: string[];
  /** Bytes queued for it and not yet sent: how far behind it is */
  buffered: number;
  /** Milliseconds since it last sent anything (a client pings every 30 s) */
  idleMs: number;
  /** Milliseconds since it opened */
  openMs: number;
}

/** The open sockets rolled up, for a status endpoint */
export interface SocketStats {
  sockets: number;
  /** Bytes queued and unsent, over every socket */
  buffered: number;
  /** The most any one socket has queued */
  largest: number;
  /** Sockets closed for passing `maxBuffered` since the server started */
  cutOff: number;
}

export interface CloseOptions {
  /** The close reason clients see (default 'Server shutting down') */
  reason?: string;
}

export interface Handlers {
  /** null when the URL is not ours, undefined after a successful upgrade, or a Response: the snapshot route's, or an error */
  upgrade(req: Request, server: any): Promise<Response | undefined | null>;
  /** Pass through to Bun.serve */
  websocket: any;
  /** Refuse new sockets, close the open ones with code 1001, dispose the registry (flushing every store) */
  close(options?: CloseOptions): Promise<void>;
  readonly closing: boolean;
  /** The open sockets, and how far behind each is */
  sockets(): SocketInfo[];
  /** The open sockets rolled up */
  socketStats(): SocketStats;
}

export function createHandlers(options: HandlerOptions): Handlers;

export interface ServeOptions extends HandlerOptions {
  /** Default 3200 */
  port?: number;
  /** Handles other requests; return null to fall through to 404 */
  fetch?(req: Request): Response | null | Promise<Response | null>;
}

/** Bun's server (`Bun.serve`), with a graceful `shutdown` added */
export interface BunServer {
  readonly port: number;
  readonly hostname: string;
  stop(closeActiveConnections?: boolean): void;
  /** Graceful shutdown (see Handlers.close), then stop the server */
  shutdown(options?: CloseOptions): Promise<void>;
  /** See Handlers.sockets */
  sockets(): SocketInfo[];
  /** See Handlers.socketStats */
  socketStats(): SocketStats;
  [key: string]: any;
}

export function serve(options: ServeOptions): BunServer;

export type { Store };
