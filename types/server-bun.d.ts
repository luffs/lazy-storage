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
  /**
   * When the session `authenticate` let in runs out: ms since the epoch or a
   * Date, nothing for never; read at upgrade, may return a promise. The
   * socket is closed then with code 4001 and the client reconnects, to
   * authenticate afresh with whatever credentials it has by then
   */
  expiresAt?(user: unknown, req: Request): number | Date | null | undefined | Promise<number | Date | null | undefined>;
  /** The shortest session (ms) an `expiresAt` may leave; one running out sooner is turned away as unauthorized. Default 30000 */
  minSession?: number;
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
  /** When its session runs out (see `expiresAt`), in ms since the epoch; null for never */
  expiresAt: number | null;
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
  /** Sockets closed by `disconnect()` since the server started */
  disconnected: number;
  /** Sockets closed as their session ran out (see `expiresAt`) since the server started */
  expired: number;
  /** Store sessions closed by `revalidate()` since the server started */
  revoked: number;
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
  /**
   * Close the sockets of the users `filter` picks (every socket without one):
   * their clients reconnect, and `authenticate` decides whether they get back
   * in. For a logout, a changed role, a deleted account. Returns how many
   */
  disconnect(filter?: (user: unknown) => boolean): number;
  /**
   * Run the authorize hooks again on the open stores `filter` picks, as the
   * user each socket authenticated as, and close with 'forbidden' those now
   * refused. For a change to who may open a store. Resolves to how many
   */
  revalidate(filter?: (user: unknown, storeId: string) => boolean): Promise<number>;
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
  /** See Handlers.disconnect */
  disconnect(filter?: (user: unknown) => boolean): number;
  /** See Handlers.revalidate */
  revalidate(filter?: (user: unknown, storeId: string) => boolean): Promise<number>;
  [key: string]: any;
}

export function serve(options: ServeOptions): BunServer;

export type { Store };
