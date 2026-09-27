// Type declarations for `lazy-storage/server/bun`
import type { Authorize, AuthorizeId, Store, StoreRegistry, StoreResolver } from './server.js';

export interface HandlerOptions {
  /** A registry from createStores, or a resolver; for a single store, `() => store` */
  stores: StoreRegistry | StoreResolver;
  /** WebSocket path (default '/ws') */
  path?: string;
  /** The user for a request, or null/undefined to refuse (401); may return a promise */
  authenticate?(req: Request, context?: { relay: unknown }): unknown;
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
  expiresAt?(user: unknown, req: Request, context?: { relay: unknown }): number | Date | null | undefined | Promise<number | Date | null | undefined>;
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
  /**
   * Let relays carry clients (see src/relay): a relay connects at `path`
   * (default `<path>/relay`), is `authenticate`d as itself, and reads each
   * store once for its clients, each judged by `authenticate` and
   * `expiresAt` (handed `{ relay }` as their last argument) over the
   * credential the relay vouches with
   */
  relays?: RelaysOptions;
  /** Server faults; default console */
  onError?(error: unknown): void;
}

export interface RelaysOptions {
  /** The relay a request is, or null/undefined to turn it away */
  authenticate(req: Request): unknown;
  /** When that runs out, as `expiresAt` */
  expiresAt?(relay: unknown, req: Request): number | Date | null | undefined | Promise<number | Date | null | undefined>;
  /** Whether the relay may carry a store at all (default: any its clients may read); judged again by `revalidate` */
  authorize?(relay: unknown, storeId: string): boolean | Promise<boolean>;
  /** The relay route (default `<path>/relay`) */
  path?: string;
  /** How long (ms) a relay's session on a store outlives the store's last client there (default 30 000) */
  linger?: number;
  /** More names of a client's headers its credential may carry, besides authorization, cookie and user-agent */
  headers?: string[];
  /** Vouches turned away as unauthorized before every vouch waits (default 100 burst, 10 a second); false disables */
  rate?: { burst: number; perSecond: number } | false;
}

/**
 * An open socket, as `sockets()` lists it. A relay's socket has `relay`
 * and `grants`, and each client it carries is listed after it with `via`
 */
export interface SocketInfo {
  /** What `authenticate` returned for it (a relay's socket: none) */
  user?: unknown;
  /** A relay's socket: the relay */
  relay?: unknown;
  /** A relay's socket: how many clients it carries */
  grants?: number;
  /** A client a relay carries: the relay */
  via?: unknown;
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
  /** Of them, relays' */
  relays: number;
  /** Clients relays carry: no sockets of the server's */
  clients: number;
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
   * in. For a logout, a changed role, a deleted account. A relay's clients it
   * picks are revoked alike, and a relay it picks (handed the relay) is cut
   * off with its clients. Returns how many
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
