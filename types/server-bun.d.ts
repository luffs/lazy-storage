// Type declarations for `lazy-storage/server/bun`
import type { Authorize, Store, StoreRegistry, StoreResolver } from './server.js';

export interface HandlerOptions {
  /** A registry from createStores, or a resolver; for a single store, `() => store` */
  stores: StoreRegistry | StoreResolver;
  /** WebSocket path (default '/ws') */
  path?: string;
  /** The user for a request, or null/undefined to refuse (401); may return a promise */
  authenticate?(req: Request): unknown;
  authorize?: Authorize;
  /** The largest message (bytes) a socket may send; default 4 MB */
  maxPayload?: number;
  /** Offer the permessage-deflate extension (compressed snapshots and deltas for clients that take it); the runtime's own options object is accepted. Default false */
  perMessageDeflate?: boolean | object;
  /** Server faults; default console */
  onError?(error: unknown): void;
}

export interface CloseOptions {
  /** The close reason clients see (default 'Server shutting down') */
  reason?: string;
}

export interface Handlers {
  /** null when the URL is not ours, undefined after a successful upgrade, or an error Response */
  upgrade(req: Request, server: any): Promise<Response | undefined | null>;
  /** Pass through to Bun.serve */
  websocket: any;
  /** Refuse new sockets, close the open ones with code 1001, dispose the registry (flushing every store) */
  close(options?: CloseOptions): Promise<void>;
  readonly closing: boolean;
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
  [key: string]: any;
}

export function serve(options: ServeOptions): BunServer;

export type { Store };
