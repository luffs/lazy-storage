// Type declarations for `lazy-storage/relay/bun`. Hand-written.
import type { TransportFactory } from './client.js';
import type { Relay, RelaySocketState } from './relay.js';

export interface RelayHandlerOptions {
  /** From createRelay */
  relay: Relay;
  /** A transport factory that dials the server for this request's client, with its credential (see upstreamSocket) */
  upstream(req: Request): TransportFactory;
  /** A fingerprint of the request's credential; without one (null) the client is passed through and never answered offline */
  key?(req: Request): string | null | Promise<string | null>;
  /** WebSocket path (default '/ws') */
  path?: string;
  /** The largest message (bytes) a client may send; default 4 MB */
  maxPayload?: number;
  /** As the server's: messages of `threshold` bytes or more (default 1024) go compressed; false turns it off */
  perMessageDeflate?: boolean | ({ threshold?: number } & Record<string, unknown>);
  /** Close a socket whose unsent output passes this many bytes (code 1013); default 16 MB */
  maxBuffered?: number | false;
  /** Default console */
  onError?(error: unknown): void;
}

export interface RelayHandlerSocket {
  key: string | null;
  state: RelaySocketState;
  /** Bytes queued for it and not yet sent */
  buffered: number;
  openMs: number;
}

export interface RelayHandlers {
  /** null when the URL is not the path, undefined after an upgrade, or a Response */
  upgrade(req: Request, server: any): Promise<Response | undefined | null>;
  /** Pass through to Bun.serve */
  websocket: any;
  /** Close this listener's sockets (code 1001) and take no more; the relay stays for the host to close */
  close(options?: { reason?: string }): Promise<void>;
  sockets(): RelayHandlerSocket[];
}

export function createRelayHandlers(options: RelayHandlerOptions): RelayHandlers;

export interface UpstreamSocketOptions {
  /** Headers for the handshake, or a function giving them per dial: the client's credential */
  headers?: Record<string, string> | (() => Record<string, string>);
  /** A WebSocket that takes `{ headers }` (default: the global one, Bun's) */
  WebSocket?: any;
}

/** A transport factory on a WebSocket with headers, keeping frames as they came, for a relay to dial the server with */
export function upstreamSocket(url: string | (() => string), options?: UpstreamSocketOptions): TransportFactory;
