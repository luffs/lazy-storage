// Type declarations for `lazy-storage/relay/bun`. Hand-written.
import type { TransportFactory } from './client.js';
import type { Relay, RelayCredential } from './relay.js';

export interface RelayHandlerOptions {
  /** From createRelay */
  relay: Relay;
  /** What the server judges the client by: the headers and query its own socket would carry */
  credential?(req: Request): RelayCredential | null | Promise<RelayCredential | null>;
  /** A fingerprint of the credential, by which the relay knows the client while the server is away (default: its SHA-256) */
  key?(req: Request): string | null | Promise<string | null>;
  /** WebSocket path (default '/ws') */
  path?: string;
  /** The largest message (bytes) a client may send; default 4 MB */
  maxPayload?: number;
  /** Messages of `threshold` bytes or more (default 64 KB here) go compressed; false turns it off */
  perMessageDeflate?: boolean | ({ threshold?: number } & Record<string, unknown>);
  /** Close a socket whose unsent output passes this many bytes (code 1013); default 16 MB */
  maxBuffered?: number | false;
  onError?(error: unknown): void;
}

export interface RelayHandlers {
  /** null when the URL is not the path, undefined after an upgrade, or a Response */
  upgrade(req: Request, server: any): Promise<Response | undefined | null>;
  websocket: any;
  /** Close this listener's sockets (code 1001) and take no more; the relay stays for the host to close */
  close(options?: { reason?: string }): Promise<void>;
  sockets(): Array<{ key: string | null; buffered: number; openMs: number }>;
}

export function createRelayHandlers(options: RelayHandlerOptions): RelayHandlers;

export interface UpstreamSocketOptions {
  headers?: Record<string, string> | (() => Record<string, string>);
  WebSocket?: any;
}

/** A transport factory on a WebSocket that takes headers: what a relay's link dials the server with */
export function upstreamSocket(url: string | (() => string), options?: UpstreamSocketOptions): TransportFactory;
