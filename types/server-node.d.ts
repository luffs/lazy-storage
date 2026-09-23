// Type declarations for `lazy-storage/server/node` (needs the `ws` package)
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import type { Duplex } from 'node:stream';
import type { CloseOptions, HandlerOptions } from './server-bun.js';

export type { CloseOptions, HandlerOptions } from './server-bun.js';

/** A Web Request for an incoming Node request, headers included; what `authenticate` receives */
export function toRequest(req: IncomingMessage): Request;

export interface NodeHandlers {
  /** Handle an http server's 'upgrade' event; resolves to false when the URL is not ours */
  upgrade(req: IncomingMessage, socket: Duplex, head: Buffer): Promise<boolean>;
  /** Handle a plain request when it is ours (the snapshot route); resolves to false when it is not, for the app to answer */
  request(req: IncomingMessage, res: ServerResponse): Promise<boolean>;
  /** Refuse new sockets, close the open ones with code 1001, dispose the registry (flushing every store) */
  close(options?: CloseOptions): Promise<void>;
  readonly closing: boolean;
  /** The underlying `ws` WebSocketServer */
  readonly wss: any;
}

export interface NodeHandlerOptions extends HandlerOptions {
  /** Close a socket that has sent nothing for this long (ms); default 120000, false keeps quiet sockets */
  idleTimeout?: number | false;
  /** Close a socket whose unsent output passes this many bytes; default 16 MB, false lets it grow */
  maxBuffered?: number | false;
}

export function createHandlers(options: NodeHandlerOptions): NodeHandlers;

export interface NodeServeOptions extends NodeHandlerOptions {
  /** Default 3200 */
  port?: number;
  host?: string;
  /** Handles other requests (default: 404) */
  request?(req: IncomingMessage, res: ServerResponse): void;
}

export type NodeServer = Server & {
  /** Graceful shutdown (see NodeHandlers.close), then close the server */
  shutdown(options?: CloseOptions): Promise<void>;
};

/** An http server with the handlers mounted; listening has been started */
export function serve(options: NodeServeOptions): NodeServer;
