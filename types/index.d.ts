// Type declarations for the `lazy-storage` entry: everything a browser or
// Bun/Node client needs. The server lives under `lazy-storage/server`.
import type { Connection, ConnectionOptions, DocumentStorage, MessagePortLike, PortConnection, RowDocument, RowStorage, SharedConnection, SharedConnectionOptions, TransportFactory } from './client.js';

export { LazyWatch } from 'lazy-watch';
export * from './core.js';
export * from './client.js';

/** A WebSocket transport with JSON messages; `url` may be a function, for a token per connection */
/**
 * A WebSocket transport with JSON messages. `fetch` fetches a large
 * snapshot from the socket's server when the server points there (see
 * its snapshot route): by default the global fetch, on the route resolved
 * against the socket URL (ws to http) with the socket URL's query, so a
 * token there applies to both; your own to add headers; false to take
 * every snapshot over the socket
 */
export function webSocketTransport(url: string | (() => string), options?: { WebSocket?: any; fetch?: false | typeof fetch }): TransportFactory;
/** A transport over a MessagePort, for a page a shared connection lets follow the browser's replica (`connection.follow`) */
export function messagePortTransport(port: MessagePortLike, options?: { onControl?: (message: { lazy: string; [key: string]: unknown }) => void }): TransportFactory;
/**
 * The connection of a page that follows a browser's replica over a MessagePort (the other end of
 * `connection.follow`): a plain connection on the port, plus the browser's socket status
 * (`upstream`, which its clients report as theirs) and the replica's unsent ops (`pending`), as the host says
 */
export function portConnection(port: MessagePortLike, options?: { reconnect?: { min: number; max: number } | false; keepalive?: number | false }): PortConnection;

/** One socket shared by any number of clients */
export function createConnection(options: ConnectionOptions): Connection;

/** One socket per browser: the tabs elect a leader that runs the browser's replica, and every tab's clients follow it */
export function sharedConnection(options: SharedConnectionOptions): SharedConnection;

/** Outbox and state cache in memory: nothing survives a reload; `clear()` forgets both */
export function memoryOutbox(): DocumentStorage & { clear(): void };

/** Outbox under `key` in localStorage, the state cache under `key:state`; `clear()` removes both keys */
export function localStorageOutbox(key?: string): DocumentStorage & { clear(): void };

export interface IndexedDBStorageOptions {
  /** Defaults to the global */
  indexedDB?: any;
  /** Defaults to the global */
  IDBKeyRange?: any;
  /** A failed write (quota, a closed database) */
  onError?: (error: unknown) => void;
}

export interface IndexedDBStorage extends RowStorage {
  load(): Promise<RowDocument | null>;
  /** Resolves once every write issued so far has landed (or failed) */
  settled(): Promise<void>;
  close(): void;
  /** Close and delete the database */
  destroy(): Promise<void>;
}

/** Row persistence in IndexedDB, one database per adapter; open the client with `openClient` */
export function indexedDBStorage(name?: string, options?: IndexedDBStorageOptions): IndexedDBStorage;
