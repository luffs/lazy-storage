// Type declarations for `lazy-storage/server/sqlite` (Bun only)
import type { ServerStorage } from './server.js';

export interface SqliteStorage {
  /** The storage adapter for one store (created on its first commit); loading it takes the store's lease */
  store(id: string): ServerStorage & { close(): void };
  /** Ids of every store that has committed at least once */
  ids(): string[];
  /** Delete a store's rows, replicas, log, version, and lease */
  remove(id: string): void;
  /** The underlying bun:sqlite Database, for backups or ad-hoc queries */
  readonly db: any;
  /** Give up every lease this process holds and close the file */
  close(): void;
}

export interface SqliteStorageOptions {
  /** Write-ahead logging (default true for files) */
  wal?: boolean;
  /**
   * One process per store: loading a store takes a lease on it in the file,
   * renewed with every commit and on a timer, lapsing `ttl` ms (default
   * 30 000) after its holder stops; another process is refused with code
   * 'store-locked'. false serves stores without leases
   */
  lease?: { ttl?: number } | false;
}

/** One database file for any number of stores, one row per leaf, WAL mode, the delta log alongside */
export function sqliteStorage(file?: string, options?: SqliteStorageOptions): SqliteStorage;
