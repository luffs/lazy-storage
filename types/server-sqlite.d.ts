// Type declarations for `lazy-storage/server/sqlite` (Bun only)
import type { ServerStorage } from './server.js';

export interface SqliteStorage {
  /** The storage adapter for one store (created on its first commit) */
  store(id: string): ServerStorage;
  /** Ids of every store that has committed at least once */
  ids(): string[];
  /** Delete a store's rows, replicas, log, and version */
  remove(id: string): void;
  /** The underlying bun:sqlite Database, for backups or ad-hoc queries */
  readonly db: any;
  close(): void;
}

/** One database file for any number of stores, one row per leaf, WAL mode, the delta log alongside */
export function sqliteStorage(file?: string, options?: { wal?: boolean }): SqliteStorage;
