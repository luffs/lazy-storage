// Type declarations for `lazy-storage/client/sqlite` (Bun only)
import type { RowDocument, RowStorage } from './client.js';

export interface SqliteClientStorage extends RowStorage {
  load(): RowDocument | null;
  /** The underlying bun:sqlite Database */
  readonly db: any;
  close(): void;
}

/** Row persistence on bun:sqlite for a client that runs in Bun; synchronous, so it fits `createClient` */
export function sqliteClientStorage(file?: string, options?: { wal?: boolean }): SqliteClientStorage;
