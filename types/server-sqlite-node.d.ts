// Type declarations for `lazy-storage/server/sqlite-node` (Node 22.13+)
import type { SqliteStorage, SqliteStorageOptions } from './server-sqlite.js';

export type { SqliteStorage, SqliteStorageOptions } from './server-sqlite.js';

/** The Bun adapter's schema on node:sqlite; the two read each other's files */
export function sqliteStorage(file?: string, options?: SqliteStorageOptions): SqliteStorage;
