// Type declarations for `lazy-storage/server/sqlite-node` (Node 22.13+)
import type { SqliteStorage } from './server-sqlite.js';

export type { SqliteStorage } from './server-sqlite.js';

/** The Bun adapter's schema on node:sqlite; the two read each other's files */
export function sqliteStorage(file?: string, options?: { wal?: boolean }): SqliteStorage;
