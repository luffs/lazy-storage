// Type declarations for `lazy-storage/server/sqlite` (Bun only)
import type { ServerStorage, StorageDocument } from './server.js';

export interface SqliteStorage {
  /** The storage adapter for one store (created on its first commit); loading it takes the store's lease */
  store(id: string): ServerStorage & {
    close(): void;
    /**
     * Take a document as the store's whole storage, in one transaction,
     * under a new epoch; refused with code 'store-open' while this process
     * has it loaded and 'store-locked' while another does
     */
    replace(doc: StorageDocument): void;
  };
  /** Ids of every store that has committed at least once */
  ids(): string[];
  /** Delete a store's rows, replicas, log, version, and lease */
  remove(id: string): void;
  /**
   * Copy the whole file to `file` (which must not exist) with VACUUM INTO:
   * consistent while the server runs. The copy holds no leases, and each
   * of its stores a new epoch
   */
  backup(file: string): void;
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
   * renewed on a timer (and by a commit that finds it with less than half
   * its `ttl` left), lapsing `ttl` ms (default 30 000) after its holder
   * stops; every commit checks it is still the holder's. Another process is
   * refused with code 'store-locked'. false serves stores without leases
   */
  lease?: { ttl?: number } | false;
  /**
   * Where the write-ahead log is copied back into the file: on a worker
   * thread with a connection of its own ('worker', the default for a file in
   * WAL mode), so the thread that commits never waits on the disk for it, or
   * by SQLite on the thread that commits ('inline')
   */
  checkpoints?: 'worker' | 'inline';
}

/** One database file for any number of stores, one row per leaf, WAL mode, the delta log alongside */
export function sqliteStorage(file?: string, options?: SqliteStorageOptions): SqliteStorage;
