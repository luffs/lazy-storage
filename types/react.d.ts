// Type declarations for `lazy-storage/react`. Hand-written.
import type { Client, ClientStatus, Closed } from './client.js';
import type { Peer } from './core.js';

/** What useClient returns: the client's state and other facts, a new object per change */
export interface ClientSnapshot<S extends object = any> {
  /** The client's state itself (the same proxy every time); read it, write to it */
  readonly state: S;
  readonly status: ClientStatus;
  /** Distinct users with a live session on the store */
  readonly presence: unknown[];
  /** Every live session on the store, with what it shares */
  readonly peers: Peer[];
  /** Unacknowledged local ops */
  readonly pending: number;
  /** Why the server closed the store or the socket for us, or null */
  readonly closed: Closed | null;
  readonly canUndo: boolean;
  readonly canRedo: boolean;
  /** True when the client started from a cached state */
  readonly restored: boolean;
}

/** The subscribe/getSnapshot pair useSyncExternalStore reads, one per client */
export interface ClientTracker<S extends object = any> {
  subscribe(listener: () => void): () => void;
  getSnapshot(): ClientSnapshot<S>;
}

export function trackClient<S extends object = any>(db: Client<S>): ClientTracker<S>;

/**
 * The client's state and facts, read again by the component on every
 * batch, outbox change, and event, through useSyncExternalStore
 */
export function useClient<S extends object = any>(db: Client<S>): ClientSnapshot<S>;
