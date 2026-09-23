// Type declarations for `lazy-storage/vue`. Hand-written.
import type { DeepReadonly, ShallowRef, UnwrapNestedRefs } from 'vue';
import type { Client, ClientStatus, Closed } from './client.js';
import type { Peer } from './core.js';

/** What useClient returns: a reactive mirror of the state, and the client's other facts as refs */
export interface ClientView<S extends object = any> {
  /** A read-only reactive copy of `db.state`, patched on every batch and shared by every caller on the client. Read from it; write to `db.state` */
  state: DeepReadonly<UnwrapNestedRefs<S>>;
  status: ShallowRef<ClientStatus>;
  /** Distinct users with a live session on the store */
  presence: ShallowRef<unknown[]>;
  /** Every live session on the store, with what it shares */
  peers: ShallowRef<Peer[]>;
  /** Unacknowledged local ops */
  pending: ShallowRef<number>;
  /** Why the server closed the store or the socket for us, or null */
  closed: ShallowRef<Closed | null>;
  canUndo: ShallowRef<boolean>;
  canRedo: ShallowRef<boolean>;
  /** True when the client started from a cached state */
  restored: boolean;
  /** Let go of the mirror (the last caller to let go stops it following the client); called for you when the current effect scope ends */
  stop(): void;
}

/**
 * A read-only reactive mirror of the client's state, patched on every
 * batch, and refs for its status, presence, outbox size, closed reason,
 * and undo state: one of each per client, shared by every caller. Lets go
 * with the current effect scope (a component's), else by `stop()`. Works
 * as `data()` in the Options API too
 */
export function useClient<S extends object = any>(db: Client<S>): ClientView<S>;
