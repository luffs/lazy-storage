// Type declarations for `lazy-storage/testing`. Hand-written.
import type { Client, ClientOptionsBase, Transport, TransportFactory } from './client.js';
import type { Session, Store } from './server.js';

/** One client's way onto the network: a transport factory, and offline/online control */
export interface TestLink {
  factory: TransportFactory;
  /** Whether new connections succeed; false also closed the current one */
  online: boolean;
  /** The transport currently open on this link, or null */
  current: Transport | null;
  goOffline(): void;
  goOnline(): void;
}

export interface TestClient<S extends object = any> extends Client<S> {
  link: TestLink;
}

/** What a network needs on the server side: a store, or anything with `session()` such as a hub factory */
export type TestEndpoint<S extends object = any> =
  | Store<S>
  | { session(options: { send: (message: object) => void; user?: unknown; onEvict?: () => void }): Session };

export interface TestNetwork {
  /** Messages queued and not yet delivered */
  readonly pending: number;
  /** Deliver everything, repeatedly, until the network and microtasks are quiet */
  settle(): Promise<void>;
  /** A link for one client; `user` is attached to the sessions it opens */
  link(options?: { user?: unknown }): TestLink;
  /** A connected client on its own link (store 'main' unless given), with `reconnect` off so the test drives it */
  client<S extends object = any>(options?: Partial<ClientOptionsBase<S>>, linkOptions?: { user?: unknown }): TestClient<S>;
}

export function createNetwork<S extends object = any>(target: TestEndpoint<S>): TestNetwork;

/** A controllable wall clock: pass it as `now` to a store and its clients */
export interface FakeTime {
  (): number;
  /** Move forward by `ms`; returns the new time */
  advance(ms: number): number;
  set(ms: number): void;
}

export function fakeTime(start?: number): FakeTime;
