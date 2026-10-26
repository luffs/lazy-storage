// snapshot-fetch.js - A large snapshot fetched over HTTP, for a client and for a browser's replica
//
// A hello from a link that can fetch says so (`fetch: true`), and the
// server answers a large snapshot with where to fetch it rather than the
// state (`{ t: 'snapshot', fetch: url }`, no `state`). Whoever said hello
// then fetches it, and what the link brings meanwhile (patches, acks)
// waits for it: a patch the snapshot already has is passed over, the rest
// handled on top, in order. A fetch that fails is reported with code
// `snapshot-fetch`, and the hello is said again without asking to fetch,
// so the server sends the snapshot inline; the next link tries again.
//
// One fetcher per link's store: `cancel()` lets go of a fetch under way
// (its answer is dropped) whenever what it was for is gone: a new link or
// a hello said again, the link closing, the store closed by the server or
// let go of. A plain client (client/index.js) and a browser's replica
// (relay/follower.js) differ only in what they do with the snapshot and
// with a failure.

import { LazyWatch } from 'lazy-watch';

const isObj = LazyWatch.Utils.isPlainObject;

/** Whether a snapshot message points at the HTTP route rather than carrying the state */
export const pointsToFetch = msg => typeof msg?.fetch === 'string' && msg.state === undefined;

/**
 * @param {{ onError: (error: any) => void }} options - where an error thrown while
 *   handling a fetched snapshot goes (a failed fetch goes to `failed`)
 */
export function createSnapshotFetcher({ onError }) {
  let attempt = 0;
  let held = null;
  let failed = false;
  return {
    /** Whether a hello on `connection` asks to fetch: it can, and fetching has not failed on this link */
    offers: connection => typeof connection?.fetch === 'function' && !failed,
    /** While a fetch is under way, a message from the link waits for it (true); the store's end does not */
    hold(msg) {
      if (!held || msg?.t === 'closed') return false;
      held.push(msg);
      return true;
    },
    /** Let go of a fetch under way: its answer and what waited for it are dropped */
    cancel() {
      attempt++;
      held = null;
    },
    /** A new link: nothing under way, and fetching may be tried again */
    reset() {
      attempt++;
      held = null;
      failed = false;
    },
    /**
     * Fetch the snapshot `msg` points at over `connection`. `landed(body, after)`
     * gets the snapshot (`{ state, v, epoch, clocks }`) and the messages that came
     * meanwhile, a patch it already has left out; `failed(error, waited)` the error
     * (code `snapshot-fetch`) and everything that waited. Neither is called once the
     * fetch is cancelled or another begun
     */
    fetch(connection, msg, { landed, failed: onFailed }) {
      const mine = ++attempt;
      const queue = (held = []);
      const current = () => attempt === mine && held === queue;
      Promise.resolve()
        .then(() => connection.fetch(msg.fetch))
        .then(async response => {
          if (!response.ok) throw new Error(`the server answered ${response.status}`);
          const body = await response.json();
          if (!isObj(body) || !isObj(body.state) || !Number.isInteger(body.v)) throw new Error('the response is not a snapshot');
          return body;
        })
        .then(
          body => {
            if (!current()) return;
            held = null;
            landed(body, queue.filter(m => !(m.t === 'patch' && Number.isInteger(m.v) && m.v <= body.v)));
          },
          err => {
            if (!current()) return;
            held = null;
            failed = true;
            const error = new Error(`The snapshot could not be fetched: ${err?.message ?? err}`);
            error.code = 'snapshot-fetch';
            onFailed(error, queue);
          }
        )
        .catch(onError);
    }
  };
}
