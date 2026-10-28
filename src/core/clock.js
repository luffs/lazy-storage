// clock.js - Timestamps that a server can trust without a hybrid clock
//
// A timestamp is a whole number of milliseconds, one per op. Three rules
// make that enough:
//
// - A replica stamps causally: max(wall clock + offset, highest seen + 1).
//   `offset` is learnt from the server's `now` (in every snapshot, delta
//   and ack), so a clock that runs slow is corrected, and `highest seen`
//   is the largest timestamp the replica has received, so an edit made
//   after seeing something is always newer than it. The merge's `stale`
//   check (doc.js) depends on that.
// - The server clamps a timestamp ahead of its own time to its own time
//   (`clamp`): a clock that runs fast wins nothing, nothing is re-stamped,
//   and an op is never stamped later than it arrived, which is what makes
//   retention safe (store.js).
// - The server stamps its own writes with its own time, so they win over
//   what is there.
//
// A clock's time is never before a timestamp it has made or seen. Stamps
// made in one millisecond run ahead of the wall clock (each one after the
// last), and a server that said its time was the wall's, or clamped to it,
// would hand a replica that had seen them a time to stamp below them, and
// clamp its edit back below them: lost as stale under a record born then.

/**
 * @param {Object} [options]
 * @param {() => number} [options.now] - the wall clock (injectable for tests)
 */
export function createClock({ now = Date.now } = {}) {
  let offset = 0;
  let highest = 0;
  return {
    /** A timestamp for a local event: never below one seen, never repeated */
    stamp() {
      const t = Math.max(Math.floor(now() + offset), highest + 1);
      highest = t;
      return t;
    },
    /** A timestamp received from elsewhere: later stamps sort after it */
    observe(ts) {
      if (isTimestamp(ts) && ts > highest) highest = ts;
    },
    /** The server's time, as it said it: adopt its clock as an offset */
    adopt(serverNow) {
      if (!isTimestamp(serverNow)) return;
      offset = serverNow - now();
      if (serverNow > highest) highest = serverNow - 1;
    },
    /** Where this clock thinks the server's time is: never before a stamp it has made or seen */
    get time() { return Math.max(Math.floor(now() + offset), highest); },
    get offset() { return offset; },
    get highest() { return highest; }
  };
}

export const isTimestamp = ts => Number.isSafeInteger(ts) && ts >= 0;

/** A received timestamp, clamped to `limit` (the receiver's time) when ahead of it */
export const clamp = (ts, limit) => (ts > limit ? limit : ts);
