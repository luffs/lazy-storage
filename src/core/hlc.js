// hlc.js - Hybrid logical clock
//
// A timestamp is [ms, count, replicaId]: wall-clock milliseconds, a
// counter for events within the same millisecond, and the replica id as
// the final tie-break. Timestamps from different replicas never compare
// equal, so every conflict has exactly one winner on every replica.
//
// The clock never runs behind what it has observed: receiving a remote
// timestamp from a replica whose wall clock is ahead pulls this clock
// forward, so a client with a slow clock does not lose every conflict for
// as long as it lags, and one with a fast clock does not win them all
// once the others have seen it.

/**
 * @param {string} replicaId - Unique id of this replica
 * @param {() => number} [now] - Wall clock in ms (injectable for tests)
 */
export function createClock(replicaId, now = Date.now) {
  if (typeof replicaId !== 'string' || replicaId.length === 0) {
    throw new TypeError('createClock requires a non-empty replicaId string');
  }
  let ms = 0;
  let count = 0;

  return {
    get replicaId() {
      return replicaId;
    },

    /** Timestamp for a local event; strictly greater than every earlier one */
    now() {
      const wall = now();
      if (wall > ms) {
        ms = wall;
        count = 0;
      } else {
        count++;
      }
      return [ms, count, replicaId];
    },

    /** Observe a remote timestamp so later local events sort after it */
    receive(ts) {
      if (!isTimestamp(ts)) return;
      const wall = now();
      const [rms, rcount] = ts;
      if (wall > ms && wall > rms) {
        ms = wall;
        count = 0;
      } else if (rms > ms) {
        ms = rms;
        count = rcount + 1;
      } else if (rms === ms) {
        count = Math.max(count, rcount) + 1;
      } else {
        count++;
      }
    },

    /** The current value without advancing */
    peek() {
      return [ms, count, replicaId];
    }
  };
}

export function isTimestamp(ts) {
  return Array.isArray(ts) && ts.length === 3 &&
    Number.isInteger(ts[0]) && Number.isInteger(ts[1]) && typeof ts[2] === 'string';
}

/**
 * Total order over timestamps; a missing timestamp sorts first.
 * @returns {number} negative if a < b, positive if a > b, 0 if equal
 */
export function compareTs(a, b) {
  if (!a) return b ? -1 : 0;
  if (!b) return 1;
  if (a[0] !== b[0]) return a[0] - b[0];
  if (a[1] !== b[1]) return a[1] - b[1];
  return a[2] < b[2] ? -1 : a[2] > b[2] ? 1 : 0;
}
