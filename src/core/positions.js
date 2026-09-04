// positions.js - Position keys for ordered lists (fractional indexing)
//
// A record in a list carries a position key; the list's order is the
// keys' string order, ties broken by id. A key can always be made between
// two others, so an insert or a move writes one field on one record and
// nothing else has to change, and two replicas inserting at the same
// spot at the same time both keep their records.
//
// A key is an integer part followed by an optional fraction, over the
// digits 0-9A-Za-z (which sort in that order as strings). The integer
// part's first character says its length and sign: 'a' introduces one
// digit, 'b' two, ... 'z' twenty-six, for zero and positive integers;
// 'Z' one digit, 'Y' two, ... 'A' twenty-six, for negative ones. So keys
// made by appending count up compactly ('a0', 'a1', ... 'az', 'b00'),
// keys made by prepending count down ('Zz', 'Zy', ...), and only inserts
// between two existing keys grow a fraction, one digit per five or so
// inserts at the very same spot. A fraction never ends in '0'. This is
// the scheme David Greenspan described for Figma, in the form rocicorp's
// fractional-indexing package popularized.
const DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
const BASE = DIGITS.length;
const SMALLEST_INTEGER = 'A' + '0'.repeat(26);
const ZERO = 'a0';

function integerLength(head) {
  if (head >= 'a' && head <= 'z') return head.charCodeAt(0) - 'a'.charCodeAt(0) + 2;
  if (head >= 'A' && head <= 'Z') return 'Z'.charCodeAt(0) - head.charCodeAt(0) + 2;
  throw new TypeError(`Invalid position key: "${head}" cannot start one`);
}

function integerPart(key) {
  const length = integerLength(key[0]);
  if (length > key.length) throw new TypeError(`Invalid position key "${key}": integer part cut short`);
  return key.slice(0, length);
}

function assertKey(key) {
  if (typeof key !== 'string' || key.length === 0) throw new TypeError('A position key must be a non-empty string');
  for (const ch of key) if (!DIGITS.includes(ch)) throw new TypeError(`Invalid position key "${key}": "${ch}" is not a digit`);
  const integer = integerPart(key);
  const fraction = key.slice(integer.length);
  if (fraction.endsWith('0')) throw new TypeError(`Invalid position key "${key}": a fraction never ends in 0`);
  if (integer === SMALLEST_INTEGER && fraction === '') throw new TypeError(`Invalid position key "${key}": below the range`);
}

/** True for a well-formed position key */
export function isPositionKey(key) {
  try {
    assertKey(key);
    return true;
  } catch {
    return false;
  }
}

function incrementInteger(integer) {
  const head = integer[0];
  const digits = integer.slice(1).split('');
  let carry = true;
  for (let i = digits.length - 1; carry && i >= 0; i--) {
    const d = DIGITS.indexOf(digits[i]) + 1;
    if (d === BASE) digits[i] = '0';
    else { digits[i] = DIGITS[d]; carry = false; }
  }
  if (!carry) return head + digits.join('');
  if (head === 'Z') return ZERO;
  if (head === 'z') return null;
  const next = String.fromCharCode(head.charCodeAt(0) + 1);
  // A longer positive integer gains a digit; a shorter negative one loses one
  return next > 'a' ? next + digits.join('') + '0' : next + digits.join('').slice(1);
}

function decrementInteger(integer) {
  const head = integer[0];
  const digits = integer.slice(1).split('');
  let borrow = true;
  for (let i = digits.length - 1; borrow && i >= 0; i--) {
    const d = DIGITS.indexOf(digits[i]) - 1;
    if (d === -1) digits[i] = DIGITS[BASE - 1];
    else { digits[i] = DIGITS[d]; borrow = false; }
  }
  if (!borrow) return head + digits.join('');
  if (head === 'a') return 'Zz';
  if (head === 'A') return null;
  const prev = String.fromCharCode(head.charCodeAt(0) - 1);
  return prev < 'Z' ? prev + digits.join('') + 'z' : prev + digits.join('').slice(1);
}

/** A fraction strictly between `a` and `b` (`b` empty: no upper bound); neither ends in '0' */
function midpoint(a, b) {
  if (b !== '' && a >= b) throw new RangeError(`"${a}" is not below "${b}"`);
  if (b !== '') {
    let n = 0;
    while ((a[n] ?? '0') === b[n]) n++;
    if (n > 0) return b.slice(0, n) + midpoint(a.slice(n), b.slice(n));
  }
  const digitA = a ? DIGITS.indexOf(a[0]) : 0;
  const digitB = b ? DIGITS.indexOf(b[0]) : BASE;
  if (digitB - digitA > 1) return DIGITS[Math.round((digitA + digitB) / 2)];
  if (b.length > 1) return b[0];
  return DIGITS[digitA] + midpoint(a.slice(1), '');
}

/**
 * A key strictly between `a` and `b`; null on either side means no bound
 * (before everything, after everything). Both null gives the first key.
 * @param {string|null} a
 * @param {string|null} b
 * @returns {string}
 */
export function keyBetween(a, b) {
  if (a != null) assertKey(a);
  if (b != null) assertKey(b);
  if (a != null && b != null && a >= b) throw new RangeError(`Position keys out of order: "${a}" is not below "${b}"`);
  if (a == null) {
    if (b == null) return ZERO;
    const ib = integerPart(b);
    const fb = b.slice(ib.length);
    if (ib === SMALLEST_INTEGER) return ib + midpoint('', fb);
    if (ib < b) return ib;
    const below = decrementInteger(ib);
    if (below == null) throw new RangeError(`No position key below "${b}"`);
    return below;
  }
  if (b == null) {
    const ia = integerPart(a);
    const fa = a.slice(ia.length);
    const above = incrementInteger(ia);
    return above == null ? ia + midpoint(fa, '') : above;
  }
  const ia = integerPart(a);
  const fa = a.slice(ia.length);
  const ib = integerPart(b);
  const fb = b.slice(ib.length);
  if (ia === ib) return ia + midpoint(fa, fb);
  const above = incrementInteger(ia);
  if (above == null) throw new RangeError(`No position key above "${a}"`);
  return above < b ? above : ia + midpoint(fa, '');
}

/**
 * `n` keys in order, all strictly between `a` and `b`. Keys after `a`
 * alone (or before `b` alone) count up (down) compactly; keys between two
 * bounds are bisected.
 * @returns {string[]}
 */
export function keysBetween(a, b, n) {
  if (!Number.isInteger(n) || n < 0) throw new TypeError('keysBetween needs a count');
  if (n === 0) return [];
  if (n === 1) return [keyBetween(a, b)];
  if (b == null) {
    let key = keyBetween(a, null);
    const out = [key];
    for (let i = 1; i < n; i++) out.push(key = keyBetween(key, null));
    return out;
  }
  if (a == null) {
    let key = keyBetween(null, b);
    const out = [key];
    for (let i = 1; i < n; i++) out.push(key = keyBetween(null, key));
    return out.reverse();
  }
  const half = Math.floor(n / 2);
  const middle = keyBetween(a, b);
  return [...keysBetween(a, middle, half), middle, ...keysBetween(middle, b, n - half - 1)];
}

/**
 * The order of two records: by position key, a missing key sorting last,
 * ties by id. Total, so every replica sorts a list the same way.
 */
export function comparePositions(keyA, idA, keyB, idB) {
  const a = typeof keyA === 'string' ? keyA : null;
  const b = typeof keyB === 'string' ? keyB : null;
  if (a !== null && b !== null) {
    if (a < b) return -1;
    if (a > b) return 1;
  } else if (a !== null) return -1;
  else if (b !== null) return 1;
  return idA < idB ? -1 : idA > idB ? 1 : 0;
}
