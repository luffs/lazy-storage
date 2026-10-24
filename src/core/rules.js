// rules.js - Who may write what: a verdict per record, on the record before and after
//
// `rules` names a rule per collection or singleton, and `'*'` for the
// rest: true (anyone), false (nobody but the server), or a function
// `(user, before, after, { c, id, state, replica, changes, store })` that
// returns false or throws (its message is the refusal's) to refuse.
// `before` is the record as it is, `after` as the op's changes would leave
// it if every one landed, null where there is none. The server and a
// follower judge with the same rules, so a site that shares them refuses
// offline what the server would.
import { LazyWatch } from 'lazy-watch';
import { createDoc } from './doc.js';

const { Utils } = LazyWatch;

/** A rule as it is used: false (refuse), null (allow), or a function */
function ruleOf(rule) {
  if (rule === false) return false;
  if (rule === true || rule === undefined || rule === null) return null;
  if (typeof rule === 'function') return rule;
  if (Utils.isPlainObject(rule) && typeof rule.write === 'function') return rule.write;
  if (Utils.isPlainObject(rule) && rule.write === false) return false;
  throw new TypeError('A rule is true, false, a function, or { write }');
}

/** Why a rule refused: carries a message for the writer */
export class RuleError extends Error {
  constructor(message) {
    super(message);
    this.name = 'RuleError';
    this.code = 'forbidden';
  }
}

/**
 * @param {Object} shape - from createShape
 * @param {Object} [rules]
 * @param {boolean} [readOnly] - refuse everything
 * @returns {(doc: Object, changes: Object[], user: any, replica: string, extra?: Object) => void} throws a RuleError
 */
export function createRules(shape, rules = {}, readOnly = false) {
  if (!Utils.isPlainObject(rules)) throw new TypeError('rules must be an object of rules by collection');
  const byKey = new Map();
  for (const [key, rule] of Object.entries(rules)) {
    if (key !== '*' && !shape.has(key)) throw new TypeError(`rules: "${key}" is not a collection or singleton`);
    byKey.set(key, ruleOf(rule));
  }
  const ruleOn = c => (readOnly === true ? false : byKey.has(c) ? byKey.get(c) : byKey.has('*') ? byKey.get('*') : null);

  /** What the changes would make of each record they touch: as if every one landed */
  function attempted(doc, list) {
    const state = {};
    for (const c of shape.collections) state[c] = {};
    for (const c of shape.singletons) state[c] = undefined;
    for (const change of list) {
      const record = doc.get(change.c, change.id);
      if (shape.isSingleton(change.c)) state[change.c] = structuredClone(record);
      else if (record !== undefined) state[change.c][change.id] = structuredClone(record);
    }
    const scratch = createDoc(shape, { state, track: false });
    for (const change of list) scratch.apply(change, 0, 'trust');
    return scratch;
  }

  return function judge(doc, list, user, replica, extra = {}) {
    if (readOnly !== true && byKey.size === 0) return;
    const byRecord = new Map();
    for (const change of list) {
      const key = shape.recordKey(change.c, change.id);
      if (!byRecord.has(key)) byRecord.set(key, { c: change.c, id: change.id, changes: [] });
      byRecord.get(key).changes.push(change);
    }
    let scratch = null;
    for (const { c, id, changes } of byRecord.values()) {
      const rule = ruleOn(c);
      if (rule === null) continue;
      if (rule === false) throw new RuleError(`${c} is the server's to write`);
      scratch ??= attempted(doc, list);
      const before = doc.get(c, id);
      const after = scratch.get(c, id);
      let verdict;
      try {
        verdict = rule(user, before === undefined ? null : before, after === undefined ? null : after, { c, id, state: doc.state, replica, changes, ...extra });
      } catch (err) {
        throw new RuleError(err?.message || `Not allowed to write ${c}`);
      }
      if (verdict === false) throw new RuleError(`Not allowed to write ${c}${id === undefined ? '' : `/${id}`}`);
    }
  };
}
