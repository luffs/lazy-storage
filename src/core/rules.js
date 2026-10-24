// rules.js - Who may write what: a verdict per record, on the record before and after
//
// `rules` names a rule per collection or singleton, and `'*'` for the
// rest: true (anyone), false (nobody but the server), or a function
// `(user, before, after, { c, id, state, replica, changes, store })` that
// returns false or throws (its message is the refusal's) to refuse.
// `before` is the record as it is, `after` as the op leaves it, null where
// there is none. `after` is what the merge makes of the op, not what the
// op asked for: each change is decided on its own by the op's time, so a
// writer could otherwise pick a time at which one change of a pair lands
// and the other loses, and leave a record no rule allowed. So the op is
// applied first, as a trial, the rules judge what it made, and a refusal
// puts back every record it touched; `state` is the store as the op leaves
// it. The server and a follower judge with the same rules, so a site that
// shares them refuses offline what the server would.
import { LazyWatch } from 'lazy-watch';

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
 * @returns {(doc: Object, changes: Object[], user: any, replica: string, extra: Object, apply: () => any) => any} throws a RuleError, having undone `apply`
 */
export function createRules(shape, rules = {}, readOnly = false) {
  if (!Utils.isPlainObject(rules)) throw new TypeError('rules must be an object of rules by collection');
  const byKey = new Map();
  for (const [key, rule] of Object.entries(rules)) {
    if (key !== '*' && !shape.has(key)) throw new TypeError(`rules: "${key}" is not a collection or singleton`);
    byKey.set(key, ruleOf(rule));
  }
  const ruleOn = c => (readOnly === true ? false : byKey.has(c) ? byKey.get(c) : byKey.has('*') ? byKey.get('*') : null);

  /**
   * Judge an op and apply it: `apply()` applies its changes to `doc` and
   * returns what it reports, which `judge` returns. What a `false` rule
   * refuses is refused before anything is applied; a rule function judges
   * the records as `apply` left them, and a refusal undoes it
   */
  return function judge(doc, list, user, replica, extra, apply) {
    if (readOnly !== true && byKey.size === 0) return apply();
    const byRecord = new Map();
    for (const change of list) {
      const key = shape.recordKey(change.c, change.id);
      if (!byRecord.has(key)) byRecord.set(key, { key, c: change.c, id: change.id, changes: [], rule: ruleOn(change.c) });
      byRecord.get(key).changes.push(change);
    }
    const judged = [];
    for (const record of byRecord.values()) {
      if (record.rule === false) throw new RuleError(`${record.c} is the server's to write`);
      if (record.rule !== null) judged.push(record);
    }
    if (!judged.length) return apply();
    const { result, before } = doc.trial(apply);
    try {
      for (const { key, c, id, changes, rule } of judged) {
        const was = before.has(key) ? before.get(key)[2] : doc.get(c, id);
        const now = doc.get(c, id);
        let verdict;
        try {
          verdict = rule(user, was === undefined ? null : was, now === undefined ? null : now, { c, id, state: doc.state, replica, changes, ...extra });
        } catch (err) {
          throw new RuleError(err?.message || `Not allowed to write ${c}`);
        }
        if (verdict === false) throw new RuleError(`Not allowed to write ${c}${id === undefined ? '' : `/${id}`}`);
      }
    } catch (err) {
      doc.undo(before);
      throw err;
    }
    return result;
  };
}
