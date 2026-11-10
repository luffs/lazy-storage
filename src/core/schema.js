// schema.js - What a record may hold: a check per collection or singleton, on the record as a write leaves it
//
// `schemas` names a schema per collection or singleton. A store judges
// with it every record a write touched, as the write left it: the merge
// decides each change on its own, so a record is checked whole, after
// the merge, and not the op's changes one by one (a field set on a record
// is fine or not by what the record then holds). A record that breaks its
// schema refuses the write whole, with code 'invalid' for a replica's op
// and a throw for the server's own. A relay given the same schemas refuses
// while the server is away what the server would (rules.js).
//
// A schema is any of:
//
// - a type by name: 'string', 'number', 'integer', 'boolean', 'any', with
//   a trailing `?` for a field that may be absent ('string?')
// - an object of fields, each a schema: the value is an object with those
//   fields and no others, but for `'*'`, the schema of every field not
//   named ({ '*': 'number' } is a map of numbers)
// - an array of one schema: a list of such values (['string'])
// - a function `(value, { c, id }) => true | false | string | void`: false
//   refuses, a string refuses with that message, a throw with its own
// - a validator of another library: one with the Standard Schema
//   interface (`~standard`, as Zod, Valibot and ArkType have), or an
//   instance of a library's class with `safeParse` or `parse` (a plain
//   object is always an object of fields, so a record may have fields of
//   those names). It is asked whether the value is valid and no more: what
//   it would default, strip or transform is not written
// - `optional(schema)` for any of these where a field may be absent
// - `listItem(fields)` for a record of a list (client/list.js), which
//   carries its id and its position key beside its own fields
//
// Every check is synchronous. A schema checks, and never changes, a value.
import { LazyWatch } from 'lazy-watch';
import { isPositionKey } from './positions.js';

const { Utils } = LazyWatch;
const isObj = Utils.isPlainObject;

/** Why a record broke its schema: `path` from the store's root, the message for the writer */
export class SchemaError extends Error {
  constructor(message, path) {
    super(message);
    this.name = 'SchemaError';
    this.code = 'invalid';
    this.path = path;
  }
}

const OPTIONAL = Symbol('lazy-storage.optional');

/** A field that may be absent, for a schema that is not a type's name (`'string?'` says it for those) */
export const optional = schema => ({ [OPTIONAL]: schema });

/** One of these values and no other: a check for a field that is a choice */
export const oneOf = (...values) => value => values.includes(value) || `expected one of ${values.map(v => JSON.stringify(v)).join(', ')}`;

/**
 * The record of a list: `fields`, and the two a list writes on every record it adds, its `id` and its
 * position key (`position`, default 'pos'). Either may be absent, on a record written without the list
 */
export function listItem(fields = {}, { position = 'pos' } = {}) {
  if (!isObj(fields)) throw new TypeError('listItem takes the fields of a list\'s record');
  return { id: 'string?', [position]: optional(value => isPositionKey(value) || 'expected a position key'), ...fields };
}

/** A store's schemas as they are given: for TypeScript, which keeps their literal types through it (`Infer`, `InferState`) */
export const defineSchemas = schemas => schemas;

const TYPES = {
  string: [v => typeof v === 'string', 'a string'],
  number: [v => typeof v === 'number', 'a number'],
  integer: [v => Number.isInteger(v), 'a whole number'],
  boolean: [v => typeof v === 'boolean', 'true or false'],
  any: [() => true, 'a value']
};

// What is wrong, and where: the path is built on the way out, a key at a time (`under`), so a value that fits costs no path
const issue = (message, path = []) => ({ path, message });
const under = (key, found) => { found.path.unshift(key); return found; };

/** The first thing another library's validator found wrong, as { path, message } */
function foreign(found, fallback) {
  const first = Array.isArray(found?.issues) ? found.issues[0] : found;
  const beneath = Array.isArray(first?.path) ? first.path.map(seg => String(seg !== null && typeof seg === 'object' ? seg.key : seg)) : [];
  return issue(typeof first?.message === 'string' && first.message ? first.message : fallback, beneath);
}

/**
 * A schema as it is used: { check(value, context) -> null | { path, message }, optional }, the path from the value.
 * `where` names the schema in what is thrown for one that is not a schema
 */
function compile(spec, where) {
  if (typeof spec === 'string') {
    const may = spec.endsWith('?');
    const type = TYPES[may ? spec.slice(0, -1) : spec];
    if (!type) throw new TypeError(`schemas: "${spec}" at ${where} is not a type (${Object.keys(TYPES).join(', ')}, each with ? for a field that may be absent)`);
    const [is, name] = type;
    return { optional: may, check: value => (is(value) ? null : issue(`expected ${name}`)) };
  }
  if (typeof spec === 'function') {
    return {
      optional: false,
      check(value, context) {
        let verdict;
        try {
          verdict = spec(value, context);
        } catch (err) {
          return issue(err?.message || 'not allowed');
        }
        if (typeof verdict === 'string') return issue(verdict);
        return verdict === false ? issue('not allowed') : null;
      }
    };
  }
  if (Array.isArray(spec)) {
    if (spec.length !== 1) throw new TypeError(`schemas: a list at ${where} is written [schema], the schema of its items`);
    const item = compile(spec[0], `${where}/*`);
    return {
      optional: false,
      check(value, context) {
        if (!Array.isArray(value)) return issue('expected a list');
        for (let i = 0; i < value.length; i++) {
          const found = item.check(value[i], context);
          if (found) return under(String(i), found);
        }
        return null;
      }
    };
  }
  if (spec === null || typeof spec !== 'object') throw new TypeError(`schemas: ${where} is not a schema`);
  if (Object.hasOwn(spec, OPTIONAL)) return { ...compile(spec[OPTIONAL], where), optional: true };
  const standard = spec['~standard'];
  if (standard !== null && typeof standard === 'object' && typeof standard.validate === 'function') {
    return {
      optional: false,
      check(value) {
        let result;
        try {
          result = standard.validate(value);
        } catch (err) {
          return issue(err?.message || 'not valid');
        }
        if (typeof result?.then === 'function') return issue('its schema validates asynchronously, which a store cannot wait for');
        return result?.issues?.length ? foreign(result, 'not valid') : null;
      }
    };
  }
  // `safeParse` and `parse` are names a record's fields may have too: an object of fields is a plain object, and
  // another library's schema an instance of its classes, so only the second is asked to parse
  const plain = isObj(spec);
  if (!plain && typeof spec.safeParse === 'function') {
    return {
      optional: false,
      check(value) {
        let result;
        try {
          result = spec.safeParse(value);
        } catch (err) {
          return issue(err?.message || 'not valid');
        }
        return result?.success ? null : foreign(result?.error, 'not valid');
      }
    };
  }
  if (!plain && typeof spec.parse === 'function') {
    return {
      optional: false,
      check(value) {
        try {
          spec.parse(value);
          return null;
        } catch (err) {
          return foreign(err, 'not valid');
        }
      }
    };
  }
  if (!plain) throw new TypeError(`schemas: ${where} is not a schema`);
  const fields = new Map();
  let rest = null;
  for (const [key, field] of Object.entries(spec)) {
    if (key === '*') rest = compile(field, `${where}/*`);
    else fields.set(key, compile(field, `${where}/${key}`));
  }
  return {
    optional: false,
    check(value, context) {
      if (!isObj(value)) return issue('expected an object');
      for (const key in value) {
        const field = fields.get(key) ?? rest;
        if (!field) return issue('not a field of its schema', [key]);
        const found = field.check(value[key], context);
        if (found) return under(key, found);
      }
      for (const [key, field] of fields) if (!field.optional && !Object.hasOwn(value, key)) return issue('missing', [key]);
      return null;
    }
  };
}

/**
 * @param {Object} shape - from createShape
 * @param {Object} [schemas] - a schema per collection or singleton (see the header)
 * @returns {{ size: number, has: (c: string) => boolean, check: (c: string, id: string|undefined, record: any) => SchemaError|null }}
 *   `check` says what is wrong with a record, as an error to throw, or null for a record that fits or has no schema
 */
export function createSchemas(shape, schemas = {}) {
  if (!isObj(schemas)) throw new TypeError('schemas must be an object of schemas by collection');
  const byKey = new Map();
  for (const [key, spec] of Object.entries(schemas)) {
    if (!shape.has(key)) throw new TypeError(`schemas: "${key}" is not a collection or singleton`);
    byKey.set(key, compile(spec, key));
  }
  return {
    size: byKey.size,
    has: c => byKey.has(c),
    check(c, id, record) {
      const schema = byKey.get(c);
      if (!schema) return null;
      const found = schema.check(record, { c, id });
      if (!found) return null;
      const path = [...shape.full(c, id), ...found.path];
      return new SchemaError(`${path.join('/')}: ${found.message}`, path);
    }
  };
}
