// clocks.js - The per-path clock table with a children index
//
// The merge keeps one entry per leaf path, keyed by the path's JSON key.
// A write or deletion at a path must find every descendant entry (to drop
// them, or to check that none is newer); scanning every key for a prefix
// would make each op cost as much as the store is large. This Map keeps
// a parent -> children index alongside its entries, linking intermediate
// paths that have no entry of their own, so descendants cost only what
// they are. Everything else about it is a Map.
import { pathKey, parsePathKey } from './paths.js';

export class ClockMap extends Map {
  #children = new Map(); // parent key -> Set of child keys (entries and intermediate paths)

  constructor(entries) {
    super();
    if (entries) for (const [key, value] of entries) this.set(key, value);
  }

  set(key, value) {
    if (!super.has(key)) this.#link(key);
    return super.set(key, value);
  }

  delete(key) {
    if (!super.delete(key)) return false;
    this.#unlink(key);
    return true;
  }

  clear() {
    super.clear();
    this.#children.clear();
  }

  /** Keys of every entry strictly under `path`, in no particular order */
  *descendants(path) {
    const stack = [...(this.#children.get(pathKey(path)) ?? [])];
    while (stack.length) {
      const key = stack.pop();
      if (super.has(key)) yield key;
      const below = this.#children.get(key);
      if (below) stack.push(...below);
    }
  }

  #link(key) {
    const path = parsePathKey(key);
    for (let i = path.length; i > 0; i--) {
      const parent = pathKey(path.slice(0, i - 1));
      const child = i === path.length ? key : pathKey(path.slice(0, i));
      let set = this.#children.get(parent);
      if (set) {
        if (set.has(child)) return; // the ancestors above are linked already
        set.add(child);
        return;
      }
      set = new Set([child]);
      this.#children.set(parent, set);
    }
  }

  #unlink(key) {
    const path = parsePathKey(key);
    for (let i = path.length; i > 0; i--) {
      const child = i === path.length ? key : pathKey(path.slice(0, i));
      // A path stays in the index while it is an entry or still has children
      if (super.has(child) || this.#children.has(child)) return;
      const parent = pathKey(path.slice(0, i - 1));
      const set = this.#children.get(parent);
      if (!set) return;
      set.delete(child);
      if (set.size) return;
      this.#children.delete(parent);
    }
  }
}
