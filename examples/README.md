# Examples

Small, complete programs that run from a checkout of this repository
(`bun install` first). Each serves the library's own source, so there is
no build step.

- **`basic/`** — a shared list in the browser over a Bun server.
  `bun examples/basic/server.js`, then open http://localhost:3200 in two
  tabs (or two browsers): edits appear everywhere, survive a reload while
  offline, and the server keeps them in `examples/basic/state.sqlite`.
  The page is 80 lines of vanilla JavaScript; the server 40.
- **`vue/`**, **`vue-options/`**, and **`react/`** — the same list as a
  Vue app (Composition API, inline), Vue single-file components (Options
  API: `App.vue` owns the client, `SharedList.vue` is the list; compiled
  in the browser by vue3-sfc-loader), and a React app, served by either
  server at http://localhost:3200/vue,
  http://localhost:3200/vue-options, and http://localhost:3200/react, on
  the same store as the basic page, so all of them can be open at once.
  Every page uses `sharedConnection`, so however many of them a browser
  has open, it holds one socket and is one replica; the tabs elect a
  leader, and the others follow it.
  Each loads its framework from a CDN through an import map, so there is
  no build step. The Vue ones use `useClient` from `lazy-storage/vue` (a
  reactive mirror patched on every batch, and writes to the client); the
  React one `useClient` from `lazy-storage/react` (the client's state
  read directly, re-rendered on every batch).
- **`node/`** — the same page served by Node instead of Bun, with the `ws`
  package for sockets and `node:sqlite` for storage (Node 22.13 or later).
  `node examples/node/server.js`.
- **`mirror.js`** — a client that runs on the server side: it follows the
  `shared` store of either server from a Bun or Node process, prints every
  change, and keeps its own copy in SQLite when run with Bun. Start one of
  the servers, then `bun examples/mirror.js` (or `node examples/mirror.js`).

The todo app that drove the library's design is a fuller reference:
https://github.com/luffs/kimi-wa-best-todo.
