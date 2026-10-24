// relay/index.js - lazy-storage/relay: a follower of the server, and where it keeps its copies
//
// The follower itself (follower.js) uses nothing of Node's, so a browser's
// tabs run it too (client/shared.js); fileCopies is Node's and Bun's.
export { createRelay } from './follower.js';
export { memoryCopies } from './copies.js';
export { fileCopies } from './storage.js';
