// lazy-storage - client entry (the server lives under 'lazy-storage/server')
export * from './core/index.js';
export { createClient } from './client/index.js';
export { memoryOutbox, localStorageOutbox } from './client/storage.js';
export { webSocketTransport } from './client/transport.js';
