// lazy-storage - client entry (the server lives under 'lazy-storage/server')
// LazyWatch is re-exported so an application can drive `db.state` with the
// very instance lazy-storage uses (two copies of lazy-watch in one bundle
// would not recognize each other's proxies).
export { LazyWatch } from 'lazy-watch';
export * from './core/index.js';
export { createClient, openClient } from './client/index.js';
export { createConnection } from './client/connection.js';
export { memoryOutbox, localStorageOutbox } from './client/storage.js';
export { indexedDBStorage } from './client/indexeddb.js';
export { webSocketTransport } from './client/transport.js';
