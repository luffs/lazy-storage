// native.js - Node's own fetch classes, kept before a DOM is put on the globals; import it before dom.js
//
// happy-dom brings a Request, Response and Headers of its own. A test that
// runs a page against a server's handler in one process wants the page to
// have the DOM and the handler what it has in a server: Node's.
export const native = { Request: globalThis.Request, Response: globalThis.Response, Headers: globalThis.Headers, fetch: globalThis.fetch };
