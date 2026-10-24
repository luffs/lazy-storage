// protocol.js - The wire protocol's version
//
// A hello says which protocol the client speaks (`proto`), and a server
// that speaks another closes the store for it with code 'outdated', so a
// page left open across a deploy learns to reload rather than have its
// edits judged by rules it does not know. 3: changes on records and
// paths, a millisecond timestamp per op (1.0).
export const PROTOCOL = 3;
