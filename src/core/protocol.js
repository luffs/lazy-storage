// protocol.js - The wire protocol's version
//
// A hello says which protocol the client speaks (`proto`), and a server
// that speaks another closes the store for it with code 'outdated', so
// a page left open across a deploy learns to reload rather than have its
// edits judged by rules it does not know. Bumped when a message gains a
// field a server of the old version would judge wrongly without (2: an op's
// `v` and `epoch`, what its batch was made in sight of).
export const PROTOCOL = 2;
