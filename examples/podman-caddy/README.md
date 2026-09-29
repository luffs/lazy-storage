# The shared list behind Caddy, with relays on the other cores

How to run a lazy-storage server for as many clients as one machine takes,
in a podman container behind Caddy. It serves the basic example's page
(`../basic/index.html`), on the same `shared` store.

A store lives in one process and uses one core, and each change costs a
write to every socket that hears it. So the clients never connect to the
server itself: they connect to relays, which share one port and read
every store once from the server, on a link of their own, and write each
change to their clients from the other cores. The server merges, stores
(SQLite, with its commits grouped and its log checkpointed on a thread of
its own) and judges who may read what; Caddy ends TLS.

```
client ──TLS── Caddy :8443 ── relay :3000 ┐
                              relay :3000 ┴── server 127.0.0.1:3201 ── /data/stores.sqlite
```

- `main.js` — the container's entry: starts the server and `RELAYS` relays
  (2 by default), gives the relays a token for their link, passes
  `podman stop` on to them, and ends when one of them does.
- `server.js` — the stores and the route the relays link to, on
  127.0.0.1 only.
- `relay.js` — what the clients connect to, on `PORT`, with `reusePort`:
  on Linux the kernel spreads the connections over the relays.
- `Containerfile`, `Caddyfile`.

## Without containers

```bash
PORT=3200 bun examples/podman-caddy/main.js
```

then open http://localhost:3200 in two tabs, as with the basic example.

## In a podman pod, behind Caddy

Caddy and the app in one pod reach each other on localhost, so only
Caddy's port is published. From the repository's root:

```bash
podman build -f examples/podman-caddy/Containerfile -t lazy-storage-example .
podman pod create --name lazy-example -p 8443:8443 --sysctl net.ipv4.ip_local_port_range="10000 65535"
podman run -d --pod lazy-example --name lazy-example-app --ulimit nofile=65536:65536 -v lazy-example-data:/data lazy-storage-example
podman run -d --pod lazy-example --name lazy-example-caddy --ulimit nofile=131072:131072 -v "$PWD/examples/podman-caddy/Caddyfile:/etc/caddy/Caddyfile:ro,Z" docker.io/library/caddy:2
```

then open https://localhost:8443 (the browser warns about Caddy's local
certificate). For a real domain, publish 80 and 443 and run Caddy with
`-e SITE=app.example.com`. The sysctl and the two limits are for many
clients ("Open files" and "Ports to the relays" below say why); the
sysctl is the pod's own, as its network is, and leaves the host's alone.

## For your own app

- **Authentication.** The example takes a name from the query string. A
  real app checks a token or a cookie in the server's `authenticate`, and
  the relays pass on the header that carries it: in `key`, `upstream` and
  `credential` in `relay.js` (the comment there says how).
- **How many relays.** On 4 cores with Caddy on the same machine, 2: the
  server, the relays and Caddy about a core each. Without Caddy there, or
  with more cores, more (`RELAYS`). Watch `top` under load: relays at a
  full core and room elsewhere means one more.
- **Compression.** The server compresses nothing (only the relays
  connect, over loopback), and the relays compress what passes 4 KB for
  the clients, who are on the internet: a new client's snapshot above
  all. With relays on the clients' own networks it goes the other way
  round; the repository's README ("Spreading the load") says how to
  choose, hop by hop.
- **Open files.** Every client's socket at a relay is one, and every
  writing client's socket up to the server another (it closes after 30 s
  without an edit): `--ulimit` (or `Ulimit=` in a Quadlet unit) well above
  your number of clients. Caddy holds two sockets for every client, the
  client's and its own to a relay, so give its container twice that.
- **Ports to the relays.** Caddy cannot put clients' WebSockets on fewer
  sockets (their frames do not say whose they are), so it opens one to a
  relay for every client, all from 127.0.0.1 to 127.0.0.1:3000, and only
  the source port tells them apart: Linux's default range has some 28,000.
  The pod's sysctl above widens it to some 55,000, starting past the
  ports the pod listens on. Past that, give Caddy more addresses to dial:
  all of 127.0.0.0/8 is loopback and the relays listen on every address,
  so `reverse_proxy 127.0.0.1:3000 127.0.0.2:3000 127.0.0.3:3000` is a
  range for each.
- **Data.** `/data` is a volume, so the SQLite file survives the
  container. `podman stop` gives the processes 10 s, and the server writes
  what is pending and closes the file in far less.
- **Caddy reloads.** `stream_close_delay` keeps open WebSockets through a
  config reload, rather than every client reconnecting at once.
- **Measure** with your numbers of clients and writes, and your patch
  sizes: `npm run bench:fanout` (see the repository's README), from
  another machine through Caddy.
