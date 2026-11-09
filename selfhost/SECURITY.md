# How to protect broker transport

BetterMQ's public, admin, internal replication/Raft, and fleet/lease listeners
serve **plain HTTP**, not native TLS. Bearer tokens, cluster secrets, signing
secrets, and message bodies are readable to anyone on an unprotected network
path. A separate internal listener prevents public routing; it does not encrypt
traffic. Never send these credentials over the public internet with `http://`.

## Protect browser and API traffic with TLS

1. Bind the broker's public port to loopback and terminate TLS with a maintained
   reverse proxy. For Compose, replace the host mapping with
   `127.0.0.1:8080:8080` (8081–8083 for the HA examples); do not leave a second
   `0.0.0.0` mapping alongside it. Restrict the host firewall as defense in depth.
2. Point a real DNS name at your proxy and install a valid certificate. For
   example, a Caddy instance on the **same host** can use this Caddyfile:

   ```caddyfile
   broker.example.com {
       @internal path /internal/*
       respond @internal 404
       reverse_proxy 127.0.0.1:8080
   }
   ```

   Caddy obtains/manages the public TLS certificate when DNS and ports 80/443
   are configured. If the proxy is a container, loopback refers to that
   container: use an isolated shared network and do not publish the broker port.
3. Use only `https://broker.example.com` in API clients and browser bookmarks.
   A redirect does not protect a token already sent to an HTTP URL. Do not use
   `curl -k`, invalid-certificate exceptions, or plaintext upstream connections
   over another host's network. Configure proxy body limits and timeouts for
   your message sizes and long-poll traffic.
4. Keep any separate admin listener private to operators. If exposing the
   embedded panel, its authenticated `/admin/v1` API must reach the same
   protected management origin. Do not publish the internal listener or proxy
   `/internal/v1/*` on the public hostname.

## Protect cross-host cluster and fleet traffic

1. Put **every** broker, controller, fleet worker, shared metadata service, and
   object-store connection on a private encrypted transport. For multi-host
   cells use WireGuard (or equivalent authenticated host-to-host encryption),
   or mutually authenticated TLS proxies/sidecars on both ends. A private IP
   without a trusted/encrypted network is not sufficient on an untrusted LAN.
2. With WireGuard, bind or firewall peer listeners to the VPN interface, publish
   only that interface's address if containers need host port mapping, and
   advertise VPN-reachable URLs to peers. Allow ingress only from enrolled VPN
   peer addresses. Use split internal listeners; keep public API and management
   endpoints out of the cluster trust boundary where possible.
3. Configure fleet `BETTERMQ_BROKER_URLS` to the private **internal** endpoints.
   HA Compose uses Docker-network-only `http://brokerN:9080`; these names are
   a single-host example, not routable multi-VPS addresses. A Docker bridge
   alone does **not** encrypt traffic spanning physical hosts. Do not use a
   public `http://HOST:8080` as a multi-host peer or fleet URL.
4. For mTLS proxies, verify server identity and require client certificates
   issued to enrolled nodes only. Bind the plaintext upstream to loopback or
   an isolated same-host network. Proxy **all** internal Raft, replication,
   catalog, auth-propagation, and lease paths with suitable streaming/body/time
   limits, not only health probes. Never disable upstream TLS verification.
5. Generate `BETTERMQ_CLUSTER_SECRET` and `BETTERMQ_METRICS_TOKEN` separately
   with `openssl rand -hex 32`. Keep `.env`, controller configuration, and
   backups private (0600/0700), distribute secrets out of band, and never put
   them in URLs or access logs. Do not use the cluster secret as a public API
   credential. If a secret crossed an untrusted plaintext path, treat it as
   compromised and perform a coordinated maintenance-window rotation across
   all members and workers; changing one node alone breaks cluster auth.
6. Use HTTPS with verification for production S3/R2/MinIO endpoints. Keep MinIO
   root API/console ports private; the single-node Compose MinIO is development
   storage, not object-store HA. Protect shared filesystem traffic separately
   if local HA shared metadata crosses hosts.

## Verify before sending production data

1. From outside the trusted network, confirm only intended TLS ingress is
   reachable. Internal/admin/MinIO ports must be blocked, and public
   `/internal/v1/cluster/catalog` must return 404 rather than an auth challenge.
2. Check certificate chain and hostname validation with a normal HTTPS client.
   If using mTLS, verify missing/untrusted client certificates are rejected.
3. Verify every advertised peer and fleet URL uses the VPN or verified TLS
   path. Packet capture on the physical interface must not reveal HTTP headers
   or payloads (capture only in an authorized test environment).
4. Check readiness/quorum and deliver an idempotent canary using
   [OPERATIONS.md](OPERATIONS.md). Do not open a public plaintext port to work
   around a failed proxy or VPN healthcheck.
