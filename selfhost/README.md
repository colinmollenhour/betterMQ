# BetterMQ self-host

### Pin deployment versions

Before **any** Compose example, export `BETTERMQ_VERSION` to a tested fork
release tag (for example `0.6.2`, only if published), including source builds.
Compose deliberately fails when it is unset. Never set it to `latest` in HA.
Slate examples also require `MINIO_IMAGE` and `MINIO_MC_IMAGE` as complete,
tested release references, preferably `minio/minio@sha256:<digest>` and
`minio/mc@sha256:<digest>`. No floating MinIO defaults are supplied. Record these
values in a protected deployment `.env` and retain them with offline backups.
Pinning requires deliberate version selection; this is intentional rather than
silently pulling a new release on the next restart. See [BACKUP.md](BACKUP.md)
for the per-node upgrade procedure.

### Shutdown grace

Compose gives broker, fleet and panel services 120 seconds to stop. The broker's
`BETTERMQ_SHUTDOWN_TIMEOUT_SECS` (default 30, maximum 300) is a **per-phase**
budget for HTTP drain, dispatch drain and WAL flush, followed by up to 5 seconds
of task joining. Set `BETTERMQ_STOP_GRACE_PERIOD` above `3 × timeout + 5s` if
raising that budget (for example `920s` for 300). For Docker run use
`--stop-timeout 120`; Kubernetes needs `terminationGracePeriodSeconds: 120`
(plus any preStop-hook time). Do not override Compose with `stop -t 10`.

HA Compose isolates `/internal/v1/*` on Docker-network port **9080**, which is
never published. Fleet workers use that private endpoint; host ports 8081–8083
are for public API/panel access only. Generate the shared cluster secret with
`openssl rand -hex 32`; never use example passwords in production. Keep peer
traffic on a trusted private network (or encrypted VPN across hosts), and do
not publish internal ports through a load balancer or reverse proxy.

Each node has two advertised addresses: `publicUrl` remains the API/enrollment
URL, while optional `internalUrl` selects the private peer transport. Set local
`--internal-url` / `BETTERMQ_INTERNAL_URL` (Compose advertises
`http://brokerN:9080`), or `node.internalUrl` and `cluster.nodes[].internalUrl`
in managed JSON. Enrollment and membership propagation carry both URLs. Raft,
replication/catch-up, peer health, catalog/auth sync and internal forwarding use
`internalUrl`; old configurations without it fall back to the public URL.
For fleet/gateway-only processes, explicitly configure `BETTERMQ_BROKER_URLS`
with the internal endpoints. Reachable public enrollment URLs are still required.

HA examples require both `BETTERMQ_CLUSTER_SECRET` and `BETTERMQ_METRICS_TOKEN`;
generate each separately with `openssl rand -hex 32`. Slate also requires
`MINIO_ROOT_USER` and a strong `MINIO_ROOT_PASSWORD`. Fleet Compose healthchecks
use `/healthz` so workers can start before first-time auth/cluster setup. A
healthy container is **not** necessarily ready to serve: configure local auth
and membership using the management panel, then require `/readyz` on every
broker before sending traffic. Fleet workers retry while brokers are unready.

Free self-hosted BetterMQ. **No config files required** — install the CLI or run Docker, then use the panel.

Shared engine code lives in [`../engine/`](../engine/). This folder is the self-host product (Docker + docs only).
Standalone panel Compose stores `cell-registry.json` in its `panel-data` named
volume at `/data`. Container recreation and image upgrades retain the registry;
`docker compose down --volumes` deletes it. Include this volume in backups.
For node loss, replacement, seed loss, drain/rebalance and controller changes,
use the [HA operations runbook](OPERATIONS.md).
For any internet-facing or multi-host deployment, follow the
[TLS and encrypted peer transport guide](SECURITY.md). The binary itself does
not provide TLS; HTTP peer examples assume an isolated local Docker network.

### Panel build contract

Rust builds and Docker images package the committed `engine/panel/dist` only;
Cargo never runs npm. When editing panel sources, use the CI-pinned Node
26.13.0, run `npm ci && npm run build` in `engine/panel`, and commit the generated
assets with the source changes. CI requires the build/typecheck and dependency
audit to pass and rejects any difference between generated and committed assets.

## Install CLI (recommended)

Downloads the latest release binary from GitHub — no Rust toolchain or Docker required.
Defaults target `colinmollenhour/betterMQ` releases and
`ghcr.io/colinmollenhour/bettermq`, not upstream. Override `BETTERMQ_REPO` for
installers or `BETTERMQ_IMAGE` (repository without tag) / `BETTERMQ_VERSION` for
Compose when mirroring artifacts. A missing fork release is an error, not a
fallback to upstream. Source builds use the checkout's code with `--build`.

### macOS, Linux, WSL, Git Bash (Windows)

```bash
curl -fsSL https://raw.githubusercontent.com/colinmollenhour/betterMQ/main/install.sh | bash
```

Installs to `~/.bettermq/bin` and links `bettermq` into `~/.local/bin` (add that directory to your `PATH` if prompted).

Then start the server:

```bash
bettermq serve
open http://localhost:8080/panel/    # macOS
# xdg-open http://localhost:8080/panel/   # Linux
```

The installer can offer to run `bettermq serve` immediately when run in an interactive terminal.

### Windows (PowerShell or CMD)

Stock Windows does not ship with `bash`. Use the PowerShell installer instead:

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/colinmollenhour/betterMQ/main/install.ps1 | iex"
```

Then open a **new** terminal and run:

```powershell
bettermq serve
```

Panel: http://localhost:8080/panel/

### Options

| Variable / arg | Effect |
|----------------|--------|
| `BETTERMQ_FORCE=1` | Reinstall even if the same version is already present |
| `BETTERMQ_NO_START=1` | Install only; do not prompt to start the server |
| `BETTERMQ_INSTALL_DIR` | Base install dir (default `~/.bettermq`) |
| `BETTERMQ_BIN_DIR` | Directory for the `bettermq` symlink (default `~/.local/bin`) |
| `BETTERMQ_VERSION` | Windows installer: pin a release (e.g. `0.3.1`). Unix: pass the version as the first argument instead |
| First argument `0.3.1` | Unix installer: install a specific version instead of latest |

### First-time setup

Open **http://localhost:8080/panel/**, enter the **setup code** and set a panel password. Copy the `sk_local_…` API token shown once.

The setup code is printed to the process's stderr when it starts unconfigured (`Setup code: XXXX-XXXX-XXXX-XXXX`; with Docker: `docker logs bettermq`). It is kept in memory only, so someone who can reach the port but cannot read your logs cannot claim the broker. `BETTERMQ_SETUP_CODE` presets the code for scripted installs.

Setup is open for **15 minutes** after the process starts. If the window closes, restart the process (which also prints a new code). `BETTERMQ_ALLOW_OPEN_SETUP=1` keeps it open until a password is set **and skips the setup code** — only for trusted networks. `BETTERMQ_SETUP_WINDOW_SECS` changes the duration (`0` locks setup).

On a remote server, prefer publishing the port on loopback (`-p 127.0.0.1:8080:8080`, as `docker-compose.yml` does) and open the panel through an SSH tunnel (`ssh -L 8080:127.0.0.1:8080 server`) or a TLS reverse proxy.

Data (queues, WAL, schedules) is stored under `./data` in the current directory unless you pass `--data-dir`.

```bash
bettermq serve --data-dir /var/lib/bettermq
```

---

## CLI reference

Verified from `bettermq --help` / `bettermq <cmd> --help` on the self-host binary. For the live list on your install: `bettermq --help`.

### Commands

| Command | Purpose |
|---------|---------|
| `bettermq serve` | Run the HTTP broker (data plane) |
| `bettermq panel` | Standalone management gateway + panel (no broker storage) |
| `bettermq config init` | Write a starter `bettermq.json` |
| `bettermq config validate` | Validate a `bettermq.json` |
| `bettermq config schema` | Print JSON Schema for editors |
| `bettermq doctor` | Check config compatibility, WAL frames, and data-dir access |
| `bettermq support-bundle` | Write a redacted JSON diagnostic bundle |
| `bettermq cluster init` | Retired: exits with migration instructions; no files changed |
| `bettermq cluster join` | Retired: use Infrastructure enrollment; no files changed |

Prefer creating/joining a cluster from the **panel → Infrastructure**. `cluster` subcommands remain for scripting.

### `bettermq serve`

```text
bettermq serve [OPTIONS]
```

| Flag | Env | Meaning |
|------|-----|---------|
| `-c`, `--config <PATH>` | `BETTERMQ_CONFIG` | Path to `bettermq.json` |
| `--listen <HOST:PORT>` | `BETTERMQ_LISTEN` | Listen address (overrides `-p` and config) |
| `-p`, `--port <PORT>` | `BETTERMQ_PORT` | Bind `0.0.0.0:{port}` (default 8080 if unset) |
| `--data-dir <DIR>` | `BETTERMQ_DATA_DIR` | Persistent data (WAL, RocksDB, auth, managed config) |
| `--cluster <true\|false>` | `BETTERMQ_CLUSTER` | Enable/disable cluster mode (overrides config) |
| `--broker-only` | `BETTERMQ_BROKER_ONLY` | Compatibility alias for `--profile broker` |
| `--gateway-only` | `BETTERMQ_GATEWAY_ONLY` | Compatibility alias for `--profile gateway` (no WAL) |
| `--dispatch-fleet` | `BETTERMQ_DISPATCH_FLEET` | Compatibility alias for `--profile dispatch` |
| `--profile <NAME>` | `BETTERMQ_PROFILE` | `all` (default), `broker`, `dispatch`, `gateway`, `panel` |
| `--components <A,B>` | `BETTERMQ_COMPONENTS` | Extra components to enable (comma-separated) |
| `--no-panel` | `BETTERMQ_NO_PANEL` | Disable the embedded panel |
| `--admin-listen <HOST:PORT>` | `BETTERMQ_ADMIN_LISTEN` | Private admin + panel listener |
| `--internal-listen <HOST:PORT>` | `BETTERMQ_INTERNAL_LISTEN` | Replication/controller listener |
| `--panel-listen <HOST:PORT>` | `BETTERMQ_PANEL_LISTEN` | Alias: separate panel+admin listener |

Listener roles sharing an address bind that socket only once. Setting only
`--admin-listen` (or `--panel-listen`) moves admin routes, docs and the panel off
the public socket; internal routes remain on public unless `--internal-listen`
is also set. An explicit `--admin-listen` takes precedence over the panel alias.
For the public bind address, CLI/env `--listen` or `--port` wins over
`listeners.public`; `node.listen` remains the compatibility fallback.
`config validate` and `doctor` check V2 component dependencies, panel modes and
all listener addresses. Invalid listener strings fail instead of falling back
to a potentially public socket. Separate-panel mode needs a panel/admin address.
Controller-only operation is not implemented: `--profile controller` and a
controller component without broker fail explicitly. Use `--profile broker`
for controller + ingest without local delivery; it retains the setup panel.

Examples:

Configuration precedence is CLI flags, then supported environment variables,
then the saved file, then defaults. `BETTERMQ_STORAGE`, `S3_*`, webhook timeout
variables and `BETTERMQ_DEFAULT_MAX_RETRIES` override file values. Invalid values
or incomplete Slate credentials fail startup rather than selecting local storage.
See the [configuration reference](configuration.md) for V2 fields and how to
generate the JSON Schema. Unknown configuration keys are rejected.

Config saves use private (Unix `0600`) temporary files and atomic replacement,
with file and directory sync. Newly created config directories are private
(`0700`); existing directories retain their permissions.

The cluster template stores local auth at `/cluster-shared/meta/local-auth.json`,
inside the HA compose persistent metadata mount. Startup checks that the auth
directory is writable before opening first-boot setup. Custom auth paths must
also point at a persistent writable volume.

```bash
bettermq serve
bettermq serve -p 9000
bettermq serve --listen 0.0.0.0:8080 --panel-listen 127.0.0.1:8090
bettermq serve --data-dir /var/lib/bettermq --broker-only
bettermq serve --profile gateway
bettermq panel --listen 127.0.0.1:8090 --controller http://broker1:8080
BETTERMQ_BROKER_URLS=http://broker1:8080,http://broker2:8080 bettermq serve --dispatch-fleet
```

Multi-node cells default to **RF=3 / minISR=2** (limited by the initial node count).
Additional nodes host other shards rather than extra copies of every message.
An explicit `replication.factor` / `replication.minIsr` must match the controller's
effective policy: startup fails on mismatch rather than acknowledging fewer
copies than configured. Change an existing policy through the admin replication
policy API and align the saved config before restart. ACKs never cross regions;
a global `bettermq panel` federates cells.

The standalone panel authenticates through `/admin/v1` using its configured
`BETTERMQ_CLUSTER_SECRET` bearer credential, not a broker's `sk_local_` token.
Its Infrastructure view queries the configured controller through the admin
connection. Broker-only queue, publishing, scheduling, HTTP, node settings and
storage screens are available only in the broker's embedded panel.

Each remote cell also needs its own `sk_local_` admin token: enter it in
Infrastructure → Cells → Create/Edit cell. This write-only field is never returned
by reads; leaving it blank on edit keeps the saved credential. Federated reads
use only that cell credential, never the panel login bearer or cluster secret.
If a cell reports "cell credential not configured", edit its entry to supply the
token. Rotating a cell's token requires updating its saved credential here.

Infrastructure's **Create cluster** initializes only an unclustered node and
requires confirmation. The API returns 409 if saved or active membership already
exists; it never replaces a cluster. Join an existing cluster instead.

Selecting a remote cell enables federated Infrastructure inspection only.
Broker-only screens and requests are blocked while a remote cell is selected;
open that cell's embedded panel to manage queues, schedules or HTTP delivery.
The console labels the local API target explicitly.

Panel destination forms start empty and require an explicit HTTP(S) destination.
If the secret is blank, submission generates a unique cryptographically random
secret; use **Reveal** to copy it to your receiver before enabling HMAC signing.

The live `/v1/infra/storage` API returns **409** for every backend or storage
location change (endpoint, WAL bucket, payload bucket or region), even when the
store currently appears empty. Checking emptiness cannot prevent new writes
before restart. The panel makes these fields read-only; same-location credential
updates remain supported and require a restart to take effect.

There is no automatic storage migration or force override. To change stores,
stop producers and ingestion, preserve the original data and configuration, and
use a verified offline migration with validation of pending, retrying and DLQ
records before resuming traffic. Alternatively, create a new deployment and keep
the original available until its accepted work is delivered or recovered. For a
cluster, coordinate the procedure across all replicas; do not switch nodes one
at a time. Simply changing configuration does not move messages.

### `bettermq config`

```bash
bettermq config init [-o bettermq.json] [--template local|slate|cluster]
bettermq config validate [-c bettermq.json]
bettermq config schema
```

| `--template` | Meaning |
|--------------|---------|
| `local` (default) | Single node, local WAL + RocksDB |
| `slate` | Single node, SlateDB + S3-compatible store |
| `cluster` | Three-node HA cluster (local storage) template |

### `bettermq cluster` (retired)

`cluster init` and `cluster join` now exit nonzero with migration instructions
without contacting a seed or modifying membership files. The old commands used
inconsistent node IDs and a local-only join did not register the new member.

For initial static configuration, run `bettermq config init --template cluster`,
set named `cluster.nodes` and each host's `node.name`, then validate before serving.
For enrollment into an existing cluster, use Panel → Infrastructure with a seed
join token (the `POST /v1/infra/cluster/enroll` flow), which registers membership
instead of merely copying a seed's local file.

### `bettermq panel`

Standalone management gateway (no broker storage). Same `--config` / `--listen` as `serve`, plus:

| Flag | Env | Meaning |
|------|-----|---------|
| `--controller <URL>` | `BETTERMQ_CONTROLLER` | Broker/controller base URL this panel talks to |
| `--data-dir <DIR>` | `BETTERMQ_PANEL_DATA_DIR` | Panel-only data directory (`bettermq panel`) |

### Related environment variables

The public repo reads **92** `BETTERMQ_*` names used by self-host. **16** are CLI flags for `serve`/`panel` (tables above). **5** are installer-only (`BETTERMQ_FORCE`, `BETTERMQ_NO_START`, `BETTERMQ_INSTALL_DIR`, `BETTERMQ_BIN_DIR`, `BETTERMQ_VERSION`). The rest are process env (some also written from `bettermq.json` at startup).

#### Auth, setup, metrics

| Env | Used for |
|-----|----------|
| `BETTERMQ_ALLOW_OPEN_SETUP` | Keep setup open until a password is set (no time window, no setup code) |
| `BETTERMQ_SETUP_CODE` | Preset the first-boot setup code (default: random, printed to stderr) |
| `BETTERMQ_SETUP_WINDOW_SECS` | First-boot open-setup duration (default `900`; `0` locks until restart or allow-open) |
| `BETTERMQ_INSECURE_NO_AUTH` | Skip auth (`1`/`true`/`yes`). Local development only |
| `BETTERMQ_LOCAL_AUTH_FILE` | Path to local auth file (also set from config) |
| `BETTERMQ_ALLOW_QUERY_SECRET` | Deprecated: accept GET publish `?secret=` (`1`) |
| `BETTERMQ_METRICS_TOKEN` | Bearer / header gate for `/metrics` (required in HA compose files) |
| `BETTERMQ_JOIN_TOKEN_QUERY` | Allow join token in query string (`1`/`true`/`yes`) |
| `BETTERMQ_TRUST_PROXY` | Trust `X-Forwarded-For` for rate-limit client IP |

#### Cluster, fleet, identity

| Env | Used for |
|-----|----------|
| `BETTERMQ_CLUSTER_SECRET` | Internal `/internal/v1/*` (replication, lease, catalog). Also an admin credential for `/admin/v1`, and the **only** one on a standalone `bettermq panel` (required there) |
| `BETTERMQ_SHARED_META_DIR` | Local-WAL HA only: one shared filesystem for leases, cursors, catalogs, host breaker. Each node still has its own `--data-dir`. Skip this when nodes have no shared disk — use Slate + S3 instead |
| `BETTERMQ_BROKER_URLS` | Fleet/gateway: comma-separated broker base URLs |
| `BETTERMQ_FLEET_CONCURRENCY` | Fleet in-flight jobs (default `1`) |
| `BETTERMQ_FLEET_HOLDER` | Optional fleet worker id |
| `BETTERMQ_LONG_WAIT_TIER` | Fleet long HTTP wait: `15m` \| `1h` \| `6h` \| `12h` |
| `BETTERMQ_NODE_NAME` | Node display name (also set from config) |
| `BETTERMQ_NODE_PUBLIC_URL` | Node public HTTP URL (also set from config) |
| `BETTERMQ_CELL_LABEL` | Cell label in panel bootstrap and admin (default `local`) |
| `BETTERMQ_CELL_REGION` | Cell region in admin (default `local`) |

#### Ingest, HTTP, payloads

| Env | Used for |
|-----|----------|
| `BETTERMQ_INGEST_MAX_IN_FLIGHT` | Process in-flight ingest records (default `4096`) |
| `BETTERMQ_INGEST_MAX_QUEUED_BYTES` | Process queued-byte cap (default 64 MiB) |
| `BETTERMQ_INGEST_MAX_TENANT_RECORDS` | Per-tenant queued-record cap; excess gets `429` |
| `BETTERMQ_INGEST_MAX_TENANT_BYTES` | Per-tenant queued-byte cap; excess gets `429` |
| `BETTERMQ_MAX_HTTP_BODY_BYTES` | HTTP body cap (default 64 MiB) |
| `BETTERMQ_INLINE_MAX_BYTES` | Inline payload threshold before blob store (default 256 KiB) |
| `BETTERMQ_MAX_PAYLOAD_BLOB_BYTES` | Hard cap for blob payloads (default 256 MiB) |
| `BETTERMQ_CORS_ORIGINS` | CORS allowlist (`*` or comma-separated origins) |
| `BETTERMQ_SHUTDOWN_TIMEOUT_SECS` | Per-phase HTTP, dispatch and WAL drain bound (default 30 seconds each, maximum 300); see Shutdown grace |

#### Dispatch / delivery

| Env | Used for |
|-----|----------|
| `BETTERMQ_ALLOW_PRIVATE_DESTINATIONS` | Allow loopback/LAN webhook destinations |
| `BETTERMQ_DISPATCH_MAX_IN_FLIGHT` | Concurrent outbound deliveries across direct, flow-controlled, and fleet paths per process (default `256`) |
| `BETTERMQ_DISPATCH_QUEUE_CAP` | Ready-channel capacity and separate aggregate flow-work capacity (default `4096` each); overflow remains durable for backfill |
| `BETTERMQ_DISPATCH_GLOBAL_MAX` | Optional global in-flight delivery cap |
| `BETTERMQ_DEFAULT_MAX_RETRIES` | Default webhook retries (default `3`) |
| `BETTERMQ_HTTP_TIMEOUT_SECS` | Webhook HTTP timeout (default `300`; overrides config) |
| `BETTERMQ_LONG_HTTP_TIMEOUT_SECS` | Long-payload webhook timeout (default `7200`; overrides config) |
| `BETTERMQ_FLOW_WAITLIST_CAP` | Per-lane flow waitlist (default `8192`) |
| `BETTERMQ_MEMORY_LIMIT_MB` | Pause dispatch fetches above this RSS (optional) |
| `BETTERMQ_FAIRNESS_TENANT_SOFT_LIMIT` | Tenant fairness tracking soft cap (default `1024`) |

Unpaused, unpinned flow lanes are reclaimed after 60 seconds idle (no queued or active deliveries). Paused lanes and operator pins remain until explicitly cleared.

DNS lookup failures (including the two-second resolution timeout) use the normal durable retry budget in both local and fleet dispatch. Egress-policy rejections remain terminal.

Retries hold back later work only within the same strict-FIFO flow key. Other keys and unconstrained destinations continue independently; each retry still observes its own durable backoff deadline.

Webhook connections use reusable short/long HTTP pools. DNS is policy-checked on each new connection and all allowed answers are available for connection fallback. Redirects and environment HTTP proxies are disabled so outbound connections cannot bypass the egress policy. `BETTERMQ_LONG_WAIT_TIER`, when set, overrides the long-payload pool timeout.

With shared metadata, host-breaker snapshots merge per-host revisions under the shared file lock; clear markers prevent stale peers from resurrecting removed blocks. Only block/clear transitions persist, off the async delivery path. Keep broker clocks synchronized; clear markers are retained in shared metadata.

Panel destination block/unblock controls acknowledge only the serving node (`scope: serving_process`, `applied_nodes` in the API response). They do not confirm application to peers or fleet workers. Shared-metadata nodes may observe changes asynchronously; Slate/fleet deployments without a shared metadata directory have process-local controls. Stopping all fleet delivery requires deployment-level controls, not this panel action.

Dispatch metrics include `bettermq_dispatch_delivery_outcomes_total` (bounded HTTP class, transport error, or policy block), `bettermq_dispatch_webhook_duration_seconds` (attempt to response headers/failure), `bettermq_dispatch_flow_errors_total`, and per-shard `bettermq_dispatch_oldest_pending_age_seconds`. Pending age grows while delivery is stalled and is reconstructed from durable pending work on backfill/startup. It covers the bounded tracked cursor lanes; monitor `bettermq_dispatch_dropped_cursor_lanes_total` for coverage loss. No destination URLs or routing keys are metric labels.

Queue rejection, deferral, and delivery-failure logs are aggregated into at most one pressure summary per 30 seconds. Exact rejection/deferral totals remain available as `bettermq_dispatch_queue_rejected_total`, `bettermq_dispatch_deferred_total`, and `bettermq_dispatch_defer_rejected_total`. Per-message delivery details require debug logging; logged destinations contain only scheme, host and port, not credentials, paths or query strings.

#### DLQ and group fan-out

Fan-out vars are **group bookkeeping**, not DLQ TTL.

| Env | Used for |
|-----|----------|
| `BETTERMQ_DLQ_RETENTION_DAYS` | Auto-delete DLQ messages older than N days. Unset, `0`, or `never` keeps them forever (default) |
| `BETTERMQ_DLQ_RETENTION_INTERVAL_MS` | DLQ sweeper interval (default `900000` = 15m; clamp 5s–1d) |
| `BETTERMQ_FANOUT_MAX_ATTEMPTS` | Group fan-out delivery attempts (default `20`) |
| `BETTERMQ_FANOUT_REPLAY_INTERVAL_MS` | Fan-out replay tick (default `2000`; clamp 100–60000) |
| `BETTERMQ_FANOUT_RETENTION_MS` | Completed fan-out retention (default 24h) |
| `BETTERMQ_FANOUT_MAX_COMPLETED` | Completed fan-out entries kept (default `10000`) |

#### Storage, WAL, archive, replication

| Env | Used for |
|-----|----------|
| `BETTERMQ_STORAGE` | `local` or `slate` (also set from config) |
| `BETTERMQ_FSYNC` | WAL durability: `group` (default, shard commit epoch), `always` (every message), `os` (no explicit fsync) |
| `BETTERMQ_FSYNC_INTERVAL_MS` | Background group-commit timer in milliseconds (default `10`, clamp 1–60000) |
| `BETTERMQ_COMMIT_LINGER_MS` | Ingest waiter linger before shard fsync (default `1`, max 20) |
| `BETTERMQ_SHARD_LAYOUT` | New cell only: `v1` (default) or `v2` |
| `BETTERMQ_SHARD_COUNT` | New v2 cells: physical shard count (default 256). Never remaps an existing `shard-layout.json` |
| `BETTERMQ_SHARD_QUEUE_CAPACITY` | Per-shard I/O queue (default `1024`, clamp 1–65536) |
| `BETTERMQ_SEGMENT_READER_CACHE` | Open WAL segment file cache (default `64`, clamp 1–4096) |
| `BETTERMQ_METADATA_RECOVER` | `empty` allows replacing corrupt JSON meta with defaults |
| `BETTERMQ_MAX_REPLICATION_EPOCH_BYTES` | Replication epoch size (default 16 MiB) |
| `BETTERMQ_MAX_REPLICATION_IN_FLIGHT_BYTES` | Replication in-flight bytes (default 64 MiB) |
| `BETTERMQ_ARCHIVE_DIR` | Directory for async copies of sealed WAL segments |
| `BETTERMQ_ARCHIVE_QUEUE_CAPACITY` | Bounded sealed-segment archive queue (default 128) |
| `BETTERMQ_ARCHIVE_BUCKET` | S3/R2 bucket for sealed-segment archive (aliases `S3_ARCHIVE_BUCKET` / `R2_ARCHIVE_BUCKET`) |
| `BETTERMQ_ARCHIVE_PREFIX` | Object-key prefix inside the archive bucket |
| `BETTERMQ_ARCHIVE_MAX_RETRIES` | Archive upload retries (default `5`) |
| `BETTERMQ_ARCHIVE_MAX_QUEUED_BYTES` | Archive queue byte cap (default 4 GiB) |
| `BETTERMQ_ARCHIVE_MAX_LAG_MS` | Archive lag before health degrades (default 5 minutes) |

S3/R2 credentials for Slate and archive are **not** `BETTERMQ_*`: `S3_ENDPOINT` or `R2_ENDPOINT`, plus `S3_ACCESS_KEY` / `R2_ACCESS_KEY` / `AWS_ACCESS_KEY_ID`, matching secret and region vars, and `S3_BUCKET` / `R2_BUCKET` (payload blobs: `S3_PAYLOAD_BUCKET` / `R2_PAYLOAD_BUCKET`). See [`engine/broker-storage/ARCHIVE.md`](../engine/broker-storage/ARCHIVE.md).

#### Gateway-only profile

| Env | Used for |
|-----|----------|
| `BETTERMQ_GATEWAY_SHARD_COUNT` | Gateway shard count (default 256) |
| `BETTERMQ_GATEWAY_LAYOUT_VERSION` | Gateway layout version (default `2`) |
| `BETTERMQ_GATEWAY_TENANT` | Tenant id when forwarding (default `default`) |
| `BETTERMQ_GATEWAY_TOKEN` | Token the gateway presents to brokers |
| `BETTERMQ_GATEWAY_IDLE_CONNECTIONS_PER_HOST` | HTTP pool idle per host (default `16`) |

#### Panel bootstrap

| Env | Used for |
|-----|----------|
| `BETTERMQ_PANEL_DIR` | Serve panel assets from a directory instead of embedded (`engine/panel/dist` after `npm run build`) |
| `BETTERMQ_PANEL_MODE` | Runtime mode: `embedded`, `separate`, `disabled`, or `standalone` (set automatically from resolved settings; `bettermq panel` selects standalone) |
| `BETTERMQ_ADMIN_API_BASE` | Admin API prefix injected into the panel (default `/admin/v1`) |
| `BETTERMQ_PANEL_FLAGS` | Comma-separated feature flags injected into the panel |

#### Tools

| Env | Used for |
|-----|----------|
| `BETTERMQ_TOKEN` | Bearer token for `tools/bettermq-loadgen` |

Operational runbooks: [backup/restore and migration](BACKUP.md) and
[production launch/soak gates](PRODUCTION.md). Compose health checks use the
deep `/readyz` endpoint; `/healthz` is liveness only.

---

## Deploy on Railway

One-click deploy with HTTPS and a persistent `/data` volume:

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/bettermq?referralCode=O5l32o&utm_medium=integration&utm_source=template&utm_campaign=generic)

Panel: `https://<your-railway-domain>/panel/`

---

## Quick start (Docker)

Published image (recommended — no Rust build):

```bash
docker pull ghcr.io/colinmollenhour/bettermq:latest
docker run -d --name bettermq -p 8080:8080 -v bettermq-data:/data \
  ghcr.io/colinmollenhour/bettermq:latest serve --data-dir /data
open http://localhost:8080/panel/
```

Or Compose (pulls the same image; use `--build` only if you want to compile locally):

```bash
git clone https://github.com/colinmollenhour/betterMQ.git
cd betterMQ/selfhost
docker compose up -d
open http://localhost:8080/panel/
```

| Image | Notes |
|-------|--------|
| `ghcr.io/colinmollenhour/bettermq:latest` | Latest fork release |
| `ghcr.io/colinmollenhour/bettermq:0.6.2` | Pin a fork release version (must exist) |

1. Set a **panel password** and copy your API token.
2. **Infrastructure** — storage (local or Slate + S3), optional cluster.
3. **Create cluster** on the first broker, then **Add node** from that same panel (or **Join cluster** on the new broker’s panel).

Settings persist in the Docker volume at `/data/bettermq.json`.

## Compose files

| File | Use case |
|------|----------|
| `docker-compose.yml` | Solo — local WAL |
| `docker-compose.slate.yml` | Solo — Slate + MinIO (dev) |
| `docker-compose.ha.yml` | HA LAN — 3 brokers + shared meta volume |
| `docker-compose.ha-slate.yml` | HA multi-VPS — 3 brokers + MinIO/S3 |
| `docker-compose.ha-fleet.yml` | HA + `--broker-only` + `--dispatch-fleet` (scale fleet) |
| `docker-compose.ha-slate-fleet.yml` | Slate HA + fleet |

### Scale path

| Stage | Command |
|-------|---------|
| Solo | `docker compose up -d` |
| HA LAN | `BETTERMQ_CLUSTER_SECRET=… docker compose -f docker-compose.ha.yml up -d` |
| HA + Slate | `… -f docker-compose.ha-slate.yml up -d` |
| Egress fleet | `… -f docker-compose.ha-fleet.yml up -d --scale fleet=3` |

Roles (one binary):

- `bettermq serve` — broker + local dispatch
- `bettermq serve --broker-only` — ingest + controller + lease API + admin/panel (no local push workers; use `--no-panel` to hide the UI)
- `bettermq serve --dispatch-fleet` — claim/push via `BETTERMQ_BROKER_URLS`

### Panel on its own port

By default the panel is on the same listen address as the API (`http://localhost:8080/panel/`).

To bind the panel on a **separate** address (e.g. loopback-only while the API stays on `0.0.0.0:8080`):

```bash
bettermq serve --listen 0.0.0.0:8080 --panel-listen 127.0.0.1:8090
# or
BETTERMQ_PANEL_LISTEN=127.0.0.1:8090 bettermq serve
```

Then open **http://127.0.0.1:8090/panel/**. That panel port also serves the full `/v1` API and `/healthz` (same process) so the UI can talk to itself. The public API remains at `http://<host>:8080` without `/panel` or `/docs`. Replication stays on `--internal-listen` (default: same as `--listen` unless set).

Set `BETTERMQ_NODE_PUBLIC_URL` to the public API URL so cluster join advertises that host, not the panel port.

Useful when you expose the API publicly but keep the panel on localhost / VPN only. Day-to-day ops use the seed (or standalone `bettermq panel`).

Fleet long waits (fleet workers only): `BETTERMQ_LONG_WAIT_TIER=15m|1h|6h|12h`.

### Slate + MinIO

```bash
docker compose -f docker-compose.slate.yml up -d --build
```

In **Infrastructure → Storage**, choose SlateDB and set:

- Endpoint: `http://minio:9000`
- Buckets: `bettermq`, `bettermq-payloads` (payload bucket is **required**)
- Access key / secret: set `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` (no compose defaults)

**Note:** the MinIO in `docker-compose.slate.yml` is **one node**. Good for local/dev. Not real object-store HA. For production HA use a durable S3-compatible store (or multi-node MinIO) plus 3 brokers.

Any S3-compatible endpoint works the same way in the panel (endpoint, buckets, access key / secret).

### Multi-node HA (same cell)

Prefer compose profiles above, or run **one broker per server**. Two `all` nodes are a valid cluster but **not** failure-tolerant (RF=2 / minISR=2 — both must be up). A third broker is HA (RF=3 / minISR=2). A fourth adds shard capacity; RF stays 3.

**From one panel:**

1. Open broker 1 → **Infrastructure** → **Create cluster**. Copy the join token.
2. Start broker 2 on another port and data dir, e.g. `bettermq serve --listen 127.0.0.1:8082 --data-dir ./data2`.
3. On broker 1, **Add node**, paste **This panel connects here**. Two local processes: `http://127.0.0.1:8082`. Panel on the laptop and brokers in Docker: that published port, then tick **Other brokers cannot use that URL** and enter `http://broker2:8080`.
4. Restart every cluster node when the join response says so.
5. Later add broker 3 the same way for HA. Optional 4th = capacity, not RF=4.
6. Dispatch / gateway / panel: same form, other profile. They are **not** replicas.

Regenerating the API token (Settings) revokes it for every client and pushes the new credentials to every peer, which accept them because they carry a newer version; the response lists any peer that did not apply them (that peer still accepts the old token until **Re-sync**).

Every broker in a cell needs the same `BETTERMQ_CLUSTER_SECRET`. **Add node** signs the enroll request with it (the secret itself is never sent to the new node's URL), and a joining node refuses a seed that cannot prove it holds the same secret. A node that already has a panel password needs its own API token to enroll, and a node that is already in a cluster refuses to join another one unless `force` is set on `/v1/infra/cluster/join`.

`docker-compose.ha.yml` maps broker1→8081, broker2→8082, broker3→8083. Use the URL this panel process can call (compose DNS if the panel is in the compose network; `127.0.0.1` plus the published port if the panel is on the host).

Standalone panel: `BETTERMQ_CLUSTER_SECRET=… docker compose -f docker-compose.panel.yml up -d` then register each cell’s admin URL. The panel publishes on `127.0.0.1:8090` and authenticates admins with the cluster secret.

`/admin/v1` fails closed. Every route except `/admin/v1/health` needs `Authorization: Bearer <token>`: the local `sk_local_…` API token (embedded panel) or `BETTERMQ_CLUSTER_SECRET`. Before a panel password is set, admin calls return `401` (`auth_not_configured`); a standalone panel without a cluster secret rejects every admin call. Registering a remote cell (`POST /admin/v1/cells`) requires `adminToken`, that cell's own `sk_local_…` token; leave it blank on update to keep the saved one. Federated cell queries authenticate to each cell with only that token (never the caller's bearer or the cluster secret); a cell without one is reported as `cell credential not configured`. Responses show `adminTokenConfigured` instead of the token, and `cell-registry.json` is written with mode `0600`.

Local WAL HA still requires `BETTERMQ_SHARED_META_DIR` (shared volume/NFS). Separate servers without shared FS → Slate + S3.

See [BACKUP.md](BACKUP.md) for backup/restore and host-breaker cold-start notes.

### Honesty

| Topic | Reality |
|-------|---------|
| Delivery | **At-least-once** — use message / idempotency keys in handlers |
| HA model | Fenced shard leaders + quorum/Slate + shared meta — **not** full Raft |
| MinIO single-node | Dev only; not object-store HA |
| Host breaker | Shared via `BETTERMQ_SHARED_META_DIR/host_breaker.json` when set; otherwise in-memory (lost on restart) |
| Fleet | Not in `cluster.nodes`; claims via internal lease API |

## Build from source

If you prefer to compile yourself (or no prebuilt binary exists for your platform):

```bash
git clone https://github.com/colinmollenhour/betterMQ.git
cd betterMQ
cargo build --release -p broker-server
./target/release/bettermq serve
```

Panel still writes `./data/bettermq.json` when using `--data-dir ./data`.

## Contributions

Pull requests are disabled. Coding agents make it too easy to send a large, low-context change that costs maintainers more time than it saves.
