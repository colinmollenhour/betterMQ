# Gateway-only mode

Gateway-only mode runs the public high-throughput ingest routes without opening
Broker, WAL, RocksDB, schedules, dispatch workers, or the archive service.

```bash
BETTERMQ_BROKER_URLS=http://broker-1:8080,http://broker-2:8080 \
BETTERMQ_GATEWAY_TOKEN=replace-me \
BETTERMQ_CLUSTER_SECRET=shared-internal-secret \
bettermq serve --gateway-only --listen 0.0.0.0:8080
```

`BETTERMQ_BROKER_URLS` must use the broker's **internal** listener addresses.
For split listeners (for example, public `:8080` and `--internal-listen :8082`),
set `BETTERMQ_BROKER_URLS=http://broker-1:8082,http://broker-2:8082`. A collapsed
listener serves both operations at `:8080`. Both probes and forwarding authenticate
with `BETTERMQ_CLUSTER_SECRET`; `/internal/v1/readyz` is available on the internal
listener even though public `/healthz` is not. Do not expose internal listeners
publicly. A credential rejection returns `cluster_auth_failed`; pointing at the
public listener returns `internal_route_not_found`, not a JSON decode error.

Routing defaults to layout V1 with four shards, matching new default broker
cells. Match the cell's persisted `shard-layout.json`, not its initial environment:

- `BETTERMQ_GATEWAY_LAYOUT_VERSION=1|2`
- `BETTERMQ_GATEWAY_SHARD_COUNT=<count>`
- `BETTERMQ_GATEWAY_TENANT=default` for self-hosted routing

For a fresh V2 cell, initialize **every broker** with `BETTERMQ_SHARD_LAYOUT=v2`
(256 shards by default), then run the gateway with matching settings:

```bash
BETTERMQ_BROKER_URLS=http://broker-1:8080,http://broker-2:8080 \
BETTERMQ_GATEWAY_LAYOUT_VERSION=2 BETTERMQ_GATEWAY_SHARD_COUNT=256 \
BETTERMQ_GATEWAY_TOKEN=replace-me BETTERMQ_CLUSTER_SECRET=shared-internal-secret \
bettermq serve --gateway-only --listen 0.0.0.0:8080
```

For existing V1 cells with a nondefault partition count, set
`BETTERMQ_GATEWAY_LAYOUT_VERSION=1` and `BETTERMQ_GATEWAY_SHARD_COUNT` to that
persisted count. Invalid counts or unsupported versions stop gateway startup.

The gateway keeps pooled connections and learns authoritative leader addresses
and expires learned routes after 30 seconds. It refreshes the leader map from
authenticated seeds every 30 seconds. On a transport failure it evicts the failed
route and retries through another seed, allowing one redirect to the new leader
(at most three attempts). All attempts retain the same routing and idempotency
identities; requests without an idempotency key receive a gateway-local key.
The gateway learns generations
and terms from stale-leader responses. `/readyz` requires configured auth and
at least one ready broker with matching shard count, layout version, and tenant.
Any responding seed with a mismatched layout fails gateway readiness, even if
another seed matches. `/v1/gateway/status` includes routing mismatch and failed
probe counts. Unreachable, unready, or unauthenticated seeds are not healthy.

`GET /metrics/prometheus` exports forwarding attempts, failures, and total/count
latency by shard and HTTP status (`0` means a transport failure). It uses the same
`BETTERMQ_METRICS_TOKEN` protection as broker metrics. Forwarding failures emit
warnings limited to one per shard/status every 30 seconds.

Supported ingest routes:

Keyless requests receive a routing-only UUID before shard selection. Forwarding
preserves that identity without enabling FIFO delivery or changing the message key.
Routing uses the canonical storage topic: direct URLs use the direct topic and
queue IDs resolve to their queue name. A gateway batch containing queue IDs makes
one authenticated metadata lookup (IDs/names only, no destination secrets) before
splitting by shard. This extra round trip avoids routing V1 queue-ID requests by
an empty or caller-supplied topic.
Batch failures include a stable `code`, a `retryable` flag and, for back-pressure,
`retry_after_ms`. Retry only retryable items, with their original idempotency
keys. Broker forwarding preserves leader rejection status and `Retry-After`;
transport failures return 503 and may have an unknown outcome.

Repeated inline-flow publishes with unchanged limits reuse the existing profile
without a catalog consensus write or peer fan-out. Actual flow changes submit
controller metadata before asynchronous peer propagation.

- `POST /v1/gateway/enqueue`
- `POST /v1/ingest/batch`
- `POST /v1/ingest/ndjson`

NDJSON is parsed incrementally with decoded-byte and record limits. Compressed
NDJSON is rejected so limits cannot be bypassed through decompression.
