# Gateway-only mode

Gateway-only mode runs the public high-throughput ingest routes without opening
Broker, WAL, RocksDB, schedules, dispatch workers, or the archive service.

```bash
BETTERMQ_BROKER_URLS=http://broker-1:8080,http://broker-2:8080 \
BETTERMQ_GATEWAY_TOKEN=replace-me \
BETTERMQ_CLUSTER_SECRET=shared-internal-secret \
bettermq serve --gateway-only --listen 0.0.0.0:8080
```

Routing defaults to layout V2 with 256 physical shards. Override this only to
match an existing cell:

- `BETTERMQ_GATEWAY_LAYOUT_VERSION=1|2`
- `BETTERMQ_GATEWAY_SHARD_COUNT=<count>`
- `BETTERMQ_GATEWAY_TENANT=default` for self-hosted routing

The gateway keeps pooled connections and learns authoritative leader addresses
and terms from stale-leader responses. `/readyz` requires configured auth and
at least one healthy broker. `/v1/gateway/status` reports routing readiness.

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

- `POST /v1/gateway/enqueue`
- `POST /v1/ingest/batch`
- `POST /v1/ingest/ndjson`

NDJSON is parsed incrementally with decoded-byte and record limits. Compressed
NDJSON is rejected so limits cannot be bypassed through decompression.
