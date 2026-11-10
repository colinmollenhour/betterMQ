# Backlog inspection reference

`GET /v1/inspection/counts` returns maintained `delayed`, `crons`, and node-local
`dlq` counts without scanning message history. A DLQ count is exact only when
`dlq_ready` is true. During local WAL recovery, `dlq` is null and
`dlq_count_reason` describes initialization. On Slate, it is null with the reason
`count unavailable on this storage backend`; paginated listing still works.
These counts do not represent a cluster-wide aggregate.

| Endpoint | Pagination | Default payload behavior |
| --- | --- | --- |
| `GET /v1/delayed` | `limit` (default 10, clamped 1–1000), opaque `cursor` | Delivery deadline, id, queue, and routing key only |
| `GET /v1/dlq` | Select `queue` or `dlq_topic`; same page limit and opaque `cursor` | Metadata only; `include_body=true` opts in |
| `GET /v1/dlq/message` | Exact `dlq_topic`, `partition`, and `offset` | One message with its body |
| `GET /v1/dlq/sources` | Source inventory | Maintained counts, `count_ready`, and an unavailable reason instead of a misleading count |

Pass each `next_cursor` unchanged to the same endpoint and source. Stop only when
it is null. A DLQ page can be empty and still have a continuation: shared shards
can contain other topics. Each request scans at most 512 records and stops after
approximately 8 MiB of matching payloads; a single oversized record can exceed
that threshold. Pages can therefore contain fewer rows than the requested limit.
Delayed cursors order by delivery deadline and id; DLQ cursors traverse partition
and offset. The listings are live views, not immutable snapshots.

The panel Overview uses the count endpoint, not backlog bodies. Schedules and DLQ
tables load one bounded page at a time; DLQ bodies load only when **View** is
selected. Inspection scans, deletion, and retention run on a bounded blocking
worker pool, returning 503 when all inspection slots are busy.

Local WAL DLQ counters rebuild once when an existing shard opens, then update
with appends, replicated frames, and purges. This adds recovery work at startup
instead of repeating full history scans on dashboard loads. Counters are
ephemeral, not a new durable per-topic index.
