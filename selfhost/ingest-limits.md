# Ingest request limits

The default HTTP request body limit is **8 MiB**. Override it with
`BETTERMQ_MAX_HTTP_BODY_BYTES`; batch ingestion still has its own 8 MiB cap.

Authenticated publish, enqueue (including queue-specific enqueue), batch and
gateway requests reserve their `Content-Length` against the process/tenant
in-flight record and byte budgets **before reading their body**. Requests without
a length reserve the full route body limit. Excess requests receive 429 (tenant)
or 503 (process) rather than waiting in an unbounded upload queue.

The reservation remains held through parsing and processing; canonical message
admission separately accounts for decoded records and fan-out expansion. This is
conservative: effective producer concurrency can be lower than the configured
record limit, especially for lengthless uploads and large batches.

`BETTERMQ_INGEST_BODY_TIMEOUT_MS` limits the entire body read, including slow or
stalled streams (default 30,000 ms; maximum 120,000 ms). A timeout returns 408 and
releases the reservation. Bodies exceeding their reserved size return 413.
The existing `BETTERMQ_INGEST_MAX_IN_FLIGHT`, `BETTERMQ_INGEST_MAX_QUEUED_BYTES`,
`BETTERMQ_INGEST_MAX_TENANT_RECORDS` and `BETTERMQ_INGEST_MAX_TENANT_BYTES` settings
bound concurrent uploads as well as canonical ingestion.

Single-message failures increment `bettermq_ingest_rejected_total` exactly once
at the canonical single-record boundary, including validation, admission and
replication errors. `bettermq_ingest_single_rejected_total{reason="..."}` provides
a bounded reason breakdown without request identifiers or free-form messages.

Batch 207 responses preserve each input's zero-based `index`. Failed items carry
a stable `code`, `retryable`, human-readable `error`, and optionally
`retry_after_ms`. Retry only retryable failures, retaining idempotency keys:
commit or replication failures can have an ambiguous durable outcome. Invalid
input is not retryable; availability, storage, quorum and saturation failures are.
