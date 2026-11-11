# Bounded dispatch and memory

Dispatch treats the write-ahead log (WAL) as the durable backlog. In-memory
queues and timers are bounded scheduling aids, not a second message store.
Saturation rejects an admission or wakeup; the committed message remains
available to backfill. Durability, quorum visibility, and completion identity
checks still apply before a delivery can advance its cursor.

## Separate admitted work from the durable backlog

One process-wide job budget covers ready work, hydration, flow waiting, and
active delivery. A slow flow also has a smaller look-ahead of
`max(16, 2 × parallelism)` admitted jobs. This prevents one blocked destination
from filling every stage independently.

Flow waitlists retain WAL references and ordering metadata, not payloads or
copies of the engine's clients and configuration. Workers reload the immutable
message before delivery. Warm reads await the shard I/O actor directly;
blocking preparation, payload persistence, and completion work run outside
async workers.

Inline delivery borrows the verified message rather than cloning its body
before building the outbound request. External-blob hydration runs on the
blocking pool and still verifies blob integrity before delivery.

Hot-path blocking I/O shares a process-wide concurrency budget of 32 operations
(`BETTERMQ_BLOCKING_IO_CONCURRENCY` overrides it). Callers wait asynchronously
before submitting to Tokio's blocking pool, rather than creating hundreds of
native threads that contend for the same storage locks. Running I/O keeps its
permit after caller cancellation. The cost is explicit queueing under storage
saturation; unrelated Tokio blocking work is not capped. Request tenant scope
is captured before crossing task/thread boundaries and restored on return.

Retry and dead-letter journal writes also use that offload path. Even clearing
a nonexistent retry must serialize behind an in-progress writer; it cannot
simply return early while the writer's new row is not visible yet. A no-op
cleanup stays synchronous only when it can take the writer lock without
waiting. Compaction copies cached metadata under the state lock, then encodes,
persists and rotates under the separate writer lock so cached readers do not
wait for its fsync. DLQ preparation, local append and dedup persistence remain
ordered around the asynchronous quorum durability step.

Backfill shares pending metadata through reference-counted entries. Its window
is bounded to 4,096 entries **per topic/partition**, not per process, so metadata
memory still depends on the number of active topics and partitions. Message
bodies do not enter this index. Metadata scans still read and verify payload
checksums; sealed V2 WAL reads temporarily decode an epoch body. This is not a
zero-I/O index.

Deferred wakeups have one bounded, deduplicated deadline queue. Live admission
and backfill honor those deadlines. Backfill also filters blocked hosts and
paused queues or group members before hydration. Completions release capacity
and wake backfill instead of waiting only for its periodic safety scan.

## Bounded admission must preserve ordering and recovery

A FIFO predecessor can be durable but absent from a full in-memory queue.
Checking only the flow waitlist would let later arrivals overtake it. The
pending scan therefore indexes FIFO heads, and delivery waits for earlier
durable work of the same lane. Group topics participate in backfill too, so
fan-out records remain recoverable after overflow or restart.

Live fan-out publishing and outbox replay share an execution registry. Failed
claims never create cleanup guards: dropping an eagerly constructed guard while
holding that registry's mutex would deadlock both publishing and replay.

Production construction installs lease and leadership controls before replay
or workers start. Replicated cells begin fenced until quorum recovery finishes.
Tracked child deliveries are cancelled and awaited during shutdown; RAII guards
release flow membership, counters, and permits even when a destination hangs.

## Completion cleanup must not grow with the completion gap

Original `dcomp` completion markers remain authoritative and readable by older
binaries. A derived `dcomp2` index uses fixed-width offsets so numeric cursor
cleanup can stop at its boundary instead of scanning every later completion.
Open repopulates this index from authoritative markers in bounded batches,
including changes made by an older binary.

On RocksDB and shared metadata, a completion and its gap-aware cursor use one
durable metadata batch. Slate retains its sequential compatibility behavior.
The cursor's owned lane lock stays with blocking I/O if its awaiting task is
cancelled. A scan failure after HTTP success still attempts to persist the
identity-aware completion marker for recovery.

## RSS is not an HTTP concurrency counter

The server uses mimalloc by default; reusable crates do not choose an
allocator. Unused heap pages can be reclaimed after bursts, while live work,
WAL indexes, RocksDB working sets, and retained allocator pages still contribute
to resident set size (RSS). A smaller queue does not imply an immediate return
to the exact startup RSS. The [build guide](README.md) includes the
system-allocator fallback for comparisons.

`bettermq_dispatch_in_flight_deliveries` preserves its historical meaning:
**owned jobs**, including ready and flow-waiting work. It can exceed a flow's
parallelism because of look-ahead. Use
`bettermq_dispatch_active_http_deliveries` and the benchmark sink's observed
concurrency checks when evaluating delivery parallelism.

The [benchmark suite](../tools/bettermq-bench/README.md) describes the measurement
protocol and environment requirements. Correctness gates reject a failing
candidate even when the baseline has the same failure. Drain throughput counts
completed work, and failed recovery records the full observed wait rather than
claiming an early recovery.
