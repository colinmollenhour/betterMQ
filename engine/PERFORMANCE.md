# Bounded dispatch and memory

Dispatch treats the write-ahead log (WAL) as the durable backlog. In-memory
queues and timers are bounded scheduling aids, not a second message store.
Saturation rejects an admission or wakeup; the committed message remains
available to backfill. Durability, quorum visibility, and completion identity
checks still apply before a delivery can advance its cursor.

## Separate admitted work from the durable backlog

One process-wide job budget covers ready work, hydration, flow waiting, and
active delivery. A parallel flow also has a smaller look-ahead of
`max(16, 2 × parallelism)` admitted jobs. Backfill reserves that flow quota
synchronously before queue submission, not after asynchronous hydration. Live
work takes the same quota once its immutable flow metadata is known. The owned
permit spans preparation, flow waiting, and delivery, and is released before
notifying backfill of available capacity.

This uses a small atomic gate per live flow, with a weak registry that releases
historical keys after lane eviction. Operator pins update the same gate. Already
admitted work is not revoked by a lower limit, but new admission waits for its
quota. An offset replacement with different flow metadata cannot reuse the old
flow's permit. Per-pass counter estimates were rejected: they would still race
other backfill passes and live work. Merely observing the async lane's pending
count allowed a whole ready window to enter before hydration ran, producing
repeated native reads and millions of rejections under overload.

Backfill offers one record per scanned log per turn when filling those budgets.
Allowing the first partition to take a whole shared flow window would serialize
its completions behind one cursor and defeat cross-partition metadata group
commit. Round-robin admission keeps the cursor owners progressing together. A
lone active log can still fill the whole window; there is no permanent
per-partition quota or timer-limited scheduling quantum.

The cost is temporary vectors of shared metadata references for the logs scanned
in a pass, not another metadata or payload copy. The nominal two-second pass
budget is split between scanning and admission. Admission gets its own budget
after the reads so one slow storage call cannot repeatedly consume the pass
without scheduling the work it discovered. A single storage call can still
overrun the scan budget; later passes rotate the starting log.

Flow waitlists retain WAL references and ordering metadata, not payloads or
copies of the engine's clients and configuration. Workers reload the immutable
message before delivery. Warm reads await the shard I/O actor directly;
blocking preparation, payload persistence, and completion work run outside
async workers.

Inline delivery borrows the verified message rather than cloning its body
before building the outbound request. External-blob hydration runs on the
blocking pool and still verifies blob integrity before delivery.

Blocking preparation, reads, and journal I/O share a process-wide budget of 32 operations
(`BETTERMQ_BLOCKING_IO_CONCURRENCY` overrides it). Callers wait asynchronously
before submitting to Tokio's blocking pool, rather than creating hundreds of
native threads that contend for the same storage locks. Running I/O keeps its
permit after caller cancellation. The cost is explicit queueing under storage
saturation; unrelated Tokio blocking work is not capped. Request tenant scope
is captured before crossing task/thread boundaries and restored on return.

Asynchronous keyed publishes use the shared WAL commit coordinator rather than
forcing a private flush. An admitted append transfers its response and dedup
reservation directly into an owned completion task. The task waits for local
or quorum durability outside the blocking-I/O budget, then persists dedup.
Cancelling the request cannot release an already-appended key prematurely.
Synchronous publishing and Slate keep their existing durability behavior.

Local RocksDB metadata has one dedicated writer and a 256-request queue.
Asynchronous dedup and completion calls await that queue directly, without
occupying blocking-I/O permits. Ready caller batches share a synchronous,
atomic RocksDB write, bounded to 64 requests, 4,096 operations, and roughly
4 MiB of estimated metadata per group. A larger existing atomic caller batch
runs alone rather than being split. This costs one native thread per local
store and explicit queueing under saturation; it does not relax fsync.

Dedup reservations and cursor-lane guards travel with queued writes and their
receipts, surviving caller cancellation through persistence and bookkeeping.
Expiry deletes, explicit rewinds, marker cleanup, and checkpoint barriers use
the same writer ordering. Uncertain RocksDB failures fence new writes and
metadata reads until recovery on reopen. Shared-file and Slate metadata retain
their synchronous compatibility paths. The synchronous local API uses channel
primitives without requiring an active Tokio runtime.

Retry and dead-letter journal writes also use that offload path. Even clearing
a nonexistent retry must serialize behind an in-progress writer; it cannot
simply return early while the writer's new row is not visible yet. A no-op
cleanup stays synchronous only when it can take the writer lock without
waiting. Compaction copies cached metadata under the state lock, then encodes,
persists and rotates under the separate writer lock so cached readers do not
wait for its fsync. DLQ preparation, local append and dedup persistence remain
ordered around the asynchronous quorum durability step. An owned task is
registered before native DLQ preparation can append. Cancellation cannot drop
the append's reservation between native I/O and asynchronous completion. A
locally durable identity survives a failed quorum attempt without making the
source message complete before quorum visibility.
Fleet ACK cleanup uses the same offload path and takes the writer lock before
selecting retries by offset, so an in-progress append cannot escape cleanup.

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

Strict-FIFO backfill admits only the durable head of each key. Live arrivals
check the same index before entering a flow waitlist. Successors remain in the
WAL rather than repeatedly consuming queue capacity, native reads, and deferred
timers while their predecessor runs. Once completion metadata is durable,
removing the old head hands the next indexed head directly to ordinary bounded
admission. This avoids imposing the backfill loop's 25 ms notification debounce
on every serial delivery. A full admission budget or an unindexed successor
still falls back to recoverable backfill; no payloads or unbounded successor
queues are added.

Live fan-out publishing and outbox replay share an execution registry. Failed
claims never create cleanup guards: dropping an eagerly constructed guard while
holding that registry's mutex would deadlock both publishing and replay.

Fan-out result cohorts use one bounded native journal writer. Up to eight
sibling publishes run concurrently; their outcomes and the authoritative
command update share a durable journal batch. The writer clones only touched
commands, not the whole outbox. It retains admitted work after cancellation,
fences uncertain writes, and replays a torn final journal entry conservatively.

Production construction installs lease and leadership controls before replay
or workers start. Replicated cells begin fenced until quorum recovery finishes.
Tracked child deliveries are cancelled and awaited during shutdown; RAII guards
release flow membership, counters, and permits even when a destination hangs.

## Recovery proofs belong to one WAL incarnation

Recovery distinguishes catching up, verifying, and serving. A recovery token
binds dispatch targets, producer-index writes, and visibility waits to their
original incarnation, including when the same leader recovers in the same
term. Quorum watermark updates serialize their authority check against local
tail rewrites. A later recovery cannot reauthorize an earlier queued result.

Follower repair invalidates old progress before probing or cutting a tail.
ACKs and repair completion carry a session revision, and only sent, verified
ranges advance progress. A large reported follower log end is not proof of a
matching prefix. Repair compares every retained comparable UUID in bounded
pages; a matching final record cannot conceal a divergent middle record. This
costs a retained-prefix scan on repair, not on normal publishing. Frame export
loads one source body at a time before applying its byte cap.

Follower ACK validation and the actual truncate transaction share one fence
lock. An ACK after a cut must still validate its original append incarnation;
an ACK before the cut protects that prefix within its leader term. A failed
fence fsync cannot make the next attempt skip persistence.

Local GC pins pending producer-index work and rebuilds identities from raw
retained headers, including logically purged records. An abandoned append
dirties that coverage even when its metadata store remains healthy. A durable,
non-expiring checkpoint in the same index precedes physical payload deletion.
Missing coverage for an already-collected prefix keeps recovery and GC fenced.
The existing all-configured-replica payload floor remains in force.

Tail cuts use a WAL-first durable transaction with a bounded identity inventory.
Cleanup conditionally removes only the captured log/offset/UUID identity.
Startup aborts an untouched prepared cut or completes cleanup for a durable
cut before offset reuse; it never reapplies a completed cut to replacement data.

With shared delivery metadata, replicated local WALs keep replica producer
indexes and GC checkpoints in each node's `replica-identities` directory.
Follower cuts do not invalidate another replica's legacy shared producer rows.
Read-only migration accepts only rows matching the retained raw WAL's tenant,
log, offset, and UUID, preserving expiry. Stripped legacy headers can recover
their key before migration through a lazily built UUID lookup, maintained on
later mutations and checked against the authoritative TTL row. An optional
header trailer preserves the original producer timestamp without changing the
historical bincode prefix or consumer JSON.

Shared cells also retain tenant-wide retry lookup through a separate committed
producer directory. Publishers acquire fixed, versioned file-lock stripes on
the actual WAL leader before local key reservations. Batch admission acquires
sorted unique stripes once, including collisions between different keys. The
guard survives native appends and directory persistence. Forwarding hops,
follower replication, and shard recovery never acquire those locks.

Only identities already visible on a contiguous quorum prefix enter the shared
directory, and its fsync precedes the response. A batch shares one directory
epoch. A retry can therefore return the original identity on a node outside
the source replica set. Legacy global rows require read-only verification by
their canonical shard leader before adoption. Local trims never delete either
form of global evidence. Directory hits confirm journal durability themselves,
including when another process's previous fsync had an uncertain outcome.

The cost is an additional shared durability epoch for keyed publishes and
bounded cross-key serialization on 4,096 admission stripes. Lock inodes are
never removed or replaced while the cell runs. Standalone RocksDB, standalone
shared metadata, and Slate do not use this coordination path. This is not a
pre-append transaction journal: an interrupted publish without a successful
acknowledgement retains the existing at-least-once, unknown-outcome semantics.
The committed directory preserves the original expiry rather than renewing it
on retries, recovery, or legacy adoption.

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

The server retains the system allocator. Three interleaved burst/idle trials
per allocator favored its lower absolute resident memory over mimalloc's lower
retained-growth percentage; the [allocator experiment](../tools/bettermq-bench/baselines/README.md#allocator-experiment)
records the comparison and its shared-host limitations. Live work, WAL indexes,
RocksDB working sets, and retained allocator pages still contribute to resident
set size (RSS). A smaller queue does not imply an immediate return to the exact
startup RSS.

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
