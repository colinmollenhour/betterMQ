# Production launch and soak gates

These are release gates, not current benchmark claims.

CI runs the real three-process replicated HTTP gate with:

```bash
cargo test -p broker-server --test replicated_http -- --ignored --nocapture
```

It checks keyed and keyless single/batch ingest, idempotent retries, acknowledged
IDs in recovered replica WALs after leader SIGKILL, failover, restart, and refusal
without quorum. This correctness check does not replace the hardware soak below.

## Before launch

- `bettermq doctor --data-dir <data-dir>` passes on every broker.
- Every container reports `/readyz`; `/healthz` alone is only liveness.
- During shutdown drain, `/readyz` returns 503 with `draining: true` before
  listener teardown; `/healthz` stays a liveness signal while HTTP is available.
- A restore drill from a checksummed backup completes within the documented RTO.
- Capacity tests use production body sizes, batch sizes, durability, TLS, and
  destination latency. Keep at least 30% disk, memory, and ingest headroom.
- Slate remains outside the low-latency ACK profile. Core V2 ACKs from the
  local/RF=3 WAL; sealed segment archive is asynchronous.

## Performance gates

Progressively require:

1. at least 10k messages/s on one local-WAL node;
2. at least 50k messages/s per broker with bounded batches;
3. at least 100k messages/s per RF=3 cell, p50 at most 5ms and p99 at most
   20ms, with zero loss of acknowledged records after any one-node failure.

`202` is minISR=2 (two durable copies). Do not wait for three fsyncs.
Record hardware, filesystem, fsync mode, payload distribution, batch size, and
open-loop offered load with every result. A saturated closed-loop benchmark is
not a launch result.

### Shared metadata qualification

The local-WAL HA profile shares dedup/cursor metadata on one filesystem. Its
incremental journal avoids whole-history rewrites, but every metadata batch
still serializes on a cell-wide lock and shared-storage fsync. The volume is a
single availability and metadata failure domain; RF=3 message replication does
not replicate this metadata. Network filesystem latency directly affects ingest
p99. **Gate 3 remains unverified for shared-meta HA.**

The ignored `shared_indexes::tests::metadata_microbenchmark` in `broker-storage`
measures local metadata append batches only, with no HTTP, RF=3 quorum, dispatch,
TLS, or cross-host filesystem contention. It is a diagnostic, not a launch
result. Run the mixed ingest/dispatch load generator in `tools/bettermq-loadgen`
against the actual supported shared filesystem and record the above settings
before claiming any gate.

Local diagnostic measured on 2026-11-10: Linux x86-64, Intel Core Ultra 9 285H,
ext4 on an LVM volume, debug test build, one writer, 500 transactions per case,
unique tenant-wide keys, one journal `fdatasync` per batch, no HTTP/TLS, message
WAL, replication, or dispatch. The development host also ran other builds.

| Records per batch | Records | Measured records/s | Batch p99 |
|---|---:|---:|---:|
| 1 | 500 | 21 | 50.809 ms |
| 64 | 32,000 | 1,804 | 64.926 ms |

These numbers do **not** meet or waive the RF=3 gate. They demonstrate why
shared-storage latency and batch size require deployment-specific qualification.
To repeat the metadata-only diagnostic, point `BETTERMQ_TEST_BENCH_DIR` at an
existing directory on the filesystem being tested (not tmpfs), then run:

```bash
cargo test -p broker-storage --features slate metadata_microbenchmark -- --ignored --nocapture
```

## Soak and fault gates

- Pull requests that change durability, admission, replication, archive, or
  shutdown run a 24-hour soak on dedicated Linux/NVMe hardware.
- Release candidates run 72 hours with rolling restarts, SIGTERM during load,
  SIGKILL, disk-full/fsync failure, slow archive, slow replica, and destination
  outage injection.
- Pass only with no acknowledged-message loss, bounded memory/queued bytes,
  bounded archive lag, no readiness false positives, and stable p99 latency.
- Save metrics, load-generator settings, logs, and a redacted
  `bettermq support-bundle` with the release evidence.

## WAL corruption and fenced shards

Recovery never discards records it cannot prove were unacknowledged:

- Only an incomplete or zero-filled record at the very end of `active.wal`,
  above the last fsynced offset, is a torn tail and is truncated on open.
- Any other decode failure, a short sealed segment, a missing segment, or fewer
  records than were last fsynced (`durable_offset` in `meta.json`) quarantines
  the shard. The broker writes `quarantine.json` in the shard directory and
  leaves the WAL files untouched. The shard refuses to open, through lazy
  reopens and restarts, until an operator acts.
- To recover, restore the named file from a replica or archive, or knowingly
  truncate it at the reported offset. Then delete `quarantine.json`. A file
  named `active.corrupt.<ts>` (left by older versions) also blocks open; move
  it out of the shard directory once its records are restored or written off.
- `bettermq_wal_quarantine_events_total` counts detections and refused opens.
  `bettermq_wal_fence_events_total` counts shards that stopped accepting writes
  after a WAL fsync failed or a failed WAL write could not be rolled back. A
  retried fsync can succeed after Linux has already dropped the pages it could
  not write, so a fenced shard never acknowledges again in that process: its
  appends and commits fail, and it serves only records fsynced before the
  failure. It recovers when the broker restarts and the log is rebuilt from
  disk. Alert on any increase of either counter.
