# Sealed-segment archive

The archive worker is bounded and asynchronous from normal WAL commits. It
streams each sealed segment, retries failures, writes a SHA-256 manifest, and
blocks new ingest when configured lag limits are exceeded.

Each sealed segment has durable pending/completion markers beside the local
segment. Normal broker startup initializes the archive worker and streams every
unfinished segment back through the bounded queue. Failed jobs remain in the
worker and retry indefinitely with exponential backoff capped at 30 seconds.
After `BETTERMQ_ARCHIVE_MAX_RETRIES` consecutive attempts, the segment counts as
currently failing and ingest stays blocked until it archives successfully. Backend
recovery clears this failure automatically; a restart is not required. The one
worker retains a failed job ahead of later jobs, keeping memory bounded and
preserving backpressure during an outage.

Monitor `bettermq_archive_failing_segments` and `bettermq_archive_failing_bytes`
as gauges of outstanding failures. The older `bettermq_archive_failures_total`
and `bettermq_archive_failed_bytes_total` names remain deprecated gauge aliases;
they are not cumulative counters and decrease when recovery succeeds.

Ingest also blocks at 75% of queue capacity (including the in-flight job), or when
queued bytes or oldest outstanding age reach the configured limits below.

Local archive:

```bash
BETTERMQ_ARCHIVE_DIR=/srv/bettermq-archive bettermq serve
bettermq-archive scrub /srv/bettermq-archive
bettermq-archive restore /srv/bettermq-archive /srv/restored
```

S3, MinIO, or R2 archive:

```bash
export BETTERMQ_ARCHIVE_BUCKET=bettermq-segments
export R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export R2_ACCESS_KEY=...
export R2_SECRET_KEY=...
export BETTERMQ_ARCHIVE_PREFIX=production

bettermq-archive scrub-s3 production
bettermq-archive restore-s3 /srv/restored production
```

For AWS/MinIO, use `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and
`S3_REGION`. HTTP endpoints are permitted only when the configured endpoint
explicitly starts with `http://`.

Relevant limits:

- `BETTERMQ_ARCHIVE_QUEUE_CAPACITY` (default 128 segments)
- `BETTERMQ_ARCHIVE_MAX_RETRIES` (default 5; attempts before failure blocks ingest,
  not a limit on background recovery retries)
- `BETTERMQ_ARCHIVE_MAX_QUEUED_BYTES` (default 4 GiB)
- `BETTERMQ_ARCHIVE_MAX_LAG_MS` (default 5 minutes)

Point-in-time restore (optional extra storage; S3 is never on the ACK path):

```bash
# Restore only segments archived at or before this Unix millisecond.
bettermq-archive restore /srv/bettermq-archive /srv/restored --until-unix-ms 1710000000000
bettermq-archive restore-s3 /srv/restored production --until-unix-ms 1710000000000
```

Restore writes only paths validated as safe relative archive keys and verifies
both byte length and SHA-256 before replacing a destination file. Segments
newer than `--until-unix-ms` are skipped and counted in the report's `skipped`
field.
