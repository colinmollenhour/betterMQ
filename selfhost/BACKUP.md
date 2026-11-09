# Backup & restore (self-host)

Stop the broker before backup/restore. A live tar can split a WAL commit from
its RocksDB/checkpoint metadata and is not a supported disaster-recovery image.

## Local WAL / RocksDB

```bash
# Backup
install -d -m 700 "$HOME/bettermq-backups"
./backup.sh /var/lib/bettermq "$HOME/bettermq-backups/backup.tgz"
# Archives and checksum sidecars are private (0600); existing files are refused.

# Restore (parent directory that will contain the data folder)
./restore.sh "$HOME/bettermq-backups/backup.tgz" /var/lib
# Existing directory? Stop the broker and explicitly replace the whole tree:
./restore.sh "$HOME/bettermq-backups/backup.tgz" /var/lib --force
# Previous tree is retained under bettermq.pre-restore-*/data, never overlaid.
# then: bettermq serve --data-dir /var/lib/bettermq
```

## Multi-node local HA (`sharedMetaDir`)

Also copy the shared meta directory (leases, cursors, catalog JSON):

```bash
tar -czf shared-meta.tgz -C /cluster-shared meta
```

Restore that path on the volume every broker mounts before starting nodes.
The image initializes fresh volumes for UID/GID 1000. For an older root-owned
volume, stop **all** brokers, then repair ownership before restarting:

```bash
docker compose -f docker-compose.ha.yml run --rm --no-deps --user 0 \
  --entrypoint chown broker1 -R 1000:1000 /cluster-shared/meta
```

Do not change ownership while a broker is writing shared metadata. CI's image
smoke test verifies a fresh HA volume can be written by the runtime user.

## Slate + S3/MinIO

- Data dir backup still covers local cache + `bettermq.json` + auth.
- **Message durability is in object storage** — use your provider’s bucket versioning / snapshot (both `bucket` and `payloadBucket`).
- Single-node MinIO in compose is **not** HA; treat it as dev-only.
- Slate is an optional object-store profile, not the Core V2 fast ACK path.
  The low-latency profile acknowledges from the local/RF=3 WAL and archives
  sealed segments asynchronously.

## After restore

1. Run `bettermq doctor --data-dir /var/lib/bettermq --json`.
2. Confirm `BETTERMQ_CLUSTER_SECRET` / panel token still match.
3. For a standalone broker, start it and require `/readyz` (not only `/healthz`).
4. For a **cold cluster restore**, restore every member's own matching snapshot
   and shared/object-store state first, then start enough original members to
   form controller quorum (at least two voters in a three-voter cell). Start
   the remaining restored members promptly. Do **not** wait for `/readyz` on
   the first isolated member before starting its peers: readiness needs quorum.
   Use `/healthz` only to check startup progress, then require `/readyz` and
   healthy replica/ISR state on all members before resuming traffic. Do not
   replace missing voters with empty data directories or bootstrap a new cell.
5. Publish an idempotent canary, confirm delivery, then restore normal traffic.

This differs from a **rolling restart** of a live cell: there, maintain quorum
throughout and wait for the restarted member's `/readyz` before stopping the
next one. If original quorum cannot be recovered, keep traffic stopped and
follow the seed/quorum-loss escalation in [OPERATIONS.md](OPERATIONS.md).

## Restore drill gate

Before every release, restore the latest production-format backup into an
isolated environment, run `bettermq doctor`, verify WAL frame counts, publish
and deliver a canary, and record elapsed restore time. Quarterly drills should
also simulate loss of one broker and loss of the archive credentials.

## Point-in-time archive restore

Sealed-segment archive is asynchronous and off the `202` path. Optional
`--until-unix-ms` restores only manifests archived at or before that time.
See [ARCHIVE.md](../engine/broker-storage/ARCHIVE.md). This is extra storage,
not a substitute for the RF=3 WAL.

Images include `bettermq-archive` (with S3 support) and `bettermq-wal-migrate`.
GitHub releases also ship platform-specific ZIPs for each tool; download the
same version as the broker and verify its ZIP against `checksums.txt` before
extracting it into your private tools directory. The one-line broker installer
does not install the recovery tools. No Rust compiler is needed for recovery.
To run a tool in the image, override its broker entrypoint, for example:

```bash
docker run --rm --network none --entrypoint bettermq-wal-migrate \
  -v /srv/offline-copy:/recovery \
  "ghcr.io/colinmollenhour/bettermq:$BETTERMQ_VERSION" inspect /recovery/wal
```

Stop the broker before either restore or migration. Restore archives to a fresh
directory, not a live or previously populated WAL. A missing, malformed or
duplicate `--until-unix-ms` value is an error, never an unrestricted restore.
PITR filters archive timestamps, not individual message timestamps.

### Offline V1-to-V2 WAL migration

1. Stop the broker and take a checksummed backup of **all** state. Work on a
   copy; this tool migrates a WAL directory, not cluster identities, RocksDB
   metadata, auth or object storage.
2. Run `bettermq-wal-migrate inspect /srv/copy/v1-wal` and record its report.
3. Run `bettermq-wal-migrate migrate /srv/copy/v1-wal /srv/copy/v2-wal SHARD_ID`,
   replacing `SHARD_ID` with the actual numeric shard. Inspect the destination
   and verify frame counts and offsets before any cutover.
4. Before the V2 WAL takes writes, `bettermq-wal-migrate rollback /srv/copy/v2-wal`
   quarantines the migrated destination; use the untouched V1 source to roll
   back. The command refuses a changed source or V2
   destination; once new writes occur, restore a consistent offline backup of
   **all** stores instead. Never point an older broker at newer-format state.
5. Validate the complete recovered data layout with `bettermq doctor` and an
   isolated canary before resuming traffic. Do not rename WAL formats in place.

## Rolling migration

1. Take and checksum an offline backup. Keep the old binary and data directory.
2. Run the new binary's `bettermq doctor` against a copy of the data.
3. Upgrade one non-leader/dispatch node, wait for `/readyz`, then continue one
   node at a time. Do not upgrade without quorum headroom.
   For Compose, record the old `.env`, set `BETTERMQ_VERSION` to the tested
   target tag, and pull without recreating services. Then recreate **one**
   member with `docker compose -f docker-compose.ha.yml up -d --no-deps broker2`.
   Wait for its authenticated readiness and healthy ISR/replication metrics
   before repeating for the next member. Do not run project-wide `up -d`
   during a rolling upgrade; it may recreate all three members together.
4. Do not reinterpret or move an existing data directory between V1/V2
   layouts. Use a versioned migration command when one is released.
5. Roll back by stopping the new binary and restoring the untouched backup
   using `restore.sh backup.tgz /var/lib --force` (retain the moved-aside tree);
   never open a data directory written by a newer incompatible format with an
   older binary.
6. The cluster controller replicates individual commands rather than full
   state snapshots. Older binaries cannot read these log entries, so the
   first upgrade to this format cannot be rolled: stop every node, upgrade
   them all, then start them as described above. A node left on the older
   binary cannot replicate with the upgraded nodes.

## Host breaker note

When `BETTERMQ_SHARED_META_DIR` is set, the outbound host circuit breaker is persisted as `host_breaker.json` under that directory (shared across brokers). Without shared meta it is in-memory only and starts cold after restart.
