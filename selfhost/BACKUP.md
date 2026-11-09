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

## Host breaker note

When `BETTERMQ_SHARED_META_DIR` is set, the outbound host circuit breaker is persisted as `host_breaker.json` under that directory (shared across brokers). Without shared meta it is in-memory only and starts cold after restart.
