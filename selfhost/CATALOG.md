# Cell catalog consistency

Cell queue, flow, group, member, and cron definitions use the OpenRaft controller
as their authority. Local registries are serving caches. Single-node deployments
without a cluster keep their local-only behavior.

The [catalog mutation API reference](CATALOG_API.md) defines operation IDs,
revision/version headers, replay receipts, and retryable outcomes.

Catalog writes require controller quorum. A committed change reaches other
nodes within about one 100 ms apply interval; commit does not mean simultaneous
application everywhere. A node whose cache is dirty or behind the committed
revision stops admitting work and reports `catalog_ready: false` in `/readyz`.
It retries materialization rather than rolling back a committed definition.

The controller assigns revisions and checks identity versions, unique names,
and parent groups. Deleting a group tombstones its members in the same commit.
A deleted UUID cannot be revived; recreation uses a new UUID. Tombstones are not
garbage-collected. Cron definitions exclude execution progress, so replay does
not rewind a completed fire.

## Automatic upgrade activation

During a rolling upgrade, nodes advertise their catalog-writer capability on
the authenticated internal listener. Until every configured data node supports
authoritative writes, nodes keep unknown local items and apply committed
tombstones. No node assumes the old, incomplete controller catalog is complete.

Once every configured data node is reachable and capable, the leader commits a
write freeze, collects their catalogs, and commits one bootstrap union. Tombstones
win over live entries. Conflicting definitions are not resolved by wall clocks:
activation remains blocked with a diagnostic identifying the ambiguous identity.
Reconnect or upgrade missing nodes; reconcile ambiguous definitions against your
known-good configuration before retrying. Activation retries automatically.

After activation, legacy internal catalog push/snapshot writes return 409; their
read endpoints remain diagnostics. A genuinely unknown local item is adopted
create-only. Existing records, including tombstones, cannot be overwritten by
that adoption path. No routine manual migration is required.

Keep each node's catalog cache and `catalog-applied.json` checkpoint together in
its private data directory. The checkpoint advances atomically only after local
application succeeds. Failed local writes leave the node fenced until retry.

Legacy shared flow, subscription, group, and tombstone files are read-only
activation/adoption inputs in a cell; new nodes never keep writing them. A
standalone server with empty private registries imports those files once under
exclusive nonblocking locks, replays their journals, durably writes private
snapshots, and renames shared sources to `*.imported-<timestamp>`. An interrupted
import resumes from its private intent marker. If private and shared catalogs
both contain data, the server warns and uses the private catalog without merging
or renaming the shared files. Stop any legacy writer holding the import lock.

## Monitoring and costs

Broker Prometheus metrics include `bettermq_catalog_committed_revision`,
`bettermq_catalog_applied_revision`, `bettermq_catalog_apply_lag_revisions`, and
`bettermq_catalog_apply_errors_total`. Investigate a sustained apply lag or any
unready cache; inspect logs for activation or local-storage failures.

Each catalog mutation pays a controller commit and local materialization. The
controller stores its state as one JSON value, so write volume grows with catalog
size. Ingest and dispatch still read local caches and add no catalog RPC. Repeated
unchanged inline flows do not submit a catalog mutation.
Operation receipts are retained with controller state for durable retries; this
release does not garbage-collect them, so state also grows with mutation count.
