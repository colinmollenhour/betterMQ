# How to recover and maintain an HA cell

Use this runbook for a three-replica (RF=3), minimum-two-in-sync-replica
(minISR=2) cell. Do not lower minISR to make an incident appear healthy.
Keep offline backups of each node's data, shared metadata/object storage,
deployment versions, node identities, and the original seed's configuration.
Use the transport protection in [SECURITY.md](SECURITY.md) across hosts.

## Inspect the cell before changing it

1. Stop unrelated deployments. Fence a failed host so it cannot return with
   stale state while a replacement uses its identity.
2. Call the **embedded** management listener of a surviving controller-capable
   broker, not the standalone panel. Set `ADMIN_URL` to its protected URL and
   `ADMIN_TOKEN` to a local admin credential. Never use the public listener if
   admin routes have been moved to a separate listener.
3. Record these snapshots:

   ```bash
   curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/admin/v1/cluster"
   curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/admin/v1/nodes"
   curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/admin/v1/shards"
   curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/admin/v1/metrics"
   ```

   Map `shards[].leaderId` to `nodes[].id` to locate shard leaders. There is
   no single broker that is a non-leader for every shard. `controllerVoter`
   identifies metadata voters, not shard leadership.
4. Check each broker's `/readyz`; a 200 must accompany healthy replication,
   not replace it. `/admin/v1/readyz` returns JSON: inspect `ready` and
   `cluster_healthy`, not only its HTTP status. Authenticated `/metrics` uses
   `BETTERMQ_METRICS_TOKEN`; watch `bettermq_controller_quorum_ready = 1`,
   `bettermq_replication_isr_members` (3 before planned maintenance, at least
   2 during a single failure), `bettermq_replication_max_lag_records`, and
   increases in `bettermq_replication_quorum_failures_total`. Idle shards may
   have no recent request telemetry; compare controller placements and send
   an idempotent canary rather than interpreting absent metrics as success.

## Recover one unavailable node

1. If two original members remain healthy, keep both online. Do not restart
   another member: the cell has no remaining failure headroom.
2. Repair the failed member with its **own** intact data and original identity,
   address, cluster secret, and pinned binary version. Never copy a different
   member's directory or reuse an identity while the old process is alive.
3. Start that member. Wait for `/readyz`, then verify all shard placements have
   three replicas in ISR, lag converges, and the canary publishes and delivers.
4. If its data is missing or corrupt, retain the failed disk and use the
   replacement procedure below. If fewer than controller quorum can return,
   stop traffic and use the cold recovery procedure in [BACKUP.md](BACKUP.md).

## Replace a permanently failed non-seed node

1. Fence the failed node and record its node UUID and name. Verify the surviving
   controller quorum and durable source replicas before changing membership.
2. Provision a new broker with a **new** node name and empty private data volume.
   Join it through **Infrastructure** on the seed/panel (the enrollment endpoints
   include `GET /v1/infra/join/bootstrap` and `POST /v1/infra/cluster/join`). Keep the
   join token and peer transport private. Do not bootstrap a second cluster.
3. Repair replica placement before deleting the old member. Use
   `POST /admin/v1/rebalance` with `{"shard":0,"add":"NEW_NODE_UUID","remove":"OLD_NODE_UUID"}`
   for each affected shard. Replace shard 0 and UUID placeholders with values
   from the snapshots. A successful response records a rebalance request; it
   is **not** proof that data was copied or a learner was promoted. Wait for
   actual copy/catch-up and finalized placement/ISR before proceeding. If your
   release cannot complete that transition safely, stop here: recover the
   original member from a matching backup or escalate. Never hand-edit ISR,
   promote an empty replica, or remove the only copy of an acknowledged record.
4. Drain the old node with `POST /admin/v1/nodes/drain` and
   `{"nodeId":"OLD_NODE_UUID"}`. Reinspect every shard. A `409` means stop and
   resolve the reported placement/quorum restriction, not bypass it.
5. If the old node is a controller voter, complete the voter procedure below
   before deleting its infrastructure membership.
6. On the **original seed**, call `POST /v1/infra/cluster/remove-node` with
   `{"node_name":"OLD_NODE_NAME"}` using the seed's local admin credential.
   Inspect `removed`, `needs_restart`, and propagation warnings. Restart only
   where required, one member at a time with readiness/ISR checks. This endpoint
   also drains placements; it is not a substitute for verifying catch-up.
7. Require RF=3, minISR=2, three in-sync durable replicas, stable quorum metrics,
   and a successful publish/delivery canary before declaring recovery complete.

## Drain a live node for planned maintenance

1. Require three healthy durable replicas and controller quorum first.
2. For a permanent evacuation, add replacement capacity before removing any
   replica. Submit and complete the per-shard rebalances above, then call
   `/admin/v1/nodes/drain`. Drain transfers leadership/removes placements; it
   does not magically provision replacement copies.
3. For a temporary rolling binary restart, preserve the member's identity and
   volume, restart only that member with the configured shutdown grace, and
   wait for readiness **and** ISR catch-up before touching another member.
   Follow [rolling migration](BACKUP.md#rolling-migration).

## Expand replication or move shard placement

1. Enroll data brokers before calling `POST /admin/v1/expand-rf` with
   `{"nodeName":"NEW_NODE_NAME","advertiseUrl":"PRIVATE_PEER_URL"}`. This is
   the RF=1-to-RF=3 expansion path, not a request for unbounded replication or a
   generic node-replacement shortcut. RF remains fixed at 3 for larger cells.
2. Inspect the response's `replicationFactor`, `minIsr`, `haReady`, `actions`,
   and warnings. Require actual historical-data catch-up and shard ISR; neither
   `ok: true` nor readiness alone establishes that a new replica is durable.
3. For an existing RF=3 cell, move placements with `/admin/v1/rebalance` as
   above. Do not run garbage collection (`/admin/v1/gc/{shard}`) to clear a
   stuck migration: retention watermarks are not a repair mechanism.

## Change controller voters

1. Keep the old quorum online and back up controller state. Enroll/catch up the
   replacement before changing voting membership.
2. Send `POST /admin/v1/controller/voters` with
   `{"nodeId":"NEW_NODE_UUID","action":"add"}`. Inspect `controllerVoters`
   and wait for a valid quorum lease with the expanded membership.
3. Remove the old voter with the same endpoint and
   `{"nodeId":"OLD_NODE_UUID","action":"remove"}` only after the new voter
   is durably participating. Recheck quorum and readiness after **each** change.
   Adding a voter can increase the quorum requirement; do not do it when the
   old quorum is already unavailable. Data-replica and voter changes are separate.

## Recover a lost seed or lost controller quorum

1. Fence the old seed host. The infrastructure API only lets the seed remove
   other members, and forbids removing the seed itself. There is **no supported
   online seed-reassignment API**. Do not reset a survivor or reorder membership
   files to impersonate the seed during an incident.
2. If the seed's own durable state is intact, restore its original identity,
   configuration, endpoint, and data on a replacement host while the old one
   remains fenced. Rejoin the existing cell, not a newly initialized cell.
3. If original state/quorum cannot be recovered, stop publishers and dispatch,
   preserve all surviving disks and logs, and escalate for a reviewed offline
   recovery using consistent backups of all stores. Follow the cold-start
   quorum sequence in [BACKUP.md](BACKUP.md). No generic command here can
   reconstruct acknowledged records whose last durable copies were lost.
4. Keep traffic stopped until original membership, controller quorum, shard
   durability, and the publish/delivery canary have been verified. Record the
   restore point and any recovery-point loss explicitly.
