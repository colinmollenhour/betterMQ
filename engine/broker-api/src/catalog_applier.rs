//! Committed controller catalog -> local serving cache. No peer clock is authority.
use crate::{routes::ApiError, AppState};
use broker_raft_meta::catalog::{catalog_identity, CatalogKind, CatalogMutation};
use broker_raft_meta::CatalogRecord;
use broker_raft_meta::{ControllerCommand, ControllerState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use uuid::Uuid;

/// Enabled by H13 only after every write handler follows commit-before-apply.
pub(crate) const WRITER_CAPABLE: bool = false;

pub(crate) fn preflight(state: &AppState) -> Result<(), ApiError> {
    if let Some(cluster) = &state.cluster {
        let controller = cluster.runtime.controller().ok_or_else(|| {
            unavailable(
                "cell catalog mutations require a controller; no-controller Ok(0) is not a commit",
            )
        })?;
        if !controller.is_ready() || controller.catalog_writes_frozen() {
            return Err(unavailable(
                "controller quorum unavailable or catalog activation in progress",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn mutation_preflight(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, ApiError> {
    let path = request.uri().path();
    if request.method() != axum::http::Method::GET
        && (["/v1/flows", "/v1/groups", "/v1/crons", "/v1/subscriptions"]
            .iter()
            .any(|prefix| path.starts_with(prefix))
            || (path.starts_with("/v1/queues") && !path.ends_with("/enqueue"))
            || (request.method() == axum::http::Method::PUT && path.starts_with("/v1/flow/")))
    {
        preflight(&state)?;
    }
    Ok(next.run(request).await)
}

pub(crate) struct ApplyState {
    lock: parking_lot::Mutex<Option<u64>>,
    applied: AtomicU64,
    dirty: AtomicBool,
    migration_pending: AtomicBool,
    adoption_pending: AtomicBool,
    pub errors: AtomicU64,
    started: AtomicBool,
    pub diagnostic: parking_lot::Mutex<Option<String>>,
}

impl Default for ApplyState {
    fn default() -> Self {
        Self {
            lock: Default::default(),
            applied: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            migration_pending: AtomicBool::new(false),
            adoption_pending: AtomicBool::new(false),
            errors: AtomicU64::new(0),
            started: AtomicBool::new(false),
            diagnostic: Default::default(),
        }
    }
}

impl ApplyState {
    pub fn ready(&self, committed: u64) -> bool {
        !self.dirty.load(Ordering::Acquire)
            && !self.migration_pending.load(Ordering::Acquire)
            && !self.adoption_pending.load(Ordering::Acquire)
            && self.applied.load(Ordering::Acquire) >= committed
    }
    pub fn revision(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    applied_revision: u64,
}

pub(crate) fn record<T: Serialize>(
    tenant: String,
    kind: CatalogKind,
    id: Uuid,
    payload: &T,
    expected_version: Option<u64>,
    tombstone: bool,
) -> Result<CatalogMutation, ApiError> {
    let mut payload = serde_json::to_value(payload).map_err(unavailable)?;
    // Execution state is deliberately absent from committed cron definitions.
    if kind == CatalogKind::Cron {
        if let Some(object) = payload.as_object_mut() {
            object.remove("next_run_at_ms");
            object.remove("last_run_at_ms");
        }
    }
    Ok(CatalogMutation {
        tenant_id: tenant,
        kind,
        id,
        payload_json: payload.to_string(),
        tombstone,
        expected_version,
    })
}

pub(crate) fn unavailable(error: impl std::fmt::Display) -> ApiError {
    ApiError::Unavailable(format!("catalog unavailable: {error}"))
}

pub(crate) fn controller_error(error: broker_raft_meta::ControllerError) -> ApiError {
    match error {
        broker_raft_meta::ControllerError::CatalogConflict(message) => ApiError::Conflict(message),
        other => unavailable(other),
    }
}

fn apply_record(state: &AppState, record: &CatalogRecord) -> Result<(), ApiError> {
    let id = Uuid::parse_str(&record.key).map_err(unavailable)?;
    let tenant = if record.tenant_id.is_empty() {
        "*"
    } else {
        &record.tenant_id
    };
    macro_rules! value {
        () => {
            if record.tombstone {
                None
            } else {
                Some(serde_json::from_str(&record.payload_json).map_err(unavailable)?)
            }
        };
    }
    match record.kind.as_str() {
        "flow" => state
            .broker
            .apply_flow_authoritative(tenant, id, value!())?,
        "queue" => state
            .broker
            .apply_queue_authoritative(tenant, id, value!())?,
        "group" => state
            .broker
            .apply_group_authoritative(tenant, id, value!())?,
        "group_member" => state
            .broker
            .apply_member_authoritative(tenant, id, value!())?,
        "cron" => state
            .crons
            .apply_authoritative(id, value!())
            .map_err(unavailable)?,
        other => {
            return Err(unavailable(format!(
                "unknown committed catalog kind {other}"
            )))
        }
    }
    if record.tombstone {
        state
            .catalog_tombstones
            .merge_remote(&[(id, 0)].into_iter().collect())
            .map_err(unavailable)?;
    }
    Ok(())
}

pub(crate) fn materialize(state: &AppState) -> Result<u64, ApiError> {
    let Some(cluster) = &state.cluster else {
        return Ok(0);
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Ok(0);
    };
    let committed = controller.state();
    let apply = &cluster.catalog_apply;
    let mut checkpoint = apply.lock.lock();
    let path = state.broker.config().data_dir.join("catalog-applied.json");
    let result = (|| {
        let previous = match *checkpoint {
            Some(revision) => revision,
            None => match std::fs::read(&path) {
                Ok(bytes) => {
                    serde_json::from_slice::<Checkpoint>(&bytes)
                        .map_err(unavailable)?
                        .applied_revision
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(unavailable(error)),
            },
        };
        if previous > committed.catalog_epoch {
            return Err(unavailable(
                "applied checkpoint exceeds controller revision",
            ));
        }
        if checkpoint.is_none() && !committed.catalog_authoritative {
            let dir = &state.broker.config().data_dir;
            let (groups, members) =
                broker_partition::GroupRegistry::legacy_shared(dir).map_err(unavailable)?;
            let pending = !broker_partition::FlowProfileRegistry::legacy_shared(dir)
                .map_err(unavailable)?
                .is_empty()
                || !broker_partition::SubscriptionRegistry::legacy_shared(dir)
                    .map_err(unavailable)?
                    .is_empty()
                || !groups.is_empty()
                || !members.is_empty();
            let pending = pending
                || !crate::CatalogTombstones::legacy_shared_snapshot(dir)
                    .map_err(unavailable)?
                    .is_empty();
            apply.migration_pending.store(pending, Ordering::Release);
        }
        if checkpoint.is_none() && committed.catalog_authoritative {
            let unknown = snapshot_records(crate::cluster::catalog_snapshot(state)?)?
                .into_iter()
                .any(|m| {
                    !m.tombstone
                        && !committed.catalog_records.contains_key(&m.key())
                        && !committed.catalog_records.contains_key(&catalog_identity(
                            "*",
                            m.kind.as_str(),
                            &m.id.to_string(),
                        ))
                });
            apply.adoption_pending.store(unknown, Ordering::Release);
        }
        if checkpoint.is_some()
            && !apply.dirty.load(Ordering::Acquire)
            && apply.revision() >= committed.catalog_epoch
        {
            return Ok(previous);
        }
        apply.dirty.store(true, Ordering::Release);
        let mut records: Vec<_> = broker_raft_meta::catalog::committed_records(&committed)
            .into_iter()
            .filter(|r| r.revision > previous || (r.tombstone && r.revision == 0))
            .collect();
        records.sort_by_key(|r| r.revision);
        for record in records {
            apply_record(state, &record)?;
        }
        // Publish the checkpoint only after every local registry is durable.
        broker_storage::atomic_write_file(
            &path,
            &serde_json::to_vec(&Checkpoint {
                applied_revision: committed.catalog_epoch,
            })
            .map_err(unavailable)?,
        )
        .map_err(unavailable)?;
        *checkpoint = Some(committed.catalog_epoch);
        apply
            .applied
            .store(committed.catalog_epoch, Ordering::Release);
        apply.dirty.store(false, Ordering::Release);
        if committed.catalog_authoritative {
            apply.migration_pending.store(false, Ordering::Release);
        }
        Ok(committed.catalog_epoch)
    })();
    if let Err(error) = &result {
        apply.dirty.store(true, Ordering::Release);
        apply.errors.fetch_add(1, Ordering::Relaxed);
        *apply.diagnostic.lock() = Some(error.to_string());
    }
    result
}

pub(crate) fn authoritative(state: &AppState) -> bool {
    state
        .cluster
        .as_ref()
        .and_then(|c| c.runtime.controller())
        .is_some_and(|c| c.state().catalog_authoritative)
}

pub(crate) fn legacy_write_allowed(state: &AppState) -> Result<(), ApiError> {
    if authoritative(state)
        || state
            .cluster
            .as_ref()
            .and_then(|c| c.runtime.controller())
            .is_some_and(|c| c.catalog_writes_frozen())
    {
        return Err(ApiError::Conflict(
            "catalog is controller-authoritative; legacy peer writes are read-only diagnostics"
                .into(),
        ));
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Capability {
    pub controller_catalog_v1: bool,
}

pub(crate) async fn capabilities() -> axum::Json<Capability> {
    axum::Json(Capability {
        controller_catalog_v1: WRITER_CAPABLE,
    })
}

fn activation_snapshot(state: &AppState) -> Result<crate::cluster::CatalogSnapshot, ApiError> {
    let mut snapshot = crate::cluster::catalog_snapshot(state)?;
    let dir = &state.broker.config().data_dir;
    for (id, timestamp) in
        crate::CatalogTombstones::legacy_shared_snapshot(dir).map_err(unavailable)?
    {
        snapshot
            .tombstones
            .entry(id)
            .and_modify(|existing| *existing = (*existing).max(timestamp))
            .or_insert(timestamp);
    }
    snapshot
        .flows
        .extend(broker_partition::FlowProfileRegistry::legacy_shared(dir).map_err(unavailable)?);
    snapshot
        .queues
        .extend(broker_partition::SubscriptionRegistry::legacy_shared(dir).map_err(unavailable)?);
    let (groups, members) =
        broker_partition::GroupRegistry::legacy_shared(dir).map_err(unavailable)?;
    snapshot.groups.extend(groups);
    snapshot.group_members.extend(members);
    Ok(snapshot)
}

pub(crate) async fn activation_snapshot_endpoint(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<axum::Json<crate::cluster::CatalogSnapshot>, ApiError> {
    Ok(axum::Json(activation_snapshot(&state)?))
}

fn semantic(payload: &str) -> Result<serde_json::Value, ApiError> {
    let mut value: serde_json::Value = serde_json::from_str(payload).map_err(unavailable)?;
    if let Some(object) = value.as_object_mut() {
        for key in [
            "updated_at_ms",
            "created_at_ms",
            "next_run_at_ms",
            "last_run_at_ms",
        ] {
            object.remove(key);
        }
    }
    Ok(value)
}

pub(crate) fn additive_no_overwrite(
    state: &AppState,
    incoming: &crate::cluster::CatalogSnapshot,
) -> Result<(), ApiError> {
    if state
        .cluster
        .as_ref()
        .and_then(|c| c.runtime.controller())
        .is_none()
    {
        return Ok(());
    }
    let local = snapshot_records(crate::cluster::catalog_snapshot(state)?)?;
    for candidate in snapshot_records(incoming.clone())?
        .into_iter()
        .filter(|m| !m.tombstone)
    {
        let value = semantic(&candidate.payload_json)?;
        for known in local.iter().filter(|m| {
            !m.tombstone && m.kind == candidate.kind && m.tenant_id == candidate.tenant_id
        }) {
            let old = semantic(&known.payload_json)?;
            let name = match candidate.kind {
                CatalogKind::Queue => "topic",
                CatalogKind::Flow => "key",
                CatalogKind::Group | CatalogKind::GroupMember => "name",
                CatalogKind::Cron => "id",
            };
            let same_name = old[name] == value[name]
                && (candidate.kind != CatalogKind::GroupMember
                    || old["group_id"] == value["group_id"]);
            if (known.id == candidate.id && old != value) || (known.id != candidate.id && same_name)
            {
                return Err(ApiError::Conflict(format!("additive catalog push conflicts with local {}; activation must reconcile the original definitions", known.key())));
            }
        }
    }
    Ok(())
}

fn snapshot_records(
    snapshot: crate::cluster::CatalogSnapshot,
) -> Result<Vec<CatalogMutation>, ApiError> {
    let mut records = Vec::new();
    for flow in snapshot.flows {
        records.push(record(
            flow.tenant_id.clone(),
            CatalogKind::Flow,
            flow.id,
            &flow,
            None,
            false,
        )?);
    }
    for queue in snapshot.queues {
        records.push(record(
            queue.tenant_id.clone(),
            CatalogKind::Queue,
            queue.id,
            &queue,
            None,
            false,
        )?);
    }
    for group in snapshot.groups {
        records.push(record(
            group.tenant_id.clone(),
            CatalogKind::Group,
            group.id,
            &group,
            None,
            false,
        )?);
    }
    for member in snapshot.group_members {
        records.push(record(
            member.tenant_id.clone(),
            CatalogKind::GroupMember,
            member.id,
            &member,
            None,
            false,
        )?);
    }
    for cron in snapshot.crons {
        records.push(record(
            "default".into(),
            CatalogKind::Cron,
            cron.id,
            &cron,
            None,
            false,
        )?);
    }
    // Legacy UUID tombstones lost their kind and tenant on peer propagation.
    // Preserve their meaning globally rather than inventing a queue identity.
    for id in snapshot.tombstones.keys() {
        for kind in [
            CatalogKind::Flow,
            CatalogKind::Queue,
            CatalogKind::Group,
            CatalogKind::GroupMember,
            CatalogKind::Cron,
        ] {
            records.push(record(
                "*".into(),
                kind,
                *id,
                &serde_json::Value::Null,
                None,
                true,
            )?);
        }
    }
    Ok(records)
}

fn union(
    snapshots: Vec<crate::cluster::CatalogSnapshot>,
    committed: &ControllerState,
) -> Result<Vec<CatalogMutation>, ApiError> {
    let mut records: BTreeMap<String, CatalogMutation> = BTreeMap::new();
    let mut all = snapshots
        .into_iter()
        .map(snapshot_records)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for record in broker_raft_meta::catalog::committed_records(committed) {
        let mutation = CatalogMutation {
            tenant_id: record.tenant_id.clone(),
            kind: serde_json::from_value(serde_json::json!(record.kind)).map_err(unavailable)?,
            id: Uuid::parse_str(&record.key).map_err(unavailable)?,
            payload_json: record.payload_json.clone(),
            tombstone: record.tombstone,
            expected_version: None,
        };
        all.retain(|m| m.key() != mutation.key() || m.tombstone);
        all.push(mutation);
    }
    let deleted: HashSet<_> = all.iter().filter(|m| m.tombstone).map(|m| m.id).collect();
    for mutation in all {
        if !mutation.tombstone && deleted.contains(&mutation.id) {
            continue;
        }
        if let Some(previous) = records.get(&mutation.key()) {
            if semantic(&previous.payload_json)? != semantic(&mutation.payload_json)? {
                return Err(ApiError::Conflict(format!(
                    "catalog activation ambiguous for {}; reconcile node catalogs before retrying",
                    mutation.key()
                )));
            }
        }
        records.insert(mutation.key(), mutation);
    }
    Ok(records
        .into_values()
        .filter(|m| m.tombstone || !deleted.contains(&m.id))
        .collect())
}

async fn activate(state: &Arc<AppState>) -> Result<(), ApiError> {
    let Some(cluster) = &state.cluster else {
        return Ok(());
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Ok(());
    };
    if !controller.is_leader_with_quorum() || controller.state().catalog_authoritative {
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(unavailable)?;
    let peers: Vec<_> = controller
        .state()
        .data_nodes
        .values()
        .filter(|node| node.id != cluster.runtime.config().node_id && node.broker)
        .map(|node| node.addr.clone())
        .collect();
    // No freeze until every configured node is reachable and advertises the
    // writer capability. Freeze is itself committed, so a leader change is safe.
    for peer in &peers {
        let response = crate::cluster_auth::apply_cluster_secret(
            client.get(format!("{peer}/internal/v1/cluster/catalog/capabilities")),
        )
        .send()
        .await
        .map_err(unavailable)?;
        if !response.status().is_success()
            || !response
                .json::<Capability>()
                .await
                .map_err(unavailable)?
                .controller_catalog_v1
        {
            return Err(unavailable(format!(
                "activation blocked: upgrade/reconnect configured data node {peer}"
            )));
        }
    }
    controller
        .submit(ControllerCommand::BeginCatalogActivation)
        .await
        .map_err(controller_error)?;
    let mut snapshots = vec![activation_snapshot(state)?];
    // Allow caches to observe the freeze before taking their diagnostic snapshots.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    for peer in &peers {
        let response = crate::cluster_auth::apply_cluster_secret(client.get(format!(
            "{peer}/internal/v1/cluster/catalog/activation-snapshot"
        )))
        .send()
        .await
        .map_err(unavailable)?;
        if !response.status().is_success() {
            return Err(unavailable(format!(
                "activation snapshot unavailable at {peer}: {}",
                response.status()
            )));
        }
        snapshots.push(response.json().await.map_err(unavailable)?);
    }
    let records = union(snapshots, &controller.state())?;
    controller
        .submit(ControllerCommand::SeedCatalog {
            operation_id: Uuid::new_v4(),
            records,
        })
        .await
        .map_err(controller_error)?;
    materialize(state)?;
    Ok(())
}

async fn adopt(state: &Arc<AppState>, committed: &ControllerState) -> Result<(), ApiError> {
    if !committed.catalog_authoritative {
        return Ok(());
    }
    let cluster = state.cluster.as_ref().expect("controller");
    let controller = cluster.runtime.controller().expect("controller");
    let local = snapshot_records(activation_snapshot(state)?)?;
    let mut missing = Vec::new();
    for mutation in local.into_iter().filter(|m| !m.tombstone) {
        if !committed.catalog_records.contains_key(&mutation.key())
            && !committed.catalog_records.contains_key(&catalog_identity(
                "*",
                mutation.kind.as_str(),
                &mutation.id.to_string(),
            ))
        {
            missing.push(mutation);
        }
    }
    if !missing.is_empty() {
        cluster
            .catalog_apply
            .adoption_pending
            .store(true, Ordering::Release);
        controller
            .submit(ControllerCommand::MutateCatalog {
                operation_id: Uuid::new_v4(),
                mutations: missing,
            })
            .await
            .map_err(controller_error)?;
        materialize(state)?;
    }
    cluster
        .catalog_apply
        .adoption_pending
        .store(false, Ordering::Release);
    Ok(())
}

pub(crate) fn spawn(state: Arc<AppState>) {
    let Some(cluster) = &state.cluster else {
        return;
    };
    if cluster.runtime.controller().is_none()
        || cluster.catalog_apply.started.swap(true, Ordering::AcqRel)
    {
        return;
    }
    let repair = state.clone();
    tokio::spawn(async move {
        while !crate::admission::is_shutting_down() {
            let result = async {
                if WRITER_CAPABLE {
                    activate(&repair).await?;
                }
                let committed = repair
                    .cluster
                    .as_ref()
                    .and_then(|c| c.runtime.controller())
                    .expect("controller")
                    .state();
                adopt(&repair, &committed).await
            }
            .await;
            if let Err(error) = result {
                *repair
                    .cluster
                    .as_ref()
                    .expect("cluster")
                    .catalog_apply
                    .diagnostic
                    .lock() = Some(error.to_string());
                tracing::warn!(%error, "catalog activation/adoption blocked");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
    tokio::spawn(async move {
        loop {
            if crate::admission::is_shutting_down() {
                break;
            }
            if let Err(error) = materialize(&state) {
                tracing::warn!(%error, "catalog materialization failed; serving fenced");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest};
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
    use broker_schedule::{CronRegistry, ScheduleQueue};

    async fn state(dir: &tempfile::TempDir) -> Arc<AppState> {
        let id = Uuid::new_v4();
        let runtime = ClusterRuntime::open_with_raft(
            dir.path(),
            ClusterConfig {
                cluster_id: Uuid::new_v4(),
                node_id: id,
                nodes: vec![NodeConfig {
                    id,
                    addr: "http://127.0.0.1:1".into(),
                    internal_url: None,
                }],
                generation: 1,
                hash_version: 1,
            },
            4,
        )
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !runtime.has_controller_lease() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().into())).unwrap();
        Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
            leases: LeaseTable::new(),
            cluster: Some(crate::Cluster::new(runtime)),
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: crate::CatalogTombstones::open(dir.path()).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }
    fn queue(state: &AppState) -> broker_partition::Subscription {
        state
            .broker
            .prepare_subscription(
                CreateSubscriptionRequest {
                    topic: "jobs".into(),
                    url: "https://example.com/hook".into(),
                    secret: "secret".into(),
                    parallelism: None,
                    default_max_retries: None,
                    retry_backoff: None,
                },
                Uuid::new_v4(),
            )
            .unwrap()
    }
    #[tokio::test]
    async fn committed_before_response_is_recovered_and_disk_failure_fences_until_retry() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        let queue = queue(&state);
        assert!(
            state.broker.get_queue("jobs").unwrap().is_none(),
            "preparation must be pure"
        );
        let cluster = state.cluster.as_ref().unwrap();
        let controller = cluster.runtime.controller().unwrap();
        controller
            .submit(ControllerCommand::MutateCatalog {
                operation_id: Uuid::new_v4(),
                mutations: vec![record(
                    "default".into(),
                    CatalogKind::Queue,
                    queue.id,
                    &queue,
                    None,
                    false,
                )
                .unwrap()],
            })
            .await
            .unwrap();
        assert!(
            state.broker.get_queue("jobs").unwrap().is_none(),
            "commit before local apply / HTTP response"
        );
        let checkpoint = dir.path().join("catalog-applied.json");
        std::fs::create_dir(&checkpoint).unwrap();
        assert!(materialize(&state).is_err());
        assert!(!cluster.catalog_ready());
        std::fs::remove_dir(&checkpoint).unwrap();
        materialize(&state).unwrap();
        assert!(cluster.catalog_ready());
        assert_eq!(
            state.broker.get_queue("jobs").unwrap().unwrap().id,
            queue.id
        );
        // Simulate a new materializer instance loading the durable checkpoint.
        *cluster.catalog_apply.lock.lock() = None;
        cluster.catalog_apply.dirty.store(true, Ordering::Release);
        materialize(&state).unwrap();
        assert!(cluster.catalog_ready());
        controller
            .submit(ControllerCommand::MutateCatalog {
                operation_id: Uuid::new_v4(),
                mutations: vec![record(
                    "default".into(),
                    CatalogKind::Queue,
                    queue.id,
                    &queue,
                    Some(1),
                    true,
                )
                .unwrap()],
            })
            .await
            .unwrap();
        materialize(&state).unwrap();
        assert!(state.broker.get_queue("jobs").unwrap().is_none());
    }
    #[test]
    fn activation_rejects_ambiguous_union_and_tombstones_win_without_clock_guessing() {
        let dir = tempfile::tempdir().unwrap();
        let config = ClusterConfig {
            cluster_id: Uuid::nil(),
            node_id: Uuid::nil(),
            nodes: vec![],
            generation: 1,
            hash_version: 1,
        };
        let committed = ControllerState::from_v1(&config, 1, &Default::default(), None).unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().into())).unwrap();
        let first = broker
            .prepare_subscription(
                CreateSubscriptionRequest {
                    topic: "jobs".into(),
                    url: "https://example.com/a".into(),
                    secret: "s".into(),
                    parallelism: None,
                    default_max_retries: None,
                    retry_backoff: None,
                },
                Uuid::new_v4(),
            )
            .unwrap();
        let mut changed = first.clone();
        changed.url = "https://example.com/b".into();
        changed.updated_at_ms = i64::MAX;
        let a = crate::cluster::CatalogSnapshot {
            queues: vec![first.clone()],
            ..Default::default()
        };
        let b = crate::cluster::CatalogSnapshot {
            queues: vec![changed],
            ..Default::default()
        };
        assert!(union(vec![a.clone(), b.clone()], &committed).is_err());
        let deleted = crate::cluster::CatalogSnapshot {
            tombstones: [(first.id, 1)].into_iter().collect(),
            ..Default::default()
        };
        assert!(union(vec![a, b, deleted], &committed)
            .unwrap()
            .iter()
            .all(|m| m.tombstone));
    }
    #[tokio::test]
    async fn activation_blocks_unreachable_configured_node_without_freezing_writes() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        let controller = state
            .cluster
            .as_ref()
            .unwrap()
            .runtime
            .controller()
            .unwrap();
        controller
            .submit(ControllerCommand::RegisterDataNode {
                node: broker_raft_meta::DataNodeRecord {
                    id: Uuid::new_v4(),
                    addr: "http://127.0.0.1:1".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: false,
                },
            })
            .await
            .unwrap();
        assert!(activate(&state).await.is_err());
        assert!(!controller.state().catalog_authoritative);
        assert!(!controller.state().catalog_frozen);
    }
    #[tokio::test]
    async fn tombstone_only_legacy_recovery_deletes_and_rejects_stale_upserts() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        let queue = queue(&state);
        state
            .broker
            .upsert_subscription_catalog(queue.clone())
            .unwrap();
        crate::cluster::apply_catalog_snapshot(
            &state,
            &crate::cluster::CatalogSnapshot {
                tombstones: [(queue.id, 1)].into_iter().collect(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(state.broker.get_queue("jobs").unwrap().is_none());
        crate::cluster::internal_catalog_queue(
            axum::extract::State(state.clone()),
            axum::Json(crate::cluster::CatalogQueueUpsert {
                subscription: queue,
            }),
        )
        .await
        .unwrap();
        assert!(state.broker.get_queue("jobs").unwrap().is_none());
    }
}
