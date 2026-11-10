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
pub(crate) const WRITER_CAPABLE: bool = true;

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
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, ApiError> {
    let path = request.uri().path();
    let catalog_request = request.method() != axum::http::Method::GET
        && (["/v1/flows", "/v1/groups", "/v1/crons", "/v1/subscriptions"]
            .iter()
            .any(|prefix| path.starts_with(prefix))
            || (path.starts_with("/v1/queues") && !path.ends_with("/enqueue"))
            || (request.method() == axum::http::Method::PUT && path.starts_with("/v1/flow/")));
    let id = match request.headers().get("x-bettermq-operation-id") {
        Some(value) => Uuid::parse_str(
            value
                .to_str()
                .map_err(|_| ApiError::BadRequest("invalid operation id".into()))?,
        )
        .map_err(|_| ApiError::BadRequest("operation id must be a UUID".into()))?,
        None => Uuid::new_v4(),
    };
    let expected_version = request
        .headers()
        .get("if-match")
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|s| s.trim_matches('"').parse::<u64>().ok())
                .ok_or_else(|| {
                    ApiError::BadRequest("If-Match must contain a catalog record version".into())
                })
        })
        .transpose()?;
    let fingerprint = if catalog_request && state.cluster.is_some() {
        use sha2::{Digest, Sha256};
        let (parts, body) = request.into_parts();
        let bytes = axum::body::to_bytes(body, crate::http_body_limit_bytes())
            .await
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        let canonical = serde_json::from_slice::<serde_json::Value>(&bytes)
            .map(|v| format!("json:{v}"))
            .unwrap_or_else(|_| format!("bytes:{bytes:?}"));
        let hash = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    state.broker.tenant(),
                    parts.method.as_str(),
                    parts.uri.to_string(),
                    canonical,
                    bytes.is_empty()
                ))
                .map_err(unavailable)?
            )
        );
        request = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
        Some(hash)
    } else {
        None
    };
    let operation = Operation {
        id,
        fingerprint,
        expected_version,
        revision: AtomicU64::new(0),
        version: AtomicU64::new(0),
    };
    let response = OPERATION
        .scope(operation, async {
            let result: Result<axum::response::Response, ApiError> = async {
                if catalog_request {
                    preflight(&state)?;
                    if let Some(response) = replay_receipt(&state, id).await? {
                        return Ok(response);
                    }
                }
                let response = next.run(request).await;
                // A concurrent copy may commit between the first receipt lookup and
                // handler preparation (notably DELETE, whose object is now absent).
                if catalog_request && !response.status().is_success() {
                    if let Some(receipt) = replay_receipt(&state, id).await? {
                        return Ok(receipt);
                    }
                }
                Ok(response)
            }
            .await;
            use axum::response::IntoResponse;
            let mut response = result.unwrap_or_else(IntoResponse::into_response);
            response.headers_mut().insert(
                "x-bettermq-operation-id",
                id.to_string().parse().expect("UUID header"),
            );
            let revision = OPERATION.with(|op| op.revision.load(Ordering::Acquire));
            if revision != 0 {
                response.headers_mut().insert(
                    "x-bettermq-catalog-revision",
                    revision.to_string().parse().expect("revision header"),
                );
            }
            let version = OPERATION.with(|op| op.version.load(Ordering::Acquire));
            if version != 0 {
                response.headers_mut().insert(
                    "x-bettermq-catalog-version",
                    version.to_string().parse().expect("version header"),
                );
            }
            response
        })
        .await;
    Ok(response)
}

struct Operation {
    id: Uuid,
    fingerprint: Option<String>,
    expected_version: Option<u64>,
    revision: AtomicU64,
    version: AtomicU64,
}
tokio::task_local! { static OPERATION: Operation; }

async fn replay_receipt(
    state: &AppState,
    id: Uuid,
) -> Result<Option<axum::response::Response>, ApiError> {
    let Some(controller) = state.cluster.as_ref().and_then(|c| c.runtime.controller()) else {
        return Ok(None);
    };
    let committed = controller.state();
    let Some(previous) = committed.catalog_operations.get(&id) else {
        return Ok(None);
    };
    if !OPERATION.with(|op| previous.request_fingerprint == op.fingerprint) {
        return Err(ApiError::Conflict(
            "operation id reused for a different request".into(),
        ));
    }
    OPERATION.with(|op| {
        op.revision.store(previous.revision, Ordering::Release);
        op.version
            .store(previous.version.unwrap_or(0), Ordering::Release);
    });
    wait_applied(state, previous.revision).await.map_err(|e| {
        unavailable(format!(
            "operation {id} committed at revision {}, local apply pending: {e}",
            previous.revision
        ))
    })?;
    let mutations: Vec<serde_json::Value> =
        serde_json::from_str(&previous.fingerprint).map_err(unavailable)?;
    let resources: Vec<_> = mutations
        .iter()
        .map(|row| serde_json::json!({"kind":row[1],"id":row[2],"deleted":row[3]}))
        .collect();
    use axum::response::IntoResponse;
    Ok(Some(axum::Json(serde_json::json!({"operation_id":id,"catalog_revision":previous.revision,"catalog_version":previous.version,"replayed":true,"resources":resources})).into_response()))
}

/// Capture versions from exactly the snapshot materialized before handler reads.
/// Taking a fresh version only at commit would overwrite concurrent changes with
/// a payload prepared from an older cache.
pub(crate) struct Mutation {
    baseline: Option<ControllerState>,
    operation_id: Uuid,
}
pub(crate) fn begin(state: &AppState) -> Result<Mutation, ApiError> {
    preflight(state)?;
    let baseline = if state.cluster.is_some() {
        materialize_snapshot(state)?
    } else {
        None
    };
    Ok(Mutation {
        baseline,
        operation_id: OPERATION
            .try_with(|op| op.id)
            .unwrap_or_else(|_| Uuid::new_v4()),
    })
}
impl Mutation {
    pub fn id(&self, tenant: &str, kind: CatalogKind) -> Uuid {
        Uuid::new_v5(
            &self.operation_id,
            format!("{tenant}:{}", kind.as_str()).as_bytes(),
        )
    }
    pub async fn commit<T: Serialize>(
        &self,
        state: &AppState,
        tenant: String,
        kind: CatalogKind,
        id: Uuid,
        payload: &T,
        tombstone: bool,
    ) -> Result<(), ApiError> {
        let Some(baseline) = &self.baseline else {
            return Ok(());
        };
        let expected = baseline
            .catalog_records
            .get(&catalog_identity(&tenant, kind.as_str(), &id.to_string()))
            .map(|r| r.version);
        if let Some(required) = OPERATION
            .try_with(|op| op.expected_version)
            .unwrap_or_default()
        {
            if expected != Some(required) {
                return Err(ApiError::Conflict(
                    "If-Match differs from the prepared catalog version; refresh and retry".into(),
                ));
            }
        }
        let controller = state
            .cluster
            .as_ref()
            .and_then(|c| c.runtime.controller())
            .ok_or_else(|| unavailable("missing controller"))?;
        let request_fingerprint = OPERATION
            .try_with(|op| op.fingerprint.clone())
            .unwrap_or_default();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            controller.submit(ControllerCommand::MutateCatalog {
                operation_id: self.operation_id,
                request_fingerprint,
                mutations: vec![record(tenant, kind, id, payload, expected, tombstone)?],
            }),
        )
        .await;
        let committed = match result {
            Ok(Ok(committed)) => committed,
            Ok(Err(broker_raft_meta::ControllerError::CatalogConflict(message))) => {
                return Err(ApiError::Conflict(message))
            }
            other => {
                return Err(unavailable(format!(
                    "outcome unknown after submission ({other:?}); retry with operation id {}",
                    self.operation_id
                )))
            }
        };
        let revision = committed
            .catalog_operations
            .get(&self.operation_id)
            .ok_or_else(|| unavailable("commit omitted operation receipt"))?
            .revision;
        let _ = OPERATION.try_with(|op| op.revision.store(revision, Ordering::Release));
        let version = committed
            .catalog_operations
            .get(&self.operation_id)
            .and_then(|op| op.version)
            .unwrap_or(0);
        let _ = OPERATION.try_with(|op| op.version.store(version, Ordering::Release));
        wait_applied(state, revision).await.map_err(|e| {
            unavailable(format!(
                "operation {} committed at revision {revision}, local apply pending: {e}",
                self.operation_id
            ))
        })
    }
}
async fn wait_applied(state: &AppState, revision: u64) -> Result<(), ApiError> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if materialize(state)? >= revision {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| unavailable("committed revision is awaiting local application"))?
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
    Ok(materialize_snapshot(state)?.map_or(0, |s| s.catalog_epoch))
}

fn materialize_snapshot(state: &AppState) -> Result<Option<ControllerState>, ApiError> {
    let Some(cluster) = &state.cluster else {
        return Ok(None);
    };
    let Some(controller) = cluster.runtime.controller() else {
        return Err(unavailable("cell has no controller"));
    };
    let apply = &cluster.catalog_apply;
    let mut checkpoint = apply.lock.lock();
    let committed = controller.state();
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
    result.map(|_| Some(committed))
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
    // Additive compatibility is not a back door around the cell's quorum fence.
    preflight(state)
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
                request_fingerprint: None,
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
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        Router,
    };
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest};
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
    use broker_schedule::{CronRegistry, ScheduleQueue};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    fn api(state: Arc<AppState>) -> Router {
        Router::new()
            .merge(crate::bettermq::bettermq_routes())
            .merge(crate::groups::group_routes())
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                mutation_preflight,
            ))
            .with_state(state)
    }
    async fn request(
        api: &Router,
        method: &str,
        path: &str,
        body: Value,
        op: Uuid,
        version: Option<u64>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header("x-bettermq-operation-id", op.to_string());
        if let Some(version) = version {
            request = request.header("if-match", version.to_string());
        }
        let response = api
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn http_catalog_operations_commit_apply_replay_conflict_and_cascade() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        activate(&state).await.unwrap();
        let api = api(state.clone());
        let op = Uuid::new_v4();
        let queue = json!({"queue":"jobs", "url":"https://example.com/hook", "secret":"s"});
        let (status, headers, _) =
            request(&api, "POST", "/v1/queues", queue.clone(), op, None).await;
        assert_eq!(status, StatusCode::CREATED);
        let revision = headers["x-bettermq-catalog-revision"].clone();
        assert!(state.broker.get_queue("jobs").unwrap().is_some());
        let (status, headers, receipt) = request(&api, "POST", "/v1/queues", queue, op, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt["replayed"], true);
        assert_eq!(headers["x-bettermq-catalog-revision"], revision);
        let (status, _, _) = request(
            &api,
            "POST",
            "/v1/queues",
            json!({"queue":"different", "url":"https://example.com/hook", "secret":"s"}),
            op,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (_, _, group) = request(
            &api,
            "POST",
            "/v1/groups",
            json!({"name":"workers"}),
            Uuid::new_v4(),
            None,
        )
        .await;
        let id = group["group_id"].as_str().unwrap();
        let path = format!("/v1/groups/{id}");
        let (status, _, _) = request(
            &api,
            "PUT",
            &path,
            json!({"paused":true}),
            Uuid::new_v4(),
            Some(1),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, _) = request(
            &api,
            "PUT",
            &path,
            json!({"name":"stale"}),
            Uuid::new_v4(),
            Some(1),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            state
                .broker
                .get_group(Uuid::parse_str(id).unwrap())
                .unwrap()
                .unwrap()
                .name,
            "workers"
        );
        let (status, _, member) = request(
            &api,
            "POST",
            &format!("{path}/members"),
            json!({"name":"one", "url":"https://example.com/hook", "secret":"s"}),
            Uuid::new_v4(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let member_id = Uuid::parse_str(member["member_id"].as_str().unwrap()).unwrap();
        let delete_op = Uuid::new_v4();
        let (status, _, _) = request(&api, "DELETE", &path, Value::Null, delete_op, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(state.broker.get_group_member(member_id).unwrap().is_none());
        let (status, _, receipt) =
            request(&api, "DELETE", &path, Value::Null, delete_op, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt["replayed"], true);
        let queue = state.broker.get_queue("jobs").unwrap().unwrap();
        let response = crate::cluster::internal_catalog_queue(
            axum::extract::State(state.clone()),
            axum::Json(crate::cluster::CatalogQueueUpsert {
                subscription: queue,
            }),
        )
        .await;
        assert!(matches!(response, Err(ApiError::Conflict(_))));
        let cron = json!({"every_seconds":60, "url":"https://example.com/hook", "secret":"s", "body":"scheduled"});
        let (status, _, cron) =
            request(&api, "POST", "/v1/crons", cron, Uuid::new_v4(), None).await;
        assert_eq!(status, StatusCode::CREATED, "{cron}");
        let cron_id = cron["cron_id"].as_str().unwrap();
        for action in ["pause", "resume"] {
            let (status, _, body) = request(
                &api,
                "POST",
                &format!("/v1/crons/{cron_id}/{action}"),
                Value::Null,
                Uuid::new_v4(),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (status, _, _) = request(
            &api,
            "DELETE",
            &format!("/v1/crons/{cron_id}"),
            Value::Null,
            Uuid::new_v4(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(state.crons.list().is_empty());
    }

    #[tokio::test]
    async fn mutation_baseline_rejects_payload_prepared_before_a_concurrent_update() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        let first = begin(&state).unwrap();
        let queue = queue(&state);
        first
            .commit(
                &state,
                queue.tenant_id.clone(),
                CatalogKind::Queue,
                queue.id,
                &queue,
                false,
            )
            .await
            .unwrap();
        let stale = begin(&state).unwrap();
        let fresh = begin(&state).unwrap();
        let mut changed = queue.clone();
        changed.url = "https://example.com/new".into();
        fresh
            .commit(
                &state,
                queue.tenant_id.clone(),
                CatalogKind::Queue,
                queue.id,
                &changed,
                false,
            )
            .await
            .unwrap();
        assert!(matches!(
            stale
                .commit(
                    &state,
                    queue.tenant_id.clone(),
                    CatalogKind::Queue,
                    queue.id,
                    &queue,
                    false
                )
                .await,
            Err(ApiError::Conflict(_))
        ));
        assert_eq!(
            state.broker.get_queue("jobs").unwrap().unwrap().url,
            changed.url
        );
    }

    #[tokio::test]
    async fn http_quorum_loss_leaves_every_catalog_cache_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        activate(&state).await.unwrap();
        state
            .cluster
            .as_ref()
            .unwrap()
            .runtime
            .controller()
            .unwrap()
            .raft()
            .shutdown()
            .await
            .unwrap();
        let before =
            serde_json::to_value(crate::cluster::catalog_snapshot(&state).unwrap()).unwrap();
        let api = api(state.clone());
        for (path, body) in [
            (
                "/v1/queues",
                json!({"queue":"jobs", "url":"https://example.com/hook", "secret":"s"}),
            ),
            (
                "/v1/flows",
                json!({"key":"key", "parallelism":1,"rate":0,"period_secs":60}),
            ),
            ("/v1/groups", json!({"name":"group"})),
            (
                "/v1/crons",
                json!({"every_seconds":60,"url":"https://example.com/hook","secret":"s","body":"job"}),
            ),
            (
                "/v1/publish",
                json!({"url":"https://example.com/hook","secret":"s","body":"job","flow":{"key":"inline","parallelism":1}}),
            ),
        ] {
            let (status, _, body) = request(&api, "POST", path, body, Uuid::new_v4(), None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {body}");
        }
        assert_eq!(
            before,
            serde_json::to_value(crate::cluster::catalog_snapshot(&state).unwrap()).unwrap()
        );
    }

    #[tokio::test]
    async fn cell_without_controller_rejects_public_and_legacy_catalog_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = Arc::try_unwrap(state(&dir).await).ok().unwrap();
        let config = state.cluster.as_ref().unwrap().runtime.config().clone();
        state.cluster = Some(crate::Cluster::new(ClusterRuntime::from_config_only(
            config,
        )));
        let state = Arc::new(state);
        assert!(matches!(
            crate::cluster::apply_catalog_snapshot(&state, &Default::default()),
            Err(ApiError::Unavailable(_))
        ));
        let (status, _, body) = request(
            &api(state.clone()),
            "POST",
            "/v1/groups",
            json!({"name":"not-local"}),
            Uuid::new_v4(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert!(state.broker.list_groups().unwrap().is_empty());
    }

    async fn fail_after_commit(
        axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    ) -> Result<axum::Json<Value>, ApiError> {
        let mutation = begin(&state)?;
        let queue = queue(&state);
        let path = state.broker.config().data_dir.join("catalog-applied.json");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        mutation
            .commit(
                &state,
                queue.tenant_id.clone(),
                CatalogKind::Queue,
                queue.id,
                &queue,
                false,
            )
            .await?;
        Ok(axum::Json(json!({"unexpected":true})))
    }
    #[tokio::test]
    async fn http_apply_failure_reports_committed_revision_and_retry_recovers_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(&dir).await;
        let api = Router::new()
            .route("/v1/queues/fail", axum::routing::post(fail_after_commit))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                mutation_preflight,
            ))
            .with_state(state.clone());
        let op = Uuid::new_v4();
        let (status, headers, body) =
            request(&api, "POST", "/v1/queues/fail", Value::Null, op, None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.to_string().contains("committed"));
        let revision = headers["x-bettermq-catalog-revision"].clone();
        assert!(!state.cluster.as_ref().unwrap().catalog_ready());
        assert!(state
            .cluster
            .as_ref()
            .unwrap()
            .runtime
            .controller()
            .unwrap()
            .state()
            .catalog_operations
            .contains_key(&op));
        std::fs::remove_dir(dir.path().join("catalog-applied.json")).unwrap();
        let (status, headers, receipt) =
            request(&api, "POST", "/v1/queues/fail", Value::Null, op, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["x-bettermq-catalog-revision"], revision);
        assert_eq!(receipt["replayed"], true);
        assert!(state.cluster.as_ref().unwrap().catalog_ready());
    }

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
                request_fingerprint: None,
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
                request_fingerprint: None,
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
