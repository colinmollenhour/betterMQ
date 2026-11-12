//! Shared producer admission belongs to publishers, never forwarding hops,
//! follower replication, or canonical shard recovery.

use crate::{routes::ApiError, AppState};
use axum::{extract::State, Json};
use broker_partition::{BrokerError, PublishResponse};
use broker_storage::{DedupEntry, ProducerLocks};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub(crate) struct LegacyCandidate {
    tenant: String,
    key: String,
    entry: DedupEntry,
}

async fn resolve_candidate(
    state: &AppState,
    candidate: LegacyCandidate,
) -> Result<Option<DedupEntry>, ApiError> {
    let cluster = state.cluster.as_ref().ok_or_else(|| {
        ApiError::Unavailable("legacy producer resolution requires a canonical shard leader".into())
    })?;
    let partition = candidate.entry.partition;
    let runtime = cluster.runtime.clone();
    let generation = runtime.shard_generation(partition);
    let incarnation = runtime.shard_incarnation(partition);
    if !runtime.is_leader_for_shard(partition) {
        return Err(BrokerError::NotShardLeader(partition).into());
    }
    let broker = state.broker.clone();
    let result = broker_partition::scope_tenant(candidate.tenant, async move {
        broker_partition::run_blocking_io(move || {
            broker.verify_legacy_shared_publish(&candidate.key, &candidate.entry)
        })
        .await
    })
    .await
    .map_err(|error| ApiError::Unavailable(error.to_string()))??;
    if !runtime.is_leader_for_shard(partition)
        || runtime.shard_generation(partition) != generation
        || runtime.shard_incarnation(partition) != incarnation
    {
        return Err(BrokerError::NotShardLeader(partition).into());
    }
    Ok(result)
}

pub(crate) async fn internal_resolve_legacy_producer(
    State(state): State<Arc<AppState>>,
    Json(candidate): Json<LegacyCandidate>,
) -> Result<Json<Option<DedupEntry>>, ApiError> {
    resolve_candidate(&state, candidate).await.map(Json)
}

/// The caller already owns the operation's producer stripes. A read-only
/// canonical-source RPC cannot recursively acquire them or publish a record.
pub(crate) async fn lookup(
    state: &Arc<AppState>,
    key: &str,
    locks: Option<Arc<ProducerLocks>>,
) -> Result<Option<PublishResponse>, ApiError> {
    if let Some(response) = state.broker.shared_committed_publish(key).await? {
        return Ok(Some(response));
    }
    let Some(entry) = state.broker.legacy_shared_publish_hint(key).await? else {
        return Ok(None);
    };
    let cluster = state.cluster.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "legacy producer identity needs authoritative source verification".into(),
        )
    })?;
    let leader = cluster
        .runtime
        .elect_leader_for_shard(entry.partition)
        .ok_or_else(|| {
            ApiError::Unavailable("legacy producer identity has no canonical leader".into())
        })?;
    let candidate = LegacyCandidate {
        tenant: state.broker.tenant(),
        key: key.to_owned(),
        entry,
    };
    let entry = if leader == cluster.runtime.config().node_id {
        resolve_candidate(state, candidate).await?
    } else {
        let peer = cluster
            .runtime
            .config()
            .nodes
            .iter()
            .find(|peer| peer.id == leader)
            .ok_or_else(|| {
                ApiError::Unavailable("canonical producer source is not a configured peer".into())
            })?;
        let mut request = cluster
            .http
            .post(format!(
                "{}/internal/v1/producer/resolve-legacy",
                peer.internal_base()
            ))
            .json(&candidate);
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                request = request.header("x-bettermq-cluster-secret", secret);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| ApiError::Unavailable(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ApiError::Unavailable(format!(
                "canonical producer source is not ready: {}",
                response.status()
            )));
        }
        response
            .json::<Option<DedupEntry>>()
            .await
            .map_err(|error| ApiError::Unavailable(error.to_string()))?
    };
    match entry {
        Some(entry) => state
            .broker
            .adopt_shared_publish(key, entry, locks)
            .await
            .map_err(ApiError::from),
        None => Ok(None),
    }
}
