//! Ops endpoints: readiness, metrics, blocked destinations (CP6b.4 / CP6a).

use crate::routes::ApiError;
use crate::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use broker_dispatch::sample_process_resources;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub cluster_healthy: bool,
    pub auth_configured: bool,
    pub draining: bool,
    pub archive_blocked: bool,
    pub memory_critical: bool,
    pub controller_ready: bool,
    pub local_shards_ready: bool,
    pub catalog_ready: bool,
    pub metadata_ready: bool,
    /// False while any local partition log is quarantined or fenced.
    pub storage_healthy: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub storage_faults: Vec<ShardStorageFault>,
}

/// One unavailable local shard. Paths and topic names stay in the logs and
/// `quarantine.json`: `/readyz` is unauthenticated.
#[derive(Debug, Serialize)]
pub struct ShardStorageFault {
    pub shard: u32,
    pub state: &'static str,
    pub reason: &'static str,
}

fn shard_storage_faults(faults: Vec<broker_storage::StorageFault>) -> Vec<ShardStorageFault> {
    faults
        .into_iter()
        .map(|fault| match fault.kind {
            broker_storage::StorageFaultKind::Quarantined => ShardStorageFault {
                shard: fault.shard,
                state: "quarantined",
                reason: "WAL corruption detected; repair the WAL and remove quarantine.json",
            },
            broker_storage::StorageFaultKind::Fenced => ShardStorageFault {
                shard: fault.shard,
                state: "fenced",
                reason: "WAL write or fsync failed; restart the broker to recover from disk",
            },
        })
        .collect()
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub blocked_hosts: usize,
    pub memory_critical: bool,
    pub cluster_enabled: bool,
    pub healthy_peers: usize,
    pub ingest_accepted: u64,
    pub ingest_duplicate: u64,
    pub ingest_rejected: u64,
    pub durable_commits: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f32>,
}

#[derive(Debug, Serialize)]
pub struct BlockedHostEntry {
    pub host: String,
    pub remaining_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BlockedHostsResponse {
    pub hosts: Vec<BlockedHostEntry>,
    #[serde(flatten)]
    pub scope: DestinationControlScope,
}

#[derive(Debug, Serialize)]
pub struct DestinationControlScope {
    pub scope: &'static str,
    pub applied_nodes: Vec<String>,
    pub shared_metadata: bool,
}

#[derive(Debug, Serialize)]
pub struct DestinationControlResponse {
    #[serde(flatten)]
    pub host: BlockedHostEntry,
    #[serde(flatten)]
    pub scope: DestinationControlScope,
}

fn destination_scope(state: &AppState) -> DestinationControlScope {
    let node = state
        .cluster
        .as_ref()
        .map(|cluster| cluster.runtime.config().node_id.to_string())
        .unwrap_or_else(|| state.dispatch.process_id().to_owned());
    DestinationControlScope {
        scope: "serving_process",
        applied_nodes: vec![node],
        shared_metadata: state.dispatch.host_blocker().shared_metadata_enabled(),
    }
}

#[derive(Debug, Deserialize)]
pub struct UnblockHostRequest {
    pub host: String,
}

#[derive(Debug, Deserialize)]
pub struct BlockHostRequest {
    pub host: String,
    /// Block duration in milliseconds (default: 30 minutes).
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

pub async fn readyz(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadyResponse>) {
    let body = ready_snapshot(&state);
    let status = if body.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body))
}

pub fn ready_snapshot(state: &AppState) -> ReadyResponse {
    let auth_configured = crate::auth::insecure_no_auth()
        || state.local_auth.as_ref().is_some_and(|a| a.is_configured())
        || state.uses_cloud_auth();
    let cluster_healthy = match &state.cluster {
        None => true,
        Some(c) => {
            let now = Utc::now().timestamp_millis();
            let cfg = c.runtime.config();
            let alive = cfg
                .nodes
                .iter()
                .filter(|n| c.runtime.is_peer_alive(n.id, now))
                .count();
            alive >= cfg.quorum_size()
        }
    };
    let storage_faults = shard_storage_faults(broker_storage::storage_faults());
    let storage_healthy = storage_faults.is_empty();
    let metadata_ready = state.broker.metadata_ready();
    let draining = crate::admission::is_shutting_down();
    let archive_blocked = broker_storage::archive_admission_blocked();
    let memory_critical = state.dispatch.memory_guard().is_critical();
    let controller_ready = state
        .cluster
        .as_ref()
        .is_none_or(|c| c.runtime.has_controller_lease());
    let local_shards_ready = state.cluster.as_ref().is_none_or(|c| {
        (0..state.broker.config().partitions).all(|shard| {
            c.runtime.elect_leader_for_shard(shard) != Some(c.runtime.config().node_id)
                || c.runtime.shard_ready(shard)
        })
    });
    let catalog_ready = state.cluster.as_ref().is_none_or(|c| c.catalog_ready());
    ReadyResponse {
        ready: auth_configured
            && cluster_healthy
            && storage_healthy
            && metadata_ready
            && !draining
            && !archive_blocked
            && !memory_critical
            && controller_ready
            && local_shards_ready
            && catalog_ready,
        cluster_healthy,
        auth_configured,
        storage_healthy,
        storage_faults,
        metadata_ready,
        draining,
        archive_blocked,
        memory_critical,
        controller_ready,
        local_shards_ready,
        catalog_ready,
    }
}

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<MetricsResponse>, StatusCode> {
    metrics_authorized(&headers)?;
    Ok(Json(metrics_snapshot(&state)))
}

pub fn metrics_snapshot(state: &AppState) -> MetricsResponse {
    let blocked = state.dispatch.host_blocker().blocked_hosts();
    let cluster_enabled = state.cluster.is_some();
    let healthy_peers = state
        .cluster
        .as_ref()
        .map(|c| {
            let now = Utc::now().timestamp_millis();
            c.runtime
                .config()
                .nodes
                .iter()
                .filter(|n| c.runtime.is_peer_alive(n.id, now))
                .count()
        })
        .unwrap_or(1);
    let guard = state.dispatch.memory_guard();
    let resources = sample_process_resources();
    let memory_limit_mb = guard.limit_mb();
    let memory_percent = match (resources.rss_mb, memory_limit_mb) {
        (Some(rss), Some(limit)) if limit > 0 => Some(((rss * 100) / limit).min(100) as u8),
        _ => None,
    };
    let ingest = crate::metrics::snapshot();
    metrics_snapshot_from(
        blocked.len(),
        guard.is_critical(),
        cluster_enabled,
        healthy_peers,
        ingest,
        resources.rss_mb,
        memory_limit_mb,
        memory_percent,
        resources.cpu_percent,
    )
}

#[allow(clippy::too_many_arguments)]
fn metrics_snapshot_from(
    blocked_hosts: usize,
    memory_critical: bool,
    cluster_enabled: bool,
    healthy_peers: usize,
    ingest: crate::metrics::IngestSnapshot,
    rss_mb: Option<u64>,
    memory_limit_mb: Option<u64>,
    memory_percent: Option<u8>,
    cpu_percent: Option<f32>,
) -> MetricsResponse {
    MetricsResponse {
        blocked_hosts,
        memory_critical,
        cluster_enabled,
        healthy_peers,
        ingest_accepted: ingest.accepted,
        ingest_duplicate: ingest.duplicate,
        ingest_rejected: ingest.rejected,
        durable_commits: ingest.durable_commits,
        rss_mb,
        memory_limit_mb,
        memory_percent,
        cpu_percent,
    }
}

pub(crate) fn metrics_authorized(headers: &axum::http::HeaderMap) -> Result<(), StatusCode> {
    if let Some(expected) = std::env::var("BETTERMQ_METRICS_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let presented = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .or_else(|| {
                headers
                    .get("x-bettermq-metrics-token")
                    .and_then(|v| v.to_str().ok())
            })
            .unwrap_or("");
        use subtle::ConstantTimeEq;
        let a = presented.as_bytes();
        let b = expected.as_bytes();
        if a.len() != b.len() || !bool::from(a.ct_eq(b)) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

pub async fn metrics_prometheus(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, StatusCode> {
    metrics_authorized(&headers)?;
    let native = crate::metrics::native_snapshot(&state);
    let mut body = crate::metrics::prometheus_text_with_native(&native);
    if let Some(cluster) = &state.cluster {
        use std::fmt::Write;
        let committed = cluster.runtime.catalog_epoch();
        let applied = cluster.catalog_apply.revision();
        let _ = writeln!(body, "# TYPE bettermq_catalog_apply_lag_revisions gauge\nbettermq_catalog_apply_lag_revisions {}\nbettermq_catalog_committed_revision {committed}\nbettermq_catalog_applied_revision {applied}\nbettermq_catalog_apply_errors_total {}", committed.saturating_sub(applied), cluster.catalog_apply.errors.load(std::sync::atomic::Ordering::Relaxed));
    }
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response())
}

pub async fn list_blocked_hosts(State(state): State<Arc<AppState>>) -> Json<BlockedHostsResponse> {
    let hosts = state
        .dispatch
        .host_blocker()
        .blocked_hosts()
        .into_iter()
        .map(|(host, remaining_ms)| BlockedHostEntry { host, remaining_ms })
        .collect();
    Json(BlockedHostsResponse {
        hosts,
        scope: destination_scope(&state),
    })
}

pub async fn unblock_host(
    State(state): State<Arc<AppState>>,
    Json(body): Json<UnblockHostRequest>,
) -> Result<Json<DestinationControlResponse>, ApiError> {
    if body.host.trim().is_empty() {
        return Err(ApiError::BadRequest("host required".into()));
    }
    let blocker = state.dispatch.host_blocker();
    let host = body.host.trim().to_owned();
    let input = host.clone();
    tokio::task::spawn_blocking(move || blocker.unblock(&input))
        .await
        .map_err(|_| ApiError::Unavailable("host control task failed".into()))?;
    Ok(Json(DestinationControlResponse {
        host: BlockedHostEntry {
            host,
            remaining_ms: 0,
        },
        scope: destination_scope(&state),
    }))
}

pub async fn block_host(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BlockHostRequest>,
) -> Result<Json<DestinationControlResponse>, ApiError> {
    if body.host.trim().is_empty() {
        return Err(ApiError::BadRequest("host required".into()));
    }
    let duration_ms = body.duration_ms.unwrap_or(30 * 60 * 1000);
    let blocker = state.dispatch.host_blocker();
    let host = body.host.trim().to_owned();
    let key = tokio::task::spawn_blocking(move || blocker.block_manual(&host, duration_ms))
        .await
        .map_err(|_| ApiError::Unavailable("host control task failed".into()))?;
    Ok(Json(DestinationControlResponse {
        host: BlockedHostEntry {
            host: key,
            remaining_ms: duration_ms.max(1_000),
        },
        scope: destination_scope(&state),
    }))
}

/// Aggregate DLQ/delayed/metrics/fleet across local + peer brokers (Phase D).
/// A peer that cannot be queried is reported as unreachable (`active_leases:
/// null`), and `fleet.complete` is false, rather than counted as zero leases.
pub async fn aggregate_ops_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let local_leases = state.leases.active_count() as u64;
    let blocked = state.dispatch.host_blocker().blocked_hosts().len();
    let delayed = state.schedule.count();
    let targets = crate::cluster::catalog_peer_targets(&state);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build();
    let mut peers = Vec::with_capacity(targets.len());
    let mut fleet_total = local_leases;
    let mut unreachable = 0usize;
    for (_id, addr) in targets {
        let result = match &client {
            Ok(http) => peer_active_leases(http, &addr).await,
            Err(e) => Err(format!("http client: {e}")),
        };
        match result {
            Ok(leases) => {
                fleet_total += leases;
                peers.push(serde_json::json!({
                    "addr": addr,
                    "reachable": true,
                    "active_leases": leases,
                }));
            }
            Err(error) => {
                unreachable += 1;
                peers.push(serde_json::json!({
                    "addr": addr,
                    "reachable": false,
                    "active_leases": null,
                    "error": error,
                }));
            }
        }
    }
    Json(serde_json::json!({
        "local": {
            "active_leases": local_leases,
            "blocked_hosts": blocked,
            "delayed": delayed,
            "broker_only": state.broker_only,
            "dispatch_fleet": state.dispatch_fleet,
        },
        "peers": peers,
        "fleet": {
            "active_leases_total": fleet_total,
            "peers_unreachable": unreachable,
            "complete": unreachable == 0,
        }
    }))
}

async fn peer_active_leases(http: &reqwest::Client, addr: &str) -> Result<u64, String> {
    let url = format!("{}/internal/v1/lease/status", addr.trim_end_matches('/'));
    let resp = crate::cluster_auth::apply_cluster_secret(http.get(&url))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<serde_json::Value>()
        .await
        .map_err(|e| format!("invalid lease status: {e}"))?
        .get("active_leases")
        .and_then(|n| n.as_u64())
        .ok_or_else(|| "lease status has no active_leases".to_string())
}
