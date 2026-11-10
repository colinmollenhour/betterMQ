//! Repair legacy payload blobs from peers before a shard serves (D3).
//!
//! Local-WAL cells now keep bodies inline, but messages accepted before that
//! may reference a blob that exists only on the node that accepted them. When
//! this node takes a shard, every such reference whose blob is missing or
//! corrupt locally is fetched from a peer (`GET
//! /internal/v1/payload-blob/{tenant}/{message_id}`), verified (size, and
//! sha256 when recorded) and installed atomically. Unrepaired references are
//! counted per shard; delivery keeps deferring them (never a DLQ, never an
//! empty body).

use crate::AppState;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Response,
};
use broker_partition::{LogId, PayloadError, PayloadPolicy, PayloadRef};
use futures_util::StreamExt;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PayloadRepairReport {
    pub scanned: usize,
    pub repaired: u64,
    pub unhashed: u64,
    pub unresolved: u64,
}

/// Serve this node's copy of a payload blob to a repairing peer.
pub async fn internal_payload_blob(
    State(state): State<Arc<AppState>>,
    Path((tenant, message_id)): Path<(String, Uuid)>,
) -> Result<Response, crate::routes::ApiError> {
    let broker = state.broker.clone();
    let read = tokio::task::spawn_blocking(move || {
        broker.blob_store().read_local_blob(&tenant, message_id)
    })
    .await
    .map_err(|error| crate::routes::ApiError::BadRequest(error.to_string()))?;
    match read {
        Ok(bytes) => Response::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(bytes))
            .map_err(|error| crate::routes::ApiError::BadRequest(error.to_string())),
        Err(PayloadError::NotFound(key)) => Err(crate::routes::ApiError::NotFound(format!(
            "payload blob {key} not found"
        ))),
        Err(error) => Err(crate::routes::ApiError::Broker(
            broker_partition::BrokerError::Payload(error),
        )),
    }
}

fn shard_logs(state: &AppState, shard: u32) -> Vec<LogId> {
    if state.broker.layout().is_physical() {
        let log = state.broker.log_id("", shard);
        return state
            .broker
            .retained_logs()
            .into_iter()
            .filter(|candidate| *candidate == log)
            .collect();
    }
    state
        .broker
        .retained_logs()
        .into_iter()
        .filter(|log| log.partition == shard)
        .collect()
}

async fn fetch_from_peer(
    http: &reqwest::Client,
    peer: &str,
    reference: &PayloadRef,
) -> Result<Vec<u8>, String> {
    let url = format!(
        "{}/internal/v1/payload-blob/{}/{}",
        peer.trim_end_matches('/'),
        urlencoding::encode(&reference.tenant_id),
        reference.message_id
    );
    let response = crate::cluster_auth::apply_cluster_secret(http.get(&url))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("{url}: {}", response.status()));
    }
    let limit = reference.size;
    if response
        .content_length()
        .is_some_and(|length| length != limit)
    {
        return Err(format!("{url}: size differs from the reference ({limit})"));
    }
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024 * 1024) as usize);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if bytes.len() as u64 + chunk.len() as u64 > limit {
            return Err(format!("{url}: more bytes than the reference ({limit})"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Repair this node's legacy blobs for `shard` from peers. A no-op outside
/// local-WAL cells.
pub async fn repair_legacy_payloads(state: &Arc<AppState>, shard: u32) -> PayloadRepairReport {
    let mut report = PayloadRepairReport::default();
    if !matches!(
        state.broker.payload_policy(),
        PayloadPolicy::InlineOnly { .. }
    ) {
        return report;
    }
    let Some(cluster) = state.cluster.as_ref() else {
        return report;
    };
    let now = chrono::Utc::now().timestamp_millis();
    let peers: Vec<String> = cluster
        .runtime
        .config()
        .peer_nodes()
        .into_iter()
        .filter(|peer| cluster.runtime.is_peer_alive(peer.id, now))
        .map(|peer| peer.addr)
        .collect();
    for log in shard_logs(state, shard) {
        let scan_state = Arc::clone(state);
        let scan_log = log.clone();
        let scanned =
            tokio::task::spawn_blocking(move || scan_state.broker.damaged_payload_refs(&scan_log))
                .await;
        let (scanned, damaged) = match scanned {
            Ok(Ok(found)) => found,
            Ok(Err(error)) => {
                warn!(%log, %error, "legacy payload scan failed");
                report.unresolved += 1;
                continue;
            }
            Err(error) => {
                warn!(%log, %error, "legacy payload scan task failed");
                report.unresolved += 1;
                continue;
            }
        };
        report.scanned += scanned;
        for reference in damaged {
            let mut repaired = false;
            for peer in &peers {
                let bytes = match fetch_from_peer(&cluster.http, peer, &reference).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        warn!(%peer, key = %reference.bucket_key, %error, "legacy blob fetch failed");
                        continue;
                    }
                };
                let install_state = Arc::clone(state);
                let install_ref = reference.clone();
                match tokio::task::spawn_blocking(move || {
                    install_state
                        .broker
                        .install_repaired_blob(&install_ref, &bytes)
                })
                .await
                {
                    Ok(Ok(())) => {
                        repaired = true;
                        break;
                    }
                    Ok(Err(error)) => {
                        warn!(%peer, key = %reference.bucket_key, %error, "peer blob failed verification")
                    }
                    Err(error) => warn!(%error, "legacy blob install task failed"),
                }
            }
            if repaired {
                report.repaired += 1;
                if reference.sha256.is_none() {
                    report.unhashed += 1;
                }
            } else {
                report.unresolved += 1;
                warn!(
                    %log,
                    key = %reference.bucket_key,
                    message_id = %reference.message_id,
                    "legacy payload blob unavailable on every live peer; delivery stays deferred"
                );
            }
        }
    }
    broker_partition::record_payload_repair(
        shard,
        report.unresolved,
        report.repaired,
        report.unhashed,
    );
    if report.repaired > 0 || report.unresolved > 0 {
        info!(shard, ?report, "legacy payload repair pass");
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, PublishRequest};
    use broker_schedule::{CronRegistry, ScheduleQueue};

    fn state(dir: &std::path::Path) -> Arc<AppState> {
        let mut config = BrokerConfig::new(dir.to_path_buf());
        config.partitions = 1;
        let broker = Broker::open(config).unwrap();
        Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir).unwrap(),
            crons: CronRegistry::open(dir).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
            leases: LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: crate::catalog_tombstones::CatalogTombstones::open(dir).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_serves_legacy_blob_and_fetch_verifies_it() {
        let dir = tempfile::tempdir().unwrap();
        let peer = state(dir.path());
        let body = "legacy".repeat(broker_payload_len() / 6 + 2);
        peer.broker
            .publish(PublishRequest {
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "lane".into(),
                payload: body.clone(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: Some("http://127.0.0.1:9/h".into()),
                secret: Some("s".into()),
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
                routing_id: None,
            })
            .unwrap();
        let log = peer.broker.log_id(broker_partition::DIRECT_TOPIC, 0);
        let message = peer.broker.read_log(&log, 0, 10).unwrap().remove(0);
        let reference: PayloadRef =
            serde_json::from_str(message.payload_ref_json.as_deref().unwrap()).unwrap();

        let app = axum::Router::new()
            .route(
                "/internal/v1/payload-blob/{tenant}/{message_id}",
                axum::routing::get(internal_payload_blob),
            )
            .with_state(Arc::clone(&peer));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();

        let bytes = fetch_from_peer(&http, &addr, &reference).await.unwrap();
        assert_eq!(bytes, body.as_bytes());

        // A node that lost the blob repairs it and can hydrate again.
        let target_dir = tempfile::tempdir().unwrap();
        let target = state(target_dir.path());
        assert!(target.broker.blob_store().verify_blob(&reference).is_err());
        target
            .broker
            .install_repaired_blob(&reference, &bytes)
            .unwrap();
        target.broker.blob_store().verify_blob(&reference).unwrap();

        // Unavailable source and a size that disagrees with the reference.
        let missing = PayloadRef {
            message_id: Uuid::new_v4(),
            ..reference.clone()
        };
        assert!(fetch_from_peer(&http, &addr, &missing).await.is_err());
        let wrong_size = PayloadRef {
            size: reference.size - 1,
            ..reference.clone()
        };
        assert!(fetch_from_peer(&http, &addr, &wrong_size).await.is_err());
        assert!(fetch_from_peer(&http, "http://127.0.0.1:9", &reference)
            .await
            .is_err());
    }

    fn broker_payload_len() -> usize {
        // Larger than the default inline threshold so the peer stores a blob.
        300 * 1024
    }

    #[tokio::test]
    async fn repair_is_a_noop_outside_local_wal_cells() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        assert_eq!(
            repair_legacy_payloads(&state, 0).await,
            PayloadRepairReport::default()
        );
    }
}
