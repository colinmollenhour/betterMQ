//! Public HTTP surface for the BetterMQ data plane.

mod admin;
mod admission;
mod auth;
mod batch;
mod bettermq;
mod catalog_tombstones;
mod cluster;
pub mod cluster_auth;
mod fanout;
mod gateway;
mod groups;
mod http_fields;
mod infra;
mod ingest;
mod lease;
mod local_auth;
mod metering;
mod metrics;
mod ops;
mod publish_path;
mod rate_limit;
mod routes;

use axum::{extract::DefaultBodyLimit, routing::get, Json, Router};
use broker_dispatch::{DispatchEngine, LeaseTable};
use broker_partition::Broker;
use broker_schedule::{CronRegistry, ScheduleQueue};
use cluster::ClusterHandle;
use serde::Serialize;
use std::sync::Arc;

/// HTTP body cap (cloud plan ceiling or self-host default). Override with BETTERMQ_MAX_HTTP_BODY_BYTES.
fn http_body_limit_bytes() -> usize {
    if let Some(n) = std::env::var("BETTERMQ_MAX_HTTP_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return n;
    }
    #[cfg(feature = "cloud")]
    if let Some(n) = std::env::var("BETTERMQ_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return n;
    }
    // Match the documented batch wire cap unless explicitly overridden.
    ingest::MAX_BATCH_BYTES
}

pub use admin::{
    router as admin_router, validate_attach_url, AdminState, CellMember, CellRecord, CellRegistry,
};
pub use catalog_tombstones::CatalogTombstones;
pub use cluster::{
    build_cluster_status, catalog_peer_targets, enqueue_dispatch_after_publish,
    publish_with_cluster, push_catalog_to_recovered_peer, spawn_cluster_catalog_sync,
    sync_catalog_from_peers, ClusterGossipRequest, ClusterGossipResponse, ClusterHandle as Cluster,
    ClusterStatusResponse,
};
pub use gateway::GatewayOnlyState;
pub use local_auth::{close_setup_window, open_setup_window};

/// Reject new ingest while allowing already-admitted requests to drain.
pub fn begin_shutdown() {
    admission::begin_shutdown();
}

#[derive(Clone)]
pub struct AppState {
    pub broker: Broker,
    pub schedule: ScheduleQueue,
    pub crons: CronRegistry,
    pub dispatch: DispatchEngine,
    pub leases: LeaseTable,
    pub cluster: Option<ClusterHandle>,
    pub local_auth: Option<Arc<broker_local_auth::LocalAuthStore>>,
    pub fair_queue: Arc<broker_dispatch::TenantFairQueue>,
    pub catalog_tombstones: CatalogTombstones,
    /// When true, public ingest routes are not mounted (fleet mode).
    pub dispatch_fleet: bool,
    /// When true, local delivery workers are not the primary path (broker-only).
    pub broker_only: bool,
    #[cfg(feature = "cloud")]
    pub auth: Option<broker_control_plane::ApiKeyValidator>,
    #[cfg(feature = "cloud")]
    pub control_plane: Option<broker_control_plane::ControlPlanePool>,
}

impl AppState {
    /// Postgres API-key auth (BetterMQ Cloud edition only).
    pub fn uses_cloud_auth(&self) -> bool {
        #[cfg(feature = "cloud")]
        {
            return self.auth.is_some();
        }
        #[cfg(not(feature = "cloud"))]
        {
            false
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub protocol: u32,
}

fn into_shared(state: AppState, spawn: bool) -> Arc<AppState> {
    let shared = Arc::new(state);
    if spawn {
        bettermq::spawn_fanout_replay(shared.clone());
        bettermq::spawn_dlq_retention(shared.clone());
    }
    shared
}

fn data_plane_routes(shared: Arc<AppState>) -> Router<Arc<AppState>> {
    let dispatch_fleet = shared.dispatch_fleet;
    let protected = if dispatch_fleet {
        Router::new()
    } else {
        Router::new()
            .route(
                "/v1/enqueue/batch",
                axum::routing::post(batch::batch_enqueue),
            )
            .route(
                "/v1/ingest/batch",
                axum::routing::post(batch::batch_enqueue_ht),
            )
            .route(
                "/v1/ingest/ndjson",
                axum::routing::post(batch::batch_enqueue_ndjson),
            )
            .route(
                "/v1/gateway/enqueue",
                axum::routing::post(gateway::gateway_enqueue),
            )
            .merge(bettermq::bettermq_routes())
            .merge(groups::group_routes())
            .route("/v1/destinations/blocked", get(ops::list_blocked_hosts))
            .route(
                "/v1/destinations/block",
                axum::routing::post(ops::block_host),
            )
            .route(
                "/v1/destinations/unblock",
                axum::routing::post(ops::unblock_host),
            )
            .merge(infra::protected_infra_routes())
            .route("/v1/ops/aggregate", get(ops::aggregate_ops_status))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                admission::buffer_ingest_body,
            ))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                auth::check_ingest_limits,
            ))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                auth::require_api_key,
            ))
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(ops::readyz))
        .route("/metrics", get(ops::metrics))
        .route("/metrics/prometheus", get(ops::metrics_prometheus))
        .merge(local_auth::routes())
        .merge(infra::public_infra_routes())
        .merge(protected)
}

fn finish_router(routes: Router<Arc<AppState>>, shared: Arc<AppState>) -> Router {
    routes
        .layer(DefaultBodyLimit::max(http_body_limit_bytes()))
        .with_state(shared)
}

/// Data-plane + `/internal/v1` (single collapsed listener).
pub fn router(state: AppState) -> Router {
    let shared = into_shared(state, true);
    finish_router(
        data_plane_routes(shared.clone()).merge(internal_cluster_routes()),
        shared,
    )
}

/// Public ingest/API (`/v1`, `/healthz`, metrics). Does not include `/internal/v1`.
pub fn data_plane_router(state: AppState) -> Router {
    data_plane_router_spawn(state, true)
}

/// Same as [`data_plane_router`] without starting fan-out/DLQ tasks (already spawned).
pub fn data_plane_router_no_spawn(state: AppState) -> Router {
    data_plane_router_spawn(state, false)
}

fn data_plane_router_spawn(state: AppState, spawn: bool) -> Router {
    let shared = into_shared(state, spawn);
    finish_router(data_plane_routes(shared.clone()), shared)
}

/// Public data-plane listener when panel/internal are split off.
pub fn public_router(state: AppState) -> Router {
    data_plane_router(state)
}

/// Replication/lease/controller listener (`/internal/v1` only).
pub fn internal_router(state: AppState) -> Router {
    let shared = into_shared(state, false);
    finish_router(internal_cluster_routes(), shared)
}

fn internal_cluster_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/internal/v1/lease/claim",
            axum::routing::post(lease::lease_claim),
        )
        .route(
            "/internal/v1/lease/heartbeat",
            axum::routing::post(lease::lease_heartbeat),
        )
        .route(
            "/internal/v1/lease/complete",
            axum::routing::post(lease::lease_complete),
        )
        .route(
            "/internal/v1/lease/fail",
            axum::routing::post(lease::lease_fail),
        )
        .route(
            "/internal/v1/lease/status",
            axum::routing::get(lease::lease_status),
        )
        .route(
            "/internal/v1/replicate",
            axum::routing::post(cluster::internal_replicate),
        )
        .route(
            "/internal/v1/replicate/batch",
            axum::routing::post(cluster::internal_replicate_batch),
        )
        .route(
            "/internal/v1/replicate/catch-up",
            axum::routing::post(cluster::internal_replicate_catch_up),
        )
        .route(
            "/internal/v1/cluster",
            axum::routing::get(cluster::internal_cluster_config),
        )
        .route(
            "/internal/v1/cluster/gossip",
            axum::routing::post(cluster::internal_cluster_gossip),
        )
        .route(
            "/internal/v1/controller/raft/vote",
            axum::routing::post(cluster::internal_controller_vote),
        )
        .route(
            "/internal/v1/controller/raft/append",
            axum::routing::post(cluster::internal_controller_append),
        )
        .route(
            "/internal/v1/controller/raft/snapshot",
            axum::routing::post(cluster::internal_controller_snapshot),
        )
        .route(
            "/internal/v1/controller/command",
            axum::routing::post(cluster::internal_controller_command),
        )
        .route(
            "/internal/v1/cluster/membership",
            axum::routing::get(infra::internal_cluster_membership),
        )
        .route(
            "/internal/v1/cluster/update-node",
            axum::routing::post(infra::internal_cluster_update_node),
        )
        .route(
            "/internal/v1/cluster/apply-membership",
            axum::routing::post(infra::internal_cluster_apply_membership),
        )
        .route(
            "/internal/v1/cluster/publish",
            axum::routing::post(cluster::internal_cluster_publish),
        )
        .route(
            "/internal/v1/gateway/ingest",
            axum::routing::post(gateway::internal_gateway_ingest),
        )
        .route(
            "/internal/v1/cluster/catalog",
            axum::routing::get(cluster::internal_catalog_snapshot),
        )
        .route(
            "/internal/v1/cluster/catalog/apply",
            axum::routing::post(cluster::internal_catalog_apply),
        )
        .route(
            "/internal/v1/cluster/catalog/flow",
            axum::routing::post(cluster::internal_catalog_flow),
        )
        .route(
            "/internal/v1/cluster/catalog/queue",
            axum::routing::post(cluster::internal_catalog_queue),
        )
        .route(
            "/internal/v1/cluster/catalog/flow/delete",
            axum::routing::post(cluster::internal_catalog_delete_flow),
        )
        .route(
            "/internal/v1/cluster/catalog/queue/delete",
            axum::routing::post(cluster::internal_catalog_delete_queue),
        )
        .route(
            "/internal/v1/cluster/catalog/cron",
            axum::routing::post(cluster::internal_catalog_cron),
        )
        .route(
            "/internal/v1/cluster/catalog/cron/delete",
            axum::routing::post(cluster::internal_catalog_delete_cron),
        )
        .route(
            "/internal/v1/cluster/catalog/group",
            axum::routing::post(cluster::internal_catalog_group),
        )
        .route(
            "/internal/v1/cluster/catalog/group/delete",
            axum::routing::post(cluster::internal_catalog_delete_group),
        )
        .route(
            "/internal/v1/cluster/catalog/group-member",
            axum::routing::post(cluster::internal_catalog_group_member),
        )
        .route(
            "/internal/v1/cluster/catalog/group-member/delete",
            axum::routing::post(cluster::internal_catalog_delete_group_member),
        )
        .route(
            "/internal/v1/cluster/auth",
            axum::routing::get(cluster::internal_cluster_auth),
        )
        .route(
            "/internal/v1/cluster/auth/apply",
            axum::routing::post(cluster::internal_cluster_auth_apply),
        )
        .route_layer(axum::middleware::from_fn(
            cluster_auth::require_cluster_secret,
        ))
}

/// Stateless gateway router. This state contains only routing/auth clients and
/// never opens broker storage, WAL, indexes, schedules, dispatch, or archives.
pub fn gateway_only_router(state: GatewayOnlyState) -> Router {
    let shared = Arc::new(state);
    let protected = Router::new()
        .route(
            "/v1/gateway/enqueue",
            axum::routing::post(gateway::gateway_only_enqueue),
        )
        .route(
            "/v1/ingest/batch",
            axum::routing::post(gateway::gateway_only_enqueue),
        )
        .route(
            "/v1/ingest/ndjson",
            axum::routing::post(gateway::gateway_only_ndjson),
        )
        .route_layer(axum::middleware::from_fn(admission::buffer_gateway_body))
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            gateway::gateway_only_auth,
        ));
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(gateway::gateway_only_ready))
        .route("/v1/gateway/status", get(gateway::gateway_only_status))
        .merge(protected)
        .layer(DefaultBodyLimit::max(ingest::MAX_BATCH_BYTES))
        .with_state(shared)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        protocol: broker_proto::PROTOCOL_VERSION,
    })
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use broker_dispatch::DispatchConfig;
    use broker_partition::BrokerConfig;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tower::ServiceExt;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_state(dir: &tempfile::TempDir) -> AppState {
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let schedule = ScheduleQueue::open(dir.path()).unwrap();
        let crons = CronRegistry::open(dir.path()).unwrap();
        let dispatch = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        let catalog_tombstones = CatalogTombstones::open(dir.path()).expect("catalog tombstones");
        AppState {
            broker,
            schedule,
            crons,
            dispatch,
            leases: LeaseTable::new(),
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones,
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        }
    }

    #[tokio::test]
    async fn destination_controls_report_only_confirmed_serving_node() {
        let dir = tempdir().unwrap();
        let state = Arc::new(test_state(&dir));
        let axum::Json(blocked) = ops::block_host(
            axum::extract::State(state.clone()),
            axum::Json(ops::BlockHostRequest {
                host: "https://example.com/hook".into(),
                duration_ms: Some(60_000),
            }),
        )
        .await
        .unwrap();
        let body = serde_json::to_value(blocked).unwrap();
        assert_eq!(body["scope"], "serving_process");
        assert_eq!(
            body["applied_nodes"],
            serde_json::json!([state.dispatch.process_id()])
        );
        let axum::Json(list) = ops::list_blocked_hosts(axum::extract::State(state.clone())).await;
        assert_eq!(list.hosts.len(), 1);
        assert_eq!(list.scope.applied_nodes.len(), 1);
        let axum::Json(unblocked) = ops::unblock_host(
            axum::extract::State(state.clone()),
            axum::Json(ops::UnblockHostRequest {
                host: "https://example.com".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            unblocked.scope.applied_nodes,
            vec![state.dispatch.process_id()]
        );
        assert!(state.dispatch.host_blocker().blocked_hosts().is_empty());
        state.dispatch.shutdown_background().await;
    }

    #[tokio::test]
    async fn node_metadata_update_preserves_saved_listener() {
        let dir = tempdir().unwrap();
        let mut cfg = broker_config::BetterMqConfig::template_single_local();
        cfg.node.listen = "127.0.0.1:9123".into();
        broker_config::save_managed_config(dir.path(), &cfg).unwrap();
        let body = serde_json::from_value(serde_json::json!({
            "name": "renamed", "public_url": "http://localhost:9123"
        }))
        .unwrap();
        let _ = infra::infra_update_node(
            axum::extract::State(Arc::new(test_state(&dir))),
            axum::Json(body),
        )
        .await
        .unwrap();
        let saved = broker_config::load_managed_config(dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(saved.node.listen, "127.0.0.1:9123");
        assert_eq!(saved.node.name, "renamed");
    }

    #[tokio::test]
    async fn cluster_create_refuses_existing_membership_and_runtime_file() {
        let dir = tempdir().unwrap();
        let state = Arc::new(test_state(&dir));
        let _ = infra::infra_cluster_create(axum::extract::State(state.clone()))
            .await
            .unwrap();
        let before = std::fs::read(broker_config::managed_config_path(dir.path())).unwrap();
        let error = infra::infra_cluster_create(axum::extract::State(state.clone()))
            .await
            .unwrap_err();
        assert!(matches!(error, routes::ApiError::Conflict(_)));
        assert_eq!(
            before,
            std::fs::read(broker_config::managed_config_path(dir.path())).unwrap()
        );
        let cfg = broker_config::BetterMqConfig::template_single_local();
        broker_config::save_managed_config(dir.path(), &cfg).unwrap();
        std::fs::write(dir.path().join("cluster-config.json"), "{}").unwrap();
        let error = infra::infra_cluster_create(axum::extract::State(state))
            .await
            .unwrap_err();
        assert!(matches!(error, routes::ApiError::Conflict(_)));
    }

    #[tokio::test]
    async fn live_storage_switch_is_rejected_without_changing_config() {
        let dir = tempdir().unwrap();
        let cfg = broker_config::BetterMqConfig::template_single_local();
        broker_config::save_managed_config(dir.path(), &cfg).unwrap();
        let before = std::fs::read(broker_config::managed_config_path(dir.path())).unwrap();
        let body = serde_json::from_value(serde_json::json!({
            "mode": "slate", "s3": { "endpoint": "https://s3.example.com", "bucket": "wal", "payloadBucket": "payload", "accessKey": "access", "secretKey": "secret", "region": "auto" }
        })).unwrap();
        let state = Arc::new(test_state(&dir));
        let error =
            infra::infra_update_storage(axum::extract::State(state.clone()), axum::Json(body))
                .await
                .unwrap_err();
        assert!(matches!(error, routes::ApiError::Conflict(_)));
        assert_eq!(
            before,
            std::fs::read(broker_config::managed_config_path(dir.path())).unwrap()
        );
        let body = serde_json::from_value(serde_json::json!({ "mode": "local" })).unwrap();
        let _ = infra::infra_update_storage(axum::extract::State(state), axum::Json(body))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn inspection_lists_page_metadata_and_read_maintained_counts() {
        async fn get(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap(),
            )
            .unwrap();
            (status, value)
        }
        let dir = tempdir().unwrap();
        let state = Arc::new(test_state(&dir));
        for key in ["a", "b", "c"] {
            let dead: broker_partition::PublishRequest =
                serde_json::from_value(serde_json::json!({
                    "topic": "jobs.__dlq", "routing_key": key,
                    "payload": { "body": "hidden payload", "reason": "retries exhausted" }
                }))
                .unwrap();
            state.broker.publish(dead).unwrap();
            let delayed = serde_json::from_value(serde_json::json!({ "payload": "delayed payload", "url": "https://example.com/hook", "secret": "s", "delay_ms": 60000 })).unwrap();
            ingest::submit_one(&state, delayed, None).await.unwrap();
        }
        let app = bettermq::bettermq_routes().with_state(state);
        let (_, counts) = get(&app, "/v1/inspection/counts").await;
        let spec: serde_json::Value = serde_json::from_str(include_str!(
            "../../broker-server/openapi/bettermq.openapi.json"
        ))
        .unwrap();
        assert_contract_fields(
            &spec,
            &spec["components"]["schemas"]["InspectionCounts"],
            &counts,
        );
        assert_eq!(counts["delayed"], 3);
        assert_eq!(counts["dlq"], 3);
        assert_eq!(counts["dlq_ready"], true);
        let (_, delayed) = get(&app, "/v1/delayed?limit=2").await;
        assert_contract_fields(
            &spec,
            &spec["components"]["schemas"]["DelayedListResponse"],
            &delayed,
        );
        assert_eq!(delayed["delayed"].as_array().unwrap().len(), 2);
        assert!(delayed["delayed"][0].get("body").is_none());
        let (_, delayed_next) = get(
            &app,
            &format!(
                "/v1/delayed?limit=2&cursor={}",
                delayed["next_cursor"].as_str().unwrap()
            ),
        )
        .await;
        assert_eq!(delayed_next["delayed"].as_array().unwrap().len(), 1);
        assert!(delayed_next["next_cursor"].is_null());
        assert_eq!(
            get(&app, "/v1/delayed?cursor=!").await.0,
            StatusCode::BAD_REQUEST
        );

        let mut path = "/v1/dlq?dlq_topic=jobs.__dlq&limit=1".to_string();
        let mut ids = std::collections::HashSet::new();
        let mut detail_position = None;
        loop {
            let (status, page) = get(&app, &path).await;
            assert_eq!(status, StatusCode::OK);
            assert_contract_fields(
                &spec,
                &spec["components"]["schemas"]["DlqListResponse"],
                &page,
            );
            let rows = page["messages"].as_array().unwrap();
            assert!(rows.len() <= 1);
            for row in rows {
                assert!(row.get("body").is_none());
                assert_eq!(row["reason"], "retries exhausted");
                assert!(ids.insert(row["message_id"].as_str().unwrap().to_string()));
                detail_position = Some((
                    row["partition"].as_u64().unwrap(),
                    row["offset"].as_u64().unwrap(),
                ));
            }
            let Some(cursor) = page["next_cursor"].as_str() else {
                break;
            };
            assert_eq!(
                get(
                    &app,
                    &format!("/v1/dlq?dlq_topic=other.__dlq&cursor={cursor}")
                )
                .await
                .0,
                StatusCode::BAD_REQUEST
            );
            path = format!("/v1/dlq?dlq_topic=jobs.__dlq&limit=1&cursor={cursor}");
        }
        assert_eq!(ids.len(), 3);
        let (partition, offset) = detail_position.unwrap();
        let (_, detail) = get(
            &app,
            &format!("/v1/dlq/message?dlq_topic=jobs.__dlq&partition={partition}&offset={offset}"),
        )
        .await;
        assert_eq!(detail["body"], "hidden payload");
        let response = app
            .clone()
            .oneshot(
                Request::delete("/v1/dlq?dlq_topic=jobs.__dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(get(&app, "/v1/inspection/counts").await.1["dlq"], 0);
    }

    #[tokio::test]
    async fn storage_api_rejects_slate_on_v2_without_saving() {
        let dir = tempdir().unwrap();
        let layout = broker_partition::ShardLayout {
            version: broker_partition::LAYOUT_V2,
            shard_count: 1,
            route_hash_version: broker_proto::HASH_VERSION_V2,
        };
        std::fs::write(
            broker_partition::ShardLayout::path(dir.path()),
            serde_json::to_vec(&layout).unwrap(),
        )
        .unwrap();
        let state = Arc::new(test_state(&dir));
        let result = infra::infra_update_storage(
            axum::extract::State(state),
            Json(infra::StorageUpdateRequest {
                mode: "slate".into(),
                s3: None,
            }),
        )
        .await;
        assert!(
            matches!(result, Err(routes::ApiError::BadRequest(message)) if message.contains("physical shard layout V2"))
        );
        assert!(!broker_config::managed_config_path(dir.path()).exists());
    }

    #[tokio::test]
    async fn missing_bearer_is_401_when_auth_configured() {
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/queues")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    fn assert_contract_fields(
        spec: &serde_json::Value,
        schema: &serde_json::Value,
        value: &serde_json::Value,
    ) {
        if let Some(reference) = schema["$ref"].as_str() {
            return assert_contract_fields(
                spec,
                spec.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
                value,
            );
        }
        if let Some(object) = value.as_object() {
            let properties = schema["properties"]
                .as_object()
                .expect("object schema properties");
            if let Some(required) = schema["required"].as_array() {
                for field in required {
                    assert!(
                        object.contains_key(field.as_str().unwrap()),
                        "missing {field}"
                    );
                }
            }
            for (key, value) in object {
                assert!(properties.contains_key(key), "undocumented field {key}");
                if !value.is_null() {
                    assert_contract_fields(spec, &properties[key], value);
                }
            }
        } else if let Some(array) = value.as_array() {
            for item in array {
                assert_contract_fields(spec, &schema["items"], item);
            }
        } else {
            match schema["type"].as_str() {
                Some("integer") => assert!(value.is_i64() || value.is_u64()),
                Some("boolean") => assert!(value.is_boolean()),
                Some("string") => assert!(value.is_string()),
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn partial_ingest_router_responses_match_openapi() {
        let spec: serde_json::Value = serde_json::from_str(include_str!(
            "../../broker-server/openapi/bettermq.openapi.json"
        ))
        .unwrap();
        let dir = tempdir().unwrap();
        let state = test_state(&dir);
        let group = state.broker.create_group("contract".into()).unwrap();
        for (name, url) in [
            ("valid", "https://example.com/hook"),
            ("invalid", "not-a-url"),
        ] {
            state
                .broker
                .add_group_member(
                    group.id,
                    name.into(),
                    url.into(),
                    "s".into(),
                    1,
                    0,
                    60,
                    None,
                )
                .unwrap();
        }
        // Mount the real handlers without auth to isolate the wire contract.
        let app = Router::new()
            .route(
                "/v1/enqueue/batch",
                axum::routing::post(batch::batch_enqueue),
            )
            .merge(bettermq::bettermq_routes())
            .with_state(Arc::new(state));
        let bodies = [
            (
                "/v1/enqueue/batch",
                serde_json::json!({"messages": [
                    {"payload":"ok", "url":"https://example.com/hook", "secret":"s", "delay_ms":60000},
                    {"payload":"bad", "topic":"missing"}
                ]}),
            ),
            (
                "/v1/publish",
                serde_json::json!({"group_id": group.id, "body":"body"}),
            ),
        ];
        for (path, body) in bodies {
            let response = app
                .clone()
                .oneshot(
                    Request::post(path)
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::MULTI_STATUS);
            let body: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(body["partial"], true);
            let schema = &spec["paths"][path]["post"]["responses"]["207"]["content"]
                ["application/json"]["schema"];
            assert_contract_fields(&spec, schema, &body);
        }
        for path in [
            "/v1/enqueue/batch",
            "/v1/ingest/batch",
            "/v1/ingest/ndjson",
            "/v1/gateway/enqueue",
        ] {
            assert!(spec["paths"][path]["post"]["responses"]["207"].is_object());
        }
    }

    #[tokio::test]
    async fn regenerated_token_revokes_the_old_one_immediately() {
        let dir = tempdir().unwrap();
        let store = Arc::new(broker_local_auth::LocalAuthStore::open(dir.path()).unwrap());
        let old = store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(store.clone());
        let app = router(state);
        let get = |token: String| {
            let app = app.clone();
            async move {
                app.oneshot(
                    Request::builder()
                        .uri("/v1/queues")
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            }
        };
        assert_eq!(get(old.clone()).await, StatusCode::OK);
        assert_eq!(get("x".repeat(64 * 1024)).await, StatusCode::UNAUTHORIZED);
        let new = store.regenerate("password1234").unwrap();
        assert_eq!(get(old).await, StatusCode::UNAUTHORIZED);
        assert_eq!(get(new).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_local_auth_is_401_fail_closed() {
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/publish")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn setup_outside_window_is_401() {
        let _guard = ENV_LOCK.lock().unwrap();
        close_setup_window();
        std::env::remove_var("BETTERMQ_ALLOW_OPEN_SETUP");
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/local-auth/setup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"password":"password1234"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    async fn post_setup(app: Router, body: serde_json::Value) -> StatusCode {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/local-auth/setup")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
    }

    #[tokio::test]
    async fn setup_during_open_window_requires_the_setup_code() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BETTERMQ_ALLOW_OPEN_SETUP");
        std::env::remove_var("BETTERMQ_SETUP_CODE");
        let code = open_setup_window(std::time::Duration::from_secs(60));
        let dir = tempdir().unwrap();
        let store = Arc::new(broker_local_auth::LocalAuthStore::open(dir.path()).unwrap());
        let mut state = test_state(&dir);
        state.local_auth = Some(store.clone());
        let app = router(state);
        let missing = post_setup(
            app.clone(),
            serde_json::json!({ "password": "password1234" }),
        )
        .await;
        let wrong = post_setup(
            app.clone(),
            serde_json::json!({ "password": "password1234", "setup_code": "0000-0000-0000-0000" }),
        )
        .await;
        let configured_after_bad = store.is_configured();
        // Codes are case- and dash-insensitive so they survive retyping.
        let typed = code.replace('-', "").to_ascii_lowercase();
        let ok = post_setup(
            app,
            serde_json::json!({ "password": "password1234", "setup_code": typed }),
        )
        .await;
        close_setup_window();
        assert_eq!(missing, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong, StatusCode::UNAUTHORIZED);
        assert!(
            !configured_after_bad,
            "a caller without the code must not claim the broker"
        );
        assert_eq!(ok, StatusCode::OK);
        assert!(store.is_configured());
    }

    #[test]
    fn setup_codes_are_random_and_readable() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BETTERMQ_SETUP_CODE");
        let a = open_setup_window(std::time::Duration::from_secs(60));
        let b = open_setup_window(std::time::Duration::from_secs(60));
        std::env::set_var("BETTERMQ_SETUP_CODE", "  preset-code-1234 ");
        let preset = open_setup_window(std::time::Duration::from_secs(60));
        std::env::remove_var("BETTERMQ_SETUP_CODE");
        close_setup_window();
        assert_ne!(a, b);
        assert_eq!(a.len(), 19);
        assert!(a
            .chars()
            .all(|c| c == '-' || "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)));
        assert_eq!(preset, "preset-code-1234");
    }

    #[tokio::test]
    async fn internal_replicate_without_cluster_secret_is_401() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/replicate")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn replicate_topic_mismatch_is_400() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "test-cluster-secret");
        let dir = tempdir().unwrap();
        let app = router(test_state(&dir));
        let header = broker_proto::LogRecord {
            id: uuid::Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 1,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        };
        let mut frame = Vec::new();
        broker_proto::encode_frame(&header, b"hi", &mut frame).unwrap();
        let body = serde_json::json!({
            "tenant_id": "default",
            "topic": "other-topic",
            "partition": 0,
            "offset": 0,
            "frame_b64": base64::engine::general_purpose::STANDARD.encode(&frame),
            "leader_generation": 1
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/replicate")
                    .header("content-type", "application/json")
                    .header("x-bettermq-cluster-secret", "test-cluster-secret")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    async fn admin_call(
        app: Router,
        method: &str,
        uri: &str,
        bearer: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> StatusCode {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(token) = bearer {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let req = match body {
            Some(body) => req
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap();
        app.oneshot(req).await.unwrap().status()
    }

    fn embedded_admin(dir: &tempfile::TempDir, configure: bool) -> (AdminState, Option<String>) {
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let token = configure.then(|| store.setup("password1234").unwrap());
        let mut state = test_state(dir);
        state.local_auth = Some(Arc::new(store));
        let mut admin = AdminState::local(Arc::new(state));
        admin.cluster_secret = None;
        (admin, token)
    }

    #[tokio::test]
    async fn embedded_admin_is_401_before_local_auth_setup() {
        let dir = tempdir().unwrap();
        let (admin, _) = embedded_admin(&dir, false);
        let app = admin_router(admin);
        for (method, uri, body) in [
            ("GET", "/admin/v1/cells", None),
            ("GET", "/admin/v1/cluster", None),
            (
                "POST",
                "/admin/v1/cells",
                Some(serde_json::json!({
                    "id": "x", "region": "x", "controllerUrl": "http://attacker.example"
                })),
            ),
            (
                "POST",
                "/admin/v1/gc/0",
                Some(serde_json::json!({ "watermark": 1_000_000 })),
            ),
        ] {
            assert_eq!(
                admin_call(app.clone(), method, uri, None, body).await,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must fail closed before setup"
            );
        }
        assert_eq!(
            admin_call(app, "GET", "/admin/v1/health", None, None).await,
            StatusCode::OK,
            "health stays public"
        );
    }

    #[tokio::test]
    async fn embedded_admin_requires_local_token_or_cluster_secret() {
        let dir = tempdir().unwrap();
        let (mut admin, token) = embedded_admin(&dir, true);
        let token = token.unwrap();
        admin.cluster_secret = Some("cluster-secret".into());
        let app = admin_router(admin);
        assert_eq!(
            admin_call(app.clone(), "GET", "/admin/v1/cells", None, None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_call(
                app.clone(),
                "GET",
                "/admin/v1/cells",
                Some("sk_local_wrong"),
                None
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_call(app.clone(), "GET", "/admin/v1/cells", Some(&token), None).await,
            StatusCode::OK
        );
        assert_eq!(
            admin_call(app, "GET", "/admin/v1/cells", Some("cluster-secret"), None).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn embedded_admin_without_local_store_is_401() {
        let dir = tempdir().unwrap();
        let mut admin = AdminState::local(Arc::new(test_state(&dir)));
        admin.cluster_secret = None;
        let app = admin_router(admin);
        assert_eq!(
            admin_call(app, "GET", "/admin/v1/cells", Some("anything"), None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn standalone_admin_fails_closed_without_cluster_secret() {
        let mut admin = AdminState::remote("http://127.0.0.1:9".into(), CellRegistry::default());
        admin.cluster_secret = None;
        let app = admin_router(admin);
        assert_eq!(
            admin_call(app.clone(), "GET", "/admin/v1/cells", None, None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_call(
                app,
                "POST",
                "/admin/v1/cells",
                None,
                Some(serde_json::json!({
                    "id": "x", "region": "x", "controllerUrl": "http://attacker.example"
                })),
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn standalone_admin_accepts_only_the_cluster_secret() {
        let mut admin = AdminState::remote("http://127.0.0.1:9".into(), CellRegistry::default());
        admin.cluster_secret = Some("cluster-secret".into());
        let app = admin_router(admin);
        assert_eq!(
            admin_call(app.clone(), "GET", "/admin/v1/cells", Some("nope"), None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_call(
                app.clone(),
                "GET",
                "/admin/v1/cells",
                Some("cluster-secret"),
                None
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            admin_call(
                app,
                "POST",
                "/admin/v1/cells",
                Some("cluster-secret"),
                Some(serde_json::json!({
                    "id": "x", "region": "x", "controllerUrl": "http://169.254.169.254/latest?x="
                })),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "link-local controller URLs are rejected"
        );
    }

    #[tokio::test]
    async fn federated_query_uses_only_the_cell_credential() {
        use axum::routing::get;
        let seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
        let record = seen.clone();
        let probe = Router::new().route(
            "/admin/v1/nodes",
            get(move |headers: axum::http::HeaderMap| {
                let record = record.clone();
                async move {
                    record.lock().unwrap().push(
                        headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string),
                    );
                    Json(serde_json::json!({ "nodes": [] }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, probe).await.unwrap() });

        let mut registry = CellRegistry::default();
        for (id, admin_token) in [("with-token", Some("sk_local_cell")), ("no-token", None)] {
            registry.cells.push(CellRecord {
                id: id.into(),
                region: "r".into(),
                controller_url: format!("http://{addr}"),
                label: None,
                admin_token: admin_token.map(str::to_string),
            });
        }
        let mut admin = AdminState::remote("http://127.0.0.1:9".into(), registry);
        admin.cluster_secret = Some("cluster-secret".into());
        let response = admin_router(admin)
            .oneshot(
                Request::builder()
                    .uri("/admin/v1/cells/query/nodes")
                    .header("authorization", "Bearer cluster-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"][0]["cell"], "with-token");
        assert_eq!(body["failed"][0]["cell"], "no-token");
        assert_eq!(body["failed"][0]["error"], "cell credential not configured");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[Some("Bearer sk_local_cell".to_string())],
            "only the cell's own token is sent; never the caller's bearer"
        );
    }

    async fn admin_json(
        app: Router,
        method: &str,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", "Bearer cluster-secret")
            .header("content-type", "application/json");
        let req = req
            .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
            .unwrap();
        let response = app.oneshot(req).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    #[tokio::test]
    async fn cell_admin_token_is_required_kept_and_redacted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cell-registry.json");
        let mut admin = AdminState::remote_with_path(
            "http://127.0.0.1:9".into(),
            CellRegistry::default(),
            Some(path.clone()),
        );
        admin.cluster_secret = Some("cluster-secret".into());
        let app = admin_router(admin);
        let cell = |token: Option<&str>| {
            let mut body = serde_json::json!({
                "id": "eu", "region": "eu", "controllerUrl": "http://10.0.0.9:8080"
            });
            if let Some(token) = token {
                body["adminToken"] = token.into();
            }
            body
        };
        let (status, _) =
            admin_json(app.clone(), "POST", "/admin/v1/cells", Some(cell(None))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "new cell needs a token");
        let (status, _) = admin_json(
            app.clone(),
            "POST",
            "/admin/v1/cells",
            Some(cell(Some("not-a-local-token"))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, body) = admin_json(
            app.clone(),
            "POST",
            "/admin/v1/cells",
            Some(cell(Some("sk_local_eu"))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cells"][0]["adminTokenConfigured"], true);
        assert!(!body.to_string().contains("sk_local_eu"));
        let (status, body) = admin_json(
            app.clone(),
            "POST",
            "/admin/v1/cells",
            Some(cell(Some(" "))),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "blank token on update keeps the saved one"
        );
        assert_eq!(body["cells"][0]["adminTokenConfigured"], true);
        let (_, listed) = admin_json(app, "GET", "/admin/v1/cells", None).await;
        assert_eq!(listed["cells"][0]["adminTokenConfigured"], true);
        assert!(!listed.to_string().contains("sk_local_eu"));
        let on_disk = CellRegistry::load(&path);
        assert_eq!(on_disk.cells[0].admin_token.as_deref(), Some("sk_local_eu"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    fn enroll_body(seed: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "seed_url": seed,
            "join_token": "join_x",
            "public_url": "http://127.0.0.1:1",
            "node_name": "victim",
        }))
        .unwrap()
    }

    async fn enroll(
        app: Router,
        body: Vec<u8>,
        proof: Option<String>,
        bearer: Option<&str>,
    ) -> StatusCode {
        let mut req = Request::builder()
            .method("POST")
            .uri("/v1/infra/cluster/enroll")
            .header("content-type", "application/json");
        if let Some(proof) = proof {
            req = req.header(cluster_auth::ENROLL_PROOF_HEADER, proof);
        }
        if let Some(token) = bearer {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        app.oneshot(req.body(Body::from(body)).unwrap())
            .await
            .unwrap()
            .status()
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    /// Fake seed that answers bootstrap/register like an attacker would and
    /// counts every request that carries the cluster-secret header.
    async fn fake_seed(
        proof_secret: Option<&'static str>,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use axum::routing::{get, post};
        let leaks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let leak_counter = leaks.clone();
        let count_leak = axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let leaks = leak_counter.clone();
                async move {
                    if req
                        .headers()
                        .contains_key(cluster_auth::CLUSTER_SECRET_HEADER)
                    {
                        leaks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    next.run(req).await
                }
            },
        );
        let app = Router::new()
            .route(
                "/v1/infra/join/bootstrap",
                get(move |headers: axum::http::HeaderMap| async move {
                    let mut out = axum::http::HeaderMap::new();
                    if let (Some(secret), Some(challenge)) = (
                        proof_secret,
                        headers
                            .get(cluster_auth::CLUSTER_CHALLENGE_HEADER)
                            .and_then(|v| v.to_str().ok()),
                    ) {
                        out.insert(
                            cluster_auth::CLUSTER_PROOF_HEADER,
                            cluster_auth::seed_proof(secret, challenge).unwrap().parse().unwrap(),
                        );
                    }
                    (
                        out,
                        Json(serde_json::json!({
                            "cluster_id": uuid::Uuid::new_v4(),
                            "generation": 1,
                            "nodes": [],
                        })),
                    )
                }),
            )
            .route(
                "/v1/infra/cluster/register",
                post(|| async {
                    Json(serde_json::json!({
                        "cluster_id": uuid::Uuid::new_v4(),
                        "generation": 1,
                        "nodes": [
                            { "name": "evil", "public_url": "http://127.0.0.1:2", "id": uuid::Uuid::new_v4() },
                            { "name": "victim", "public_url": "http://127.0.0.1:1", "id": uuid::Uuid::new_v4() },
                        ],
                    }))
                }),
            )
            .fallback(|| async { StatusCode::NOT_FOUND })
            .layer(count_leak);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), leaks)
    }

    fn joined_a_cluster(dir: &tempfile::TempDir) -> bool {
        broker_config::load_managed_config(dir.path())
            .unwrap()
            .and_then(|cfg| cfg.cluster)
            .is_some_and(|c| c.enabled)
    }

    #[tokio::test]
    async fn enroll_requires_proof_or_token() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "enroll-secret");
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let body = enroll_body("http://127.0.0.1:9");
        let no_proof = enroll(app.clone(), body.clone(), None, None).await;
        let wrong_secret = enroll(
            app.clone(),
            body.clone(),
            Some(cluster_auth::sign_enroll("not-the-secret", now_ms(), &body)),
            None,
        )
        .await;
        let other_body = enroll(
            app.clone(),
            body.clone(),
            Some(cluster_auth::sign_enroll("enroll-secret", now_ms(), b"{}")),
            None,
        )
        .await;
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        let no_secret = enroll(
            app,
            body.clone(),
            Some(cluster_auth::sign_enroll("enroll-secret", now_ms(), &body)),
            None,
        )
        .await;
        assert_eq!(no_proof, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong_secret, StatusCode::UNAUTHORIZED);
        assert_eq!(other_body, StatusCode::UNAUTHORIZED);
        assert_eq!(no_secret, StatusCode::UNAUTHORIZED);
        assert!(!joined_a_cluster(&dir));
    }

    #[tokio::test]
    async fn enroll_on_configured_node_requires_its_api_token() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "enroll-secret");
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let body = enroll_body("http://127.0.0.1:9");
        let status = enroll(
            app,
            body.clone(),
            Some(cluster_auth::sign_enroll("enroll-secret", now_ms(), &body)),
            None,
        )
        .await;
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn join_rejects_seed_that_cannot_prove_the_cluster_secret() {
        let _guard = ENV_LOCK.lock().unwrap();
        for seed_secret in [None, Some("attacker-guess")] {
            std::env::set_var("BETTERMQ_CLUSTER_SECRET", "enroll-secret");
            let (seed, leaks) = fake_seed(seed_secret).await;
            let dir = tempdir().unwrap();
            let app = router(test_state(&dir));
            let body = enroll_body(&seed);
            let status = enroll(
                app,
                body.clone(),
                Some(cluster_auth::sign_enroll("enroll-secret", now_ms(), &body)),
                None,
            )
            .await;
            std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "seed secret {seed_secret:?}"
            );
            assert_eq!(leaks.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(!joined_a_cluster(&dir), "membership must not be rewritten");
        }
    }

    #[tokio::test]
    async fn join_refuses_a_node_already_in_a_cluster() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "enroll-secret");
        let (seed, _) = fake_seed(Some("enroll-secret")).await;
        let dir = tempdir().unwrap();
        let mut cfg = broker_config::BetterMqConfig::template_single_local();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.cluster = Some(broker_config::ClusterConfigSection {
            enabled: true,
            id: Some(uuid::Uuid::new_v4().to_string()),
            nodes: vec![broker_config::ClusterNode {
                name: cfg.node.name.clone(),
                public_url: cfg.node.public_url.clone(),
                internal_url: None,
            }],
            shared_meta_dir: None,
        });
        broker_config::save_managed_config(dir.path(), &cfg).unwrap();
        let app = router(test_state(&dir));
        let body = enroll_body(&seed);
        let status = enroll(
            app,
            body.clone(),
            Some(cluster_auth::sign_enroll("enroll-secret", now_ms(), &body)),
            None,
        )
        .await;
        std::env::remove_var("BETTERMQ_CLUSTER_SECRET");
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn publish_with_flow_and_no_flow_id_does_not_panic() {
        let dir = tempdir().unwrap();
        let store = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
        let token = store.setup("password1234").unwrap();
        let mut state = test_state(&dir);
        state.local_auth = Some(Arc::new(store));
        let app = router(state);
        let body = serde_json::json!({
            "url": "https://example.com/hook",
            "secret": "whsec_test",
            "body": "hi",
            "flow": { "key": "k", "parallelism": 1 }
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/publish")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}
