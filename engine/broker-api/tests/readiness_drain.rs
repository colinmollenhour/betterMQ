//! Separate test process: shutdown admission is deliberately irreversible.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use broker_api::{AppState, CatalogTombstones};
use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
use broker_partition::{Broker, BrokerConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn readiness_turns_unhealthy_before_http_drain_on_split_listeners() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().into())).unwrap();
    let auth = broker_local_auth::LocalAuthStore::open(dir.path()).unwrap();
    auth.setup("test-password").unwrap();
    let state = AppState {
        broker: broker.clone(),
        schedule: ScheduleQueue::open(dir.path()).unwrap(),
        crons: CronRegistry::open(dir.path()).unwrap(),
        dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
        leases: LeaseTable::new(),
        cluster: None,
        local_auth: Some(Arc::new(auth)),
        fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
        catalog_tombstones: CatalogTombstones::open(dir.path()).unwrap(),
        dispatch_fleet: false,
        broker_only: false,
        #[cfg(feature = "cloud")]
        auth: None,
        #[cfg(feature = "cloud")]
        control_plane: None,
    };
    let public = broker_api::public_router(state.clone());
    // A separately mounted public/admin probe remains live until task teardown.
    let separate = broker_api::data_plane_router_no_spawn(state);
    for router in [&public, &separate] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    // This is the exact admission transition the SIGTERM handler invokes,
    // before the HTTP server starts its graceful shutdown future.
    broker_api::begin_shutdown();
    for router in [public, separate] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["draining"], true);
        assert_eq!(body["ready"], false);
        let live = router
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
    }
}
