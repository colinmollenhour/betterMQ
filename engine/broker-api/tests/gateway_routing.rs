//! Use the real internal gateway and WAL to verify routing/storage invariants.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use broker_api::{AppState, CatalogTombstones};
use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn gateway_routes_queue_id_only_and_raw_url_to_the_storage_topic() {
    std::env::set_var("BETTERMQ_SHARD_LAYOUT", "v1");
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().into())).unwrap();
    let queue = broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "canonical-queue".into(),
            url: "https://example.com/hook".into(),
            secret: "delivery-secret".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let node_id = Uuid::new_v4();
    let runtime = ClusterRuntime::from_config_only(ClusterConfig {
        cluster_id: Uuid::new_v4(),
        node_id,
        nodes: vec![NodeConfig {
            id: node_id,
            addr: base.clone(),
            internal_url: None,
        }],
        generation: 1,
        hash_version: broker_proto::HASH_VERSION,
    });
    runtime.record_self_alive(chrono::Utc::now().timestamp_millis());
    let state = AppState {
        broker: broker.clone(),
        schedule: ScheduleQueue::open(dir.path()).unwrap(),
        crons: CronRegistry::open(dir.path()).unwrap(),
        dispatch: DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default()),
        leases: LeaseTable::new(),
        cluster: Some(broker_api::Cluster::new(runtime)),
        local_auth: None,
        fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
        catalog_tombstones: CatalogTombstones::open(dir.path()).unwrap(),
        dispatch_fleet: false,
        broker_only: true,
        #[cfg(feature = "cloud")]
        auth: None,
        #[cfg(feature = "cloud")]
        control_plane: None,
    };
    // This integration-test binary owns its environment and has only this test.
    std::env::set_var("BETTERMQ_CLUSTER_SECRET", "routing-integration-secret");
    std::env::set_var("BETTERMQ_BROKER_URLS", &base);
    std::env::set_var("BETTERMQ_GATEWAY_TOKEN", "gateway-token");
    std::env::set_var("BETTERMQ_GATEWAY_LAYOUT_VERSION", "1");
    std::env::set_var("BETTERMQ_GATEWAY_SHARD_COUNT", "4");
    let internal = broker_api::internal_router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, internal).await.unwrap();
    });
    let gateway =
        broker_api::gateway_only_router(broker_api::GatewayOnlyState::from_env().unwrap());
    let response = gateway.clone().oneshot(Request::builder().method("POST").uri("/v1/ingest/batch")
        .header("authorization", "Bearer gateway-token").header("content-type", "application/json")
        .body(Body::from(json!({"messages":[
            {"queue_id":queue.id,"payload":"queued","idempotency_key":"queue-id-only"},
            {"topic":"not-the-stored-topic","url":"https://example.com/hook","secret":"delivery-secret","routing_key":"   ","payload":"direct","idempotency_key":"raw-url"}
        ]}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), 16384).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["accepted"], 2, "{body}");
    for (index, topic, key) in [
        (0, "canonical-queue", "queue-id-only"),
        (1, broker_partition::DIRECT_TOPIC, "raw-url"),
    ] {
        let routing_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes());
        let expected = broker.assign_shard(topic, &routing_id.to_string());
        let result = &body["results"][index];
        assert_eq!(result["topic"], topic);
        assert_eq!(result["partition"], expected);
        let messages = broker.list_topic_messages(topic, 10).unwrap();
        let message = messages
            .iter()
            .find(|message| message.id.to_string() == result["message_id"].as_str().unwrap())
            .unwrap();
        assert_eq!(message.partition, expected);
        assert!(!broker_partition::delivery_uses_flow_control(message));
    }
    let response = gateway
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/ingest/batch")
                .header("authorization", "Bearer gateway-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages":[{"queue_id":Uuid::new_v4(),"payload":"missing"}]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    server.abort();
}
