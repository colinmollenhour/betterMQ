//! An accepted message stays dead-letterable after BETTERMQ_MAX_HTTP_BODY_BYTES
//! is lowered below its size. Own test binary: it changes the environment.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use broker_proto::{RetryBackoff, RetryBackoffKind, RetryDefaults};
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BODY: usize = 200 * 1024;

#[tokio::test]
async fn accepted_body_above_a_lowered_limit_still_moves_to_dlq() {
    std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    std::env::remove_var("BETTERMQ_MAX_HTTP_BODY_BYTES");
    let dir = tempdir().unwrap();
    let no_retries = RetryDefaults {
        max_retries: 0,
        backoff: RetryBackoff {
            kind: RetryBackoffKind::Fixed,
            initial_ms: 5,
            max_ms: 5,
            multiplier: 1.0,
        },
    };
    let mut cfg = BrokerConfig::new(dir.path().to_path_buf());
    cfg.retry_defaults = no_retries.clone();
    let broker = Broker::open(cfg).unwrap();
    let dispatch = DispatchEngine::new(
        broker.clone(),
        DispatchConfig {
            retry_defaults: no_retries,
            ..DispatchConfig::default()
        },
    );

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/fail"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock)
        .await;
    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "jobs".into(),
            url: format!("{}/fail", mock.uri()),
            secret: "whsec_test".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let body = "x".repeat(BODY);
    let resp = broker
        .publish(PublishRequest {
            routing_id: None,
            topic: "jobs".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: body.clone(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: None,
            secret: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: Some(0),
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        })
        .unwrap();
    broker.flush_wal().unwrap();

    // The operator lowers the ingest limit after the message was accepted.
    std::env::set_var("BETTERMQ_MAX_HTTP_BODY_BYTES", "65536");
    dispatch.enqueue(broker_dispatch::DeliveryJob::live(
        "jobs",
        resp.partition.unwrap(),
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));

    let mut dlq = Vec::new();
    for _ in 0..100 {
        dlq = broker.list_topic_messages("jobs.__dlq", 10).unwrap();
        if !dlq.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    std::env::remove_var("BETTERMQ_MAX_HTTP_BODY_BYTES");
    assert_eq!(
        dlq.len(),
        1,
        "dead-lettering failed: {:?}",
        dispatch.telemetry_snapshot()
    );
    let mut message = dlq.remove(0);
    broker.hydrate_message_payload(&mut message).unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&message.payload).unwrap();
    assert_eq!(envelope["body"].as_str().unwrap().len(), BODY);
    assert_eq!(dispatch.telemetry_snapshot().dlq_failures, 0);
}
