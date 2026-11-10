//! Webhook delivery with HMAC signature.

use broker_dispatch::{DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Once;
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Recompute the signature the receiver would check, independently of
/// `broker_dispatch::sign_payload`.
fn expected_signature(secret: &str, timestamp_ms: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(timestamp_ms.as_bytes());
    mac.update(b".");
    mac.update(body);
    format!(
        "t={timestamp_ms},v1={}",
        hex::encode(mac.finalize().into_bytes())
    )
}

fn allow_wiremock_localhost() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    });
}

#[tokio::test]
async fn publish_triggers_webhook_with_signature() {
    allow_wiremock_localhost();
    let dir = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let dispatch = DispatchEngine::new(
        broker.clone(),
        DispatchConfig {
            retry_defaults: broker_proto::RetryDefaults {
                max_retries: 2,
                backoff: broker_proto::RetryBackoff {
                    initial_ms: 50,
                    max_ms: 200,
                    ..broker_proto::RetryBackoff::default()
                },
            },
            ..DispatchConfig::default()
        },
    );

    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(header_exists("bettermq-signature"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    broker
        .create_subscription(CreateSubscriptionRequest {
            topic: "hooks".into(),
            url: format!("{}/hook", mock.uri()),
            secret: "whsec_test".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let resp = broker
        .publish(PublishRequest {
            topic: "hooks".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "rk".into(),
            payload: "ping".into(),
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
            max_retries: Some(2),
            retry_backoff: None,
            method: None,
            headers: None,
            sign: Some(true),
            request: None,
        })
        .unwrap();
    broker
        .wait_committed(&resp.topic, resp.partition.unwrap(), resp.offset.unwrap())
        .await
        .unwrap();

    dispatch.enqueue(broker_dispatch::DeliveryJob::live(
        "hooks",
        resp.partition.unwrap(),
        resp.offset.unwrap(),
        resp.message_id.unwrap(),
    ));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let request = loop {
        if let Some(request) = mock.received_requests().await.unwrap().into_iter().next() {
            break request;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "webhook was not delivered"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(request.body, b"ping");
    let signature = request.headers["bettermq-signature"].to_str().unwrap();
    let timestamp = request.headers["bettermq-timestamp"].to_str().unwrap();
    assert_eq!(
        signature,
        expected_signature("whsec_test", timestamp, &request.body)
    );
    assert_ne!(
        signature,
        expected_signature("wrong-secret", timestamp, &request.body)
    );
    mock.verify().await;
}
