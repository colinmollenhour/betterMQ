//! Quorum-gated dispatch (decision D2): delivery waits for the quorum
//! watermark, completions are identity-aware, and takeover clamps cursors.

use broker_dispatch::{DeliveryJob, DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest, PublishResponse};
use std::sync::Once;
use std::time::Duration;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn allow_wiremock_localhost() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    });
}

fn publish(broker: &Broker, url: &str) -> PublishResponse {
    let response = broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "gate".into(),
            payload: "body".into(),
            routing_id: None,
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(url.into()),
            secret: Some("whsec_test".into()),
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
    response
}

fn job(response: &PublishResponse) -> DeliveryJob {
    DeliveryJob::live(
        response.topic.clone(),
        response.partition.unwrap(),
        response.offset.unwrap(),
        response.message_id.unwrap(),
    )
}

fn lane_of(broker: &Broker, response: &PublishResponse) -> String {
    broker_partition::flow_lane_owner(
        &broker
            .read_message(
                &response.topic,
                response.partition.unwrap(),
                response.offset.unwrap(),
            )
            .unwrap(),
    )
    .to_string()
}

async fn hook() -> MockServer {
    allow_wiremock_localhost();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    server
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_without_quorum_is_not_delivered_until_quorum_proves_it() {
    let destination = hook().await;
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    broker.enable_quorum_gating();
    // Appended and fsynced locally, but its replication failed (5xx to the producer).
    let response = publish(&broker, &format!("{}/hook", destination.uri()));
    let (topic, partition, offset) = (
        response.topic.clone(),
        response.partition.unwrap(),
        response.offset.unwrap(),
    );
    let dispatch = DispatchEngine::new(broker.clone(), DispatchConfig::default());
    dispatch.enqueue(job(&response));
    dispatch.backfill_pending();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        destination.received_requests().await.unwrap().is_empty(),
        "neither the live path nor backfill delivers above the quorum watermark"
    );
    let tenant = broker.tenant();
    let lane = lane_of(&broker, &response);
    assert_eq!(
        broker.dispatch_offset(&tenant, &lane, partition).unwrap(),
        0
    );

    broker
        .record_quorum_commit(&topic, partition, offset, offset + 1)
        .unwrap();
    for _ in 0..60 {
        if !destination.received_requests().await.unwrap().is_empty() {
            break;
        }
        dispatch.backfill_pending();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(destination.received_requests().await.unwrap().len(), 1);
    dispatch.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_marker_of_a_different_message_never_skips_delivery() {
    let destination = hook().await;
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let response = publish(&broker, &format!("{}/hook", destination.uri()));
    let partition = response.partition.unwrap();
    let offset = response.offset.unwrap();
    let tenant = broker.tenant();
    let lane = lane_of(&broker, &response);
    // An old leader completed a different record at this offset.
    broker
        .mark_dispatch_complete_for(&tenant, &lane, partition, offset, Uuid::new_v4())
        .unwrap();
    assert!(!broker
        .is_delivery_complete(
            &response.topic,
            &tenant,
            &lane,
            partition,
            offset,
            response.message_id.unwrap()
        )
        .unwrap());

    let dispatch = DispatchEngine::new(broker.clone(), DispatchConfig::default());
    dispatch.enqueue(job(&response));
    for _ in 0..50 {
        if !destination.received_requests().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        destination.received_requests().await.unwrap().len(),
        1,
        "the acknowledged record at the reused offset is delivered"
    );
    dispatch.stop();
}

#[tokio::test]
async fn takeover_clamps_cursors_to_the_verified_quorum_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    broker.enable_quorum_gating();
    let responses: Vec<_> = (0..3)
        .map(|_| publish(&broker, "https://example.com/hook"))
        .collect();
    let topic = responses[0].topic.clone();
    let partition = responses[0].partition.unwrap();
    let tenant = broker.tenant();
    let lane = lane_of(&broker, &responses[0]);
    let first = responses[0].offset.unwrap();
    broker
        .record_quorum_commit(&topic, partition, first, first + 2)
        .unwrap();
    // Shared cursor written by the previous leader past what this node proved.
    broker
        .set_dispatch_offset(&tenant, &lane, partition, first + 3)
        .unwrap();
    // A legacy marker inside the unproven tail.
    broker
        .mark_dispatch_complete(&tenant, &lane, partition, first + 2)
        .unwrap();

    let dispatch = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
    dispatch.fence_partition(partition);
    assert!(dispatch.is_fenced(partition));
    dispatch.set_recovery_targets(partition, vec![(topic.clone(), first + 3)]);
    assert!(
        dispatch.complete_recovery(partition).is_err(),
        "recovery waits until the takeover target is quorum-verified"
    );
    assert!(dispatch.is_fenced(partition));

    dispatch.set_recovery_targets(partition, vec![(topic.clone(), first + 2)]);
    assert_eq!(dispatch.complete_recovery(partition).unwrap(), 1);
    assert!(!dispatch.is_fenced(partition));
    assert_eq!(
        broker.dispatch_offset(&tenant, &lane, partition).unwrap(),
        first + 2,
        "a cursor above the verified prefix is clamped (redeliver, never skip)"
    );
    assert!(
        !broker
            .is_delivery_complete(
                &topic,
                &tenant,
                &lane,
                partition,
                first + 2,
                responses[2].message_id.unwrap()
            )
            .unwrap(),
        "identity-less markers above the takeover point do not count"
    );
}
