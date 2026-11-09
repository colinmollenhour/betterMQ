//! CP7a: replicated log frames visible on follower; replication quorum rules.

use broker_partition::{
    Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest, DIRECT_TOPIC,
};
use broker_raft_meta::{ClusterConfig, NodeConfig};
use broker_replication::{
    ReplicateBatchAck, ReplicateBatchRequest, ReplicateError, ReplicationClient,
};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use uuid::Uuid;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn replicated_frame_is_readable_on_follower() {
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();

    let broker_l = Broker::open(BrokerConfig::new(leader_dir.path().to_path_buf())).unwrap();
    let broker_f = Broker::open(BrokerConfig::new(follower_dir.path().to_path_buf())).unwrap();

    broker_l
        .create_subscription(CreateSubscriptionRequest {
            topic: "jobs".into(),
            url: "http://example/hook".into(),
            secret: "sec".into(),
            parallelism: None,
            default_max_retries: None,
            retry_backoff: None,
        })
        .unwrap();

    let resp = broker_l
        .publish(PublishRequest {
            routing_id: None,
            topic: "jobs".into(),
            routing_key: "k1".into(),
            payload: "hello".into(),
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
            url: None,
            secret: None,
        })
        .unwrap();
    let partition = resp.partition.expect("partition");
    let frame = resp.replication_frame.expect("frame");

    broker_f
        .append_replicated_frame(&resp.topic, partition, &frame, resp.offset)
        .unwrap();

    let msgs = broker_f.list_topic_messages("jobs", 10).unwrap();
    assert_eq!(msgs.len(), 1);

    // Leader-only delivery and follower fencing are asserted against a live
    // webhook in `src/failover_tests.rs` on the production OpenRaft wiring.
}

fn replication_config(local: Uuid, peers: [(Uuid, String); 2]) -> ClusterConfig {
    ClusterConfig {
        cluster_id: Uuid::new_v4(),
        nodes: vec![
            NodeConfig {
                internal_url: None,
                id: local,
                addr: "http://local.invalid".into(),
            },
            NodeConfig {
                internal_url: None,
                id: peers[0].0,
                addr: peers[0].1.clone(),
            },
            NodeConfig {
                internal_url: None,
                id: peers[1].0,
                addr: peers[1].1.clone(),
            },
        ],
        node_id: local,
        generation: 1,
        hash_version: 1,
    }
}

fn binary_epoch(local: Uuid) -> ReplicateBatchRequest {
    ReplicateBatchRequest::new(
        "default".into(),
        "jobs".into(),
        0,
        local,
        3,
        0,
        vec![b"raw\0epoch\xff".to_vec()],
    )
    .unwrap()
}

#[tokio::test]
async fn rf3_quorum_uses_fast_durable_follower_without_waiting_for_slow_third() {
    let fast = MockServer::start().await;
    let slow = MockServer::start().await;
    let local = Uuid::new_v4();
    let fast_id = Uuid::new_v4();
    let slow_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/internal/v1/replicate/batch"))
        .and(header("content-type", "application/octet-stream"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ReplicateBatchAck {
            node_id: fast_id,
            partition: 0,
            leader_epoch: 3,
            durable_hwm: 1,
        }))
        .mount(&fast)
        .await;
    Mock::given(method("POST"))
        .and(path("/internal/v1/replicate/batch"))
        .respond_with(ResponseTemplate::new(503).set_delay(Duration::from_secs(1)))
        .mount(&slow)
        .await;

    let client = ReplicationClient::new(replication_config(
        local,
        [(fast_id, fast.uri()), (slow_id, slow.uri())],
    ));
    let started = Instant::now();
    let outcome = client
        .replicate_batch(binary_epoch(local), 1)
        .await
        .unwrap();
    assert_eq!(outcome.durable_acks, 2);
    assert_eq!(outcome.quorum, 2);
    assert!(started.elapsed() < Duration::from_millis(750));
}

#[tokio::test]
async fn rf3_rejects_http_success_without_durable_offset_and_lacks_quorum() {
    let stale = MockServer::start().await;
    let failed = MockServer::start().await;
    let local = Uuid::new_v4();
    let stale_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/internal/v1/replicate/batch"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ReplicateBatchAck {
            node_id: stale_id,
            partition: 0,
            leader_epoch: 3,
            durable_hwm: 0,
        }))
        .mount(&stale)
        .await;
    Mock::given(method("POST"))
        .and(path("/internal/v1/replicate/batch"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&failed)
        .await;

    let client = ReplicationClient::new(replication_config(
        local,
        [(stale_id, stale.uri()), (failed_id, failed.uri())],
    ));
    let error = client
        .replicate_batch(binary_epoch(local), 1)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ReplicateError::QuorumNotReached {
            acked: 1,
            quorum: 2
        }
    ));
}

#[tokio::test]
async fn quorum_never_counts_an_unflushed_local_epoch() {
    let peer1 = MockServer::start().await;
    let peer2 = MockServer::start().await;
    let local = Uuid::new_v4();
    let client = ReplicationClient::new(replication_config(
        local,
        [(Uuid::new_v4(), peer1.uri()), (Uuid::new_v4(), peer2.uri())],
    ));
    let error = client
        .replicate_batch(binary_epoch(local), 0)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ReplicateError::LocalNotDurable {
            required: 1,
            actual: 0
        }
    ));
}

#[test]
fn divergent_follower_tail_is_rejected_without_overwrite() {
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();
    let leader = Broker::open(BrokerConfig::new(leader_dir.path().to_path_buf())).unwrap();
    let follower = Broker::open(BrokerConfig::new(follower_dir.path().to_path_buf())).unwrap();
    let publish = |broker: &Broker, payload: &str| {
        broker
            .publish(PublishRequest {
                routing_id: None,
                topic: DIRECT_TOPIC.into(),
                routing_key: "same-shard".into(),
                payload: payload.into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                queue_id: None,
                group_id: None,
                group_member_id: None,
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
                url: Some("http://example/hook".into()),
                secret: Some("s".into()),
            })
            .unwrap()
    };
    let leader_message = publish(&leader, "leader");
    let follower_message = publish(&follower, "divergent");
    assert_eq!(leader_message.partition, follower_message.partition);
    let error = follower
        .append_replicated_frame(
            &leader_message.topic,
            leader_message.partition.unwrap(),
            &leader_message.replication_frame.unwrap(),
            Some(0),
        )
        .unwrap_err();
    assert!(error.to_string().contains("offset mismatch"));
    let stored = follower
        .read_message(
            &follower_message.topic,
            follower_message.partition.unwrap(),
            0,
        )
        .unwrap();
    assert_eq!(stored.payload, b"divergent");
}
