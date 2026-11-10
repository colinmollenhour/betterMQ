//! D3/B11: in a local-WAL cell, large bodies live inline in the replicated
//! WAL frame. Destroying the accepting node (and its disk) must not lose
//! them: a surviving replica delivers the body byte-for-byte, through both
//! local dispatch and the fleet lease path.

use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
use broker_partition::{Broker, BrokerConfig, PayloadPolicy, PublishRequest, DIRECT_TOPIC};
use std::sync::Once;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use wiremock::matchers::{body_bytes, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn allow_wiremock_localhost() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    });
}

fn cell_broker(dir: &std::path::Path) -> Broker {
    let mut config = BrokerConfig::new(dir.to_path_buf());
    config.partitions = 1;
    let broker = Broker::open(config).unwrap();
    let limits = broker_replication::cell_body_limits_for(
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .unwrap();
    broker.set_payload_policy(PayloadPolicy::InlineOnly {
        max_body: limits.max_inline_body,
    });
    broker
}

fn body_of(len: usize) -> Vec<u8> {
    // Binary, non-repeating at short periods, so truncation or reordering shows.
    (0..len)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

fn publish(broker: &Broker, url: String, body: &[u8]) -> broker_partition::PublishResponse {
    broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "acceptor".into(),
            payload: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
            payload_encoding: Some("base64".into()),
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(url),
            secret: Some("s".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: Some(0),
            retry_backoff: None,
            method: None,
            headers: None,
            sign: Some(false),
            request: None,
            routing_id: None,
        })
        .unwrap()
}

fn files_under(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    files_under(&path)
                } else {
                    1
                }
            })
            .sum()
    })
}

#[derive(Clone, Copy)]
enum Mode {
    Local,
    Fleet,
}

async fn destroy_acceptor_then_deliver(len: usize, mode: Mode) {
    allow_wiremock_localhost();
    let mock = MockServer::start().await;
    let body = body_of(len);
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(body_bytes(body.clone()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let acceptor_dir = tempdir().unwrap();
    let replica_dirs = [tempdir().unwrap(), tempdir().unwrap()];
    let acceptor = cell_broker(acceptor_dir.path());
    let replicas: Vec<Broker> = replica_dirs
        .iter()
        .map(|dir| cell_broker(dir.path()))
        .collect();

    let response = publish(&acceptor, format!("{}/hook", mock.uri()), &body);
    let partition = response.partition.unwrap();
    let offset = response.offset.unwrap();
    let frame = response.replication_frame.clone().unwrap();
    // RF=3: the same frame the leader replicates lands on both replicas.
    for replica in &replicas {
        replica
            .append_replicated_frame(DIRECT_TOPIC, partition, &frame, Some(offset))
            .unwrap();
        replica.flush_wal().unwrap();
    }
    assert_eq!(
        files_under(&acceptor_dir.path().join("payload-blobs")),
        0,
        "a cell must not create node-local blobs"
    );

    // Destroy the accepting node and its storage. Its shard I/O thread exits
    // asynchronously once the broker is dropped, so retry until it is gone.
    acceptor.flush_wal().unwrap();
    drop(acceptor);
    let acceptor_path = acceptor_dir.keep();
    let deadline = Instant::now() + Duration::from_secs(5);
    while acceptor_path.exists() {
        let _ = std::fs::remove_dir_all(&acceptor_path);
        assert!(Instant::now() < deadline, "could not destroy the acceptor");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let survivor = &replicas[0];
    let stored = survivor
        .read_message(DIRECT_TOPIC, partition, offset)
        .unwrap();
    assert!(stored.payload_ref_json.is_none());
    assert_eq!(stored.payload, body);
    let mut streamed = Vec::new();
    survivor
        .stream_message_payload_to_writer(&stored, &mut streamed)
        .unwrap();
    assert_eq!(streamed, body);
    assert_eq!(
        replicas[1]
            .read_message(DIRECT_TOPIC, partition, offset)
            .unwrap()
            .payload,
        body
    );

    match mode {
        Mode::Local => {
            let dispatch = DispatchEngine::new(survivor.clone(), DispatchConfig::default());
            dispatch.enqueue(broker_dispatch::DeliveryJob::live(
                DIRECT_TOPIC,
                partition,
                offset,
                response.message_id.unwrap(),
            ));
        }
        Mode::Fleet => {
            let leases = LeaseTable::new();
            let worker =
                DispatchEngine::new_broker_only(survivor.clone(), DispatchConfig::default());
            let claimed = broker_dispatch::claim_from_broker(
                &worker,
                &leases,
                None,
                "fleet-worker",
                8,
                30_000,
                None,
                &|_| true,
                &|_| 0,
            )
            .await;
            let job = claimed
                .jobs
                .into_iter()
                .find(|job| job.offset == offset)
                .expect("claimed the surviving record");
            let message = job.message.expect("claim carries the message");
            assert_eq!(message.payload, body, "claim carries the inline body");
            let fleet = broker_dispatch::FleetDelivery::new(DispatchConfig::default());
            fleet
                .push(&message, job.committed_hwm, job.attempts)
                .await
                .unwrap();
            fleet.shutdown();
        }
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while mock
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "body never delivered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    mock.verify().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_mib_body_survives_losing_the_acceptor_local_dispatch() {
    destroy_acceptor_then_deliver(1024 * 1024, Mode::Local).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_mib_body_survives_losing_the_acceptor_fleet_dispatch() {
    destroy_acceptor_then_deliver(1024 * 1024, Mode::Fleet).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_mib_body_survives_losing_the_acceptor() {
    destroy_acceptor_then_deliver(4 * 1024 * 1024, Mode::Local).await;
    destroy_acceptor_then_deliver(4 * 1024 * 1024, Mode::Fleet).await;
}

#[test]
fn cell_limit_boundary_is_exact() {
    let dir = tempdir().unwrap();
    let broker = cell_broker(dir.path());
    let PayloadPolicy::InlineOnly { max_body } = broker.payload_policy() else {
        panic!("cell policy");
    };
    let fits = publish(&broker, "http://127.0.0.1:9/h".into(), &body_of(max_body));
    assert_eq!(
        broker
            .read_message(DIRECT_TOPIC, fits.partition.unwrap(), fits.offset.unwrap())
            .unwrap()
            .payload
            .len(),
        max_body
    );
    // The worst-case frame still fits one replication epoch.
    let frame = fits.replication_frame.unwrap();
    assert!(frame.len() + 4 + 64 <= 16 * 1024 * 1024);
    let hwm = broker.partition_high_watermark(DIRECT_TOPIC, 0).unwrap();
    let error = broker
        .publish(PublishRequest {
            payload: "x".repeat(max_body + 1),
            payload_encoding: None,
            ..PublishRequest {
                url: Some("http://127.0.0.1:9/h".into()),
                secret: Some("s".into()),
                ..serde_json::from_value(serde_json::json!({ "payload": "" })).unwrap()
            }
        })
        .unwrap_err();
    assert!(error.to_string().contains("too large for this cell"));
    assert_eq!(
        broker.partition_high_watermark(DIRECT_TOPIC, 0).unwrap(),
        hwm
    );
}
