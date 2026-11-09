//! Broker lease API + dispatch fleet worker with separate data directories.

use axum::{extract::State, http::StatusCode, routing::post, Router};
use broker_api::{AppState, CatalogTombstones};
use broker_dispatch::{
    deliver_claimed_job, DispatchConfig, DispatchEngine, LeaseClient, LeaseTable, TenantFairQueue,
};
use broker_partition::{Broker, BrokerConfig, PublishRequest};
use broker_schedule::{CronRegistry, ScheduleQueue};
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::sync::mpsc;

fn test_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Every body above 1 KiB goes to the blob store.
        std::env::set_var("BETTERMQ_INLINE_MAX_BYTES", "1024");
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
        std::env::set_var("BETTERMQ_CLUSTER_SECRET", "fleet-test-secret");
    });
}

struct Harness {
    broker: Broker,
    broker_url: String,
    fleet: DispatchEngine,
    client: LeaseClient,
    received: mpsc::UnboundedReceiver<Vec<u8>>,
    hook_url: String,
    dir: tempfile::TempDir,
    _fleet_dir: tempfile::TempDir,
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn harness() -> Harness {
    test_env();
    let (tx, received) = mpsc::unbounded_channel::<Vec<u8>>();
    let destination = Router::new()
        .route(
            "/hook",
            post(
                |State(tx): State<mpsc::UnboundedSender<Vec<u8>>>, body: axum::body::Bytes| async move {
                    let _ = tx.send(body.to_vec());
                    StatusCode::OK
                },
            ),
        )
        .with_state(tx);
    let hook_url = format!("{}/hook", serve(destination).await);

    let broker_dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(broker_dir.path().to_path_buf())).unwrap();
    let state = AppState {
        broker: broker.clone(),
        schedule: ScheduleQueue::open(broker_dir.path()).unwrap(),
        crons: CronRegistry::open(broker_dir.path()).unwrap(),
        dispatch: DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default()),
        leases: LeaseTable::new(),
        cluster: None,
        local_auth: None,
        fair_queue: Arc::new(TenantFairQueue::new()),
        catalog_tombstones: CatalogTombstones::open(broker_dir.path()).unwrap(),
        dispatch_fleet: false,
        broker_only: true,
        #[cfg(feature = "cloud")]
        auth: None,
        #[cfg(feature = "cloud")]
        control_plane: None,
    };
    let broker_url = serve(broker_api::internal_router(state)).await;

    // The fleet process has its own data directory, as in docker-compose.ha-fleet.yml.
    let fleet_dir = tempfile::tempdir().unwrap();
    let fleet_broker = Broker::open(BrokerConfig::new(fleet_dir.path().to_path_buf())).unwrap();
    let fleet = DispatchEngine::new_broker_only(fleet_broker, DispatchConfig::default());
    let client = LeaseClient::new(vec![broker_url.clone()], "fleet-test".into());
    Harness {
        broker,
        broker_url,
        fleet,
        client,
        received,
        hook_url,
        dir: broker_dir,
        _fleet_dir: fleet_dir,
    }
}

fn publish_binary(broker: &Broker, url: &str, routing_key: &str, body: &[u8]) -> uuid::Uuid {
    use base64::Engine;
    let response = broker
        .publish(PublishRequest {
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: routing_key.into(),
            payload: base64::engine::general_purpose::STANDARD.encode(body),
            routing_id: None,
            payload_encoding: Some("base64".into()),
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some(url.into()),
            secret: Some("whsec_test".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        })
        .unwrap();
    broker.flush_wal().unwrap();
    response.message_id.unwrap()
}

fn large_body() -> Vec<u8> {
    (0..64 * 1024u32).map(|i| (i % 251) as u8).collect()
}

fn is_complete(broker: &Broker, job: &broker_dispatch::ClaimedJob) -> bool {
    broker
        .is_dispatch_complete(&broker.tenant(), &job.cursor_key, job.partition, job.offset)
        .unwrap()
        || broker
            .dispatch_offset(&broker.tenant(), &job.cursor_key, job.partition)
            .unwrap()
            > job.offset
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_delivers_blob_backed_payload_intact() {
    let mut h = harness().await;
    let body = large_body();
    let message_id = publish_binary(&h.broker, &h.hook_url, "blob", &body);

    let claimed = h.client.claim(&h.broker_url, 4).await.unwrap();
    assert_eq!(claimed.jobs.len(), 1);
    let job = claimed.jobs.into_iter().next().unwrap();
    assert_eq!(job.message_id, message_id);
    let envelope = job.message.as_ref().unwrap();
    assert!(
        envelope.payload_ref_json.is_some(),
        "the body is stored in the blob store"
    );
    assert_eq!(
        envelope.payload, body,
        "the broker loads and verifies the blob before issuing the claim"
    );

    deliver_claimed_job(
        &h.fleet,
        &h.client,
        &h.broker_url,
        job.clone(),
        broker_dispatch::DEFAULT_HEARTBEAT_INTERVAL,
    )
    .await;

    let delivered = tokio::time::timeout(Duration::from_secs(5), h.received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered, body,
        "destination must receive the original bytes"
    );
    assert!(is_complete(&h.broker, &job));
}

fn corrupt_blobs(dir: &std::path::Path) -> usize {
    let mut corrupted = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            corrupted += corrupt_blobs(&path);
        } else {
            std::fs::write(&path, b"corrupt").unwrap();
            corrupted += 1;
        }
    }
    corrupted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_withholds_job_whose_blob_cannot_be_verified() {
    let h = harness().await;
    publish_binary(&h.broker, &h.hook_url, "blob-corrupt", &large_body());
    assert!(corrupt_blobs(&h.dir.path().join("payload-blobs")) > 0);

    let claimed = h.client.claim(&h.broker_url, 4).await.unwrap();
    assert!(
        claimed.jobs.is_empty(),
        "an unverifiable body is never handed out"
    );
    let dlq = h
        .broker
        .list_topic_messages(
            &broker_partition::dlq_topic(broker_partition::DIRECT_TOPIC),
            10,
        )
        .unwrap();
    assert!(
        dlq.is_empty(),
        "the message stays pending, not dead-lettered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_refuses_to_complete_a_lease_without_its_body() {
    let mut h = harness().await;
    publish_binary(&h.broker, &h.hook_url, "blob-unresolved", &large_body());
    let claimed = h.client.claim(&h.broker_url, 4).await.unwrap();
    let mut job = claimed.jobs.into_iter().next().unwrap();
    // A claim from a broker that did not load the blob.
    job.message.as_mut().unwrap().payload.clear();

    deliver_claimed_job(
        &h.fleet,
        &h.client,
        &h.broker_url,
        job.clone(),
        broker_dispatch::DEFAULT_HEARTBEAT_INTERVAL,
    )
    .await;
    assert!(
        h.received.try_recv().is_err(),
        "no empty body was delivered"
    );
    assert!(
        !is_complete(&h.broker, &job),
        "the lease is failed for a retry, not completed"
    );
}
