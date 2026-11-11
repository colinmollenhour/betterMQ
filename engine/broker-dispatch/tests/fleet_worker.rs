//! Fleet worker loop against a mocked broker lease API.

use broker_dispatch::{
    spawn_fleet, ClaimResponse, ClaimedJob, DispatchConfig, FleetDelivery, FleetOptions,
    LeaseClient,
};
use broker_storage::StoredMessage;
use std::sync::Once;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn allow_wiremock_localhost() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    });
}

fn job(offset: u64, url: &str) -> ClaimedJob {
    let message_id = Uuid::new_v4();
    ClaimedJob {
        lease_id: Uuid::new_v4(),
        topic: "__direct".into(),
        partition: 0,
        offset,
        message_id,
        expires_at_ms: chrono::Utc::now().timestamp_millis() + 30_000,
        generation: 0,
        attempts: 0,
        committed_hwm: offset + 100,
        cursor_key: "lane".into(),
        message: Some(StoredMessage {
            idempotency_key: None,
            id: message_id,
            tenant_id: "default".into(),
            topic: "__direct".into(),
            partition: 0,
            offset,
            routing_key: format!("key-{offset}"),
            payload: b"body".to_vec(),
            published_at_ms: 0,
            priority: 4,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: Some(url.into()),
            destination_secret: Some("whsec_test".into()),
            max_retries: 3,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        }),
    }
}

fn fleet_delivery() -> FleetDelivery {
    FleetDelivery::new(DispatchConfig::default())
}

async fn requests_to(server: &MockServer, route: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == route)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_claimed_lease_is_heartbeated_while_deliveries_run() {
    allow_wiremock_localhost();
    let destination = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(1500)))
        .mount(&destination)
        .await;
    let hook = format!("{}/slow", destination.uri());

    let broker = MockServer::start().await;
    let jobs = vec![job(1, &hook), job(2, &hook)];
    let lease_ids: Vec<Uuid> = jobs.iter().map(|j| j.lease_id).collect();
    Mock::given(method("POST"))
        .and(path("/internal/v1/lease/claim"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ClaimResponse { jobs }))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&broker)
        .await;
    Mock::given(method("POST"))
        .and(path("/internal/v1/lease/claim"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ClaimResponse { jobs: vec![] }))
        .with_priority(2)
        .mount(&broker)
        .await;
    for route in [
        "/internal/v1/lease/heartbeat",
        "/internal/v1/lease/complete",
    ] {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&broker)
            .await;
    }

    let engine = fleet_delivery();
    let client = LeaseClient::new(vec![broker.uri()], "fleet-test".into());
    let fleet = spawn_fleet(
        engine,
        client,
        FleetOptions {
            concurrency: 2,
            heartbeat_interval: Duration::from_millis(100),
            ..FleetOptions::from_env()
        },
    );

    // Both webhooks are still in flight (they take 1.5 s).
    tokio::time::sleep(Duration::from_millis(800)).await;
    let claims = requests_to(&broker, "/internal/v1/lease/claim").await;
    let first: serde_json::Value = serde_json::from_slice(&claims[0].body).unwrap();
    assert_eq!(first["max_jobs"], 2, "claim is sized to the free permits");
    let heartbeats = requests_to(&broker, "/internal/v1/lease/heartbeat").await;
    for lease_id in &lease_ids {
        let renewed = heartbeats
            .iter()
            .filter(|r| {
                let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
                body["lease_id"] == lease_id.to_string()
            })
            .count();
        assert!(
            renewed >= 2,
            "lease {lease_id} was renewed {renewed} times while its job waited"
        );
    }
    assert_eq!(
        destination.received_requests().await.unwrap().len(),
        2,
        "claimed jobs are delivered concurrently, not one after another"
    );

    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        requests_to(&broker, "/internal/v1/lease/complete")
            .await
            .len(),
        2
    );
    fleet.abort();
}

/// Hands out queued jobs, honouring each claim's `max_jobs`.
struct ClaimQueue(Arc<Mutex<Vec<ClaimedJob>>>);

impl Respond for ClaimQueue {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        let max = body["max_jobs"].as_u64().unwrap() as usize;
        let mut queue = self.0.lock().unwrap();
        let take = max.min(queue.len());
        let jobs: Vec<ClaimedJob> = queue.drain(..take).collect();
        ResponseTemplate::new(200).set_body_json(ClaimResponse { jobs })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_keeps_claiming_while_brokers_return_work() {
    allow_wiremock_localhost();
    let destination = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/fast"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&destination)
        .await;
    let hook = format!("{}/fast", destination.uri());

    let broker = MockServer::start().await;
    const JOBS: usize = 120;
    let queue = Arc::new(Mutex::new(
        (0..JOBS as u64).map(|offset| job(offset, &hook)).collect(),
    ));
    Mock::given(method("POST"))
        .and(path("/internal/v1/lease/claim"))
        .respond_with(ClaimQueue(queue.clone()))
        .mount(&broker)
        .await;
    for route in [
        "/internal/v1/lease/heartbeat",
        "/internal/v1/lease/complete",
    ] {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&broker)
            .await;
    }

    let engine = fleet_delivery();
    let client = LeaseClient::new(vec![broker.uri()], "fleet-test".into());
    let started = std::time::Instant::now();
    let fleet = spawn_fleet(
        engine,
        client,
        FleetOptions {
            concurrency: 4,
            claim_batch: 4,
            idle_poll: Duration::from_millis(250),
            heartbeat_interval: Duration::from_secs(10),
            on_claim_failure: None,
        },
    );
    loop {
        let done = requests_to(&broker, "/internal/v1/lease/complete")
            .await
            .len();
        if done == JOBS {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "only {done}/{JOBS} delivered; claims are tick-gated"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A 250 ms tick with 4 jobs per claim would need 7.5 s for 120 jobs.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );

    // Once the backlog is gone the loop backs off instead of spinning.
    let before = requests_to(&broker, "/internal/v1/lease/claim").await.len();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle_claims = requests_to(&broker, "/internal/v1/lease/claim").await.len() - before;
    assert!(idle_claims <= 6, "{idle_claims} claims in one idle second");
    fleet.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_broker_does_not_halt_claims_on_healthy_brokers() {
    allow_wiremock_localhost();
    let destination = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/fast"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&destination)
        .await;
    let hook = format!("{}/fast", destination.uri());

    // Accepts connections but never answers claims in time.
    let stalled = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
        .mount(&stalled)
        .await;

    let healthy = MockServer::start().await;
    const JOBS: usize = 20;
    let queue = Arc::new(Mutex::new(
        (0..JOBS as u64).map(|offset| job(offset, &hook)).collect(),
    ));
    Mock::given(method("POST"))
        .and(path("/internal/v1/lease/claim"))
        .respond_with(ClaimQueue(queue))
        .mount(&healthy)
        .await;
    for route in [
        "/internal/v1/lease/heartbeat",
        "/internal/v1/lease/complete",
    ] {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&healthy)
            .await;
    }

    let engine = fleet_delivery();
    let client = LeaseClient::with_rpc_timeout(
        vec![stalled.uri(), healthy.uri()],
        "fleet-test".into(),
        Duration::from_millis(300),
    );
    static CLAIM_FAILURES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn count_claim_failure() {
        CLAIM_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let started = std::time::Instant::now();
    let fleet = spawn_fleet(
        engine,
        client,
        FleetOptions {
            concurrency: 1,
            claim_batch: 4,
            idle_poll: Duration::from_millis(50),
            heartbeat_interval: Duration::from_secs(10),
            on_claim_failure: Some(count_claim_failure),
        },
    );
    loop {
        let done = requests_to(&healthy, "/internal/v1/lease/complete")
            .await
            .len();
        if done == JOBS {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "only {done}/{JOBS} delivered while one broker stalls"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let stalled_claims = requests_to(&stalled, "/internal/v1/lease/claim")
        .await
        .len();
    assert!(
        stalled_claims <= 3,
        "stalled broker should be backed off, got {stalled_claims} claims"
    );
    assert!(
        CLAIM_FAILURES.load(std::sync::atomic::Ordering::Relaxed) >= 1,
        "failed claims reach the metrics hook"
    );
    fleet.abort();
}
