//! Process-level dispatch fleet: a broker-only node plus a `--dispatch-fleet`
//! process deliver a webhook end to end.

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SECRET: &str = "dispatch-fleet-test-secret";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> std::net::SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    address
}

fn spawn(args: &[&str], broker_urls: &str, data_dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_bettermq"))
        .args(args)
        .env("BETTERMQ_INSECURE_NO_AUTH", "1")
        .env("BETTERMQ_CLUSTER_SECRET", SECRET)
        .env("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1")
        .env("BETTERMQ_BROKER_URLS", broker_urls)
        .env("RUST_LOG", "error")
        .current_dir(data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

async fn wait_status(client: &reqwest::Client, url: &str, want: u16, attempts: usize) -> bool {
    for _ in 0..attempts {
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().as_u16() == want)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_process_claims_from_broker_only_node_and_delivers() {
    let destination = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&destination)
        .await;

    let broker_dir = tempfile::tempdir().unwrap();
    let broker_addr = free_addr();
    let _broker = ChildGuard(spawn(
        &[
            "serve",
            "--broker-only",
            "--listen",
            &broker_addr.to_string(),
            "--data-dir",
            broker_dir.path().to_str().unwrap(),
        ],
        "",
        broker_dir.path(),
    ));
    let broker_url = format!("http://{broker_addr}");
    let client = reqwest::Client::new();
    assert!(wait_status(&client, &format!("{broker_url}/healthz"), 200, 200).await);

    let fleet_dir = tempfile::tempdir().unwrap();
    let fleet_addr = free_addr();
    let _fleet = ChildGuard(spawn(
        &[
            "serve",
            "--dispatch-fleet",
            "--listen",
            &fleet_addr.to_string(),
            "--data-dir",
            fleet_dir.path().join("data").to_str().unwrap(),
        ],
        &broker_url,
        fleet_dir.path(),
    ));
    let fleet_url = format!("http://{fleet_addr}");
    assert!(
        wait_status(&client, &format!("{fleet_url}/readyz"), 200, 200).await,
        "fleet becomes ready once a broker answers its claims"
    );

    let accepted = client
        .post(format!("{broker_url}/v1/publish"))
        .json(&serde_json::json!({
            "url": format!("{}/hook", destination.uri()),
            "secret": "whsec_test",
            "key": "fleet-e2e",
            "body": "delivered-by-fleet",
        }))
        .send()
        .await
        .unwrap();
    assert!(
        accepted.status().is_success(),
        "publish: {}",
        accepted.status()
    );

    let mut delivered = false;
    for _ in 0..200 {
        let requests = destination.received_requests().await.unwrap_or_default();
        if let Some(request) = requests.first() {
            assert_eq!(request.body, b"delivered-by-fleet");
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        delivered,
        "the fleet process must claim and deliver the job"
    );

    // The lease is completed on the broker, not left to expire.
    let mut completed = false;
    for _ in 0..100 {
        let status: serde_json::Value = client
            .get(format!("{broker_url}/internal/v1/lease/status"))
            .header("x-bettermq-cluster-secret", SECRET)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["active_leases"] == 0 {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(completed, "fleet completes its lease");
    assert!(
        !fleet_dir.path().join("data").exists(),
        "the fleet process opens no local storage"
    );

    // The fleet exposes no ingest or infra API, so it cannot join a replica set.
    let enroll = client
        .post(format!("{fleet_url}/v1/infra/cluster/enroll"))
        .json(&serde_json::json!({
            "seed_url": "http://127.0.0.1:9",
            "join_token": "nope",
            "public_url": "http://127.0.0.1:9",
            "node_name": "fleet1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(enroll.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fleet_is_not_ready_without_a_reachable_broker() {
    let dir = tempfile::tempdir().unwrap();
    let address = free_addr();
    let _fleet = ChildGuard(spawn(
        &[
            "serve",
            "--dispatch-fleet",
            "--listen",
            &address.to_string(),
            "--data-dir",
            dir.path().to_str().unwrap(),
        ],
        "http://127.0.0.1:9",
        dir.path(),
    ));
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert!(wait_status(&client, &format!("{base}/healthz"), 200, 100).await);
    let ready = client.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["running"], true);
    assert_eq!(body["last_claim_ok_ms"], serde_json::Value::Null);
}

#[test]
fn fleet_without_broker_urls_fails_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let address = free_addr();
    let mut child = spawn(
        &[
            "serve",
            "--dispatch-fleet",
            "--listen",
            &address.to_string(),
            "--data-dir",
            dir.path().to_str().unwrap(),
        ],
        "",
        dir.path(),
    );
    for _ in 0..200 {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success(), "startup must fail without broker URLs");
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    panic!("fleet kept running without BETTERMQ_BROKER_URLS");
}
