//! Durability across restarts: a graceful close/reopen, and a SIGKILLed
//! server process whose HTTP acknowledgements must survive.

use broker_partition::{Broker, BrokerConfig, CreateSubscriptionRequest, PublishRequest};
use std::collections::HashSet;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::tempdir;

struct Server {
    child: Child,
    base: String,
}

impl Server {
    async fn start(data_dir: &std::path::Path) -> Self {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let child = Command::new(env!("CARGO_BIN_EXE_bettermq"))
            .args([
                "serve",
                "--broker-only",
                "--no-panel",
                "--listen",
                &address.to_string(),
                "--data-dir",
                data_dir.to_str().unwrap(),
            ])
            .env("BETTERMQ_INSECURE_NO_AUTH", "1")
            // A long group-commit timer leaves only the request's own commit
            // wait to persist a record before its 202. SIGKILL keeps the page
            // cache, so this catches records acknowledged before they reach
            // the WAL file, not a missing fsync (that needs power loss).
            .env("BETTERMQ_FSYNC", "group")
            .env("BETTERMQ_FSYNC_INTERVAL_MS", "60000")
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            base: format!("http://{address}"),
        };
        let client = reqwest::Client::new();
        for _ in 0..200 {
            if client
                .get(format!("{}/healthz", server.base))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("bettermq serve did not become healthy");
    }

    /// SIGKILL: no drain, no WAL flush on shutdown.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_enqueues_survive_sigkill() {
    let dir = tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path()).await;
    let created = client
        .post(format!("{}/v1/queues", server.base))
        .json(&serde_json::json!({
            "queue": "crash",
            "url": "https://example.invalid/hook",
            "secret": "sec",
        }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "{}", created.status());

    let mut acknowledged = HashSet::new();
    for i in 0..25 {
        let response = client
            .post(format!("{}/v1/enqueue", server.base))
            .json(&serde_json::json!({
                "queue": "crash",
                "key": format!("k{i}"),
                "body": format!("payload-{i}"),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let body: serde_json::Value = response.json().await.unwrap();
        acknowledged.insert(body["message_id"].as_str().unwrap().to_string());
    }
    server.kill();

    // The restarted server recovers the WAL on boot.
    Server::start(dir.path()).await.kill();

    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let recovered: HashSet<String> = broker
        .list_topic_messages("crash", 100)
        .unwrap()
        .into_iter()
        .map(|message| message.id.to_string())
        .collect();
    assert_eq!(recovered, acknowledged);
}

#[test]
fn messages_survive_graceful_reopen() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let mut expected = HashSet::new();
    {
        let broker = Broker::open(BrokerConfig::new(data_dir.clone())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "events".into(),
                url: "http://127.0.0.1:1/pull-only".into(),
                secret: "sec".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        for i in 0..100 {
            let resp = broker
                .publish(PublishRequest {
                    topic: "events".into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: format!("key-{i}"),
                    payload: format!("payload-{i}"),
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
                    max_retries: None,
                    retry_backoff: None,
                    method: None,
                    headers: None,
                    sign: None,
                    request: None,
                })
                .unwrap();
            expected.insert((resp.partition.unwrap(), resp.offset.unwrap()));
        }
    }

    let broker = Broker::open(BrokerConfig::new(data_dir)).unwrap();
    let batch = broker.list_topic_messages("events", 200).unwrap();

    assert_eq!(batch.len(), 100);
    let found: HashSet<_> = batch.iter().map(|m| (m.partition, m.offset)).collect();
    assert_eq!(found, expected);
}

#[test]
#[ignore = "run: cargo test -p broker-server --test crash_recovery ten_thousand -- --ignored"]
fn ten_thousand_messages_durable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let broker = Broker::open(BrokerConfig::new(data_dir.clone())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "load".into(),
                url: "http://127.0.0.1:1/pull-only".into(),
                secret: "sec".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        for i in 0..10_000 {
            broker
                .publish(PublishRequest {
                    topic: "load".into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: format!("k{i}"),
                    payload: "x".repeat(64),
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
                    max_retries: None,
                    retry_backoff: None,
                    method: None,
                    headers: None,
                    sign: None,
                    request: None,
                })
                .unwrap();
        }
    }

    let broker = Broker::open(BrokerConfig::new(data_dir)).unwrap();
    let batch = broker.list_topic_messages("load", 10_000).unwrap();
    assert_eq!(batch.len(), 10_000);
}
