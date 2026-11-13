//! Real-process RF=3 HTTP correctness gate; no mocked replication acknowledgments.

use broker_partition::{Broker, BrokerConfig};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;

struct Node {
    child: Option<Child>,
    dir: PathBuf,
    config: PathBuf,
    base: String,
    shared: Option<PathBuf>,
}

impl Node {
    fn start(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("process.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_bettermq"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("BETTERMQ_")) {
            command.env_remove(key);
        }
        if let Some(shared) = &self.shared {
            command.env("BETTERMQ_SHARED_META_DIR", shared);
        }
        self.child = Some(
            command
                .args(["serve", "--config"])
                .arg(&self.config)
                .args(["--broker-only", "--no-panel"])
                .env("BETTERMQ_CLUSTER_SECRET", "http-integration-secret")
                .env("BETTERMQ_INSECURE_NO_AUTH", "1")
                .env("RUST_LOG", "warn")
                .env("TOKIO_WORKER_THREADS", "2")
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
    }

    fn crash(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap(); // SIGKILL on Unix: deliberately bypass graceful drain.
            child.wait().unwrap();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(self.dir.join("process.log")) {
                eprintln!("{} log:\n{log}", self.base);
            }
        }
    }
}

async fn wait_ready(client: &Client, nodes: &[Node], indices: &[usize]) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let mut ready = true;
        for index in indices {
            ready &= client
                .get(format!("{}/readyz", nodes[*index].base))
                .send()
                .await
                .is_ok_and(|response| response.status() == StatusCode::OK);
        }
        if ready {
            return;
        }
        assert!(Instant::now() < deadline, "cell did not become ready");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn post(client: &Client, node: &Node, path: &str, body: &Value) -> (StatusCode, Value) {
    let response = client
        .post(format!("{}{path}", node.base))
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    let value = serde_json::from_str(&text).unwrap_or_else(|_| panic!("{path}: {status} {text}"));
    (status, value)
}

fn accepted_id(status: StatusCode, body: &Value) -> Uuid {
    assert!(status.is_success(), "HTTP {status}: {body}");
    Uuid::parse_str(
        body["message_id"]
            .as_str()
            .unwrap_or_else(|| panic!("missing acknowledged ID: {body}")),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real three-process failover gate; run explicitly in CI"]
async fn replicated_http_ingest_survives_sigkill_and_refuses_missing_quorum() {
    replicated_http_gate(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real shared-metadata three-process failover gate; run explicitly in CI"]
async fn replicated_http_shared_metadata_retains_global_keys_and_migrates_legacy_rows() {
    const CHILD: &str = "BETTERMQ_TEST_SHARED_HTTP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "replicated_http_shared_metadata_retains_global_keys_and_migrates_legacy_rows",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    replicated_http_gate(true).await;
}

async fn replicated_http_gate(shared: bool) {
    let root = tempfile::tempdir().unwrap();
    let shared_dir = root.path().join("shared-metadata");
    if shared {
        std::fs::create_dir(&shared_dir).unwrap();
        std::env::set_var("BETTERMQ_SHARED_META_DIR", &shared_dir);
    }
    // Hold all ports until the child configurations have been written.
    let listeners: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let bases: Vec<_> = listeners
        .iter()
        .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
        .collect();
    let names: Vec<_> = (0..3).map(|index| format!("http-test-{index}")).collect();
    let members: Vec<_> = names
        .iter()
        .zip(&bases)
        .map(|(name, base)| json!({"name":name,"publicUrl":base}))
        .collect();
    let cluster_id = Uuid::new_v4().to_string();
    let mut nodes: Vec<_> = (0..3).map(|index| {
        let dir = root.path().join(index.to_string());
        std::fs::create_dir(&dir).unwrap();
        let config = dir.join("bettermq.json");
        let body = json!({
            "version":2,
            "node":{"name":names[index], "listen":listeners[index].local_addr().unwrap().to_string(), "publicUrl":bases[index]},
            "dataDir":dir, "storage":{"mode":"local"}, "auth":{"mode":"local"},
            "cluster":{"enabled":true,"id":cluster_id,"nodes":members},
        });
        std::fs::write(&config, serde_json::to_vec_pretty(&body).unwrap()).unwrap();
        Node { child: None, dir, config, base: bases[index].clone(), shared: shared.then(|| shared_dir.clone()) }
    }).collect();
    drop(listeners);
    for node in &mut nodes {
        node.start();
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap();
    wait_ready(&client, &nodes, &[0, 1, 2]).await;
    // Activation is automatic, but the legacy endpoint becomes read-only only
    // after every configured node has advertised its writer capability.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut active = true;
        for node in &nodes {
            let response = client.post(format!("{}/internal/v1/cluster/catalog/apply", node.base))
                .header("x-bettermq-cluster-secret", "http-integration-secret")
                .json(&json!({"flows":[],"queues":[],"groups":[],"group_members":[],"crons":[],"tombstones":{}})).send().await.unwrap();
            active &= response.status() == StatusCode::CONFLICT;
        }
        if active {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "catalog did not activate automatically"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_ready(&client, &nodes, &[0, 1, 2]).await;
    let catalog_op = Uuid::new_v4();
    let queue_definition = json!({
        "queue":"http-test", "url":"https://example.com/hook", "secret":"delivery-secret"
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let response = loop {
        let response = client
            .post(format!("{}/v1/queues", nodes[0].base))
            .header("x-bettermq-operation-id", catalog_op.to_string())
            .json(&queue_definition)
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            break response;
        }
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            Instant::now() < deadline,
            "catalog stayed frozen after capability activation: {}",
            response.text().await.unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let status = response.status();
    let catalog_revision = response.headers()["x-bettermq-catalog-revision"].clone();
    let body: Value = response.json().await.unwrap();
    assert!(status.is_success(), "queue create: {status} {body}");
    // Wait for real catalog propagation before testing forwarding.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut synced = true;
        for node in &nodes {
            let body: Value = client
                .get(format!("{}/v1/queues", node.base))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            synced &= body.to_string().contains("http-test");
        }
        if synced {
            break;
        }
        assert!(Instant::now() < deadline, "queue catalog did not propagate");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let response = client
        .post(format!("{}/v1/queues", nodes[2].base))
        .header("x-bettermq-operation-id", catalog_op.to_string())
        .json(&queue_definition)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-bettermq-catalog-revision"],
        catalog_revision
    );
    assert_eq!(response.json::<Value>().await.unwrap()["replayed"], true);

    let mut acknowledged = HashSet::new();
    let enqueue =
        json!({"queue":"http-test", "body":"keyless-single", "idempotency_key":"enqueue-one"});
    let (status, body) = post(&client, &nodes[2], "/v1/enqueue", &enqueue).await;
    let enqueue_id = accepted_id(status, &body);
    acknowledged.insert(enqueue_id);
    let (status, body) = post(&client, &nodes[2], "/v1/enqueue", &enqueue).await;
    assert_eq!(accepted_id(status, &body), enqueue_id);
    assert_eq!(body["duplicate"], true);

    let publish = json!({"url":"https://example.com/hook", "secret":"delivery-secret", "body":"direct-single", "key":"failover-key", "idempotency_key":"publish-one"});
    let (status, body) = post(&client, &nodes[1], "/v1/publish", &publish).await;
    let old_leader_id = accepted_id(status, &body);
    acknowledged.insert(old_leader_id);
    let shard = body["delivery"]["shard"].as_u64().unwrap();
    let batch = json!({"messages": (0..16).map(|index| json!({
        "topic":"http-test", "routing_key": if index % 2 == 0 { "" } else { "   " },
        "payload":format!("batch-{index}"), "idempotency_key":format!("batch-{index}")
    })).collect::<Vec<_>>()});
    let (status, body) = post(&client, &nodes[0], "/v1/ingest/batch", &batch).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["accepted"], 16);
    for item in body["results"].as_array().unwrap() {
        acknowledged.insert(Uuid::parse_str(item["message_id"].as_str().unwrap()).unwrap());
    }

    let routes: Value = client
        .get(format!("{}/internal/v1/gateway/routes", nodes[0].base))
        .header("x-bettermq-cluster-secret", "http-integration-secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let leader_base = routes["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route[0] == shard)
        .unwrap()[1]
        .as_str()
        .unwrap();
    let leader = nodes
        .iter()
        .position(|node| node.base == leader_base)
        .unwrap();
    nodes[leader].crash();
    let survivors: Vec<_> = (0..3).filter(|index| *index != leader).collect();
    wait_ready(&client, &nodes, &survivors).await;
    let failover_publish = json!({"url":"https://example.com/hook", "secret":"delivery-secret", "body":"after-failover", "key":"failover-key", "idempotency_key":"after-failover"});
    let deadline = Instant::now() + Duration::from_secs(60);
    let failover_id = loop {
        let (status, body) = post(
            &client,
            &nodes[survivors[0]],
            "/v1/publish",
            &failover_publish,
        )
        .await;
        if status.is_success() {
            break accepted_id(status, &body);
        }
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "transient failure must remain retryable: {body}"
        );
        assert!(Instant::now() < deadline, "shard did not fail over: {body}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    acknowledged.insert(failover_id);
    let (status, body) = post(
        &client,
        &nodes[survivors[0]],
        "/v1/publish",
        &failover_publish,
    )
    .await;
    assert_eq!(accepted_id(status, &body), failover_id);

    // The takeover also needs the original leader's durable producer keys,
    // not only keys first created on the new leader after election.
    let (status, body) = post(&client, &nodes[survivors[0]], "/v1/publish", &publish).await;
    assert_eq!(accepted_id(status, &body), old_leader_id);
    assert_eq!(body["duplicate"], true);

    if shared {
        let mut cross_route = publish.clone();
        cross_route["key"] = json!("different-shard-after-failover");
        let (status, body) = post(&client, &nodes[survivors[1]], "/v1/publish", &cross_route).await;
        assert_eq!(accepted_id(status, &body), old_leader_id);
        assert_eq!(body["duplicate"], true);
    }

    nodes[survivors[0]].crash();
    let lone = survivors[1];
    let (status, body) = post(&client, &nodes[lone], "/v1/publish", &json!({
        "url":"https://example.com/hook", "secret":"delivery-secret", "body":"must-not-ack", "key":"failover-key", "idempotency_key":"no-quorum"
    })).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "missing quorum was acknowledged: {body}"
    );
    assert!(body.get("message_id").is_none());
    let (status, body) = post(&client, &nodes[lone], "/v1/ingest/batch", &json!({
        "messages":[{"topic":"http-test","payload":"must-not-ack-batch","routing_key":"","idempotency_key":"no-quorum-batch"}]
    })).await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["accepted"], 0);
    assert_eq!(body["results"][0]["retryable"], true);
    let before: Value = client
        .get(format!("{}/v1/queues", nodes[lone].base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let (status, body) = post(
        &client,
        &nodes[lone],
        "/v1/queues",
        &json!({"queue":"no-quorum-catalog","url":"https://example.com/hook","secret":"s"}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "catalog mutation without quorum: {body}"
    );
    let after: Value = client
        .get(format!("{}/v1/queues", nodes[lone].base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "no-quorum mutation changed the serving cache"
    );
    nodes[lone].crash();

    if shared {
        // Model a pre-directory shared row without changing its original TTL.
        // Canonical-source verification, not a follower's offset, must adopt it
        // after restart. The actual source remains in replica-local indexes.
        let store =
            broker_storage::MetadataStore::open(root.path().join("offline-shared-index")).unwrap();
        let mut cursor = None;
        let mut legacy = None;
        loop {
            let page = store.dedup_page(cursor).unwrap();
            for row in page.records {
                if row.key == "publish-one"
                    && row.tenant_id.starts_with('\0')
                    && row.entry.message_id == old_leader_id
                {
                    legacy = Some(row);
                }
            }
            let Some(next) = page.next else {
                break;
            };
            cursor = Some(next);
        }
        let row = legacy.expect("shared committed directory entry");
        let mut batch = broker_storage::MetadataWriteBatch::default();
        batch.put_dedup("default", &row.key, row.entry);
        batch.delete_dedup(row.tenant_id, row.key);
        store.write_batch(batch).unwrap();
    }

    // With the old leader absent, the surviving replicas must contain every
    // acknowledged ID. Read real recovered WALs, not response mocks.
    let mut recovered = HashSet::new();
    for index in &survivors {
        let mut config = BrokerConfig::new(nodes[*index].dir.clone());
        config.local_replica_identities = shared;
        let broker = Broker::open(config).unwrap();
        for topic in ["http-test", broker_partition::DIRECT_TOPIC] {
            for message in broker.list_topic_messages(topic, 1000).unwrap() {
                recovered.insert(message.id);
            }
        }
    }
    assert!(
        acknowledged.is_subset(&recovered),
        "acknowledged IDs missing from surviving replicas: {:?}",
        acknowledged.difference(&recovered).collect::<Vec<_>>()
    );

    // Restart the SIGKILLed processes using the same durable directories.
    for node in &mut nodes {
        node.start();
    }
    wait_ready(&client, &nodes, &[0, 1, 2]).await;
    let (status, body) = post(&client, &nodes[2], "/v1/enqueue", &enqueue).await;
    assert_eq!(accepted_id(status, &body), enqueue_id);
    assert_eq!(body["duplicate"], true);
    if shared {
        let mut cross_route = publish.clone();
        cross_route["key"] = json!("another-route-after-legacy-adoption");
        let (status, body) = post(&client, &nodes[1], "/v1/publish", &cross_route).await;
        assert_eq!(accepted_id(status, &body), old_leader_id);
        assert_eq!(body["duplicate"], true);
    }
    let response = client
        .post(format!("{}/v1/queues", nodes[1].base))
        .header("x-bettermq-operation-id", catalog_op.to_string())
        .json(&queue_definition)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-bettermq-catalog-revision"],
        catalog_revision,
        "durable operation receipt survives all process restarts"
    );
}
