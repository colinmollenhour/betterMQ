//! Opt-in real object-store gate, run by CI's isolated MinIO job.
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(config: &std::path::Path) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_bettermq"))
            .args(["serve", "--config", config.to_str().unwrap()])
            .env("BETTERMQ_INSECURE_NO_AUTH", "1")
            .env("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1")
            .env_remove("BETTERMQ_SLATE_TAKEOVER")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

async fn ready(client: &reqwest::Client, base: &str) {
    for _ in 0..200 {
        if client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Slate broker did not become ready");
}

#[tokio::test]
#[ignore = "requires isolated MinIO; CI slate-minio job"]
async fn single_broker_dispatch_restart_and_duplicate_survive_minio() {
    let dir = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let base = format!("http://{addr}");
    let mut cfg = broker_config::BetterMqConfig::template_single_slate();
    cfg.data_dir = dir.path().join("data");
    cfg.node.listen = addr.to_string();
    cfg.node.public_url = base.clone();
    if let broker_config::StorageConfig::Slate { s3 } = &mut cfg.storage {
        s3.endpoint = std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT");
        s3.bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET");
        s3.payload_bucket = Some(std::env::var("S3_PAYLOAD_BUCKET").expect("S3_PAYLOAD_BUCKET"));
        s3.access_key = std::env::var("S3_ACCESS_KEY").expect("S3_ACCESS_KEY");
        s3.secret_key = std::env::var("S3_SECRET_KEY").expect("S3_SECRET_KEY");
        s3.region = "us-east-1".into();
    }
    let config = dir.path().join("bettermq.json");
    std::fs::write(&config, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let mock = MockServer::start().await;
    Mock::given(wiremock::matchers::method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;
    let client = reqwest::Client::new();
    let process = spawn(&config);
    ready(&client, &base).await;
    let body = serde_json::json!({"url": format!("{}/hook", mock.uri()), "secret": "test-hook-secret", "body": "hello", "idempotency_key": "durable-slate-key", "key": "stable"});
    let first: serde_json::Value = client
        .post(format!("{base}/v1/publish"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["duplicate"], false);
    for _ in 0..200 {
        if !mock.received_requests().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !mock.received_requests().await.unwrap().is_empty(),
        "message must dispatch before restart"
    );
    let node = std::fs::read(cfg.data_dir.join("slate-node-id")).unwrap();
    drop(process); // Abrupt exit deliberately retains object-store ownership.
    let _restarted = spawn(&config);
    ready(&client, &base).await;
    assert_eq!(
        std::fs::read(cfg.data_dir.join("slate-node-id")).unwrap(),
        node
    );
    let duplicate: serde_json::Value = client
        .post(format!("{base}/v1/publish"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["message_id"], first["message_id"]);
}
