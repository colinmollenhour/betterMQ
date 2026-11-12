//! Admission overflow must leave work durable and cannot break strict FIFO.

use broker_dispatch::{DeliveryJob, DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, PublishRequest, ResolvedFlow};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_fifo_lane_drains_without_loss_duplicates_or_overtaking() {
    // This integration binary contains one test, so these settings cannot
    // race unrelated tests or alter another broker in this process.
    std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    std::env::set_var("BETTERMQ_DISPATCH_QUEUE_CAP", "32");
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::new(dir.path().to_path_buf());
    config.partitions = 1;
    let broker = Broker::open(config).unwrap();
    let sink = MockServer::start().await;
    let order = Arc::new(Mutex::new(Vec::<usize>::new()));
    let seen = order.clone();
    Mock::given(method("POST"))
        .respond_with(move |request: &wiremock::Request| {
            seen.lock()
                .unwrap()
                .push(serde_json::from_slice(&request.body).unwrap());
            ResponseTemplate::new(200).set_delay(Duration::from_millis(2))
        })
        .mount(&sink)
        .await;
    let mut jobs = Vec::new();
    for sequence in 0..128 {
        let request: PublishRequest = serde_json::from_value(serde_json::json!({
            "url": format!("{}/hook", sink.uri()), "secret": "test",
            "payload": sequence.to_string(), "routing_key": "entity",
            "flow": { "key": "lane", "parallelism": 1 },
        }))
        .unwrap();
        let response = broker.publish(request).unwrap();
        jobs.push(DeliveryJob::live(
            response.topic,
            response.partition.unwrap(),
            response.offset.unwrap(),
            response.message_id.unwrap(),
        ));
    }
    broker.flush_wal().unwrap();
    let msg = broker
        .read_message(&jobs[0].topic, 0, jobs[0].offset)
        .unwrap();
    let owner = broker_partition::flow_lane_owner(&msg);
    let dispatch = DispatchEngine::new(broker, DispatchConfig::default());
    dispatch
        .flow
        .ensure_lane(
            owner,
            "lane",
            ResolvedFlow {
                key: "lane".into(),
                parallelism: 1,
                rate: 0,
                period_secs: 1,
            },
        )
        .await;
    dispatch.flow.pause(owner, "lane").await;
    for job in jobs {
        dispatch.enqueue(job);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while dispatch
            .flow
            .get(owner, "lane")
            .await
            .unwrap()
            .wait_list_size
            < 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(dispatch.telemetry_snapshot().in_flight_deliveries <= 32);
    assert!(dispatch.telemetry_snapshot().queue_rejected > 0);
    dispatch.flow.resume(owner, "lane").await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let engine = dispatch.clone();
            tokio::task::spawn_blocking(move || engine.backfill_pending())
                .await
                .unwrap();
            if order.lock().unwrap().len() == 128 && dispatch.flow.pending_count().await == 0 {
                break;
            }
            tokio::select! {
                _ = dispatch.backfill_notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(25)) => {},
            }
        }
    })
    .await
    .expect("bounded admission must recover all durable work");
    assert_eq!(*order.lock().unwrap(), (0..128).collect::<Vec<_>>());
    dispatch.shutdown_background().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_hung_flow_deliveries_and_releases_all_capacity() {
    use tokio::io::AsyncReadExt;
    std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    std::env::set_var("BETTERMQ_DISPATCH_QUEUE_CAP", "32");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (arrived, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 1024];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        arrived.send(()).unwrap();
        while socket.read(&mut bytes).await.unwrap() > 0 {}
    });
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let response = broker
        .publish(
            serde_json::from_value(serde_json::json!({
                "url": format!("http://{address}/hook"), "secret": "test", "payload": "body",
                "flow": { "key": "lane", "parallelism": 1 },
            }))
            .unwrap(),
        )
        .unwrap();
    broker.flush_wal().unwrap();
    let dispatch = DispatchEngine::new(
        broker,
        DispatchConfig {
            http_timeout_secs: 60,
            ..DispatchConfig::default()
        },
    );
    dispatch.enqueue(DeliveryJob::live(
        response.topic,
        response.partition.unwrap(),
        response.offset.unwrap(),
        response.message_id.unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), dispatch.shutdown_background())
        .await
        .expect("shutdown must cancel child delivery tasks, not just their parent workers");
    let snapshot = dispatch.telemetry_snapshot();
    assert_eq!(snapshot.active_http_deliveries, 0);
    assert_eq!(snapshot.in_flight_deliveries, 0);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(dispatch.flow.pending_count().await, 0);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
