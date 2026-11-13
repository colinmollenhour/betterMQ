//! Strict FIFO successors wait in the WAL, not in a timer/re-admission loop.

use broker_dispatch::{DeliveryJob, DispatchConfig, DispatchEngine};
use broker_partition::{Broker, BrokerConfig, ResolvedFlow};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_fifo_head_does_not_admit_successors_and_completion_wakes_the_next_head() {
    std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    std::env::set_var("BETTERMQ_DISPATCH_QUEUE_CAP", "32");
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::new(dir.path().to_owned());
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
    for sequence in 0..512 {
        let response = broker
            .publish(
                serde_json::from_value(serde_json::json!({
                    "url": format!("{}/hook", sink.uri()), "secret": "test",
                    "payload": sequence.to_string(), "routing_key": "entity",
                    "flow": {"key": "lane", "parallelism": 1},
                }))
                .unwrap(),
            )
            .unwrap();
        jobs.push(DeliveryJob::live(
            response.topic,
            0,
            response.offset.unwrap(),
            response.message_id.unwrap(),
        ));
    }
    broker.flush_wal().unwrap();
    let first = broker
        .read_message(&jobs[0].topic, 0, jobs[0].offset)
        .unwrap();
    let owner = broker_partition::flow_lane_owner(&first);
    // Do not start workers until the pause is installed: deliberately deliver
    // live notifications in reverse order, dropping the oldest on overflow.
    let leading = Arc::new(AtomicBool::new(false));
    let check = leading.clone();
    let dispatch = DispatchEngine::new_with_controls(
        broker,
        DispatchConfig::default(),
        true,
        None,
        Some(Arc::new(move |_| check.load(Ordering::Acquire))),
    );
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
    leading.store(true, Ordering::Release);
    for job in jobs.iter().rev() {
        dispatch.enqueue(job.clone());
    }
    for _ in 0..20 {
        let engine = dispatch.clone();
        tokio::task::spawn_blocking(move || engine.backfill_pending())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let parked = dispatch.flow.get(owner, "lane").await.unwrap();
    assert_eq!(
        parked.wait_list_size, 1,
        "only the durable head belongs in the flow waitlist; successors stay in the WAL"
    );
    assert_eq!(
        dispatch.telemetry_snapshot().deferred_jobs,
        0,
        "a blocked FIFO predecessor is not a timer retry"
    );
    dispatch.flow.resume(owner, "lane").await;
    // No polling backfill at all: completion must hand the next indexed head
    // directly to admission instead of paying the server's 25ms scan debounce.
    tokio::time::timeout(Duration::from_secs(10), async {
        while order.lock().unwrap().len() != jobs.len() || dispatch.flow.pending_count().await != 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("completion-driven FIFO handoff must drain without a backfill timer");
    assert_eq!(*order.lock().unwrap(), (0..512).collect::<Vec<_>>());
    assert_eq!(dispatch.telemetry_snapshot().deferred_jobs, 0);
    dispatch.shutdown_background().await;
}
