//! End-to-end shard failover on the production wiring: `open_with_raft`
//! runtimes, the full broker router over HTTP, the cluster health monitor
//! (placement, catch-up, fencing) and leader-only dispatch.

use crate::cluster_health::spawn_cluster_health_monitor;
use broker_api::{AppState, CatalogTombstones, Cluster};
use broker_dispatch::{DeliveryJob, DispatchConfig, DispatchEngine, LeaseTable};
use broker_partition::{Broker, BrokerConfig, PublishRequest, PublishResponse, DIRECT_TOPIC};
use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Node {
    state: Arc<AppState>,
    tasks: Vec<JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl Node {
    fn cluster(&self) -> &Cluster {
        self.state.cluster.as_ref().expect("cluster node")
    }

    fn leads_ready(&self, shard: u32) -> bool {
        self.cluster().runtime.is_leader_for_shard(shard)
    }

    /// Hard-stop: HTTP, health monitor and the OpenRaft core go away, so
    /// peers see neither health probes, replication nor controller traffic.
    async fn kill(self) {
        for task in &self.tasks {
            task.abort();
        }
        if let Some(controller) = self.cluster().runtime.controller() {
            let _ = controller.raft().shutdown().await;
        }
    }
}

async fn start_cluster(count: usize) -> Vec<Node> {
    std::env::set_var("BETTERMQ_CLUSTER_SECRET", "failover-test-secret");
    std::env::set_var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS", "1");
    let mut listeners = Vec::new();
    let mut nodes = Vec::new();
    for _ in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        nodes.push(NodeConfig {
            internal_url: None,
            id: Uuid::new_v4(),
            addr: format!("http://{}", listener.local_addr().unwrap()),
        });
        listeners.push(listener);
    }
    let cluster_id = Uuid::new_v4();
    let mut started = Vec::new();
    for (listener, node) in listeners.into_iter().zip(&nodes) {
        let dir = tempfile::tempdir().unwrap();
        let config = ClusterConfig {
            cluster_id,
            nodes: nodes.clone(),
            node_id: node.id,
            generation: 1,
            hash_version: 1,
        };
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let runtime = ClusterRuntime::open_with_raft(dir.path(), config, broker.shard_count())
            .await
            .unwrap();
        let cluster = Cluster::new(runtime);
        let rt = cluster.runtime.clone();
        broker.set_shard_leader_check(Arc::new(move |p| {
            rt.is_leader_for_shard(p).then(|| rt.shard_generation(p))
        }));
        let rt = cluster.runtime.clone();
        let leases = LeaseTable::new();
        let dispatch = DispatchEngine::new(broker.clone(), DispatchConfig::default())
            .with_leases(leases.clone())
            .with_shard_leader_check(Arc::new(move |p| rt.is_leader_for_shard(p)));
        let state = Arc::new(AppState {
            schedule: ScheduleQueue::open(dir.path()).unwrap(),
            crons: CronRegistry::open(dir.path()).unwrap(),
            catalog_tombstones: CatalogTombstones::open(dir.path()).unwrap(),
            broker,
            dispatch: dispatch.clone(),
            leases,
            cluster: Some(cluster.clone()),
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        let app = broker_api::router((*state).clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let monitor = spawn_cluster_health_monitor(cluster, dispatch, state.clone());
        started.push(Node {
            state,
            tasks: vec![server, monitor],
            _dir: dir,
        });
    }
    started
}

async fn eventually<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn leader_of(nodes: &[Node], shard: u32) -> Option<usize> {
    nodes.iter().position(|node| node.leads_ready(shard))
}

/// The ready leader and term of `shard` once every node agrees on them and
/// they survive a full health/placement cycle. Startup placement churn would
/// otherwise move the shard (and its redelivery-on-gain backfill) mid-test.
async fn settled_leader(nodes: &[Node], shard: u32) -> (usize, u64) {
    let observe = || {
        let index = leader_of(nodes, shard)?;
        let runtime = &nodes[index].cluster().runtime;
        let (leader_id, term) = (runtime.config().node_id, runtime.shard_generation(shard));
        let shard_count = nodes[index].state.broker.shard_count();
        nodes
            .iter()
            .all(|node| {
                let view = &node.cluster().runtime;
                view.elect_leader_for_shard(shard) == Some(leader_id)
                    && view.shard_generation(shard) == term
            })
            .then_some(())?;
        (0..shard_count)
            .all(|other| leader_of(nodes, other).is_some())
            .then_some((index, term))
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "shard {shard} placement never settled"
        );
        if let Some(seen) = observe() {
            tokio::time::sleep(Duration::from_secs(3)).await;
            if observe() == Some(seen) {
                return seen;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn webhook_bodies(hook: &MockServer) -> Vec<String> {
    hook.received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect()
}

async fn publish_on(node: &Node, hook: &MockServer, payload: &str) -> PublishResponse {
    let request = PublishRequest {
        routing_id: None,
        topic: DIRECT_TOPIC.into(),
        routing_key: "failover".into(),
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
        max_retries: Some(0),
        retry_backoff: None,
        method: None,
        headers: None,
        sign: None,
        request: None,
        url: Some(format!("{}/hook", hook.uri())),
        secret: Some("hook-secret".into()),
    };
    let response = broker_api::publish_with_cluster(&node.state, request, None)
        .await
        .expect("quorum-acknowledged publish");
    broker_api::enqueue_dispatch_after_publish(&node.state, &response);
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acknowledged_message_survives_shard_leader_loss_and_only_leaders_deliver() {
    let hook = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&hook)
        .await;
    let mut nodes = start_cluster(3).await;
    let shard = nodes[0].state.broker.assign_shard(DIRECT_TOPIC, "failover");

    let (leader, _) = settled_leader(&nodes, shard).await;
    let first = publish_on(&nodes[leader], &hook, "before-failover").await;
    let offset = first.offset.unwrap();

    eventually(
        "every replica to hold the record",
        Duration::from_secs(10),
        || async {
            nodes.iter().all(|node| {
                node.state
                    .broker
                    .committed_hwm(DIRECT_TOPIC, shard)
                    .is_ok_and(|hwm| hwm > offset)
            })
        },
    )
    .await;
    eventually("the leader's delivery", Duration::from_secs(10), || async {
        !webhook_bodies(&hook).await.is_empty()
    })
    .await;

    // Followers hold the replicated record but are fenced from delivering it.
    for (index, node) in nodes.iter().enumerate() {
        if index != leader {
            node.state.dispatch.enqueue(DeliveryJob::live(
                DIRECT_TOPIC,
                shard,
                offset,
                first.message_id.unwrap(),
            ));
            node.state.dispatch.backfill_pending();
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        webhook_bodies(&hook).await.len(),
        1,
        "only the shard leader may deliver"
    );

    nodes.remove(leader).kill().await;
    eventually(
        "a surviving replica to take over the shard",
        Duration::from_secs(45),
        || async { leader_of(&nodes, shard).is_some() },
    )
    .await;
    let successor = leader_of(&nodes, shard).unwrap();
    let survived = nodes[successor]
        .state
        .broker
        .read_message(DIRECT_TOPIC, shard, offset)
        .unwrap();
    assert_eq!(survived.payload, b"before-failover");

    publish_on(&nodes[successor], &hook, "after-failover").await;
    eventually(
        "delivery from the new leader",
        Duration::from_secs(10),
        || async {
            webhook_bodies(&hook)
                .await
                .iter()
                .any(|body| body.contains("after-failover"))
        },
    )
    .await;

    for node in nodes {
        node.kill().await;
    }
}
