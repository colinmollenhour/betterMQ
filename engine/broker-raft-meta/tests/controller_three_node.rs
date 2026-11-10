use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use broker_raft_meta::{
    ClusterConfig, ControllerAppendRequest, ControllerAppendResponse, ControllerCommand,
    ControllerSnapshotRequest, ControllerSnapshotResponse, ControllerState, NodeConfig,
    OpenRaftController, RaftVoteRequest, RaftVoteResponse,
};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use uuid::Uuid;

type ApiError = (StatusCode, String);

async fn vote(
    State(controller): State<OpenRaftController>,
    Json(request): Json<RaftVoteRequest>,
) -> Result<Json<RaftVoteResponse>, ApiError> {
    controller
        .raft()
        .vote(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

async fn append(
    State(controller): State<OpenRaftController>,
    Json(request): Json<ControllerAppendRequest>,
) -> Result<Json<ControllerAppendResponse>, ApiError> {
    controller
        .raft()
        .append_entries(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

async fn snapshot(
    State(controller): State<OpenRaftController>,
    Json(request): Json<ControllerSnapshotRequest>,
) -> Result<Json<ControllerSnapshotResponse>, ApiError> {
    controller
        .raft()
        .install_snapshot(request)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))
}

fn app(controller: OpenRaftController) -> Router {
    Router::new()
        .route("/internal/v1/controller/raft/vote", post(vote))
        .route("/internal/v1/controller/raft/append", post(append))
        .route("/internal/v1/controller/raft/snapshot", post(snapshot))
        .with_state(controller)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_http_cluster_commits_and_snapshots_controller_state() {
    catalog_cluster(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_voter_learner_receives_catalog_and_new_leader_preserves_revisions() {
    catalog_cluster(4).await;
}

async fn catalog_cluster(node_count: usize) {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..node_count {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(format!("http://{}", listener.local_addr().unwrap()));
        listeners.push(listener);
    }
    let ids: Vec<_> = (0..node_count).map(|_| Uuid::new_v4()).collect();
    let nodes: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| NodeConfig {
            internal_url: None,
            id: *id,
            addr: addresses[index].clone(),
        })
        .collect();
    let cluster_id = Uuid::new_v4();
    let dirs: Vec<_> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();
    let mut controllers = Vec::new();
    for index in 0..node_count {
        let config = ClusterConfig {
            cluster_id,
            nodes: nodes.clone(),
            node_id: ids[index],
            generation: 1,
            hash_version: 1,
        };
        let initial = ControllerState::from_v1(&config, 8, &HashMap::new(), None).unwrap();
        controllers.push(
            OpenRaftController::open(dirs[index].path(), config, initial)
                .await
                .unwrap(),
        );
    }

    let servers: Vec<_> = listeners
        .into_iter()
        .zip(controllers.iter().cloned())
        .map(|(listener, controller)| {
            tokio::spawn(async move {
                axum::serve(listener, app(controller)).await.unwrap();
            })
        })
        .collect();

    let mut leader = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(controller) = controllers
                .iter()
                .find(|controller| controller.is_leader_with_quorum())
            {
                break controller.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("three-node OpenRaft election");

    use broker_raft_meta::catalog::{CatalogKind, CatalogMutation};
    let id = Uuid::new_v4();
    let mutation = CatalogMutation {
        tenant_id: "tenant".into(),
        kind: CatalogKind::Queue,
        id,
        payload_json: serde_json::json!({"id":id,"tenant_id":"tenant","topic":"jobs"}).to_string(),
        tombstone: false,
        expected_version: None,
    };
    let committed = leader
        .submit(ControllerCommand::MutateCatalog {
            operation_id: Uuid::new_v4(),
            mutations: vec![mutation.clone()],
        })
        .await
        .unwrap();
    assert_eq!(committed.catalog_epoch, 1);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if controllers
                .iter()
                .all(|controller| controller.state().catalog_epoch == 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("controller state replicated to all three nodes");

    // Lose the current leader between mutations. The next leader's barrier must
    // precede its state read, including when its cached state is still behind.
    let previous_id = leader.raft().metrics().borrow().id;
    let old_index = controllers
        .iter()
        .position(|c| c.raft().metrics().borrow().id == previous_id)
        .unwrap();
    servers[old_index].abort();
    controllers[old_index].raft().shutdown().await.unwrap();
    leader = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(next) = controllers
                .iter()
                .enumerate()
                .find(|(index, c)| *index != old_index && c.is_leader_with_quorum())
            {
                break next.1.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("new leader after leader loss");
    let mut updated = mutation.clone();
    updated.expected_version = Some(1);
    let updated_state = leader
        .submit(ControllerCommand::MutateCatalog {
            operation_id: Uuid::new_v4(),
            mutations: vec![updated],
        })
        .await
        .unwrap();
    assert_eq!(updated_state.catalog_epoch, 2);
    assert_eq!(updated_state.catalog_records[&mutation.key()].version, 2);
    let stale = tokio::time::timeout(
        Duration::from_secs(3),
        controllers[old_index].submit_local(ControllerCommand::MutateCatalog {
            operation_id: Uuid::new_v4(),
            mutations: vec![mutation.clone()],
        }),
    )
    .await;
    assert!(!matches!(stale, Ok(Ok(_))));

    leader.trigger_snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if leader.raft().metrics().borrow().snapshot.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("OpenRaft snapshot");

    let leader_id = leader.raft().metrics().borrow().id;
    for (index, server) in servers.iter().enumerate() {
        if controllers[index].raft().metrics().borrow().id != leader_id {
            server.abort();
            let _ = controllers[index].raft().shutdown().await;
        }
    }
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if !leader.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("isolated controller leader loses quorum readiness");

    for server in servers {
        server.abort();
    }
}
