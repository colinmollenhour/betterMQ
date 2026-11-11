//! Shared fixtures for unit tests.

use crate::AppState;
use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable};
use broker_partition::{Broker, BrokerConfig};
use broker_schedule::{CronRegistry, ScheduleQueue};
use std::sync::Arc;

/// App state for a configured three-node cell (no peers reachable).
pub(crate) fn cell_state(dir: &tempfile::TempDir) -> Arc<AppState> {
    let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
    let ids: Vec<uuid::Uuid> = (0..3).map(|_| uuid::Uuid::new_v4()).collect();
    let config = broker_raft_meta::ClusterConfig {
        cluster_id: uuid::Uuid::new_v4(),
        nodes: ids
            .iter()
            .enumerate()
            .map(|(index, id)| broker_raft_meta::NodeConfig {
                id: *id,
                addr: format!("http://broker{index}:8080"),
                internal_url: None,
            })
            .collect(),
        node_id: ids[0],
        generation: 1,
        hash_version: 1,
    };
    Arc::new(AppState {
        broker: broker.clone(),
        schedule: ScheduleQueue::open(dir.path()).unwrap(),
        crons: CronRegistry::open(dir.path()).unwrap(),
        dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default()),
        leases: LeaseTable::new(),
        cluster: Some(crate::cluster::ClusterHandle::new(
            broker_raft_meta::ClusterRuntime::from_config_only(config),
        )),
        local_auth: None,
        fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
        catalog_tombstones: crate::catalog_tombstones::CatalogTombstones::open(dir.path()).unwrap(),
        dispatch_fleet: false,
        broker_only: false,
        #[cfg(feature = "cloud")]
        auth: None,
        #[cfg(feature = "cloud")]
        control_plane: None,
    })
}
