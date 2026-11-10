//! Cluster membership, peer health, shard leader election, scheduler lease.
//!
//! Peer health still uses gossip. Controller state is an OpenRaft state
//! machine: commands are replicated as log entries and stored per entity.

mod cluster;
mod controller;
mod controller_store;
mod durable_store;
mod election;
mod epoch;
mod network;
mod placement;
mod rocks_store;
mod telemetry;

pub use cluster::{ClusterConfig, ClusterError, ClusterRuntime, NodeConfig, SchedulerLease};
pub use controller::{
    raft_id, ControllerAppendRequest, ControllerAppendResponse, ControllerCommand,
    ControllerCommandResponse, ControllerError, ControllerNode, ControllerRaft,
    ControllerRejection, ControllerSnapshotRequest, ControllerSnapshotResponse, ControllerState,
    ControllerVoteRequest as RaftVoteRequest, ControllerVoteResponse as RaftVoteResponse,
    DrainReport, FanoutOutboxEntry, GcWatermarkProposal, OpenRaftController, ShardPlacement,
    TypeConfig, CONTROLLER_RESPONSE_APPLIED_INDEX, CONTROLLER_RESPONSE_HEADER,
    FANOUT_MAX_ENTRY_BYTES, FANOUT_OUTBOX_MAX_PENDING_BYTES,
};
pub use durable_store::DurableRocksStore;
pub use election::{elect_shard_leader, DEFAULT_PEER_TTL_MS};
pub use epoch::{
    ControllerEpoch, ControllerLeaderProof, ControllerVoteRequest, ControllerVoteResponse,
    EpochError,
};
pub use placement::{
    initial_controller_voters, place_replicas, replication_policy, CatalogRecord, DataNodeRecord,
    RebalanceOp, DEFAULT_MIN_ISR, DEFAULT_RF,
};
pub use rocks_store::RocksStore;
pub use telemetry::{controller_telemetry_snapshot, ControllerTelemetrySnapshot};
pub mod catalog;
