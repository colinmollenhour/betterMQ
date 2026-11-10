//! OpenRaft-backed durable cluster controller.
//!
//! OpenRaft owns terms, votes, membership, replication, and snapshots. Each
//! controller mutation is replicated as a `ControllerCommand` log entry and
//! applied deterministically by every node's state machine, so a command
//! always builds on the latest committed state.

use crate::cluster::{ClusterConfig, SchedulerLease};
use crate::durable_store::DurableRocksStore;
use crate::election::eligible_shard_leaders;
use crate::network::HttpNetworkFactory;
use crate::placement::{
    catalog_key, initial_controller_voters, min_isr_for, pick_leader, place_replicas,
    replication_policy, upsert_catalog, CatalogRecord, DataNodeRecord, RebalanceOp,
};
pub use crate::rocks_store::TypeConfig;
use crate::rocks_store::{RocksRequest, RocksStore};
use openraft::storage::Adaptor;
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type NodeId = u64;
pub type ControllerRaft = Raft<TypeConfig>;
pub type ControllerAppendRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type ControllerAppendResponse = openraft::raft::AppendEntriesResponse<NodeId>;
pub type ControllerVoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type ControllerVoteResponse = openraft::raft::VoteResponse<NodeId>;
pub type ControllerSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type ControllerSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;

pub(crate) const STATE_KEY: &str = "bettermq/controller-state/v1";
const QUORUM_ACK_MAX_AGE_MS: u64 = 2_000;

/// Largest accepted fan-out command (`payload_json`) or outcome record
/// (`progress_json`). Keeps every controller log entry well inside one Raft
/// append request.
pub const FANOUT_MAX_ENTRY_BYTES: usize = 4 * 1024 * 1024;
/// Admission limit for the accepted bodies of fan-outs that are not complete.
pub const FANOUT_OUTBOX_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

/// One independently persisted part of the controller state. Each command
/// reports the parts it changed so the state machine rewrites only those.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StateKey {
    /// Scalars, node tables, scheduler lease and replication policy.
    Meta,
    Shard(u32),
    Fanout(Uuid),
    Catalog(String),
    GcWatermark(u32),
    Rebalance(u32),
    CatalogOperation(Uuid),
    LogGcWatermark {
        namespace: String,
        partition: u32,
    },
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("catalog conflict: {0}")]
    CatalogConflict(String),
    #[error("catalog unavailable: {0}")]
    CatalogUnavailable(String),
    #[error("openraft: {0}")]
    Raft(String),
    #[error("controller storage: {0}")]
    Storage(String),
    #[error("controller transport: {0}")]
    Transport(String),
    #[error("controller has no known leader")]
    NoLeader,
    #[error("controller node id collision between {first} and {second}")]
    NodeIdCollision { first: Uuid, second: Uuid },
    #[error("unknown controller node {0}")]
    UnknownNode(Uuid),
    #[error("invalid controller command: {0}")]
    InvalidCommand(String),
}

/// Why the replicated state machine rejected a controller command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControllerRejection {
    UnknownNode(Uuid),
    InvalidCommand(String),
}

impl From<ControllerError> for ControllerRejection {
    fn from(error: ControllerError) -> Self {
        match error {
            ControllerError::UnknownNode(id) => Self::UnknownNode(id),
            ControllerError::InvalidCommand(message) => Self::InvalidCommand(message),
            other => Self::InvalidCommand(other.to_string()),
        }
    }
}

impl From<ControllerRejection> for ControllerError {
    fn from(rejection: ControllerRejection) -> Self {
        match rejection {
            ControllerRejection::UnknownNode(id) => Self::UnknownNode(id),
            ControllerRejection::InvalidCommand(message) => Self::InvalidCommand(message),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerNode {
    pub id: Uuid,
    pub raft_id: NodeId,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardPlacement {
    pub shard: u32,
    pub leader_id: Uuid,
    pub leader_term: u64,
    pub replicas: Vec<Uuid>,
    #[serde(default)]
    pub learners: Vec<Uuid>,
    #[serde(default)]
    pub isr: Vec<Uuid>,
    /// Leader's durable log end when it last reported the ISR: the physical
    /// shard log end for V2 layouts, 0 where a shard has one log per topic.
    #[serde(default)]
    pub committed_hwm: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FanoutOutboxEntry {
    pub id: Uuid,
    pub tenant_id: String,
    #[serde(default)]
    pub acceptance_hash: String,
    pub version: u64,
    pub authority_term: u64,
    pub owner_id: Option<Uuid>,
    pub lease_expires_at_ms: i64,
    /// Accepted fan-out command; immutable after acceptance and shared, not
    /// copied, between state snapshots.
    pub payload_json: Arc<str>,
    /// Latest per-member outcomes, replaced by each `UpdateFanout`. Empty
    /// until the first update; entries written before this field existed
    /// carry their outcomes inside `payload_json`.
    #[serde(default)]
    pub progress_json: String,
    pub completed: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerState {
    #[serde(default)]
    pub catalog_authoritative: bool,
    #[serde(default)]
    pub catalog_frozen: bool,
    #[serde(default)]
    pub catalog_operations: BTreeMap<Uuid, crate::catalog::CatalogOperation>,
    pub version: u32,
    pub cluster_id: Uuid,
    pub membership_generation: u64,
    pub nodes: BTreeMap<NodeId, ControllerNode>,
    pub shards: HashMap<u32, ShardPlacement>,
    pub catalog_epoch: u64,
    pub scheduler: Option<SchedulerLease>,
    #[serde(default)]
    pub fanout_outbox: BTreeMap<Uuid, FanoutOutboxEntry>,
    #[serde(default)]
    pub data_nodes: BTreeMap<Uuid, DataNodeRecord>,
    #[serde(default)]
    pub controller_voters: Vec<Uuid>,
    #[serde(default)]
    pub replication_factor: u32,
    #[serde(default)]
    pub min_isr: u32,
    #[serde(default)]
    pub catalog_records: BTreeMap<String, CatalogRecord>,
    /// Legacy per-shard watermark from the removed `SetGcWatermark` command.
    /// It was accepted unvalidated, so it is kept only for decoding old state
    /// and never consumed; see `log_gc_watermarks`.
    #[serde(default)]
    pub gc_watermarks: HashMap<u32, u64>,
    #[serde(default)]
    pub rebalance: BTreeMap<u32, RebalanceOp>,
    /// Committed retention watermark (exclusive) per log: storage namespace
    /// (V1 topic, or `__physical` in V2) → partition → offset. Every replica
    /// deletes below `min(watermark, local committed HWM)`.
    #[serde(default)]
    pub log_gc_watermarks: BTreeMap<String, BTreeMap<u32, u64>>,
}

impl ControllerState {
    /// Committed retention watermark of one log (0 when none).
    pub fn log_gc_watermark(&self, namespace: &str, partition: u32) -> u64 {
        self.log_gc_watermarks
            .get(namespace)
            .and_then(|partitions| partitions.get(&partition))
            .copied()
            .unwrap_or(0)
    }
}

/// One log's proposed retention watermark, computed by its shard leader.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcWatermarkProposal {
    pub namespace: String,
    pub partition: u32,
    /// Shard leader term the watermark was computed under.
    pub leader_term: u64,
    pub watermark: u64,
}

impl ControllerState {
    pub fn from_v1(
        config: &ClusterConfig,
        shard_count: u32,
        shard_terms: &HashMap<u32, u64>,
        scheduler: Option<SchedulerLease>,
    ) -> Result<Self, ControllerError> {
        let mut nodes: BTreeMap<NodeId, ControllerNode> = BTreeMap::new();
        for node in &config.nodes {
            let raft_id = raft_id(node.id);
            if let Some(existing) = nodes.get(&raft_id) {
                if existing.id != node.id {
                    return Err(ControllerError::NodeIdCollision {
                        first: existing.id,
                        second: node.id,
                    });
                }
            }
            nodes.insert(
                raft_id,
                ControllerNode {
                    id: node.id,
                    raft_id,
                    addr: node.internal_base().to_owned(),
                },
            );
        }
        let provisional: Vec<_> = config
            .nodes
            .iter()
            .map(|node| DataNodeRecord {
                id: node.id,
                addr: node.internal_base().to_owned(),
                rack: None,
                region: None,
                broker: true,
                controller_voter: false,
            })
            .collect();
        let voters = initial_controller_voters(&provisional);
        let data_nodes: BTreeMap<_, _> = provisional
            .into_iter()
            .map(|mut node| {
                node.controller_voter = voters.contains(&node.id);
                (node.id, node)
            })
            .collect();
        let node_list: Vec<_> = data_nodes.values().cloned().collect();
        let (replication_factor, min_isr) = replication_policy(node_list.len());
        let mut shards = HashMap::new();
        for shard in 0..shard_count.max(1) {
            let replicas = place_replicas(&node_list, shard, replication_factor as usize);
            let leader_id = pick_leader(&replicas, shard)
                .or_else(|| node_list.first().map(|n| n.id))
                .unwrap_or(config.node_id);
            shards.insert(
                shard,
                ShardPlacement {
                    shard,
                    leader_id,
                    leader_term: shard_terms.get(&shard).copied().unwrap_or(0),
                    isr: replicas.clone(),
                    learners: Vec::new(),
                    replicas,
                    committed_hwm: 0,
                },
            );
        }
        Ok(Self {
            catalog_authoritative: false,
            catalog_frozen: false,
            catalog_operations: BTreeMap::new(),
            version: 1,
            cluster_id: config.cluster_id,
            membership_generation: config.generation,
            nodes,
            shards,
            catalog_epoch: 0,
            scheduler,
            fanout_outbox: BTreeMap::new(),
            controller_voters: initial_controller_voters(&node_list),
            data_nodes,
            replication_factor,
            min_isr,
            catalog_records: BTreeMap::new(),
            gc_watermarks: HashMap::new(),
            rebalance: BTreeMap::new(),
            log_gc_watermarks: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControllerCommand {
    MutateCatalog {
        operation_id: Uuid,
        #[serde(default)]
        request_fingerprint: Option<String>,
        mutations: Vec<crate::catalog::CatalogMutation>,
    },
    BeginCatalogActivation,
    SeedCatalog {
        operation_id: Uuid,
        records: Vec<crate::catalog::CatalogMutation>,
    },
    AcquireShardLeadership {
        shard: u32,
        leader_id: Uuid,
    },
    ObserveShardTerm {
        shard: u32,
        leader_id: Uuid,
        term: u64,
    },
    AcquireScheduler {
        holder: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    },
    AdvanceCatalogEpoch,
    AcceptFanout {
        id: Uuid,
        tenant_id: String,
        acceptance_hash: String,
        payload_json: String,
        created_at_ms: i64,
        controller_term: u64,
    },
    ClaimFanout {
        id: Uuid,
        owner_id: Uuid,
        expected_version: u64,
        now_ms: i64,
        ttl_ms: i64,
        controller_term: u64,
    },
    UpdateFanout {
        id: Uuid,
        owner_id: Uuid,
        expected_version: u64,
        /// Outcome record only; the accepted command is never resent.
        progress_json: String,
        completed: bool,
        now_ms: i64,
        controller_term: u64,
    },
    GarbageCollectFanout {
        ids: Vec<Uuid>,
        completed_before_ms: i64,
        controller_term: u64,
    },
    RegisterDataNode {
        node: DataNodeRecord,
    },
    PutCatalogRecord {
        kind: String,
        key: String,
        payload_json: String,
        tombstone: bool,
    },
    BeginRebalance {
        shard: u32,
        add: Option<Uuid>,
        remove: Option<Uuid>,
    },
    /// Submitted by the shard leader once it has verified the learner holds
    /// its whole log; rejected from anyone else.
    PromoteLearner {
        shard: u32,
        node_id: Uuid,
        leader_id: Uuid,
        leader_term: u64,
    },
    /// The shard leader's measured in-sync replica set.
    UpdateIsr {
        shard: u32,
        leader_id: Uuid,
        leader_term: u64,
        isr: Vec<Uuid>,
        committed_hwm: u64,
    },
    CompleteRebalance {
        shard: u32,
    },
    /// Batch of retention watermarks from one node. Each entry is accepted
    /// only if `leader_id` is the shard's placement leader at exactly
    /// `leader_term` and the watermark does not move backwards; rejected
    /// entries are skipped (the command fails only if nothing applied).
    SetGcWatermarks {
        leader_id: Uuid,
        entries: Vec<GcWatermarkProposal>,
    },
    SetReplicationPolicy {
        factor: u32,
        min_isr: u32,
    },
    AddControllerVoter {
        node_id: Uuid,
    },
    RemoveControllerVoter {
        node_id: Uuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainReport {
    pub node_id: Uuid,
    pub shards: Vec<u32>,
    pub leadership_transfers: Vec<u32>,
}

/// Request header a forwarding node sets when it will read the result from its
/// own replica: the leader then answers with the applied log index only.
pub const CONTROLLER_RESPONSE_HEADER: &str = "x-bettermq-controller-response";
pub const CONTROLLER_RESPONSE_APPLIED_INDEX: &str = "applied-index";

/// How long a forwarding node waits for its replica to apply its own command.
const FORWARDED_APPLY_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerCommandResponse {
    /// Full state, only for callers that do not hold a controller replica.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Arc<ControllerState>>,
    /// Log index at which the command was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_index: Option<u64>,
}

#[derive(Clone)]
pub struct OpenRaftController {
    raft: ControllerRaft,
    store: Arc<RocksStore>,
    node_id: NodeId,
    config: ClusterConfig,
    bootstrap: Arc<ControllerState>,
    last_leader_contact_ms: Arc<AtomicI64>,
    command_lock: Arc<Mutex<()>>,
    http: reqwest::Client,
}

impl OpenRaftController {
    pub async fn open(
        data_dir: &Path,
        config: ClusterConfig,
        bootstrap_state: ControllerState,
    ) -> Result<Self, ControllerError> {
        let node_id = raft_id(config.node_id);
        let store = RocksStore::new(data_dir.join("controller-rocksdb"))
            .await
            .map_err(|error| ControllerError::Storage(error.to_string()))?;
        let durable_store = DurableRocksStore::new(Arc::clone(&store));
        let (log_store, state_machine) = Adaptor::new(durable_store);
        let network = HttpNetworkFactory::new()
            .map_err(|error| ControllerError::Transport(error.to_string()))?
            .with_config(&config);
        let raft_config = Config {
            cluster_name: config.cluster_id.to_string(),
            heartbeat_interval: 250,
            election_timeout_min: 750,
            election_timeout_max: 1_500,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1_000),
            replication_lag_threshold: 2_000,
            max_in_snapshot_log_to_keep: 256,
            ..Default::default()
        }
        .validate()
        .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let raft = Raft::new(
            node_id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        let controller = Self {
            raft,
            store,
            node_id,
            config,
            bootstrap: Arc::new(bootstrap_state),
            last_leader_contact_ms: Arc::new(AtomicI64::new(0)),
            command_lock: Arc::new(Mutex::new(())),
            http,
        };
        controller.initialize_membership().await?;
        controller.spawn_state_sync();
        Ok(controller)
    }

    async fn initialize_membership(&self) -> Result<(), ControllerError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?
        {
            return Ok(());
        }
        let state = self.state();
        let voter_ids: BTreeSet<_> = if state.controller_voters.is_empty() {
            self.config.nodes.iter().map(|node| node.id).collect()
        } else {
            state.controller_voters.iter().copied().collect()
        };
        // A non-voter must wait for the elected leader to add it as a learner;
        // OpenRaft refuses initialize() when the local ID is not a voter.
        if !voter_ids.contains(&self.config.node_id) {
            return Ok(());
        }
        let members: BTreeMap<_, _> = self
            .config
            .nodes
            .iter()
            .filter(|node| voter_ids.contains(&node.id))
            .map(|node| (raft_id(node.id), BasicNode::new(node.internal_base())))
            .collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))
    }

    /// Bootstraps the migrated state and reconciles membership on the leader.
    /// Committed state itself is published by the state machine on apply, so
    /// nothing here re-reads or decodes it.
    fn spawn_state_sync(&self) {
        let controller = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                if !controller.is_leader_with_quorum() {
                    continue;
                }
                if controller.store.controller_state().is_none() {
                    if let Err(error) = controller.bootstrap_local().await {
                        tracing::warn!(%error, "failed to commit migrated controller state");
                    }
                }
                if let Err(error) = controller.reconcile_membership().await {
                    tracing::warn!(%error, "failed to reconcile OpenRaft membership");
                }
            }
        });
    }

    /// Commit the migrated V1 state as the initial controller state. The state
    /// machine ignores this once any controller state exists.
    async fn bootstrap_local(&self) -> Result<(), ControllerError> {
        self.raft
            .client_write(RocksRequest::ControllerBootstrap {
                state: Box::new(self.bootstrap.as_ref().clone()),
            })
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        Ok(())
    }

    fn committed_state(&self) -> Result<Arc<ControllerState>, ControllerError> {
        self.store
            .controller_state()
            .ok_or_else(|| ControllerError::Storage("controller state is not bootstrapped".into()))
    }

    fn is_raft_member(&self) -> bool {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == self.node_id)
    }

    pub async fn submit(
        &self,
        command: ControllerCommand,
    ) -> Result<Arc<ControllerState>, ControllerError> {
        if self.is_leader_with_quorum() {
            return self.submit_local(command).await;
        }
        let leader = self
            .raft
            .metrics()
            .borrow()
            .current_leader
            .ok_or(ControllerError::NoLeader)?;
        let addr = self
            .state()
            .nodes
            .get(&leader)
            .map(|node| {
                self.config
                    .nodes
                    .iter()
                    .find(|entry| entry.id == node.id)
                    .map(|entry| entry.internal_base().to_owned())
                    .unwrap_or_else(|| node.addr.clone())
            })
            .ok_or(ControllerError::NoLeader)?;
        let url = format!(
            "{}/internal/v1/controller/command",
            addr.trim_end_matches('/')
        );
        // A replica reads the result locally once it has applied the entry, so
        // the leader does not ship the whole controller state back.
        let read_locally = self.is_raft_member();
        let mut request = self.http.post(url).json(&command);
        if read_locally {
            request = request.header(
                CONTROLLER_RESPONSE_HEADER,
                CONTROLLER_RESPONSE_APPLIED_INDEX,
            );
        }
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                request = request.header("x-bettermq-cluster-secret", secret);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            if response.status() == reqwest::StatusCode::CONFLICT {
                return Err(ControllerError::CatalogConflict(
                    response.text().await.unwrap_or_default(),
                ));
            }
            return Err(ControllerError::Transport(format!(
                "controller leader returned {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        let response: ControllerCommandResponse = response
            .json()
            .await
            .map_err(|error| ControllerError::Transport(error.to_string()))?;
        if let Some(state) = response.state {
            return Ok(state);
        }
        let index = response.applied_index.ok_or_else(|| {
            ControllerError::Transport("controller leader returned neither state nor index".into())
        })?;
        self.raft
            .wait(Some(FORWARDED_APPLY_WAIT))
            .applied_index_at_least(Some(index), "forwarded controller command")
            .await
            .map_err(|error| {
                ControllerError::Transport(format!(
                    "command committed at index {index} but not yet applied locally: {error}"
                ))
            })?;
        self.committed_state()
    }

    /// Serve a command forwarded by another node.
    pub async fn serve_forwarded(
        &self,
        command: ControllerCommand,
        applied_index_only: bool,
    ) -> Result<ControllerCommandResponse, ControllerError> {
        if self.is_leader_with_quorum() {
            let (state, index) = self.submit_local_indexed(command).await?;
            return Ok(ControllerCommandResponse {
                state: (!applied_index_only).then_some(state),
                applied_index: Some(index),
            });
        }
        Ok(ControllerCommandResponse {
            state: Some(self.submit(command).await?),
            applied_index: None,
        })
    }

    pub async fn submit_local(
        &self,
        command: ControllerCommand,
    ) -> Result<Arc<ControllerState>, ControllerError> {
        self.submit_local_indexed(command)
            .await
            .map(|(state, _)| state)
    }

    async fn submit_local_indexed(
        &self,
        mut command: ControllerCommand,
    ) -> Result<(Arc<ControllerState>, u64), ControllerError> {
        let _guard = self.command_lock.lock().await;
        let now = chrono::Utc::now().timestamp_millis();
        let term = self.term();
        match &mut command {
            ControllerCommand::AcquireScheduler { now_ms, .. } => *now_ms = now,
            ControllerCommand::AcceptFanout {
                controller_term,
                created_at_ms,
                ..
            } => {
                *controller_term = term;
                if *created_at_ms <= 0 {
                    *created_at_ms = now;
                }
            }
            ControllerCommand::ClaimFanout {
                now_ms,
                controller_term,
                ..
            }
            | ControllerCommand::UpdateFanout {
                now_ms,
                controller_term,
                ..
            } => {
                *now_ms = now;
                *controller_term = term;
            }
            ControllerCommand::GarbageCollectFanout {
                controller_term, ..
            } => *controller_term = term,
            _ => {}
        }
        // Barrier under the command lock, before the read: once it returns, this
        // leader has applied every entry committed before the call, so the
        // pre-check below sees current state.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        let current = match self.store.controller_state() {
            Some(state) => state,
            None => {
                self.bootstrap_local().await?;
                self.committed_state()?
            }
        };
        // Rejected commands are not written to the log. The command lock keeps
        // other local commands from landing between this check and the write;
        // the state machine still re-validates against the committed state.
        let mut probe = current.as_ref().clone();
        drop(current);
        apply_replicated(&mut probe, command.clone(), term)?;
        drop(probe);
        let response = self
            .raft
            .client_write(RocksRequest::Controller { command })
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        if let Some(rejected) = response.data.rejected {
            return Err(rejected.into());
        }
        Ok((self.committed_state()?, response.log_id.index))
    }

    async fn reconcile_membership(&self) -> Result<(), ControllerError> {
        let state = self.state();
        let desired: BTreeSet<_> = if state.controller_voters.is_empty() {
            self.config
                .nodes
                .iter()
                .map(|node| raft_id(node.id))
                .collect()
        } else {
            state
                .controller_voters
                .iter()
                .map(|id| raft_id(*id))
                .collect()
        };
        let (current, members): (BTreeSet<_>, BTreeSet<_>) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            let membership = metrics.membership_config.membership();
            (
                membership.voter_ids().collect(),
                membership.nodes().map(|(id, _)| *id).collect(),
            )
        };
        // Every broker joins as a learner first (voters are promoted below);
        // the others stay non-voting learners, so brokers added after
        // bootstrap receive catalog records and committed placement and can
        // validate replication epochs.
        let mut node_by_raft: BTreeMap<NodeId, String> = BTreeMap::new();
        for node in &self.config.nodes {
            node_by_raft.insert(raft_id(node.id), node.internal_base().to_owned());
        }
        for node in state.data_nodes.values().filter(|node| node.broker) {
            node_by_raft
                .entry(raft_id(node.id))
                .or_insert_with(|| node.addr.clone());
        }
        for (id, addr) in &node_by_raft {
            if !members.contains(id) {
                self.raft
                    .add_learner(*id, BasicNode::new(addr), false)
                    .await
                    .map_err(|error| ControllerError::Raft(error.to_string()))?;
            }
        }
        if desired == current {
            return Ok(());
        }
        // Removed voters that are still data nodes are re-added as learners on
        // the next pass; decommissioned ones drop out of the group.
        self.raft
            .change_membership(desired, false)
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))?;
        Ok(())
    }

    pub fn raft(&self) -> &ControllerRaft {
        &self.raft
    }

    /// Latest committed controller state (the migrated bootstrap state until
    /// the first commit). Cheap: shares the snapshot the state machine
    /// published instead of copying it.
    pub fn state(&self) -> Arc<ControllerState> {
        self.store
            .controller_state()
            .unwrap_or_else(|| Arc::clone(&self.bootstrap))
    }

    pub fn catalog_revision(&self) -> u64 {
        self.state().catalog_epoch
    }

    pub fn catalog_writes_frozen(&self) -> bool {
        self.state().catalog_frozen
    }

    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    pub fn leader_uuid(&self) -> Option<Uuid> {
        let metrics = self.raft.metrics();
        let leader = metrics.borrow().current_leader?;
        self.state().nodes.get(&leader).map(|node| node.id)
    }

    /// Move shard replicas off `node_id` before dropping membership.
    /// Refuses if the node is the last ISR copy of any shard.
    pub async fn drain_data_node(&self, node_id: Uuid) -> Result<DrainReport, ControllerError> {
        let state = self.state();
        let min_isr = state.min_isr.max(1) as usize;
        let mut shards = Vec::new();
        for placement in state.shards.values() {
            let holds = placement.replicas.contains(&node_id)
                || placement.isr.contains(&node_id)
                || placement.leader_id == node_id
                || placement.learners.contains(&node_id);
            if holds {
                shards.push(placement.shard);
            }
        }
        let last_copy = drain_last_copy_shards(&state, node_id, min_isr);
        if !last_copy.is_empty() {
            return Err(ControllerError::InvalidCommand(format!(
                "refuse drain: node is last ISR copy for shards {last_copy:?} (minISR={min_isr})"
            )));
        }
        shards.sort_unstable();
        let mut leadership_transfers = Vec::new();
        for shard in &shards {
            let placement =
                self.state().shards.get(shard).cloned().ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
            if placement.leader_id == node_id {
                if let Some(next) = placement.isr.iter().copied().find(|id| *id != node_id) {
                    self.submit(ControllerCommand::AcquireShardLeadership {
                        shard: *shard,
                        leader_id: next,
                    })
                    .await?;
                    leadership_transfers.push(*shard);
                }
            }
            self.submit(ControllerCommand::BeginRebalance {
                shard: *shard,
                add: None,
                remove: Some(node_id),
            })
            .await?;
            self.submit(ControllerCommand::CompleteRebalance { shard: *shard })
                .await?;
        }
        Ok(DrainReport {
            node_id,
            shards,
            leadership_transfers,
        })
    }

    pub fn is_leader_with_quorum(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        metrics.running_state.is_ok()
            && metrics.state == ServerState::Leader
            && metrics.current_leader == Some(self.node_id)
            && metrics
                .millis_since_quorum_ack
                .is_some_and(|age| age <= QUORUM_ACK_MAX_AGE_MS)
    }

    pub fn is_ready(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        self.store.controller_state().is_some()
            && metrics.running_state.is_ok()
            && metrics.current_leader.is_some()
            && (if matches!(metrics.state, ServerState::Leader) {
                metrics
                    .millis_since_quorum_ack
                    .is_some_and(|age| age <= QUORUM_ACK_MAX_AGE_MS)
            } else {
                let contact = self.last_leader_contact_ms.load(Ordering::Acquire);
                contact > 0
                    && chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_sub(contact)
                        <= QUORUM_ACK_MAX_AGE_MS as i64
            })
    }

    pub fn note_leader_contact(&self) {
        self.last_leader_contact_ms
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Release);
    }

    pub async fn trigger_snapshot(&self) -> Result<(), ControllerError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| ControllerError::Raft(error.to_string()))
    }
}

/// Apply a command from a log entry proposed in `log_term`. Term fences use
/// the entry's own Raft term rather than whatever the proposer wrote, so every
/// replica reaches the same result. On error the state is left unchanged.
/// Returns the parts of the state the command changed.
pub(crate) fn apply_replicated(
    state: &mut ControllerState,
    mut command: ControllerCommand,
    log_term: u64,
) -> Result<Vec<StateKey>, ControllerError> {
    match &mut command {
        ControllerCommand::AcceptFanout {
            controller_term, ..
        }
        | ControllerCommand::ClaimFanout {
            controller_term, ..
        }
        | ControllerCommand::UpdateFanout {
            controller_term, ..
        }
        | ControllerCommand::GarbageCollectFanout {
            controller_term, ..
        } => *controller_term = log_term,
        _ => {}
    }
    apply_command(state, command)
}

fn pending_fanout_bytes(state: &ControllerState) -> usize {
    state
        .fanout_outbox
        .values()
        .filter(|entry| !entry.completed)
        .map(|entry| {
            entry
                .payload_json
                .len()
                .saturating_add(entry.progress_json.len())
        })
        .fold(0usize, usize::saturating_add)
}

/// Reject a shard-leader report unless it comes from the committed leader in
/// its committed term, so a deposed leader cannot rewrite placement.
fn check_shard_leader(
    placement: &ShardPlacement,
    leader_id: Uuid,
    leader_term: u64,
) -> Result<(), ControllerError> {
    if placement.leader_id != leader_id || placement.leader_term != leader_term {
        return Err(ControllerError::InvalidCommand(format!(
            "shard {} report from {leader_id}@{leader_term} is not from the committed leader {}@{}",
            placement.shard, placement.leader_id, placement.leader_term
        )));
    }
    Ok(())
}

/// Shards on which `node_id` holds an ISR copy that draining would take
/// below `min_isr`.
pub(crate) fn drain_last_copy_shards(
    state: &ControllerState,
    node_id: Uuid,
    min_isr: usize,
) -> Vec<u32> {
    let mut shards: Vec<_> = state
        .shards
        .values()
        .filter(|placement| {
            placement.isr.contains(&node_id)
                && placement.isr.iter().filter(|id| **id != node_id).count() < min_isr
        })
        .map(|placement| placement.shard)
        .collect();
    shards.sort_unstable();
    shards
}

/// Validates fully before mutating, so an `Err` leaves `state` untouched.
fn apply_command(
    state: &mut ControllerState,
    command: ControllerCommand,
) -> Result<Vec<StateKey>, ControllerError> {
    let touched =
        match command {
            ControllerCommand::MutateCatalog {
                operation_id,
                request_fingerprint,
                mutations,
            } => {
                if let Some(previous) = state.catalog_operations.get(&operation_id) {
                    if previous.request_fingerprint != request_fingerprint {
                        return Err(ControllerError::CatalogConflict(
                            "operation id reused for a different request".into(),
                        ));
                    }
                }
                let before = state.catalog_records.clone();
                crate::catalog::mutate(state, operation_id, mutations, false)?;
                state
                    .catalog_operations
                    .get_mut(&operation_id)
                    .expect("committed operation")
                    .request_fingerprint = request_fingerprint;
                catalog_touched(&before, state, operation_id)
            }
            ControllerCommand::BeginCatalogActivation => {
                if !state.catalog_authoritative {
                    state.catalog_frozen = true;
                }
                vec![StateKey::Meta]
            }
            ControllerCommand::SeedCatalog {
                operation_id,
                records,
            } => {
                let before = state.catalog_records.clone();
                crate::catalog::mutate(state, operation_id, records, true)?;
                catalog_touched(&before, state, operation_id)
            }
            ControllerCommand::AcquireShardLeadership { shard, leader_id } => {
                if !state.nodes.values().any(|node| node.id == leader_id) {
                    return Err(ControllerError::UnknownNode(leader_id));
                }
                let placement = state.shards.get_mut(&shard).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
                if !eligible_shard_leaders(placement).contains(&leader_id) {
                    return Err(ControllerError::InvalidCommand(format!(
                        "{leader_id} is not an in-sync replica of shard {shard} (isr {:?})",
                        placement.isr
                    )));
                }
                placement.leader_term = placement
                    .leader_term
                    .checked_add(1)
                    .filter(|term| *term != u64::MAX)
                    .ok_or_else(|| {
                        ControllerError::InvalidCommand("shard term exhausted".into())
                    })?;
                placement.leader_id = leader_id;
                vec![StateKey::Shard(shard)]
            }
            ControllerCommand::ObserveShardTerm {
                shard,
                leader_id,
                term,
            } => {
                let placement = state.shards.get_mut(&shard).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
                if placement.leader_id != leader_id || placement.leader_term != term {
                    return Err(ControllerError::InvalidCommand(format!(
                        "term {term} for {leader_id} is not the committed placement {}@{}",
                        placement.leader_id, placement.leader_term
                    )));
                }
                Vec::new()
            }
            ControllerCommand::AcquireScheduler {
                holder,
                now_ms,
                ttl_ms,
            } => {
                if !state.nodes.values().any(|node| node.id == holder) {
                    return Err(ControllerError::UnknownNode(holder));
                }
                let can_acquire = state
                    .scheduler
                    .as_ref()
                    .is_none_or(|lease| lease.expires_at_ms <= now_ms || lease.holder == holder);
                if !can_acquire {
                    return Err(ControllerError::InvalidCommand(
                        "scheduler lease is held by another node".into(),
                    ));
                }
                state.scheduler = Some(SchedulerLease {
                    holder,
                    expires_at_ms: now_ms.saturating_add(ttl_ms.max(1)),
                });
                vec![StateKey::Meta]
            }
            ControllerCommand::AdvanceCatalogEpoch => {
                state.catalog_epoch = state.catalog_epoch.checked_add(1).ok_or_else(|| {
                    ControllerError::InvalidCommand("catalog epoch exhausted".into())
                })?;
                vec![StateKey::Meta]
            }
            ControllerCommand::AcceptFanout {
                id,
                tenant_id,
                acceptance_hash,
                payload_json,
                created_at_ms,
                controller_term,
            } => {
                if let Some(existing) = state.fanout_outbox.get(&id) {
                    if existing.tenant_id == tenant_id
                        && ((!acceptance_hash.is_empty()
                            && existing.acceptance_hash == acceptance_hash)
                            || (acceptance_hash.is_empty()
                                && *existing.payload_json == *payload_json))
                    {
                        return Ok(Vec::new());
                    }
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} already exists with different content"
                    )));
                }
                if payload_json.len() > FANOUT_MAX_ENTRY_BYTES {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} is too large: {} bytes (limit {FANOUT_MAX_ENTRY_BYTES})",
                        payload_json.len()
                    )));
                }
                if pending_fanout_bytes(state).saturating_add(payload_json.len())
                    > FANOUT_OUTBOX_MAX_PENDING_BYTES
                {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout outbox is full ({FANOUT_OUTBOX_MAX_PENDING_BYTES} pending bytes)"
                    )));
                }
                state.fanout_outbox.insert(
                    id,
                    FanoutOutboxEntry {
                        id,
                        tenant_id,
                        acceptance_hash,
                        version: 1,
                        authority_term: controller_term,
                        owner_id: None,
                        lease_expires_at_ms: 0,
                        payload_json: payload_json.into(),
                        progress_json: String::new(),
                        completed: false,
                        created_at_ms,
                        updated_at_ms: created_at_ms,
                    },
                );
                vec![StateKey::Fanout(id)]
            }
            ControllerCommand::ClaimFanout {
                id,
                owner_id,
                expected_version,
                now_ms,
                ttl_ms,
                controller_term,
            } => {
                let entry = state.fanout_outbox.get_mut(&id).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown fanout {id}"))
                })?;
                if entry.completed {
                    return Ok(Vec::new());
                }
                if entry.owner_id == Some(owner_id)
                    && entry.authority_term == controller_term
                    && entry.lease_expires_at_ms > now_ms
                {
                    return Ok(Vec::new());
                }
                if expected_version != entry.version {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} version fence failed: expected {expected_version}, have {}",
                        entry.version
                    )));
                }
                if entry.authority_term > controller_term {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} term fence failed: stale {controller_term}, have {}",
                        entry.authority_term
                    )));
                }
                if entry.owner_id.is_some()
                    && entry.lease_expires_at_ms > now_ms
                    && entry.authority_term == controller_term
                {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} is leased by another worker"
                    )));
                }
                entry.version = entry.version.checked_add(1).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("fanout {id} version exhausted"))
                })?;
                entry.authority_term = controller_term;
                entry.owner_id = Some(owner_id);
                entry.lease_expires_at_ms = now_ms.saturating_add(ttl_ms.max(1));
                entry.updated_at_ms = now_ms;
                vec![StateKey::Fanout(id)]
            }
            ControllerCommand::UpdateFanout {
                id,
                owner_id,
                expected_version,
                progress_json,
                completed,
                now_ms,
                controller_term,
            } => {
                let entry = state.fanout_outbox.get_mut(&id).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown fanout {id}"))
                })?;
                if entry.progress_json == progress_json && entry.completed == completed {
                    return Ok(Vec::new());
                }
                if expected_version != entry.version
                    || entry.authority_term != controller_term
                    || entry.owner_id != Some(owner_id)
                {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} ownership fence failed"
                    )));
                }
                if entry.lease_expires_at_ms <= now_ms {
                    return Err(ControllerError::InvalidCommand(format!(
                        "fanout {id} lease expired"
                    )));
                }
                if progress_json.len() > FANOUT_MAX_ENTRY_BYTES {
                    return Err(ControllerError::InvalidCommand(format!(
                    "fanout {id} progress is too large: {} bytes (limit {FANOUT_MAX_ENTRY_BYTES})",
                    progress_json.len()
                )));
                }
                entry.version = entry.version.checked_add(1).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("fanout {id} version exhausted"))
                })?;
                entry.progress_json = progress_json;
                entry.completed = completed;
                entry.updated_at_ms = now_ms;
                if completed {
                    entry.owner_id = None;
                    entry.lease_expires_at_ms = 0;
                }
                vec![StateKey::Fanout(id)]
            }
            ControllerCommand::GarbageCollectFanout {
                ids,
                completed_before_ms,
                controller_term,
            } => {
                let ids: BTreeSet<_> = ids.into_iter().collect();
                let removed: Vec<_> = state
                    .fanout_outbox
                    .values()
                    .filter(|entry| {
                        entry.completed
                            && entry.authority_term <= controller_term
                            && (ids.contains(&entry.id)
                                || entry.updated_at_ms <= completed_before_ms)
                    })
                    .map(|entry| entry.id)
                    .collect();
                for id in &removed {
                    state.fanout_outbox.remove(id);
                }
                removed.into_iter().map(StateKey::Fanout).collect()
            }
            ControllerCommand::RegisterDataNode { mut node } => {
                node.controller_voter = state.controller_voters.contains(&node.id);
                state.data_nodes.insert(node.id, node.clone());
                if !state.nodes.values().any(|existing| existing.id == node.id) {
                    let raft_id = raft_id(node.id);
                    state.nodes.insert(
                        raft_id,
                        ControllerNode {
                            id: node.id,
                            raft_id,
                            addr: node.addr.clone(),
                        },
                    );
                }
                state.membership_generation = state.membership_generation.saturating_add(1);
                vec![StateKey::Meta]
            }
            ControllerCommand::PutCatalogRecord {
                kind,
                key,
                payload_json,
                tombstone,
            } => {
                if state.catalog_authoritative || state.catalog_frozen {
                    return Err(ControllerError::CatalogConflict(
                        "legacy catalog writes disabled; use MutateCatalog".into(),
                    ));
                }
                state.catalog_epoch = state.catalog_epoch.saturating_add(1);
                let map_key = catalog_key(&kind, &key);
                upsert_catalog(
                    &mut state.catalog_records,
                    kind,
                    key,
                    payload_json,
                    tombstone,
                );
                vec![StateKey::Meta, StateKey::Catalog(map_key)]
            }
            ControllerCommand::BeginRebalance { shard, add, remove } => {
                let placement = state.shards.get_mut(&shard).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
                if let Some(add) = add {
                    if !placement.replicas.contains(&add) && !placement.learners.contains(&add) {
                        placement.learners.push(add);
                    }
                }
                let generation = state
                    .rebalance
                    .get(&shard)
                    .map(|op| op.generation.saturating_add(1))
                    .unwrap_or(1);
                state.rebalance.insert(
                    shard,
                    RebalanceOp {
                        shard,
                        adding: add.into_iter().collect(),
                        removing: remove.into_iter().collect(),
                        generation,
                    },
                );
                vec![StateKey::Shard(shard), StateKey::Rebalance(shard)]
            }
            ControllerCommand::PromoteLearner {
                shard,
                node_id,
                leader_id,
                leader_term,
            } => {
                let placement = state.shards.get_mut(&shard).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
                check_shard_leader(placement, leader_id, leader_term)?;
                if !placement.learners.contains(&node_id) {
                    return Err(ControllerError::InvalidCommand(format!(
                        "{node_id} is not a learner of shard {shard}"
                    )));
                }
                placement.learners.retain(|id| *id != node_id);
                if !placement.replicas.contains(&node_id) {
                    placement.replicas.push(node_id);
                }
                if !placement.isr.contains(&node_id) {
                    placement.isr.push(node_id);
                }
                vec![StateKey::Shard(shard)]
            }
            ControllerCommand::UpdateIsr {
                shard,
                leader_id,
                leader_term,
                isr,
                committed_hwm,
            } => {
                let placement = state.shards.get_mut(&shard).ok_or_else(|| {
                    ControllerError::InvalidCommand(format!("unknown shard {shard}"))
                })?;
                check_shard_leader(placement, leader_id, leader_term)?;
                if !isr.contains(&leader_id) {
                    return Err(ControllerError::InvalidCommand(format!(
                        "ISR for shard {shard} must contain its leader"
                    )));
                }
                if let Some(stranger) = isr
                    .iter()
                    .find(|id| **id != leader_id && !placement.replicas.contains(id))
                {
                    return Err(ControllerError::InvalidCommand(format!(
                        "{stranger} is not a replica of shard {shard}"
                    )));
                }
                // Keep replica order so leader choice stays deterministic.
                let mut ordered: Vec<Uuid> = placement
                    .replicas
                    .iter()
                    .copied()
                    .filter(|id| isr.contains(id))
                    .collect();
                if !ordered.contains(&leader_id) {
                    ordered.insert(0, leader_id);
                }
                if ordered == placement.isr && committed_hwm == placement.committed_hwm {
                    return Ok(Vec::new());
                }
                placement.isr = ordered;
                placement.committed_hwm = committed_hwm;
                vec![StateKey::Shard(shard)]
            }
            ControllerCommand::CompleteRebalance { shard } => {
                if let Some(op) = state.rebalance.remove(&shard) {
                    if let Some(placement) = state.shards.get_mut(&shard) {
                        for id in op.removing {
                            placement.replicas.retain(|r| *r != id);
                            placement.isr.retain(|r| *r != id);
                            placement.learners.retain(|r| *r != id);
                            if placement.leader_id == id {
                                if let Some(next) = pick_leader(&placement.isr, shard)
                                    .or_else(|| pick_leader(&placement.replicas, shard))
                                {
                                    placement.leader_id = next;
                                    placement.leader_term = placement.leader_term.saturating_add(1);
                                }
                            }
                        }
                    }
                    vec![StateKey::Shard(shard), StateKey::Rebalance(shard)]
                } else {
                    Vec::new()
                }
            }
            ControllerCommand::SetGcWatermarks { leader_id, entries } => {
                let mut rejected = Vec::new();
                let mut touched = Vec::new();
                for entry in entries {
                    let fenced = state.shards.get(&entry.partition).is_some_and(|placement| {
                        placement.leader_id == leader_id
                            && placement.leader_term == entry.leader_term
                    });
                    let current = state.log_gc_watermark(&entry.namespace, entry.partition);
                    if !fenced {
                        rejected.push(format!(
                            "{}/{}: {leader_id} is not the shard leader at term {}",
                            entry.namespace, entry.partition, entry.leader_term
                        ));
                    } else if entry.watermark < current {
                        rejected.push(format!(
                            "{}/{}: watermark cannot move backwards ({} < {current})",
                            entry.namespace, entry.partition, entry.watermark
                        ));
                    } else {
                        state
                            .log_gc_watermarks
                            .entry(entry.namespace.clone())
                            .or_default()
                            .insert(entry.partition, entry.watermark);
                        touched.push(StateKey::LogGcWatermark {
                            namespace: entry.namespace,
                            partition: entry.partition,
                        });
                    }
                }
                if touched.is_empty() && !rejected.is_empty() {
                    return Err(ControllerError::InvalidCommand(format!(
                        "gc watermarks rejected: {}",
                        rejected.join("; ")
                    )));
                }
                if !rejected.is_empty() {
                    tracing::warn!(?rejected, "some gc watermarks were rejected");
                }
                touched
            }
            ControllerCommand::SetReplicationPolicy { factor, min_isr } => {
                let factor = factor.max(1);
                state.replication_factor = factor;
                state.min_isr = min_isr_for(factor, min_isr);
                vec![StateKey::Meta]
            }
            ControllerCommand::AddControllerVoter { node_id } => {
                if !state.data_nodes.contains_key(&node_id)
                    && !state.nodes.values().any(|node| node.id == node_id)
                {
                    return Err(ControllerError::InvalidCommand(format!(
                        "unknown node {node_id}"
                    )));
                }
                if !state.controller_voters.contains(&node_id) {
                    if state.controller_voters.len() >= 5 {
                        return Err(ControllerError::InvalidCommand(
                            "controller voter set is already at 5".into(),
                        ));
                    }
                    state.controller_voters.push(node_id);
                }
                if let Some(node) = state.data_nodes.get_mut(&node_id) {
                    node.controller_voter = true;
                }
                vec![StateKey::Meta]
            }
            ControllerCommand::RemoveControllerVoter { node_id } => {
                if !state.controller_voters.contains(&node_id) {
                    return Err(ControllerError::InvalidCommand(format!(
                        "{node_id} is not a controller voter"
                    )));
                }
                if state.controller_voters.len() <= 1 {
                    return Err(ControllerError::InvalidCommand(
                        "cannot remove the last controller voter".into(),
                    ));
                }
                state.controller_voters.retain(|id| *id != node_id);
                if let Some(node) = state.data_nodes.get_mut(&node_id) {
                    node.controller_voter = false;
                }
                vec![StateKey::Meta]
            }
        };
    Ok(touched)
}

/// Entities a catalog mutation changed: the meta record (epoch, activation
/// flags), its operation receipt and every catalog record that differs.
fn catalog_touched(
    before: &BTreeMap<String, CatalogRecord>,
    state: &ControllerState,
    operation_id: Uuid,
) -> Vec<StateKey> {
    let mut touched = vec![StateKey::Meta, StateKey::CatalogOperation(operation_id)];
    for (key, record) in &state.catalog_records {
        if before.get(key) != Some(record) {
            touched.push(StateKey::Catalog(key.clone()));
        }
    }
    touched.extend(
        before
            .keys()
            .filter(|key| !state.catalog_records.contains_key(*key))
            .cloned()
            .map(StateKey::Catalog),
    );
    touched
}

pub fn raft_id(id: Uuid) -> NodeId {
    u64::from_be_bytes(id.as_bytes()[..8].try_into().expect("UUID prefix"))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod replicated {
        use super::*;
        use crate::rocks_store::RocksResponse;
        use openraft::{
            CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftStorage,
        };

        fn config(count: usize) -> (ClusterConfig, Vec<Uuid>) {
            let ids: Vec<_> = (0..count).map(|_| Uuid::new_v4()).collect();
            let config = ClusterConfig {
                cluster_id: Uuid::new_v4(),
                nodes: ids
                    .iter()
                    .enumerate()
                    .map(|(index, id)| crate::cluster::NodeConfig {
                        id: *id,
                        addr: format!("http://n{index}"),
                        internal_url: None,
                    })
                    .collect(),
                node_id: ids[0],
                generation: 1,
                hash_version: 1,
            };
            (config, ids)
        }

        fn entry(term: u64, index: u64, request: RocksRequest) -> Entry<TypeConfig> {
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
                payload: EntryPayload::Normal(request),
            }
        }

        fn command(term: u64, index: u64, command: ControllerCommand) -> Entry<TypeConfig> {
            entry(term, index, RocksRequest::Controller { command })
        }

        async fn apply(
            store: &mut Arc<RocksStore>,
            entries: &[Entry<TypeConfig>],
        ) -> Vec<RocksResponse> {
            store.apply_to_state_machine(entries).await.unwrap()
        }

        async fn stored(store: &Arc<RocksStore>) -> ControllerState {
            store.controller_state().unwrap().as_ref().clone()
        }

        #[tokio::test]
        async fn commands_from_successive_terms_build_on_committed_state() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = RocksStore::new(dir.path()).await.unwrap();
            let (config, ids) = config(3);
            let bootstrap = ControllerState::from_v1(&config, 2, &HashMap::new(), None).unwrap();
            apply(
                &mut store,
                &[entry(
                    1,
                    1,
                    RocksRequest::ControllerBootstrap {
                        state: Box::new(bootstrap.clone()),
                    },
                )],
            )
            .await;
            // The old leader's last command and the new leader's first command
            // were both proposed against the same view; both must survive.
            apply(
                &mut store,
                &[command(
                    1,
                    2,
                    ControllerCommand::AcquireShardLeadership {
                        shard: 0,
                        leader_id: ids[1],
                    },
                )],
            )
            .await;
            apply(
                &mut store,
                &[
                    command(
                        2,
                        3,
                        ControllerCommand::PutCatalogRecord {
                            kind: "queue".into(),
                            key: "orders".into(),
                            payload_json: "{}".into(),
                            tombstone: false,
                        },
                    ),
                    command(
                        2,
                        4,
                        ControllerCommand::AcquireShardLeadership {
                            shard: 0,
                            leader_id: ids[2],
                        },
                    ),
                ],
            )
            .await;
            let state = stored(&store).await;
            assert_eq!(state.shards[&0].leader_id, ids[2]);
            assert_eq!(
                state.shards[&0].leader_term, 2,
                "shard terms are never reused"
            );
            assert!(state.catalog_records.contains_key("queue:orders"));

            // A second bootstrap (another node's migrated V1 view) is ignored.
            apply(
                &mut store,
                &[entry(
                    2,
                    5,
                    RocksRequest::ControllerBootstrap {
                        state: Box::new(bootstrap),
                    },
                )],
            )
            .await;
            assert_eq!(stored(&store).await.shards[&0].leader_term, 2);
        }

        fn accept(index: u64, id: Uuid, payload_json: String) -> Entry<TypeConfig> {
            command(
                1,
                index,
                ControllerCommand::AcceptFanout {
                    id,
                    tenant_id: "tenant".into(),
                    acceptance_hash: id.to_string(),
                    payload_json,
                    created_at_ms: index as i64,
                    controller_term: 0,
                },
            )
        }

        #[tokio::test]
        async fn command_writes_only_the_entities_it_changes() {
            let dir = tempfile::tempdir().unwrap();
            let (config, ids) = config(3);
            let bootstrap = ControllerState::from_v1(&config, 8, &HashMap::new(), None).unwrap();
            let fanouts: Vec<_> = (0..20).map(|_| Uuid::new_v4()).collect();
            {
                let mut store = RocksStore::new(dir.path()).await.unwrap();
                let mut entries = vec![entry(
                    1,
                    1,
                    RocksRequest::ControllerBootstrap {
                        state: Box::new(bootstrap),
                    },
                )];
                for (index, id) in fanouts.iter().enumerate() {
                    entries.push(accept(index as u64 + 2, *id, "x".repeat(64 * 1024)));
                }
                apply(&mut store, &entries).await;
                assert!(
                    store
                        .last_commit_bytes
                        .load(std::sync::atomic::Ordering::SeqCst)
                        > 20 * 64 * 1024
                );

                // A scheduler renewal must not rewrite 1.3 MB of fan-out bodies.
                apply(
                    &mut store,
                    &[command(
                        1,
                        30,
                        ControllerCommand::AcquireScheduler {
                            holder: ids[0],
                            now_ms: 100,
                            ttl_ms: 1_000,
                        },
                    )],
                )
                .await;
                let bytes = store
                    .last_commit_bytes
                    .load(std::sync::atomic::Ordering::SeqCst);
                assert!(bytes < 4 * 1024, "scheduler renewal wrote {bytes} bytes");

                // Readers share the published snapshot rather than copying it.
                let first = store.controller_state().unwrap();
                let second = store.controller_state().unwrap();
                assert!(Arc::ptr_eq(&first, &second));
                assert!(Arc::ptr_eq(
                    &first.fanout_outbox[&fanouts[0]].payload_json,
                    &second.fanout_outbox[&fanouts[0]].payload_json
                ));
            }
            let reopened = RocksStore::new(dir.path()).await.unwrap();
            let state = reopened.controller_state().unwrap();
            assert_eq!(state.fanout_outbox.len(), 20);
            assert_eq!(state.scheduler.as_ref().unwrap().holder, ids[0]);
            assert_eq!(state.shards.len(), 8);
        }

        #[tokio::test]
        async fn oversized_fanout_is_rejected_and_gc_honours_cutoff() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = RocksStore::new(dir.path()).await.unwrap();
            let (config, ids) = config(1);
            let bootstrap = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
            let (big, old, recent) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
            let responses = apply(
                &mut store,
                &[
                    entry(
                        1,
                        1,
                        RocksRequest::ControllerBootstrap {
                            state: Box::new(bootstrap),
                        },
                    ),
                    accept(2, big, "x".repeat(FANOUT_MAX_ENTRY_BYTES + 1)),
                    accept(3, old, "{}".into()),
                    accept(4, recent, "{}".into()),
                ],
            )
            .await;
            assert!(matches!(
                &responses[1].rejected,
                Some(ControllerRejection::InvalidCommand(message)) if message.contains("too large")
            ));
            let mut index = 5;
            for (id, now_ms) in [(old, 10), (recent, 1_000)] {
                apply(
                    &mut store,
                    &[
                        command(
                            1,
                            index,
                            ControllerCommand::ClaimFanout {
                                id,
                                owner_id: ids[0],
                                expected_version: 1,
                                now_ms,
                                ttl_ms: 100,
                                controller_term: 0,
                            },
                        ),
                        command(
                            1,
                            index + 1,
                            ControllerCommand::UpdateFanout {
                                id,
                                owner_id: ids[0],
                                expected_version: 2,
                                progress_json: "{\"done\":true}".into(),
                                completed: true,
                                now_ms,
                                controller_term: 0,
                            },
                        ),
                    ],
                )
                .await;
                index += 2;
            }
            let state = store.controller_state().unwrap();
            assert!(!state.fanout_outbox.contains_key(&big));
            assert_eq!(state.fanout_outbox[&old].progress_json, "{\"done\":true}");
            assert_eq!(&*state.fanout_outbox[&old].payload_json, "{}");
            apply(
                &mut store,
                &[command(
                    1,
                    index,
                    ControllerCommand::GarbageCollectFanout {
                        ids: Vec::new(),
                        completed_before_ms: 500,
                        controller_term: 0,
                    },
                )],
            )
            .await;
            let state = store.controller_state().unwrap();
            assert!(!state.fanout_outbox.contains_key(&old));
            assert!(state.fanout_outbox.contains_key(&recent));
            drop(store);
            let reopened = RocksStore::new(dir.path()).await.unwrap();
            assert!(!reopened
                .controller_state()
                .unwrap()
                .fanout_outbox
                .contains_key(&old));
        }

        #[tokio::test]
        async fn legacy_single_value_state_is_migrated_and_snapshots_round_trip() {
            let dir = tempfile::tempdir().unwrap();
            let (config, ids) = config(3);
            let mut legacy = ControllerState::from_v1(&config, 4, &HashMap::new(), None).unwrap();
            legacy.catalog_epoch = 9;
            {
                let mut store = RocksStore::new(dir.path()).await.unwrap();
                // Raw legacy layout, as written by older controllers.
                let sm = store.state_machine.read().await;
                sm.db
                    .put_cf(
                        sm.cf_sm_data(),
                        STATE_KEY,
                        serde_json::to_vec(&legacy).unwrap(),
                    )
                    .unwrap();
                drop(sm);
                drop(store);
                store = RocksStore::new(dir.path()).await.unwrap();
                assert_eq!(*store.controller_state().unwrap(), legacy);
                assert_eq!(
                    store.state_machine.read().await.get(STATE_KEY).unwrap(),
                    None
                );
                apply(
                    &mut store,
                    &[command(
                        2,
                        1,
                        ControllerCommand::AcquireShardLeadership {
                            shard: 3,
                            leader_id: ids[1],
                        },
                    )],
                )
                .await;
                let snapshot = store.build_snapshot().await.unwrap();
                let restored_dir = tempfile::tempdir().unwrap();
                let mut restored = RocksStore::new(restored_dir.path()).await.unwrap();
                restored
                    .install_snapshot(&snapshot.meta, snapshot.snapshot)
                    .await
                    .unwrap();
                assert_eq!(
                    *restored.controller_state().unwrap(),
                    *store.controller_state().unwrap()
                );
                let current = restored.get_current_snapshot().await.unwrap().unwrap();
                assert_eq!(current.meta, snapshot.meta);
            }
        }

        #[tokio::test]
        async fn rejected_command_is_a_replicated_no_op() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = RocksStore::new(dir.path()).await.unwrap();
            let (config, _) = config(1);
            let bootstrap = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
            let stranger = Uuid::new_v4();
            let responses = apply(
                &mut store,
                &[
                    entry(
                        1,
                        1,
                        RocksRequest::ControllerBootstrap {
                            state: Box::new(bootstrap.clone()),
                        },
                    ),
                    command(
                        1,
                        2,
                        ControllerCommand::AcquireShardLeadership {
                            shard: 0,
                            leader_id: stranger,
                        },
                    ),
                ],
            )
            .await;
            assert_eq!(
                responses[1].rejected,
                Some(ControllerRejection::UnknownNode(stranger))
            );
            assert_eq!(stored(&store).await, bootstrap);
            let (applied, _) = store.last_applied_state().await.unwrap();
            assert_eq!(applied.map(|id| id.index), Some(2));
        }

        #[tokio::test]
        async fn fanout_authority_term_comes_from_the_log_entry() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = RocksStore::new(dir.path()).await.unwrap();
            let (config, _) = config(1);
            let bootstrap = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
            let id = Uuid::new_v4();
            apply(
                &mut store,
                &[
                    // A legacy full-state write followed by a command in the same batch.
                    entry(
                        6,
                        1,
                        RocksRequest::Set {
                            key: STATE_KEY.into(),
                            value: serde_json::to_string(&bootstrap).unwrap(),
                        },
                    ),
                    command(
                        7,
                        2,
                        ControllerCommand::AcceptFanout {
                            id,
                            tenant_id: "tenant".into(),
                            acceptance_hash: "hash".into(),
                            payload_json: "{}".into(),
                            created_at_ms: 1,
                            controller_term: 0,
                        },
                    ),
                ],
            )
            .await;
            assert_eq!(stored(&store).await.fanout_outbox[&id].authority_term, 7);
        }
    }

    #[test]
    fn shard_leadership_is_restricted_to_the_isr() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                    internal_url: None,
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        let drained = state.shards[&0].leader_id;
        apply_command(
            &mut state,
            ControllerCommand::BeginRebalance {
                shard: 0,
                add: None,
                remove: Some(drained),
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            ControllerCommand::CompleteRebalance { shard: 0 },
        )
        .unwrap();
        let placement = state.shards[&0].clone();
        assert_ne!(placement.leader_id, drained);
        assert!(placement.isr.contains(&placement.leader_id));
        let before = state.clone();
        let error = apply_command(
            &mut state,
            ControllerCommand::AcquireShardLeadership {
                shard: 0,
                leader_id: drained,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("not an in-sync replica"));
        assert_eq!(state, before);
        // The health loop's recommendation never points at the drained node.
        assert_eq!(
            crate::election::choose_shard_leader(&placement, |_| true),
            Some(placement.leader_id)
        );
    }

    #[test]
    fn measured_isr_is_leader_fenced_and_drives_drain_safety() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                    internal_url: None,
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        let placement = state.shards[&0].clone();
        let leader = placement.leader_id;
        let others: Vec<_> = placement
            .replicas
            .iter()
            .copied()
            .filter(|id| *id != leader)
            .collect();
        assert!(drain_last_copy_shards(&state, others[0], 2).is_empty());

        // A deposed leader (stale term) cannot rewrite the ISR.
        let before = state.clone();
        assert!(apply_command(
            &mut state,
            ControllerCommand::UpdateIsr {
                shard: 0,
                leader_id: leader,
                leader_term: placement.leader_term.wrapping_sub(1),
                isr: vec![leader],
                committed_hwm: 9,
            },
        )
        .is_err());
        assert!(apply_command(
            &mut state,
            ControllerCommand::UpdateIsr {
                shard: 0,
                leader_id: others[0],
                leader_term: placement.leader_term,
                isr: vec![others[0]],
                committed_hwm: 9,
            },
        )
        .is_err());
        assert_eq!(state, before);

        // The leader reports one follower lagging.
        apply_command(
            &mut state,
            ControllerCommand::UpdateIsr {
                shard: 0,
                leader_id: leader,
                leader_term: placement.leader_term,
                isr: vec![others[0], leader],
                committed_hwm: 9,
            },
        )
        .unwrap();
        let shard = &state.shards[&0];
        assert_eq!(shard.isr.len(), 2);
        assert!(!shard.isr.contains(&others[1]));
        assert_eq!(shard.committed_hwm, 9);
        // With minISR=2 the remaining in-sync follower is now the last spare
        // copy: draining it is refused, draining the lagging one is not.
        assert_eq!(drain_last_copy_shards(&state, others[0], 2), vec![0]);
        assert!(drain_last_copy_shards(&state, others[1], 2).is_empty());
        // ISR members must be replicas.
        assert!(apply_command(
            &mut state,
            ControllerCommand::UpdateIsr {
                shard: 0,
                leader_id: leader,
                leader_term: placement.leader_term,
                isr: vec![leader, Uuid::new_v4()],
                committed_hwm: 9,
            },
        )
        .is_err());
    }

    #[test]
    fn controller_commands_update_versioned_state() {
        let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    internal_url: None,
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 4, &HashMap::new(), None).unwrap();
        apply_command(
            &mut state,
            ControllerCommand::AcquireShardLeadership {
                shard: 1,
                leader_id: ids[2],
            },
        )
        .unwrap();
        assert_eq!(state.shards[&1].leader_id, ids[2]);
        assert_eq!(state.shards[&1].leader_term, 1);
        apply_command(&mut state, ControllerCommand::AdvanceCatalogEpoch).unwrap();
        assert_eq!(state.catalog_epoch, 1);
    }

    #[test]
    fn fanout_outbox_is_version_and_term_fenced() {
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    internal_url: None,
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        let fanout_id = Uuid::new_v4();
        apply_command(
            &mut state,
            ControllerCommand::AcceptFanout {
                id: fanout_id,
                tenant_id: "tenant".into(),
                acceptance_hash: "hash".into(),
                payload_json: r#"{"completed":false}"#.into(),
                created_at_ms: 1,
                controller_term: 5,
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            ControllerCommand::ClaimFanout {
                id: fanout_id,
                owner_id: ids[0],
                expected_version: 1,
                now_ms: 10,
                ttl_ms: 100,
                controller_term: 5,
            },
        )
        .unwrap();
        assert!(apply_command(
            &mut state,
            ControllerCommand::UpdateFanout {
                id: fanout_id,
                owner_id: ids[0],
                expected_version: 2,
                progress_json: r#"{"completed":true}"#.into(),
                completed: true,
                now_ms: 11,
                controller_term: 4,
            },
        )
        .is_err());
        apply_command(
            &mut state,
            ControllerCommand::ClaimFanout {
                id: fanout_id,
                owner_id: ids[1],
                expected_version: 2,
                now_ms: 12,
                ttl_ms: 100,
                controller_term: 6,
            },
        )
        .unwrap();
        assert_eq!(state.fanout_outbox[&fanout_id].owner_id, Some(ids[1]));
        assert_eq!(state.fanout_outbox[&fanout_id].authority_term, 6);
    }

    #[test]
    fn nine_node_cell_uses_rf3_not_all_peers() {
        let ids: Vec<_> = (0..9).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    internal_url: None,
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let state = ControllerState::from_v1(&config, 8, &HashMap::new(), None).unwrap();
        assert_eq!(state.replication_factor, 3);
        assert_eq!(state.min_isr, 2);
        assert_eq!(state.controller_voters.len(), 5);
        for placement in state.shards.values() {
            assert_eq!(placement.replicas.len(), 3);
            assert_eq!(placement.isr.len(), 3);
        }
        let a = &state.shards[&0].replicas;
        let b = &state.shards[&1].replicas;
        assert_ne!(a, b);
    }

    #[test]
    fn catalog_and_gc_commands_are_monotonic() {
        let id = Uuid::new_v4();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![crate::cluster::NodeConfig {
                internal_url: None,
                id,
                addr: "http://n0".into(),
            }],
            node_id: id,
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 1, &HashMap::new(), None).unwrap();
        apply_command(
            &mut state,
            ControllerCommand::PutCatalogRecord {
                kind: "queue".into(),
                key: "orders".into(),
                payload_json: "{}".into(),
                tombstone: false,
            },
        )
        .unwrap();
        assert_eq!(state.catalog_epoch, 1);
        let placement = state.shards[&0].clone();
        let proposal = |watermark| ControllerCommand::SetGcWatermarks {
            leader_id: placement.leader_id,
            entries: vec![GcWatermarkProposal {
                namespace: "__physical".into(),
                partition: 0,
                leader_term: placement.leader_term,
                watermark,
            }],
        };
        apply_command(&mut state, proposal(10)).unwrap();
        assert!(apply_command(&mut state, proposal(9)).is_err());
    }

    #[test]
    fn gc_watermarks_are_fenced_to_the_shard_leader_term() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    id: *id,
                    addr: format!("http://n{index}"),
                    internal_url: None,
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 2, &HashMap::new(), None).unwrap();
        let placement = state.shards[&0].clone();
        let other = ids
            .iter()
            .copied()
            .find(|id| *id != placement.leader_id)
            .unwrap();
        let proposal = |watermark, leader_term| GcWatermarkProposal {
            namespace: "__physical".into(),
            partition: 0,
            leader_term,
            watermark,
        };
        let submit = |state: &mut ControllerState, leader_id, entries| {
            apply_command(
                state,
                ControllerCommand::SetGcWatermarks { leader_id, entries },
            )
        };

        submit(
            &mut state,
            placement.leader_id,
            vec![proposal(10, placement.leader_term)],
        )
        .unwrap();
        assert_eq!(state.log_gc_watermark("__physical", 0), 10);
        // Not the leader, or a stale term: rejected.
        assert!(submit(&mut state, other, vec![proposal(20, placement.leader_term)]).is_err());
        assert!(submit(
            &mut state,
            placement.leader_id,
            vec![proposal(20, placement.leader_term + 1)]
        )
        .is_err());
        // Backwards: rejected.
        assert!(submit(
            &mut state,
            placement.leader_id,
            vec![proposal(9, placement.leader_term)]
        )
        .is_err());
        assert_eq!(state.log_gc_watermark("__physical", 0), 10);

        // A batch applies its valid entries and skips the rest.
        let stale_term = state.shards[&1].leader_term + 7;
        submit(
            &mut state,
            placement.leader_id,
            vec![
                proposal(30, placement.leader_term),
                GcWatermarkProposal {
                    namespace: "__physical".into(),
                    partition: 1,
                    leader_term: stale_term,
                    watermark: 5,
                },
            ],
        )
        .unwrap();
        assert_eq!(state.log_gc_watermark("__physical", 0), 30);
        assert_eq!(state.log_gc_watermark("__physical", 1), 0);

        let decoded: ControllerState =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert_eq!(decoded.log_gc_watermark("__physical", 0), 30);
        let mut legacy = serde_json::to_value(&state).unwrap();
        legacy.as_object_mut().unwrap().remove("log_gc_watermarks");
        let legacy: ControllerState = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.log_gc_watermark("__physical", 0), 0);
    }

    #[test]
    fn fourth_data_node_is_not_an_automatic_voter() {
        let ids: Vec<_> = (0..3).map(|_| Uuid::new_v4()).collect();
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    internal_url: None,
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 2, &HashMap::new(), None).unwrap();
        assert_eq!(state.controller_voters.len(), 3);
        let extra = Uuid::new_v4();
        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: extra,
                    addr: "http://n3".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: true,
                },
            },
        )
        .unwrap();
        assert_eq!(state.controller_voters.len(), 3);
        assert!(!state.controller_voters.contains(&extra));
        apply_command(
            &mut state,
            ControllerCommand::AddControllerVoter { node_id: extra },
        )
        .unwrap();
        assert!(state.controller_voters.contains(&extra));
        apply_command(
            &mut state,
            ControllerCommand::RemoveControllerVoter { node_id: extra },
        )
        .unwrap();
        assert!(!state.controller_voters.contains(&extra));
    }

    #[test]
    fn two_node_cell_grows_to_rf3_without_becoming_rf4() {
        let ids = [
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        ];
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids[..2]
                .iter()
                .enumerate()
                .map(|(index, id)| crate::cluster::NodeConfig {
                    internal_url: None,
                    id: *id,
                    addr: format!("http://n{index}"),
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        };
        let mut state = ControllerState::from_v1(&config, 4, &HashMap::new(), None).unwrap();
        assert_eq!(state.replication_factor, 2);
        assert_eq!(state.min_isr, 2);
        assert!(state.shards.values().all(|s| s.replicas.len() == 2));

        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: ids[2],
                    addr: "http://n2".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: false,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            ControllerCommand::SetReplicationPolicy {
                factor: 3,
                min_isr: 2,
            },
        )
        .unwrap();
        assert_eq!(state.replication_factor, 3);
        for shard in 0..4 {
            apply_command(
                &mut state,
                ControllerCommand::BeginRebalance {
                    shard,
                    add: Some(ids[2]),
                    remove: None,
                },
            )
            .unwrap();
            let (leader_id, leader_term) = (
                state.shards[&shard].leader_id,
                state.shards[&shard].leader_term,
            );
            // Only the committed shard leader may promote.
            assert!(apply_command(
                &mut state,
                ControllerCommand::PromoteLearner {
                    shard,
                    node_id: ids[2],
                    leader_id,
                    leader_term: leader_term + 1,
                },
            )
            .is_err());
            assert!(!state.shards[&shard].isr.contains(&ids[2]));
            apply_command(
                &mut state,
                ControllerCommand::PromoteLearner {
                    shard,
                    node_id: ids[2],
                    leader_id,
                    leader_term,
                },
            )
            .unwrap();
            apply_command(&mut state, ControllerCommand::CompleteRebalance { shard }).unwrap();
            assert_eq!(state.shards[&shard].replicas.len(), 3);
            assert!(state.shards[&shard].isr.contains(&ids[2]));
        }

        apply_command(
            &mut state,
            ControllerCommand::RegisterDataNode {
                node: DataNodeRecord {
                    id: ids[3],
                    addr: "http://n3".into(),
                    rack: None,
                    region: None,
                    broker: true,
                    controller_voter: false,
                },
            },
        )
        .unwrap();
        assert_eq!(state.replication_factor, 3);
        assert_eq!(state.data_nodes.len(), 4);
        assert!(state.shards.values().all(|s| s.replicas.len() == 3));
        assert!(state.shards.values().all(|s| !s.replicas.contains(&ids[3])));
    }
}
