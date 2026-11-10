//! Peer health probes, dispatch backfill on shard leadership failover, and catalog sync (CP7b).

use broker_api::{
    catalog_peer_targets, push_catalog_to_recovered_peer, sync_catalog_from_peers, AppState,
    Cluster, ClusterGossipRequest,
};
use broker_dispatch::DispatchEngine;
use broker_partition::Broker;
use broker_raft_meta::{ClusterRuntime, NodeConfig, DEFAULT_PEER_TTL_MS};
use broker_replication::{ReplicateCatchUpRange, ReplicateCatchUpRequest};
use broker_storage::StorageMode;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

/// Health ticks (2 s) between retries of unrepaired legacy payloads.
const LEGACY_REPAIR_RETRY_TICKS: u64 = 30;

pub(crate) fn discover_topics(state: &AppState) -> HashSet<String> {
    let mut topics = HashSet::from([
        broker_partition::DIRECT_TOPIC.to_string(),
        broker_partition::dlq_topic(broker_partition::DIRECT_TOPIC),
    ]);
    if let Ok(queues) = state.broker.list_queues() {
        for queue in queues {
            topics.insert(broker_partition::dlq_topic(&queue.topic));
            topics.insert(queue.topic);
        }
    }
    // DLQ namespaces come from the catalog too: a node taking over a shard may
    // never have received the first record of a DLQ topic.
    if let Ok(groups) = state.broker.list_groups() {
        for group in groups {
            topics.insert(broker_partition::group_topic(group.id));
            if let Ok(members) = state.broker.list_group_members(group.id) {
                topics.extend(
                    members.into_iter().map(|member| {
                        broker_partition::group_member_dlq_topic(group.id, member.id)
                    }),
                );
            }
        }
    }
    let root = state
        .broker
        .config()
        .data_dir
        .join("partitions")
        .join(state.broker.tenant());
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                topics.insert(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    topics
}

/// How a newly acquired shard regains a trustworthy durable prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardRecovery {
    /// Local WAL: copy the committed prefix from replica peers.
    ReplicaCatchUp,
    /// Slate: the log already lives on shared object storage. Frame
    /// replication is disabled there, so reopen it under the new fence.
    SharedStorage,
}

impl ShardRecovery {
    fn for_storage(mode: StorageMode) -> Self {
        match mode {
            StorageMode::Local => Self::ReplicaCatchUp,
            StorageMode::Slate => Self::SharedStorage,
        }
    }
}

async fn recover_gained_shard(
    cluster: &Cluster,
    state: &AppState,
    shard: u32,
) -> Result<(), CatchUpError> {
    match ShardRecovery::for_storage(state.broker.config().storage) {
        ShardRecovery::ReplicaCatchUp => catch_up_reconciling_tail(cluster, state, shard).await,
        ShardRecovery::SharedStorage => {
            let broker = state.broker.clone();
            let topics = discover_topics(state);
            tokio::task::spawn_blocking(move || reopen_shared_shard(&broker, &topics, shard))
                .await
                .map_err(|e| format!("shard {shard}: shared-storage reopen task failed: {e}"))?
                .map_err(CatchUpError::Retryable)
        }
    }
}

/// Tail truncations one recovery attempt may make before the shard stays
/// fenced until the next monitor cycle.
const MAX_TAIL_RECONCILIATIONS: usize = 3;

/// Catch up `shard`, cutting this node's own log tail where the replicas'
/// read quorum proves it was never acknowledged (decision D5, fenced
/// truncation), then retrying. A tail that cannot be cut (sealed segment,
/// inside a V2 epoch) keeps the shard fenced.
async fn catch_up_reconciling_tail(
    cluster: &Cluster,
    state: &AppState,
    shard: u32,
) -> Result<(), CatchUpError> {
    let mut reconciled = 0;
    loop {
        let error = match catch_up_shard(cluster, state, shard).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let Some((topic, to_offset)) = error.reconcile_target() else {
            return Err(error);
        };
        if reconciled >= MAX_TAIL_RECONCILIATIONS {
            return Err(error);
        }
        reconciled += 1;
        // V2 shards keep one physical log for every topic.
        let stream = if state.broker.layout().is_physical() {
            ""
        } else {
            topic
        };
        match cluster.reconcile_local_tail(&state.broker, stream, shard, to_offset) {
            Ok(log_end) => warn!(
                shard,
                %topic,
                to_offset,
                log_end,
                %error,
                "cut this node's unacknowledged log tail; retrying catch-up"
            ),
            Err(broker_partition::BrokerError::Storage(
                broker_storage::LogError::TruncateUnsupported(reason),
            )) => {
                tracing::error!(
                    shard,
                    %topic,
                    to_offset,
                    %reason,
                    %error,
                    "cannot cut the unverified log tail; shard stays fenced"
                );
                return Err(error);
            }
            Err(other) => {
                return Err(CatchUpError::Retryable(format!(
                    "{topic}/{shard}: truncate local tail to {to_offset}: {other}"
                )))
            }
        }
    }
}

/// Reopen every known topic of `shard` from shared storage. Cached handles
/// can predate writes made by another leader under a newer term, so they are
/// dropped first. Opening a handle recovers the durable next offset (and, for
/// SlateDB, fences the previous writer); a failure keeps the shard fenced.
fn reopen_shared_shard(
    broker: &Broker,
    topics: &HashSet<String>,
    shard: u32,
) -> Result<(), String> {
    broker.evict_slate_partitions(&[shard]);
    for topic in topics {
        let leo = broker
            .partition_high_watermark(topic, shard)
            .map_err(|e| format!("{topic}/{shard}: reopen shared storage: {e}"))?;
        let committed = broker
            .committed_hwm(topic, shard)
            .map_err(|e| format!("{topic}/{shard}: read durable HWM: {e}"))?;
        if committed != leo {
            return Err(format!(
                "{topic}/{shard}: shared storage reports durable HWM {committed} but next offset {leo}"
            ));
        }
    }
    Ok(())
}

/// Replica answers (besides this node's own log) a new shard leader needs
/// before it can trust its view of the committed prefix. A write is
/// acknowledged once `min_isr` replicas hold it, so any `rf - min_isr + 1`
/// replicas include at least one copy of every acknowledged record.
fn required_catch_up_answers(rf: usize, min_isr: usize, self_is_replica: bool) -> usize {
    let rf = rf.max(1);
    (rf + 1)
        .saturating_sub(min_isr.clamp(1, rf))
        .saturating_sub(usize::from(self_is_replica))
}

type ProbeResult = Result<ReplicateCatchUpRange, broker_replication::ReplicateError>;

/// Probe every replica concurrently so one unreachable peer costs a single
/// request timeout rather than one per peer.
async fn probe_replicas(
    cluster: &Cluster,
    peers: &[NodeConfig],
    request: &ReplicateCatchUpRequest,
) -> Vec<(NodeConfig, ProbeResult)> {
    let mut probes = tokio::task::JoinSet::new();
    for peer in peers {
        let client = cluster.replication.clone();
        let peer = peer.clone();
        let request = request.clone();
        probes.spawn(async move {
            let result = client.catch_up_from(peer.internal_base(), &request).await;
            (peer, result)
        });
    }
    let mut answers = Vec::with_capacity(peers.len());
    while let Some(joined) = probes.join_next().await {
        match joined {
            Ok(answer) => answers.push(answer),
            Err(error) => warn!(%error, "leadership catch-up probe task failed"),
        }
    }
    answers
}

/// Why a gained shard could not be unfenced.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CatchUpError {
    /// Storage, transport or quorum failure; a later attempt may succeed.
    Retryable(String),
    /// This node's log holds records past every answering replica's durable
    /// HWM. They may be unacknowledged, but cannot be proven so.
    LocalTailAhead {
        topic: String,
        local_leo: u64,
        replica_hwm: u64,
    },
    /// Two logs hold different records at the same offset. `local` when this
    /// node's log disagrees with the replicas (which agree with each other);
    /// otherwise the replicas disagree among themselves.
    Divergent {
        topic: String,
        offset: u64,
        detail: String,
        local: bool,
    },
}

impl CatchUpError {
    /// True when retrying cannot help: the shard stays fenced until the
    /// conflicting tail is reconciled under the leader fence (decision D5,
    /// fenced truncation) or leadership moves to a replica with the
    /// authoritative log.
    fn needs_tail_reconciliation(&self) -> bool {
        matches!(self, Self::LocalTailAhead { .. } | Self::Divergent { .. })
    }

    /// `(topic, offset)` to cut this node's log back to, when the catch-up
    /// read quorum proves everything from there on was never acknowledged:
    /// past every answering replica's committed HWM, or a local record the
    /// agreeing replicas do not hold. Replicas that disagree among themselves
    /// give no such proof.
    fn reconcile_target(&self) -> Option<(&str, u64)> {
        match self {
            Self::LocalTailAhead {
                topic, replica_hwm, ..
            } => Some((topic, *replica_hwm)),
            Self::Divergent {
                topic,
                offset,
                local: true,
                ..
            } => Some((topic, *offset)),
            _ => None,
        }
    }
}

impl std::fmt::Display for CatchUpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(message) => f.write_str(message),
            Self::LocalTailAhead {
                topic,
                local_leo,
                replica_hwm,
            } => write!(
                f,
                "{topic}: local LEO {local_leo} exceeds every answering replica's committed HWM \
                 {replica_hwm}; refusing to serve or overwrite an unverified tail"
            ),
            Self::Divergent {
                topic,
                offset,
                detail,
                ..
            } => write!(f, "{topic}: divergent record at offset {offset}: {detail}"),
        }
    }
}

impl From<String> for CatchUpError {
    fn from(message: String) -> Self {
        Self::Retryable(message)
    }
}

fn frame_message_id(bytes: &[u8]) -> Option<Uuid> {
    broker_proto::decode_frame(&mut std::io::Cursor::new(bytes))
        .ok()
        .map(|(header, _)| header.id)
}

/// Every answering replica must agree with the chosen source on the offsets
/// both returned; otherwise the replicas disagree about the prefix and copying
/// from either one could discard an acknowledged record.
fn verify_answers_agree(
    topic: &str,
    source: &ReplicateCatchUpRange,
    others: &[&ReplicateCatchUpRange],
) -> Result<(), CatchUpError> {
    let expected: HashMap<u64, Option<Uuid>> = source
        .frames
        .iter()
        .map(|frame| (frame.offset, frame_message_id(&frame.bytes)))
        .collect();
    for other in others {
        for frame in &other.frames {
            let Some(want) = expected.get(&frame.offset) else {
                continue;
            };
            let have = frame_message_id(&frame.bytes);
            if want.is_none() || have != *want {
                return Err(CatchUpError::Divergent {
                    topic: topic.to_string(),
                    offset: frame.offset,
                    detail: "replicas disagree on the record at this offset".into(),
                    local: false,
                });
            }
        }
    }
    Ok(())
}

async fn catch_up_shard(
    cluster: &Cluster,
    state: &AppState,
    shard: u32,
) -> Result<(), CatchUpError> {
    // Probe the shard's replica set directly instead of trusting this node's
    // health view: a briefly "dead" replica may hold acknowledged records.
    let node_id = cluster.runtime.config().node_id;
    let replicas = cluster.runtime.replica_ids_for_shard(shard);
    // Learners are still being seeded and never hold acknowledged records.
    let targets = cluster.runtime.replication_targets(shard);
    let (mut peers, min_isr) = (targets.replicas, targets.min_isr);
    let required = required_catch_up_answers(replicas.len(), min_isr, replicas.contains(&node_id));
    for topic in discover_topics(state) {
        let mut local_leo = state
            .broker
            .partition_high_watermark(&topic, shard)
            .map_err(|e| format!("{topic}/{shard}: read local LEO: {e}"))?;
        let local_committed = state
            .broker
            .committed_hwm(&topic, shard)
            .map_err(|e| format!("{topic}/{shard}: read local durable HWM: {e}"))?;
        let probe_from = local_committed.saturating_sub(1);

        let request = ReplicateCatchUpRequest {
            tenant_id: state.broker.tenant(),
            topic: topic.clone(),
            partition: shard,
            from_offset: probe_from,
            max_records: 4096,
        };
        let mut answered = Vec::new();
        let mut failed = HashSet::new();
        for (peer, result) in probe_replicas(cluster, &peers, &request).await {
            match result {
                Ok(range) => answered.push((peer.internal_base().to_owned(), range)),
                Err(error) => {
                    warn!(
                        peer = %peer.addr,
                        %topic,
                        shard,
                        error = %error,
                        "leadership catch-up probe failed"
                    );
                    failed.insert(peer.id);
                }
            }
        }
        // A replica that failed once is not retried for the remaining topics
        // of this attempt; the next monitor cycle starts over.
        peers.retain(|peer| !failed.contains(&peer.id));
        if answered.len() < required {
            return Err(CatchUpError::Retryable(format!(
                "{topic}/{shard}: {} of {required} required replicas answered the catch-up probe \
                 (RF {}, minISR {min_isr}); cannot prove the committed prefix",
                answered.len(),
                replicas.len().max(1),
            )));
        }
        let Some(source_index) = answered
            .iter()
            .enumerate()
            .max_by_key(|(_, (_, range))| range.committed_hwm)
            .map(|(index, _)| index)
        else {
            // This node's own log already covers every acknowledged record.
            continue;
        };
        let (source, mut page) = answered.swap_remove(source_index);
        let others: Vec<_> = answered.iter().map(|(_, range)| range).collect();
        verify_answers_agree(&topic, &page, &others)?;
        if local_leo > page.committed_hwm {
            return Err(CatchUpError::LocalTailAhead {
                topic,
                local_leo,
                replica_hwm: page.committed_hwm,
            });
        }
        if page.base_offset > local_leo {
            // The source already collected everything this node is missing
            // below its base (all of it delivered under a committed GC
            // watermark). Everything held locally is below that base, so
            // continue the log at the base instead of refusing forever.
            let log = state.broker.log_id(&topic, shard);
            state
                .broker
                .reset_log_base(&log, page.base_offset)
                .map_err(|e| {
                    format!(
                        "{topic}/{shard}: reset log base to {}: {e}",
                        page.base_offset
                    )
                })?;
            info!(
                %topic,
                shard,
                local_leo,
                base_offset = page.base_offset,
                "catch-up source collected this prefix; continuing at its retained base"
            );
        }

        loop {
            let mut last_appended = None;
            for frame in &page.frames {
                state
                    .broker
                    .append_replicated_frame(&topic, shard, &frame.bytes, Some(frame.offset))
                    .map_err(|e| match e {
                        broker_partition::BrokerError::Storage(
                            broker_storage::LogError::OffsetMismatch { .. },
                        ) => CatchUpError::Divergent {
                            topic: topic.clone(),
                            offset: frame.offset,
                            detail: e.to_string(),
                            local: true,
                        },
                        other => CatchUpError::Retryable(format!(
                            "{topic}/{shard}: apply catch-up frame at offset {}: {other}",
                            frame.offset
                        )),
                    })?;
                last_appended = Some(frame.offset);
            }
            if let Some(offset) = last_appended {
                state
                    .broker
                    .wait_committed(&topic, shard, offset)
                    .await
                    .map_err(|e| format!("{topic}/{shard}: fsync catch-up page: {e}"))?;
            }
            local_leo = state
                .broker
                .partition_high_watermark(&topic, shard)
                .map_err(|e| format!("{topic}/{shard}: refresh local LEO: {e}"))?;
            if local_leo >= page.committed_hwm {
                break;
            }
            let request = ReplicateCatchUpRequest {
                tenant_id: state.broker.tenant(),
                topic: topic.clone(),
                partition: shard,
                from_offset: local_leo,
                max_records: 4096,
            };
            page = cluster
                .replication
                .catch_up_from(&source, &request)
                .await
                .map_err(|e| format!("{topic}/{shard}: continue catch-up: {e}"))?;
            if page.frames.is_empty() && local_leo < page.committed_hwm {
                return Err(CatchUpError::Retryable(format!(
                    "{topic}/{shard}: source reported HWM {} but returned no frame at {local_leo}",
                    page.committed_hwm
                )));
            }
        }
    }
    Ok(())
}

const HEALTH_INTERVAL: Duration = Duration::from_secs(2);
/// Per-request timeout for health probes and gossip.
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// A health refresh (probes, then gossip) must finish well inside the peer
/// TTL so a slow or blackholed peer cannot make healthy peers look expired.
fn health_cycle_deadline() -> Duration {
    Duration::from_millis((DEFAULT_PEER_TTL_MS / 2).max(1) as u64)
}

/// Aborts the wrapped task when dropped, tying a helper task's lifetime to
/// the task that owns the guard.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Probe every peer concurrently, stamp each success when its response
/// arrives, persist the observations in one write, then gossip the merged view
/// to the peers that answered.
async fn refresh_peer_health(runtime: &ClusterRuntime, client: &reqwest::Client) {
    let node_id = runtime.config().node_id;
    let peers = runtime.config().peer_nodes();
    let mut probes = tokio::task::JoinSet::new();
    for peer in peers.iter().cloned() {
        let client = client.clone();
        probes.spawn(async move {
            let path = if peer.internal_url.is_some() {
                "/internal/v1/cluster"
            } else {
                "/healthz"
            };
            let url = format!("{}{path}", peer.internal_base());
            let result = broker_api::cluster_auth::apply_cluster_secret(client.get(&url))
                .send()
                .await
                .map(|resp| resp.status());
            (peer, result, Utc::now().timestamp_millis())
        });
    }
    let mut seen = HashMap::from([(node_id, Utc::now().timestamp_millis())]);
    while let Some(joined) = probes.join_next().await {
        let Ok((peer, result, seen_at)) = joined else {
            continue;
        };
        match result {
            Ok(status) if status.is_success() => {
                seen.insert(peer.id, seen_at);
            }
            Ok(status) => warn!(peer = %peer.addr, %status, "peer health check failed"),
            Err(error) => warn!(peer = %peer.addr, %error, "peer health check unreachable"),
        }
    }
    let healthy: HashSet<Uuid> = seen.keys().copied().collect();
    let store = runtime.clone();
    let snapshot = match tokio::task::spawn_blocking(move || {
        store.merge_peer_health(&seen);
        store.peer_health_snapshot()
    })
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(%error, "persisting peer health observations failed");
            return;
        }
    };

    let gossip = Arc::new(ClusterGossipRequest {
        observer: node_id,
        seen: snapshot,
        controller_vote: None,
        controller_leader: None,
    });
    let mut sends = tokio::task::JoinSet::new();
    for peer in peers.into_iter().filter(|peer| healthy.contains(&peer.id)) {
        let client = client.clone();
        let gossip = Arc::clone(&gossip);
        sends.spawn(async move {
            let url = format!("{}/internal/v1/cluster/gossip", peer.internal_base());
            let result =
                broker_api::cluster_auth::apply_cluster_secret(client.post(&url).json(&*gossip))
                    .send()
                    .await;
            (peer, result.map(|resp| resp.status()))
        });
    }
    while let Some(joined) = sends.join_next().await {
        match joined {
            Ok((_, Ok(status))) if status.is_success() => {}
            Ok((peer, Ok(status))) => {
                warn!(peer = %peer.addr, %status, "cluster gossip rejected");
            }
            Ok((peer, Err(error))) => warn!(peer = %peer.addr, %error, "cluster gossip failed"),
            Err(error) => warn!(%error, "cluster gossip task failed"),
        }
    }
}

async fn peer_health_loop(runtime: ClusterRuntime, client: reqwest::Client) {
    let mut interval = tokio::time::interval(HEALTH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if tokio::time::timeout(
            health_cycle_deadline(),
            refresh_peer_health(&runtime, &client),
        )
        .await
        .is_err()
        {
            warn!(
                deadline_ms = health_cycle_deadline().as_millis() as u64,
                "peer health refresh exceeded its deadline; abandoned outstanding probes"
            );
        }
    }
}

pub fn spawn_cluster_health_monitor(
    cluster: Cluster,
    dispatch: DispatchEngine,
    state: Arc<AppState>,
) -> tokio::task::JoinHandle<()> {
    let runtime = cluster.runtime.clone();
    let node_id = runtime.config().node_id;

    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(PEER_REQUEST_TIMEOUT)
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "cluster health monitor: failed to build HTTP client");
                return;
            }
        };
        // Health refresh runs on its own cadence so catch-up, catalog sync or
        // placement commits below can never delay liveness observations.
        let _health = AbortOnDrop(tokio::spawn(peer_health_loop(runtime.clone(), client)));

        let mut prev_leaders: HashMap<u32, Uuid> = HashMap::new();
        let mut repair_ticks = 0u64;
        let mut prev_peer_alive: HashMap<Uuid, bool> = HashMap::new();
        let mut interval = tokio::time::interval(HEALTH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate tick so the first health refresh lands before
        // the first placement and recovered-peer decisions.
        interval.tick().await;
        loop {
            interval.tick().await;
            let now = Utc::now().timestamp_millis();

            for (peer_id, peer_url) in catalog_peer_targets(&state) {
                let alive = runtime.is_peer_alive(peer_id, now);
                if alive && prev_peer_alive.get(&peer_id) == Some(&false) {
                    if push_catalog_to_recovered_peer(&peer_url, &state).await {
                        info!(peer = %peer_url, "pushed catalog to recovered peer");
                    } else {
                        warn!(peer = %peer_url, "failed to push catalog to recovered peer");
                    }
                }
                prev_peer_alive.insert(peer_id, alive);
            }

            if !runtime.has_controller_lease() {
                continue;
            }
            // Repair lagging followers (backstop for missed pushes), catch up
            // and promote learners, and commit the measured ISR of led shards.
            cluster.replication.maintain_led_shards().await;

            repair_ticks = repair_ticks.wrapping_add(1);
            if repair_ticks % LEGACY_REPAIR_RETRY_TICKS == 0 {
                let unresolved = broker_partition::payload_repair_snapshot().unresolved_by_shard;
                for shard in unresolved.into_keys() {
                    if runtime.is_leader_for_shard(shard) {
                        broker_api::repair_legacy_payloads(&state, shard).await;
                    }
                }
            }

            let mut gained_shards = Vec::new();
            let mut lost_shards = Vec::new();
            let mut led_shards = Vec::new();
            for shard in 0..state.broker.layout().shard_count {
                if runtime.is_controller_leader() {
                    let current = runtime.elect_leader_for_shard(shard);
                    let desired = runtime.desired_leader_for_shard(shard);
                    if let Some(desired) = desired.filter(|desired| Some(*desired) != current) {
                        if let Err(error) = runtime.assign_shard_leadership(shard, desired).await {
                            warn!(shard, %error, "failed to commit OpenRaft shard placement");
                        }
                    }
                }
                let leader = match runtime.elect_leader_for_shard(shard) {
                    Some(id) => id,
                    None => continue,
                };
                let was = prev_leaders.insert(shard, leader);
                if leader == node_id {
                    led_shards.push(shard);
                }
                if leader == node_id && was != Some(node_id) {
                    runtime.set_shard_ready(shard, false);
                    gained_shards.push(shard);
                }
                if was == Some(node_id) && leader != node_id {
                    lost_shards.push(shard);
                }
            }

            if !lost_shards.is_empty() {
                info!(shards = ?lost_shards, "failover: evicting slate handles for lost shards");
                state.broker.evict_slate_partitions(&lost_shards);
            }

            if !gained_shards.is_empty() {
                info!(
                    shards = ?gained_shards,
                    "failover: this node gained shard leadership; catching up durable prefixes"
                );
                // Quorum-gated cells: no dispatch on a taken-over shard until its
                // tail is re-verified on minISR and cursors are reconciled.
                let gated = state.broker.quorum_gating();
                if gated {
                    for shard in &gained_shards {
                        dispatch.fence_partition(*shard);
                    }
                }
                sync_catalog_from_peers(&state).await;
                let mut caught_up = Vec::new();
                for shard in gained_shards {
                    match recover_gained_shard(&cluster, &state, shard).await {
                        Ok(()) => {
                            // Legacy blob references must be repaired before
                            // the shard serves; unrepairable ones stay deferred.
                            let repair = broker_api::repair_legacy_payloads(&state, shard).await;
                            if repair.unresolved > 0 {
                                warn!(
                                    shard,
                                    unresolved = repair.unresolved,
                                    "shard serving with unrepaired legacy payloads; retrying periodically"
                                );
                            }
                            if gated {
                                dispatch.set_recovery_targets(
                                    shard,
                                    crate::quorum_repair::recovery_targets(&state, shard),
                                );
                            }
                            runtime.set_shard_ready(shard, true);
                            caught_up.push(shard);
                        }
                        Err(error) => {
                            runtime.set_shard_ready(shard, false);
                            prev_leaders.remove(&shard);
                            broker_api::record_shard_recovery_failure(
                                if error.needs_tail_reconciliation() {
                                    broker_api::ShardRecoveryFailure::UnverifiedTail
                                } else {
                                    broker_api::ShardRecoveryFailure::Retryable
                                },
                            );
                            if error.needs_tail_reconciliation() {
                                tracing::error!(
                                    shard,
                                    %error,
                                    "failover catch-up found a log tail replicas do not confirm; \
                                     shard remains fenced until the tail is reconciled or \
                                     leadership moves"
                                );
                            } else {
                                warn!(shard, %error, "failover catch-up failed; shard remains fenced");
                            }
                        }
                    }
                }
                if !caught_up.is_empty() && gated {
                    info!(
                        shards = ?caught_up,
                        "failover catch-up complete; ingest open, dispatch waits for quorum verification"
                    );
                } else if !caught_up.is_empty() {
                    info!(shards = ?caught_up, "failover catch-up complete; shards unfenced");
                    // Catch-up may have rewritten these logs; rescan them.
                    dispatch.reset_dispatch_scan(&caught_up);
                    let dispatch = dispatch.clone();
                    let _ = tokio::task::spawn_blocking(move || dispatch.backfill_pending()).await;
                }
            }
            broker_api::set_fenced_shards(
                led_shards
                    .iter()
                    .filter(|shard| !runtime.shard_ready(**shard))
                    .count() as u64,
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};

    #[test]
    fn leadership_gained_when_preferred_peer_stale() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let nodes: Vec<NodeConfig> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| NodeConfig {
                internal_url: None,
                id: *id,
                addr: format!("http://b{i}:8080"),
            })
            .collect();
        let cfg = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: nodes.clone(),
            node_id: ids[2],
            generation: 1,
            hash_version: 1,
        };
        let rt = ClusterRuntime::from_config_only(cfg);
        let now = Utc::now().timestamp_millis();
        rt.record_self_alive(now);
        rt.record_peer_alive(ids[0], now);
        // ids[1] stale → shard 1 fails over from preferred ids[1] to ids[2]
        assert!(rt.is_leader_for_shard(1));
    }

    #[test]
    fn slate_shards_recover_from_shared_storage_not_replica_frames() {
        assert_eq!(
            ShardRecovery::for_storage(StorageMode::Slate),
            ShardRecovery::SharedStorage
        );
        assert_eq!(
            ShardRecovery::for_storage(StorageMode::Local),
            ShardRecovery::ReplicaCatchUp
        );
    }

    fn direct_publish(payload: &str) -> broker_partition::PublishRequest {
        broker_partition::PublishRequest {
            routing_id: None,
            topic: broker_partition::DIRECT_TOPIC.into(),
            routing_key: "same-shard".into(),
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
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
            url: Some("http://example.invalid/hook".into()),
            secret: Some("s".into()),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_storage_reopen_validates_every_topic_without_peers() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(broker_partition::BrokerConfig::new(dir.path().into())).unwrap();
        let resp = broker.publish(direct_publish("durable")).unwrap();
        let shard = resp.partition.unwrap();
        broker
            .wait_committed(&resp.topic, shard, resp.offset.unwrap())
            .await
            .unwrap();
        let topics = HashSet::from([broker_partition::DIRECT_TOPIC.to_string()]);
        reopen_shared_shard(&broker, &topics, shard).unwrap();
        assert_eq!(
            broker
                .partition_high_watermark(broker_partition::DIRECT_TOPIC, shard)
                .unwrap(),
            1
        );

        let error = reopen_shared_shard(&broker, &topics, broker.shard_count()).unwrap_err();
        assert!(error.contains("reopen shared storage"), "{error}");
    }

    const UNREACHABLE: &str = "http://127.0.0.1:1";

    /// A local-WAL node in a three-node (RF=3, minISR=2) cluster whose two
    /// peers live at `peer_addrs`.
    fn three_node_state(
        dir: &tempfile::TempDir,
        peer_addrs: [&str; 2],
    ) -> (Arc<AppState>, Cluster, [Uuid; 2]) {
        let local = Uuid::new_v4();
        let peer_ids = [Uuid::new_v4(), Uuid::new_v4()];
        let mut nodes = vec![NodeConfig {
            id: local,
            addr: "http://local.invalid".into(),
            internal_url: None,
        }];
        nodes.extend(
            peer_ids
                .iter()
                .zip(peer_addrs)
                .map(|(id, addr)| NodeConfig {
                    id: *id,
                    addr: addr.to_string(),
                    internal_url: None,
                }),
        );
        let cluster = Cluster::new(ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes,
            node_id: local,
            generation: 1,
            hash_version: 1,
        }));
        let broker = Broker::open(broker_partition::BrokerConfig::new(dir.path().into())).unwrap();
        let state = Arc::new(AppState {
            schedule: broker_schedule::ScheduleQueue::open(dir.path()).unwrap(),
            crons: broker_schedule::CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(
                broker.clone(),
                broker_dispatch::DispatchConfig::default(),
            ),
            broker,
            leases: broker_dispatch::LeaseTable::new(),
            cluster: Some(cluster.clone()),
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: broker_api::CatalogTombstones::open(dir.path()).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        (state, cluster, peer_ids)
    }

    fn catch_up_body(shard: u32, committed_hwm: u64, frames: Vec<(u64, Vec<u8>)>) -> Vec<u8> {
        ReplicateCatchUpRange {
            partition: shard,
            leader_generation: 0,
            committed_hwm,
            base_offset: 0,
            frames: frames
                .into_iter()
                .map(|(offset, bytes)| broker_replication::CatchUpFrame { offset, bytes })
                .collect(),
        }
        .encode_binary()
        .unwrap()
    }

    async fn catch_up_peer(body: Vec<u8>) -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/internal/v1/replicate/catch-up"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .insert_header("content-type", "application/octet-stream")
                    .set_body_bytes(body),
            )
            .mount(&server)
            .await;
        server
    }

    #[test]
    fn catch_up_read_quorum_intersects_every_ack_set() {
        // RF=3/minISR=2: self plus one replica covers every acked record.
        assert_eq!(required_catch_up_answers(3, 2, true), 1);
        // minISR=1 acks on one copy, so every other replica must answer.
        assert_eq!(required_catch_up_answers(3, 1, true), 2);
        // A leader outside the replica set holds nothing itself.
        assert_eq!(required_catch_up_answers(3, 2, false), 2);
        assert_eq!(required_catch_up_answers(2, 2, true), 0);
        assert_eq!(required_catch_up_answers(1, 1, true), 0);
        assert_eq!(required_catch_up_answers(1, 1, false), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catch_up_stays_fenced_when_no_replica_answers() {
        let dir = tempfile::tempdir().unwrap();
        let (state, cluster, _) = three_node_state(&dir, [UNREACHABLE, UNREACHABLE]);
        let error = catch_up_shard(&cluster, &state, 0).await.unwrap_err();
        assert!(!error.needs_tail_reconciliation());
        assert!(
            error.to_string().contains("0 of 1 required replicas"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catch_up_proceeds_with_a_read_quorum_while_one_replica_is_down() {
        let dir = tempfile::tempdir().unwrap();
        let shard = 0;
        let peer = catch_up_peer(catch_up_body(shard, 0, Vec::new())).await;
        let (state, cluster, _) = three_node_state(&dir, [&peer.uri(), UNREACHABLE]);
        catch_up_shard(&cluster, &state, shard).await.unwrap();
    }

    async fn committed_publish(broker: &Broker, payload: &str) -> (u32, Vec<u8>) {
        let resp = broker.publish(direct_publish(payload)).unwrap();
        let shard = resp.partition.unwrap();
        broker
            .wait_committed(&resp.topic, shard, resp.offset.unwrap())
            .await
            .unwrap();
        (shard, resp.replication_frame.unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uncommitted_local_tail_stays_fenced_without_being_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let empty = catch_up_body(0, 0, Vec::new());
        let (a, b) = (
            catch_up_peer(empty.clone()).await,
            catch_up_peer(empty).await,
        );
        let (state, cluster, _) = three_node_state(&dir, [&a.uri(), &b.uri()]);
        let (shard, _) = committed_publish(&state.broker, "local-only").await;

        let error = catch_up_shard(&cluster, &state, shard).await.unwrap_err();
        assert!(error.needs_tail_reconciliation(), "{error}");
        assert_eq!(
            error,
            CatchUpError::LocalTailAhead {
                topic: broker_partition::DIRECT_TOPIC.into(),
                local_leo: 1,
                replica_hwm: 0,
            }
        );
        let kept = state
            .broker
            .read_message(broker_partition::DIRECT_TOPIC, shard, 0)
            .unwrap();
        assert_eq!(kept.payload, b"local-only");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn divergent_replica_record_keeps_the_shard_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let other =
            Broker::open(broker_partition::BrokerConfig::new(other_dir.path().into())).unwrap();
        let (shard, foreign) = committed_publish(&other, "replica").await;
        let peer = catch_up_peer(catch_up_body(shard, 1, vec![(0, foreign)])).await;
        let (state, cluster, _) = three_node_state(&dir, [&peer.uri(), UNREACHABLE]);
        committed_publish(&state.broker, "local").await;

        let error = catch_up_shard(&cluster, &state, shard).await.unwrap_err();
        assert!(
            matches!(&error, CatchUpError::Divergent { offset: 0, .. }),
            "{error}"
        );
        assert!(error.needs_tail_reconciliation());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_cuts_an_unacknowledged_local_tail_and_catches_up() {
        let dir = tempfile::tempdir().unwrap();
        let empty = catch_up_body(0, 0, Vec::new());
        let (a, b) = (
            catch_up_peer(empty.clone()).await,
            catch_up_peer(empty).await,
        );
        let (state, cluster, _) = three_node_state(&dir, [&a.uri(), &b.uri()]);
        let (shard, _) = committed_publish(&state.broker, "local-only").await;
        let topic = broker_partition::DIRECT_TOPIC;

        recover_gained_shard(&cluster, &state, shard).await.unwrap();
        assert_eq!(
            state.broker.partition_high_watermark(topic, shard).unwrap(),
            0,
            "no answering replica held the record, so it was never acknowledged"
        );
        assert_eq!(state.broker.committed_hwm(topic, shard).unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_replaces_a_divergent_local_record_with_the_replicas_copy() {
        let dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let other =
            Broker::open(broker_partition::BrokerConfig::new(other_dir.path().into())).unwrap();
        let (shard, foreign) = committed_publish(&other, "replica").await;
        let peer = catch_up_peer(catch_up_body(shard, 1, vec![(0, foreign)])).await;
        let (state, cluster, _) = three_node_state(&dir, [&peer.uri(), UNREACHABLE]);
        committed_publish(&state.broker, "local").await;

        recover_gained_shard(&cluster, &state, shard).await.unwrap();
        let topic = broker_partition::DIRECT_TOPIC;
        assert_eq!(
            state.broker.read_message(topic, shard, 0).unwrap().payload,
            b"replica"
        );
        assert_eq!(state.broker.committed_hwm(topic, shard).unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_does_not_cut_the_tail_when_replicas_disagree() {
        let dir = tempfile::tempdir().unwrap();
        let (x_dir, y_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let x = Broker::open(broker_partition::BrokerConfig::new(x_dir.path().into())).unwrap();
        let y = Broker::open(broker_partition::BrokerConfig::new(y_dir.path().into())).unwrap();
        let (shard, x_frame) = committed_publish(&x, "x").await;
        let (_, y_frame) = committed_publish(&y, "y").await;
        let a = catch_up_peer(catch_up_body(shard, 1, vec![(0, x_frame)])).await;
        let b = catch_up_peer(catch_up_body(shard, 1, vec![(0, y_frame)])).await;
        let (state, cluster, _) = three_node_state(&dir, [&a.uri(), &b.uri()]);
        committed_publish(&state.broker, "local").await;

        let error = recover_gained_shard(&cluster, &state, shard)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatchUpError::Divergent { local: false, .. }),
            "{error}"
        );
        assert_eq!(
            state
                .broker
                .read_message(broker_partition::DIRECT_TOPIC, shard, 0)
                .unwrap()
                .payload,
            b"local"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replicas_that_disagree_on_the_prefix_are_not_copied() {
        let dir = tempfile::tempdir().unwrap();
        let (x_dir, y_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let x = Broker::open(broker_partition::BrokerConfig::new(x_dir.path().into())).unwrap();
        let y = Broker::open(broker_partition::BrokerConfig::new(y_dir.path().into())).unwrap();
        let (shard, x_frame) = committed_publish(&x, "x").await;
        let (_, y_frame) = committed_publish(&y, "y").await;
        let a = catch_up_peer(catch_up_body(shard, 1, vec![(0, x_frame)])).await;
        let b = catch_up_peer(catch_up_body(shard, 1, vec![(0, y_frame)])).await;
        let (state, cluster, _) = three_node_state(&dir, [&a.uri(), &b.uri()]);

        let error = catch_up_shard(&cluster, &state, shard).await.unwrap_err();
        assert!(
            matches!(&error, CatchUpError::Divergent { offset: 0, .. }),
            "{error}"
        );
        assert_eq!(
            state
                .broker
                .partition_high_watermark(broker_partition::DIRECT_TOPIC, shard)
                .unwrap(),
            0
        );
    }

    async fn health_peer(delay: Duration) -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/healthz"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(delay))
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/internal/v1/cluster/gossip"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;
        server
    }

    async fn gossip_requests(server: &wiremock::MockServer) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| request.url.path() == "/internal/v1/cluster/gossip")
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blackholed_peers_cannot_stretch_a_health_cycle_past_the_peer_ttl() {
        let healthy = health_peer(Duration::ZERO).await;
        let blackholed = [
            health_peer(Duration::from_secs(30)).await,
            health_peer(Duration::from_secs(30)).await,
        ];
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let addrs = [
            "http://local.invalid".to_string(),
            healthy.uri(),
            blackholed[0].uri(),
            blackholed[1].uri(),
        ];
        let runtime = ClusterRuntime::from_config_only(ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: ids
                .iter()
                .zip(addrs)
                .map(|(id, addr)| NodeConfig {
                    id: *id,
                    addr,
                    internal_url: None,
                })
                .collect(),
            node_id: ids[0],
            generation: 1,
            hash_version: 1,
        });
        let client = reqwest::Client::builder()
            .timeout(PEER_REQUEST_TIMEOUT)
            .build()
            .unwrap();

        let started = Utc::now().timestamp_millis();
        let clock = std::time::Instant::now();
        refresh_peer_health(&runtime, &client).await;
        let elapsed = clock.elapsed();

        // Probes run concurrently: two blackholed peers cost one timeout.
        assert!(
            elapsed < PEER_REQUEST_TIMEOUT + Duration::from_secs(1),
            "{elapsed:?}"
        );
        assert!(elapsed < health_cycle_deadline());
        let seen = runtime.peer_health_snapshot();
        assert!(seen[&ids[1]] >= started, "stamped at response time");
        assert!(!seen.contains_key(&ids[2]) && !seen.contains_key(&ids[3]));
        let now = Utc::now().timestamp_millis();
        assert!(runtime.is_peer_alive(ids[1], now));
        assert!(!runtime.is_peer_alive(ids[2], now));
        // Gossip goes only to peers that answered this cycle.
        assert_eq!(gossip_requests(&healthy).await, 1);
        assert_eq!(gossip_requests(&blackholed[0]).await, 0);
        assert_eq!(gossip_requests(&blackholed[1]).await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lagging_leader_copies_the_committed_prefix_from_a_replica() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = Broker::open(broker_partition::BrokerConfig::new(
            source_dir.path().into(),
        ))
        .unwrap();
        let (shard, first) = committed_publish(&source, "one").await;
        let (_, second) = committed_publish(&source, "two").await;
        let peer = catch_up_peer(catch_up_body(shard, 2, vec![(0, first), (1, second)])).await;
        let (state, cluster, _) = three_node_state(&dir, [&peer.uri(), UNREACHABLE]);

        catch_up_shard(&cluster, &state, shard).await.unwrap();
        let topic = broker_partition::DIRECT_TOPIC;
        assert_eq!(state.broker.committed_hwm(topic, shard).unwrap(), 2);
        assert_eq!(
            state.broker.read_message(topic, shard, 1).unwrap().payload,
            b"two"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn short_catch_up_page_keeps_the_shard_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = Broker::open(broker_partition::BrokerConfig::new(
            source_dir.path().into(),
        ))
        .unwrap();
        let (shard, first) = committed_publish(&source, "one").await;
        let peer = wiremock::MockServer::start().await;
        let octet = |body: Vec<u8>| {
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(body)
        };
        // The replica claims HWM 2 but stops returning frames after offset 0.
        wiremock::Mock::given(wiremock::matchers::path("/internal/v1/replicate/catch-up"))
            .and(wiremock::matchers::body_partial_json(
                serde_json::json!({"from_offset": 1}),
            ))
            .respond_with(octet(catch_up_body(shard, 2, Vec::new())))
            .with_priority(1)
            .mount(&peer)
            .await;
        wiremock::Mock::given(wiremock::matchers::path("/internal/v1/replicate/catch-up"))
            .respond_with(octet(catch_up_body(shard, 2, vec![(0, first)])))
            .mount(&peer)
            .await;
        let (state, cluster, _) = three_node_state(&dir, [&peer.uri(), UNREACHABLE]);

        let error = catch_up_shard(&cluster, &state, shard).await.unwrap_err();
        assert!(!error.needs_tail_reconciliation());
        assert!(
            error.to_string().contains("returned no frame at 1"),
            "{error}"
        );
    }
}
