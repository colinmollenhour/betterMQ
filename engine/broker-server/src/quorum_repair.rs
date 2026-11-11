//! Quorum verification for multi-node local-WAL cells (decision D2).
//!
//! Live publishes advance the quorum watermark as their epochs reach minISR.
//! This task covers the rest:
//! - an epoch whose replication failed leaves a gap that would stall dispatch
//!   for the shard; its records are re-replicated (idempotent on replicas that
//!   already hold them, identity-checked by expected offset + message id);
//! - after a takeover (and at startup) the local tail above the persisted
//!   watermark is re-verified on minISR before dispatch is unfenced and lane
//!   cursors are reconciled.
//!
//! The watermark is never seeded from a peer's reported HWM: it only moves on
//! replication acks for frames this node sent. When no quorum answers, the
//! shard stays fenced / gated (fail closed). Replica selection and minISR come
//! from `ReplicationClient` (truthful ISR is decision D5's integration point).

use broker_api::{AppState, Cluster};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Records and bytes re-replicated per verification epoch.
const VERIFY_RECORDS: usize = 1024;
const VERIFY_BYTES: usize = 8 * 1024 * 1024;
/// Verification epochs per log per tick.
const VERIFY_EPOCHS_PER_TICK: usize = 16;
const TICK: Duration = Duration::from_millis(500);
const WARN_EVERY: Duration = Duration::from_secs(60);

/// Logs whose quorum watermark must be tracked for `shard`: in V2 the physical
/// shard is one log; in V1 every topic (queues, groups, direct, DLQs) is.
pub(crate) fn quorum_namespaces(state: &AppState, shard: u32) -> Vec<String> {
    let _ = shard;
    if state.broker.layout().is_physical() {
        return vec![broker_partition::DIRECT_TOPIC.to_string()];
    }
    let mut topics: Vec<String> = crate::cluster_health::discover_topics(state)
        .into_iter()
        .collect();
    topics.sort();
    topics
}

/// `(topic, committed watermark)` targets for a takeover of `shard`.
pub(crate) fn recovery_targets(state: &AppState, shard: u32) -> Vec<(String, u64)> {
    quorum_namespaces(state, shard)
        .into_iter()
        .filter_map(|topic| {
            let committed = state.broker.committed_hwm(&topic, shard).ok()?;
            Some((topic, committed))
        })
        .collect()
}

/// Re-replicate `[quorum, committed)` of one log in bounded epochs and advance
/// the quorum watermark on success. Returns the watermark reached.
async fn verify_tail(
    cluster: &Cluster,
    state: &AppState,
    topic: &str,
    shard: u32,
    committed: u64,
) -> Result<u64, String> {
    let broker = &state.broker;
    let mut quorum = broker
        .quorum_hwm(topic, shard)
        .map_err(|e| format!("read quorum watermark: {e}"))?;
    let epoch_topic = if broker.layout().is_physical() {
        String::new()
    } else {
        topic.to_string()
    };
    for _ in 0..VERIFY_EPOCHS_PER_TICK {
        if quorum >= committed {
            break;
        }
        let frames = broker
            .namespace_replication_frames(topic, shard, quorum, VERIFY_RECORDS, VERIFY_BYTES)
            .map_err(|e| format!("read frames at {quorum}: {e}"))?;
        let first = frames.first().map(|(offset, _)| *offset);
        match first {
            // Offsets below `committed` with no readable record were purged:
            // there is nothing at them to deliver or to prove.
            None => {
                quorum = broker
                    .record_quorum_commit(topic, shard, quorum, committed)
                    .map_err(|e| e.to_string())?;
                continue;
            }
            Some(first) if first > quorum => {
                quorum = broker
                    .record_quorum_commit(topic, shard, quorum, first.min(committed))
                    .map_err(|e| e.to_string())?;
                continue;
            }
            Some(_) => {}
        }
        let run: Vec<Vec<u8>> = frames
            .into_iter()
            .enumerate()
            .take_while(|(i, (offset, _))| *offset == quorum + *i as u64 && *offset < committed)
            .map(|(_, (_, frame))| frame)
            .collect();
        let end = quorum + run.len() as u64;
        let request = broker_replication::ReplicateBatchRequest::new(
            broker.tenant(),
            epoch_topic.clone(),
            shard,
            cluster.runtime.config().node_id,
            cluster.runtime.shard_generation(shard),
            quorum,
            run,
        )
        .map_err(|e| format!("build verification epoch: {e}"))?;
        cluster
            .replication
            .replicate_batch(request, committed)
            .await
            .map_err(|e| format!("verify [{quorum}, {end}) on minISR: {e}"))?;
        quorum = broker
            .record_quorum_commit(topic, shard, quorum, end)
            .map_err(|e| e.to_string())?;
    }
    Ok(quorum)
}

pub fn spawn_quorum_repair(cluster: Cluster, state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seen: HashMap<(String, u32), (u64, u64)> = HashMap::new();
        let mut warned: HashMap<(String, u32), Instant> = HashMap::new();
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let recovering: HashMap<u32, Vec<(String, u64)>> =
                state.dispatch.recovery_targets().into_iter().collect();
            for shard in 0..state.broker.layout().shard_count {
                if !cluster.runtime.is_leader_for_shard(shard) {
                    continue;
                }
                let fenced = recovering.contains_key(&shard);
                for topic in quorum_namespaces(&state, shard) {
                    let (Ok(quorum), Ok(committed)) = (
                        state.broker.quorum_hwm(&topic, shard),
                        state.broker.committed_hwm(&topic, shard),
                    ) else {
                        continue;
                    };
                    let key = (topic.clone(), shard);
                    // Live epochs advance the watermark themselves; step in when
                    // it made no progress for a whole tick, or during recovery.
                    let stalled = seen
                        .insert(key.clone(), (quorum, committed))
                        .is_some_and(|(q, c)| q == quorum && c > quorum);
                    if quorum >= committed || !(fenced || stalled) {
                        continue;
                    }
                    if let Err(error) =
                        verify_tail(&cluster, &state, &topic, shard, committed).await
                    {
                        let now = Instant::now();
                        if warned
                            .get(&key)
                            .is_none_or(|at| now.duration_since(*at) >= WARN_EVERY)
                        {
                            warned.insert(key, now);
                            warn!(
                                %topic,
                                shard,
                                quorum,
                                committed,
                                fenced,
                                %error,
                                "quorum verification blocked; dispatch stays gated"
                            );
                        }
                    }
                }
                if let Some(targets) = recovering.get(&shard) {
                    let verified = targets.iter().all(|(topic, target)| {
                        state
                            .broker
                            .quorum_hwm(topic, shard)
                            .is_ok_and(|quorum| quorum >= *target)
                    });
                    if !verified {
                        continue;
                    }
                    match state.dispatch.complete_recovery(shard) {
                        Ok(clamped) => {
                            info!(
                                shard,
                                clamped, "takeover verified on minISR; dispatch resumed"
                            );
                            let dispatch = state.dispatch.clone();
                            let _ =
                                tokio::task::spawn_blocking(move || dispatch.backfill_pending())
                                    .await;
                        }
                        Err(error) => warn!(shard, %error, "dispatch recovery failed"),
                    }
                }
            }
        }
    })
}

/// How often a shard leader shares its lane cursors with peers.
const CURSOR_CHECKPOINT_EVERY: Duration = Duration::from_secs(2);

/// Share the lane cursors of shards this node leads with every peer.
///
/// Without a shared metadata dir each node keeps its own cursors, so a node
/// taking over a shard would otherwise redeliver everything delivered since it
/// last led it. With checkpoints the redelivery after failover is bounded by
/// the checkpoint interval (plus out-of-order completion gaps above cursors).
pub fn spawn_cursor_checkpoints(
    cluster: Cluster,
    state: Arc<AppState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = cluster.runtime.config().node_id;
        let http = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(http) => http,
            Err(error) => {
                warn!(%error, "cursor checkpoints disabled: HTTP client");
                return;
            }
        };
        let mut sent: HashMap<uuid::Uuid, HashMap<(String, String, u32), u64>> = HashMap::new();
        let mut warned: HashMap<uuid::Uuid, Instant> = HashMap::new();
        let mut tick = tokio::time::interval(CURSOR_CHECKPOINT_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let broker = state.broker.clone();
            let Ok(Ok(all)) =
                tokio::task::spawn_blocking(move || broker.all_dispatch_offsets()).await
            else {
                continue;
            };
            let led: Vec<_> = all
                .into_iter()
                .filter(|(_, _, partition, _)| cluster.runtime.is_leader_for_shard(*partition))
                .collect();
            for peer in cluster.runtime.config().peer_nodes() {
                let last = sent.entry(peer.id).or_default();
                let changed: Vec<broker_api::DispatchCursorEntry> = led
                    .iter()
                    .filter(|(tenant, lane, partition, offset)| {
                        last.get(&(tenant.clone(), lane.clone(), *partition)) != Some(offset)
                    })
                    .map(
                        |(tenant, lane, partition, offset)| broker_api::DispatchCursorEntry {
                            tenant_id: tenant.clone(),
                            lane: lane.clone(),
                            partition: *partition,
                            next_offset: *offset,
                        },
                    )
                    .collect();
                if changed.is_empty() {
                    continue;
                }
                let url = format!("{}/internal/v1/dispatch/cursors", peer.internal_base());
                let body = broker_api::DispatchCursorCheckpoint {
                    leader_id: node_id,
                    cursors: changed.clone(),
                };
                let request =
                    broker_api::cluster_auth::apply_cluster_secret(http.post(&url)).json(&body);
                match request.send().await {
                    Ok(response) if response.status().is_success() => {
                        for entry in changed {
                            last.insert(
                                (entry.tenant_id, entry.lane, entry.partition),
                                entry.next_offset,
                            );
                        }
                    }
                    outcome => {
                        let now = Instant::now();
                        if warned
                            .get(&peer.id)
                            .is_none_or(|at| now.duration_since(*at) >= WARN_EVERY)
                        {
                            warned.insert(peer.id, now);
                            let error = match outcome {
                                Ok(response) => format!("HTTP {}", response.status()),
                                Err(error) => error.to_string(),
                            };
                            warn!(peer = %peer.addr, %error, "dispatch cursor checkpoint failed");
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{DispatchConfig, DispatchEngine, LeaseTable, TenantFairQueue};
    use broker_partition::{Broker, BrokerConfig, PublishRequest};
    use broker_raft_meta::{ClusterConfig, ClusterRuntime, NodeConfig};
    use broker_replication::ReplicateBatchAck;
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn peer(id: Uuid, generation: u64, healthy: bool) -> MockServer {
        let server = MockServer::start().await;
        let response = if healthy {
            ResponseTemplate::new(200).set_body_json(ReplicateBatchAck {
                node_id: id,
                partition: 0,
                leader_epoch: generation,
                durable_hwm: u64::MAX / 2,
            })
        } else {
            ResponseTemplate::new(503)
        };
        Mock::given(method("POST"))
            .and(path("/internal/v1/replicate/batch"))
            .respond_with(response)
            .mount(&server)
            .await;
        server
    }

    async fn cell(healthy: bool) -> (Cluster, Arc<AppState>, tempfile::TempDir, Vec<MockServer>) {
        let dir = tempfile::tempdir().unwrap();
        let local = Uuid::new_v4();
        let peers = [Uuid::new_v4(), Uuid::new_v4()];
        let probe = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![NodeConfig {
                id: local,
                addr: "http://local.invalid".into(),
                internal_url: None,
            }],
            node_id: local,
            generation: 1,
            hash_version: 1,
        };
        let generation = ClusterRuntime::open(tempfile::tempdir().unwrap().path(), probe)
            .unwrap()
            .shard_generation(0);
        let mut servers = Vec::new();
        for id in peers {
            servers.push(peer(id, generation, healthy).await);
        }
        let config = ClusterConfig {
            cluster_id: Uuid::new_v4(),
            nodes: vec![
                NodeConfig {
                    id: local,
                    addr: "http://local.invalid".into(),
                    internal_url: None,
                },
                NodeConfig {
                    id: peers[0],
                    addr: servers[0].uri(),
                    internal_url: None,
                },
                NodeConfig {
                    id: peers[1],
                    addr: servers[1].uri(),
                    internal_url: None,
                },
            ],
            node_id: local,
            generation: 1,
            hash_version: 1,
        };
        let runtime = ClusterRuntime::open(dir.path(), config).unwrap();
        assert_eq!(runtime.shard_generation(0), generation);
        let cluster = Cluster::new(runtime);
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker.enable_quorum_gating();
        let state = Arc::new(AppState {
            broker: broker.clone(),
            schedule: broker_schedule::ScheduleQueue::open(dir.path()).unwrap(),
            crons: broker_schedule::CronRegistry::open(dir.path()).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default()),
            leases: LeaseTable::new(),
            cluster: Some(cluster.clone()),
            local_auth: None,
            fair_queue: Arc::new(TenantFairQueue::new()),
            catalog_tombstones: broker_api::CatalogTombstones::open(dir.path()).unwrap(),
            dispatch_fleet: false,
            broker_only: true,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        });
        (cluster, state, dir, servers)
    }

    fn publish(broker: &Broker) -> (String, u32) {
        let response = broker
            .publish(PublishRequest {
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "verify".into(),
                payload: "body".into(),
                routing_id: None,
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: Some("https://example.com/hook".into()),
                secret: Some("secret".into()),
                destination: None,
                flow: None,
                parallelism: None,
                max_retries: None,
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        broker.flush_wal().unwrap();
        (response.topic, response.partition.unwrap())
    }

    #[tokio::test]
    async fn unreplicated_tail_is_verified_on_quorum_before_dispatch() {
        let (cluster, state, _dir, peers) = cell(true).await;
        let (topic, partition) = publish(&state.broker);
        publish(&state.broker);
        let committed = state.broker.committed_hwm(&topic, partition).unwrap();
        assert_eq!(
            state.broker.dispatch_hwm(&topic, partition).unwrap(),
            0,
            "locally durable but unreplicated records are not dispatchable"
        );
        let reached = verify_tail(&cluster, &state, &topic, partition, committed)
            .await
            .unwrap();
        assert_eq!(reached, committed);
        assert_eq!(
            state.broker.dispatch_hwm(&topic, partition).unwrap(),
            committed
        );
        assert!(!peers[0].received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn verification_fails_closed_without_a_quorum() {
        let (cluster, state, _dir, _peers) = cell(false).await;
        let (topic, partition) = publish(&state.broker);
        let committed = state.broker.committed_hwm(&topic, partition).unwrap();
        assert!(verify_tail(&cluster, &state, &topic, partition, committed)
            .await
            .is_err());
        assert_eq!(state.broker.dispatch_hwm(&topic, partition).unwrap(), 0);
    }

    async fn dead_letter_first_message(
        state: &Arc<AppState>,
    ) -> (String, u32, u64, Result<(), String>) {
        state
            .dispatch
            .set_dlq_durability(Arc::new(broker_api::ClusterDlqDurability::new(state)));
        let (topic, partition) = publish(&state.broker);
        let offset = state.broker.committed_hwm(&topic, partition).unwrap() - 1;
        // The source was delivered, so it is quorum-durable.
        state
            .broker
            .record_quorum_commit(&topic, partition, 0, offset + 1)
            .unwrap();
        let result = state
            .dispatch
            .dead_letter_offset(&topic, partition, offset, "retries exhausted")
            .await
            .map_err(|e| e.to_string());
        (topic, partition, offset, result)
    }

    #[tokio::test]
    async fn dlq_move_is_quorum_replicated_on_the_source_shard() {
        let (_cluster, state, _dir, peers) = cell(true).await;
        let (topic, partition, offset, result) = dead_letter_first_message(&state).await;
        result.unwrap();
        let dlq = broker_partition::dlq_topic(&topic);
        let records = state.broker.list_topic_messages(&dlq, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].partition, partition,
            "pinned to the source shard"
        );
        assert!(
            state.broker.quorum_hwm(&dlq, partition).unwrap() > records[0].offset,
            "the DLQ record is quorum-durable"
        );
        let lane = broker_partition::flow_lane_owner(
            &state
                .broker
                .read_message(&topic, partition, offset)
                .unwrap(),
        )
        .to_string();
        assert_eq!(
            state
                .broker
                .dispatch_offset(&state.broker.tenant(), &lane, partition)
                .unwrap(),
            offset + 1
        );
        let replicated = peers[0].received_requests().await.unwrap();
        assert!(!replicated.is_empty());
    }

    #[tokio::test]
    async fn dlq_move_without_quorum_leaves_the_source_pending() {
        let (_cluster, state, _dir, _peers) = cell(false).await;
        let (topic, partition, offset, result) = dead_letter_first_message(&state).await;
        assert!(result.is_err(), "no quorum, no DLQ completion");
        let lane = broker_partition::flow_lane_owner(
            &state
                .broker
                .read_message(&topic, partition, offset)
                .unwrap(),
        )
        .to_string();
        assert_eq!(
            state
                .broker
                .dispatch_offset(&state.broker.tenant(), &lane, partition)
                .unwrap(),
            0,
            "the source cursor does not advance"
        );
        assert_eq!(state.dispatch.telemetry_snapshot().dlq_pending_prepared, 1);
    }
}
