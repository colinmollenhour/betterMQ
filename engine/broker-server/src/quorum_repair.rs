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
    generation: u64,
) -> Result<u64, String> {
    let broker = &state.broker;
    let incarnation = cluster.runtime.shard_incarnation(shard);
    let local = broker
        .log_metadata_validator(shard)
        .map_err(|error| error.to_string())?;
    let validate = || {
        local()?;
        if !cluster.runtime.is_recovery_leader_for_shard(shard)
            || cluster.runtime.shard_generation(shard) != generation
            || cluster.runtime.shard_incarnation(shard) != incarnation
        {
            return Err(broker_storage::IndexError::Validation(
                "quorum verification ownership changed".into(),
            ));
        }
        Ok(())
    };
    let mut quorum = broker
        .quorum_hwm(topic, shard)
        .map_err(|e| format!("read quorum watermark: {e}"))?;
    let epoch_topic = if broker.layout().is_physical() {
        String::new()
    } else {
        topic.to_string()
    };
    for _ in 0..VERIFY_EPOCHS_PER_TICK {
        if !cluster.runtime.is_recovery_leader_for_shard(shard)
            || cluster.runtime.shard_generation(shard) != generation
        {
            return Err("shard recovery ownership changed".into());
        }
        if quorum >= committed {
            break;
        }
        if broker
            .committed_hwm(topic, shard)
            .map_err(|error| error.to_string())?
            < committed
        {
            return Err("local committed prefix changed during verification".into());
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
                    .record_quorum_commit_checked(topic, shard, quorum, committed, validate)
                    .map_err(|e| e.to_string())?;
                continue;
            }
            Some(first) if first > quorum => {
                quorum = broker
                    .record_quorum_commit_checked(
                        topic,
                        shard,
                        quorum,
                        first.min(committed),
                        validate,
                    )
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
            generation,
            quorum,
            run,
        )
        .map_err(|e| format!("build verification epoch: {e}"))?;
        cluster
            .replication
            .replicate_batch(request, committed)
            .await
            .map_err(|e| format!("verify [{quorum}, {end}) on minISR: {e}"))?;
        if !cluster.runtime.is_recovery_leader_for_shard(shard)
            || cluster.runtime.shard_generation(shard) != generation
        {
            return Err("shard recovery ownership changed during replication".into());
        }
        quorum = broker
            .record_quorum_commit_checked(topic, shard, quorum, end, validate)
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
            let recovering: HashMap<u32, _> = state
                .dispatch
                .recovery_targets()
                .into_iter()
                .map(|target| (target.partition, (target.epoch, target.logs)))
                .collect();
            for shard in 0..state.broker.layout().shard_count {
                if !cluster.runtime.is_recovery_leader_for_shard(shard) {
                    continue;
                }
                let generation = cluster.runtime.shard_generation(shard);
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
                        verify_tail(&cluster, &state, &topic, shard, committed, generation).await
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
                if let Some((epoch, targets)) = recovering.get(&shard) {
                    let verified = targets.iter().all(|(topic, target)| {
                        state
                            .broker
                            .quorum_hwm(topic, shard)
                            .is_ok_and(|quorum| quorum >= *target)
                    });
                    if !verified {
                        continue;
                    }
                    if !cluster.runtime.is_recovery_leader_for_shard(shard)
                        || cluster.runtime.shard_generation(shard) != generation
                    {
                        continue;
                    }
                    let mut recovery_error = None;
                    for (topic, target) in targets {
                        let mut next = 0;
                        while next < *target {
                            if !cluster.runtime.is_recovery_leader_for_shard(shard)
                                || cluster.runtime.shard_generation(shard) != generation
                            {
                                recovery_error = Some(
                                    "shard ownership changed during dedup recovery".to_owned(),
                                );
                                break;
                            }
                            let broker = state.broker.clone();
                            let topic = topic.clone();
                            let target = *target;
                            let runtime = cluster.runtime.clone();
                            let epoch = *epoch;
                            let local = broker.log_metadata_validator(shard);
                            let Ok(local) = local else {
                                recovery_error = Some("log rewrite in progress".into());
                                break;
                            };
                            let validator: broker_storage::MetadataValidator =
                                Arc::new(move || {
                                    local()?;
                                    if !runtime.is_recovery_leader_for_shard(shard)
                                        || runtime.shard_generation(shard) != generation
                                        || runtime.shard_incarnation(shard) != epoch
                                    {
                                        return Err(broker_storage::IndexError::Validation(
                                            "recovery incarnation changed".into(),
                                        ));
                                    }
                                    Ok(())
                                });
                            match broker_partition::run_blocking_io(move || {
                                broker.check_gc_identity_base(&broker.log_id(&topic, shard))?;
                                if next == 0 {
                                    broker.seed_legacy_replica_identities(
                                        &broker.log_id(&topic, shard),
                                        target,
                                        validator.clone(),
                                    )?;
                                }
                                broker.recover_dedup_page(
                                    &topic,
                                    shard,
                                    next,
                                    target,
                                    Some(validator),
                                )
                            })
                            .await
                            {
                                Ok(Ok(reached)) => next = reached,
                                result => {
                                    recovery_error =
                                        Some(format!("dedup recovery failed: {result:?}"));
                                    break;
                                }
                            }
                        }
                        if recovery_error.is_some() {
                            break;
                        }
                    }
                    if let Some(error) = recovery_error {
                        warn!(shard, %error, "takeover remains fenced");
                        continue;
                    }
                    if !cluster.runtime.is_recovery_leader_for_shard(shard)
                        || cluster.runtime.shard_generation(shard) != generation
                    {
                        continue;
                    }
                    match state.dispatch.complete_recovery(shard, *epoch) {
                        Ok(Some(clamped)) => {
                            if !cluster.runtime.is_recovery_leader_for_shard(shard)
                                || cluster.runtime.shard_generation(shard) != generation
                            {
                                state.dispatch.fence_partition(shard);
                                continue;
                            }
                            if !cluster
                                .runtime
                                .finish_shard_recovery(shard, *epoch, generation)
                            {
                                continue;
                            }
                            info!(
                                shard,
                                clamped, "takeover verified on minISR; dispatch resumed"
                            );
                            let dispatch = state.dispatch.clone();
                            let _ =
                                tokio::task::spawn_blocking(move || dispatch.backfill_pending())
                                    .await;
                        }
                        Ok(None) => {}
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
        for peer in peers {
            runtime.record_peer_alive(peer, chrono::Utc::now().timestamp_millis());
        }
        runtime
            .observe_controller_leader(&broker_raft_meta::ControllerLeaderProof {
                term: 1,
                leader_id: local,
                cluster_generation: 1,
            })
            .unwrap();
        assert_eq!(runtime.shard_generation(0), generation);
        let cluster = Cluster::new(runtime);
        let mut config = BrokerConfig::new(dir.path().to_path_buf());
        config.partitions = 1;
        let broker = Broker::open(config).unwrap();
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

    async fn assert_replicated_to_a_peer(peers: &[MockServer]) {
        // minISR includes the local leader and either healthy follower. The
        // slower request may be cancelled once quorum is reached, so no fixed
        // follower is guaranteed to have received it.
        let mut received = 0;
        for peer in peers {
            received += peer.received_requests().await.unwrap().len();
        }
        assert!(received > 0, "quorum verification must contact a follower");
    }

    #[tokio::test]
    async fn takeover_repairs_while_ingest_fenced_and_only_opens_after_prefix_verification() {
        let (cluster, mut state, dir, _peers) = cell(true).await;
        let mut config = cluster.runtime.config().clone();
        let generation = cluster.runtime.shard_generation(0);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut servers = Vec::new();
        for node in config.nodes.iter_mut().skip(1) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            node.addr = format!("http://{}", listener.local_addr().unwrap());
            let node_id = node.id;
            let gate = gate.clone();
            let entered = entered.clone();
            let app = axum::Router::new().route(
                "/internal/v1/replicate/batch",
                axum::routing::post(move || {
                    let gate = gate.clone();
                    let entered = entered.clone();
                    async move {
                        entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();
                        axum::Json(ReplicateBatchAck {
                            node_id,
                            partition: 0,
                            leader_epoch: generation,
                            durable_hwm: u64::MAX / 2,
                        })
                    }
                }),
            );
            servers.push(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
        }
        let runtime = ClusterRuntime::open(dir.path(), config).unwrap();
        for node in runtime.config().nodes.iter().skip(1) {
            runtime.record_peer_alive(node.id, chrono::Utc::now().timestamp_millis());
        }
        runtime
            .observe_controller_leader(&broker_raft_meta::ControllerLeaderProof {
                term: 1,
                leader_id: runtime.config().node_id,
                cluster_generation: 1,
            })
            .unwrap();
        let cluster = Cluster::new(runtime);
        Arc::get_mut(&mut state).unwrap().cluster = Some(cluster.clone());
        let (topic, partition) = publish(&state.broker);
        let target = state.broker.committed_hwm(&topic, partition).unwrap();
        let epoch = state.dispatch.fence_partition(partition);
        cluster.runtime.begin_shard_recovery(
            partition,
            epoch,
            cluster.runtime.shard_generation(partition),
        );
        state
            .dispatch
            .set_recovery_targets(partition, epoch, vec![(topic.clone(), target)]);
        assert!(cluster.runtime.mark_shard_verifying(
            partition,
            epoch,
            cluster.runtime.shard_generation(partition)
        ));
        assert!(!cluster.runtime.is_leader_for_shard(partition));
        assert!(cluster.runtime.is_recovery_leader_for_shard(partition));
        let repair = spawn_quorum_repair(cluster.clone(), state.clone());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if entered.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !cluster.runtime.shard_ready(partition),
            "unacknowledged verification cannot open ingest"
        );
        assert!(state.dispatch.is_fenced(partition));
        gate.add_permits(2);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !cluster.runtime.shard_ready(partition) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(state.broker.quorum_hwm(&topic, partition).unwrap() >= target);
        assert!(!state.dispatch.is_fenced(partition));
        repair.abort();
        let _ = repair.await;
        for server in servers {
            server.abort();
            let _ = server.await;
        }
        state.dispatch.shutdown_background().await;
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
        let reached = verify_tail(
            &cluster,
            &state,
            &topic,
            partition,
            committed,
            cluster.runtime.shard_generation(partition),
        )
        .await
        .unwrap();
        assert_eq!(reached, committed);
        assert_eq!(
            state.broker.dispatch_hwm(&topic, partition).unwrap(),
            committed
        );
        assert_replicated_to_a_peer(&peers).await;
    }

    #[tokio::test]
    async fn verification_fails_closed_without_a_quorum() {
        let (cluster, state, _dir, _peers) = cell(false).await;
        let (topic, partition) = publish(&state.broker);
        let committed = state.broker.committed_hwm(&topic, partition).unwrap();
        assert!(verify_tail(
            &cluster,
            &state,
            &topic,
            partition,
            committed,
            cluster.runtime.shard_generation(partition)
        )
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
        assert_replicated_to_a_peer(&peers).await;
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
