//! Automatic WAL retention loop.
//!
//! Each tick, for every local log this node leads, compute the safe
//! watermark (`broker_partition::RetentionStore::safe_watermark`). A single
//! node applies it directly. In a replicated cell the leader proposes it to
//! the controller (`SetGcWatermarks`, fenced to its shard leader term) and
//! every replica, leader included, deletes below `min(committed watermark,
//! local committed HWM)`. Without a controller quorum nothing new is
//! committed, so collection pauses (fail closed). Blob tombstones are swept
//! at the end of every tick.

use crate::AppState;
use broker_partition::{
    pins_by_log, record_log_retention, record_retention_error, LogId, RetentionConfig,
    SafeWatermark,
};
use broker_raft_meta::{ControllerCommand, GcWatermarkProposal};
use broker_storage::StorageMode;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// How this node may collect.
enum Mode {
    /// No replicas: compute and apply locally.
    Single,
    /// Replicated cell with an OpenRaft controller.
    Controlled,
    /// Replicated without a controller: no authority to agree on deletion.
    Paused,
}

fn mode(state: &AppState) -> Mode {
    match &state.cluster {
        Some(cluster) if cluster.runtime.config().node_count() > 1 => {
            if cluster.runtime.controller().is_some() {
                Mode::Controlled
            } else {
                Mode::Paused
            }
        }
        _ => Mode::Single,
    }
}

/// Lowest durable HWM any other replica (ISR or learner) acknowledged for
/// `log`. Unknown progress pins collection at the base.
fn replica_floor(state: &AppState, log: &LogId, base: u64, physical: bool) -> u64 {
    let Some(cluster) = &state.cluster else {
        return u64::MAX;
    };
    if cluster.runtime.config().node_count() <= 1 {
        return u64::MAX;
    }
    let Some(controller) = cluster.runtime.controller() else {
        return base;
    };
    let self_id = cluster.runtime.config().node_id;
    let Some(placement) = controller.state().shards.get(&log.partition).cloned() else {
        return base;
    };
    let mut peers: Vec<Uuid> = placement
        .replicas
        .iter()
        .chain(&placement.learners)
        .copied()
        .filter(|id| *id != self_id)
        .collect();
    peers.sort_unstable();
    peers.dedup();
    if peers.is_empty() {
        return u64::MAX;
    }
    let progress = cluster.replication.replica_progress();
    peers
        .into_iter()
        .map(|peer| {
            progress
                .iter()
                .filter(|entry| {
                    entry.peer_id == peer
                        && entry.partition == log.partition
                        // V2 epochs may be labelled with any topic of the shard.
                        && (physical || entry.topic == log.namespace)
                })
                .map(|entry| entry.durable_hwm)
                .max()
                .unwrap_or(base)
        })
        .min()
        .unwrap_or(base)
}

fn leads(state: &AppState, partition: u32) -> bool {
    match &state.cluster {
        Some(cluster) if cluster.runtime.config().node_count() > 1 => {
            cluster.runtime.is_leader_for_shard(partition) && cluster.runtime.shard_ready(partition)
        }
        _ => true,
    }
}

fn dispatch_pins(state: &AppState) -> HashMap<LogId, u64> {
    let pins = state.dispatch.retention_pins().into_iter().chain(
        state
            .leases
            .snapshot_for_status()
            .into_iter()
            .map(|job| (job.topic, job.partition, job.offset)),
    );
    pins_by_log(&state.broker, pins)
}

/// Safe watermark for one log as this node sees it (leader view). Used by
/// the retention loop and to validate manual GC requests.
pub fn compute_safe_watermark(
    state: &AppState,
    log: &LogId,
    config: &RetentionConfig,
) -> Result<SafeWatermark, broker_partition::BrokerError> {
    let pins = dispatch_pins(state);
    compute_with_pins(state, log, config, pins.get(log).copied())
}

fn compute_with_pins(
    state: &AppState,
    log: &LogId,
    config: &RetentionConfig,
    pin: Option<u64>,
) -> Result<SafeWatermark, broker_partition::BrokerError> {
    let base = state.broker.log_retention_status(log, 0)?.base_offset;
    let floor = replica_floor(state, log, base, state.broker.layout().is_physical());
    state
        .broker
        .retention()
        .safe_watermark(&state.broker, log, config, pin, floor)
}

/// Delete below `watermark` on this node and drop dispatch state that
/// referenced the collected range.
pub(crate) fn apply_watermark(
    state: &AppState,
    log: &LogId,
    watermark: u64,
) -> Result<broker_partition::CollectReport, broker_partition::BrokerError> {
    let broker = &state.broker;
    let report = broker.retention().collect(broker, log, watermark)?;
    if report.segments_deleted > 0 {
        info!(
            %log,
            watermark,
            base_offset = report.base_offset,
            segments = report.segments_deleted,
            blobs = report.blobs_tombstoned,
            "retention collected delivered WAL segments"
        );
        let dropped = state
            .dispatch
            .forget_collected(
                |topic, partition| broker.log_id(topic, partition) == *log,
                report.base_offset,
            )
            .map_err(|error| broker_partition::BrokerError::InvalidConfig(error.to_string()))?;
        if dropped > 0 {
            info!(%log, dropped, "dropped dispatch state for collected records");
        }
    }
    Ok(report)
}

fn compute_proposals(state: &AppState, config: &RetentionConfig) -> Vec<SafeWatermark> {
    let pins = dispatch_pins(state);
    let mut safes = Vec::new();
    for log in state.broker.retained_logs() {
        if !leads(state, log.partition) {
            continue;
        }
        match compute_with_pins(state, &log, config, pins.get(&log).copied()) {
            Ok(safe) => {
                debug!(?safe, "retention safe watermark");
                safes.push(safe);
            }
            Err(error) => {
                record_retention_error();
                warn!(%log, %error, "retention: safe watermark computation failed");
            }
        }
    }
    safes
}

/// Watermarks to apply on this node, per local log.
fn applied_targets(state: &AppState, safes: &[SafeWatermark]) -> Vec<(LogId, u64)> {
    match mode(state) {
        Mode::Single => safes
            .iter()
            .map(|safe| (safe.log.clone(), safe.safe))
            .collect(),
        Mode::Controlled => {
            let Some(controller) = state
                .cluster
                .as_ref()
                .and_then(|cluster| cluster.runtime.controller())
            else {
                return Vec::new();
            };
            let committed = controller.state();
            state
                .broker
                .retained_logs()
                .into_iter()
                .filter_map(|log| {
                    let watermark = committed.log_gc_watermark(&log.namespace, log.partition);
                    (watermark > 0).then_some((log, watermark))
                })
                .collect()
        }
        Mode::Paused => Vec::new(),
    }
}

async fn propose(state: &AppState, safes: &[SafeWatermark]) {
    let Some(cluster) = &state.cluster else {
        return;
    };
    let Some(controller) = cluster.runtime.controller() else {
        return;
    };
    let committed = controller.state();
    let entries: Vec<GcWatermarkProposal> = safes
        .iter()
        .filter(|safe| {
            safe.safe > committed.log_gc_watermark(&safe.log.namespace, safe.log.partition)
        })
        .map(|safe| GcWatermarkProposal {
            namespace: safe.log.namespace.clone(),
            partition: safe.log.partition,
            leader_term: cluster.runtime.shard_generation(safe.log.partition),
            watermark: safe.safe,
        })
        .collect();
    if entries.is_empty() {
        return;
    }
    let count = entries.len();
    if let Err(error) = controller
        .submit(ControllerCommand::SetGcWatermarks {
            leader_id: cluster.runtime.config().node_id,
            entries,
        })
        .await
    {
        // No quorum or no longer leader: nothing new is committed (fail closed).
        warn!(%error, count, "retention: controller rejected gc watermarks");
    }
}

fn apply_and_sweep(state: &AppState, config: &RetentionConfig, safes: &[SafeWatermark]) {
    let targets: HashMap<LogId, u64> = applied_targets(state, safes).into_iter().collect();
    for log in state.broker.retained_logs() {
        let target = targets.get(&log).copied();
        if let Some(watermark) = target {
            if let Err(error) = apply_watermark(state, &log, watermark) {
                record_retention_error();
                warn!(%log, watermark, %error, "retention: collection failed");
            }
        }
        match state.broker.log_retention_status(&log, 0) {
            Ok(status) => record_log_retention(
                &log,
                status.retained_bytes,
                status.base_offset,
                safes.iter().find(|safe| safe.log == log),
                target,
            ),
            Err(error) => warn!(%log, %error, "retention: status unavailable"),
        }
    }
    if let Err(error) = state.broker.retention().persist() {
        record_retention_error();
        warn!(%error, "retention: persisting frontiers failed");
    }
    match state
        .broker
        .retention()
        .sweep_blobs(state.broker.blob_store(), config.blob_grace)
    {
        Ok(report) if report.deleted > 0 || report.failed > 0 => {
            info!(?report, "retention swept payload blobs")
        }
        Ok(_) => {}
        Err(error) => {
            record_retention_error();
            warn!(%error, "retention: payload blob sweep failed");
        }
    }
}

/// One retention pass (exposed for tests and operators).
pub async fn run_retention_tick(state: Arc<AppState>, config: RetentionConfig) {
    if state.broker.config().storage != StorageMode::Local {
        return;
    }
    let blocking = Arc::clone(&state);
    let compute_config = config.clone();
    let safes =
        match tokio::task::spawn_blocking(move || compute_proposals(&blocking, &compute_config))
            .await
        {
            Ok(safes) => safes,
            Err(error) => {
                record_retention_error();
                warn!(%error, "retention: compute task failed");
                return;
            }
        };
    if matches!(mode(&state), Mode::Controlled) {
        propose(&state, &safes).await;
    }
    let blocking = Arc::clone(&state);
    if let Err(error) =
        tokio::task::spawn_blocking(move || apply_and_sweep(&blocking, &config, &safes)).await
    {
        record_retention_error();
        warn!(%error, "retention: apply task failed");
    }
}

/// Start the periodic retention loop (`None` when disabled or not local WAL).
pub fn spawn_retention_loop(state: Arc<AppState>) -> Option<tokio::task::JoinHandle<()>> {
    let config = RetentionConfig::from_env();
    if !config.enabled {
        warn!("BETTERMQ_RETENTION is off: delivered WAL segments and payload blobs are never reclaimed");
        return None;
    }
    if state.broker.config().storage != StorageMode::Local {
        return None;
    }
    if matches!(mode(&state), Mode::Paused) {
        warn!("retention paused: replicated cell without an OpenRaft controller cannot agree on GC watermarks");
    }
    info!(
        interval_secs = config.interval.as_secs(),
        min_age_secs = config.min_age.as_secs(),
        blob_grace_secs = config.blob_grace.as_secs(),
        "WAL retention enabled"
    );
    Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            run_retention_tick(Arc::clone(&state), config.clone()).await;
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_dispatch::{ClaimedJob, DispatchConfig, DispatchEngine, LeaseTable};
    use broker_partition::{Broker, BrokerConfig, PublishRequest, RetentionLimit};
    use broker_schedule::{CronRegistry, ScheduleQueue};
    use broker_storage::{FsyncMode, PartitionLogConfig};
    use std::time::Duration;

    fn state(dir: &std::path::Path) -> Arc<AppState> {
        let mut config = BrokerConfig::new(dir.to_path_buf());
        config.partitions = 1;
        config.log = PartitionLogConfig {
            segment_max_bytes: 2048,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(5),
        };
        let broker = Broker::open(config).unwrap();
        let leases = LeaseTable::new();
        Arc::new(AppState {
            broker: broker.clone(),
            schedule: ScheduleQueue::open(dir).unwrap(),
            crons: CronRegistry::open(dir).unwrap(),
            dispatch: DispatchEngine::new_broker_only(broker, DispatchConfig::default())
                .with_leases(leases.clone()),
            leases,
            cluster: None,
            local_auth: None,
            fair_queue: Arc::new(broker_dispatch::TenantFairQueue::new()),
            catalog_tombstones: crate::catalog_tombstones::CatalogTombstones::open(dir).unwrap(),
            dispatch_fleet: false,
            broker_only: false,
            #[cfg(feature = "cloud")]
            auth: None,
            #[cfg(feature = "cloud")]
            control_plane: None,
        })
    }

    fn publish(state: &AppState, count: usize) -> Vec<u64> {
        (0..count)
            .map(|index| {
                state
                    .broker
                    .publish(PublishRequest {
                        topic: String::new(),
                        queue_id: None,
                        group_id: None,
                        group_member_id: None,
                        routing_key: "lane".into(),
                        payload: format!("{index}-{}", "x".repeat(200)),
                        payload_encoding: None,
                        idempotency_key: None,
                        delay_ms: None,
                        priority: None,
                        flow_id: None,
                        url: Some("http://127.0.0.1:9/h".into()),
                        secret: Some("s".into()),
                        routing_id: None,
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
                    .unwrap()
                    .offset
                    .unwrap()
            })
            .collect()
    }

    fn deliver_below(state: &AppState, log: &LogId, below: u64) {
        for message in state.broker.read_log(log, 0, 10_000).unwrap() {
            if message.offset < below {
                state
                    .broker
                    .mark_dispatch_complete(
                        &message.tenant_id,
                        &broker_partition::flow_lane_owner(&message).to_string(),
                        message.partition,
                        message.offset,
                    )
                    .unwrap();
            }
        }
    }

    fn no_min_age() -> RetentionConfig {
        RetentionConfig {
            min_age: Duration::ZERO,
            ..RetentionConfig::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_tick_collects_only_the_delivered_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let offsets = publish(&state, 80);
        let log = state.broker.log_id(broker_partition::DIRECT_TOPIC, 0);

        // Nothing delivered: nothing collected.
        run_retention_tick(Arc::clone(&state), no_min_age()).await;
        assert_eq!(
            state
                .broker
                .log_retention_status(&log, 0)
                .unwrap()
                .base_offset,
            0
        );

        deliver_below(&state, &log, offsets[50]);
        run_retention_tick(Arc::clone(&state), no_min_age()).await;
        let base = state
            .broker
            .log_retention_status(&log, 0)
            .unwrap()
            .base_offset;
        assert!(base > 0 && base <= offsets[50], "base {base}");
        let retained = state.broker.read_log(&log, 0, 10_000).unwrap();
        assert_eq!(retained.first().unwrap().offset, base);
        let safe = compute_safe_watermark(&state, &log, &no_min_age()).unwrap();
        assert_eq!(safe.frontier, offsets[50]);
        assert_eq!(safe.limited_by, RetentionLimit::Undelivered);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lease_pins_collection_even_when_delivery_marks_exist() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let offsets = publish(&state, 80);
        let log = state.broker.log_id(broker_partition::DIRECT_TOPIC, 0);
        deliver_below(&state, &log, u64::MAX);
        state.leases.insert(
            "fleet".into(),
            ClaimedJob {
                lease_id: Uuid::new_v4(),
                topic: broker_partition::DIRECT_TOPIC.into(),
                partition: 0,
                offset: offsets[2],
                message_id: Uuid::new_v4(),
                expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                generation: 0,
                committed_hwm: 0,
                cursor_key: String::new(),
                message: None,
                attempts: 0,
            },
        );
        let safe = compute_safe_watermark(&state, &log, &no_min_age()).unwrap();
        assert_eq!(
            (safe.safe, safe.limited_by),
            (offsets[2], RetentionLimit::DispatchState)
        );
        run_retention_tick(Arc::clone(&state), no_min_age()).await;
        let base = state
            .broker
            .log_retention_status(&log, 0)
            .unwrap()
            .base_offset;
        assert!(base <= offsets[2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn min_age_keeps_fresh_delivered_records() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        publish(&state, 80);
        let log = state.broker.log_id(broker_partition::DIRECT_TOPIC, 0);
        deliver_below(&state, &log, u64::MAX);
        run_retention_tick(Arc::clone(&state), RetentionConfig::default()).await;
        assert_eq!(
            state
                .broker
                .log_retention_status(&log, 0)
                .unwrap()
                .base_offset,
            0
        );
    }
}
