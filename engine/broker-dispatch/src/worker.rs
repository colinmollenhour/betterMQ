//! Webhook push workers: HMAC signing, retries, DLQ on exhaustion.
//! Each message carries the destination URL frozen at enqueue time.

use crate::flow_control::FlowController;
use crate::host_blocker::{HostBlocker, HostBlockerConfig};
use crate::lifecycle::{DlqPhase, DlqTransition, LifecycleStore};
use crate::memory_guard::{MemoryGuard, MemoryGuardConfig};
use crate::outbound::{apply_to_reqwest, build_outbound};
use crate::retry_state::{RetryKey, RetryRecord, RetryState};
use broker_partition::{dlq_topic, group_member_dlq_topic};
use broker_partition::{Broker, PublishRequest, DIRECT_TOPIC};
use broker_proto::RetryDefaults;
use broker_storage::StoredMessage;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Cap concurrent in-flight delivery tasks (HA M1). Override with BETTERMQ_DISPATCH_MAX_IN_FLIGHT.
fn dispatch_max_in_flight() -> usize {
    std::env::var("BETTERMQ_DISPATCH_MAX_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
        .max(1)
}

fn dispatch_queue_cap() -> usize {
    std::env::var("BETTERMQ_DISPATCH_QUEUE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096)
        .max(1)
}

const WEBHOOK_RESPONSE_BODY_CAP: usize = 64 * 1024;

#[derive(Default)]
struct PressureLogs {
    last_log_ms: AtomicU64,
    events: [AtomicU64; 4],
}

impl PressureLogs {
    fn record(&self, event: usize, now_ms: u64) -> Option<[u64; 4]> {
        self.events[event].fetch_add(1, Ordering::Relaxed);
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 30_000
            || self
                .last_log_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return None;
        }
        Some(std::array::from_fn(|index| {
            self.events[index].swap(0, Ordering::Relaxed)
        }))
    }
}

fn redacted_destination(url: &str) -> String {
    HostBlocker::host_key(url).unwrap_or_else(|| "<invalid destination>".into())
}

#[derive(Debug, Clone)]
pub struct DispatchConfig {
    pub retry_defaults: RetryDefaults,
    /// Default outbound webhook timeout (CP6).
    pub http_timeout_secs: u64,
    /// Plan-tier long wait pool (up to 12h on Scale).
    pub long_http_timeout_secs: u64,
    /// Payload size above which the long client is used.
    pub long_payload_threshold_bytes: usize,
    pub host_blocker: HostBlockerConfig,
    pub memory_guard: MemoryGuardConfig,
    /// HTTP status codes that should not be retried (CP6a).
    pub non_retry_status_codes: Vec<u16>,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        let http_timeout_secs = std::env::var("BETTERMQ_HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let long_http_timeout_secs = std::env::var("BETTERMQ_LONG_HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7200);
        let max_retries = std::env::var("BETTERMQ_DEFAULT_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let retry_defaults = RetryDefaults {
            max_retries,
            ..RetryDefaults::default()
        };
        Self {
            retry_defaults,
            http_timeout_secs,
            long_http_timeout_secs,
            long_payload_threshold_bytes: 256 * 1024,
            host_blocker: HostBlockerConfig::default(),
            memory_guard: MemoryGuardConfig::default(),
            non_retry_status_codes: vec![400, 401, 403, 404, 422],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPriority {
    High,
    Low,
}

#[derive(Debug, Clone)]
pub struct DeliveryJob {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub priority: DeliveryPriority,
}

impl DeliveryJob {
    pub fn live(topic: impl Into<String>, partition: u32, offset: u64, message_id: Uuid) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            message_id,
            priority: DeliveryPriority::High,
        }
    }
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("broker error: {0}")]
    Broker(#[from] broker_partition::BrokerError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("message has no push destination")]
    NoDestination,
    #[error("egress: {0}")]
    Egress(String),
    #[error("host blocked")]
    HostBlocked,
    #[error("non-retryable delivery failure: {0}")]
    NonRetryable(String),
    #[error("message offset is above committed high watermark")]
    Uncommitted,
    #[error("delivery retries exhausted: {0}")]
    RetryExhausted(String),
    #[error("delivery retry deferred for {retry_after_ms} ms: {reason}")]
    RetryDeferred { retry_after_ms: u64, reason: String },
    #[error("message body is stored externally and was not loaded")]
    PayloadUnresolved,
    #[error("{0}")]
    Failed(String),
}

type ShardLeaderFn = Arc<dyn Fn(u32) -> bool + Send + Sync>;
type InFlightSet = Arc<parking_lot::Mutex<HashSet<(String, u32, u64)>>>;

#[derive(Debug)]
struct DeferredJob {
    due: tokio::time::Instant,
    sequence: u64,
    job: DeliveryJob,
}

impl PartialEq for DeferredJob {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.sequence == other.sequence
    }
}

impl Eq for DeferredJob {}

impl PartialOrd for DeferredJob {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeferredJob {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .due
            .cmp(&self.due)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

struct LeaseDrop {
    leases: Option<crate::lease::LeaseTable>,
    lease_id: Option<Uuid>,
    holder: String,
}

pub(crate) struct DeliveryOwnership {
    key: (String, u32, u64),
    jobs: InFlightSet,
    notify: Arc<Notify>,
    lease: Option<LeaseDrop>,
}

impl Drop for DeliveryOwnership {
    fn drop(&mut self) {
        self.jobs.lock().remove(&self.key);
        self.notify.notify_waiters();
    }
}

struct ActiveHttpGuard {
    active: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl Drop for ActiveHttpGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

impl Drop for LeaseDrop {
    fn drop(&mut self) {
        if let (Some(leases), Some(id)) = (&self.leases, self.lease_id) {
            let _ = leases.take(id, &self.holder);
        }
    }
}

#[derive(Clone)]
pub struct DispatchEngine {
    broker: Broker,
    config: DispatchConfig,
    client: reqwest::Client,
    long_client: reqwest::Client,
    high_tx: mpsc::Sender<DeliveryJob>,
    defer_tx: mpsc::Sender<DeferredJob>,
    pub flow: FlowController,
    in_flight_jobs: InFlightSet,
    is_shard_leader: Option<ShardLeaderFn>,
    host_blocker: Arc<HostBlocker>,
    memory_guard: Arc<MemoryGuard>,
    in_flight: Arc<Semaphore>,
    /// Shared lease table — same CAS path as fleet claim API.
    leases: Option<crate::lease::LeaseTable>,
    local_holder: String,
    workers_enabled: bool,
    stop: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
    active_http: Arc<AtomicU64>,
    drain_notify: Arc<Notify>,
    defer_sequence: Arc<AtomicU64>,
    retry_state: RetryState,
    lifecycle: LifecycleStore,
    telemetry: Arc<crate::telemetry::DispatchTelemetry>,
    pressure_logs: Arc<PressureLogs>,
    background: Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>,
}

impl DispatchEngine {
    pub fn new(broker: Broker, config: DispatchConfig) -> Self {
        Self::new_with_mode(broker, config, true)
    }

    /// Broker-only: no local delivery workers (fleet claims via lease API).
    pub fn new_broker_only(broker: Broker, config: DispatchConfig) -> Self {
        Self::new_with_mode(broker, config, false)
    }

    fn new_with_mode(broker: Broker, config: DispatchConfig, workers_enabled: bool) -> Self {
        let (high_tx, high_rx) = mpsc::channel(dispatch_queue_cap());
        let (defer_tx, defer_rx) = mpsc::channel(dispatch_queue_cap());
        let config_clone = config.clone();
        let memory_guard = Arc::new(MemoryGuard::new(config_clone.memory_guard.clone()));
        // Apply fleet long-wait tier when set (Phase E).
        let long_secs =
            crate::lease::long_wait_tier_secs().unwrap_or(config_clone.long_http_timeout_secs);
        let global_max = std::env::var("BETTERMQ_DISPATCH_GLOBAL_MAX")
            .ok()
            .and_then(|s| s.parse().ok());
        let retry_state =
            RetryState::open(&broker.config().data_dir).expect("open durable dispatch retry state");
        let lifecycle = LifecycleStore::open(&broker.config().data_dir)
            .expect("open durable dispatch lifecycle state");
        let engine = Self {
            broker,
            config,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(config_clone.http_timeout_secs))
                .redirect(reqwest::redirect::Policy::none())
                .dns_resolver(Arc::new(crate::egress::PolicyResolver))
                .no_proxy()
                .build()
                .expect("reqwest short client"),
            long_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(long_secs))
                .redirect(reqwest::redirect::Policy::none())
                .dns_resolver(Arc::new(crate::egress::PolicyResolver))
                .no_proxy()
                .build()
                .expect("reqwest long client"),
            high_tx,
            defer_tx,
            flow: FlowController::new(global_max, memory_guard.clone()),
            in_flight_jobs: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            is_shard_leader: None,
            host_blocker: Arc::new(HostBlocker::new(config_clone.host_blocker.clone())),
            memory_guard,
            in_flight: Arc::new(Semaphore::new(dispatch_max_in_flight())),
            leases: None,
            local_holder: format!("local-{}", Uuid::new_v4()),
            workers_enabled,
            stop: Arc::new(AtomicBool::new(false)),
            draining: Arc::new(AtomicBool::new(false)),
            stop_notify: Arc::new(Notify::new()),
            active_http: Arc::new(AtomicU64::new(0)),
            drain_notify: Arc::new(Notify::new()),
            defer_sequence: Arc::new(AtomicU64::new(0)),
            retry_state,
            lifecycle,
            telemetry: Arc::new(crate::telemetry::DispatchTelemetry::new()),
            pressure_logs: Arc::new(PressureLogs::default()),
            background: Arc::new(parking_lot::Mutex::new(Vec::new())),
        };
        if let Some(handle) = engine.memory_guard.spawn_monitor() {
            engine.background.lock().push(handle);
        }
        if workers_enabled {
            engine.spawn_workers(high_rx);
            engine.spawn_defer_scheduler(defer_rx);
            engine.replay_lifecycle_state();
            engine.replay_retry_state();
        } else {
            // Fleet brokers still need a durable backlog baseline after restart.
            engine.backfill_pending();
        }
        engine
    }

    pub fn with_leases(mut self, leases: crate::lease::LeaseTable) -> Self {
        self.leases = Some(leases);
        self
    }

    pub fn workers_enabled(&self) -> bool {
        self.workers_enabled
    }

    pub fn process_id(&self) -> &str {
        &self.local_holder
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub fn stop(&self) {
        self.draining.store(true, Ordering::Release);
        self.telemetry.record_stopped();
        self.stop.store(true, Ordering::Relaxed);
        self.stop_notify.notify_waiters();
    }

    pub async fn drain(&self, timeout: Duration) -> bool {
        self.draining.store(true, Ordering::Release);
        self.telemetry.record_drain_started();
        let started = Instant::now();
        let wait = async {
            loop {
                let queued = !self.in_flight_jobs.lock().is_empty();
                let active_http = self.active_http.load(Ordering::Acquire);
                let flow_pending = self.flow.pending_count().await;
                if !queued && active_http == 0 && flow_pending == 0 {
                    break;
                }
                tokio::select! {
                    _ = self.drain_notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            }
        };
        let drained = tokio::time::timeout(timeout, wait).await.is_ok();
        self.telemetry.record_drain_finished(
            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            drained,
        );
        self.stop();
        self.shutdown_background().await;
        drained
    }

    /// Abort timer-backed worker tasks and wait for them to drop while the runtime
    /// is still alive. Prevents Tokio's "context was found, but it is being shutdown".
    pub async fn shutdown_background(&self) {
        self.stop();
        self.memory_guard.stop();
        let mut handles: Vec<_> = std::mem::take(&mut *self.background.lock());
        handles.extend(self.flow.take_drainer_tasks());
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
        self.flow.discard_pending().await;
    }

    fn active_http_guard(&self) -> ActiveHttpGuard {
        self.active_http.fetch_add(1, Ordering::AcqRel);
        ActiveHttpGuard {
            active: self.active_http.clone(),
            notify: self.drain_notify.clone(),
        }
    }

    /// Deliver a hydrated message (used by fleet workers).
    pub async fn deliver_stored_message(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        self.deliver_message(msg).await
    }

    pub fn host_blocker(&self) -> Arc<HostBlocker> {
        self.host_blocker.clone()
    }

    pub fn memory_guard(&self) -> Arc<MemoryGuard> {
        self.memory_guard.clone()
    }

    pub(crate) fn record_flow_rejected(&self) {
        self.telemetry.record_queue_rejected();
    }

    /// WAL records that log retention must keep: pending retries, DLQ
    /// transitions not yet finalized, and fleet leases, as
    /// `(topic, partition, offset)`.
    pub fn retention_pins(&self) -> Vec<(String, u32, u64)> {
        let mut pins: Vec<_> = self
            .retry_state
            .pending()
            .into_iter()
            .map(|record| record.key)
            .chain(
                self.lifecycle
                    .pending()
                    .into_iter()
                    .map(|transition| transition.source),
            )
            .map(|key| (key.topic, key.partition, key.offset))
            .collect();
        if let Some(leases) = &self.leases {
            pins.extend(
                leases
                    .snapshot_for_status()
                    .into_iter()
                    .map(|job| (job.topic, job.partition, job.offset)),
            );
        }
        pins
    }

    /// Drop retry and DLQ-transition records for offsets that retention has
    /// already collected (`offset < base` in the log `in_log` selects). The
    /// committed GC watermark proves those records were fully handled, and a
    /// stale entry would otherwise block its lane and pin retention forever.
    pub fn forget_collected(
        &self,
        in_log: impl Fn(&str, u32) -> bool,
        base: u64,
    ) -> Result<usize, DispatchError> {
        let mut dropped = 0usize;
        for record in self.retry_state.pending() {
            if record.key.offset < base && in_log(&record.key.topic, record.key.partition) {
                self.retry_state
                    .clear(&record.key)
                    .map_err(|error| DispatchError::Failed(error.to_string()))?;
                dropped += 1;
            }
        }
        for transition in self.lifecycle.pending() {
            let source = &transition.source;
            if source.offset < base && in_log(&source.topic, source.partition) {
                self.lifecycle
                    .clear(source)
                    .map_err(|error| DispatchError::Failed(error.to_string()))?;
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    pub fn telemetry_snapshot(&self) -> crate::telemetry::DispatchTelemetrySnapshot {
        let counters = self.telemetry.snapshot();
        let now = chrono::Utc::now().timestamp_millis();
        let retries = self.retry_state.pending();
        let retry_due = retries
            .iter()
            .filter(|record| record.due_at_ms <= now)
            .count() as u64;
        let transitions = self.lifecycle.pending();
        let dlq_pending_prepared = transitions
            .iter()
            .filter(|transition| transition.phase == DlqPhase::Prepared)
            .count() as u64;
        let dlq_pending_committed = transitions
            .iter()
            .filter(|transition| transition.phase == DlqPhase::DlqCommitted)
            .count() as u64;
        crate::telemetry::DispatchTelemetrySnapshot {
            delivery: counters.delivery,
            committed_ready_ranges: counters.committed_ready_ranges,
            committed_ready_records: counters.committed_ready_records,
            queue_depth: counters.queue_depth,
            queue_rejected: counters.queue_rejected,
            deferred_jobs: counters.deferred_jobs,
            defer_rejected: counters.defer_rejected,
            in_flight_deliveries: self.in_flight_jobs.lock().len() as u64,
            active_http_deliveries: self.active_http.load(Ordering::Acquire),
            retries_pending: retries.len() as u64,
            retries_pending_due: retry_due,
            retries_scheduled: counters.retries_scheduled,
            retries_due: counters.retries_due,
            retries_exhausted: counters.retries_exhausted,
            dlq_pending_prepared,
            dlq_pending_committed,
            dlq_prepared: counters.dlq_prepared,
            dlq_committed: counters.dlq_committed,
            dlq_failures: counters.dlq_failures,
            drain_started: counters.drain_started,
            drain_completed: counters.drain_completed,
            drain_timeouts: counters.drain_timeouts,
            last_drain_duration_ms: counters.last_drain_duration_ms,
            draining: counters.draining,
            shards: counters.shards,
            dropped_cursor_lanes: counters.dropped_cursor_lanes,
            host_pressure: self.host_blocker.telemetry_snapshot(),
        }
    }

    /// Only dispatch/backfill partitions where this node is shard leader (CP7a).
    pub fn with_shard_leader_check(mut self, check: ShardLeaderFn) -> Self {
        self.is_shard_leader = Some(check);
        self
    }

    fn shard_leader(&self, partition: u32) -> bool {
        self.is_shard_leader
            .as_ref()
            .map(|f| f(partition))
            .unwrap_or(true)
    }

    pub fn enqueue(&self, job: DeliveryJob) {
        if !self.workers_enabled
            || self.stop.load(Ordering::Relaxed)
            || self.draining.load(Ordering::Acquire)
        {
            return;
        }
        if !self.shard_leader(job.partition) {
            return;
        }
        let key = (job.topic.clone(), job.partition, job.offset);
        {
            let mut inflight = self.in_flight_jobs.lock();
            if !inflight.insert(key.clone()) {
                return;
            }
        }
        match self.high_tx.try_send(job) {
            Ok(()) => self.telemetry.record_queue_enqueue(),
            Err(mpsc::error::TrySendError::Full(job)) => {
                self.telemetry.record_queue_rejected();
                self.in_flight_jobs
                    .lock()
                    .remove(&(job.topic, job.partition, job.offset));
                self.record_pressure(0);
            }
            Err(mpsc::error::TrySendError::Closed(job)) => {
                self.telemetry.record_queue_rejected();
                self.in_flight_jobs
                    .lock()
                    .remove(&(job.topic, job.partition, job.offset));
            }
        }
    }

    /// Wake dispatch for a committed offset range `[from, to)` (exclusive end).
    pub fn notify_committed_range(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        to_offset: u64,
        message_id: Uuid,
    ) {
        if to_offset <= from_offset {
            return;
        }
        self.telemetry
            .record_committed_range(to_offset.saturating_sub(from_offset));
        for offset in from_offset..to_offset {
            if let Ok(msg) = self.broker.read_message(topic, partition, offset) {
                self.observe_pending(&msg, to_offset);
            }
            self.enqueue(DeliveryJob::live(
                topic.to_string(),
                partition,
                offset,
                message_id,
            ));
        }
    }

    /// Re-enqueue undelivered messages after restart (CP2.5 / CP7a).
    /// Pages per partition from cursor → high-water mark (no global 50k cliff).
    pub fn backfill_pending(&self) {
        if self.memory_guard.is_critical() {
            info!("dispatch backfill skipped: memory critical");
            return;
        }
        let tenant_id = self.broker.config().tenant_id.clone();
        let mut topics: HashSet<String> = HashSet::new();
        topics.insert(DIRECT_TOPIC.to_string());
        if let Ok(queues) = self.broker.list_endpoints() {
            for q in queues {
                topics.insert(q.topic);
            }
        }

        let mut enqueued = 0u64;
        const PAGE: usize = 500;
        for topic in topics {
            if broker_partition::is_dlq_topic(&topic) {
                continue;
            }
            let Ok(partition_count) = self.broker.partition_count(&topic) else {
                continue;
            };
            for partition in 0..partition_count {
                if !self.shard_leader(partition) {
                    continue;
                }
                // Walk messages in pages; broker list API may still cap — use offset cursor.
                let mut from_offset = 0u64;
                while let Ok(messages) =
                    self.broker
                        .list_topic_messages_from(&topic, partition, from_offset, PAGE)
                {
                    if messages.is_empty() {
                        break;
                    }
                    let mut advanced = false;
                    for msg in &messages {
                        from_offset = from_offset.max(msg.offset.saturating_add(1));
                        advanced = true;
                        if msg
                            .destination_url
                            .as_ref()
                            .map(|u| u.is_empty())
                            .unwrap_or(true)
                        {
                            continue;
                        }
                        let lane_owner = broker_partition::flow_lane_owner(msg);
                        let cursor = self
                            .broker
                            .dispatch_offset(&tenant_id, &lane_owner.to_string(), msg.partition)
                            .unwrap_or(0);
                        if msg.offset < cursor {
                            continue;
                        }
                        if self
                            .broker
                            .is_dispatch_complete(
                                &tenant_id,
                                &lane_owner.to_string(),
                                msg.partition,
                                msg.offset,
                            )
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        let hwm = self.broker.committed_hwm(&topic, partition).unwrap_or(0);
                        if msg.offset >= hwm {
                            continue;
                        }
                        self.observe_pending(msg, hwm);
                        self.enqueue(DeliveryJob::live(
                            msg.topic.clone(),
                            msg.partition,
                            msg.offset,
                            msg.id,
                        ));
                        enqueued += 1;
                    }
                    if !advanced || messages.len() < PAGE {
                        break;
                    }
                }
            }
        }
        if enqueued > 0 {
            info!(enqueued, "dispatch backfill enqueued pending messages");
        }
    }

    fn observe_pending(&self, msg: &StoredMessage, hwm: u64) {
        if msg
            .destination_url
            .as_ref()
            .is_none_or(|url| url.is_empty())
        {
            return;
        }
        let lane = broker_partition::flow_lane_owner(msg).to_string();
        let tenant = if msg.tenant_id.is_empty() {
            self.broker.tenant()
        } else {
            msg.tenant_id.clone()
        };
        let cursor = self
            .broker
            .dispatch_offset(&tenant, &lane, msg.partition)
            .unwrap_or(0);
        self.telemetry
            .record_pending(msg.partition, &lane, cursor, hwm);
        self.telemetry
            .record_pending_age(msg.partition, &lane, msg.offset, msg.published_at_ms);
    }

    fn record_pressure(&self, event: usize) {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        if let Some([queue_rejected, deferred, defer_rejected, delivery_failures]) =
            self.pressure_logs.record(event, now)
        {
            warn!(
                queue_rejected,
                deferred,
                defer_rejected,
                delivery_failures,
                "dispatch pressure summary (at most once per 30 seconds; totals are in metrics)"
            );
        }
    }
}

impl DispatchEngine {
    fn spawn_workers(&self, mut high_rx: mpsc::Receiver<DeliveryJob>) {
        let this = self.clone();
        // Bound hydration/admission separately from outbound HTTP. Flow handoff
        // releases this permit, but not the HTTP permit taken by delivery itself.
        let admission = Arc::new(Semaphore::new(dispatch_max_in_flight()));
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = this.stop_notify.notified() => break,
                    job = high_rx.recv() => {
                        let Some(job) = job else { break };
                        this.telemetry.record_queue_dequeue();
                        if this.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let engine = this.clone();
                        let permit = match admission.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("dispatch semaphore closed");
                                break;
                            }
                        };
                        tokio::spawn(async move {
                            let _permit = permit;
                            let key = (job.topic.clone(), job.partition, job.offset);
                            let ownership = DeliveryOwnership { key,
                                jobs: engine.in_flight_jobs.clone(), notify: engine.drain_notify.clone(), lease: None };
                            let result = engine.deliver_job(job, ownership).await;
                            if let Err(e) = result {
                                engine.record_pressure(3);
                                debug!(error = %e, "delivery job failed");
                            }
                        });
                    }
                }
            }
        });
        self.background.lock().push(handle);
    }

    fn spawn_defer_scheduler(&self, mut rx: mpsc::Receiver<DeferredJob>) {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut heap = BinaryHeap::<DeferredJob>::new();
            loop {
                if let Some(next) = heap.peek() {
                    let due = next.due;
                    tokio::select! {
                        _ = this.stop_notify.notified() => break,
                        incoming = rx.recv() => {
                            match incoming {
                                Some(job) => heap.push(job),
                                None => break,
                            }
                        }
                        _ = tokio::time::sleep_until(due) => {
                            let now = tokio::time::Instant::now();
                            while heap.peek().is_some_and(|job| job.due <= now) {
                                if let Some(job) = heap.pop() {
                                    this.enqueue(job.job);
                                }
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        _ = this.stop_notify.notified() => break,
                        incoming = rx.recv() => {
                            match incoming {
                                Some(job) => heap.push(job),
                                None => break,
                            }
                        }
                    }
                }
            }
        });
        self.background.lock().push(handle);
    }

    fn replay_retry_state(&self) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        for mut record in self.retry_state.pending() {
            // Upgrade legacy retry records using the immutable delivery metadata.
            if record.fifo_key.is_none() {
                if let Ok(msg) = self.broker.read_message(
                    &record.key.topic,
                    record.key.partition,
                    record.key.offset,
                ) {
                    record.fifo_key = Self::fifo_retry_key(&msg);
                    if record.fifo_key.is_some() {
                        if let Err(error) = self.retry_state.schedule(record.clone()) {
                            warn!(%error, "could not upgrade FIFO retry index");
                        }
                    }
                }
            }
            let delay_ms = record.due_at_ms.saturating_sub(now_ms).max(0) as u64;
            self.defer_job(
                DeliveryJob::live(
                    record.key.topic,
                    record.key.partition,
                    record.key.offset,
                    record.key.message_id,
                ),
                Duration::from_millis(delay_ms),
                "durable_retry_replay",
            );
        }
    }

    fn replay_lifecycle_state(&self) {
        for transition in self.lifecycle.pending() {
            let source = &transition.source;
            let message = self
                .broker
                .read_message(&source.topic, source.partition, source.offset);
            let Ok(_message) = message else {
                let _ = self.lifecycle.clear(source);
                continue;
            };
            let _ = self.retry_state.clear(source);
            self.enqueue(DeliveryJob::live(
                source.topic.clone(),
                source.partition,
                source.offset,
                source.message_id,
            ));
        }
    }

    fn retry_key(msg: &StoredMessage) -> RetryKey {
        RetryKey {
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            message_id: msg.id,
        }
    }

    fn fifo_retry_key(msg: &StoredMessage) -> Option<String> {
        if !broker_partition::delivery_uses_flow_control(msg) {
            return None;
        }
        let flow = broker_partition::ResolvedFlow::for_delivery(&msg.routing_key, msg);
        (flow.parallelism == 1).then_some(flow.key)
    }

    fn blocking_delivery_retry(&self, msg: &StoredMessage) -> Option<RetryRecord> {
        Self::fifo_retry_key(msg)
            .and_then(|flow| {
                self.retry_state.blocking_retry(
                    &broker_partition::flow_lane_owner(msg).to_string(),
                    &flow,
                    msg.partition,
                    msg.offset,
                )
            })
            .or_else(|| self.retry_state.get(&Self::retry_key(msg)))
    }

    fn clear_retry(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        self.retry_state
            .clear(&Self::retry_key(msg))
            .map_err(|error| DispatchError::Failed(error.to_string()))
    }

    fn schedule_retry(
        &self,
        msg: &StoredMessage,
        attempts: u32,
        delay_ms: u64,
        reason: String,
    ) -> Result<(), DispatchError> {
        let delay_ms = delay_ms.max(1);
        self.persist_retry(msg, attempts, delay_ms, reason)?;
        self.telemetry.record_retry_scheduled();
        self.defer_job(
            DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id),
            Duration::from_millis(delay_ms),
            "delivery_retry",
        );
        Ok(())
    }

    fn persist_retry(
        &self,
        msg: &StoredMessage,
        attempts: u32,
        delay_ms: u64,
        reason: String,
    ) -> Result<(), DispatchError> {
        let delay_ms = delay_ms.max(1);
        let due_at_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(delay_ms as i64);
        self.retry_state
            .schedule(RetryRecord {
                key: Self::retry_key(msg),
                cursor_key: broker_partition::flow_lane_owner(msg).to_string(),
                fifo_key: Self::fifo_retry_key(msg),
                attempts,
                due_at_ms,
                last_error: reason,
            })
            .map_err(|error| DispatchError::Failed(error.to_string()))?;
        Ok(())
    }

    /// Re-enqueue after a delay without advancing the dispatch cursor (pause / host block).
    fn defer_job(&self, job: DeliveryJob, delay: Duration, reason: &'static str) {
        self.telemetry.record_deferred();
        self.record_pressure(1);
        debug!(
            topic = %job.topic,
            partition = job.partition,
            offset = job.offset,
            delay_ms = delay.as_millis() as u64,
            reason,
            "delivery deferred; will retry"
        );
        let deferred = DeferredJob {
            due: tokio::time::Instant::now() + delay,
            sequence: self.defer_sequence.fetch_add(1, Ordering::Relaxed),
            job,
        };
        match self.defer_tx.try_send(deferred) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(job)) => {
                self.telemetry.record_defer_rejected();
                self.record_pressure(2);
                debug!(
                    topic = %job.job.topic,
                    offset = job.job.offset,
                    reason,
                    "defer scheduler full; relying on durable backfill"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    async fn deliver_job(
        &self,
        job: DeliveryJob,
        mut ownership: DeliveryOwnership,
    ) -> Result<(), DispatchError> {
        if !self.shard_leader(job.partition) {
            return Ok(());
        }
        // Lease seam: skip if already claimed by fleet/another worker.
        if let Some(leases) = &self.leases {
            if leases.is_offset_leased(&job.topic, job.partition, job.offset) {
                return Ok(());
            }
        }
        let msg = match self
            .broker
            .read_message(&job.topic, job.partition, job.offset)
        {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    error = %e,
                    topic = %job.topic,
                    partition = job.partition,
                    offset = job.offset,
                    message_id = %job.message_id,
                    "delivery skipped: message not found in log"
                );
                return Ok(());
            }
        };
        let committed_hwm = self.broker.committed_hwm(&job.topic, job.partition)?;
        if job.offset >= committed_hwm {
            self.defer_job(job, Duration::from_millis(250), "awaiting_commit");
            return Ok(());
        }
        let lane_key = broker_partition::flow_lane_owner(&msg).to_string();
        if let Some(blocking) = self.blocking_delivery_retry(&msg) {
            let is_retry = blocking.key == Self::retry_key(&msg);
            let wait_ms = blocking
                .due_at_ms
                .saturating_sub(chrono::Utc::now().timestamp_millis())
                .max(0) as u64;
            if !is_retry || wait_ms > 0 {
                self.defer_job(
                    job,
                    Duration::from_millis(wait_ms.max(100)),
                    "lane_retry_fence",
                );
                return Ok(());
            }
        }

        let lease_id = if let Some(leases) = &self.leases {
            let id = Uuid::new_v4();
            let claimed = crate::lease::ClaimedJob {
                lease_id: id,
                topic: job.topic.clone(),
                partition: job.partition,
                offset: job.offset,
                message_id: job.message_id,
                expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                generation: 0,
                committed_hwm,
                cursor_key: lane_key,
                message: None,
            };
            if !leases.try_insert(self.local_holder.clone(), claimed) {
                return Ok(());
            }
            Some(id)
        } else {
            None
        };
        ownership.lease = Some(LeaseDrop {
            leases: self.leases.clone(),
            lease_id,
            holder: self.local_holder.clone(),
        });

        if msg
            .destination_url
            .as_ref()
            .map(|u| u.is_empty())
            .unwrap_or(true)
        {
            self.finalize_dead_letter(&msg, "missing destination URL — moved to DLQ")
                .await?;
            return Ok(());
        }

        if let Some(queue_id) = msg.queue_id {
            if let Some(queue) = self.broker.get_queue_by_id(queue_id)? {
                if queue.paused {
                    self.defer_job(job, Duration::from_secs(15), "queue_paused");
                    return Ok(());
                }
            }
        }
        if let Some(member_id) = msg.group_member_id {
            if let Some(member) = self.broker.get_group_member(member_id)? {
                if member.paused {
                    self.defer_job(job, Duration::from_secs(15), "member_paused");
                    return Ok(());
                }
            }
        }

        if !broker_partition::delivery_uses_flow_control(&msg) {
            return self.deliver_message(&msg).await;
        }

        let limits = broker_partition::ResolvedFlow::for_delivery(&msg.routing_key, &msg);
        let lane_owner = broker_partition::flow_lane_owner(&msg);
        self.flow
            .submit_owned(
                self.clone(),
                lane_owner,
                msg.clone(),
                limits,
                Some(ownership),
            )
            .await;
        Ok(())
    }

    pub(crate) async fn deliver_message(&self, msg: &StoredMessage) -> Result<(), DispatchError> {
        let _permit = self
            .in_flight
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DispatchError::Failed("dispatch semaphore closed".into()))?;
        let _active_http = self.active_http_guard();
        if let Some(blocking) = self.blocking_delivery_retry(msg) {
            let is_retry = blocking.key == Self::retry_key(msg);
            let wait_ms = blocking
                .due_at_ms
                .saturating_sub(chrono::Utc::now().timestamp_millis())
                .max(0) as u64;
            if !is_retry || wait_ms > 0 {
                self.defer_job(
                    DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id),
                    Duration::from_millis(wait_ms.max(100)),
                    "lane_retry_fence",
                );
                return Ok(());
            }
        }
        self.memory_guard.wait_below_limit().await;
        let committed_hwm = self.broker.committed_hwm(&msg.topic, msg.partition)?;
        if msg.offset >= committed_hwm {
            return Err(DispatchError::Uncommitted);
        }

        let url = msg
            .destination_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(DispatchError::NoDestination)?;

        if self.host_blocker.is_blocked_async(url).await {
            let job = DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id);
            self.defer_job(job, Duration::from_secs(30), "host_blocked");
            return Ok(());
        }
        let _secret = msg
            .destination_secret
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or(DispatchError::NoDestination)?;
        let lane_owner = broker_partition::flow_lane_owner(msg);

        let tenant_id = if !msg.tenant_id.is_empty() {
            msg.tenant_id.clone()
        } else {
            self.broker.tenant()
        };
        let cursor =
            self.broker
                .dispatch_offset(&tenant_id, &lane_owner.to_string(), msg.partition)?;

        if msg.offset < cursor {
            return Ok(());
        }
        if self
            .broker
            .is_dispatch_complete(
                &tenant_id,
                &lane_owner.to_string(),
                msg.partition,
                msg.offset,
            )
            .unwrap_or(false)
        {
            self.commit_dispatch_offset(
                &tenant_id,
                &msg.topic,
                &lane_owner.to_string(),
                msg.partition,
                msg.offset,
            )
            .await?;
            self.lifecycle
                .clear(&Self::retry_key(msg))
                .map_err(|error| DispatchError::Failed(error.to_string()))?;
            let _ = self
                .broker
                .try_purge_message(&msg.topic, msg.partition, msg.offset);
            return Ok(());
        }
        let mut delivery_msg = msg.clone();
        if let Err(e) = self.broker.hydrate_message_payload(&mut delivery_msg) {
            self.record_pressure(3);
            debug!(
                error = %e,
                message_id = %msg.id,
                "delivery deferred: could not load payload blob"
            );
            let job = DeliveryJob::live(msg.topic.clone(), msg.partition, msg.offset, msg.id);
            self.defer_job(job, Duration::from_secs(10), "hydrate_failed");
            return Ok(());
        }

        let outbound = build_outbound(&delivery_msg);
        let retry_key = Self::retry_key(msg);
        let prior_retry = self.retry_state.get(&retry_key);
        if prior_retry
            .as_ref()
            .is_some_and(|record| record.due_at_ms <= chrono::Utc::now().timestamp_millis())
        {
            self.telemetry.record_retry_due();
        }
        let attempt = prior_retry.map(|record| record.attempts).unwrap_or(0);
        let started = Instant::now();
        #[allow(unused_assignments)]
        let mut last_failure = String::from("delivery failed");

        {
            let long = delivery_msg.payload.len() >= self.config.long_payload_threshold_bytes;
            if let Err(error) = crate::egress::validate_destination_url(url) {
                self.telemetry.record_http_outcome(5, Duration::ZERO);
                self.finalize_dead_letter(
                    msg,
                    format!("destination blocked by egress policy: {error}"),
                )
                .await?;
                return Ok(());
            }
            let http = if long {
                &self.long_client
            } else {
                &self.client
            };
            let response = apply_to_reqwest(http, url, outbound.clone()).send().await;
            self.record_http_result(&response, started.elapsed());
            let mut requested_retry_after_ms = None;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    self.host_blocker.record_success_async(url).await;
                    discard_body_capped(resp).await;
                    self.clear_retry(msg)?;
                    self.commit_dispatch_offset(
                        &tenant_id,
                        &msg.topic,
                        &lane_owner.to_string(),
                        msg.partition,
                        msg.offset,
                    )
                    .await?;
                    let _ = self
                        .broker
                        .try_purge_message(&msg.topic, msg.partition, msg.offset);
                    debug!(
                        lane_owner = %lane_owner,
                        queue = %msg.topic,
                        destination = %redacted_destination(url),
                        partition = msg.partition,
                        offset = msg.offset,
                        routing_key = %msg.routing_key,
                        priority = msg.priority,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook delivered"
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    last_failure = format!("HTTP {status} from destination");
                    self.record_pressure(3);
                    debug!(
                        status = %resp.status(),
                        lane_owner = %lane_owner,
                        destination = %redacted_destination(url),
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook non-success"
                    );
                    let retry_after_ms = resp.headers().get("retry-after").and_then(|v| {
                        v.to_str()
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(|secs| {
                                let cap = self.config.retry_defaults.backoff.max_ms;
                                (secs * 1000).min(cap)
                            })
                    });
                    discard_body_capped(resp).await;
                    if self.config.non_retry_status_codes.contains(&status) {
                        self.finalize_dead_letter(
                            msg,
                            format!(
                                "HTTP {status} — non-retryable status (moved to DLQ without further retries)"
                            ),
                        )
                        .await?;
                        return Ok(());
                    }
                    requested_retry_after_ms = retry_after_ms;
                }
                Err(e) if crate::egress::is_policy_failure(&e) => {
                    self.finalize_dead_letter(msg, "destination blocked by egress policy")
                        .await?;
                    return Ok(());
                }
                Err(e) => {
                    let e = e.without_url();
                    self.host_blocker.record_transport_failure_async(url).await;
                    last_failure = format!("transport error: {e}");
                    self.record_pressure(3);
                    debug!(
                        error = %e,
                        lane_owner = %lane_owner,
                        destination = %redacted_destination(url),
                        attempt,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "webhook request failed"
                    );
                }
            }

            self.retry_local_failure(msg, attempt, requested_retry_after_ms, last_failure)
                .await
        }
    }

    async fn retry_local_failure(
        &self,
        msg: &StoredMessage,
        attempt: u32,
        requested_retry_after_ms: Option<u64>,
        last_failure: String,
    ) -> Result<(), DispatchError> {
        let attempt = attempt.saturating_add(1);
        let max_retries = msg.max_retries;
        if attempt > max_retries {
            self.telemetry.record_retry_exhausted();
            let attempts = max_retries.saturating_add(1);
            let detail = last_failure.as_str();
            self.finalize_dead_letter(
                    msg,
                    format!(
                        "{detail} — exhausted {attempts} delivery attempt(s) (max_retries={max_retries})"
                    ),
                )
                .await?;
            return Ok(());
        }

        let backoff_cfg = msg
            .retry_backoff
            .as_ref()
            .unwrap_or(&self.config.retry_defaults.backoff);
        let backoff = backoff_cfg.delay_ms(attempt);
        let delay_ms = requested_retry_after_ms
            .unwrap_or(backoff)
            .min(backoff_cfg.max_ms);
        self.schedule_retry(msg, attempt, delay_ms, last_failure)?;
        Ok(())
    }

    pub async fn commit_cursor(
        &self,
        tenant_id: &str,
        topic: &str,
        cursor_key: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), DispatchError> {
        self.commit_dispatch_offset(tenant_id, topic, cursor_key, partition, offset)
            .await
    }

    async fn commit_dispatch_offset(
        &self,
        tenant_id: &str,
        topic: &str,
        cursor_key: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), DispatchError> {
        let cursor = self
            .broker
            .dispatch_offset(tenant_id, cursor_key, partition)?;
        if offset < cursor {
            return Ok(());
        }
        let newly_completed = !self
            .broker
            .is_dispatch_complete(tenant_id, cursor_key, partition, offset)?;
        // Persist completion first. Reconstruct the contiguous cursor entirely
        // from durable completion keys so restart does not lose gap state.
        self.broker
            .mark_dispatch_complete(tenant_id, cursor_key, partition, offset)?;
        let hwm = self.broker.committed_hwm(topic, partition)?;
        let mut next = cursor;
        const PAGE: usize = 256;
        'scan: while next < hwm {
            let messages = self
                .broker
                .list_topic_messages_from(topic, partition, next, PAGE)?;
            if messages.is_empty() {
                next = hwm;
                break;
            }
            let count = messages.len();
            for msg in messages {
                next = msg.offset.saturating_add(1);
                if broker_partition::flow_lane_owner(&msg).to_string() != cursor_key {
                    continue;
                }
                if !self
                    .broker
                    .is_dispatch_complete(tenant_id, cursor_key, partition, msg.offset)?
                {
                    next = msg.offset;
                    break 'scan;
                }
            }
            if count < PAGE {
                next = hwm;
                break;
            }
        }
        if next != cursor {
            self.broker
                .set_dispatch_offset(tenant_id, cursor_key, partition, next)?;
        }
        self.telemetry
            .record_completion(partition, cursor_key, cursor, next, hwm, newly_completed);
        let next_head = if next < hwm {
            self.broker
                .read_message(topic, partition, next)
                .ok()
                .map(|msg| (msg.offset, msg.published_at_ms))
        } else {
            None
        };
        self.telemetry
            .advance_pending_head(partition, cursor_key, next, next_head);
        Ok(())
    }

    async fn finalize_dead_letter(
        &self,
        msg: &StoredMessage,
        reason: impl Into<String>,
    ) -> Result<(), DispatchError> {
        let reason = reason.into();
        let source = Self::retry_key(msg);
        let cursor_key = broker_partition::flow_lane_owner(msg).to_string();
        self.lifecycle
            .prepare(DlqTransition {
                source: source.clone(),
                cursor_key: cursor_key.clone(),
                reason: reason.clone(),
                phase: DlqPhase::Prepared,
                updated_at_ms: chrono::Utc::now().timestamp_millis(),
            })
            .map_err(|error| DispatchError::Failed(error.to_string()))?;
        self.telemetry.record_dlq_prepared();
        if let Err(error) = self.move_to_dlq(msg, reason).await {
            self.telemetry.record_dlq_failure();
            return Err(error);
        }
        if let Err(error) = self.lifecycle.mark_dlq_committed(&source) {
            self.telemetry.record_dlq_failure();
            return Err(DispatchError::Failed(error.to_string()));
        }
        self.telemetry.record_dlq_committed();
        let tenant = if msg.tenant_id.is_empty() {
            self.broker.tenant()
        } else {
            msg.tenant_id.clone()
        };
        self.commit_dispatch_offset(&tenant, &msg.topic, &cursor_key, msg.partition, msg.offset)
            .await?;
        self.clear_retry(msg)?;
        self.lifecycle
            .clear(&source)
            .map_err(|error| DispatchError::Failed(error.to_string()))?;
        let _ = self
            .broker
            .try_purge_message(&msg.topic, msg.partition, msg.offset);
        Ok(())
    }

    async fn move_to_dlq(
        &self,
        msg: &StoredMessage,
        reason: impl Into<String>,
    ) -> Result<(), DispatchError> {
        let mut msg = msg.clone();
        self.broker
            .hydrate_message_payload(&mut msg)
            .map_err(DispatchError::Broker)?;
        let dlq = match (msg.group_id, msg.group_member_id) {
            (Some(gid), Some(mid)) => group_member_dlq_topic(gid, mid),
            _ => dlq_topic(&msg.topic),
        };
        let reason = reason.into();
        let payload = serde_json::json!({
            "source_queue": msg.topic,
            "message_id": msg.id,
            "destination_url": msg.destination_url,
            "method": msg.http_method,
            "reason": reason,
            "body": String::from_utf8_lossy(&msg.payload),
        });
        let dlq_response = self.broker.publish_immediate(PublishRequest {
            routing_id: None,
            topic: dlq,
            routing_key: msg.id.to_string(),
            payload: payload.to_string(),
            payload_encoding: None,
            idempotency_key: Some(format!(
                "dlq:{}:{}:{}:{}",
                msg.topic, msg.partition, msg.offset, msg.id
            )),
            delay_ms: None,
            priority: None,
            parallelism: None,
            flow: None,
            destination: None,
            flow_id: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            url: None,
            secret: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        })?;
        if let (Some(partition), Some(offset)) = (dlq_response.partition, dlq_response.offset) {
            self.broker
                .wait_committed(&dlq_response.topic, partition, offset)
                .await?;
        }
        debug!(
            queue = %msg.topic,
            message_id = %msg.id,
            "message moved to DLQ"
        );
        Ok(())
    }

    /// Fleet push: HTTP only (no local cursor). Caller completes/fails the lease on the broker.
    pub async fn push_http_only(
        &self,
        msg: &StoredMessage,
        committed_hwm: u64,
    ) -> Result<(), DispatchError> {
        let _permit = self
            .in_flight
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DispatchError::Failed("dispatch semaphore closed".into()))?;
        let _active_http = self.active_http_guard();
        self.memory_guard.wait_below_limit().await;
        if msg.offset >= committed_hwm {
            return Err(DispatchError::Uncommitted);
        }
        // Blob-backed bodies arrive from the claim with an empty inline payload;
        // the caller must load them first or the destination gets an empty body.
        if msg.payload_ref_json.is_some() && msg.payload.is_empty() {
            return Err(DispatchError::PayloadUnresolved);
        }
        let url = msg
            .destination_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(DispatchError::NoDestination)?;
        if self.host_blocker.is_blocked_async(url).await {
            return Err(DispatchError::HostBlocked);
        }
        let outbound = build_outbound(msg);
        let long = msg.payload.len() >= self.config.long_payload_threshold_bytes;
        let retry_key = Self::retry_key(msg);
        let prior = self.retry_state.get(&retry_key);
        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Some(record) = &prior {
            if record.due_at_ms > now_ms {
                return Err(DispatchError::RetryDeferred {
                    retry_after_ms: record.due_at_ms.saturating_sub(now_ms) as u64,
                    reason: record.last_error.clone(),
                });
            }
            self.telemetry.record_retry_due();
        }
        crate::egress::validate_destination_url(url).map_err(|error| {
            self.telemetry.record_http_outcome(5, Duration::ZERO);
            DispatchError::Egress(error.to_string())
        })?;
        let http = if long {
            &self.long_client
        } else {
            &self.client
        };
        let started = Instant::now();
        let response = apply_to_reqwest(http, url, outbound).send().await;
        self.record_http_result(&response, started.elapsed());
        let retry_after_ms;
        let failure = match response {
            Ok(response) if response.status().is_success() => {
                self.host_blocker.record_success_async(url).await;
                discard_body_capped(response).await;
                self.clear_retry(msg)?;
                return Ok(());
            }
            Ok(response) => {
                let code = response.status().as_u16();
                retry_after_ms = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|secs| secs.saturating_mul(1000));
                discard_body_capped(response).await;
                if self.config.non_retry_status_codes.contains(&code) {
                    self.clear_retry(msg)?;
                    return Err(DispatchError::NonRetryable(format!("HTTP {code}")));
                }
                self.host_blocker.record_transport_failure_async(url).await;
                format!("HTTP {code}")
            }
            Err(error) if crate::egress::is_policy_failure(&error) => {
                return Err(DispatchError::Egress(
                    "destination blocked by egress policy".into(),
                ));
            }
            Err(error) => {
                let error = error.without_url();
                retry_after_ms = None;
                self.host_blocker.record_transport_failure_async(url).await;
                format!("transport error: {error}")
            }
        };
        self.retry_fleet_failure(msg, prior, retry_after_ms, failure)
    }

    fn retry_fleet_failure(
        &self,
        msg: &StoredMessage,
        prior: Option<RetryRecord>,
        retry_after_ms: Option<u64>,
        failure: String,
    ) -> Result<(), DispatchError> {
        let attempts = prior
            .map(|record| record.attempts)
            .unwrap_or(0)
            .saturating_add(1);
        if attempts > msg.max_retries {
            self.telemetry.record_retry_exhausted();
            self.clear_retry(msg)?;
            return Err(DispatchError::RetryExhausted(format!(
                "{failure}; exhausted {} delivery attempt(s)",
                msg.max_retries.saturating_add(1)
            )));
        }
        let backoff_cfg = msg
            .retry_backoff
            .as_ref()
            .unwrap_or(&self.config.retry_defaults.backoff);
        let wait_ms = retry_after_ms
            .unwrap_or_else(|| backoff_cfg.delay_ms(attempts))
            .min(backoff_cfg.max_ms)
            .max(1);
        self.persist_retry(msg, attempts, wait_ms, failure.clone())?;
        self.telemetry.record_retry_scheduled();
        Err(DispatchError::RetryDeferred {
            retry_after_ms: wait_ms,
            reason: failure,
        })
    }

    fn record_http_result(
        &self,
        response: &Result<reqwest::Response, reqwest::Error>,
        elapsed: Duration,
    ) {
        let class = match response {
            Ok(response) => match response.status().as_u16() / 100 {
                2 => 0,
                3 => 1,
                4 => 2,
                5 => 3,
                _ => 6,
            },
            Err(error) if crate::egress::is_policy_failure(error) => 5,
            Err(_) => 4,
        };
        self.telemetry.record_http_outcome(class, elapsed);
    }

    pub(crate) fn record_flow_error(&self) {
        self.telemetry.record_flow_error();
    }

    /// Move a leased offset to the DLQ and advance the gap-aware cursor.
    pub async fn dead_letter_offset(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        reason: &str,
    ) -> Result<(), DispatchError> {
        let msgs = self
            .broker
            .list_topic_messages_from(topic, partition, offset, 8)?;
        let Some(msg) = msgs.into_iter().find(|m| m.offset == offset) else {
            return Err(DispatchError::Failed(
                "message not found for dead-letter".into(),
            ));
        };
        self.finalize_dead_letter(&msg, reason).await
    }
}

async fn discard_body_capped(resp: reqwest::Response) {
    use futures_util::StreamExt;
    if let Some(len) = resp.content_length() {
        if len > WEBHOOK_RESPONSE_BODY_CAP as u64 {
            drop(resp);
            return;
        }
    }
    let mut n = 0usize;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(bytes) = chunk else { break };
        n = n.saturating_add(bytes.len());
        if n >= WEBHOOK_RESPONSE_BODY_CAP {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_partition::BrokerConfig;

    #[test]
    fn pressure_logs_are_aggregated_and_destination_credentials_are_redacted() {
        let logs = PressureLogs::default();
        assert_eq!(logs.record(0, 30_000), Some([1, 0, 0, 0]));
        for _ in 0..10_000 {
            assert!(logs.record(1, 30_001).is_none());
        }
        assert_eq!(logs.record(2, 60_000), Some([0, 10_000, 1, 0]));
        assert_eq!(
            redacted_destination("https://user:password@example.com/secret/path?token=abc#private"),
            "https://example.com:443"
        );
        assert_eq!(
            redacted_destination("not a URL with a secret"),
            "<invalid destination>"
        );
    }

    fn publish_test_message(broker: &Broker, url: &str) -> StoredMessage {
        let request = serde_json::from_value(serde_json::json!({
            "url": url, "secret": "secret", "payload": "test", "routing_key": "lane"
        }))
        .unwrap();
        let response = broker.publish(request).unwrap();
        broker.flush_wal().unwrap();
        broker
            .read_message(
                &response.topic,
                response.partition.unwrap(),
                response.offset.unwrap(),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn webhook_client_reuses_connections() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..2 {
                let mut request = Vec::new();
                loop {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let engine = DispatchEngine::new_broker_only(broker, DispatchConfig::default());
        for _ in 0..2 {
            engine
                .client
                .get(format!("http://{address}/hook"))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        engine.shutdown_background().await;
    }

    #[tokio::test]
    async fn dns_failure_is_durable_retry_not_dead_letter() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let msg = publish_test_message(&broker, "https://destination.invalid/hook");
        let engine = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        tokio::time::timeout(Duration::from_secs(5), engine.deliver_message(&msg))
            .await
            .unwrap()
            .unwrap();
        let key = DispatchEngine::retry_key(&msg);
        assert_eq!(engine.retry_state.get(&key).unwrap().attempts, 1);
        assert!(engine.lifecycle.pending().is_empty());
        assert!(!broker
            .is_dispatch_complete(
                &broker.tenant(),
                &broker_partition::flow_lane_owner(&msg).to_string(),
                msg.partition,
                msg.offset
            )
            .unwrap());
        engine.clear_retry(&msg).unwrap();
        assert!(matches!(
            engine.push_http_only(&msg, msg.offset + 1).await,
            Err(DispatchError::RetryDeferred { .. })
        ));
        assert_eq!(engine.retry_state.get(&key).unwrap().attempts, 1);
        engine.shutdown_background().await;
        assert_eq!(engine.telemetry_snapshot().delivery.outcomes[4], 2);
    }

    #[tokio::test]
    async fn retries_fence_only_the_actual_strict_fifo_key() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let mut first = publish_test_message(&broker, "https://example.com/a");
        let engine = DispatchEngine::new_broker_only(broker, DispatchConfig::default());
        first.queue_id = Some(Uuid::new_v4());
        first.flow_parallelism = Some(1);
        first.flow_key = Some("a".into());
        engine
            .persist_retry(&first, 1, 60_000, "HTTP 503".into())
            .unwrap();
        let mut later = first.clone();
        later.offset += 1;
        later.id = Uuid::new_v4();
        assert!(engine.blocking_delivery_retry(&later).is_some());
        later.flow_key = Some("b".into());
        assert!(engine.blocking_delivery_retry(&later).is_none());
        later.flow_key = Some("a".into());
        later.flow_parallelism = Some(2);
        assert!(engine.blocking_delivery_retry(&later).is_none());
        let mut direct = first.clone();
        direct.queue_id = None;
        direct.flow_key = None;
        direct.flow_parallelism = None;
        engine
            .persist_retry(&direct, 1, 60_000, "HTTP 503".into())
            .unwrap();
        assert!(
            engine.blocking_delivery_retry(&direct).is_some(),
            "own deadline is retained"
        );
        direct.offset += 1;
        direct.id = Uuid::new_v4();
        direct.destination_url = Some("https://example.net/other".into());
        assert!(engine.blocking_delivery_retry(&direct).is_none());
        engine.shutdown_background().await;
    }

    #[tokio::test]
    async fn backfill_cannot_duplicate_work_waiting_in_a_flow_lane() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let request = serde_json::from_value(serde_json::json!({
            "url": "https://example.com/hook", "secret": "secret", "payload": "test",
            "routing_key": "lane", "flow": { "key": "a", "parallelism": 1, "rate": 1, "period_secs": 3600 }
        })).unwrap();
        let response = broker.publish(request).unwrap();
        broker.flush_wal().unwrap();
        let msg = broker
            .read_message(
                &response.topic,
                response.partition.unwrap(),
                response.offset.unwrap(),
            )
            .unwrap();
        let owner = broker_partition::flow_lane_owner(&msg);
        let engine = DispatchEngine::new(broker, DispatchConfig::default());
        let limits = broker_partition::ResolvedFlow::for_delivery(&msg.routing_key, &msg);
        engine.flow.ensure_lane(owner, "a", limits.clone()).await;
        engine.flow.pause(owner, "a").await;
        engine.enqueue(DeliveryJob::live(
            &msg.topic,
            msg.partition,
            msg.offset,
            msg.id,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while engine.flow.get(owner, "a").await.unwrap().wait_list_size != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..5 {
            engine.backfill_pending();
            engine
                .flow
                .submit(engine.clone(), owner, msg.clone(), limits.clone(), true)
                .await;
        }
        assert_eq!(engine.flow.get(owner, "a").await.unwrap().wait_list_size, 1);
        assert_eq!(engine.in_flight_jobs.lock().len(), 1);
        engine.shutdown_background().await;
        assert!(engine.in_flight_jobs.lock().is_empty());
    }

    #[tokio::test]
    async fn flow_and_fleet_delivery_wait_for_http_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let msg = publish_test_message(&broker, "https://example.com/hook");
        let mut engine = DispatchEngine::new_broker_only(broker, DispatchConfig::default());
        engine.in_flight = Arc::new(Semaphore::new(1));
        let permit = engine.in_flight.clone().acquire_owned().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), engine.deliver_message(&msg))
                .await
                .is_err()
        );
        assert!(tokio::time::timeout(
            Duration::from_millis(50),
            engine.push_http_only(&msg, msg.offset + 1)
        )
        .await
        .is_err());
        assert_eq!(engine.active_http.load(Ordering::Acquire), 0);
        drop(permit);
        engine.shutdown_background().await;
    }

    #[tokio::test]
    async fn global_capacity_release_wakes_another_lane() {
        global_capacity_case(false).await;
    }

    #[tokio::test]
    async fn lane_paused_while_waiting_for_global_capacity_does_not_start() {
        global_capacity_case(true).await;
    }

    async fn global_capacity_case(pause_waiter: bool) {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let mut first = publish_test_message(&broker, "https://example.com/one");
        let mut second = publish_test_message(&broker, "https://example.com/two");
        first.destination_url = Some("http://169.254.169.254/".into());
        second.destination_url = first.destination_url.clone();
        let mut engine = DispatchEngine::new_broker_only(broker, DispatchConfig::default());
        engine.in_flight = Arc::new(Semaphore::new(0));
        engine.flow = FlowController::new(Some(1), engine.memory_guard.clone());
        let first_owner = Uuid::new_v4();
        let second_owner = Uuid::new_v4();
        for (owner, key, msg) in [(first_owner, "a", first), (second_owner, "b", second)] {
            engine
                .flow
                .submit(
                    engine.clone(),
                    owner,
                    msg,
                    broker_partition::ResolvedFlow {
                        key: key.into(),
                        parallelism: 1,
                        rate: 0,
                        period_secs: 1,
                    },
                    false,
                )
                .await;
            if key == "a" {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while engine.flow.global_parallelism().await.parallelism_count != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while engine.flow.global_parallelism().await.parallelism_count != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        if pause_waiter {
            engine.flow.pause(second_owner, "b").await;
        }
        engine.in_flight.add_permits(1);
        if pause_waiter {
            tokio::time::timeout(Duration::from_secs(1), async {
                while engine.telemetry_snapshot().dlq_committed < 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(engine.telemetry_snapshot().dlq_committed, 1);
            assert_eq!(
                engine
                    .flow
                    .get(second_owner, "b")
                    .await
                    .unwrap()
                    .wait_list_size,
                1
            );
            engine.flow.resume(second_owner, "b").await;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while engine.telemetry_snapshot().dlq_committed < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the other lane must wake without resubmission");
        engine.shutdown_background().await;
    }

    #[tokio::test]
    async fn broker_only_telemetry_seeds_backlog_without_completions() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let first = publish_test_message(&broker, "https://example.com/hook");
        let engine = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        assert!(engine
            .telemetry_snapshot()
            .shards
            .iter()
            .any(|shard| shard.completion_gap_records > 0));
        let second = publish_test_message(&broker, "https://example.com/hook");
        engine.notify_committed_range(
            &second.topic,
            second.partition,
            second.offset,
            second.offset + 1,
            second.id,
        );
        let gap = engine
            .telemetry_snapshot()
            .shards
            .iter()
            .map(|shard| shard.completion_gap_records)
            .sum::<u64>();
        assert!(gap >= 2);
        assert_eq!(first.partition, second.partition);
        engine.shutdown_background().await;
    }

    #[test]
    fn retention_pins_report_and_forget_collected_dispatch_state() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let engine = DispatchEngine::new_broker_only(broker, DispatchConfig::default());
        let key = |topic: &str, offset| RetryKey {
            topic: topic.into(),
            partition: 0,
            offset,
            message_id: Uuid::new_v4(),
        };
        for (topic, offset) in [("jobs", 3), ("jobs", 9), ("other", 1)] {
            engine
                .retry_state
                .schedule(RetryRecord {
                    key: key(topic, offset),
                    cursor_key: "lane".into(),
                    fifo_key: None,
                    attempts: 1,
                    due_at_ms: 0,
                    last_error: "503".into(),
                })
                .unwrap();
        }
        engine
            .lifecycle
            .prepare(DlqTransition {
                source: key("jobs", 5),
                cursor_key: "lane".into(),
                reason: "exhausted".into(),
                phase: DlqPhase::Prepared,
                updated_at_ms: 0,
            })
            .unwrap();
        let mut pins = engine.retention_pins();
        pins.sort();
        assert_eq!(
            pins,
            vec![
                ("jobs".to_string(), 0, 3),
                ("jobs".to_string(), 0, 5),
                ("jobs".to_string(), 0, 9),
                ("other".to_string(), 0, 1),
            ]
        );

        let dropped = engine
            .forget_collected(|topic, partition| topic == "jobs" && partition == 0, 6)
            .unwrap();
        assert_eq!(dropped, 2);
        let mut pins = engine.retention_pins();
        pins.sort();
        assert_eq!(
            pins,
            vec![("jobs".to_string(), 0, 9), ("other".to_string(), 0, 1)]
        );
    }

    #[tokio::test]
    async fn commit_cursor_reconstructs_completed_gap_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let publish = |body: &str| {
            broker
                .publish(PublishRequest {
                    routing_id: None,
                    topic: String::new(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: "lane".into(),
                    payload: body.into(),
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
                .unwrap()
        };
        let first = publish("first");
        let second = publish("second");
        broker.flush_wal().unwrap();
        let topic = first.topic.clone();
        let partition = first.partition.unwrap();
        assert_eq!(second.partition, Some(partition));
        let lane = broker_partition::flow_lane_owner(
            &broker
                .read_message(&topic, partition, first.offset.unwrap())
                .unwrap(),
        )
        .to_string();
        let engine = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        let tenant = broker.tenant();
        engine
            .commit_cursor(&tenant, &topic, &lane, partition, second.offset.unwrap())
            .await
            .unwrap();
        assert_eq!(
            broker.dispatch_offset(&tenant, &lane, partition).unwrap(),
            first.offset.unwrap()
        );
        let snapshot = engine.telemetry_snapshot();
        let shard = snapshot
            .shards
            .iter()
            .find(|item| item.shard == partition)
            .unwrap();
        assert_eq!(shard.completions, 1);
        assert_eq!(shard.completion_gap_records, 2);
        drop(engine);

        let restarted = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        restarted
            .commit_cursor(&tenant, &topic, &lane, partition, first.offset.unwrap())
            .await
            .unwrap();
        assert_eq!(
            broker.dispatch_offset(&tenant, &lane, partition).unwrap(),
            second.offset.unwrap() + 1
        );
        let snapshot = restarted.telemetry_snapshot();
        let shard = snapshot
            .shards
            .iter()
            .find(|item| item.shard == partition)
            .unwrap();
        assert_eq!(shard.completions, 1);
        assert_eq!(shard.cursor_advanced_records, 2);
        assert_eq!(shard.completion_gap_records, 0);
    }

    #[tokio::test]
    async fn dlq_committed_boundary_repairs_source_cursor_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let response = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: String::new(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "lane".into(),
                payload: "body".into(),
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
                max_retries: Some(0),
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        broker.flush_wal().unwrap();
        let partition = response.partition.unwrap();
        let offset = response.offset.unwrap();
        let message = broker
            .read_message(&response.topic, partition, offset)
            .unwrap();
        let source = DispatchEngine::retry_key(&message);
        let lane = broker_partition::flow_lane_owner(&message).to_string();
        let tenant = broker.tenant();
        broker
            .mark_dispatch_complete(&tenant, &lane, partition, offset)
            .unwrap();
        let lifecycle = LifecycleStore::open(&broker.config().data_dir).unwrap();
        lifecycle
            .prepare(DlqTransition {
                source: source.clone(),
                cursor_key: lane.clone(),
                reason: "crash boundary".into(),
                phase: DlqPhase::Prepared,
                updated_at_ms: 1,
            })
            .unwrap();
        lifecycle.mark_dlq_committed(&source).unwrap();
        drop(lifecycle);

        let engine = DispatchEngine::new(broker.clone(), DispatchConfig::default());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            broker.dispatch_offset(&tenant, &lane, partition).unwrap(),
            offset + 1
        );
        assert!(engine.lifecycle.pending().is_empty());
        engine.stop();
    }
}
