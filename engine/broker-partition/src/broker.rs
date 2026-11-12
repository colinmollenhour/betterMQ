//! In-process broker coordinating partition logs and RocksDB metadata.

use crate::flow::FlowSpec;
use crate::flows::FlowProfileRegistry;
use crate::groups::GroupRegistry;
use crate::http_delivery::HttpDeliverySpec;
use crate::layout::ShardLayout;
use crate::priority::normalize_priority;
use crate::shard::ShardHandle;
use crate::subscriptions::{Subscription, SubscriptionRegistry};
use crate::topic::{group_topic, is_dlq_topic, partition_dir, DIRECT_TOPIC};
use broker_payload::{hydrate_payload, prepare_for_log_with, BlobStore, PayloadPolicy};
use broker_proto::{LogRecord, RetryBackoff, RetryDefaults};
#[cfg(feature = "slate")]
use broker_storage::{open_object_store_from_env, slate_db_path};
use broker_storage::{
    DedupEntry, MetadataStore, MetadataWriteBatch, PartitionBackend, PartitionLogConfig,
    StorageMode, StoredMessage,
};
use chrono::Utc;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

pub const DEFAULT_TENANT: &str = "default";
pub const DEFAULT_PARTITIONS: u32 = 4;

tokio::task_local! {
    static OWNED_BROKER: Uuid;
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub tenant_id: String,
    pub partitions: u32,
    pub log: PartitionLogConfig,
    pub storage: StorageMode,
    pub retry_defaults: RetryDefaults,
    /// Replicated local-WAL producer indexes belong to each WAL copy, even
    /// when delivery cursors use the optional shared metadata backend.
    pub local_replica_identities: bool,
}

impl BrokerConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            tenant_id: DEFAULT_TENANT.into(),
            partitions: DEFAULT_PARTITIONS,
            log: PartitionLogConfig::from_env(),
            storage: StorageMode::from_env(),
            retry_defaults: RetryDefaults::default(),
            local_replica_identities: false,
        }
    }
}

fn resolve_delivery_retry(
    req: &PublishRequest,
    queue: Option<&Subscription>,
    defaults: &RetryDefaults,
) -> (u32, RetryBackoff) {
    let max_retries = req
        .max_retries
        .or(queue.and_then(|q| q.default_max_retries))
        .unwrap_or(defaults.max_retries);
    let retry_backoff = req
        .retry_backoff
        .clone()
        .or_else(|| queue.and_then(|q| q.retry_backoff.clone()))
        .unwrap_or_else(|| defaults.backoff.clone());
    (max_retries, retry_backoff)
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("storage error: {0}")]
    Storage(#[from] broker_storage::LogError),
    #[error("index error: {0}")]
    Index(#[from] broker_storage::IndexError),
    #[error("subscription error: {0}")]
    Subscription(#[from] crate::subscriptions::SubscriptionError),
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("flow profile not found: {0}")]
    FlowProfileNotFound(Uuid),
    #[error("flow profile error: {0}")]
    FlowProfile(#[from] crate::flows::FlowProfileError),
    #[error("group error: {0}")]
    Group(#[from] crate::groups::GroupError),
    #[error("payload error: {0}")]
    Payload(#[from] broker_payload::PayloadError),
    #[error("not shard leader for partition {0}")]
    NotShardLeader(u32),
    #[error("invalid name: {0}")]
    InvalidName(#[from] broker_proto::PathSegmentError),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("idempotency key {0} is in flight on another request; retry shortly")]
    IdempotencyInFlight(String),
    #[error("idempotency key {0} is awaiting contiguous quorum verification; retry shortly")]
    DedupQuorumPending(String),
}

fn blocking_task_error(error: tokio::task::JoinError) -> BrokerError {
    BrokerError::Storage(broker_storage::LogError::Io(std::io::Error::other(
        error.to_string(),
    )))
}

/// Returns `Some(generation)` when this node is leader for the shard (Slate fence).
type ShardFenceFn = Arc<dyn Fn(u32) -> Option<u64> + Send + Sync>;
type ShardAuthorityFn = Arc<dyn Fn(u32) -> Option<(u64, Uuid)> + Send + Sync>;

mod identity_gc;
mod identity_migration;
mod log_rewrite;
mod producer_admission;

#[derive(Default)]
struct LogEpoch {
    admission: Mutex<()>,
    epoch: std::sync::atomic::AtomicU64,
    rewriting: std::sync::atomic::AtomicBool,
    pending_metadata: Mutex<std::collections::BTreeMap<u64, usize>>,
    gc_indexed: Mutex<std::collections::HashSet<String>>,
    dirty_revision: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    trim_stop: std::sync::atomic::AtomicU64,
}

pub struct PendingIdentity {
    state: Arc<LogEpoch>,
    offset: u64,
    epoch: u64,
    durable: Arc<std::sync::atomic::AtomicBool>,
}

impl PendingIdentity {
    pub fn mark_durable(&self) {
        self.durable
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for PendingIdentity {
    fn drop(&mut self) {
        // Never acquire append admission here: cancellation may drop this
        // guard on the metadata writer while GC awaits that same writer.
        let mut pending = self.state.pending_metadata.lock();
        if !self.durable.load(std::sync::atomic::Ordering::Acquire) {
            self.state
                .dirty_revision
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.state.gc_indexed.lock().clear();
        }
        if let Some(count) = pending.get_mut(&self.offset) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&self.offset);
            }
        }
    }
}

pub(crate) struct GcLease<'a> {
    _admission: parking_lot::MutexGuard<'a, ()>,
    pub(crate) target: u64,
}

struct LogRewrite {
    state: Arc<LogEpoch>,
    release: bool,
}
impl Drop for LogRewrite {
    fn drop(&mut self) {
        if self.release {
            self.state
                .rewriting
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Frozen at schedule/enqueue time so later queue URL edits do not affect in-flight jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationSnapshot {
    pub queue_id: Option<Uuid>,
    pub url: String,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishRequest {
    /// Routing identity for keyless requests, preserved across internal forwarding.
    /// Does not enable FIFO delivery or replace the message's routing key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_id: Option<Uuid>,
    /// Queue name (omit when `queue_id` or direct `url` is set).
    #[serde(default)]
    pub topic: String,
    /// Resolve queue by id (preferred for `POST /v1/enqueue`).
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    #[serde(default)]
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub group_member_id: Option<Uuid>,
    #[serde(default)]
    pub routing_key: String,
    #[serde(deserialize_with = "crate::payload::deserialize_flexible_payload")]
    pub payload: String,
    #[serde(default)]
    pub payload_encoding: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub priority: Option<u8>,
    /// Reference a flow profile created with `POST /v1/flows`.
    #[serde(default)]
    pub flow_id: Option<Uuid>,
    /// One-off publish (`POST /v1/publish`) — do not use with `queue` / `queue_id`.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    /// When set (delayed/cron fire), use this destination instead of re-reading the queue registry.
    #[serde(default)]
    pub destination: Option<DestinationSnapshot>,
    /// Legacy inline flow — prefer `flow_id`.
    #[serde(default)]
    pub flow: Option<FlowSpec>,
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Push retries after first failure (falls back to queue / broker defaults).
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::http_delivery::deserialize_optional_headers"
    )]
    pub headers: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
    #[serde(default)]
    pub request: Option<crate::http_delivery::HttpDeliveryInput>,
}

impl PublishRequest {
    /// Select once, before choosing a leader. Repeated forwarding preserves it.
    pub fn ensure_routing_id(&mut self) {
        if self.routing_key.trim().is_empty() && self.routing_id.is_none() {
            self.routing_id = Some(
                self.idempotency_key
                    .as_ref()
                    .map_or_else(Uuid::new_v4, |key| {
                        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
                    }),
            );
        }
    }

    pub fn shard_routing_key(&self) -> String {
        let key = self.routing_key.trim();
        if key.is_empty() {
            self.routing_id.map(|id| id.to_string()).unwrap_or_default()
        } else {
            key.to_string()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<Uuid>,
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<ScheduledInfo>,
    /// Commit epoch / durable high watermark after the producing shard fsyncs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    /// Encoded log frame for CP7a replication (not serialized to API clients).
    #[serde(skip)]
    pub replication_frame: Option<Vec<u8>>,
    /// Original append/lookup authority, retained across every awaited barrier.
    #[serde(skip)]
    pub publish_fence: Option<PublishFence>,
}

#[derive(Clone)]
pub struct PublishFence {
    generation: Option<u64>,
    validate: broker_storage::MetadataValidator,
}

impl std::fmt::Debug for PublishFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishFence")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl PublishFence {
    pub fn generation(&self) -> Option<u64> {
        self.generation
    }
    pub fn validate(&self) -> Result<(), broker_storage::IndexError> {
        (self.validate)()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledInfo {
    pub schedule_id: Uuid,
    pub deliver_at_ms: i64,
}

/// Identity of one physical log on this node: `(topic, partition)` in layout
/// V1, `("__physical", shard)` in V2.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogId {
    pub namespace: String,
    pub partition: u32,
}

impl std::fmt::Display for LogId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.partition)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReservationKey {
    tenant_id: String,
    idempotency_key: String,
}

/// How long an async publish waits for another request holding the same
/// idempotency key before giving up with [`BrokerError::IdempotencyInFlight`].
pub const DEDUP_IN_FLIGHT_WAIT: Duration = Duration::from_secs(10);

/// In-flight idempotency keys. Each entry owns a `watch` sender that is never
/// written; dropping it on release wakes every async waiter. Nothing here
/// blocks a thread, so waiters cannot starve the runtime that must poll the
/// reservation holder.
#[derive(Default)]
struct DedupReservations {
    pending: Mutex<HashMap<ReservationKey, watch::Sender<()>>>,
}

impl DedupReservations {
    fn try_acquire(
        self: &Arc<Self>,
        key: ReservationKey,
    ) -> Result<ReservationToken, DedupInFlight> {
        let mut pending = self.pending.lock();
        if let Some(sender) = pending.get(&key) {
            return Err(DedupInFlight {
                idempotency_key: key.idempotency_key,
                released: sender.subscribe(),
            });
        }
        pending.insert(key.clone(), watch::channel(()).0);
        Ok(ReservationToken {
            reservations: Arc::clone(self),
            key,
            release_on_drop: true,
        })
    }

    fn release(&self, key: &ReservationKey) {
        // Dropping the sender closes the channel and wakes all waiters.
        drop(self.pending.lock().remove(key));
    }
}

/// Another request currently owns this idempotency key.
struct DedupInFlight {
    idempotency_key: String,
    released: watch::Receiver<()>,
}

impl DedupInFlight {
    /// Resolves once the owning request releases the key.
    async fn released(mut self) {
        // The sender is never written, so `changed` only returns once it is dropped.
        let _ = self.released.changed().await;
    }

    fn into_error(self) -> BrokerError {
        BrokerError::IdempotencyInFlight(self.idempotency_key)
    }
}

struct ReservationToken {
    reservations: Arc<DedupReservations>,
    key: ReservationKey,
    release_on_drop: bool,
}

impl ReservationToken {
    fn release(mut self) {
        self.reservations.release(&self.key);
        self.release_on_drop = false;
    }
}

impl Drop for ReservationToken {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.reservations.release(&self.key);
        }
    }
}

enum ReservationOutcome {
    Existing(DedupEntry),
    Acquired(ReservationToken),
}

enum DedupVerification {
    Verified(String),
    Unverified,
    Stale,
}

/// Idempotency reservations kept past the local append until replication
/// quorum resolves. [`Broker::commit_publish_dedup`] records the mappings and
/// releases the keys; dropping it releases the keys without recording them, so
/// a retry after a failed quorum appends again.
#[must_use = "dropping PendingDedup releases idempotency keys without recording them"]
#[derive(Default)]
pub struct PendingDedup {
    entries: Vec<(ReservationToken, DedupEntry)>,
    validators: Vec<broker_storage::MetadataValidator>,
    identity_leases: Vec<PendingIdentity>,
}

impl PendingDedup {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PreparedShardKey {
    partition: u32,
    storage_namespace: String,
    fence_generation: Option<u64>,
}

impl PreparedShardKey {
    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn fence_generation(&self) -> Option<u64> {
        self.fence_generation
    }
}

struct PreparedRecord {
    topic: String,
    partition: u32,
    header: LogRecord,
    payload: Vec<u8>,
    fence_generation: Option<u64>,
    reservation: Option<ReservationToken>,
    validator: Option<broker_storage::MetadataValidator>,
}

pub struct PreparedShardBatch {
    key: PreparedShardKey,
    records: Vec<PreparedRecord>,
}

impl PreparedShardBatch {
    pub fn key(&self) -> &PreparedShardKey {
        &self.key
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

enum PreparedSlot {
    Existing(PublishResponse),
    Awaiting {
        message_id: Uuid,
        duplicate: bool,
        topic_override: Option<String>,
    },
    Final(PublishResponse),
}

pub struct PreparedPublishBatch {
    slots: Vec<PreparedSlot>,
    shard_batches: Vec<PreparedShardBatch>,
    deferred_dedup: PendingDedup,
}

impl PreparedPublishBatch {
    pub fn take_shard_batches(&mut self) -> Vec<PreparedShardBatch> {
        std::mem::take(&mut self.shard_batches)
    }

    /// Reservations retained by `finalize_prepared_batch(.., commit_dedup = false)`.
    pub fn take_pending_dedup(&mut self) -> PendingDedup {
        std::mem::take(&mut self.deferred_dedup)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn finish(self) -> Result<Vec<PublishResponse>, BrokerError> {
        self.slots
            .into_iter()
            .map(|slot| match slot {
                PreparedSlot::Existing(response) | PreparedSlot::Final(response) => Ok(response),
                PreparedSlot::Awaiting { .. } => Err(BrokerError::InvalidConfig(
                    "prepared batch has unfinalized shard records".into(),
                )),
            })
            .collect()
    }
}

struct FinalizeRecord {
    message_id: Uuid,
    topic: String,
    partition: u32,
    idempotency_key: Option<String>,
    reservation: Option<ReservationToken>,
    validator: Option<broker_storage::MetadataValidator>,
}

pub struct AppendedShardBatch {
    appended: Vec<(StoredMessage, Vec<u8>)>,
    records: Vec<FinalizeRecord>,
    identity_lease: Option<PendingIdentity>,
    fence_generation: Option<u64>,
}

pub struct AppendedRecordRef<'a> {
    pub message_id: Uuid,
    pub topic: &'a str,
    pub partition: u32,
    pub offset: u64,
    pub replication_frame: &'a [u8],
}

pub struct TopicMessagePage {
    pub messages: Vec<StoredMessage>,
    pub next: Option<(u32, u64)>,
}

impl AppendedShardBatch {
    pub fn publish_fence(&self) -> Option<PublishFence> {
        self.records
            .first()
            .and_then(|record| record.validator.clone())
            .map(|validate| PublishFence {
                generation: self.fence_generation,
                validate,
            })
    }
    pub fn len(&self) -> usize {
        self.appended.len()
    }

    pub fn is_empty(&self) -> bool {
        self.appended.is_empty()
    }

    pub fn partition(&self) -> Option<u32> {
        self.appended.first().map(|(stored, _)| stored.partition)
    }

    pub fn last_offset(&self) -> Option<u64> {
        self.appended.last().map(|(stored, _)| stored.offset)
    }

    /// Borrow exact record frames for replication before finalization.
    pub fn records(&self) -> impl ExactSizeIterator<Item = AppendedRecordRef<'_>> {
        self.appended
            .iter()
            .map(|(stored, frame)| AppendedRecordRef {
                message_id: stored.id,
                topic: &stored.topic,
                partition: stored.partition,
                offset: stored.offset,
                replication_frame: frame,
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub topic: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub parallelism: Option<u32>,
    #[serde(default)]
    pub default_max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateSubscriptionResponse {
    pub id: Uuid,
    pub topic: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_retries: Option<u32>,
}

#[cfg(feature = "slate")]
struct SlateEnv {
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_root: PathBuf,
}

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

type ShardOpenLocks = HashMap<(String, u32), Arc<Mutex<()>>>;

struct BrokerInner {
    log_epochs: Vec<Arc<LogEpoch>>,
    config: BrokerConfig,
    metadata: Arc<MetadataStore>,
    identity_metadata: Arc<MetadataStore>,
    subscriptions: SubscriptionRegistry,
    flows: FlowProfileRegistry,
    groups: GroupRegistry,
    blob_store: BlobStore,
    payload_policy: RwLock<PayloadPolicy>,
    retention: crate::retention::RetentionStore,
    layout: ShardLayout,
    shards: RwLock<HashMap<String, HashMap<u32, Arc<ShardHandle>>>>,
    // Serialize opens of the same backend without blocking unrelated shards.
    shard_open_locks: Mutex<ShardOpenLocks>,
    dedup_reservations: Arc<DedupReservations>,
    owned_completions: tokio_util::task::TaskTracker,
    accepting_owned: std::sync::atomic::AtomicBool,
    owned_id: Uuid,
    shard_leader_check: Mutex<Option<ShardAuthorityFn>>,
    /// Gate dispatch on the quorum watermark (multi-node local-WAL cells).
    quorum_gating: std::sync::atomic::AtomicBool,
    quorum: RwLock<HashMap<(String, u32), Arc<crate::quorum::QuorumWatermark>>>,
    #[cfg(feature = "slate")]
    slate: Option<SlateEnv>,
}

/// Quorum watermark of one log namespace (observability).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct QuorumStatus {
    pub namespace: String,
    pub partition: u32,
    pub quorum_hwm: u64,
    pub committed_hwm: u64,
    /// Committed records not yet proven on a replication quorum.
    pub lag: u64,
    /// Quorum-proven epochs waiting for an unproven gap below them.
    pub pending_ranges: usize,
}

impl Broker {
    /// Register before handing admitted storage work to a native closure or
    /// detached continuation. Registration after the shutdown fence is refused.
    pub fn owned_completion_token(&self) -> Result<impl Send + 'static, BrokerError> {
        let token = self.inner.owned_completions.token();
        let nested = OWNED_BROKER
            .try_with(|id| *id == self.inner.owned_id)
            .unwrap_or(false);
        if !nested
            && !self
                .inner
                .accepting_owned
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(BrokerError::Storage(broker_storage::LogError::Io(
                std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "broker is draining owned storage work",
                ),
            )));
        }
        Ok(token)
    }

    /// Own the whole admitted operation, including caller-supplied resource
    /// guards, until it completes even if its response waiter disappears.
    pub fn spawn_owned<F>(
        &self,
        future: F,
    ) -> Result<tokio::task::JoinHandle<F::Output>, BrokerError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let token = self.owned_completion_token()?;
        let tenant = self.tenant();
        Ok(tokio::spawn(OWNED_BROKER.scope(
            self.inner.owned_id,
            crate::scope_tenant(tenant, async move {
                let _owned = token;
                future.await
            }),
        )))
    }

    /// Stop new owned work and await admitted continuations before the final
    /// WAL/metadata checkpoint. Producers must be stopped before invoking this.
    pub async fn close_and_drain_owned_completions(&self) {
        self.inner
            .accepting_owned
            .store(false, std::sync::atomic::Ordering::Release);
        self.inner.owned_completions.close();
        self.inner.owned_completions.wait().await;
    }

    /// Only the standalone server invokes this. Cells seed/adopt through Raft.
    pub fn import_legacy_shared_catalog(&self) -> Result<(), BrokerError> {
        crate::catalog_import::import(
            &self.inner.config.data_dir,
            &self.inner.flows,
            &self.inner.subscriptions,
            &self.inner.groups,
        )
    }
    /// Pure preparation: reads existing identity but never publishes a catalog change.
    pub fn prepare_subscription(
        &self,
        req: CreateSubscriptionRequest,
        id: Uuid,
    ) -> Result<Subscription, BrokerError> {
        broker_proto::validate_queue_name(&req.topic)?;
        let existing = self.get_queue(&req.topic)?;
        Ok(Subscription {
            id: existing.as_ref().map_or(id, |s| s.id),
            tenant_id: self.tenant(),
            topic: req.topic,
            url: req.url,
            secret: req.secret,
            paused: existing.as_ref().is_some_and(|s| s.paused),
            parallelism: req
                .parallelism
                .filter(|p| *p > 0)
                .or(existing.as_ref().and_then(|s| s.parallelism)),
            default_max_retries: req
                .default_max_retries
                .or(existing.as_ref().and_then(|s| s.default_max_retries)),
            retry_backoff: req.retry_backoff.or(existing.and_then(|s| s.retry_backoff)),
            updated_at_ms: Utc::now().timestamp_millis(),
        })
    }

    pub fn prepare_flow(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
        id: Uuid,
    ) -> Result<crate::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        if parallelism == 0 || period_secs == 0 {
            return Err(BrokerError::InvalidConfig(
                "parallelism and period_secs must be positive".into(),
            ));
        }
        Ok(crate::FlowProfile {
            id,
            tenant_id: self.tenant(),
            key,
            parallelism,
            rate,
            period_secs,
            updated_at_ms: Utc::now().timestamp_millis(),
        })
    }

    pub fn validate_catalog_group(&self, group: &crate::DispatchGroup) -> Result<(), BrokerError> {
        if group.name.trim().is_empty() {
            return Err(BrokerError::InvalidConfig("group name required".into()));
        }
        Ok(())
    }
    pub fn validate_catalog_member(&self, member: &crate::GroupMember) -> Result<(), BrokerError> {
        if member.name.trim().is_empty() || member.parallelism == 0 || member.period_secs == 0 {
            return Err(BrokerError::InvalidConfig(
                "member name and positive limits required".into(),
            ));
        }
        Ok(())
    }

    pub fn catalog_flows(&self) -> Result<Vec<crate::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.catalog_all()?)
    }
    pub fn catalog_queues(&self) -> Result<Vec<Subscription>, BrokerError> {
        Ok(self.inner.subscriptions.catalog_all()?)
    }
    pub fn catalog_groups(
        &self,
    ) -> Result<(Vec<crate::DispatchGroup>, Vec<crate::GroupMember>), BrokerError> {
        Ok(self.inner.groups.catalog_all()?)
    }
    pub fn apply_flow_authoritative(
        &self,
        tenant: &str,
        id: Uuid,
        value: Option<crate::FlowProfile>,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.flows.apply_authoritative(tenant, id, value)?)
    }
    pub fn apply_queue_authoritative(
        &self,
        tenant: &str,
        id: Uuid,
        value: Option<Subscription>,
    ) -> Result<(), BrokerError> {
        Ok(self
            .inner
            .subscriptions
            .apply_authoritative(tenant, id, value)?)
    }
    pub fn apply_group_authoritative(
        &self,
        tenant: &str,
        id: Uuid,
        value: Option<crate::DispatchGroup>,
    ) -> Result<(), BrokerError> {
        Ok(self
            .inner
            .groups
            .apply_group_authoritative(tenant, id, value)?)
    }
    pub fn apply_member_authoritative(
        &self,
        tenant: &str,
        id: Uuid,
        value: Option<crate::GroupMember>,
    ) -> Result<(), BrokerError> {
        Ok(self
            .inner
            .groups
            .apply_member_authoritative(tenant, id, value)?)
    }
    /// Active tenant for this call (request-scoped in cloud, else config default).
    pub fn tenant(&self) -> String {
        crate::tenant_scope::effective_tenant(&self.inner.config.tenant_id).into_owned()
    }

    pub fn layout(&self) -> &ShardLayout {
        &self.inner.layout
    }

    pub fn shard_count(&self) -> u32 {
        self.inner.layout.shard_count
    }

    /// Physical storage path for V2. V1 remains topic-scoped and returns `None`.
    pub fn physical_shard_path(&self, shard_id: u32) -> Option<PathBuf> {
        (self.inner.layout.is_physical() && shard_id < self.inner.layout.shard_count).then(|| {
            self.inner
                .layout
                .physical_shard_dir(&self.inner.config.data_dir, shard_id)
        })
    }

    pub fn open(config: BrokerConfig) -> Result<Self, BrokerError> {
        if config.partitions == 0 {
            return Err(BrokerError::InvalidConfig(
                "partitions must be greater than 0".into(),
            ));
        }
        if config.log.fsync == broker_storage::FsyncMode::Os
            && std::env::var("BETTERMQ_SAAS")
                .map(|s| matches!(s.trim(), "1" | "true" | "TRUE" | "yes"))
                .unwrap_or(false)
        {
            return Err(BrokerError::InvalidConfig(
                "BETTERMQ_FSYNC=os is not allowed for SaaS (RPO would not be 0)".into(),
            ));
        }
        let layout = ShardLayout::load_or_create(&config.data_dir, config.partitions)
            .map_err(|e| BrokerError::InvalidConfig(format!("shard layout: {e}")))?;
        layout
            .validate_storage(config.storage)
            .map_err(|error| BrokerError::InvalidConfig(error.to_string()))?;
        let mut config = config;
        config.partitions = layout.shard_count;
        let rocks_path = config.data_dir.join("rocksdb");

        #[cfg(feature = "slate")]
        let slate = if config.storage == StorageMode::Slate {
            let object_store = open_object_store_from_env().map_err(|e| {
                BrokerError::Storage(broker_storage::LogError::Slate(e.to_string()))
            })?;
            let cache_root = config.data_dir.join("slate-cache");
            std::fs::create_dir_all(&cache_root)
                .map_err(|e| BrokerError::Storage(broker_storage::LogError::Io(e)))?;
            tracing::info!(
                cache = %cache_root.display(),
                "SlateDB storage enabled (messages + indexes on object store)"
            );
            Some(SlateEnv {
                object_store,
                cache_root,
            })
        } else {
            None
        };

        #[cfg(not(feature = "slate"))]
        if config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                "rebuild bettermq with --features slate (enabled in stack docker image)".into(),
            )));
        }

        let metadata = {
            #[cfg(feature = "slate")]
            {
                if let Some(ref env) = slate {
                    MetadataStore::open_slate(Arc::clone(&env.object_store), &config.data_dir)?
                } else {
                    MetadataStore::open(rocks_path)?
                }
            }
            #[cfg(not(feature = "slate"))]
            {
                MetadataStore::open(rocks_path)?
            }
        };

        let metadata = Arc::new(metadata);
        let identity_metadata = if config.local_replica_identities
            && config.storage == StorageMode::Local
            && metadata.is_shared()
        {
            Arc::new(MetadataStore::open_local(
                config.data_dir.join("replica-identities"),
            )?)
        } else {
            metadata.clone()
        };
        let subscriptions = SubscriptionRegistry::open(&config.data_dir)?;
        subscriptions.compact_duplicates()?;
        let flows = FlowProfileRegistry::open(&config.data_dir)?;
        let groups = GroupRegistry::open(&config.data_dir)?;
        let blob_store = crate::blob::open_blob_store(&config.data_dir, config.storage)?;
        let retention = crate::retention::RetentionStore::open(&config.data_dir)?;

        let broker = Self {
            inner: Arc::new(BrokerInner {
                log_epochs: (0..layout.shard_count)
                    .map(|_| Arc::new(LogEpoch::default()))
                    .collect(),
                config,
                metadata,
                identity_metadata,
                subscriptions,
                flows,
                groups,
                blob_store,
                payload_policy: RwLock::new(PayloadPolicy::External),
                retention,
                layout,
                shards: RwLock::new(HashMap::new()),
                shard_open_locks: Mutex::new(HashMap::new()),
                dedup_reservations: Arc::new(DedupReservations::default()),
                owned_completions: tokio_util::task::TaskTracker::new(),
                accepting_owned: std::sync::atomic::AtomicBool::new(true),
                owned_id: Uuid::new_v4(),
                shard_leader_check: Mutex::new(None),
                quorum_gating: std::sync::atomic::AtomicBool::new(false),
                quorum: RwLock::new(HashMap::new()),
                #[cfg(feature = "slate")]
                slate,
            }),
        };
        broker.recover_log_rewrites()?;
        broker.initialize_dlq_counts()?;
        Ok(broker)
    }

    /// Recover ephemeral counters once at shard open, never on a count request.
    fn initialize_dlq_counts(&self) -> Result<(), BrokerError> {
        if self.inner.config.storage != StorageMode::Local {
            return Ok(());
        }
        if self.layout().is_physical() {
            for partition in 0..self.shard_count() {
                if self
                    .layout()
                    .physical_shard_dir(&self.inner.config.data_dir, partition)
                    .exists()
                {
                    self.shard_handle("__physical", partition)?;
                }
            }
        } else {
            for topic in self.list_dlq_topics_on_disk()? {
                for partition in 0..self.shard_count() {
                    if partition_dir(
                        &self.inner.config.data_dir,
                        &self.tenant(),
                        &topic,
                        partition,
                    )?
                    .exists()
                    {
                        self.shard_handle(&topic, partition)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Node-local retained DLQ counts, plus recovery completeness. No WAL reads.
    pub fn dlq_counts(&self) -> (HashMap<String, usize>, bool) {
        let tenant = self.tenant();
        let mut counts = HashMap::new();
        let mut ready = self.inner.config.storage == StorageMode::Local;
        for handle in self
            .inner
            .shards
            .read()
            .values()
            .flat_map(|partitions| partitions.values())
        {
            match handle.dlq_counts() {
                Some(shard_counts) => {
                    for ((record_tenant, topic), count) in shard_counts {
                        if record_tenant == tenant {
                            *counts.entry(topic).or_insert(0) += count;
                        }
                    }
                }
                None => ready = false,
            }
        }
        (counts, ready)
    }

    /// Where message bodies live. Local-WAL multi-node cells must use
    /// [`PayloadPolicy::InlineOnly`] (set once at startup, before writes):
    /// a node-local blob would exist only on the accepting node.
    pub fn set_payload_policy(&self, policy: PayloadPolicy) {
        *self.inner.payload_policy.write() = policy;
    }

    pub fn payload_policy(&self) -> PayloadPolicy {
        *self.inner.payload_policy.read()
    }

    /// Reject a body the payload policy cannot store, before any append,
    /// blob write or idempotency reservation.
    pub fn check_publish_body(&self, req: &PublishRequest) -> Result<(), BrokerError> {
        self.check_body(&req.payload, req.payload_encoding.as_deref())
    }

    /// [`Broker::check_publish_body`] for a body that is not yet a request
    /// (schedules, crons, fan-out commands).
    pub fn check_body(&self, payload: &str, encoding: Option<&str>) -> Result<(), BrokerError> {
        Ok(self
            .payload_policy()
            .check(decoded_body_len(payload, encoding))?)
    }

    /// Slate + cluster: only the elected shard leader may append; generation is the fence token.
    pub fn set_shard_leader_check(&self, check: ShardFenceFn) {
        self.set_shard_authority_check(Arc::new(move |partition| {
            check(partition).map(|generation| (generation, Uuid::nil()))
        }));
    }

    pub fn set_shard_authority_check(&self, check: ShardAuthorityFn) {
        *self.inner.shard_leader_check.lock() = Some(check);
    }

    /// Returns fence generation when this node may append (cluster).
    fn require_shard_fence(
        &self,
        partition: u32,
    ) -> Result<(Option<u64>, Option<broker_storage::MetadataValidator>), BrokerError> {
        self.inner.metadata.check_ready()?;
        self.inner.identity_metadata.check_ready()?;
        let local = self.log_metadata_validator(partition)?;
        if let Some(check) = self.inner.shard_leader_check.lock().as_ref() {
            match check(partition) {
                Some(authority) => {
                    let check = check.clone();
                    let validate: broker_storage::MetadataValidator = Arc::new(move || {
                        local()?;
                        if check(partition) != Some(authority) {
                            return Err(broker_storage::IndexError::Validation(
                                "shard publish ownership changed".into(),
                            ));
                        }
                        Ok(())
                    });
                    Ok((Some(authority.0), Some(validate)))
                }
                None => Err(BrokerError::NotShardLeader(partition)),
            }
        } else {
            Ok((None, Some(local)))
        }
    }

    /// Pure writer-time guard: no metadata/WAL reads and no generation adoption.
    pub fn log_metadata_validator(
        &self,
        partition: u32,
    ) -> Result<broker_storage::MetadataValidator, BrokerError> {
        use std::sync::atomic::Ordering;
        let state = self
            .inner
            .log_epochs
            .get(partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid log partition".into()))?
            .clone();
        let epoch = state.epoch.load(Ordering::Acquire);
        let validate: broker_storage::MetadataValidator = Arc::new(move || {
            if state.rewriting.load(Ordering::Acquire)
                || state.epoch.load(Ordering::Acquire) != epoch
            {
                return Err(broker_storage::IndexError::Validation(
                    "local log incarnation changed".into(),
                ));
            }
            Ok(())
        });
        validate()?;
        Ok(validate)
    }

    fn begin_log_rewrite(&self, partition: u32) -> Result<LogRewrite, BrokerError> {
        use std::sync::atomic::Ordering;
        let state = self
            .inner
            .log_epochs
            .get(partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid log partition".into()))?
            .clone();
        let admission = state.admission.lock();
        if state.rewriting.swap(true, Ordering::AcqRel) {
            return Err(broker_storage::IndexError::Validation(
                "local log rewrite in progress".into(),
            )
            .into());
        }
        state
            .epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                epoch.checked_add(1)
            })
            .map_err(|_| BrokerError::InvalidConfig("local log epoch exhausted".into()))?;
        state.gc_indexed.lock().clear();
        drop(admission);
        Ok(LogRewrite {
            state,
            release: true,
        })
    }

    /// Register before the first replica append, then retain through the
    /// durable metadata continuation. An uncommitted drop dirties GC readiness.
    pub fn pending_replica_identity(
        &self,
        partition: u32,
        offset: u64,
    ) -> Result<PendingIdentity, BrokerError> {
        let state = self
            .inner
            .log_epochs
            .get(partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid identity partition".into()))?
            .clone();
        let admission = state.admission.lock();
        self.log_metadata_validator(partition)?()?;
        let lease = Self::register_identity(&state, offset);
        drop(admission);
        Ok(lease)
    }

    fn register_identity(state: &Arc<LogEpoch>, offset: u64) -> PendingIdentity {
        *state.pending_metadata.lock().entry(offset).or_default() += 1;
        PendingIdentity {
            state: state.clone(),
            offset,
            epoch: state.epoch.load(std::sync::atomic::Ordering::Acquire),
            durable: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Drop in-memory Slate handles when this node loses shard leadership (CP6b.1).
    pub fn evict_slate_partitions(&self, partitions: &[u32]) {
        if self.inner.config.storage == StorageMode::Slate && !partitions.is_empty() {
            #[cfg(feature = "slate")]
            {
                let opening: Vec<_> = self
                    .inner
                    .shard_open_locks
                    .lock()
                    .iter()
                    .filter(|((_, partition), _)| partitions.contains(partition))
                    .map(|(key, gate)| (key.clone(), gate.clone()))
                    .collect();
                for ((namespace, partition), gate) in opening {
                    let _opening = gate.lock();
                    if let Some(topic_shards) = self.inner.shards.write().get_mut(&namespace) {
                        topic_shards.remove(&partition);
                    }
                }
            }
        }
    }

    pub fn create_flow_profile(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .create(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    pub fn upsert_flow_profile_by_key(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .upsert_by_key(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    /// Reuse matching profile by key+limits, else create/update.
    pub fn ensure_flow_profile_by_key(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<crate::flows::FlowProfile, BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self
            .inner
            .flows
            .ensure_by_key(&self.tenant(), key, parallelism, rate, period_secs)?)
    }

    pub fn ensure_flow_profile_by_key_with_change(
        &self,
        key: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
    ) -> Result<(crate::flows::FlowProfile, bool), BrokerError> {
        broker_proto::validate_flow_key(&key)?;
        Ok(self.inner.flows.ensure_by_key_with_change(
            &self.tenant(),
            key,
            parallelism,
            rate,
            period_secs,
        )?)
    }

    pub fn get_flow_profile_by_key(
        &self,
        key: &str,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.get_by_key(&self.tenant(), key)?)
    }

    pub fn list_flow_profiles(&self) -> Result<Vec<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.list(&self.tenant())?)
    }

    pub fn list_queues(&self) -> Result<Vec<Subscription>, BrokerError> {
        Ok(self.inner.subscriptions.list_all(&self.tenant())?)
    }

    pub fn delete_flow_profile(&self, id: Uuid) -> Result<crate::flows::FlowProfile, BrokerError> {
        Ok(self.inner.flows.delete(&self.tenant(), id)?)
    }

    pub fn get_flow_profile(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::flows::FlowProfile>, BrokerError> {
        Ok(self.inner.flows.get_by_id(&self.tenant(), id)?)
    }

    pub fn create_subscription(
        &self,
        req: CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionResponse, BrokerError> {
        let tenant_id = self.tenant();
        broker_proto::validate_queue_name(&req.topic)?;
        let sub = self.inner.subscriptions.create(
            &tenant_id,
            req.topic.clone(),
            req.url.clone(),
            req.secret,
            req.parallelism,
            req.default_max_retries,
            req.retry_backoff,
        )?;
        Ok(CreateSubscriptionResponse {
            id: sub.id,
            topic: sub.topic,
            url: sub.url,
            parallelism: sub.parallelism,
            default_max_retries: sub.default_max_retries,
        })
    }

    pub fn subscriptions_for_topic(&self, topic: &str) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self
            .inner
            .subscriptions
            .unique_for_topic(&tenant_id, topic)?)
    }

    pub fn list_endpoints(&self) -> Result<Vec<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.list_all(&tenant_id)?)
    }

    pub fn delete_endpoint(&self, id: Uuid) -> Result<Subscription, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.delete(&tenant_id, id)?)
    }

    pub fn get_queue(&self, queue: &str) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.get_by_name(&tenant_id, queue)?)
    }

    pub fn get_queue_by_id(&self, id: Uuid) -> Result<Option<Subscription>, BrokerError> {
        let tenant_id = self.tenant();
        Ok(self.inner.subscriptions.get_by_id(&tenant_id, id)?)
    }

    pub fn partition_high_watermark(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        Ok(self.shard_handle(topic, partition)?.high_watermark()?)
    }

    pub fn committed_hwm(&self, topic: &str, partition: u32) -> Result<u64, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        Ok(handle.committed_hwm())
    }

    /// Gate dispatch on the quorum watermark. Called once at startup for a
    /// multi-node cell on local WAL storage; single node and Slate keep the
    /// local committed watermark (their durability is local / shared storage).
    pub fn enable_quorum_gating(&self) {
        self.inner
            .quorum_gating
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn quorum_gating(&self) -> bool {
        self.inner
            .quorum_gating
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn quorum_watermark(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Arc<crate::quorum::QuorumWatermark>, BrokerError> {
        let namespace = self.inner.layout.storage_namespace(topic).to_string();
        let key = (namespace, partition);
        if let Some(existing) = self.inner.quorum.read().get(&key) {
            return Ok(Arc::clone(existing));
        }
        let mut map = self.inner.quorum.write();
        if let Some(existing) = map.get(&key) {
            return Ok(Arc::clone(existing));
        }
        let dir = if self.inner.layout.is_physical() {
            self.inner
                .layout
                .physical_shard_dir(&self.inner.config.data_dir, partition)
        } else {
            partition_dir(
                &self.inner.config.data_dir,
                &self.tenant(),
                topic,
                partition,
            )?
        };
        std::fs::create_dir_all(&dir)
            .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?;
        let watermark = Arc::new(
            crate::quorum::QuorumWatermark::open(&dir)
                .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?,
        );
        map.insert(key, Arc::clone(&watermark));
        Ok(watermark)
    }

    /// Exclusive end of the prefix dispatch may deliver: the committed
    /// watermark, capped by the quorum watermark when quorum gating is on.
    pub fn dispatch_hwm(&self, topic: &str, partition: u32) -> Result<u64, BrokerError> {
        let committed = self.committed_hwm(topic, partition)?;
        if !self.quorum_gating() {
            return Ok(committed);
        }
        Ok(committed.min(self.quorum_watermark(topic, partition)?.get()))
    }

    /// Quorum watermark (the committed watermark when gating is off).
    pub fn quorum_hwm(&self, topic: &str, partition: u32) -> Result<u64, BrokerError> {
        if !self.quorum_gating() {
            return self.committed_hwm(topic, partition);
        }
        Ok(self.quorum_watermark(topic, partition)?.get())
    }

    /// Record that `[start, end)` of this log is durable on a replication
    /// quorum; persists and returns the contiguous quorum watermark.
    pub fn record_quorum_commit(
        &self,
        topic: &str,
        partition: u32,
        start: u64,
        end: u64,
    ) -> Result<u64, BrokerError> {
        self.record_quorum_commit_checked(topic, partition, start, end, || Ok(()))
    }

    /// Serialize the original authority check and watermark mutation against
    /// local truncation. A queued old ACK cannot mark replacement offsets as
    /// quorum-proven after their WAL incarnation has been rewritten.
    pub fn record_quorum_commit_checked(
        &self,
        topic: &str,
        partition: u32,
        start: u64,
        end: u64,
        validate: impl FnOnce() -> Result<(), broker_storage::IndexError>,
    ) -> Result<u64, BrokerError> {
        let state = self
            .inner
            .log_epochs
            .get(partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid quorum partition".into()))?;
        let _admission = state.admission.lock();
        self.log_metadata_validator(partition)?()?;
        validate()?;
        self.quorum_watermark(topic, partition)?
            .record(start, end)
            .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))
    }

    /// Wait for the contiguous quorum prefix, not just this record's own ACKs.
    /// The caller supplies its timeout and leadership/generation fence.
    pub async fn wait_quorum_visible(
        &self,
        topic: &str,
        partition: u32,
        end: u64,
    ) -> Result<(), BrokerError> {
        if self.quorum_gating() {
            self.quorum_watermark(topic, partition)?.wait_for(end).await;
        }
        Ok(())
    }

    /// Identity-less completion markers count only below this offset: above
    /// it the offset may hold a different record than the one completed.
    pub fn legacy_completion_trusted_below(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        if !self.quorum_gating() {
            return Ok(u64::MAX);
        }
        Ok(self
            .quorum_watermark(topic, partition)?
            .trusted_legacy_below())
    }

    /// Pin the legacy-marker trust point at the current quorum watermark
    /// (on taking over a shard, before its tail is re-verified).
    pub fn freeze_legacy_completion_trust(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<(), BrokerError> {
        if self.quorum_gating() {
            self.quorum_watermark(topic, partition)?
                .freeze_legacy_trust();
        }
        Ok(())
    }

    /// Quorum watermarks loaded so far, with their lag behind the local
    /// committed watermark.
    pub fn quorum_status(&self) -> Vec<QuorumStatus> {
        if !self.quorum_gating() {
            return Vec::new();
        }
        let loaded: Vec<_> = self
            .inner
            .quorum
            .read()
            .iter()
            .map(|((namespace, partition), wm)| (namespace.clone(), *partition, Arc::clone(wm)))
            .collect();
        let mut out: Vec<QuorumStatus> = loaded
            .into_iter()
            .map(|(namespace, partition, wm)| {
                let quorum_hwm = wm.get();
                let committed_hwm = self
                    .committed_hwm(&namespace, partition)
                    .unwrap_or(quorum_hwm);
                QuorumStatus {
                    lag: committed_hwm.saturating_sub(quorum_hwm),
                    namespace,
                    partition,
                    quorum_hwm,
                    committed_hwm,
                    pending_ranges: wm.pending_ranges(),
                }
            })
            .collect();
        out.sort_by(|a, b| (&a.namespace, a.partition).cmp(&(&b.namespace, b.partition)));
        out
    }

    /// Mark `offset` complete for the delivery of `message_id`.
    pub fn mark_dispatch_complete_for(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
        message_id: Uuid,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.mark_dispatch_complete_for(
            tenant_id,
            subscription_id,
            partition,
            offset,
            message_id,
        )?;
        Ok(())
    }

    /// Persist a completion and its gap-aware cursor in one durable metadata
    /// epoch. The caller serializes the lane and proves `next_offset` against
    /// its quorum-visible log before invoking this method.
    pub fn commit_dispatch_progress(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
        message_id: Uuid,
        next_offset: u64,
    ) -> Result<(), BrokerError> {
        let mut batch = broker_storage::MetadataWriteBatch::default();
        batch.mark_dispatch_complete_for(tenant_id, subscription_id, partition, offset, message_id);
        batch.set_dispatch_offset(tenant_id, subscription_id, partition, next_offset);
        self.inner.metadata.write_batch(batch)?;
        Ok(())
    }

    pub fn supports_metadata_group_commit(&self) -> bool {
        self.inner.metadata.supports_group_commit()
    }

    pub fn commit_metadata_batch(&self, batch: MetadataWriteBatch) -> Result<(), BrokerError> {
        self.inner.metadata.write_batch(batch)?;
        Ok(())
    }

    pub async fn commit_metadata_batch_grouped<G: Send + 'static>(
        &self,
        batch: MetadataWriteBatch,
        held: G,
    ) -> Result<broker_storage::MetadataWriteReceipt, BrokerError> {
        Ok(self.inner.metadata.write_batch_grouped(batch, held).await?)
    }

    /// Whether the record `message_id` at `offset` of `topic` was delivered or
    /// dead-lettered. Identity-aware: a completion marker for a different
    /// message (offset reused after failover) is dropped and reports false; an
    /// identity-less legacy marker counts only below the legacy trust point.
    /// Offsets below the lane cursor are complete (cursors never pass the
    /// quorum watermark, whose prefix has a stable identity).
    pub fn is_delivery_complete(
        &self,
        topic: &str,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
        message_id: Uuid,
    ) -> Result<bool, BrokerError> {
        if offset < self.dispatch_offset(tenant_id, subscription_id, partition)? {
            return Ok(true);
        }
        Ok(
            match self.inner.metadata.dispatch_completion(
                tenant_id,
                subscription_id,
                partition,
                offset,
            )? {
                broker_storage::DispatchCompletion::Missing => false,
                broker_storage::DispatchCompletion::Message(id) if id == message_id => true,
                broker_storage::DispatchCompletion::Message(stale_id) => {
                    // A late completion/query may itself refer to a previous
                    // occupant of this offset. Only the current WAL identity
                    // is allowed to classify a different marker as stale.
                    let current = self
                        .shard_handle(topic, partition)?
                        .read_range_metadata(partition, offset, 1)?;
                    if !current.iter().any(|message| {
                        message.offset == offset
                            && message.id == message_id
                            && message.topic == topic
                    }) {
                        return Ok(false);
                    }
                    self.inner.metadata.clear_dispatch_complete_for(
                        tenant_id,
                        subscription_id,
                        partition,
                        offset,
                        stale_id,
                    )?;
                    offset
                        < self.inner.metadata.dispatch_offset(
                            tenant_id,
                            subscription_id,
                            partition,
                        )?
                        || match self.inner.metadata.dispatch_completion(
                            tenant_id,
                            subscription_id,
                            partition,
                            offset,
                        )? {
                            broker_storage::DispatchCompletion::Message(id) => id == message_id,
                            broker_storage::DispatchCompletion::Legacy => {
                                offset < self.legacy_completion_trusted_below(topic, partition)?
                            }
                            broker_storage::DispatchCompletion::Missing => false,
                        }
                }
                broker_storage::DispatchCompletion::Legacy => {
                    offset < self.legacy_completion_trusted_below(topic, partition)?
                }
            },
        )
    }

    /// All lane cursors of `partition` as `(tenant, lane, next_offset)`.
    pub fn dispatch_offsets_for_partition(
        &self,
        partition: u32,
    ) -> Result<Vec<(String, String, u64)>, BrokerError> {
        Ok(self
            .inner
            .metadata
            .dispatch_offsets_for_partition(partition)?)
    }

    /// Every lane cursor as `(tenant, lane, partition, next_offset)`.
    pub fn all_dispatch_offsets(&self) -> Result<Vec<(String, String, u32, u64)>, BrokerError> {
        Ok(self.inner.metadata.dispatch_offsets()?)
    }

    /// Encoded frames of every record in the log namespace of `topic` from
    /// `from_offset`, for quorum (re-)verification. In V2 this covers every
    /// topic of the physical shard. Stops at `max_records` or `max_bytes`
    /// (at least one frame).
    pub fn namespace_replication_frames(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut cursor = from_offset;
        // Repair/export is a cold path. Read one body at a time rather than
        // materializing 1024 maximum-sized inline bodies before the byte cap
        // can be applied. At most the output plus one source frame is retained.
        while out.len() < max_records.max(1) {
            let Some(mut msg) = handle.read_range(partition, cursor, 1)?.into_iter().next() else {
                break;
            };
            cursor = msg.offset.saturating_add(1);
            self.restore_replication_key(&mut msg)?;
            let frame = encode_replication_frame(&msg)?;
            bytes = bytes.saturating_add(frame.len());
            if !out.is_empty() && bytes > max_bytes {
                break;
            }
            out.push((msg.offset, frame));
        }
        Ok(out)
    }

    /// Rebuild a bounded page of producer identities before a recovered shard
    /// accepts writes. Follower WALs retain the original keys, including across
    /// catch-up. Preserve the original timestamp rather than extending TTL on
    /// every election, and never replace a newer valid tenant-wide mapping.
    pub fn recover_dedup_page(
        &self,
        topic: &str,
        partition: u32,
        from: u64,
        target: u64,
        validator: Option<broker_storage::MetadataValidator>,
    ) -> Result<u64, BrokerError> {
        let messages = self
            .shard_handle(topic, partition)?
            .read_raw_metadata(partition, from, 256)?;
        let mut batch = MetadataWriteBatch::default();
        if let Some(validate) = validator {
            batch.validate_with(validate);
        }
        let mut next = from;
        for message in messages
            .into_iter()
            .take_while(|message| message.offset < target)
        {
            next = message.offset.saturating_add(1);
            let Some(key) = message.idempotency_key else {
                continue;
            };
            let timestamp = message
                .dedup_inserted_at_ms
                .unwrap_or_else(|| u64::try_from(message.published_at_ms).unwrap_or(1))
                .max(1);
            let existing = self
                .inner
                .identity_metadata
                .get_dedup(&message.tenant_id, &key)?;
            let expected = existing.as_ref().map(|entry| entry.message_id);
            if let Some(existing) = existing {
                if existing.message_id == message.id
                    || (existing.inserted_at_ms > timestamp
                        && matches!(
                            self.verify_dedup_hit_with_visibility(
                                &message.tenant_id,
                                &existing,
                                false
                            )?,
                            DedupVerification::Verified(_)
                        ))
                {
                    continue;
                }
            }
            let mut entry = DedupEntry::new(message.id, message.offset, partition, message.topic);
            entry.inserted_at_ms = timestamp;
            batch.compare_put_dedup(message.tenant_id, key, entry, expected);
        }
        self.inner.identity_metadata.write_batch(batch)?;
        Ok(if next == from {
            target
        } else {
            next.min(target)
        })
    }

    /// Original decoded replica headers survive WAL GC during this wait. CAS
    /// retries reread metadata only while the pinned local/owner epoch is valid.
    pub fn persist_replica_keys(
        &self,
        partition: u32,
        headers: Vec<(u64, LogRecord)>,
        validate: broker_storage::MetadataValidator,
    ) -> Result<(), BrokerError> {
        for _ in 0..8 {
            validate()?;
            let mut batch = MetadataWriteBatch::default();
            batch.validate_with(validate.clone());
            for (offset, header) in &headers {
                let Some(key) = &header.idempotency_key else {
                    continue;
                };
                let current = self
                    .inner
                    .identity_metadata
                    .get_dedup(&header.tenant_id, key)?;
                if current
                    .as_ref()
                    .is_some_and(|entry| entry.message_id == header.id)
                {
                    continue;
                }
                let timestamp = header
                    .dedup_inserted_at_ms
                    .unwrap_or_else(|| u64::try_from(header.published_at_ms).unwrap_or(1))
                    .max(1);
                if let Some(entry) = &current {
                    if entry.inserted_at_ms > timestamp
                        && matches!(
                            self.verify_dedup_hit_with_visibility(&header.tenant_id, entry, false)?,
                            DedupVerification::Verified(_)
                        )
                    {
                        continue;
                    }
                }
                let mut entry =
                    DedupEntry::new(header.id, *offset, partition, header.topic.clone());
                entry.inserted_at_ms = timestamp;
                batch.compare_put_dedup(
                    &header.tenant_id,
                    key,
                    entry,
                    current.map(|entry| entry.message_id),
                );
            }
            match self.inner.identity_metadata.write_batch(batch) {
                Err(error) if error.is_dedup_conflict() => continue,
                result => {
                    result?;
                    validate()?;
                    return Ok(());
                }
            }
        }
        Err(broker_storage::IndexError::DedupConflict.into())
    }

    /// Block until `offset` is on the shard's committed high watermark.
    pub async fn wait_committed(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<u64, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        handle
            .wait_committed(offset)
            .await
            .map_err(BrokerError::Storage)?;
        Ok(handle.committed_hwm())
    }

    pub fn mark_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.mark_dispatch_complete(
            tenant_id,
            subscription_id,
            partition,
            offset,
        )?;
        Ok(())
    }

    pub fn is_dispatch_complete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        Ok(self.inner.metadata.is_dispatch_complete(
            tenant_id,
            subscription_id,
            partition,
            offset,
        )?)
    }

    pub fn read_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<StoredMessage, BrokerError> {
        let batch = self
            .shard_handle(topic, partition)?
            .read_range(partition, offset, 1)?;
        batch
            .into_iter()
            .find(|message| message.offset == offset && message.topic == topic)
            .ok_or(BrokerError::Storage(
                broker_storage::LogError::OffsetNotFound(offset),
            ))
    }

    /// Async delivery read. Opening a cold shard is blocking work; warm reads
    /// only enqueue one command and await its reply from the shard actor.
    pub async fn read_message_async(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<StoredMessage, BrokerError> {
        let namespace = self.inner.layout.storage_namespace(topic);
        let warm = self
            .inner
            .shards
            .read()
            .get(namespace)
            .and_then(|partitions| partitions.get(&partition))
            .cloned();
        let handle = match warm {
            Some(handle) => handle,
            None => {
                let broker = self.clone();
                let topic = topic.to_owned();
                crate::run_blocking_io(move || broker.shard_handle(&topic, partition))
                    .await
                    .map_err(|error| {
                        BrokerError::Storage(broker_storage::LogError::Io(std::io::Error::other(
                            error.to_string(),
                        )))
                    })??
            }
        };
        handle
            .read_range_async(partition, offset, 1)
            .await?
            .into_iter()
            .find(|message| message.offset == offset && message.topic == topic)
            .ok_or(BrokerError::Storage(
                broker_storage::LogError::OffsetNotFound(offset),
            ))
    }

    pub fn dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
    ) -> Result<u64, BrokerError> {
        Ok(self
            .inner
            .metadata
            .dispatch_offset(tenant_id, subscription_id, partition)?)
    }

    pub fn set_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.set_dispatch_offset(
            tenant_id,
            subscription_id,
            partition,
            next_offset,
        )?;
        Ok(())
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    /// Explicit promotion/admin rewind. The caller must fence dispatch first;
    /// normal cursor writes never regress. Clears completion gaps atomically.
    pub fn reset_dispatch_offset(
        &self,
        tenant_id: &str,
        subscription_id: &str,
        partition: u32,
        next_offset: u64,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.reset_dispatch_offset(
            tenant_id,
            subscription_id,
            partition,
            next_offset,
        )?;
        Ok(())
    }

    pub fn metadata_ready(&self) -> bool {
        self.inner.metadata.is_ready() && self.inner.identity_metadata.is_ready()
    }

    /// Force a WAL group flush on every open shard (shutdown / tests).
    pub fn flush_wal(&self) -> Result<(), BrokerError> {
        let handles: Vec<Arc<ShardHandle>> = {
            let shards = self.inner.shards.read();
            shards.values().flat_map(|m| m.values().cloned()).collect()
        };
        for handle in handles {
            handle.flush_durable()?;
        }
        self.inner.metadata.flush_epoch()?;
        if !Arc::ptr_eq(&self.inner.metadata, &self.inner.identity_metadata) {
            self.inner.identity_metadata.flush_epoch()?;
        }
        Ok(())
    }

    /// The physical log holding `topic` on `partition`.
    pub fn log_id(&self, topic: &str, partition: u32) -> LogId {
        LogId {
            namespace: self.inner.layout.storage_namespace(topic).to_string(),
            partition,
        }
    }

    /// Local logs that exist on disk (opened lazily otherwise). V2 lists the
    /// physical shards, V1 every `topic/pN` directory of this tenant.
    pub fn retained_logs(&self) -> Vec<LogId> {
        if self.inner.config.storage != StorageMode::Local {
            return Vec::new();
        }
        let layout = &self.inner.layout;
        let data_dir = &self.inner.config.data_dir;
        if layout.is_physical() {
            return (0..layout.shard_count)
                .filter(|shard| layout.physical_shard_dir(data_dir, *shard).exists())
                .map(|partition| LogId {
                    namespace: layout.storage_namespace("").to_string(),
                    partition,
                })
                .collect();
        }
        let mut logs = Vec::new();
        let tenant_dir = data_dir.join("partitions").join(self.tenant());
        let Ok(topics) = std::fs::read_dir(tenant_dir) else {
            return logs;
        };
        for topic in topics.flatten() {
            let namespace = topic.file_name().to_string_lossy().into_owned();
            let Ok(partitions) = std::fs::read_dir(topic.path()) else {
                continue;
            };
            for partition in partitions.flatten() {
                let name = partition.file_name();
                let Some(partition) = name
                    .to_str()
                    .and_then(|name| name.strip_prefix('p'))
                    .and_then(|index| index.parse::<u32>().ok())
                    .filter(|index| *index < layout.shard_count)
                else {
                    continue;
                };
                logs.push(LogId {
                    namespace: namespace.clone(),
                    partition,
                });
            }
        }
        logs.sort();
        logs
    }

    pub fn log_committed_hwm(&self, log: &LogId) -> Result<u64, BrokerError> {
        Ok(self
            .shard_handle(&log.namespace, log.partition)?
            .committed_hwm())
    }

    /// Records of one physical log from `from` (inclusive), every topic.
    pub fn read_log(
        &self,
        log: &LogId,
        from: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        Ok(self
            .shard_handle(&log.namespace, log.partition)?
            .read_range(log.partition, from, max)?)
    }

    pub fn log_retention_status(
        &self,
        log: &LogId,
        watermark: u64,
    ) -> Result<crate::shard::LogRetentionStatus, BrokerError> {
        Ok(self
            .shard_handle(&log.namespace, log.partition)?
            .retention_status(watermark)?)
    }

    /// Delete the whole sealed segments of `log` below `boundary`. Use
    /// [`crate::RetentionStore::collect`], which tombstones their payload
    /// blobs first and never passes an unsafe boundary.
    pub(crate) fn gc_log(
        &self,
        log: &LogId,
        boundary: u64,
        _lease: &GcLease<'_>,
    ) -> Result<usize, BrokerError> {
        Ok(self
            .shard_handle(&log.namespace, log.partition)?
            .gc_sealed_below(boundary)?)
    }

    /// Continue an empty or fully-collected local log at `base`.
    pub fn reset_log_base(&self, log: &LogId, base: u64) -> Result<(), BrokerError> {
        let _rewrite = self.begin_log_rewrite(log.partition)?;
        Ok(self
            .shard_handle(&log.namespace, log.partition)?
            .reset_base(base)?)
    }

    pub fn blob_store(&self) -> &BlobStore {
        &self.inner.blob_store
    }

    pub fn retention(&self) -> &crate::retention::RetentionStore {
        &self.inner.retention
    }

    /// Group-commit timer: flush shards whose interval has elapsed.
    pub fn flush_wal_if_due(&self) -> Result<(), BrokerError> {
        let handles: Vec<Arc<ShardHandle>> = {
            let shards = self.inner.shards.read();
            shards.values().flat_map(|m| m.values().cloned()).collect()
        };
        for handle in handles {
            handle.flush_if_due()?;
        }
        Ok(())
    }

    /// Normalize an engine-facing batch without appending. Existing and
    /// within-batch duplicates retain their input slots.
    ///
    /// Never waits for another request: if one holds the batch's idempotency
    /// keys, every reservation taken so far is released and
    /// [`BrokerError::IdempotencyInFlight`] is returned. Async callers should
    /// use [`Broker::prepare_publish_batch_waiting`].
    pub fn prepare_publish_batch(
        &self,
        requests: Vec<PublishRequest>,
    ) -> Result<PreparedPublishBatch, BrokerError> {
        self.prepare_publish_batch_on(requests, None)
    }

    fn prepare_publish_batch_on(
        &self,
        requests: Vec<PublishRequest>,
        forced_partition: Option<u32>,
    ) -> Result<PreparedPublishBatch, BrokerError> {
        let tenant_id = self.tenant();
        let reservations = self
            .try_reserve_batch_dedup(&tenant_id, &requests)?
            .map_err(DedupInFlight::into_error)?;
        self.prepare_reserved_batch(requests, reservations, forced_partition)
    }

    /// Like [`Broker::prepare_publish_batch`], but waits asynchronously (up to
    /// [`DEDUP_IN_FLIGHT_WAIT`]) for in-flight requests that own one of the
    /// batch's idempotency keys, so a retry resolves to the original message.
    pub async fn prepare_publish_batch_waiting(
        &self,
        mut requests: Vec<PublishRequest>,
    ) -> Result<PreparedPublishBatch, BrokerError> {
        let deadline = tokio::time::Instant::now() + DEDUP_IN_FLIGHT_WAIT;
        loop {
            let broker = self.clone();
            // Reservation verification can read the WAL and migrate or remove
            // durable metadata. Keep the entire attempt off the async worker,
            // not just payload preparation. Return ownership on conflict rather
            // than cloning the batch's payloads for each retry.
            let attempt = crate::run_blocking_io(move || {
                match broker.try_reserve_batch_dedup(&broker.tenant(), &requests)? {
                    Ok(reservations) => broker
                        .prepare_reserved_batch(requests, reservations, None)
                        .map(Ok),
                    Err(in_flight) => Ok(Err((requests, in_flight))),
                }
            })
            .await
            .map_err(blocking_task_error)??;
            match attempt {
                Ok(prepared) => return Ok(prepared),
                Err((retry_requests, in_flight)) => {
                    requests = retry_requests;
                    let key = in_flight.idempotency_key.clone();
                    if tokio::time::timeout_at(deadline, in_flight.released())
                        .await
                        .is_err()
                    {
                        return Err(BrokerError::IdempotencyInFlight(key));
                    }
                }
            }
        }
    }

    /// Resolve every distinct idempotency key of a batch without waiting. On
    /// conflict the keys acquired so far are released before returning, so a
    /// batch never holds some keys while waiting for others (no lock-order
    /// deadlock between `[A, B]` and `[B, A]`).
    fn try_reserve_batch_dedup(
        &self,
        tenant_id: &str,
        requests: &[PublishRequest],
    ) -> Result<Result<HashMap<String, ReservationOutcome>, DedupInFlight>, BrokerError> {
        let mut reservations = HashMap::new();
        for key in requests
            .iter()
            .filter_map(|request| request.idempotency_key.as_ref())
        {
            if reservations.contains_key(key) {
                continue;
            }
            match self.try_reserve_dedup(tenant_id, key)? {
                Ok(outcome) => {
                    reservations.insert(key.clone(), outcome);
                }
                Err(in_flight) => return Ok(Err(in_flight)),
            }
        }
        Ok(Ok(reservations))
    }

    fn prepare_reserved_batch(
        &self,
        requests: Vec<PublishRequest>,
        mut reservations: HashMap<String, ReservationOutcome>,
        forced_partition: Option<u32>,
    ) -> Result<PreparedPublishBatch, BrokerError> {
        let mut slots = Vec::with_capacity(requests.len());
        let mut groups: Vec<PreparedShardBatch> = Vec::new();
        let mut group_indexes: HashMap<PreparedShardKey, usize> = HashMap::new();
        let mut local_owners: HashMap<String, Uuid> = HashMap::new();

        for request in requests {
            let original_topic = request.topic.clone();
            let payload = decode_payload(&request)?;
            self.validate_publish_request(&request)?;
            let reservation = if let Some(key) = request.idempotency_key.as_ref() {
                if let Some(message_id) = local_owners.get(key) {
                    slots.push(PreparedSlot::Awaiting {
                        message_id: *message_id,
                        duplicate: true,
                        topic_override: Some(original_topic),
                    });
                    continue;
                }
                if let Some(ReservationOutcome::Existing(entry)) = reservations.get(key) {
                    let (generation, validate) = self.require_shard_fence(entry.partition)?;
                    slots.push(PreparedSlot::Existing(PublishResponse {
                        message_id: Some(entry.message_id),
                        topic: if entry.topic.is_empty() {
                            original_topic
                        } else {
                            entry.topic.clone()
                        },
                        partition: Some(entry.partition),
                        offset: Some(entry.offset),
                        duplicate: true,
                        scheduled: None,
                        commit_epoch: None,
                        replication_frame: None,
                        publish_fence: validate.map(|validate| PublishFence {
                            generation,
                            validate,
                        }),
                    }));
                    continue;
                }
                match reservations.remove(key) {
                    Some(ReservationOutcome::Acquired(token)) => Some(token),
                    _ => {
                        return Err(BrokerError::InvalidConfig(format!(
                            "idempotency key {key} was not reserved for this batch"
                        )))
                    }
                }
            } else {
                None
            };

            let record = self.prepare_record(request, payload, reservation, forced_partition)?;
            let message_id = record.header.id;
            if let Some(key) = record.header.idempotency_key.as_ref() {
                local_owners.insert(key.clone(), message_id);
            }
            slots.push(PreparedSlot::Awaiting {
                message_id,
                duplicate: false,
                topic_override: None,
            });
            let key = PreparedShardKey {
                partition: record.partition,
                storage_namespace: self
                    .inner
                    .layout
                    .storage_namespace(&record.topic)
                    .to_string(),
                fence_generation: record.fence_generation,
            };
            let group_index = match group_indexes.get(&key) {
                Some(index) => *index,
                None => {
                    let index = groups.len();
                    group_indexes.insert(key.clone(), index);
                    groups.push(PreparedShardBatch {
                        key: key.clone(),
                        records: Vec::new(),
                    });
                    index
                }
            };
            groups[group_index].records.push(record);
        }

        Ok(PreparedPublishBatch {
            slots,
            shard_batches: groups,
            deferred_dedup: PendingDedup::default(),
        })
    }

    /// Append one prepared physical-shard group with one backend batch write.
    pub fn append_prepared_batch(
        &self,
        batch: PreparedShardBatch,
    ) -> Result<AppendedShardBatch, BrokerError> {
        self.inner.metadata.check_ready()?;
        if batch.records.is_empty() {
            return Ok(AppendedShardBatch {
                appended: Vec::new(),
                records: Vec::new(),
                identity_lease: None,
                fence_generation: batch.key.fence_generation,
            });
        }
        let partition = batch.key.partition;
        let _admission = self.inner.log_epochs[partition as usize].admission.lock();
        if let (Some(ours), Some(check)) = (
            batch.key.fence_generation,
            self.inner.shard_leader_check.lock().as_ref(),
        ) {
            if let Some((stored, _)) = check(partition) {
                if stored != ours {
                    return Err(broker_storage::LogError::StaleFence { ours, stored }.into());
                }
            }
        }
        for record in &batch.records {
            if let Some(validate) = &record.validator {
                validate()?;
            }
        }
        let fence_generation = batch.key.fence_generation;
        let topic = batch.records[0].topic.clone();
        // Registration is under append admission and precedes the native WAL
        // operation, including partial append failure and caller cancellation.
        let identity_lease = if batch
            .records
            .iter()
            .any(|record| record.header.idempotency_key.is_some())
        {
            Some(Self::register_identity(
                &self.inner.log_epochs[partition as usize],
                self.shard_handle(&topic, partition)?.high_watermark()?,
            ))
        } else {
            None
        };
        let mut finalize = Vec::with_capacity(batch.records.len());
        let mut items = Vec::with_capacity(batch.records.len());
        for record in batch.records {
            finalize.push(FinalizeRecord {
                message_id: record.header.id,
                topic: record.topic,
                partition: record.partition,
                idempotency_key: record.header.idempotency_key.clone(),
                reservation: record.reservation,
                validator: record.validator,
            });
            items.push((record.header, record.payload));
        }
        let appended = self.shard_handle(&topic, partition)?.append_batch(
            partition,
            items,
            fence_generation,
        )?;
        Ok(AppendedShardBatch {
            appended,
            records: finalize,
            identity_lease,
            fence_generation,
        })
    }

    /// Finalize one appended shard group and atomically publish all dedup
    /// mappings for that group in one RocksDB WriteBatch.
    ///
    /// With `commit_dedup = false` nothing is recorded and the reservations
    /// stay held; collect them with [`PreparedPublishBatch::take_pending_dedup`]
    /// and pass them to [`Broker::commit_publish_dedup`] after quorum.
    pub fn finalize_prepared_batch(
        &self,
        prepared: &mut PreparedPublishBatch,
        mut batch: AppendedShardBatch,
        commit_dedup: bool,
    ) -> Result<(), BrokerError> {
        self.inner.metadata.check_ready()?;
        if batch.appended.len() != batch.records.len() {
            return Err(BrokerError::InvalidConfig(
                "appended batch result count mismatch".into(),
            ));
        }
        if batch
            .records
            .iter()
            .zip(&batch.appended)
            .any(|(record, (stored, _))| {
                record.message_id != stored.id || record.partition != stored.partition
            })
        {
            return Err(BrokerError::InvalidConfig(
                "appended batch result identity mismatch".into(),
            ));
        }

        if commit_dedup {
            // The HTTP path already waited for local/quorum commit. Synchronous
            // Broker callers must establish the same local durability ordering
            // before publishing a mapping and releasing its reservation, or a
            // concurrent retry would correctly reject that not-yet-visible hit.
            // Keyless records publish no mapping, so they keep the configured
            // fsync policy (group commit stays asynchronous).
            let has_mapping = batch
                .records
                .iter()
                .any(|record| record.idempotency_key.is_some());
            if let (true, Some(last), Some(first)) =
                (has_mapping, batch.last_offset(), batch.records.first())
            {
                let shard = self.shard_handle(&first.topic, first.partition)?;
                if shard.committed_hwm() <= last {
                    shard.flush_durable()?;
                }
            }
            let tenant_id = self.tenant();
            let mut metadata = MetadataWriteBatch::default();
            for (record, (stored, _)) in batch.records.iter().zip(&batch.appended) {
                if let Some(key) = record.idempotency_key.as_ref() {
                    if let Some(validate) = &record.validator {
                        metadata.validate_with(validate.clone());
                    }
                    metadata.compare_put_dedup(
                        tenant_id.clone(),
                        key,
                        DedupEntry::new(
                            stored.id,
                            stored.offset,
                            stored.partition,
                            stored.topic.clone(),
                        ),
                        None,
                    );
                }
            }
            self.inner.identity_metadata.write_batch(metadata)?;
            if let Some(lease) = &batch.identity_lease {
                lease.mark_durable();
            }
        }

        let mut responses = HashMap::with_capacity(batch.appended.len());
        if !commit_dedup {
            if let Some(lease) = batch.identity_lease.take() {
                prepared.deferred_dedup.identity_leases.push(lease);
            }
        }
        for (record, (stored, frame)) in batch.records.iter_mut().zip(batch.appended) {
            let publish_fence = record.validator.clone().map(|validate| PublishFence {
                generation: batch.fence_generation,
                validate,
            });
            if let Some(reservation) = record.reservation.take() {
                if commit_dedup {
                    reservation.release();
                } else {
                    // Keep the key reserved until replication quorum resolves;
                    // releasing here would let a retry append a second copy.
                    prepared.deferred_dedup.entries.push((
                        reservation,
                        DedupEntry::new(
                            stored.id,
                            stored.offset,
                            stored.partition,
                            record.topic.clone(),
                        ),
                    ));
                    if let Some(validate) = record.validator.take() {
                        prepared.deferred_dedup.validators.push(validate);
                    }
                }
            }
            responses.insert(
                record.message_id,
                PublishResponse {
                    message_id: Some(stored.id),
                    topic: record.topic.clone(),
                    partition: Some(record.partition),
                    offset: Some(stored.offset),
                    duplicate: false,
                    scheduled: None,
                    commit_epoch: None,
                    replication_frame: Some(frame),
                    publish_fence,
                },
            );
        }

        for slot in &mut prepared.slots {
            let PreparedSlot::Awaiting {
                message_id,
                duplicate,
                topic_override,
            } = slot
            else {
                continue;
            };
            let Some(owner) = responses.get(message_id) else {
                continue;
            };
            let mut response = owner.clone();
            response.duplicate = *duplicate;
            if let Some(topic) = topic_override.take() {
                response.topic = topic;
            }
            if response.duplicate {
                response.replication_frame = None;
            }
            *slot = PreparedSlot::Final(response);
        }
        Ok(())
    }

    /// Load external blob bytes into `msg.payload` when `payload_ref_json` is set.
    pub fn hydrate_message_payload(&self, msg: &mut StoredMessage) -> Result<(), BrokerError> {
        hydrate_payload(
            &self.inner.blob_store,
            &mut msg.payload,
            msg.payload_ref_json.as_deref(),
        )?;
        Ok(())
    }

    /// Write inline or external payload bytes directly to a delivery writer.
    /// This avoids hydrating large blob-backed messages into `StoredMessage`.
    pub fn stream_message_payload_to_writer<W: std::io::Write + Unpin>(
        &self,
        msg: &StoredMessage,
        writer: &mut W,
    ) -> Result<u64, BrokerError> {
        let Some(reference_json) = msg.payload_ref_json.as_deref() else {
            writer
                .write_all(&msg.payload)
                .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?;
            return Ok(msg.payload.len() as u64);
        };
        if !msg.payload.is_empty() {
            writer
                .write_all(&msg.payload)
                .map_err(|error| BrokerError::Storage(broker_storage::LogError::Io(error)))?;
            return Ok(msg.payload.len() as u64);
        }
        let reference: broker_payload::PayloadRef =
            serde_json::from_str(reference_json).map_err(|error| {
                BrokerError::Payload(broker_payload::PayloadError::Store(error.to_string()))
            })?;
        self.inner
            .blob_store
            .get_blob_to_writer(&reference, writer)
            .map_err(BrokerError::Payload)
    }

    /// Publish one record. Never blocks on another request's idempotency
    /// reservation: a concurrent same-key publish fails fast with
    /// [`BrokerError::IdempotencyInFlight`]. Async callers that want to wait
    /// for the owner and receive its result should use [`Broker::publish_async`].
    pub fn publish(&self, req: PublishRequest) -> Result<PublishResponse, BrokerError> {
        let prepared = self.prepare_publish_batch(vec![req])?;
        let (response, _) = self.publish_prepared(prepared, true)?;
        Ok(response)
    }

    /// [`Broker::publish`] that waits asynchronously for an in-flight request
    /// holding the same idempotency key, then returns its duplicate result.
    pub async fn publish_async(&self, req: PublishRequest) -> Result<PublishResponse, BrokerError> {
        if self.inner.config.storage != StorageMode::Slate {
            let broker = self.clone();
            return self
                .publish_with_completion(req, move |response, pending| async move {
                    if !pending.is_empty() {
                        if let (Some(partition), Some(offset)) =
                            (response.partition, response.offset)
                        {
                            broker
                                .wait_committed(&response.topic, partition, offset)
                                .await?;
                        }
                        broker.commit_publish_dedup_async(pending).await?;
                    }
                    Ok(response)
                })
                .await?;
        }
        let prepared = self.prepare_publish_batch_waiting(vec![req]).await?;
        let broker = self.clone();
        let (response, _) = crate::run_blocking_io(move || broker.publish_prepared(prepared, true))
            .await
            .map_err(blocking_task_error)??;
        Ok(response)
    }

    /// Append, then run an owned asynchronous durability/finalization step.
    /// The admitted append closure hands its reservation directly to the
    /// completion task before returning. A cancelled caller cannot drop it
    /// between the WAL append and durable dedup publication. No blocking-I/O
    /// permit is retained while completion awaits local/quorum commit.
    pub async fn publish_with_completion<T, F, Fut>(
        &self,
        req: PublishRequest,
        complete: F,
    ) -> Result<T, BrokerError>
    where
        T: Send + 'static,
        F: FnOnce(PublishResponse, PendingDedup) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let prepared = self.prepare_publish_batch_waiting(vec![req]).await?;
        let broker = self.clone();
        let tenant = self.tenant();
        let runtime = tokio::runtime::Handle::current();
        let owned = self.owned_completion_token()?;
        let owner_id = self.inner.owned_id;
        let completion = crate::run_blocking_io(move || {
            let (response, pending) = broker.publish_prepared(prepared, false)?;
            Ok::<_, BrokerError>(runtime.spawn(OWNED_BROKER.scope(
                owner_id,
                crate::scope_tenant(tenant, async move {
                    let _owned = owned;
                    complete(response, pending).await
                }),
            )))
        })
        .await
        .map_err(blocking_task_error)??;
        completion.await.map_err(blocking_task_error)
    }

    /// Append without recording idempotency until replication quorum succeeds.
    /// Append a dead-letter record to `partition` — the shard of the source
    /// message, which this node leads while it dead-letters — instead of the
    /// hash placement, so the DLQ write depends only on the source shard's own
    /// leadership fence and quorum. Internal: only DLQ topics, partition
    /// bounds and the leadership fence are checked. Idempotency stays reserved
    /// in the returned [`PendingDedup`]: pass it to
    /// [`Broker::commit_publish_dedup`] once the record is durable on a quorum.
    /// A concurrent request holding the same key fails fast.
    pub fn publish_to_shard(
        &self,
        mut req: PublishRequest,
        partition: u32,
    ) -> Result<(PublishResponse, PendingDedup), BrokerError> {
        if !is_dlq_topic(&req.topic) {
            return Err(BrokerError::InvalidConfig(format!(
                "publish_to_shard is only for dead-letter topics, not {}",
                req.topic
            )));
        }
        if partition >= self.inner.layout.shard_count {
            return Err(BrokerError::InvalidConfig(format!(
                "unknown shard {partition}; layout has {} shards",
                self.inner.layout.shard_count
            )));
        }
        req.delay_ms = None;
        let prepared = self.prepare_publish_batch_on(vec![req], Some(partition))?;
        self.publish_prepared(prepared, false)
    }

    /// The key stays reserved in the returned [`PendingDedup`] so a retry
    /// during replication waits for this request instead of appending again.
    pub async fn publish_defer_dedup(
        &self,
        req: PublishRequest,
    ) -> Result<(PublishResponse, PendingDedup), BrokerError> {
        let prepared = self.prepare_publish_batch_waiting(vec![req]).await?;
        let broker = self.clone();
        crate::run_blocking_io(move || broker.publish_prepared(prepared, false))
            .await
            .map_err(blocking_task_error)?
    }

    fn publish_prepared(
        &self,
        mut prepared: PreparedPublishBatch,
        commit_dedup: bool,
    ) -> Result<(PublishResponse, PendingDedup), BrokerError> {
        for shard_batch in prepared.take_shard_batches() {
            let appended = self.append_prepared_batch(shard_batch)?;
            self.finalize_prepared_batch(&mut prepared, appended, commit_dedup)?;
        }
        let pending = prepared.take_pending_dedup();
        let response = prepared
            .finish()?
            .pop()
            .ok_or_else(|| BrokerError::InvalidConfig("empty single publish result".into()))?;
        Ok((response, pending))
    }

    fn prepare_record(
        &self,
        mut req: PublishRequest,
        payload: Vec<u8>,
        reservation: Option<ReservationToken>,
        forced_partition: Option<u32>,
    ) -> Result<PreparedRecord, BrokerError> {
        let tenant_id = self.tenant();
        let message_id = Uuid::new_v4();
        // Large payloads write their blob before the append. Retention deletes
        // blobs of collected or purged records; a blob orphaned by a failed
        // append or a crash before it is not reclaimed.
        let (log_payload, payload_ref_json) = prepare_for_log_with(
            &self.inner.blob_store,
            self.payload_policy(),
            &tenant_id,
            message_id,
            payload,
        )?;

        let _ = req.delay_ms.take();

        let mut queue_sub: Option<Subscription> = None;
        let (topic, queue_id, destination_url, destination_secret) = if is_dlq_topic(&req.topic) {
            (req.topic.clone(), None, None, None)
        } else if let Some(dest) = req.destination.take() {
            if let Some(id) = dest.queue_id {
                queue_sub = self.get_queue_by_id(id)?;
            }
            let topic = queue_sub
                .as_ref()
                .map(|queue| queue.topic.clone())
                .unwrap_or_else(|| DIRECT_TOPIC.to_string());
            (topic, dest.queue_id, Some(dest.url), Some(dest.secret))
        } else if let (Some(url), Some(secret)) = (req.url.take(), req.secret.take()) {
            (DIRECT_TOPIC.to_string(), None, Some(url), Some(secret))
        } else {
            let queue = if let Some(id) = req.queue_id {
                self.get_queue_by_id(id)?
                    .ok_or(BrokerError::QueueNotFound(id.to_string()))?
            } else {
                self.inner
                    .subscriptions
                    .get_by_name(&tenant_id, &req.topic)?
                    .ok_or_else(|| BrokerError::QueueNotFound(req.topic.clone()))?
            };
            queue_sub = Some(queue.clone());
            (
                queue.topic.clone(),
                Some(queue.id),
                Some(queue.url),
                Some(queue.secret),
            )
        };

        req.topic = topic.clone();
        let queue_job = queue_sub.is_some() && req.group_member_id.is_none();
        let flow_spec = if queue_job {
            let q = queue_sub.as_ref().unwrap();
            crate::flow::queue_delivery_flow(&req.routing_key, q.parallelism, &q.topic)
        } else {
            self.resolve_flow_spec(&req)?
        };
        let (max_retries, retry_backoff) =
            resolve_delivery_retry(&req, queue_sub.as_ref(), &self.inner.config.retry_defaults);

        // API-selected identity must survive leader selection and forwarding.
        if req.routing_key.trim().is_empty() && req.routing_id.is_none() {
            req.routing_id = Some(message_id);
        }
        let partition_key = req.shard_routing_key();
        let partition = forced_partition.unwrap_or_else(|| {
            self.inner
                .layout
                .assign(&tenant_id, &req.topic, &partition_key)
        });
        let (fence_gen, validator) = self.require_shard_fence(partition)?;

        let http = HttpDeliverySpec::merge(
            req.method.clone(),
            req.headers.clone(),
            req.sign,
            req.request
                .clone()
                .map(|r| HttpDeliverySpec::merge(r.method, r.headers, r.sign, None)),
        );
        http.validate().map_err(BrokerError::InvalidConfig)?;

        let header = LogRecord {
            dedup_inserted_at_ms: None,
            id: message_id,
            tenant_id: tenant_id.clone(),
            topic: req.topic.clone(),
            routing_key: req.routing_key.clone(),
            idempotency_key: req.idempotency_key.clone(),
            published_at_ms: Utc::now().timestamp_millis(),
            priority: normalize_priority(req.priority),
            flow_parallelism: flow_spec.as_ref().and_then(|f| f.parallelism),
            flow_key: flow_spec.as_ref().and_then(|f| f.key.clone()),
            flow_rate: flow_spec.as_ref().and_then(|f| f.rate),
            flow_period_secs: flow_spec.as_ref().and_then(|f| f.period_secs),
            queue_id,
            group_id: req.group_id,
            group_member_id: req.group_member_id,
            flow_profile_id: if queue_job { None } else { req.flow_id },
            destination_url,
            destination_secret,
            max_retries,
            retry_backoff: Some(retry_backoff),
            http_method: Some(http.method.clone()),
            http_headers_json: http.headers_json(),
            http_sign: Some(http.sign),
            payload_ref_json,
        };
        Ok(PreparedRecord {
            validator,
            topic: req.topic,
            partition,
            header,
            payload: log_payload,
            fence_generation: fence_gen,
            reservation,
        })
    }

    fn validate_publish_request(&self, req: &PublishRequest) -> Result<(), BrokerError> {
        if !req.topic.is_empty()
            && !is_dlq_topic(&req.topic)
            && req.queue_id.is_none()
            && req.url.is_none()
            && req.destination.is_none()
        {
            broker_proto::validate_queue_name(&req.topic)?;
        }
        if let Some(ref flow) = req.flow {
            if let Some(ref key) = flow.key {
                broker_proto::validate_flow_key(key)?;
            }
        }
        Ok(())
    }

    /// Take the in-memory reservation first, then consult the dedup index.
    /// Dedup rows are only written by a reservation holder, so reading the
    /// index while holding the key cannot race with a concurrent commit.
    fn try_reserve_dedup(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Result<ReservationOutcome, DedupInFlight>, BrokerError> {
        let key = ReservationKey {
            tenant_id: tenant_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
        };
        let token = match self.inner.dedup_reservations.try_acquire(key) {
            Ok(token) => token,
            Err(in_flight) => return Ok(Err(in_flight)),
        };
        if let Some(mut entry) = self
            .inner
            .identity_metadata
            .get_dedup(tenant_id, idempotency_key)?
        {
            match self.verify_dedup_hit(tenant_id, &entry)? {
                DedupVerification::Unverified => {
                    return Err(BrokerError::DedupQuorumPending(idempotency_key.to_owned()));
                }
                DedupVerification::Verified(topic) => {
                    if entry.topic.is_empty() && !topic.is_empty() {
                        entry.topic = topic;
                        self.inner.identity_metadata.put_dedup(
                            tenant_id,
                            idempotency_key,
                            &entry,
                        )?;
                    }
                    token.release();
                    return Ok(Ok(ReservationOutcome::Existing(entry)));
                }
                DedupVerification::Stale => {}
            }
            // Provably stale (its record is absent or belongs to another
            // message): drop it and publish as new under the held key.
            self.inner
                .identity_metadata
                .delete_dedup(tenant_id, idempotency_key)?;
        }
        Ok(Ok(ReservationOutcome::Acquired(token)))
    }

    /// End of the prefix a dedup hit may point into: the persisted quorum
    /// watermark in quorum-gated cells (D2), else the local durable HWM.
    fn dedup_visible_hwm(&self, topic: &str, partition: u32) -> Result<u64, BrokerError> {
        self.dispatch_hwm(topic, partition)
    }

    fn verify_dedup_hit(
        &self,
        tenant: &str,
        entry: &DedupEntry,
    ) -> Result<DedupVerification, BrokerError> {
        self.verify_dedup_hit_with_visibility(tenant, entry, true)
    }

    fn verify_dedup_hit_with_visibility(
        &self,
        tenant: &str,
        entry: &DedupEntry,
        quorum: bool,
    ) -> Result<DedupVerification, BrokerError> {
        let legacy_topic_unknown = entry.topic.is_empty() && !self.inner.layout.is_physical();
        let topics = if !entry.topic.is_empty() {
            vec![entry.topic.clone()]
        } else if self.inner.layout.is_physical() {
            vec![DIRECT_TOPIC.to_string()]
        } else {
            #[cfg(feature = "slate")]
            if let Some(slate) = &self.inner.slate {
                broker_storage::slate_topics(slate.object_store.clone(), tenant)?
            } else {
                self.local_dedup_topics(tenant)?
            }
            #[cfg(not(feature = "slate"))]
            self.local_dedup_topics(tenant)?
        };
        let mut ambiguous_reclaimed_topic = false;
        let mut unverified = false;
        for topic in topics {
            let shard = self.shard_handle(&topic, entry.partition)?;
            // D4's safe retained base proves a reclaimed record was committed
            // and dispatched. Keep its durable mapping throughout the TTL.
            if entry.offset < shard.base_offset()? {
                if legacy_topic_unknown {
                    ambiguous_reclaimed_topic = true;
                    continue;
                }
                return Ok(DedupVerification::Verified(topic));
            }
            let visible = if quorum {
                self.dedup_visible_hwm(&topic, entry.partition)?
            } else {
                shard.committed_hwm()
            };
            if entry.offset >= visible {
                // A locally durable record above the contiguous quorum prefix
                // is not stale. Repair may still be proving an earlier gap.
                unverified |=
                    quorum && self.quorum_gating() && entry.offset < shard.committed_hwm();
                continue;
            }
            match shard.read_raw_metadata(entry.partition, entry.offset, 1) {
                Ok(messages)
                    if messages.iter().any(|message| {
                        message.offset == entry.offset
                            && message.id == entry.message_id
                            && message.tenant_id == tenant
                    }) =>
                {
                    return Ok(DedupVerification::Verified(messages[0].topic.clone()))
                }
                Ok(_) | Err(broker_storage::LogError::OffsetNotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if ambiguous_reclaimed_topic {
            // Keep pre-upgrade behavior when retention makes a topic-less
            // legacy mapping unverifiable. Do not invent a topic to migrate.
            broker_storage::record_legacy_unverifiable_dedup_hit();
            return Ok(DedupVerification::Verified(String::new()));
        }
        Ok(if unverified {
            DedupVerification::Unverified
        } else {
            DedupVerification::Stale
        })
    }

    fn local_dedup_topics(&self, tenant: &str) -> Result<Vec<String>, BrokerError> {
        let tenant = broker_proto::sanitize_path_segment(tenant)?;
        let path = self.inner.config.data_dir.join("partitions").join(tenant);
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(broker_storage::LogError::Io(error).into()),
        };
        let mut topics = Vec::new();
        for entry in entries {
            let entry = entry.map_err(broker_storage::LogError::Io)?;
            if entry
                .file_type()
                .map_err(broker_storage::LogError::Io)?
                .is_dir()
            {
                topics.push(
                    entry.file_name().into_string().map_err(|_| {
                        BrokerError::InvalidConfig("non-UTF8 topic directory".into())
                    })?,
                );
            }
        }
        topics.sort();
        Ok(topics)
    }

    /// Record deferred idempotency mappings after replication quorum, then
    /// release their keys. On error the keys are released unrecorded.
    pub fn commit_publish_dedup(&self, pending: PendingDedup) -> Result<(), BrokerError> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut batch = MetadataWriteBatch::default();
        for validate in &pending.validators {
            batch.validate_with(validate.clone());
        }
        for (token, entry) in &pending.entries {
            batch.compare_put_dedup(
                token.key.tenant_id.clone(),
                token.key.idempotency_key.clone(),
                entry.clone(),
                None,
            );
        }
        self.inner.identity_metadata.write_batch(batch)?;
        for lease in &pending.identity_leases {
            lease.mark_durable();
        }
        for (token, _) in pending.entries {
            token.release();
        }
        Ok(())
    }

    /// Await the local metadata writer directly, without parking an admitted
    /// blocking thread. The queued intent owns the reservations until durable.
    pub async fn commit_publish_dedup_async(
        &self,
        pending: PendingDedup,
    ) -> Result<(), BrokerError> {
        if pending.is_empty() {
            return Ok(());
        }
        if !self.inner.identity_metadata.supports_group_commit() {
            let broker = self.clone();
            return crate::run_blocking_io(move || broker.commit_publish_dedup(pending))
                .await
                .map_err(blocking_task_error)?;
        }
        let mut batch = MetadataWriteBatch::default();
        for validate in &pending.validators {
            batch.validate_with(validate.clone());
        }
        for (token, entry) in &pending.entries {
            batch.compare_put_dedup(
                token.key.tenant_id.clone(),
                token.key.idempotency_key.clone(),
                entry.clone(),
                None,
            );
        }
        let settled: Vec<_> = pending
            .identity_leases
            .iter()
            .map(|lease| lease.durable.clone())
            .collect();
        let _receipt = self
            .inner
            .identity_metadata
            .write_batch_grouped(batch, pending)
            .await?;
        for durable in settled {
            durable.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// Apply a replicated log frame on a follower (CP7a). Disabled in slate mode (CP6b.1).
    pub fn append_replicated_frame(
        &self,
        topic: &str,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<PublishResponse, BrokerError> {
        self.append_replica_frame_inner(topic, partition, frame, expected_offset, None)
    }

    /// Live follower admission retains this receipt until metadata durability.
    /// Untracked raw catch-up instead invalidates GC coverage before appending.
    pub fn append_replicated_frame_pinned(
        &self,
        topic: &str,
        partition: u32,
        frame: &[u8],
        offset: u64,
        identity: &PendingIdentity,
    ) -> Result<PublishResponse, BrokerError> {
        self.append_replica_frame_inner(topic, partition, frame, Some(offset), Some(identity))
    }

    fn append_replica_frame_inner(
        &self,
        topic: &str,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
        identity: Option<&PendingIdentity>,
    ) -> Result<PublishResponse, BrokerError> {
        if self.inner.config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                "slate mode: frame replication disabled; durability is on shared object storage"
                    .into(),
            )));
        }
        let state = self
            .inner
            .log_epochs
            .get(partition as usize)
            .ok_or_else(|| BrokerError::InvalidConfig("invalid replica partition".into()))?;
        let _admission = state.admission.lock();
        self.log_metadata_validator(partition)?()?;
        if let Some(identity) = identity {
            if !Arc::ptr_eq(state, &identity.state)
                || identity.epoch != state.epoch.load(std::sync::atomic::Ordering::Acquire)
                || identity.durable.load(std::sync::atomic::Ordering::Acquire)
                || expected_offset.is_none_or(|offset| identity.offset > offset)
            {
                return Err(broker_storage::IndexError::Validation(
                    "replica append no longer owns its original identity receipt".into(),
                )
                .into());
            }
        } else {
            // Do this before the native operation: a partial/uncertain append
            // also needs reindexing. No metadata receipt promises to finish it.
            state
                .dirty_revision
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            state.gc_indexed.lock().clear();
        }
        let stored = self.shard_handle(topic, partition)?.append_raw_frame(
            partition,
            frame,
            expected_offset,
        )?;
        Ok(PublishResponse {
            message_id: Some(stored.id),
            topic: topic.to_string(),
            partition: Some(partition),
            offset: Some(stored.offset),
            duplicate: false,
            scheduled: None,
            commit_epoch: None,
            replication_frame: None,
            publish_fence: None,
        })
    }

    /// Publish without `delay_ms` (used by schedule worker and DLQ).
    pub fn publish_immediate(
        &self,
        mut req: PublishRequest,
    ) -> Result<PublishResponse, BrokerError> {
        req.delay_ms = None;
        self.publish(req)
    }

    /// DLQ partition directories present on disk (`jobs.__dlq`, `__direct.__dlq`, …).
    pub fn list_dlq_topics_on_disk(&self) -> Result<Vec<String>, BrokerError> {
        let tenant_dir = self
            .inner
            .config
            .data_dir
            .join("partitions")
            .join(self.tenant());
        let mut topics = Vec::new();
        let Ok(entries) = std::fs::read_dir(&tenant_dir) else {
            return Ok(topics);
        };
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if is_dlq_topic(&name) {
                    topics.push(name);
                }
            }
        }
        topics.sort();
        Ok(topics)
    }

    /// Scan a topic from offset 0 (used for DLQ inspection; push-only broker).
    pub fn list_topic_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        let mut messages = Vec::new();
        for partition in 0..self.inner.config.partitions {
            let remaining = max_messages.saturating_sub(messages.len());
            if remaining == 0 {
                break;
            }
            messages.extend(self.list_topic_messages_from(topic, partition, 0, remaining)?);
        }
        messages.sort_by_key(|m| (m.partition, m.offset));
        messages.truncate(max_messages);
        Ok(messages)
    }

    pub fn partition_count(&self, _topic: &str) -> Result<u32, BrokerError> {
        Ok(self.inner.config.partitions)
    }

    /// Scan at most 512 records across partitions. An empty page can still have
    /// a continuation when a shared shard contains only other logical topics.
    pub fn list_topic_messages_page(
        &self,
        topic: &str,
        start: (u32, u64),
        limit: usize,
    ) -> Result<TopicMessagePage, BrokerError> {
        let (mut partition, mut offset) = start;
        if partition >= self.shard_count() {
            return Err(BrokerError::InvalidConfig(
                "invalid listing partition".into(),
            ));
        }
        let limit = limit.clamp(1, 1000);
        let mut scanned = 0;
        let mut messages = Vec::new();
        let mut payload_bytes = 0usize;
        while partition < self.shard_count() && scanned < 512 && messages.len() < limit {
            if self.inner.config.storage == StorageMode::Local {
                let path = if self.layout().is_physical() {
                    self.layout()
                        .physical_shard_dir(&self.inner.config.data_dir, partition)
                } else {
                    partition_dir(
                        &self.inner.config.data_dir,
                        &self.tenant(),
                        topic,
                        partition,
                    )?
                };
                if !path.exists() {
                    partition += 1;
                    offset = 0;
                    continue;
                }
            }
            let handle = self.shard_handle(topic, partition)?;
            let batch = handle.read_range(partition, offset, 4.min(512 - scanned))?;
            if batch.is_empty() {
                partition += 1;
                offset = 0;
                continue;
            }
            for message in batch {
                offset = message.offset.saturating_add(1);
                scanned += 1;
                if message.topic == topic {
                    payload_bytes = payload_bytes.saturating_add(message.payload.len());
                    messages.push(message);
                }
                if messages.len() >= limit || payload_bytes >= 8 * 1024 * 1024 {
                    break;
                }
            }
            if offset >= handle.high_watermark()? {
                partition += 1;
                offset = 0;
            }
            if payload_bytes >= 8 * 1024 * 1024 {
                break;
            }
        }
        Ok(TopicMessagePage {
            messages,
            next: (partition < self.shard_count()).then_some((partition, offset)),
        })
    }

    pub fn get_topic_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Option<StoredMessage>, BrokerError> {
        Ok(self
            .shard_handle(topic, partition)?
            .read_range(partition, offset, 1)?
            .into_iter()
            .find(|message| message.offset == offset && message.topic == topic))
    }

    /// Page messages for one partition starting at `from_offset` (inclusive).
    pub fn list_topic_messages_from(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, BrokerError> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        let handle = self.shard_handle(topic, partition)?;
        // An empty topic on a physical (V2) shard reads the whole shard log,
        // which is what replica repair needs.
        let whole_shard = topic.is_empty() && self.inner.layout.is_physical();
        let mut cursor = from_offset;
        let mut out = Vec::with_capacity(max_messages.min(256));
        while out.len() < max_messages {
            let remaining = max_messages - out.len();
            let scan = remaining.saturating_mul(2).clamp(64, 1024);
            let batch = handle.read_range(partition, cursor, scan)?;
            let Some(last_offset) = batch.last().map(|message| message.offset) else {
                break;
            };
            cursor = last_offset.saturating_add(1);
            out.extend(
                batch
                    .into_iter()
                    .filter(|message| whole_shard || message.topic == topic)
                    .take(remaining),
            );
            if cursor >= handle.high_watermark()? {
                break;
            }
        }
        Ok(out)
    }

    /// Read at most `max_records` shard records from `from_offset` (inclusive)
    /// and keep those belonging to `topic`. Unlike [`Broker::list_topic_messages_from`]
    /// the work is bounded by records read, not by matches, so a scan over a
    /// shared V2 shard cannot read the whole shard to find a few matches.
    /// Returns the matches and the offset to resume from (`from_offset` when
    /// nothing was read).
    pub fn scan_topic_messages(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_records: usize,
    ) -> Result<(Vec<StoredMessage>, u64), BrokerError> {
        self.scan_topic_records(topic, partition, from_offset, max_records, true)
    }

    /// Same bounded scan as [`Self::scan_topic_messages`], with verified
    /// metadata only. Empty bodies here must never be used for delivery.
    pub fn scan_topic_metadata(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_records: usize,
    ) -> Result<(Vec<StoredMessage>, u64), BrokerError> {
        self.scan_topic_records(topic, partition, from_offset, max_records, false)
    }

    fn scan_topic_records(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_records: usize,
        keep_payload: bool,
    ) -> Result<(Vec<StoredMessage>, u64), BrokerError> {
        if max_records == 0 {
            return Ok((Vec::new(), from_offset));
        }
        let handle = self.shard_handle(topic, partition)?;
        let batch = if keep_payload {
            handle.read_range(partition, from_offset, max_records)?
        } else {
            handle.read_range_metadata(partition, from_offset, max_records)?
        };
        let next = batch
            .last()
            .map(|message| message.offset.saturating_add(1))
            .unwrap_or(from_offset);
        Ok((
            batch
                .into_iter()
                .filter(|message| message.topic == topic)
                .collect(),
            next,
        ))
    }

    /// Encoded frames from `from_offset` (inclusive) for follower catch-up.
    pub fn replication_frames(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, BrokerError> {
        self.replication_frames_bounded(topic, partition, from_offset, max_messages, usize::MAX)
    }

    /// [`Broker::replication_frames`] that also stops before `max_bytes` of
    /// frames, always returning at least one frame when any is available, so
    /// catch-up of large inline bodies advances page by page.
    pub fn replication_frames_bounded(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, BrokerError> {
        let mut out: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut bytes = 0usize;
        let mut cursor = from_offset;
        while out.len() < max_messages {
            let page = 1; // enforce the byte bound before loading another body
            let msgs = self.list_topic_messages_from(topic, partition, cursor, page)?;
            let Some(last) = msgs.last().map(|message| message.offset) else {
                break;
            };
            cursor = last.saturating_add(1);
            for mut msg in msgs {
                self.restore_replication_key(&mut msg)?;
                let frame = encode_replication_frame(&msg)?;
                if !out.is_empty() && bytes.saturating_add(frame.len()) > max_bytes {
                    return Ok(out);
                }
                bytes = bytes.saturating_add(frame.len());
                out.push((msg.offset, frame));
                if out.len() >= max_messages {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Follower reconciliation: drop this replica's records at or above
    /// `offset`, keeping the cut bytes in a sidecar (see
    /// `PartitionLog::truncate_to`). Returns the new log end.
    pub fn truncate_replica_log(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        generation: u64,
    ) -> Result<u64, BrokerError> {
        if self.inner.config.storage == StorageMode::Slate {
            return Err(BrokerError::Storage(
                broker_storage::LogError::TruncateUnsupported(
                    "slate mode: frame replication disabled".into(),
                ),
            ));
        }
        self.rewrite_log_tail(topic, partition, offset, generation)
    }

    /// Drop a primary-queue record from the partition log (not used for `*. __dlq` topics).
    pub fn purge_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        if is_dlq_topic(topic) {
            return Ok(false);
        }
        self.purge_offset_and_blob(topic, partition, offset)
    }

    /// Remove a dead-letter record (operator cleanup from the panel / API).
    pub fn purge_dlq_message(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        if !is_dlq_topic(topic) {
            return Ok(false);
        }
        self.purge_offset_and_blob(topic, partition, offset)
    }

    /// Tombstone one record and queue its payload blob (if any) for deletion:
    /// retention never sees purged records again.
    fn purge_offset_and_blob(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<bool, BrokerError> {
        let handle = self.shard_handle(topic, partition)?;
        let reference = handle
            .read_range(partition, offset, 1)?
            .into_iter()
            .find(|message| message.offset == offset)
            .and_then(|message| crate::retention::payload_reference(&message));
        let removed = handle.purge_offset(offset)?;
        if removed {
            if let Some(reference) = reference {
                self.inner.retention.tombstone_blobs(vec![reference])?;
            }
        }
        Ok(removed)
    }

    /// Tombstone DLQ records. `older_than_ms = None` deletes every message on the topic;
    /// `Some(cutoff)` keeps messages with `published_at_ms` after that instant.
    pub fn purge_dlq_topic(
        &self,
        topic: &str,
        older_than_ms: Option<i64>,
    ) -> Result<usize, BrokerError> {
        if !is_dlq_topic(topic) {
            return Ok(0);
        }
        let mut deleted = 0usize;
        let mut next = Some((0, 0));
        while let Some(position) = next {
            let page = self.list_topic_messages_page(topic, position, 256)?;
            next = page.next;
            // One durable tombstone write per partition per page, not per record.
            let mut eligible: std::collections::BTreeMap<u32, Vec<u64>> =
                std::collections::BTreeMap::new();
            for msg in page.messages {
                let keep = match older_than_ms {
                    None => true,
                    Some(cutoff) => msg.published_at_ms > 0 && msg.published_at_ms <= cutoff,
                };
                if keep {
                    eligible.entry(msg.partition).or_default().push(msg.offset);
                }
            }
            for (partition, offsets) in eligible {
                deleted += self
                    .shard_handle(topic, partition)?
                    .purge_offsets(offsets)?;
            }
        }
        Ok(deleted)
    }

    /// Resolve the routing identity and storage topic before choosing a leader.
    pub fn canonicalize_routing(&self, req: &mut PublishRequest) -> Result<(), BrokerError> {
        req.ensure_routing_id();
        if is_dlq_topic(&req.topic) {
            return Ok(());
        }
        if let Some(destination) = &req.destination {
            req.topic = match destination.queue_id {
                Some(id) => self
                    .get_queue_by_id(id)?
                    .map(|queue| queue.topic)
                    .unwrap_or_else(|| DIRECT_TOPIC.into()),
                None => DIRECT_TOPIC.into(),
            };
        } else if req.url.is_some() && req.secret.is_some() {
            req.topic = DIRECT_TOPIC.into();
        } else if let Some(id) = req.queue_id {
            req.topic = self
                .get_queue_by_id(id)?
                .ok_or_else(|| BrokerError::QueueNotFound(id.to_string()))?
                .topic;
        }
        Ok(())
    }

    /// Freeze flow limits and queue destination from this broker's catalog before forwarding to a shard leader.
    pub fn prepare_for_cluster_forward(&self, req: &mut PublishRequest) -> Result<(), BrokerError> {
        let tenant_id = self.tenant();
        if req.flow.is_none() {
            if let Some(id) = req.flow_id {
                if let Some(profile) = self.inner.flows.get_by_id(&tenant_id, id)? {
                    let mut spec = profile.to_spec();
                    if spec.key.is_none() && !req.routing_key.is_empty() {
                        spec.key = Some(req.routing_key.clone());
                    }
                    req.flow = Some(spec);
                }
            }
        }
        if req.destination.is_none() && req.url.is_none() {
            let queue = if let Some(id) = req.queue_id {
                self.inner.subscriptions.get_by_id(&tenant_id, id)?
            } else if !req.topic.is_empty() && req.topic != DIRECT_TOPIC {
                self.inner
                    .subscriptions
                    .get_by_name(&tenant_id, &req.topic)?
            } else {
                None
            };
            if let Some(q) = queue {
                req.destination = Some(DestinationSnapshot {
                    queue_id: Some(q.id),
                    url: q.url,
                    secret: q.secret,
                });
            }
        }
        Ok(())
    }

    pub fn upsert_flow_profile(
        &self,
        profile: crate::flows::FlowProfile,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.flows.upsert(profile)?)
    }

    pub fn upsert_subscription_catalog(&self, sub: Subscription) -> Result<(), BrokerError> {
        Ok(self.inner.subscriptions.upsert(sub)?)
    }

    pub fn create_group(&self, name: String) -> Result<crate::groups::DispatchGroup, BrokerError> {
        Ok(self.inner.groups.create_group(&self.tenant(), name)?)
    }

    pub fn list_groups(&self) -> Result<Vec<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self.inner.groups.list_groups(&self.tenant())?)
    }

    pub fn get_group(&self, id: Uuid) -> Result<Option<crate::groups::DispatchGroup>, BrokerError> {
        Ok(self.inner.groups.get_group(&self.tenant(), id)?)
    }

    pub fn delete_group(&self, id: Uuid) -> Result<crate::groups::DispatchGroup, BrokerError> {
        Ok(self.inner.groups.delete_group(&self.tenant(), id)?)
    }

    pub fn upsert_group_catalog(
        &self,
        group: crate::groups::DispatchGroup,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.groups.upsert_group(group)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_group_member(
        &self,
        group_id: Uuid,
        name: String,
        url: String,
        secret: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
        flow_key: Option<String>,
    ) -> Result<crate::groups::GroupMember, BrokerError> {
        Ok(self.inner.groups.add_member(
            &self.tenant(),
            group_id,
            name,
            url,
            secret,
            parallelism,
            rate,
            period_secs,
            flow_key,
        )?)
    }

    pub fn list_group_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<crate::groups::GroupMember>, BrokerError> {
        Ok(self.inner.groups.list_members(&self.tenant(), group_id)?)
    }

    pub fn get_group_member(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::groups::GroupMember>, BrokerError> {
        Ok(self.inner.groups.get_member(&self.tenant(), id)?)
    }

    pub fn delete_group_member(&self, id: Uuid) -> Result<crate::groups::GroupMember, BrokerError> {
        Ok(self.inner.groups.delete_member(&self.tenant(), id)?)
    }

    pub fn upsert_group_member_catalog(
        &self,
        member: crate::groups::GroupMember,
    ) -> Result<(), BrokerError> {
        Ok(self.inner.groups.upsert_member(member)?)
    }

    pub fn active_group_members(
        &self,
        group_id: Uuid,
    ) -> Result<Vec<crate::groups::GroupMember>, BrokerError> {
        Ok(self.inner.groups.active_members(&self.tenant(), group_id)?)
    }

    /// Build a publish request for one group member (caller runs `publish`).
    pub fn group_member_publish_request(
        &self,
        group_id: Uuid,
        member: &crate::groups::GroupMember,
        base: &PublishRequest,
    ) -> PublishRequest {
        let routing_key = if base.routing_key.is_empty() {
            member.id.to_string()
        } else {
            format!("{}:{}", base.routing_key, member.id)
        };
        let flow_key = member.flow_key.clone().or_else(|| {
            if base.routing_key.is_empty() {
                None
            } else {
                Some(base.routing_key.clone())
            }
        });
        let idempotency_key = base
            .idempotency_key
            .as_ref()
            .map(|k| format!("{k}:{}", member.id));
        PublishRequest {
            routing_id: None,
            topic: group_topic(group_id),
            queue_id: None,
            group_id: Some(group_id),
            group_member_id: Some(member.id),
            routing_key,
            payload: base.payload.clone(),
            payload_encoding: base.payload_encoding.clone(),
            idempotency_key,
            delay_ms: base.delay_ms,
            priority: base.priority,
            flow_id: None,
            url: Some(member.url.clone()),
            secret: Some(member.secret.clone()),
            destination: Some(DestinationSnapshot {
                queue_id: None,
                url: member.url.clone(),
                secret: member.secret.clone(),
            }),
            flow: Some(FlowSpec {
                key: flow_key,
                parallelism: Some(member.parallelism),
                rate: Some(member.rate),
                period_secs: Some(member.period_secs),
            }),
            parallelism: None,
            max_retries: base.max_retries,
            retry_backoff: base.retry_backoff.clone(),
            method: base.method.clone(),
            headers: base.headers.clone(),
            sign: base.sign,
            request: base.request.clone(),
        }
    }

    fn resolve_flow_spec(&self, req: &PublishRequest) -> Result<Option<FlowSpec>, BrokerError> {
        if let Some(ref flow) = req.flow {
            return Ok(Some(flow.clone()));
        }
        let tenant_id = self.tenant();
        if let Some(id) = req.flow_id {
            let profile = self
                .inner
                .flows
                .get_by_id(&tenant_id, id)?
                .ok_or(BrokerError::FlowProfileNotFound(id))?;
            let mut spec = profile.to_spec();
            if spec.key.is_none() && !req.routing_key.is_empty() {
                spec.key = Some(req.routing_key.clone());
            }
            return Ok(Some(spec));
        }
        Ok(req.flow.clone())
    }

    fn shard_handle(&self, topic: &str, partition: u32) -> Result<Arc<ShardHandle>, BrokerError> {
        if partition >= self.inner.layout.shard_count {
            return Err(BrokerError::InvalidConfig(format!(
                "unknown shard {partition}; layout has {} shards",
                self.inner.layout.shard_count
            )));
        }
        let namespace = self.inner.layout.storage_namespace(topic).to_string();
        if let Some(handle) = self
            .inner
            .shards
            .read()
            .get(&namespace)
            .and_then(|partitions| partitions.get(&partition))
            .cloned()
        {
            return Ok(handle);
        }

        let gate = self
            .inner
            .shard_open_locks
            .lock()
            .entry((namespace.clone(), partition))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _opening = gate.lock();
        if let Some(handle) = self
            .inner
            .shards
            .read()
            .get(&namespace)
            .and_then(|partitions| partitions.get(&partition))
            .cloned()
        {
            return Ok(handle);
        }

        let tenant_id = self.tenant();
        let backend = match self.inner.config.storage {
            StorageMode::Local => {
                let (path, wal_format) = if self.inner.layout.is_physical() {
                    (
                        self.inner
                            .layout
                            .physical_shard_dir(&self.inner.config.data_dir, partition),
                        broker_storage::WAL_FORMAT_V2,
                    )
                } else {
                    (
                        partition_dir(&self.inner.config.data_dir, &tenant_id, topic, partition)?,
                        broker_storage::WAL_FORMAT_V1,
                    )
                };
                PartitionBackend::open_local_for_shard(
                    path,
                    self.inner.config.log.clone(),
                    partition,
                    wal_format,
                )?
            }
            StorageMode::Slate => {
                if self.inner.layout.is_physical() {
                    return Err(BrokerError::InvalidConfig(
                        "physical shard layout V2 is not implemented for Slate storage".into(),
                    ));
                }
                #[cfg(feature = "slate")]
                {
                    let slate = self.inner.slate.as_ref().expect("slate env");
                    let db_path = slate_db_path(&tenant_id, topic, partition)?;
                    let tenant_seg = broker_proto::sanitize_path_segment(&tenant_id)?;
                    let topic_seg = broker_proto::sanitize_path_segment(topic)?;
                    let local_cache = slate
                        .cache_root
                        .join("local")
                        .join(tenant_seg)
                        .join(topic_seg)
                        .join(format!("p{partition}"));
                    PartitionBackend::open_slate(
                        db_path,
                        local_cache,
                        slate.object_store.clone(),
                        partition,
                        self.inner.config.log.clone(),
                    )?
                }
                #[cfg(not(feature = "slate"))]
                {
                    return Err(BrokerError::Storage(broker_storage::LogError::Slate(
                        "slate feature not compiled".into(),
                    )));
                }
            }
        };
        let handle = ShardHandle::new(backend);
        self.inner
            .shards
            .write()
            .entry(namespace)
            .or_default()
            .insert(partition, Arc::clone(&handle));
        Ok(handle)
    }

    pub fn assign_shard(&self, topic: &str, routing_key: &str) -> u32 {
        self.inner.layout.assign(&self.tenant(), topic, routing_key)
    }
}

/// Body size after `payload_encoding` is applied, without decoding.
pub fn decoded_payload_len(req: &PublishRequest) -> usize {
    decoded_body_len(&req.payload, req.payload_encoding.as_deref())
}

/// Decoded length of a (possibly base64) body, without decoding it.
pub fn decoded_body_len(payload: &str, encoding: Option<&str>) -> usize {
    match encoding {
        Some("base64") => {
            let encoded = payload.trim_end();
            let padding = encoded
                .bytes()
                .rev()
                .take_while(|byte| *byte == b'=')
                .count();
            (encoded.len() / 4 * 3 + encoded.len() % 4 * 3 / 4).saturating_sub(padding)
        }
        _ => payload.len(),
    }
}

/// Re-encode a stored record as the frame followers append.
fn encode_replication_frame(msg: &StoredMessage) -> Result<Vec<u8>, BrokerError> {
    let header = LogRecord {
        id: msg.id,
        tenant_id: msg.tenant_id.clone(),
        topic: msg.topic.clone(),
        routing_key: msg.routing_key.clone(),
        idempotency_key: msg.idempotency_key.clone(),
        published_at_ms: msg.published_at_ms,
        priority: msg.priority,
        flow_parallelism: msg.flow_parallelism,
        flow_key: msg.flow_key.clone(),
        flow_rate: msg.flow_rate,
        flow_period_secs: msg.flow_period_secs,
        queue_id: msg.queue_id,
        group_id: msg.group_id,
        group_member_id: msg.group_member_id,
        flow_profile_id: msg.flow_profile_id,
        destination_url: msg.destination_url.clone(),
        destination_secret: msg.destination_secret.clone(),
        max_retries: msg.max_retries,
        retry_backoff: msg.retry_backoff.clone(),
        http_method: msg.http_method.clone(),
        http_headers_json: msg.http_headers_json.clone(),
        http_sign: msg.http_sign,
        payload_ref_json: msg.payload_ref_json.clone(),
        dedup_inserted_at_ms: msg.dedup_inserted_at_ms,
    };
    broker_proto::encode_frame_vec(&header, &msg.payload)
        .map_err(|e| BrokerError::Storage(broker_storage::LogError::Record(e)))
}

fn decode_payload(req: &PublishRequest) -> Result<Vec<u8>, BrokerError> {
    match req.payload_encoding.as_deref() {
        Some("base64") => {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &req.payload)
                .map_err(|e| {
                    BrokerError::Storage(broker_storage::LogError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )))
                })
        }
        _ => Ok(req.payload.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::LAYOUT_V2;
    use tempfile::tempdir;

    #[test]
    fn opening_one_shard_does_not_hold_the_global_map_lock() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let namespace = broker.inner.layout.storage_namespace("orders").to_string();
        let gate = Arc::new(Mutex::new(()));
        broker
            .inner
            .shard_open_locks
            .lock()
            .insert((namespace, 0), gate.clone());
        let blocked = gate.lock();
        let other = broker.clone();
        let (started, waiting) = std::sync::mpsc::channel();
        let first = std::thread::spawn(move || {
            started.send(()).unwrap();
            other.partition_high_watermark("orders", 0).unwrap()
        });
        waiting.recv().unwrap();
        let other = broker.clone();
        let (done, completion) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            done.send(other.partition_high_watermark("orders", 1))
                .unwrap();
        });
        let result = completion.recv_timeout(std::time::Duration::from_secs(5));
        drop(blocked);
        assert_eq!(first.join().unwrap(), 0);
        second.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), 0);
    }

    #[test]
    fn publish_stores_readable_message() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();

        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "orders".into(),
                url: "http://127.0.0.1:9/hook".into(),
                secret: "sec".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let published = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: "orders".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "a".into(),
                payload: "hello".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
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
        assert!(!published.duplicate);
        let msg = broker
            .read_message(
                "orders",
                published.partition.unwrap(),
                published.offset.unwrap(),
            )
            .unwrap();
        assert_eq!(msg.payload, b"hello");
    }

    #[test]
    fn queue_key_is_fifo_plain_uses_parallelism() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "jobs".into(),
                url: "http://127.0.0.1:9/hook".into(),
                secret: "sec".into(),
                parallelism: Some(8),
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let keyed = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: "jobs".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "user-42".into(),
                payload: "k".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
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
        let keyed_msg = broker
            .read_message("jobs", keyed.partition.unwrap(), keyed.offset.unwrap())
            .unwrap();
        assert_eq!(keyed_msg.flow_parallelism, Some(1));
        assert_eq!(keyed_msg.flow_key.as_deref(), Some("user-42"));
        assert!(keyed_msg.flow_profile_id.is_none());

        let plain = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: "jobs".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: String::new(),
                payload: "p".into(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
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
        let plain_msg = broker
            .read_message("jobs", plain.partition.unwrap(), plain.offset.unwrap())
            .unwrap();
        assert_eq!(plain_msg.flow_parallelism, Some(8));
        assert_eq!(plain_msg.flow_key.as_deref(), Some("jobs"));
    }

    #[test]
    fn large_payload_stored_as_blob() {
        std::env::set_var("BETTERMQ_INLINE_MAX_BYTES", "1024");
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "big".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let body = "x".repeat(2048);
        let published = broker
            .publish(PublishRequest {
                routing_id: None,
                topic: "big".into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "rk".into(),
                payload: body.clone(),
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: None,
                secret: None,
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

        let mut msg = broker
            .read_message(
                "big",
                published.partition.unwrap(),
                published.offset.unwrap(),
            )
            .unwrap();
        assert!(msg.payload.is_empty());
        assert!(msg.payload_ref_json.is_some());
        let mut streamed = Vec::new();
        let streamed_bytes = broker
            .stream_message_payload_to_writer(&msg, &mut streamed)
            .unwrap();
        assert_eq!(streamed_bytes, body.len() as u64);
        assert_eq!(streamed, body.as_bytes());
        broker.hydrate_message_payload(&mut msg).unwrap();
        assert_eq!(msg.payload, body.as_bytes());
        std::env::remove_var("BETTERMQ_INLINE_MAX_BYTES");
    }

    #[test]
    fn idempotency_returns_same_offset() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "t".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();

        let req = PublishRequest {
            routing_id: None,
            topic: "t".into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: Some("idem-1".into()),
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: None,
            secret: None,
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        };
        let a = broker.publish(req.clone()).unwrap();
        let b = broker.publish(req).unwrap();
        assert!(!a.duplicate);
        assert!(b.duplicate);
        assert_eq!(a.offset, b.offset);
        assert_eq!(a.message_id, b.message_id);
    }

    #[test]
    fn independent_shards_append_concurrently() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker
            .create_subscription(CreateSubscriptionRequest {
                topic: "orders".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: "s".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        std::thread::scope(|s| {
            for i in 0..4 {
                let broker = broker.clone();
                s.spawn(move || {
                    for n in 0..25 {
                        broker
                            .publish(PublishRequest {
                                routing_id: None,
                                topic: "orders".into(),
                                queue_id: None,
                                group_id: None,
                                group_member_id: None,
                                routing_key: format!("lane-{i}"),
                                payload: format!("{n}"),
                                payload_encoding: None,
                                idempotency_key: None,
                                delay_ms: None,
                                priority: None,
                                flow_id: None,
                                url: None,
                                secret: None,
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
                    }
                });
            }
        });
        broker.flush_wal().unwrap();
        let mut total = 0usize;
        for p in 0..broker.config().partitions {
            total += broker
                .list_topic_messages_from("orders", p, 0, 10_000)
                .unwrap()
                .len();
        }
        assert_eq!(total, 100);
    }

    fn direct_request(
        payload: impl Into<String>,
        idempotency_key: Option<String>,
    ) -> PublishRequest {
        PublishRequest {
            routing_id: None,
            topic: String::new(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "shared-lane".into(),
            payload: payload.into(),
            payload_encoding: None,
            idempotency_key,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: Some("http://127.0.0.1:9/h".into()),
            secret: Some("s".into()),
            destination: None,
            flow: None,
            parallelism: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    fn queue_request(topic: &str, payload: &str) -> PublishRequest {
        let mut request = direct_request(payload, None);
        request.topic = topic.into();
        request.url = None;
        request.secret = None;
        request
    }

    fn open_v2_broker(path: &std::path::Path, shard_count: u32) -> Broker {
        std::fs::create_dir_all(path).unwrap();
        let layout = ShardLayout {
            version: LAYOUT_V2,
            shard_count,
            route_hash_version: broker_proto::HASH_VERSION_V2,
        };
        broker_storage::atomic_write_file(
            &ShardLayout::path(path),
            &serde_json::to_vec(&layout).unwrap(),
        )
        .unwrap();
        Broker::open(BrokerConfig::new(path.to_path_buf())).unwrap()
    }

    #[test]
    fn dlq_pages_bound_shared_shard_scans_and_counts_survive_reopen() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        execute_prepared(
            &broker,
            (0..600).map(|_| direct_request("normal", None)).collect(),
        );
        let topic = "jobs.__dlq";
        broker.publish(queue_request(topic, "dead letter")).unwrap();
        let first = broker
            .list_topic_messages_page(topic, (0, 0), usize::MAX)
            .unwrap();
        assert!(first.messages.is_empty());
        assert_eq!(first.next, Some((0, 512)));
        let second = broker
            .list_topic_messages_page(topic, first.next.unwrap(), 100)
            .unwrap();
        assert_eq!(second.messages.len(), 1);
        assert_eq!(second.next, None);
        assert_eq!(
            broker.dlq_counts(),
            (HashMap::from([(topic.to_string(), 1)]), true)
        );
        broker.flush_wal().unwrap();
        drop(broker);
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let message = broker
            .list_topic_messages_page(topic, (0, 600), 1)
            .unwrap()
            .messages
            .pop()
            .unwrap();
        assert_eq!(
            broker.dlq_counts(),
            (HashMap::from([(topic.to_string(), 1)]), true)
        );
        assert_eq!(message.offset, 600);
        assert_eq!(broker.purge_dlq_topic(topic, None).unwrap(), 1);
        assert_eq!(broker.dlq_counts().0[topic], 0);
        assert!(broker.list_topic_messages_page(topic, (1, 0), 1).is_err());
    }

    fn execute_prepared(broker: &Broker, requests: Vec<PublishRequest>) -> Vec<PublishResponse> {
        let mut prepared = broker.prepare_publish_batch(requests).unwrap();
        for shard_batch in prepared.take_shard_batches() {
            let appended = broker.append_prepared_batch(shard_batch).unwrap();
            broker
                .finalize_prepared_batch(&mut prepared, appended, true)
                .unwrap();
        }
        prepared.finish().unwrap()
    }

    #[test]
    fn unverified_dedup_survives_out_of_order_quorum_and_returns_original_identity() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        broker.enable_quorum_gating();
        broker
            .publish(direct_request("unverified prefix", None))
            .unwrap();
        let original = broker
            .publish(direct_request("original", Some("quorum-gap".into())))
            .unwrap();
        broker.flush_wal().unwrap();
        assert_eq!(original.offset, Some(1));
        assert_eq!(
            broker.record_quorum_commit(DIRECT_TOPIC, 0, 1, 2).unwrap(),
            0
        );
        for _ in 0..2 {
            assert!(matches!(
                broker.publish(direct_request("retry", Some("quorum-gap".into()))),
                Err(BrokerError::DedupQuorumPending(_))
            ));
            assert_eq!(
                broker
                    .inner
                    .metadata
                    .get_dedup(&broker.tenant(), "quorum-gap")
                    .unwrap()
                    .unwrap()
                    .message_id,
                original.message_id.unwrap()
            );
            assert_eq!(broker.partition_high_watermark(DIRECT_TOPIC, 0).unwrap(), 2);
        }
        assert_eq!(
            broker.record_quorum_commit(DIRECT_TOPIC, 0, 0, 1).unwrap(),
            2
        );
        let retry = broker
            .publish(direct_request("retry", Some("quorum-gap".into())))
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, original.message_id);
    }

    #[test]
    fn publish_fence_retains_original_authority_even_when_the_same_leader_recovers_in_the_same_term(
    ) {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        let authority = Arc::new(Mutex::new((9, Uuid::new_v4())));
        let observed = authority.clone();
        broker.set_shard_authority_check(Arc::new(move |_| Some(*observed.lock())));
        let original = broker
            .publish(direct_request("body", Some("original-authority".into())))
            .unwrap();
        let fence = original.publish_fence.as_ref().unwrap();
        assert_eq!(fence.generation(), Some(9));
        fence.validate().unwrap();
        authority.lock().1 = Uuid::new_v4();
        assert!(fence.validate().is_err());
        let retry = broker
            .publish(direct_request("retry", Some("original-authority".into())))
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, original.message_id);
        retry.publish_fence.as_ref().unwrap().validate().unwrap();
        assert!(
            fence.validate().is_err(),
            "a new retry must not reauthorize the original response"
        );
        authority.lock().0 = 10;
        assert!(retry.publish_fence.as_ref().unwrap().validate().is_err());
    }

    #[test]
    fn catchup_retains_keys_and_recovery_rebuilds_old_leader_dedup_without_extending_ttl() {
        let leader_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let leader = open_v2_broker(leader_dir.path(), 1);
        let original = leader
            .publish(direct_request("old leader", Some("old-leader-key".into())))
            .unwrap();
        leader.flush_wal().unwrap();
        let follower = open_v2_broker(follower_dir.path(), 1);
        let frames = leader
            .namespace_replication_frames(DIRECT_TOPIC, 0, 0, 256, 1024 * 1024)
            .unwrap();
        for (offset, frame) in &frames {
            follower
                .append_replicated_frame(DIRECT_TOPIC, 0, frame, Some(*offset))
                .unwrap();
        }
        follower.flush_wal().unwrap();
        assert!(follower
            .inner
            .metadata
            .get_dedup(&follower.tenant(), "old-leader-key")
            .unwrap()
            .is_none());
        follower.enable_quorum_gating();
        follower
            .record_quorum_commit(DIRECT_TOPIC, 0, 0, 1)
            .unwrap();
        assert_eq!(
            follower
                .recover_dedup_page(DIRECT_TOPIC, 0, 0, 1, None)
                .unwrap(),
            1
        );
        let entry = follower
            .inner
            .metadata
            .get_dedup(&follower.tenant(), "old-leader-key")
            .unwrap()
            .unwrap();
        assert_eq!(
            entry.inserted_at_ms,
            leader
                .read_message(DIRECT_TOPIC, 0, 0)
                .unwrap()
                .published_at_ms as u64
        );
        let retry = follower
            .publish(direct_request("retry", Some("old-leader-key".into())))
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, original.message_id);
        assert_eq!(
            follower.partition_high_watermark(DIRECT_TOPIC, 0).unwrap(),
            1
        );
        let caught_up_again = follower
            .namespace_replication_frames(DIRECT_TOPIC, 0, 0, 1, 1024 * 1024)
            .unwrap();
        assert_eq!(
            frames, caught_up_again,
            "catch-up must not erase the original WAL header"
        );
    }

    #[test]
    fn stale_dedup_identity_and_missing_or_uncommitted_records_are_misses() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        let original = broker.publish(direct_request("original", None)).unwrap();
        broker.flush_wal().unwrap();
        for (key, message_id, offset) in [
            ("wrong-id", Uuid::new_v4(), original.offset.unwrap()),
            ("missing", Uuid::new_v4(), 500),
        ] {
            broker
                .inner
                .metadata
                .put_dedup(
                    &broker.tenant(),
                    key,
                    &DedupEntry::new(message_id, offset, 0, DIRECT_TOPIC.into()),
                )
                .unwrap();
            let retry = broker
                .publish(direct_request("retry", Some(key.into())))
                .unwrap();
            assert!(!retry.duplicate);
            assert_ne!(retry.message_id, Some(message_id));
        }
    }

    #[test]
    fn legacy_topic_discovery_preserves_tenant_wide_cross_topic_retry() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        for topic in ["orders", "invoices"] {
            broker
                .create_subscription(CreateSubscriptionRequest {
                    topic: topic.into(),
                    url: "http://127.0.0.1:9/hook".into(),
                    secret: "s".into(),
                    parallelism: None,
                    default_max_retries: None,
                    retry_backoff: None,
                })
                .unwrap();
        }
        let mut request = queue_request("orders", "body");
        request.idempotency_key = Some("tenant-key".into());
        let original = broker.publish(request).unwrap();
        broker.flush_wal().unwrap();
        let mut legacy = broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "tenant-key")
            .unwrap()
            .unwrap();
        legacy.topic.clear();
        broker
            .inner
            .metadata
            .put_dedup(&broker.tenant(), "tenant-key", &legacy)
            .unwrap();
        let mut retry = queue_request("invoices", "different-body");
        retry.idempotency_key = Some("tenant-key".into());
        let response = broker.publish(retry).unwrap();
        assert!(response.duplicate);
        assert_eq!(response.message_id, original.message_id);
        assert_eq!(
            broker
                .inner
                .metadata
                .get_dedup(&broker.tenant(), "tenant-key")
                .unwrap()
                .unwrap()
                .topic,
            "orders"
        );
    }

    #[test]
    fn wal_retention_keeps_dedup_until_its_retry_window_expires() {
        let dir = tempdir().unwrap();
        let layout = ShardLayout {
            version: LAYOUT_V2,
            shard_count: 1,
            route_hash_version: broker_proto::HASH_VERSION_V2,
        };
        broker_storage::atomic_write_file(
            &ShardLayout::path(dir.path()),
            &serde_json::to_vec(&layout).unwrap(),
        )
        .unwrap();
        let mut config = BrokerConfig::new(dir.path().to_path_buf());
        config.log.segment_max_bytes = 1;
        config.log.fsync = broker_storage::FsyncMode::Always;
        let broker = Broker::open(config).unwrap();
        let original = broker
            .publish(direct_request("body", Some("retained-key".into())))
            .unwrap();
        broker.publish(direct_request("later", None)).unwrap();
        broker.flush_wal().unwrap();
        let shard = broker.shard_handle(DIRECT_TOPIC, 0).unwrap();
        assert!(shard.gc_sealed_below(2).unwrap() > 0);
        assert!(shard.base_offset().unwrap() > original.offset.unwrap());
        let response = broker
            .publish(direct_request("retry", Some("retained-key".into())))
            .unwrap();
        assert!(response.duplicate);
        assert_eq!(response.message_id, original.message_id);
    }

    #[test]
    fn legacy_unverifiable_reclaimed_hit_keeps_pre_upgrade_duplicate_behavior() {
        let dir = tempdir().unwrap();
        let mut config = BrokerConfig::new(dir.path().to_path_buf());
        config.log.segment_max_bytes = 1;
        config.log.fsync = broker_storage::FsyncMode::Always;
        let broker = Broker::open(config).unwrap();
        let original = broker
            .publish(direct_request("body", Some("legacy-gc".into())))
            .unwrap();
        let partition = original.partition.unwrap();
        let shard = broker.shard_handle(DIRECT_TOPIC, partition).unwrap();
        assert!(shard.gc_sealed_below(original.offset.unwrap() + 1).unwrap() > 0);
        let mut entry = broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "legacy-gc")
            .unwrap()
            .unwrap();
        entry.topic.clear();
        broker
            .inner
            .metadata
            .put_dedup(&broker.tenant(), "legacy-gc", &entry)
            .unwrap();
        let before = broker_storage::shared_metadata_telemetry().legacy_unverifiable_dedup_hits;
        let response = broker
            .publish(direct_request("retry", Some("legacy-gc".into())))
            .unwrap();
        assert!(response.duplicate);
        assert_eq!(response.message_id, original.message_id);
        assert!(
            broker_storage::shared_metadata_telemetry().legacy_unverifiable_dedup_hits > before
        );
        assert!(broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "legacy-gc")
            .unwrap()
            .unwrap()
            .topic
            .is_empty());
    }

    #[test]
    fn shared_dedup_survives_process_exit_before_reply() {
        const CHILD: &str = "BETTERMQ_TEST_BROKER_SHARED_CRASH_DIR";
        if let Some(dir) = std::env::var_os(CHILD) {
            let broker = Broker::open(BrokerConfig::new(std::path::PathBuf::from(dir))).unwrap();
            broker
                .publish(direct_request("body", Some("crash-key".into())))
                .unwrap();
            broker.flush_wal().unwrap();
            // The durable batch succeeded; omit the reply and all destructors.
            std::process::exit(73);
        }
        let dir = tempdir().unwrap();
        let shared = tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "broker::tests::shared_dedup_survives_process_exit_before_reply",
            ])
            .env(CHILD, dir.path())
            .env("BETTERMQ_SHARED_META_DIR", shared.path())
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(73),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        // Reopen in an isolated child to avoid changing process-wide env while
        // unrelated unit tests open their local stores.
        let retry = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "broker::tests::shared_dedup_retry_worker"])
            .env(CHILD, dir.path())
            .env("BETTERMQ_SHARED_META_DIR", shared.path())
            .output()
            .unwrap();
        assert!(
            retry.status.success(),
            "{}",
            String::from_utf8_lossy(&retry.stderr)
        );
    }

    #[test]
    fn shared_dedup_retry_worker() {
        let Some(dir) = std::env::var_os("BETTERMQ_TEST_BROKER_SHARED_CRASH_DIR") else {
            return;
        };
        let broker = Broker::open(BrokerConfig::new(std::path::PathBuf::from(dir))).unwrap();
        let old = broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "crash-key")
            .unwrap()
            .unwrap();
        let response = broker
            .publish(direct_request("retry", Some("crash-key".into())))
            .unwrap();
        assert!(response.duplicate);
        assert_eq!(response.message_id, Some(old.message_id));
    }

    #[test]
    fn same_shard_hundred_records_use_one_v2_epoch() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 8);
        let requests = (0..100)
            .map(|index| direct_request(format!("body-{index}"), Some(format!("key-{index}"))))
            .collect();
        let mut prepared = broker.prepare_publish_batch(requests).unwrap();
        let groups = prepared.take_shard_batches();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 100);
        let partition = groups[0].key().partition();
        let appended = broker
            .append_prepared_batch(groups.into_iter().next().unwrap())
            .unwrap();
        broker
            .finalize_prepared_batch(&mut prepared, appended, true)
            .unwrap();
        let responses = prepared.finish().unwrap();
        let last_offset = responses.last().unwrap().offset.unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let hwm = runtime
            .block_on(broker.wait_committed(DIRECT_TOPIC, partition, last_offset))
            .unwrap();
        assert_eq!(hwm, 100);
        assert_eq!(
            broker
                .shard_handle(DIRECT_TOPIC, partition)
                .unwrap()
                .flush_count()
                .unwrap(),
            1
        );

        let wal = std::fs::read(
            broker
                .physical_shard_path(partition)
                .unwrap()
                .join("active.wal"),
        )
        .unwrap();
        let mut cursor = std::io::Cursor::new(wal.as_slice());
        let (epoch, _) = broker_proto::decode_epoch(&mut cursor).unwrap();
        assert_eq!(epoch.record_count, 100);
        assert_eq!(cursor.position(), wal.len() as u64);
    }

    #[tokio::test]
    async fn async_keyed_publish_uses_shared_commit_not_a_private_flush() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        let shard = broker.shard_handle(DIRECT_TOPIC, 0).unwrap();
        let response = broker
            .publish_async(direct_request("async", Some("async-key".into())))
            .await
            .unwrap();
        assert_eq!(
            shard.forced_flush_count(),
            0,
            "async keyed requests must use commit tickets"
        );
        assert!(shard.committed_hwm() > response.offset.unwrap());
        let dedup = broker
            .inner
            .metadata
            .get_dedup(&broker.tenant(), "async-key")
            .unwrap()
            .unwrap();
        assert_eq!(Some(dedup.message_id), response.message_id);
        let sync = broker
            .publish(direct_request("sync", Some("sync-key".into())))
            .unwrap();
        assert!(shard.committed_hwm() > sync.offset.unwrap());
        assert!(
            shard.forced_flush_count() > 0,
            "synchronous callers retain their durability barrier"
        );
    }

    #[tokio::test]
    async fn cancelled_publish_callers_cannot_release_an_admitted_dedup_reservation() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_broker = broker.clone();
        let first = tokio::spawn(async move {
            let completion_broker = first_broker.clone();
            first_broker
                .publish_with_completion(
                    direct_request("original", Some("owned-completion".into())),
                    move |response, pending| async move {
                        started_tx.send(response.message_id.unwrap()).unwrap();
                        release_rx.await.unwrap();
                        completion_broker
                            .wait_committed(
                                &response.topic,
                                response.partition.unwrap(),
                                response.offset.unwrap(),
                            )
                            .await?;
                        crate::run_blocking_io(move || {
                            completion_broker.commit_publish_dedup(pending)
                        })
                        .await
                        .map_err(blocking_task_error)??;
                        Ok::<_, BrokerError>(response)
                    },
                )
                .await
                .unwrap()
        });
        let original = started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let retry_broker = broker.clone();
        let mut retry = tokio::spawn(async move {
            retry_broker
                .publish_async(direct_request("retry", Some("owned-completion".into())))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut retry)
                .await
                .is_err(),
            "the admitted completion, not the cancelled caller, owns the key"
        );
        release_tx.send(()).unwrap();
        let retry = tokio::time::timeout(Duration::from_secs(5), retry)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, Some(original));
        assert_eq!(
            broker.list_topic_messages(DIRECT_TOPIC, 10).unwrap().len(),
            1
        );
    }

    #[test]
    fn v2_batch_can_mix_topics_on_one_physical_shard() {
        let dir = tempdir().unwrap();
        let broker = open_v2_broker(dir.path(), 8);
        for topic in ["orders", "invoices"] {
            broker
                .create_subscription(CreateSubscriptionRequest {
                    topic: topic.into(),
                    url: format!("http://127.0.0.1:9/{topic}"),
                    secret: "s".into(),
                    parallelism: None,
                    default_max_retries: None,
                    retry_backoff: None,
                })
                .unwrap();
        }
        let mut prepared = broker
            .prepare_publish_batch(vec![
                queue_request("orders", "one"),
                queue_request("invoices", "two"),
            ])
            .unwrap();
        let groups = prepared.take_shard_batches();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
        let appended = broker
            .append_prepared_batch(groups.into_iter().next().unwrap())
            .unwrap();
        broker
            .finalize_prepared_batch(&mut prepared, appended, true)
            .unwrap();
        let responses = prepared.finish().unwrap();
        assert_eq!(responses[0].topic, "orders");
        assert_eq!(responses[1].topic, "invoices");
        assert_eq!(responses[0].partition, responses[1].partition);
        assert_ne!(responses[0].offset, responses[1].offset);
    }

    #[test]
    fn within_batch_duplicates_keep_input_order() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let responses = execute_prepared(
            &broker,
            vec![
                direct_request("a-owner", Some("a".into())),
                direct_request("b-owner", Some("b".into())),
                direct_request("a-duplicate", Some("a".into())),
                direct_request("b-duplicate", Some("b".into())),
            ],
        );
        assert_eq!(responses.len(), 4);
        assert_eq!(
            responses.iter().map(|r| r.duplicate).collect::<Vec<_>>(),
            vec![false, false, true, true]
        );
        assert_eq!(responses[0].message_id, responses[2].message_id);
        assert_eq!(responses[1].message_id, responses[3].message_id);
        assert_eq!(responses[0].offset, responses[2].offset);
        assert_eq!(responses[1].offset, responses[3].offset);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_same_key_reservation_allows_one_append() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let mut tasks = Vec::new();
        for index in 0..16 {
            let broker = broker.clone();
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                broker
                    .publish_async(direct_request(
                        format!("candidate-{index}"),
                        Some("race-key".into()),
                    ))
                    .await
                    .unwrap()
            }));
        }
        let mut responses = Vec::new();
        for task in tasks {
            responses.push(task.await.unwrap());
        }
        assert_eq!(
            responses
                .iter()
                .filter(|response| !response.duplicate)
                .count(),
            1
        );
        let first = &responses[0];
        assert!(responses.iter().all(|response| {
            response.message_id == first.message_id && response.offset == first.offset
        }));
        let partition = first.partition.unwrap();
        assert_eq!(
            broker
                .list_topic_messages_from(DIRECT_TOPIC, partition, 0, 100)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn dropping_unappended_batch_releases_reservation() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let prepared = broker
            .prepare_publish_batch(vec![direct_request(
                "abandoned",
                Some("released-key".into()),
            )])
            .unwrap();
        drop(prepared);

        let response = broker
            .publish(direct_request("retry", Some("released-key".into())))
            .unwrap();
        assert!(!response.duplicate);
    }

    #[test]
    fn append_failure_releases_reservation() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let generation = Arc::new(AtomicU64::new(2));
        let check = Arc::clone(&generation);
        broker.set_shard_leader_check(Arc::new(move |_| Some(check.load(Ordering::Acquire))));

        let mut prepared = broker
            .prepare_publish_batch(vec![direct_request(
                "will-fail",
                Some("retry-after-failure".into()),
            )])
            .unwrap();
        let stale_batch = prepared.take_shard_batches().pop().unwrap();

        generation.store(3, Ordering::Release);
        broker.publish(direct_request("new-term", None)).unwrap();
        let error = match broker.append_prepared_batch(stale_batch) {
            Err(error) => error,
            Ok(_) => panic!("stale prepared batch unexpectedly appended"),
        };
        assert!(matches!(
            error,
            BrokerError::Storage(broker_storage::LogError::StaleFence { .. })
        ));

        let response = broker
            .publish(direct_request("retry", Some("retry-after-failure".into())))
            .unwrap();
        assert!(!response.duplicate);
    }

    #[test]
    fn sync_publish_fails_fast_on_in_flight_key() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let held = broker
            .prepare_publish_batch(vec![direct_request("owner", Some("busy".into()))])
            .unwrap();
        let error = broker
            .publish(direct_request("retry", Some("busy".into())))
            .unwrap_err();
        assert!(matches!(error, BrokerError::IdempotencyInFlight(ref key) if key == "busy"));
        drop(held);
        assert!(
            !broker
                .publish(direct_request("retry", Some("busy".into())))
                .unwrap()
                .duplicate
        );
    }

    /// A single runtime worker must keep polling the reservation owner while
    /// same-key waiters are parked (the old Condvar wait deadlocked here).
    #[test]
    fn waiters_do_not_block_the_reservation_owner_on_one_worker() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut prepared = broker
                .prepare_publish_batch(vec![direct_request("owner", Some("slow".into()))])
                .unwrap();
            let waiters: Vec<_> = (0..8)
                .map(|index| {
                    let broker = broker.clone();
                    tokio::spawn(async move {
                        broker
                            .publish_async(direct_request(
                                format!("retry-{index}"),
                                Some("slow".into()),
                            ))
                            .await
                            .unwrap()
                    })
                })
                .collect();
            // Let every waiter run up to its await point.
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            for shard_batch in prepared.take_shard_batches() {
                let appended = broker.append_prepared_batch(shard_batch).unwrap();
                broker
                    .finalize_prepared_batch(&mut prepared, appended, true)
                    .unwrap();
            }
            let owner = prepared.finish().unwrap().pop().unwrap();
            for waiter in waiters {
                let response = waiter.await.unwrap();
                assert!(response.duplicate);
                assert_eq!(response.message_id, owner.message_id);
                assert_eq!(response.offset, owner.offset);
            }
        });
    }

    #[test]
    fn dedup_verification_does_not_block_a_single_runtime_worker() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let original = broker
            .publish(direct_request("owner", Some("durable-key".into())))
            .unwrap();
        broker.flush_wal().unwrap();

        // Dedup verification must consult the shard map. Simulate a slow
        // storage operation there; only the runtime's sibling future can
        // release it. A bounded watchdog also lets the old blocking path fail
        // the test instead of hanging the suite.
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = broker.clone();
        let thread = std::thread::spawn(move || {
            let _guard = holder.inner.shards.write();
            locked_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).is_ok()
        });
        locked_rx.recv().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (prepared, ()) = runtime.block_on(async {
            tokio::join!(
                broker.prepare_publish_batch_waiting(vec![direct_request(
                    "retry",
                    Some("durable-key".into()),
                )]),
                async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let _ = release_tx.send(());
                }
            )
        });
        assert!(
            thread.join().unwrap(),
            "dedup verification blocked the runtime"
        );
        let response = prepared.unwrap().finish().unwrap().pop().unwrap();
        assert!(response.duplicate);
        assert_eq!(response.message_id, original.message_id);
        assert_eq!(response.offset, original.offset);
    }

    #[tokio::test]
    async fn async_publish_and_dedup_keep_the_request_tenant() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let mut originals = Vec::new();
        for tenant in ["tenant-a", "tenant-b"] {
            let response = crate::scope_tenant(tenant.into(), async {
                broker
                    .publish_async(direct_request("body", Some("shared-key".into())))
                    .await
                    .unwrap()
            })
            .await;
            assert!(!response.duplicate);
            let stored = broker
                .read_message(
                    &response.topic,
                    response.partition.unwrap(),
                    response.offset.unwrap(),
                )
                .unwrap();
            assert_eq!(stored.tenant_id, tenant);
            let dedup = broker
                .inner
                .metadata
                .get_dedup(tenant, "shared-key")
                .unwrap()
                .unwrap();
            assert_eq!(Some(dedup.message_id), response.message_id);
            originals.push(response);
        }
        assert_ne!(originals[0].message_id, originals[1].message_id);
        for (tenant, original) in ["tenant-a", "tenant-b"].into_iter().zip(originals) {
            let retry = crate::scope_tenant(tenant.into(), async {
                broker
                    .publish_async(direct_request("retry", Some("shared-key".into())))
                    .await
                    .unwrap()
            })
            .await;
            assert!(retry.duplicate);
            assert_eq!(retry.message_id, original.message_id);
        }
    }

    #[test]
    fn opposite_key_order_batches_do_not_hold_partial_reservations() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let held = broker
            .prepare_publish_batch(vec![
                direct_request("a", Some("A".into())),
                direct_request("b", Some("B".into())),
            ])
            .unwrap();
        // [C, B] acquires C, conflicts on B and must roll C back.
        let error = match broker.prepare_publish_batch(vec![
            direct_request("c", Some("C".into())),
            direct_request("b2", Some("B".into())),
        ]) {
            Err(error) => error,
            Ok(_) => panic!("conflicting batch unexpectedly prepared"),
        };
        assert!(matches!(error, BrokerError::IdempotencyInFlight(ref key) if key == "B"));
        assert!(
            !broker
                .publish(direct_request("c", Some("C".into())))
                .unwrap()
                .duplicate
        );
        drop(held);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let prepared = runtime
            .block_on(broker.prepare_publish_batch_waiting(vec![
                direct_request("b", Some("B".into())),
                direct_request("a", Some("A".into())),
            ]))
            .unwrap();
        assert_eq!(prepared.len(), 2);
    }

    #[test]
    fn deferred_dedup_holds_key_until_commit() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (first, pending) = runtime
            .block_on(broker.publish_defer_dedup(direct_request("one", Some("ha".into()))))
            .unwrap();
        assert_eq!(pending.len(), 1);
        // Replication is still in flight: a retry must not append a second copy.
        assert!(matches!(
            broker.publish(direct_request("two", Some("ha".into()))),
            Err(BrokerError::IdempotencyInFlight(_))
        ));
        // Callers record dedup only once the record is committed; a row above
        // the visible HWM is treated as stale.
        runtime
            .block_on(broker.wait_committed(
                &first.topic,
                first.partition.unwrap(),
                first.offset.unwrap(),
            ))
            .unwrap();
        broker.commit_publish_dedup(pending).unwrap();
        let retry = broker
            .publish(direct_request("two", Some("ha".into())))
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.message_id, first.message_id);
        assert_eq!(retry.offset, first.offset);
    }

    #[test]
    fn deferred_dedup_dropped_after_failed_quorum_releases_unrecorded() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (first, pending) = runtime
            .block_on(broker.publish_defer_dedup(direct_request("one", Some("lost".into()))))
            .unwrap();
        drop(pending);
        let retry = broker
            .publish(direct_request("two", Some("lost".into())))
            .unwrap();
        assert!(!retry.duplicate);
        assert_ne!(retry.message_id, first.message_id);
    }

    #[test]
    fn inline_only_cell_policy_keeps_bodies_in_the_frame_and_rejects_before_append() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let limit = broker_payload::inline_max_bytes() * 2;
        broker.set_payload_policy(PayloadPolicy::InlineOnly { max_body: limit });
        let at_limit = "a".repeat(limit);
        let published = broker
            .publish(direct_request(at_limit.clone(), Some("fits".into())))
            .unwrap();
        let message = broker
            .read_message(
                DIRECT_TOPIC,
                published.partition.unwrap(),
                published.offset.unwrap(),
            )
            .unwrap();
        assert!(message.payload_ref_json.is_none());
        assert_eq!(message.payload, at_limit.as_bytes());
        // The replicated frame carries the whole body.
        let frame = published.replication_frame.unwrap();
        let (_, payload) = broker_proto::decode_frame(std::io::Cursor::new(&frame)).unwrap();
        assert_eq!(payload.len(), limit);

        let partition = published.partition.unwrap();
        let hwm = broker
            .partition_high_watermark(DIRECT_TOPIC, partition)
            .unwrap();
        let error = broker
            .publish(direct_request(
                "b".repeat(limit + 1),
                Some("too-big".into()),
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            BrokerError::Payload(broker_payload::PayloadError::TooLargeForCell { size, limit: l })
                if size == limit as u64 + 1 && l == limit as u64
        ));
        assert!(broker
            .check_publish_body(&direct_request("c".repeat(limit + 1), None))
            .is_err());
        // Base64 bodies are measured decoded.
        let mut encoded = direct_request(
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, vec![0u8; limit]),
            None,
        );
        encoded.payload_encoding = Some("base64".into());
        assert_eq!(decoded_payload_len(&encoded), limit);
        broker.check_publish_body(&encoded).unwrap();
        // Nothing appended, nothing written, and the key is not reserved.
        assert_eq!(
            broker
                .partition_high_watermark(DIRECT_TOPIC, partition)
                .unwrap(),
            hwm
        );
        let blobs = dir.path().join("payload-blobs");
        assert!(std::fs::read_dir(&blobs).map_or(true, |mut entries| entries.next().is_none()));
        assert!(
            !broker
                .publish(direct_request("small", Some("too-big".into())))
                .unwrap()
                .duplicate
        );
    }

    #[test]
    fn catch_up_frames_are_byte_bounded_but_always_advance() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        broker.set_payload_policy(PayloadPolicy::InlineOnly {
            max_body: 2 * 1024 * 1024,
        });
        let body = "m".repeat(1024 * 1024);
        let mut partition = 0;
        for _ in 0..6 {
            partition = broker
                .publish(direct_request(body.clone(), None))
                .unwrap()
                .partition
                .unwrap();
        }
        let page = broker
            .replication_frames_bounded(DIRECT_TOPIC, partition, 0, 4096, 3 * 1024 * 1024 + 1024)
            .unwrap();
        assert_eq!(page.len(), 3);
        assert!(page.iter().map(|(_, frame)| frame.len()).sum::<usize>() <= 3 * 1024 * 1024 + 1024);
        // A single frame larger than the budget still comes back alone.
        let tiny = broker
            .replication_frames_bounded(DIRECT_TOPIC, partition, page[2].0 + 1, 4096, 10)
            .unwrap();
        assert_eq!(tiny.len(), 1);
        // Paging converges on every record.
        let mut from = 0;
        let mut seen = 0;
        loop {
            let page = broker
                .replication_frames_bounded(DIRECT_TOPIC, partition, from, 4096, 2 * 1024 * 1024)
                .unwrap();
            let Some((last, _)) = page.last() else { break };
            from = last + 1;
            seen += page.len();
        }
        assert_eq!(seen, 6);
    }

    #[test]
    fn purge_dlq_topic_deletes_all_or_only_older_than_cutoff() {
        let dir = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let topic = "jobs.__dlq";
        for key in ["a", "b"] {
            broker
                .publish(PublishRequest {
                    routing_id: None,
                    topic: topic.into(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: key.into(),
                    payload: "{}".into(),
                    payload_encoding: None,
                    idempotency_key: None,
                    delay_ms: None,
                    priority: None,
                    flow_id: None,
                    url: None,
                    secret: None,
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
        }
        assert_eq!(broker.list_topic_messages(topic, 10).unwrap().len(), 2);
        assert_eq!(broker.purge_dlq_topic(topic, Some(0)).unwrap(), 0);
        assert_eq!(broker.list_topic_messages(topic, 10).unwrap().len(), 2);
        assert_eq!(broker.purge_dlq_topic(topic, None).unwrap(), 2);
        assert!(broker.list_topic_messages(topic, 10).unwrap().is_empty());
    }
}
