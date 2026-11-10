//! Job lease protocol (Phase B): claim / heartbeat / complete / fail.
//! Shard leader owns CAS pending→leased; fleet and in-process dispatch use the same path.

use broker_storage::StoredMessage;
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use crate::fairness::TenantFairQueue;
use crate::flow_control::{FlowKey, FlowOverride};
use crate::scan::PendingEntry;
use crate::worker::DispatchEngine;

#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("not shard leader for partition {0}")]
    NotShardLeader(u32),
    #[error("lease not found: {0}")]
    NotFound(Uuid),
    #[error("lease held by another worker")]
    Conflict,
    #[error("lease expired")]
    Expired,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedJob {
    pub lease_id: Uuid,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub expires_at_ms: i64,
    pub generation: u64,
    /// Broker-verified committed high watermark at claim time.
    #[serde(default)]
    pub committed_hwm: u64,
    /// Cursor key used by dispatch (lane owner UUID string, else topic).
    pub cursor_key: String,
    /// Delivery envelope — present on claim so fleet can push without a second fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StoredMessage>,
    /// Delivery attempts already spent, from the broker's durable retry state.
    #[serde(default)]
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub holder: String,
    #[serde(default = "default_max_jobs")]
    pub max_jobs: usize,
    #[serde(default = "default_ttl_ms")]
    pub lease_ttl_ms: i64,
    /// Optional partition filter (fleet may round-robin).
    pub partition: Option<u32>,
}

/// Upper bound on jobs handed out by one claim.
pub const MAX_CLAIM_JOBS: usize = 64;

fn default_max_jobs() -> usize {
    8
}
fn default_ttl_ms() -> i64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub jobs: Vec<ClaimedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub lease_id: Uuid,
    pub holder: String,
    #[serde(default = "default_ttl_ms")]
    pub lease_ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub lease_id: Uuid,
    pub holder: String,
    pub generation: u64,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub cursor_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailRequest {
    pub lease_id: Uuid,
    pub holder: String,
    pub generation: u64,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub message_id: Uuid,
    pub cursor_key: String,
    pub reason: String,
    /// When true, move to DLQ instead of retry delay.
    #[serde(default)]
    pub dead_letter: bool,
    /// For retryable failures, keep the offset unavailable until this delay.
    #[serde(default)]
    pub retry_after_ms: u64,
    /// The failure was a delivery attempt (webhook error status or transport
    /// failure): the broker counts it against the message's retry budget and
    /// dead-letters once it is spent. False for deferrals that sent nothing
    /// (blocked host, uncommitted, payload not loaded).
    #[serde(default = "default_true")]
    pub counts_attempt: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone)]
struct LeaseEntry {
    job: ClaimedJob,
    holder: String,
    /// In-process lease released only by its owner, never by expiry.
    held: bool,
    /// Flow lane the leased message occupies (fleet claims only).
    flow: Option<FlowKey>,
}

type LeasedOffsetKey = (String, u32, u64);
type LeasedOffsetMap = HashMap<LeasedOffsetKey, Uuid>;

/// In-process lease table (HA: also durable via broker cursor; leases themselves are soft state).
#[derive(Clone, Default)]
pub struct LeaseTable {
    inner: Arc<Mutex<HashMap<Uuid, LeaseEntry>>>,
    /// offset keys currently leased: (topic, partition, offset)
    leased_offsets: Arc<Mutex<LeasedOffsetMap>>,
}

impl LeaseTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, lease_id: Uuid) -> Option<ClaimedJob> {
        self.purge_expired();
        self.inner.lock().get(&lease_id).map(|e| e.job.clone())
    }

    pub fn is_offset_leased(&self, topic: &str, partition: u32, offset: u64) -> bool {
        self.purge_expired();
        self.leased_offsets
            .lock()
            .contains_key(&(topic.to_string(), partition, offset))
    }

    pub fn insert(&self, holder: String, job: ClaimedJob) {
        let _ = self.try_insert(holder, job);
    }

    /// Atomically claim an offset. Returns false if another holder already leased it.
    pub fn try_insert(&self, holder: String, job: ClaimedJob) -> bool {
        self.insert_entry(holder, job, false, None)
    }

    /// Lease an offset for an in-process delivery. The lease is held until its
    /// owner releases it with [`LeaseTable::take`] (the dispatch guard does so on
    /// drop), however long the job waits in a flow lane or for rate tokens, so it
    /// never lapses under a job that is still queued locally.
    pub fn try_insert_held(&self, holder: String, job: ClaimedJob) -> bool {
        self.insert_entry(holder, job, true, None)
    }

    fn insert_entry(
        &self,
        holder: String,
        job: ClaimedJob,
        held: bool,
        flow: Option<FlowKey>,
    ) -> bool {
        let id = job.lease_id;
        let key = (job.topic.clone(), job.partition, job.offset);
        let mut inner = self.inner.lock();
        let mut offsets = self.leased_offsets.lock();
        if offsets.contains_key(&key) {
            return false;
        }
        offsets.insert(key, id);
        inner.insert(
            id,
            LeaseEntry {
                job,
                holder,
                held,
                flow,
            },
        );
        true
    }

    /// Re-lease a job for `holder` (e.g. to hold it back after a failed
    /// attempt), keeping it counted against its flow lane.
    pub fn try_reinsert(&self, holder: String, job: ClaimedJob) -> bool {
        let flow = job
            .message
            .as_ref()
            .and_then(crate::scan::FlowAdmission::for_message)
            .map(|flow| flow.key);
        self.insert_entry(holder, job, false, flow)
    }

    /// Leases currently held per flow lane (call after [`LeaseTable::purge_expired`]).
    pub(crate) fn flow_active_counts(&self) -> HashMap<FlowKey, u32> {
        let mut counts = HashMap::new();
        for entry in self.inner.lock().values() {
            if let Some(flow) = &entry.flow {
                *counts.entry(flow.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    pub fn heartbeat(
        &self,
        lease_id: Uuid,
        holder: &str,
        ttl_ms: i64,
    ) -> Result<ClaimedJob, LeaseError> {
        self.purge_expired();
        let mut map = self.inner.lock();
        let entry = map
            .get_mut(&lease_id)
            .ok_or(LeaseError::NotFound(lease_id))?;
        if entry.holder != holder {
            return Err(LeaseError::Conflict);
        }
        let now = Utc::now().timestamp_millis();
        if entry.job.expires_at_ms <= now {
            return Err(LeaseError::Expired);
        }
        entry.job.expires_at_ms = now + ttl_ms;
        Ok(entry.job.clone())
    }

    pub fn take(&self, lease_id: Uuid, holder: &str) -> Result<ClaimedJob, LeaseError> {
        self.purge_expired();
        let mut map = self.inner.lock();
        let entry = map
            .remove(&lease_id)
            .ok_or(LeaseError::NotFound(lease_id))?;
        if entry.holder != holder {
            map.insert(lease_id, entry);
            return Err(LeaseError::Conflict);
        }
        let key = (
            entry.job.topic.clone(),
            entry.job.partition,
            entry.job.offset,
        );
        self.leased_offsets.lock().remove(&key);
        Ok(entry.job)
    }

    /// Leased right now, without first purging expired leases (callers that
    /// check many offsets purge once up front with [`LeaseTable::purge_expired`]).
    pub(crate) fn offset_leased(&self, topic: &str, partition: u32, offset: u64) -> bool {
        self.leased_offsets
            .lock()
            .contains_key(&(topic.to_string(), partition, offset))
    }

    pub(crate) fn purge_expired(&self) {
        let now = Utc::now().timestamp_millis();
        let mut map = self.inner.lock();
        let mut offsets = self.leased_offsets.lock();
        let expired: Vec<Uuid> = map
            .iter()
            .filter(|(_, e)| !e.held && e.job.expires_at_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(e) = map.remove(&id) {
                offsets.remove(&(e.job.topic, e.job.partition, e.job.offset));
            }
        }
    }

    pub fn active_count(&self) -> usize {
        self.purge_expired();
        self.inner.lock().len()
    }

    pub fn snapshot_for_status(&self) -> Vec<ClaimedJob> {
        self.purge_expired();
        self.inner
            .lock()
            .values()
            .map(|e| {
                let mut j = e.job.clone();
                j.message = None; // strip payload from status
                j
            })
            .collect()
    }
}

#[cfg(test)]
fn cursor_key_for(msg: &StoredMessage) -> String {
    broker_partition::flow_lane_owner(msg).to_string()
}

/// New log records one claim reads per `(topic, partition)` while extending
/// the pending-work index.
const CLAIM_RECORD_BUDGET: usize = 4_096;

/// Body bytes one claim carries; further jobs wait for the next claim (a
/// claim always carries at least one job).
const CLAIM_PAYLOAD_BUDGET: usize = 32 * 1024 * 1024;

/// Load a blob-backed body into `msg` and verify it against its reference.
fn resolve_payload(
    broker: &broker_partition::Broker,
    msg: &mut StoredMessage,
) -> Result<(), String> {
    let Some(reference) = msg.payload_ref_json.as_deref() else {
        return Ok(());
    };
    if !msg.payload.is_empty() {
        return Ok(());
    }
    let reference: broker_payload::PayloadRef =
        serde_json::from_str(reference).map_err(|e| format!("invalid payload reference: {e}"))?;
    broker
        .hydrate_message_payload(msg)
        .map_err(|e| e.to_string())?;
    verify_payload(&reference, &msg.payload)
}

/// Per-flow-lane rate windows for fleet claims (deliveries started per period).
#[derive(Clone, Default)]
pub(crate) struct ClaimRates {
    windows: Arc<Mutex<HashMap<FlowKey, RateWindow>>>,
}

struct RateWindow {
    period_start_ms: i64,
    count: u32,
}

impl ClaimRates {
    /// Take one start from the lane's window; false when the window is full.
    fn try_acquire(&self, key: &FlowKey, rate: u32, period_secs: u64, now_ms: i64) -> bool {
        if rate == 0 {
            return true;
        }
        let period_ms = i64::try_from(period_secs.max(1).saturating_mul(1000)).unwrap_or(i64::MAX);
        let mut windows = self.windows.lock();
        let window = windows.entry(key.clone()).or_insert(RateWindow {
            period_start_ms: now_ms,
            count: 0,
        });
        if now_ms.saturating_sub(window.period_start_ms) >= period_ms {
            window.period_start_ms = now_ms;
            window.count = 0;
        }
        if window.count < rate {
            window.count += 1;
            true
        } else {
            false
        }
    }
}

/// Catalog pause flags looked up once per claim.
struct PauseCache<'a> {
    broker: &'a broker_partition::Broker,
    queues: HashMap<Uuid, bool>,
    members: HashMap<Uuid, bool>,
}

impl PauseCache<'_> {
    fn paused(&mut self, entry: &PendingEntry) -> bool {
        let broker = self.broker;
        let queue = entry.queue_id.is_some_and(|id| {
            *self.queues.entry(id).or_insert_with(|| {
                broker
                    .get_queue_by_id(id)
                    .ok()
                    .flatten()
                    .is_some_and(|q| q.paused)
            })
        });
        let member = entry.group_member_id.is_some_and(|id| {
            *self.members.entry(id).or_insert_with(|| {
                broker
                    .get_group_member(id)
                    .ok()
                    .flatten()
                    .is_some_and(|m| m.paused)
            })
        });
        queue || member
    }
}

/// Shared claim selection used by the HTTP lease API.
///
/// Candidates come from the engine's incremental pending-work index, so a
/// claim reads only records appended since the previous claim instead of the
/// whole retained log. The broker is the authority for delivery semantics:
/// a candidate is leased only when
/// - its queue / group member and its flow lane are not paused,
/// - no earlier offset of its lane is waiting on a retry (same fence as local
///   dispatch), and it is not itself waiting on one,
/// - its flow lane has a free parallelism slot (held fleet leases count) and
///   rate budget, and
/// - for strict-FIFO lanes (parallelism 1), no earlier offset of the lane is
///   still pending.
#[allow(clippy::too_many_arguments)]
pub async fn claim_from_broker(
    engine: &DispatchEngine,
    leases: &LeaseTable,
    fair_queue: Option<&TenantFairQueue>,
    holder: &str,
    max_jobs: usize,
    lease_ttl_ms: i64,
    partition_filter: Option<u32>,
    is_leader: &(dyn Fn(u32) -> bool + Sync),
    generation: &(dyn Fn(u32) -> u64 + Sync),
) -> ClaimResponse {
    let overrides = engine.flow.admission_overrides().await;
    claim_with_overrides(
        engine,
        leases,
        fair_queue,
        holder,
        max_jobs,
        lease_ttl_ms,
        partition_filter,
        is_leader,
        generation,
        &overrides,
    )
}

#[allow(clippy::too_many_arguments)]
fn claim_with_overrides(
    engine: &DispatchEngine,
    leases: &LeaseTable,
    fair_queue: Option<&TenantFairQueue>,
    holder: &str,
    max_jobs: usize,
    lease_ttl_ms: i64,
    partition_filter: Option<u32>,
    is_leader: &(dyn Fn(u32) -> bool + Sync),
    generation: &(dyn Fn(u32) -> u64 + Sync),
    overrides: &HashMap<FlowKey, FlowOverride>,
) -> ClaimResponse {
    let broker = engine.broker();
    let max = max_jobs.clamp(1, MAX_CLAIM_JOBS);
    let ttl = lease_ttl_ms.max(5_000);
    let now = Utc::now().timestamp_millis();
    let mut jobs = Vec::new();

    // Fairness: order topics by tenant WFQ score (lower = prefer).
    let mut topic_list = engine.dispatch_topics();
    if let Some(fq) = fair_queue {
        let tenant = broker.tenant();
        topic_list.sort_by_key(|t| fq.schedule_score(&format!("{tenant}:{t}")));
    }
    leases.purge_expired();
    let mut active = leases.flow_active_counts();
    let mut claimed_bytes = 0usize;
    // Strict-FIFO lanes whose head could not be leased in this claim.
    let mut blocked: HashSet<FlowKey> = HashSet::new();
    let mut pauses = PauseCache {
        broker,
        queues: HashMap::new(),
        members: HashMap::new(),
    };

    'outer: for topic in topic_list {
        let Ok(pc) = broker.partition_count(&topic) else {
            continue;
        };
        for partition in 0..pc {
            if let Some(want) = partition_filter {
                if want != partition {
                    continue;
                }
            }
            if !is_leader(partition) {
                continue;
            }
            // Quorum-gated in a cell: claims never carry a record that may not
            // survive failover.
            let Ok(committed_hwm) = broker.dispatch_hwm(&topic, partition) else {
                continue;
            };
            let view = match engine
                .scan()
                .refresh(broker, &topic, partition, CLAIM_RECORD_BUDGET)
            {
                Ok(view) => view,
                Err(error) => {
                    tracing::warn!(%error, %topic, partition, "lease claim scan failed");
                    continue;
                }
            };
            for entry in view.entries {
                if jobs.len() >= max {
                    break 'outer;
                }
                // Limits frozen on the message, with operator pins applied.
                let flow = entry.flow.as_ref().map(|flow| {
                    let pin = overrides.get(&flow.key);
                    let parallelism = pin
                        .and_then(|p| p.parallelism)
                        .unwrap_or(flow.parallelism)
                        .max(1);
                    let rate = pin.and_then(|p| p.rate).unwrap_or(flow.rate);
                    let period_secs = pin
                        .and_then(|p| p.period_secs)
                        .unwrap_or(flow.period_secs)
                        .max(1);
                    let paused = pin.is_some_and(|p| p.paused);
                    (flow.key.clone(), parallelism, rate, period_secs, paused)
                });
                if let Some((key, ..)) = &flow {
                    if blocked.contains(key) {
                        continue;
                    }
                }
                let strict_fifo = flow.as_ref().is_some_and(|f| f.1 == 1);
                let held_back = |blocked: &mut HashSet<FlowKey>| {
                    if strict_fifo {
                        if let Some((key, ..)) = &flow {
                            blocked.insert(key.clone());
                        }
                    }
                };
                if leases.offset_leased(&topic, partition, entry.offset)
                    || pauses.paused(&entry)
                    || flow.as_ref().is_some_and(|f| f.4)
                {
                    held_back(&mut blocked);
                    continue;
                }
                // Retry fence, as for local delivery: a strict-FIFO lane waits
                // for its earliest retrying offset; others only for their own.
                if let Some(retry) = engine.blocking_entry_retry(&entry) {
                    let own = retry.key.offset == entry.offset
                        && retry.key.message_id == entry.message_id;
                    if !own || retry.due_at_ms > now {
                        held_back(&mut blocked);
                        continue;
                    }
                }
                if let Some((key, parallelism, ..)) = &flow {
                    if active.get(key).copied().unwrap_or(0) >= *parallelism {
                        held_back(&mut blocked);
                        continue;
                    }
                }
                // The index may lag completions made elsewhere; confirm.
                let done = broker
                    .is_delivery_complete(
                        &topic,
                        &entry.tenant_id,
                        &entry.cursor_key,
                        partition,
                        entry.offset,
                        entry.message_id,
                    )
                    .unwrap_or(false);
                if done {
                    engine.scan().mark_done(&topic, partition, entry.offset);
                    continue;
                }
                let Ok(msg) = broker.read_message(&topic, partition, entry.offset) else {
                    held_back(&mut blocked);
                    continue;
                };
                if msg.id != entry.message_id {
                    held_back(&mut blocked);
                    continue;
                }
                // Blob-backed bodies are loaded and verified here, so a claim is
                // always deliverable as-is; an unreadable blob is never leased
                // (the message stays pending, never an empty body or a DLQ).
                let mut msg = msg;
                if let Err(error) = resolve_payload(broker, &mut msg) {
                    tracing::warn!(
                        %error,
                        %topic,
                        partition,
                        offset = msg.offset,
                        message_id = %msg.id,
                        "lease claim withheld: payload blob could not be loaded"
                    );
                    held_back(&mut blocked);
                    continue;
                }
                if !jobs.is_empty()
                    && claimed_bytes.saturating_add(msg.payload.len()) > CLAIM_PAYLOAD_BUDGET
                {
                    break 'outer;
                }
                if let Some((key, _, rate, period_secs, _)) = &flow {
                    if !engine
                        .claim_rates()
                        .try_acquire(key, *rate, *period_secs, now)
                    {
                        held_back(&mut blocked);
                        continue;
                    }
                }
                let payload_len = msg.payload.len();
                let claimed = ClaimedJob {
                    lease_id: Uuid::new_v4(),
                    topic: topic.clone(),
                    partition,
                    offset: msg.offset,
                    message_id: msg.id,
                    expires_at_ms: now + ttl,
                    generation: generation(partition),
                    committed_hwm,
                    cursor_key: entry.cursor_key.clone(),
                    attempts: engine.prior_attempts(&topic, partition, msg.offset, msg.id),
                    message: Some(msg),
                };
                let flow_key = flow.as_ref().map(|f| f.0.clone());
                if !leases.insert_entry(
                    holder.to_string(),
                    claimed.clone(),
                    false,
                    flow_key.clone(),
                ) {
                    held_back(&mut blocked);
                    continue;
                }
                if let Some(key) = flow_key {
                    *active.entry(key).or_insert(0) += 1;
                }
                claimed_bytes = claimed_bytes.saturating_add(payload_len);
                jobs.push(claimed);
            }
        }
    }

    ClaimResponse { jobs }
}

/// HTTP client used by `--dispatch-fleet` to claim from brokers.
#[derive(Clone)]
pub struct LeaseClient {
    http: reqwest::Client,
    broker_urls: Vec<String>,
    holder: String,
}

const LEASE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_FLEET_RPC_TIMEOUT_SECS: u64 = 10;

/// Timeout for one lease RPC (`BETTERMQ_FLEET_RPC_TIMEOUT_SECS`, default 10 s).
pub fn fleet_rpc_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("BETTERMQ_FLEET_RPC_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(DEFAULT_FLEET_RPC_TIMEOUT_SECS),
    )
}

impl LeaseClient {
    pub fn from_env() -> Option<Self> {
        let urls = std::env::var("BETTERMQ_BROKER_URLS").ok()?;
        let broker_urls: Vec<String> = urls
            .split(',')
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if broker_urls.is_empty() {
            return None;
        }
        let holder = std::env::var("BETTERMQ_FLEET_HOLDER")
            .unwrap_or_else(|_| format!("fleet-{}", Uuid::new_v4()));
        Some(Self::new(broker_urls, holder))
    }

    pub fn new(broker_urls: Vec<String>, holder: String) -> Self {
        Self::with_rpc_timeout(broker_urls, holder, fleet_rpc_timeout())
    }

    /// Lease RPCs (claim, heartbeat, complete, fail) get their own short
    /// timeout, independent of the webhook timeouts, so a stalled broker
    /// cannot hold a delivery permit for the webhook long-wait budget.
    pub fn with_rpc_timeout(broker_urls: Vec<String>, holder: String, timeout: Duration) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(LEASE_CONNECT_TIMEOUT.min(timeout))
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("lease client"),
            broker_urls,
            holder,
        }
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn broker_urls(&self) -> &[String] {
        &self.broker_urls
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    fn with_secret(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Ok(secret) = std::env::var("BETTERMQ_CLUSTER_SECRET") {
            if !secret.trim().is_empty() {
                return req.header("x-bettermq-cluster-secret", secret);
            }
        }
        req
    }

    pub async fn claim(&self, broker: &str, max_jobs: usize) -> Result<ClaimResponse, String> {
        let url = format!("{broker}/internal/v1/lease/claim");
        let body = ClaimRequest {
            holder: self.holder.clone(),
            max_jobs,
            lease_ttl_ms: default_ttl_ms(),
            partition: None,
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!(
                "claim {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }
        resp.json().await.map_err(|e| e.to_string())
    }

    pub async fn heartbeat(&self, broker: &str, lease_id: Uuid) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/heartbeat");
        let body = HeartbeatRequest {
            lease_id,
            holder: self.holder.clone(),
            lease_ttl_ms: default_ttl_ms(),
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("heartbeat {}", resp.status()));
        }
        Ok(())
    }

    pub async fn complete(&self, broker: &str, job: &ClaimedJob) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/complete");
        let body = CompleteRequest {
            lease_id: job.lease_id,
            holder: self.holder.clone(),
            generation: job.generation,
            topic: job.topic.clone(),
            partition: job.partition,
            offset: job.offset,
            message_id: job.message_id,
            cursor_key: job.cursor_key.clone(),
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("complete {}", resp.status()));
        }
        Ok(())
    }

    pub async fn fail(
        &self,
        broker: &str,
        job: &ClaimedJob,
        reason: &str,
        dead_letter: bool,
        retry_after_ms: u64,
        counts_attempt: bool,
    ) -> Result<(), String> {
        let url = format!("{broker}/internal/v1/lease/fail");
        let body = FailRequest {
            lease_id: job.lease_id,
            holder: self.holder.clone(),
            generation: job.generation,
            topic: job.topic.clone(),
            partition: job.partition,
            offset: job.offset,
            message_id: job.message_id,
            cursor_key: job.cursor_key.clone(),
            reason: reason.to_string(),
            dead_letter,
            retry_after_ms,
            counts_attempt,
        };
        let resp = self
            .with_secret(self.http.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("fail {}", resp.status()));
        }
        Ok(())
    }
}

fn verify_payload(reference: &broker_payload::PayloadRef, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 != reference.size {
        return Err(format!(
            "payload size mismatch: got {} bytes, expected {}",
            bytes.len(),
            reference.size
        ));
    }
    if let Some(expected) = reference.sha256.as_deref() {
        use sha2::Digest;
        let actual = hex::encode(sha2::Sha256::digest(bytes));
        if !actual.eq_ignore_ascii_case(expected) {
            return Err("payload checksum mismatch".into());
        }
    }
    Ok(())
}

/// Fleet concurrency (jobs in flight per fleet process).
pub fn fleet_concurrency() -> usize {
    std::env::var("BETTERMQ_FLEET_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Long-wait timeout tiers for fleet (Phase E). Env BETTERMQ_LONG_WAIT_TIER=15m|1h|6h|12h
pub fn long_wait_tier_secs() -> Option<u64> {
    let tier = std::env::var("BETTERMQ_LONG_WAIT_TIER").ok()?;
    match tier.trim().to_lowercase().as_str() {
        "15m" | "15" => Some(15 * 60),
        "1h" | "60m" => Some(60 * 60),
        "6h" => Some(6 * 60 * 60),
        "12h" => Some(12 * 60 * 60),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::DispatchConfig;
    use broker_partition::{Broker, BrokerConfig, FlowSpec, PublishRequest};

    #[test]
    fn lease_insert_heartbeat_take() {
        let table = LeaseTable::new();
        let id = Uuid::new_v4();
        let job = ClaimedJob {
            lease_id: id,
            topic: "t".into(),
            partition: 0,
            offset: 1,
            message_id: Uuid::new_v4(),
            expires_at_ms: Utc::now().timestamp_millis() + 60_000,
            generation: 1,
            attempts: 0,
            committed_hwm: 2,
            cursor_key: "t".into(),
            message: None,
        };
        table.insert("w1".into(), job);
        assert!(table.is_offset_leased("t", 0, 1));
        let dup = ClaimedJob {
            lease_id: Uuid::new_v4(),
            topic: "t".into(),
            partition: 0,
            offset: 1,
            message_id: Uuid::new_v4(),
            expires_at_ms: Utc::now().timestamp_millis() + 60_000,
            generation: 1,
            attempts: 0,
            committed_hwm: 2,
            cursor_key: "t".into(),
            message: None,
        };
        assert!(!table.try_insert("w2".into(), dup));
        assert!(table.heartbeat(id, "w1", 30_000).is_ok());
        assert!(table.heartbeat(id, "other", 30_000).is_err());
        assert!(table.take(id, "w1").is_ok());
        assert!(!table.is_offset_leased("t", 0, 1));
    }

    #[test]
    fn held_local_lease_outlives_its_ttl_until_released() {
        let table = LeaseTable::new();
        let id = Uuid::new_v4();
        let job = ClaimedJob {
            lease_id: id,
            topic: "t".into(),
            partition: 0,
            offset: 7,
            message_id: Uuid::new_v4(),
            // A job parked in a flow lane longer than the lease TTL.
            expires_at_ms: Utc::now().timestamp_millis() - 120_000,
            generation: 0,
            attempts: 0,
            committed_hwm: 8,
            cursor_key: "t".into(),
            message: None,
        };
        assert!(table.try_insert_held("local".into(), job.clone()));
        assert!(table.is_offset_leased("t", 0, 7));
        let mut other = job.clone();
        other.lease_id = Uuid::new_v4();
        assert!(
            !table.try_insert("fleet".into(), other),
            "a queued local job keeps its offset"
        );
        table.take(id, "local").unwrap();
        assert!(!table.is_offset_leased("t", 0, 7));
    }

    #[test]
    fn long_wait_tiers() {
        std::env::set_var("BETTERMQ_LONG_WAIT_TIER", "1h");
        assert_eq!(long_wait_tier_secs(), Some(3600));
        std::env::remove_var("BETTERMQ_LONG_WAIT_TIER");
    }

    #[tokio::test]
    async fn claim_uses_each_messages_lane_cursor_and_skips_completions() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let publish = |lane: &str| {
            broker
                .publish(PublishRequest {
                    routing_id: None,
                    topic: String::new(),
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    routing_key: "same-shard".into(),
                    payload: lane.into(),
                    payload_encoding: None,
                    idempotency_key: None,
                    delay_ms: None,
                    priority: None,
                    flow_id: None,
                    url: Some("https://example.com/hook".into()),
                    secret: Some("secret".into()),
                    destination: None,
                    flow: Some(FlowSpec {
                        key: Some(lane.into()),
                        parallelism: Some(1),
                        rate: None,
                        period_secs: None,
                    }),
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
        let first = publish("lane-a");
        let second = publish("lane-b");
        broker.flush_wal().unwrap();
        let topic = first.topic;
        let partition = first.partition.unwrap();
        let first_msg = broker
            .read_message(&topic, partition, first.offset.unwrap())
            .unwrap();
        let second_msg = broker
            .read_message(&topic, partition, second.offset.unwrap())
            .unwrap();
        let lane_a = cursor_key_for(&first_msg);
        let lane_b = cursor_key_for(&second_msg);
        assert_ne!(lane_a, lane_b);
        let tenant = broker.tenant();
        broker
            .set_dispatch_offset(&tenant, &lane_a, partition, second_msg.offset + 1)
            .unwrap();

        let leases = LeaseTable::new();
        let engine = DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default());
        let claimed = claim_from_broker(
            &engine,
            &leases,
            None,
            "worker",
            8,
            30_000,
            Some(partition),
            &|_| true,
            &|_| 7,
        )
        .await;
        assert_eq!(claimed.jobs.len(), 1);
        assert_eq!(claimed.jobs[0].message_id, second_msg.id);
        assert_eq!(claimed.jobs[0].cursor_key, lane_b);

        leases.take(claimed.jobs[0].lease_id, "worker").unwrap();
        broker
            .mark_dispatch_complete(&tenant, &lane_b, partition, second_msg.offset)
            .unwrap();
        let again = claim_from_broker(
            &engine,
            &leases,
            None,
            "worker",
            8,
            30_000,
            Some(partition),
            &|_| true,
            &|_| 7,
        )
        .await;
        assert!(again.jobs.is_empty());
    }

    fn publish_flow(
        broker: &Broker,
        topic: &str,
        flow: Option<FlowSpec>,
    ) -> broker_partition::PublishResponse {
        let direct = topic.is_empty();
        let response = broker
            .publish(PublishRequest {
                topic: topic.into(),
                queue_id: None,
                group_id: None,
                group_member_id: None,
                routing_key: "same-key".into(),
                payload: "body".into(),
                routing_id: None,
                payload_encoding: None,
                idempotency_key: None,
                delay_ms: None,
                priority: None,
                flow_id: None,
                url: direct.then(|| "https://example.com/hook".into()),
                secret: direct.then(|| "secret".into()),
                destination: None,
                flow,
                parallelism: None,
                max_retries: Some(3),
                retry_backoff: None,
                method: None,
                headers: None,
                sign: None,
                request: None,
            })
            .unwrap();
        broker.flush_wal().unwrap();
        response
    }

    fn lane(key: &str, parallelism: u32, rate: Option<u32>) -> Option<FlowSpec> {
        Some(FlowSpec {
            key: Some(key.into()),
            parallelism: Some(parallelism),
            rate,
            period_secs: Some(60),
        })
    }

    async fn claim(engine: &DispatchEngine, leases: &LeaseTable, holder: &str) -> Vec<ClaimedJob> {
        claim_from_broker(
            engine,
            leases,
            None,
            holder,
            8,
            30_000,
            None,
            &|_| true,
            &|_| 0,
        )
        .await
        .jobs
    }

    fn engine_for(broker: &Broker) -> DispatchEngine {
        DispatchEngine::new_broker_only(broker.clone(), DispatchConfig::default())
    }

    #[tokio::test]
    async fn strict_fifo_lane_leases_only_its_head_and_waits_for_its_retry() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let first = publish_flow(&broker, "", lane("fifo", 1, None));
        publish_flow(&broker, "", lane("fifo", 1, None));
        publish_flow(&broker, "", lane("fifo", 1, None));
        let engine = engine_for(&broker);
        let leases = LeaseTable::new();

        let jobs = claim(&engine, &leases, "w1").await;
        assert_eq!(jobs.len(), 1, "one in flight per FIFO lane");
        assert_eq!(jobs[0].offset, first.offset.unwrap());
        assert!(
            claim(&engine, &leases, "w2").await.is_empty(),
            "a second worker must not overtake the head"
        );

        // The head fails: the broker records the attempt and the lane waits.
        leases.take(jobs[0].lease_id, "w1").unwrap();
        let outcome = engine
            .fail_leased_delivery(
                &jobs[0].topic,
                jobs[0].partition,
                jobs[0].offset,
                "HTTP 503",
                60_000,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            crate::LeasedFailure::RetryScheduled {
                attempts: 1,
                delay_ms: 60_000
            }
        );
        assert!(
            claim(&engine, &leases, "w2").await.is_empty(),
            "later offsets wait behind the retrying head"
        );
    }

    #[tokio::test]
    async fn retry_budget_is_counted_by_the_broker() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let response = publish_flow(&broker, "", None);
        let engine = engine_for(&broker);
        let (topic, partition, offset) = (
            response.topic.clone(),
            response.partition.unwrap(),
            response.offset.unwrap(),
        );
        for attempt in 1..=3 {
            assert_eq!(
                engine
                    .fail_leased_delivery(&topic, partition, offset, "HTTP 503", 1)
                    .await
                    .unwrap(),
                crate::LeasedFailure::RetryScheduled {
                    attempts: attempt,
                    delay_ms: 1
                }
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        let jobs = claim(&engine, &LeaseTable::new(), "w").await;
        assert_eq!(jobs[0].attempts, 3, "claims report the spent attempts");
        assert_eq!(
            engine
                .fail_leased_delivery(&topic, partition, offset, "HTTP 503", 1)
                .await
                .unwrap(),
            crate::LeasedFailure::DeadLettered,
            "max_retries=3 allows four attempts"
        );
        assert_eq!(
            broker
                .list_topic_messages(&broker_partition::dlq_topic(&topic), 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn flow_parallelism_and_rate_limit_claims() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        for _ in 0..3 {
            publish_flow(&broker, "", lane("pair", 2, None));
        }
        for _ in 0..3 {
            publish_flow(&broker, "", lane("rated", 10, Some(1)));
        }
        let engine = engine_for(&broker);
        let leases = LeaseTable::new();
        let jobs = claim(&engine, &leases, "w").await;
        let count = |key: &str| {
            jobs.iter()
                .filter(|j| j.message.as_ref().unwrap().flow_key.as_deref() == Some(key))
                .count()
        };
        assert_eq!(count("pair"), 2, "parallelism 2");
        assert_eq!(count("rated"), 1, "rate 1 per 60 s");
        assert!(claim(&engine, &leases, "w").await.is_empty());
    }

    #[tokio::test]
    async fn paused_queue_is_not_claimed() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap();
        let queue = broker
            .create_subscription(broker_partition::CreateSubscriptionRequest {
                topic: "jobs".into(),
                url: "https://example.com/hook".into(),
                secret: "secret".into(),
                parallelism: None,
                default_max_retries: None,
                retry_backoff: None,
            })
            .unwrap();
        publish_flow(&broker, "jobs", None);
        let mut paused = broker.get_queue_by_id(queue.id).unwrap().unwrap();
        paused.paused = true;
        broker.upsert_subscription_catalog(paused.clone()).unwrap();
        let engine = engine_for(&broker);
        let leases = LeaseTable::new();
        assert!(claim(&engine, &leases, "w").await.is_empty());

        paused.paused = false;
        broker.upsert_subscription_catalog(paused).unwrap();
        let jobs = claim(&engine, &leases, "w").await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].message.as_ref().unwrap().queue_id, Some(queue.id));
    }
}
