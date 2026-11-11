//! Automatic WAL retention: decide how much of each log may be deleted, delete
//! whole sealed segments below that point, and reclaim their payload blobs.
//!
//! Per log the shard leader keeps a persisted **retention frontier**: the end
//! of the contiguous prefix whose records are fully dispatched (or purged).
//! DLQ records are never "dispatched", so they hold the frontier until an
//! operator or DLQ retention purges them. The safe watermark is the minimum of
//! the frontier, every outstanding retry / DLQ transition / lease, the local
//! committed HWM, the slowest replica, and the newest record younger than
//! `min_age` (defence in depth: a frontier bug leaves a day to notice before
//! data is gone). Storage then rounds down to a whole sealed segment and keeps
//! any segment that is still being archived.
//!
//! Payload blobs of deleted (and purged) records are recorded durably as
//! tombstones *before* the segment is unlinked, and deleted after a grace
//! period that covers in-flight hydration on nodes with a stale view.

use crate::broker::{Broker, BrokerError, LogId};
use crate::flow::flow_lane_owner;
use crate::topic::is_dlq_topic;
use broker_payload::{BlobStore, PayloadRef};
use broker_storage::StoredMessage;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

const STATE_VERSION: u16 = 1;
const SCAN_BATCH: usize = 512;
/// Age marks kept per log; adjacent marks are merged (conservatively) beyond.
const MAX_AGE_MARKS: usize = 512;

fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// Retention settings (environment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionConfig {
    /// `BETTERMQ_RETENTION` (default on; `off`/`0`/`false` disables GC).
    pub enabled: bool,
    /// `BETTERMQ_RETENTION_INTERVAL_SECS` (default 60).
    pub interval: Duration,
    /// `BETTERMQ_RETENTION_MIN_AGE_SECS` (default 86400): records younger
    /// than this are never deleted, even when fully delivered.
    pub min_age: Duration,
    /// `BETTERMQ_PAYLOAD_BLOB_GC_GRACE_SECS` (default 3600).
    pub blob_grace: Duration,
    /// `BETTERMQ_RETENTION_SCAN_LIMIT` (default 100000): records the frontier
    /// may advance over per log per tick, bounding work on the shard actor.
    pub scan_limit: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(60),
            min_age: Duration::from_secs(86_400),
            blob_grace: Duration::from_secs(3_600),
            scan_limit: 100_000,
        }
    }
}

impl RetentionConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        let enabled = std::env::var("BETTERMQ_RETENTION").map_or(true, |value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | "disabled"
            )
        });
        Self {
            enabled,
            interval: Duration::from_secs(
                env_secs(
                    "BETTERMQ_RETENTION_INTERVAL_SECS",
                    defaults.interval.as_secs(),
                )
                .max(1),
            ),
            min_age: Duration::from_secs(env_secs(
                "BETTERMQ_RETENTION_MIN_AGE_SECS",
                defaults.min_age.as_secs(),
            )),
            blob_grace: Duration::from_secs(env_secs(
                "BETTERMQ_PAYLOAD_BLOB_GC_GRACE_SECS",
                defaults.blob_grace.as_secs(),
            )),
            scan_limit: env_secs("BETTERMQ_RETENTION_SCAN_LIMIT", defaults.scan_limit as u64).max(1)
                as usize,
        }
    }
}

/// Records below `end` (and at or above the previous mark) were published no
/// later than `max_published_at_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct AgeMark {
    end: u64,
    max_published_at_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct LogState {
    frontier: u64,
    #[serde(default)]
    age_marks: Vec<AgeMark>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogStateEntry {
    log: LogId,
    #[serde(flatten)]
    state: LogState,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    version: u16,
    logs: Vec<LogStateEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlobTombstone {
    reference: PayloadRef,
    condemned_at_ms: i64,
}

/// Which term of the safe watermark is the binding (smallest) one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionLimit {
    /// Oldest record not yet fully dispatched (or an unpurged DLQ record).
    Undelivered,
    /// Records younger than `min_age`.
    MinAge,
    /// A pending retry, unfinished DLQ transition, or lease.
    DispatchState,
    /// Local committed (fsynced) high watermark.
    Committed,
    /// A replica that has not durably acknowledged this range, or whose
    /// progress is unknown.
    Replica,
}

/// Inputs and result of one safe-watermark computation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SafeWatermark {
    pub log: LogId,
    pub base_offset: u64,
    pub frontier: u64,
    pub age_floor: u64,
    pub dispatch_pin: Option<u64>,
    pub committed_hwm: u64,
    pub replica_floor: u64,
    /// Exclusive offset below which every record may be deleted.
    pub safe: u64,
    pub limited_by: RetentionLimit,
}

/// Outcome of applying a watermark to one local log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CollectReport {
    pub segments_deleted: usize,
    pub blobs_tombstoned: usize,
    /// New first retained offset.
    pub base_offset: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlobSweepReport {
    pub deleted: usize,
    pub missing: usize,
    pub failed: usize,
    pub pending: usize,
}

/// Durable retention state of this node: per-log frontiers and age marks
/// (`<data>/retention/state.json`) and blob tombstones
/// (`<data>/retention/blob-tombstones.jsonl`).
pub struct RetentionStore {
    state_path: PathBuf,
    tombstones_path: PathBuf,
    logs: Mutex<BTreeMap<LogId, LogState>>,
    tombstones: Mutex<()>,
}

fn io_error(error: std::io::Error) -> BrokerError {
    BrokerError::Storage(broker_storage::LogError::Io(error))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl RetentionStore {
    pub fn open(data_dir: &Path) -> Result<Self, BrokerError> {
        let dir = data_dir.join("retention");
        std::fs::create_dir_all(&dir).map_err(io_error)?;
        let state_path = dir.join("state.json");
        let logs = match std::fs::read(&state_path) {
            Ok(bytes) => match serde_json::from_slice::<StateFile>(&bytes) {
                Ok(file) if file.version == STATE_VERSION => file
                    .logs
                    .into_iter()
                    .map(|entry| (entry.log, entry.state))
                    .collect(),
                Ok(file) => {
                    return Err(BrokerError::InvalidConfig(format!(
                        "unsupported retention state version {} in {}",
                        file.version,
                        state_path.display()
                    )))
                }
                // Losing frontiers is safe: they are rebuilt from the log
                // base, and missing age marks only delay collection.
                Err(error) => {
                    tracing::warn!(
                        %error,
                        path = %state_path.display(),
                        "unreadable retention state; rebuilding frontiers from the log base"
                    );
                    BTreeMap::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(io_error(error)),
        };
        Ok(Self {
            state_path,
            tombstones_path: dir.join("blob-tombstones.jsonl"),
            logs: Mutex::new(logs),
            tombstones: Mutex::new(()),
        })
    }

    /// Persist frontiers and age marks.
    pub fn persist(&self) -> Result<(), BrokerError> {
        let file = StateFile {
            version: STATE_VERSION,
            logs: self
                .logs
                .lock()
                .iter()
                .map(|(log, state)| LogStateEntry {
                    log: log.clone(),
                    state: state.clone(),
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&file)
            .map_err(|error| BrokerError::InvalidConfig(error.to_string()))?;
        broker_storage::atomic_write_file(&self.state_path, &bytes).map_err(io_error)
    }

    pub fn frontier(&self, log: &LogId) -> Option<u64> {
        self.logs.lock().get(log).map(|state| state.frontier)
    }

    /// Advance the retention frontier of `log` over records that are fully
    /// dispatched (or purged, which the log no longer returns). Stops at the
    /// first record still needing work, at the committed HWM, or after
    /// `scan_limit` records. Returns the new frontier.
    pub fn advance_frontier(
        &self,
        broker: &Broker,
        log: &LogId,
        scan_limit: usize,
    ) -> Result<u64, BrokerError> {
        let base = broker.log_retention_status(log, 0)?.base_offset;
        let committed = broker.log_committed_hwm(log)?;
        let start = self.frontier(log).unwrap_or(0).max(base);
        let mut cursor = start;
        let mut scanned = 0usize;
        let mut newest = i64::MIN;
        'scan: while cursor < committed && scanned < scan_limit {
            let batch = broker.read_log(log, cursor, SCAN_BATCH)?;
            if batch.is_empty() {
                // Nothing retained at or after the cursor (purged tail).
                cursor = committed;
                break;
            }
            for message in batch {
                if message.offset < cursor {
                    continue;
                }
                if message.offset >= committed {
                    cursor = committed;
                    break 'scan;
                }
                if !record_fully_handled(broker, &message)? {
                    cursor = message.offset;
                    break 'scan;
                }
                newest = newest.max(message.published_at_ms);
                cursor = message.offset + 1;
                scanned += 1;
                if scanned >= scan_limit {
                    break 'scan;
                }
            }
        }
        let mut logs = self.logs.lock();
        let state = logs.entry(log.clone()).or_default();
        state.age_marks.retain(|mark| mark.end > base);
        if cursor > state.frontier {
            state.age_marks.push(AgeMark {
                end: cursor,
                max_published_at_ms: newest,
            });
            if state.age_marks.len() > MAX_AGE_MARKS {
                state.age_marks = merge_age_marks(&state.age_marks);
            }
        }
        state.frontier = state.frontier.max(cursor);
        Ok(state.frontier)
    }

    /// End of the prefix whose records were all published at or before
    /// `cutoff_ms` (never past the frontier).
    fn age_floor(&self, log: &LogId, base: u64, cutoff_ms: i64) -> u64 {
        let logs = self.logs.lock();
        let Some(state) = logs.get(log) else {
            return base;
        };
        let mut floor = base;
        for mark in &state.age_marks {
            if mark.end <= base {
                continue;
            }
            if mark.max_published_at_ms > cutoff_ms {
                break;
            }
            floor = mark.end;
        }
        floor.min(state.frontier.max(base))
    }

    /// Advance the frontier and combine every retention term for `log`.
    /// `dispatch_pin` is the lowest offset still referenced by dispatch state;
    /// `replica_floor` the lowest durable HWM over all other replicas
    /// (`u64::MAX` without replicas, the base when progress is unknown).
    pub fn safe_watermark(
        &self,
        broker: &Broker,
        log: &LogId,
        config: &RetentionConfig,
        dispatch_pin: Option<u64>,
        replica_floor: u64,
    ) -> Result<SafeWatermark, BrokerError> {
        let frontier = self.advance_frontier(broker, log, config.scan_limit)?;
        let base = broker.log_retention_status(log, 0)?.base_offset;
        let committed_hwm = broker.log_committed_hwm(log)?;
        let min_age_ms = i64::try_from(config.min_age.as_millis()).unwrap_or(i64::MAX);
        let age_floor = self.age_floor(log, base, now_ms().saturating_sub(min_age_ms));
        // A pin below the base refers to a record that is already collected.
        let dispatch_pin = dispatch_pin.filter(|pin| *pin >= base);
        let terms = [
            (frontier, RetentionLimit::Undelivered),
            (age_floor, RetentionLimit::MinAge),
            (
                dispatch_pin.unwrap_or(u64::MAX),
                RetentionLimit::DispatchState,
            ),
            (committed_hwm, RetentionLimit::Committed),
            (replica_floor, RetentionLimit::Replica),
        ];
        let (safe, limited_by) = terms
            .into_iter()
            .min_by_key(|(value, _)| *value)
            .expect("non-empty terms");
        Ok(SafeWatermark {
            log: log.clone(),
            base_offset: base,
            frontier,
            age_floor,
            dispatch_pin,
            committed_hwm,
            replica_floor,
            safe: safe.max(base),
            limited_by,
        })
    }

    /// Delete the whole sealed segments of `log` below `watermark` (clamped
    /// to the local committed HWM). Payload blobs of the doomed records are
    /// tombstoned durably first. Segments still being archived are kept.
    pub fn collect(
        &self,
        broker: &Broker,
        log: &LogId,
        watermark: u64,
    ) -> Result<CollectReport, BrokerError> {
        let target = watermark.min(broker.log_committed_hwm(log)?);
        let status = broker.log_retention_status(log, target)?;
        let boundary = status.gc_boundary;
        if boundary <= status.base_offset {
            return Ok(CollectReport {
                base_offset: status.base_offset,
                ..CollectReport::default()
            });
        }
        let mut references = Vec::new();
        let mut cursor = status.base_offset;
        'scan: loop {
            let batch = broker.read_log(log, cursor, SCAN_BATCH)?;
            if batch.is_empty() {
                break;
            }
            for message in batch {
                if message.offset >= boundary {
                    break 'scan;
                }
                if let Some(reference) = payload_reference(&message) {
                    references.push(reference);
                }
                cursor = message.offset + 1;
            }
        }
        let blobs_tombstoned = references.len();
        self.tombstone_blobs(references)?;
        let segments_deleted = broker.gc_log(log, boundary)?;
        let base_offset = broker.log_retention_status(log, 0)?.base_offset;
        if let Some(state) = self.logs.lock().get_mut(log) {
            state.age_marks.retain(|mark| mark.end > base_offset);
            state.frontier = state.frontier.max(base_offset);
        }
        record_collect(log, segments_deleted);
        Ok(CollectReport {
            segments_deleted,
            blobs_tombstoned,
            base_offset,
        })
    }

    /// Durably record blobs whose last reference is about to disappear.
    pub fn tombstone_blobs(&self, references: Vec<PayloadRef>) -> Result<(), BrokerError> {
        if references.is_empty() {
            return Ok(());
        }
        let condemned_at_ms = now_ms();
        let mut lines = Vec::new();
        for reference in references {
            serde_json::to_writer(
                &mut lines,
                &BlobTombstone {
                    reference,
                    condemned_at_ms,
                },
            )
            .map_err(|error| BrokerError::InvalidConfig(error.to_string()))?;
            lines.push(b'\n');
        }
        let _guard = self.tombstones.lock();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.tombstones_path)
            .map_err(io_error)?;
        file.write_all(&lines).map_err(io_error)?;
        file.sync_data().map_err(io_error)
    }

    /// Delete tombstoned blobs older than `grace`; failures stay queued.
    pub fn sweep_blobs(
        &self,
        store: &BlobStore,
        grace: Duration,
    ) -> Result<BlobSweepReport, BrokerError> {
        let _guard = self.tombstones.lock();
        let file = match std::fs::File::open(&self.tombstones_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BlobSweepReport::default())
            }
            Err(error) => return Err(io_error(error)),
        };
        let grace_ms = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        let due_before = now_ms().saturating_sub(grace_ms);
        let mut report = BlobSweepReport::default();
        let mut keep = Vec::new();
        let mut changed = false;
        for line in std::io::BufReader::new(file).split(b'\n') {
            let line = line.map_err(io_error)?;
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            // A torn final line from a crash mid-append: its segment was not
            // unlinked yet, so the next collection tombstones it again.
            let Ok(tombstone) = serde_json::from_slice::<BlobTombstone>(&line) else {
                changed = true;
                continue;
            };
            if tombstone.condemned_at_ms > due_before {
                keep.extend_from_slice(&line);
                keep.push(b'\n');
                continue;
            }
            changed = true;
            match store.delete_blob(&tombstone.reference) {
                Ok(true) => report.deleted += 1,
                Ok(false) => report.missing += 1,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        key = %tombstone.reference.bucket_key,
                        "payload blob delete failed; will retry"
                    );
                    report.failed += 1;
                    keep.extend_from_slice(&line);
                    keep.push(b'\n');
                }
            }
        }
        report.pending = keep.iter().filter(|byte| **byte == b'\n').count();
        if changed {
            broker_storage::atomic_write_file(&self.tombstones_path, &keep).map_err(io_error)?;
        }
        record_sweep(&report);
        Ok(report)
    }
}

/// True when `message` needs no further delivery work.
fn record_fully_handled(broker: &Broker, message: &StoredMessage) -> Result<bool, BrokerError> {
    if is_dlq_topic(&message.topic) {
        // Retained until purged (operator or DLQ retention).
        return Ok(false);
    }
    let tenant = if message.tenant_id.is_empty() {
        broker.tenant()
    } else {
        message.tenant_id.clone()
    };
    broker.is_dispatch_complete(
        &tenant,
        &flow_lane_owner(message).to_string(),
        message.partition,
        message.offset,
    )
}

pub(crate) fn payload_reference(message: &StoredMessage) -> Option<PayloadRef> {
    let json = message.payload_ref_json.as_deref()?;
    match serde_json::from_str(json) {
        Ok(reference) => Some(reference),
        Err(error) => {
            tracing::warn!(%error, offset = message.offset, "unparseable payload reference");
            None
        }
    }
}

/// Halve the number of marks by merging neighbours: the merged mark covers
/// both ranges with the newer timestamp, which can only delay collection.
fn merge_age_marks(marks: &[AgeMark]) -> Vec<AgeMark> {
    marks
        .chunks(2)
        .map(|pair| AgeMark {
            end: pair[pair.len() - 1].end,
            max_published_at_ms: pair
                .iter()
                .map(|mark| mark.max_published_at_ms)
                .max()
                .unwrap_or(i64::MIN),
        })
        .collect()
}

/// Per-log retention gauges for `/metrics`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LogRetentionTelemetry {
    pub retained_bytes: u64,
    pub base_offset: u64,
    pub frontier: Option<u64>,
    pub safe_watermark: Option<u64>,
    pub gc_watermark: Option<u64>,
    pub limited_by: Option<RetentionLimit>,
    pub segments_deleted_total: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RetentionTelemetrySnapshot {
    pub logs: BTreeMap<String, LogRetentionTelemetry>,
    pub blobs_deleted_total: u64,
    pub blob_delete_failures_total: u64,
    pub blob_tombstones_pending: u64,
    pub errors_total: u64,
}

fn telemetry() -> &'static Mutex<RetentionTelemetrySnapshot> {
    static TELEMETRY: OnceLock<Mutex<RetentionTelemetrySnapshot>> = OnceLock::new();
    TELEMETRY.get_or_init(Default::default)
}

pub fn retention_telemetry_snapshot() -> RetentionTelemetrySnapshot {
    telemetry().lock().clone()
}

/// Record the gauges of one log after a retention pass.
pub fn record_log_retention(
    log: &LogId,
    retained_bytes: u64,
    base_offset: u64,
    safe: Option<&SafeWatermark>,
    gc_watermark: Option<u64>,
) {
    let mut snapshot = telemetry().lock();
    let entry = snapshot.logs.entry(log.to_string()).or_default();
    entry.retained_bytes = retained_bytes;
    entry.base_offset = base_offset;
    if let Some(safe) = safe {
        entry.frontier = Some(safe.frontier);
        entry.safe_watermark = Some(safe.safe);
        entry.limited_by = Some(safe.limited_by);
    }
    if gc_watermark.is_some() {
        entry.gc_watermark = gc_watermark;
    }
}

pub fn record_retention_error() {
    telemetry().lock().errors_total += 1;
}

fn record_collect(log: &LogId, segments: usize) {
    let mut snapshot = telemetry().lock();
    snapshot
        .logs
        .entry(log.to_string())
        .or_default()
        .segments_deleted_total += segments as u64;
}

fn record_sweep(report: &BlobSweepReport) {
    let mut snapshot = telemetry().lock();
    snapshot.blobs_deleted_total += report.deleted as u64;
    snapshot.blob_delete_failures_total += report.failed as u64;
    snapshot.blob_tombstones_pending = report.pending as u64;
}

/// Lowest pinned offset per log from `(topic, partition, offset)` pins.
pub fn pins_by_log(
    broker: &Broker,
    pins: impl IntoIterator<Item = (String, u32, u64)>,
) -> HashMap<LogId, u64> {
    let mut by_log: HashMap<LogId, u64> = HashMap::new();
    for (topic, partition, offset) in pins {
        let entry = by_log
            .entry(broker.log_id(&topic, partition))
            .or_insert(offset);
        *entry = (*entry).min(offset);
    }
    by_log
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerConfig, PublishRequest};
    use crate::topic::DIRECT_TOPIC;
    use broker_storage::{FsyncMode, PartitionLogConfig};
    use tempfile::tempdir;

    fn open_broker(dir: &Path) -> Broker {
        let mut config = BrokerConfig::new(dir.to_path_buf());
        config.partitions = 1;
        config.log = PartitionLogConfig {
            segment_max_bytes: 2048,
            fsync: FsyncMode::Always,
            group_interval: Duration::from_millis(5),
        };
        Broker::open(config).unwrap()
    }

    fn request(topic: &str, payload: String) -> PublishRequest {
        let direct = topic.is_empty();
        PublishRequest {
            topic: topic.into(),
            queue_id: None,
            group_id: None,
            group_member_id: None,
            routing_key: "lane".into(),
            payload,
            payload_encoding: None,
            idempotency_key: None,
            delay_ms: None,
            priority: None,
            flow_id: None,
            url: direct.then(|| "http://127.0.0.1:9/h".into()),
            secret: direct.then(|| "s".into()),
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
        }
    }

    fn publish(broker: &Broker, count: usize) -> Vec<u64> {
        (0..count)
            .map(|index| {
                broker
                    .publish(request("", format!("body-{index}-{}", "x".repeat(200))))
                    .unwrap()
                    .offset
                    .unwrap()
            })
            .collect()
    }

    fn complete(broker: &Broker, log: &LogId, below: u64) {
        for message in broker.read_log(log, 0, 10_000).unwrap() {
            if message.offset < below {
                broker
                    .mark_dispatch_complete(
                        &message.tenant_id,
                        &flow_lane_owner(&message).to_string(),
                        message.partition,
                        message.offset,
                    )
                    .unwrap();
            }
        }
    }

    fn config(min_age: Duration) -> RetentionConfig {
        RetentionConfig {
            min_age,
            ..RetentionConfig::default()
        }
    }

    fn direct_log(broker: &Broker) -> LogId {
        broker.log_id(DIRECT_TOPIC, 0)
    }

    #[test]
    fn frontier_stops_at_first_undelivered_record() {
        let dir = tempdir().unwrap();
        let broker = open_broker(dir.path());
        let offsets = publish(&broker, 20);
        let log = direct_log(&broker);
        let store = broker.retention();
        let cfg = config(Duration::ZERO);

        let safe = store
            .safe_watermark(&broker, &log, &cfg, None, u64::MAX)
            .unwrap();
        assert_eq!(safe.safe, 0);
        assert_eq!(safe.limited_by, RetentionLimit::Undelivered);

        complete(&broker, &log, offsets[12]);
        let safe = store
            .safe_watermark(&broker, &log, &cfg, None, u64::MAX)
            .unwrap();
        assert_eq!(safe.frontier, offsets[12]);
        assert_eq!(safe.safe, offsets[12]);

        // Every other term can bind instead.
        let pinned = store
            .safe_watermark(&broker, &log, &cfg, Some(offsets[3]), u64::MAX)
            .unwrap();
        assert_eq!(
            (pinned.safe, pinned.limited_by),
            (offsets[3], RetentionLimit::DispatchState)
        );
        let replica = store
            .safe_watermark(&broker, &log, &cfg, None, offsets[5])
            .unwrap();
        assert_eq!(
            (replica.safe, replica.limited_by),
            (offsets[5], RetentionLimit::Replica)
        );
        let young = store
            .safe_watermark(
                &broker,
                &log,
                &config(Duration::from_secs(3600)),
                None,
                u64::MAX,
            )
            .unwrap();
        assert_eq!((young.safe, young.limited_by), (0, RetentionLimit::MinAge));
    }

    #[test]
    fn dlq_records_hold_the_frontier_until_purged() {
        let dir = tempdir().unwrap();
        let broker = open_broker(dir.path());
        let topic = "jobs.__dlq";
        let first = broker
            .publish(request(topic, "{}".into()))
            .unwrap()
            .offset
            .unwrap();
        broker.publish(request(topic, "{}".into())).unwrap();
        let log = broker.log_id(topic, 0);
        let store = broker.retention();
        let cfg = config(Duration::ZERO);
        assert_eq!(
            store
                .safe_watermark(&broker, &log, &cfg, None, u64::MAX)
                .unwrap()
                .frontier,
            first
        );
        assert!(broker.purge_dlq_message(topic, 0, first).unwrap());
        assert_eq!(
            store
                .safe_watermark(&broker, &log, &cfg, None, u64::MAX)
                .unwrap()
                .frontier,
            first + 1
        );
    }

    #[test]
    fn collect_deletes_delivered_segments_and_survives_reopen() {
        let dir = tempdir().unwrap();
        let broker = open_broker(dir.path());
        let offsets = publish(&broker, 60);
        let log = direct_log(&broker);
        complete(&broker, &log, offsets[40]);
        let store = broker.retention();
        let safe = store
            .safe_watermark(&broker, &log, &config(Duration::ZERO), None, u64::MAX)
            .unwrap();
        assert_eq!(safe.safe, offsets[40]);
        let report = store.collect(&broker, &log, safe.safe).unwrap();
        assert!(report.segments_deleted > 0);
        assert!(report.base_offset > 0 && report.base_offset <= offsets[40]);
        let remaining = broker.read_log(&log, 0, 10_000).unwrap();
        assert_eq!(remaining.first().unwrap().offset, report.base_offset);
        assert_eq!(remaining.last().unwrap().offset, *offsets.last().unwrap());
        store.persist().unwrap();
        drop(broker);

        let broker = open_broker(dir.path());
        let remaining = broker.read_log(&log, 0, 10_000).unwrap();
        assert_eq!(remaining.first().unwrap().offset, report.base_offset);
        assert_eq!(
            broker.retention().frontier(&log),
            Some(offsets[40]),
            "frontier persisted"
        );
        // A second pass after reopen never passes undelivered data.
        let safe = broker
            .retention()
            .safe_watermark(&broker, &log, &config(Duration::ZERO), None, u64::MAX)
            .unwrap();
        assert_eq!(safe.safe, offsets[40]);
        let again = broker
            .retention()
            .collect(&broker, &log, safe.safe)
            .unwrap();
        assert!(again.base_offset <= offsets[40]);
    }

    #[test]
    fn collected_and_purged_blobs_are_deleted_after_grace() {
        let dir = tempdir().unwrap();
        let broker = open_broker(dir.path());
        let big = "b".repeat(broker_payload::inline_max_bytes() + 1);
        let mut offsets = Vec::new();
        for _ in 0..30 {
            offsets.push(
                broker
                    .publish(request("", big.clone()))
                    .unwrap()
                    .offset
                    .unwrap(),
            );
            offsets.extend(publish(&broker, 2));
        }
        let log = direct_log(&broker);
        let references: Vec<PayloadRef> = broker
            .read_log(&log, 0, 10_000)
            .unwrap()
            .iter()
            .filter_map(payload_reference)
            .collect();
        assert_eq!(references.len(), 30);
        complete(&broker, &log, u64::MAX);
        let store = broker.retention();
        let report = store.collect(&broker, &log, u64::MAX).unwrap();
        assert!(report.blobs_tombstoned > 0);

        // Within the grace period nothing is deleted.
        let early = store
            .sweep_blobs(broker.blob_store(), Duration::from_secs(3600))
            .unwrap();
        assert_eq!(early.deleted, 0);
        assert_eq!(early.pending, report.blobs_tombstoned);
        let swept = store
            .sweep_blobs(broker.blob_store(), Duration::ZERO)
            .unwrap();
        assert_eq!(swept.deleted, report.blobs_tombstoned);
        assert_eq!(swept.pending, 0);
        let (gone, kept): (Vec<_>, Vec<_>) = references
            .iter()
            .partition(|reference| broker.blob_store().get_blob(reference).is_err());
        assert_eq!(gone.len(), report.blobs_tombstoned);
        // Blobs of records still retained are untouched.
        assert!(kept
            .iter()
            .all(|reference| broker.blob_store().get_blob(reference).is_ok()));
    }

    #[test]
    fn purged_dlq_blob_is_tombstoned() {
        let dir = tempdir().unwrap();
        let broker = open_broker(dir.path());
        let topic = "jobs.__dlq";
        let big = "d".repeat(broker_payload::inline_max_bytes() + 1);
        let offset = broker.publish(request(topic, big)).unwrap().offset.unwrap();
        let message = broker.read_message(topic, 0, offset).unwrap();
        let reference = payload_reference(&message).unwrap();
        assert!(broker.purge_dlq_message(topic, 0, offset).unwrap());
        broker
            .retention()
            .sweep_blobs(broker.blob_store(), Duration::ZERO)
            .unwrap();
        assert!(broker.blob_store().get_blob(&reference).is_err());
    }

    #[test]
    fn empty_follower_continues_at_a_collected_leader_base() {
        let leader_dir = tempdir().unwrap();
        let leader = open_broker(leader_dir.path());
        let offsets = publish(&leader, 60);
        let log = direct_log(&leader);
        complete(&leader, &log, u64::MAX);
        let base = leader
            .retention()
            .collect(&leader, &log, offsets[40])
            .unwrap()
            .base_offset;
        assert!(base > 0);

        let follower_dir = tempdir().unwrap();
        let follower = open_broker(follower_dir.path());
        let frames = leader
            .replication_frames(DIRECT_TOPIC, 0, 0, 10_000)
            .unwrap();
        assert_eq!(frames.first().unwrap().0, base);
        // Without a reset the first retained frame cannot be appended.
        assert!(follower
            .append_replicated_frame(DIRECT_TOPIC, 0, &frames[0].1, Some(frames[0].0))
            .is_err());
        follower.reset_log_base(&log, base).unwrap();
        for (offset, frame) in &frames {
            follower
                .append_replicated_frame(DIRECT_TOPIC, 0, frame, Some(*offset))
                .unwrap();
        }
        let copied = follower.read_log(&log, 0, 10_000).unwrap();
        assert_eq!(copied.first().unwrap().offset, base);
        assert_eq!(copied.last().unwrap().offset, *offsets.last().unwrap());
    }

    #[test]
    fn age_marks_merge_conservatively() {
        let marks: Vec<_> = (1..=5)
            .map(|index| AgeMark {
                end: index * 10,
                max_published_at_ms: index as i64,
            })
            .collect();
        assert_eq!(
            merge_age_marks(&marks),
            vec![
                AgeMark {
                    end: 20,
                    max_published_at_ms: 2
                },
                AgeMark {
                    end: 40,
                    max_published_at_ms: 4
                },
                AgeMark {
                    end: 50,
                    max_published_at_ms: 5
                },
            ]
        );
    }
}
